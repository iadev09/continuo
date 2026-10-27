use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

mod bootable;
mod component;
mod erased;
mod error;
mod finalizable;
mod order;
pub mod priority;
mod register;
mod reload;
mod resolve;
mod runnable;
mod validation;

pub use bootable::Bootable;
pub use component::Component;
pub use erased::{AnyComponent, AnyRunnable};
pub use error::{BoxError, Error, Result, StdError};
pub use finalizable::{Finalizable, FinalizeFailure, FinalizeOutcome};
pub use order::ComponentOrder;
pub use register::Register;
pub use reload::{ReloadFailure, ReloadOutcome, ReloadState, Reloadable};
pub use resolve::{MissingDependency, Resolve};
pub use runnable::Runnable;
pub use validation::{ValidationFailure, ValidationOutcome};

/// A component with the `TypeId` it is registered under.
type KeyedComponent<S> = (TypeId, Arc<dyn AnyComponent<S>>);

/// Type-erased component registry used for service discovery and DI-style lookup.
/// Registration happens during bootstrap and runtime access is read-only via typed resolves.
/// We keep the underlying maps behind `RwLock<HashMap<..>>` so registration stays simple while
/// lookup only holds a short-lived read lock long enough to clone the stored `Arc`.
pub struct Registry<S> {
    entries: Arc<RegistryEntries<S>>,
}

struct RegistryEntries<S> {
    components: RwLock<HashMap<TypeId, Arc<dyn AnyComponent<S>>>>,
    by_type: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    dyn_by_type: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    registration_order: RwLock<Vec<TypeId>>,
    lifecycle_order: RwLock<Option<Vec<TypeId>>>,
}

pub(crate) struct RunnableEntry<S> {
    pub(crate) name: &'static str,
    pub(crate) component: Arc<dyn AnyComponent<S>>,
    pub(crate) runnable: Arc<dyn AnyRunnable<S>>,
}

impl<S: 'static> Registry<S> {
    /// Create the service with an empty registry. You can register later.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RegistryEntries {
                components: RwLock::new(HashMap::new()),
                by_type: RwLock::new(HashMap::new()),
                dyn_by_type: RwLock::new(HashMap::new()),
                registration_order: RwLock::new(Vec::new()),
                lifecycle_order: RwLock::new(None),
            }),
        }
    }

    /// Retain the same registry for dependency admission on later generations.
    /// This is not a snapshot: DI values published after boot remain visible.
    pub(crate) fn shared(&self) -> Self {
        Self { entries: self.entries.clone() }
    }

    /// Store a shared value for typed dependency injection only.
    ///
    /// No lifecycle trait is required or discovered. Use `register` for an
    /// object that is also a lifecycle participant.
    pub fn insert<T>(
        &self,
        item: Arc<T>,
    ) -> Result<&Self>
    where
        T: Any + Send + Sync,
    {
        let mut values = self
            .entries
            .by_type
            .write()
            .expect("registry by_type lock poisoned");
        let type_id = TypeId::of::<T>();
        if values.contains_key(&type_id) {
            return Err(Error::DuplicateComponent {
                type_name: std::any::type_name::<T>(),
            });
        }
        values.insert(type_id, item);
        Ok(self)
    }

    /// Register one shared component for both DI and lifecycle orchestration.
    ///
    /// This accepts `Arc<P>` where `P: Component`. The service is stored as a
    /// type-erased `Arc<dyn Component>` but continues to point to the same underlying
    /// allocation (no new allocation is created).
    ///
    /// If another service with the same concrete type is already registered,
    /// registration is rejected.
    ///
    /// # Resolving during registration
    ///
    /// Registration runs in insertion order, and nothing orders it — that is
    /// what `boot_all` and [`ComponentOrder`] are for. So a `register` function
    /// that resolves another component is reading a registry that may or may not
    /// contain it yet, and the result is decided by the order two lines happen
    /// to appear in somebody's bootstrap.
    ///
    /// The rule is not "never resolve". It is: **do not resolve a component you
    /// have not ensured.** Calling that component's own idempotent `register`
    /// first and then resolving is fine and order-independent; so is resolving
    /// your own type to avoid a duplicate insert. What is not fine is
    /// `if let Some(x) = resolve::<X>() { … }` with no else, because the
    /// difference between "X is not part of this deployment" and "X has not
    /// been registered yet" is invisible, and the second one is silent.
    ///
    /// Anything that genuinely depends on another component's *state* belongs in
    /// `boot`, where the ordering system has already run.
    ///
    /// Returns `&Self` to allow fallible fluent chaining:
    ///
    /// ```ignore
    /// registry
    ///     .register(dns.clone())?
    ///     .register(ipc.clone())?;
    /// ```
    pub fn register<P>(
        &self,
        item: Arc<P>,
    ) -> Result<&Self>
    where
        P: Component<S>,
        S: Send + Sync,
    {
        let type_id = TypeId::of::<P>();
        let any: Arc<dyn Any + Send + Sync> = item.clone();
        let mut components = self
            .entries
            .components
            .write()
            .expect("registry components lock poisoned");
        let mut by_type = self
            .entries
            .by_type
            .write()
            .expect("registry by_type lock poisoned");
        if by_type.contains_key(&type_id) || components.contains_key(&type_id) {
            return Err(Error::DuplicateComponent {
                type_name: std::any::type_name::<P>(),
            });
        }
        by_type.insert(type_id, any);
        drop(by_type);

        let it: Arc<dyn AnyComponent<S>> = item;
        components.insert(type_id, it);
        self.entries
            .registration_order
            .write()
            .expect("registry order lock poisoned")
            .push(type_id);
        *self
            .entries
            .lifecycle_order
            .write()
            .expect("registry lifecycle order lock poisoned") = None;
        Ok(self)
    }

    /// Bind a registered concrete component as a trait-object capability.
    ///
    /// This does not register a second lifecycle component. It resolves the
    /// existing concrete component `P`, casts the same `Arc<P>` into `Arc<I>`,
    /// and stores that trait-object handle under `TypeId::of::<I>()`.
    ///
    /// Returns an error if `P` has not been registered yet. If `I` already has
    /// a binding, the new binding replaces it.
    pub fn bind_dyn<I, P>(
        &self,
        cast: impl FnOnce(Arc<P>) -> Arc<I>,
    ) -> Result<&Self>
    where
        I: ?Sized + Send + Sync + 'static,
        P: Any + Send + Sync,
    {
        let concrete = self.resolve::<P>().ok_or_else(|| {
            Error::msg(format!(
                "bind_dyn: component type '{}' is not registered",
                std::any::type_name::<P>()
            ))
        })?;

        let interface_id = TypeId::of::<I>();
        let mut dyn_by_type = self
            .entries
            .dyn_by_type
            .write()
            .expect("registry dyn_by_type lock poisoned");
        let erased: Box<dyn Any + Send + Sync> = Box::new(cast(concrete));
        if dyn_by_type.insert(interface_id, erased).is_some() {
            tracing::debug!(
                interface = std::any::type_name::<I>(),
                "replaced dynamic capability binding"
            );
        }
        Ok(self)
    }

    /// Execute a closure with a concrete typed reference `&T` if the service is registered.
    pub fn with_typed<T, R>(
        &self,
        f: impl FnOnce(&T) -> R,
    ) -> Option<R>
    where
        T: Any + Send + Sync,
    {
        let typed = self.resolve::<T>()?;
        Some(f(typed.as_ref()))
    }

    /// Resolve a concrete service as an owned `Arc<T>` handle.
    ///
    /// This is the DI-style, high-level API: it returns a typed `Arc<T>` that
    /// points to the same underlying allocation as the internally registered
    /// component (no new `Arc` allocation). The returned `Arc` is obtained by
    /// downcasting from a type-indexed map (`TypeId`).
    ///
    /// Returns `None` if the type is not registered.
    pub fn resolve<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        let any = self
            .entries
            .by_type
            .read()
            .expect("registry by_type lock poisoned")
            .get(&TypeId::of::<T>())?
            .clone();
        Arc::downcast::<T>(any).ok()
    }

    /// Resolve a registered component's lifecycle capabilities by runtime
    /// type, without requiring a DI binding.
    pub fn resolve_component(
        &self,
        type_id: TypeId,
    ) -> Option<Arc<dyn AnyComponent<S>>> {
        self.entries
            .components
            .read()
            .expect("registry components lock poisoned")
            .get(&type_id)
            .cloned()
    }

    /// Registered component identities and shared lifecycle handles.
    pub fn component_entries(&self) -> Vec<(TypeId, Arc<dyn AnyComponent<S>>)> {
        self.component_entries_snapshot()
            .into_iter()
            .map(|entry| (entry.type_id, entry.component))
            .collect()
    }

    /// Configuration identity is independent of the component exposing it.
    /// A preparation wrapper and its ready service may expose the same owner;
    /// retain the last contribution (the ready service), in lifecycle order.
    /// Congif's owner identity addresses reading, watching and acknowledgement.
    #[cfg(feature = "config")]
    pub(crate) fn configuration_entries(
        &self
    ) -> Result<Vec<KeyedComponent<S>>> {
        let entries = self.lifecycle_entries()?;
        let mut seen = HashSet::new();
        let mut owners = Vec::new();
        for (_, component) in entries.into_iter().rev() {
            let Some(config) = component.as_configurable() else { continue };
            let owner = config.type_id();
            if seen.insert(owner) {
                owners.push((owner, component));
            }
        }
        owners.reverse();
        Ok(owners)
    }

    /// Resolve a trait-object capability bound with `bind_dyn`.
    pub fn resolve_dyn<I>(&self) -> Option<Arc<I>>
    where
        I: ?Sized + Send + Sync + 'static,
    {
        self.entries
            .dyn_by_type
            .read()
            .expect("registry dyn_by_type lock poisoned")
            .get(&TypeId::of::<I>())?
            .downcast_ref::<Arc<I>>()
            .cloned()
    }

    /// Return a snapshot of registered components.
    pub fn components(&self) -> Vec<Arc<dyn AnyComponent<S>>> {
        self.entries
            .components
            .read()
            .expect("registry components lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn component_entries_snapshot(&self) -> Vec<ComponentEntry<S>> {
        let components = self
            .entries
            .components
            .read()
            .expect("registry components lock poisoned");
        self.entries
            .registration_order
            .read()
            .expect("registry order lock poisoned")
            .iter()
            .enumerate()
            .filter_map(|(index, type_id)| {
                components.get(type_id).cloned().map(|component| {
                    ComponentEntry { type_id: *type_id, index, component }
                })
            })
            .collect()
    }

    /// Return the cached lifecycle plan, building it once if needed.
    ///
    /// The plan is invalidated on lifecycle registration. DI insertion does
    /// not change it. Normal lifecycle phases reuse
    /// the same known list, so reload is a boot emulation over the same
    /// component order instead of a second ordering universe.
    fn lifecycle_plan(&self) -> Result<Vec<Arc<dyn AnyComponent<S>>>> {
        if let Some(type_ids) = self
            .entries
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .as_ref()
            .cloned()
        {
            return Ok(self.components_from_type_ids(&type_ids));
        }

        let ordered = order_component_entries(
            self.component_entries_snapshot(),
            lifecycle_priority,
        )?;
        let type_ids =
            ordered.iter().map(|entry| entry.type_id).collect::<Vec<_>>();
        let components = ordered
            .iter()
            .map(|entry| entry.component.clone())
            .collect::<Vec<_>>();
        #[cfg(debug_assertions)]
        tracing::debug!(
            components = ?components.iter().map(|component| component.name()).collect::<Vec<_>>(),
            "component lifecycle order"
        );
        *self
            .entries
            .lifecycle_order
            .write()
            .expect("registry lifecycle order lock poisoned") = Some(type_ids);
        Ok(components)
    }

    /// The lifecycle plan with each component's `TypeId` alongside it, for the
    /// one caller that has to know which components it has already handled.
    pub(crate) fn lifecycle_entries(&self) -> Result<Vec<KeyedComponent<S>>> {
        let plan = self.lifecycle_plan()?;
        let type_ids = self
            .entries
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .clone()
            .unwrap_or_default();

        Ok(type_ids.into_iter().zip(plan).collect())
    }

    fn components_from_type_ids(
        &self,
        type_ids: &[TypeId],
    ) -> Vec<Arc<dyn AnyComponent<S>>> {
        let components = self
            .entries
            .components
            .read()
            .expect("registry components lock poisoned");
        type_ids
            .iter()
            .filter_map(|type_id| components.get(type_id).cloned())
            .collect()
    }

    /// Return the list of component display names (for diagnostics only).
    pub fn list_names(&self) -> Vec<&'static str> {
        self.components().iter().map(|c| c.name()).collect()
    }

    /// Return component display names in lifecycle order.
    ///
    /// This is useful for diagnostics and startup logging before running
    /// `boot_all()`.
    pub fn lifecycle_names(&self) -> Result<Vec<&'static str>> {
        Ok(self
            .lifecycle_plan()?
            .iter()
            .map(|component| component.name())
            .collect())
    }

    /// Runnables in the order they should be started.
    ///
    /// Topological like boot, not a sort by magic number. This used to iterate
    /// `HashMap::values()` and sort by `(run_priority, name)`, so `ComponentOrder`
    /// meant something for boot and nothing for run, and a runnable that had to
    /// start after another had no way to say so. It is the same ordering pass
    /// boot uses, with `run_priority` choosing among the components that are
    /// genuinely independent.
    pub(crate) fn runnable_entries(&self) -> Result<Vec<RunnableEntry<S>>> {
        let ordered = order_component_entries(
            self.component_entries_snapshot(),
            run_priority,
        )?;

        let mut runnables = Vec::new();
        for entry in ordered {
            let component = entry.component;
            let Some(runnable) = component.clone().runnable() else {
                continue;
            };
            runnables.push(RunnableEntry {
                name: component.name(),
                component,
                runnable,
            });
        }

        Ok(runnables)
    }

    /// Run `validate` for every component in lifecycle order.
    ///
    /// Lifecycle-plan failures remain outer errors. A component validation
    /// failure does not prevent later components from being checked; every
    /// component failure is retained in the outcome.
    pub fn validate_all(
        &self,
        state: &S,
    ) -> Result<ValidationOutcome> {
        let mut outcome = ValidationOutcome::default();

        for component in self.lifecycle_plan()? {
            let name = component.name();
            match component.validate(state) {
                Ok(()) => outcome.validated_count += 1,
                Err(error) => outcome.failures.push(ValidationFailure {
                    component: name,
                    error: error.into_validate(name),
                }),
            }
        }
        Ok(outcome)
    }

    pub async fn boot_all(
        &self,
        state: &S,
    ) -> Result<()> {
        let mut booted: HashSet<TypeId> = HashSet::new();

        // A preparation component can publish new lifecycle participants.
        // Replan after each boot so newly declared dependencies are honored
        // before booting another component. Already completed hooks are not
        // rerun; dependencies must be declared before their consumer boots.
        loop {
            let mut progressed = false;

            for (type_id, component) in self.lifecycle_entries()? {
                if !booted.insert(type_id) {
                    continue;
                }
                progressed = true;

                if !component.has_bootable() {
                    continue;
                }
                let name = component.name();
                // Boot is a sequence of silent blocking steps, and when one
                // of them takes seconds the log shows a gap with nothing in
                // it — the next line to print gets the blame. Saying which
                // component is being waited on, and what it cost, is what
                // turns that gap into an answer.
                tracing::debug!("⏳ booting {name}");
                let started = std::time::Instant::now();
                if let Err(e) = component.boot(self, state).await {
                    return Err(e.into_boot(name));
                }
                tracing::debug!(
                    "✅ {name} booted in {} ms",
                    started.elapsed().as_millis()
                );
                // A constructor can publish new lifecycle participants. Replan
                // before booting any dependent from the old plan.
                break;
            }

            if !progressed {
                return Ok(());
            }
        }
    }

    /// Run `Finalizable::finalize` for every component that exposes the
    /// capability, in reverse lifecycle order. Called by the bootstrap
    /// layer after the runnable tasks have drained. One failure does not stop
    /// later finalizers; every failure is retained in the outcome.
    pub async fn finalize_all(
        &self,
        state: &S,
    ) -> Result<FinalizeOutcome> {
        let mut components = self.lifecycle_plan()?;
        components.reverse();
        let mut outcome = FinalizeOutcome::default();

        for component in components {
            let Some(finalizable) = component.as_finalizable() else {
                continue;
            };
            let name = component.name();
            match finalizable.finalize(state).await {
                Ok(()) => outcome.finalized_count += 1,
                Err(error) => outcome.failures.push(FinalizeFailure {
                    component: name,
                    error: error.into_finalize(name),
                }),
            }
        }
        Ok(outcome)
    }

    fn reload_target(
        &self,
        name: &str,
    ) -> Result<Arc<dyn AnyComponent<S>>> {
        let Some(component) = self
            .components()
            .into_iter()
            .find(|component| component.name() == name)
        else {
            return Err(Error::msg(format!(
                "reload_by_name: no component registered with name '{}'",
                name
            )));
        };

        Ok(component)
    }

    /// Invoke only the component reload hook, without preparing or acknowledging
    /// configuration. Callers own those steps. Use [`Self::reload_one`] with a
    /// [`ReloadState`] for a configuration-aware reload transaction.
    pub async fn reload_hook_one(
        &self,
        name: &str,
        state: &S,
    ) -> Result<()> {
        let component = self.reload_target(name)?;

        let Some(reloadable) = component.as_reloadable() else {
            return Err(Error::msg(format!(
                "reload_by_name: component '{}' is not reloadable",
                name
            )));
        };

        let component_name = component.name();
        reloadable
            .reload(state)
            .await
            .map_err(|error| error.into_reload(component_name))
    }
}

impl<S> Registry<S>
where
    S: ReloadState + 'static,
{
    /// Prepare the addressed owner's configuration, invoke its reload hook,
    /// and acknowledge only successful application. Failed candidates remain
    /// available for retry. The application-wide `ReloadState::reload` hook is
    /// reserved for broadcasts; this operation affects only the named component.
    /// States without a config reader retain lifecycle-only behavior.
    pub async fn reload_one(
        &self,
        name: &str,
        state: &S,
    ) -> Result<()> {
        let component = self.reload_target(name)?;

        let Some(reloadable) = component.as_reloadable() else {
            return Err(Error::msg(format!(
                "reload_by_name: component '{}' is not reloadable",
                name
            )));
        };

        #[cfg(feature = "config")]
        let reader = state.reload_config_reader();
        #[cfg(feature = "config")]
        let prepared = if let (Some(reader), Some(owner)) =
            (reader, component.as_configurable())
        {
            reader.prepare_owners(&[owner]).await?;
            reader.prepared_config(owner)
        } else {
            None
        };
        #[cfg(feature = "config")]
        if let (Some(reader), Some(environmental)) =
            (reader, component.as_environmental())
        {
            let owner = (*component).type_id();
            reader.prepare_programs(
                owner,
                environmental.program_environments(),
            )?;
            reader.refresh_programs(owner)?;
        }

        let component_name = component.name();
        reloadable
            .reload(state)
            .await
            .map_err(|error| error.into_reload(component_name))?;
        #[cfg(feature = "config")]
        if let (Some(reader), Some(prepared)) = (reader, prepared) {
            reader.confirm_applied(prepared).await?;
        }
        Ok(())
    }

    /// Reload every reloadable component, in lifecycle order.
    ///
    /// # Not atomic, and not cancel-safe
    ///
    /// Components own their own state and the trait has no two-phase commit, so
    /// there is nothing to roll back to: a component that fails leaves the ones
    /// before it already reloaded, and the [`ReloadOutcome`] is how a caller
    /// learns which. That much is reported.
    ///
    /// Cancellation is not. Drop this future part-way — a `select!` against a
    /// shutdown signal is the usual way — and the configuration is half new
    /// with *no* outcome at all, because a dropped future returns nothing. A
    /// caller that then reports the cycle as complete is reporting something
    /// that did not happen.
    ///
    /// If a reload has to be interruptible, call it to completion and check the
    /// shutdown signal between cycles, or drive components one at a time with
    /// [`reload_one`] and stop where you choose.
    ///
    /// [`reload_one`]: Self::reload_one
    pub async fn reload_all(
        &self,
        state: &S,
    ) -> Result<ReloadOutcome> {
        let components = self.lifecycle_plan()?;
        #[cfg(feature = "config")]
        let reader = state.reload_config_reader();
        #[cfg(feature = "config")]
        if let Some(reader) = reader {
            let config_entries = self.configuration_entries()?;
            let owners: Vec<_> = config_entries
                .iter()
                .filter_map(|(_, component)| component.as_configurable())
                .collect();
            reader.prepare_owners(&owners).await?;
            // A reload re-reads program environments as it re-reads
            // configuration: the declarations from the prepared config, then
            // every file they name.
            for component in &components {
                let Some(environmental) = component.as_environmental() else {
                    continue;
                };
                let owner = (**component).type_id();
                reader.prepare_programs(
                    owner,
                    environmental.program_environments(),
                )?;
                reader.refresh_programs(owner)?;
            }
        }
        state.reload().await?;

        let mut outcome = ReloadOutcome::default();

        for component in components {
            let name = component.name();
            if let Some(reloadable) = component.as_reloadable() {
                #[cfg(feature = "config")]
                let prepared = reader.and_then(|reader| {
                    component
                        .as_configurable()
                        .and_then(|owner| reader.prepared_config(owner))
                });
                let result = async {
                    reloadable.reload(state).await?;
                    #[cfg(feature = "config")]
                    if let (Some(reader), Some(prepared)) = (reader, prepared) {
                        reader.confirm_applied(prepared).await?;
                    }
                    Ok::<(), Error>(())
                }
                .await;
                match result {
                    Ok(()) => outcome.reloaded_count += 1,
                    Err(error) => outcome.failures.push(ReloadFailure {
                        component: name,
                        error: error.into_reload(name),
                    }),
                }
            }
        }

        Ok(outcome)
    }
}

struct ComponentEntry<S> {
    type_id: TypeId,
    index: usize,
    component: Arc<dyn AnyComponent<S>>,
}

impl<S> Clone for ComponentEntry<S> {
    fn clone(&self) -> Self {
        Self {
            type_id: self.type_id,
            index: self.index,
            component: self.component.clone(),
        }
    }
}

/// Topologically orders components, choosing among the ones that are ready by
/// `priority_of`.
///
/// The priority is the tie-break, never an override: `before`/`after` are
/// constraints and a magic number cannot move a component past one. Which
/// priority is used is the caller's — boot asks for the boot one, run for the
/// run one — so both phases mean the same thing by `ComponentOrder`.
fn order_component_entries<S: 'static>(
    entries: Vec<ComponentEntry<S>>,
    priority_of: impl Fn(&Arc<dyn AnyComponent<S>>) -> u8,
) -> Result<Vec<ComponentEntry<S>>> {
    let len = entries.len();
    let positions: HashMap<TypeId, usize> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.type_id, idx))
        .collect();

    // A type named in `order()` that is not a registered component is a
    // target: a node with no work that completes when everything ordered
    // before it has, and releases everything ordered after it. Targets take
    // the indices after the components.
    let mut targets: HashMap<TypeId, usize> = HashMap::new();
    for entry in &entries {
        let order = entry.component.order();
        for target in order.before_types().iter().chain(order.after_types()) {
            if !positions.contains_key(target) && !targets.contains_key(target)
            {
                targets.insert(*target, len + targets.len());
            }
        }
    }
    let nodes = len + targets.len();
    let node_of = |type_id: &TypeId| {
        positions.get(type_id).or_else(|| targets.get(type_id)).copied()
    };

    let priorities: Vec<u8> = entries
        .iter()
        .map(|entry| priority_of(&entry.component))
        .chain(std::iter::repeat_n(priority::NORMAL, targets.len()))
        .collect();
    let mut outgoing: Vec<HashSet<usize>> =
        (0..nodes).map(|_| HashSet::new()).collect();
    let mut indegree = vec![0usize; nodes];

    let mut add_edge = |from: usize, to: usize| {
        if outgoing[from].insert(to) {
            indegree[to] += 1;
        }
    };

    for (idx, entry) in entries.iter().enumerate() {
        // A requirement is an edge from the required component.
        for dependency in entry.component.dependencies() {
            if let Some(&dependency_idx) = positions.get(&dependency) {
                add_edge(dependency_idx, idx);
            }
        }
        let order = entry.component.order();
        for target in order.before_types() {
            if let Some(target_idx) = node_of(target) {
                add_edge(idx, target_idx);
            }
        }
        for target in order.after_types() {
            if let Some(target_idx) = node_of(target) {
                add_edge(target_idx, idx);
            }
        }
    }

    let mut ready: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(idx, degree)| (*degree == 0).then_some(idx))
        .collect();
    let mut ordered = Vec::with_capacity(len);
    let mut visited = 0;

    while !ready.is_empty() {
        ready.sort_by_key(|idx| {
            (
                priorities[*idx],
                *idx,
                entries.get(*idx).map_or("", |entry| entry.component.name()),
            )
        });
        let idx = ready.remove(0);
        visited += 1;
        if idx < len {
            ordered.push(idx);
        }

        let next: Vec<_> = outgoing[idx].iter().copied().collect();
        for target in next {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }

    if visited != nodes {
        let blocked = indegree
            .iter()
            .enumerate()
            .filter(|(idx, degree)| **degree > 0 && *idx < len)
            .map(|(idx, _)| entries[idx].component.name())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::msg(format!(
            "component lifecycle order cycle detected: {blocked}"
        )));
    }

    Ok(ordered.into_iter().map(|idx| entries[idx].clone()).collect())
}

fn run_priority<S: 'static>(component: &Arc<dyn AnyComponent<S>>) -> u8 {
    component.run_priority().unwrap_or(priority::NORMAL)
}

fn lifecycle_priority<S: 'static>(component: &Arc<dyn AnyComponent<S>>) -> u8 {
    component
        .boot_priority()
        .or_else(|| {
            component
                .as_reloadable()
                .and_then(|reloadable| reloadable.priority())
        })
        .unwrap_or(priority::NORMAL)
}

impl<S: 'static> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "../tests/registry/unit.rs"]
mod tests;
