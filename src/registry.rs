use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

mod error;
mod finalizable;
mod order;
pub mod priority;
mod provider;
mod register;
mod reload;
mod runnable;
mod validation;

pub use error::{BoxError, Error, Result, StdError};
pub use finalizable::{Finalizable, FinalizeFailure, FinalizeOutcome};
pub use order::ProviderOrder;
pub use provider::Provider;
pub use register::Register;
pub use reload::{ReloadFailure, ReloadOutcome, ReloadState, Reloadable};
pub use runnable::Runnable;
pub use validation::{ValidationFailure, ValidationOutcome};

/// A provider with the `TypeId` it is registered under.
type KeyedProvider<S> = (TypeId, Arc<dyn Provider<S>>);

/// Type-erased provider registry used for service discovery and DI-style lookup.
/// Registration happens during bootstrap and runtime access is read-only via typed resolves.
/// We keep the underlying maps behind `RwLock<HashMap<..>>` so registration stays simple while
/// lookup only holds a short-lived read lock long enough to clone the stored `Arc`.
pub struct Registry<S> {
    providers: RwLock<HashMap<TypeId, Arc<dyn Provider<S>>>>,
    by_type: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    dyn_by_type: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    registration_order: RwLock<Vec<TypeId>>,
    lifecycle_order: RwLock<Option<Vec<TypeId>>>,
}

pub(crate) struct RunnableEntry<S> {
    pub(crate) name: &'static str,
    pub(crate) provider: Arc<dyn Provider<S>>,
    pub(crate) runnable: Arc<dyn Runnable<S>>,
}

impl<S: 'static> Registry<S> {
    /// Create the service with an empty registry. You can register later.
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
            dyn_by_type: RwLock::new(HashMap::new()),
            registration_order: RwLock::new(Vec::new()),
            lifecycle_order: RwLock::new(None),
        }
    }

    /// Register a provider into the registry.
    ///
    /// This accepts `Arc<P>` where `P: Provider`. The service is stored as a
    /// type-erased `Arc<dyn Provider>` but continues to point to the same underlying
    /// allocation (no new allocation is created).
    ///
    /// If another service with the same concrete type is already registered,
    /// registration is rejected.
    ///
    /// # Resolving during registration
    ///
    /// Registration runs in insertion order, and nothing orders it — that is
    /// what `boot_all` and [`ProviderOrder`] are for. So a `register` function
    /// that resolves another provider is reading a registry that may or may not
    /// contain it yet, and the result is decided by the order two lines happen
    /// to appear in somebody's bootstrap.
    ///
    /// The rule is not "never resolve". It is: **do not resolve a provider you
    /// have not ensured.** Calling that provider's own idempotent `register`
    /// first and then resolving is fine and order-independent; so is resolving
    /// your own type to avoid a duplicate insert. What is not fine is
    /// `if let Some(x) = resolve::<X>() { … }` with no else, because the
    /// difference between "X is not part of this deployment" and "X has not
    /// been registered yet" is invisible, and the second one is silent.
    ///
    /// Anything that genuinely depends on another provider's *state* belongs in
    /// `boot`, where the ordering system has already run.
    ///
    /// Returns `&Self` to allow fallible fluent chaining:
    ///
    /// ```ignore
    /// registry
    ///     .insert(dns.clone())?
    ///     .insert(ipc.clone())?;
    /// ```
    pub fn insert<P>(
        &self,
        item: Arc<P>,
    ) -> Result<&Self>
    where
        P: Provider<S> + 'static,
    {
        let type_id = TypeId::of::<P>();
        let any: Arc<dyn Any + Send + Sync> = item.clone();
        let mut by_type =
            self.by_type.write().expect("registry by_type lock poisoned");
        if by_type.contains_key(&type_id) {
            return Err(Error::DuplicateProvider {
                type_name: std::any::type_name::<P>(),
            });
        }
        by_type.insert(type_id, any);
        drop(by_type);

        let it: Arc<dyn Provider<S>> = item;
        self.providers
            .write()
            .expect("registry providers lock poisoned")
            .insert(type_id, it);
        self.registration_order
            .write()
            .expect("registry order lock poisoned")
            .push(type_id);
        *self
            .lifecycle_order
            .write()
            .expect("registry lifecycle order lock poisoned") = None;
        Ok(self)
    }

    /// Bind a registered concrete provider as a trait-object capability.
    ///
    /// This does not register a second lifecycle provider. It resolves the
    /// existing concrete provider `P`, casts the same `Arc<P>` into `Arc<I>`,
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
        P: Provider<S> + 'static,
    {
        let concrete = self.resolve::<P>().ok_or_else(|| {
            Error::msg(format!(
                "bind_dyn: provider type '{}' is not registered",
                std::any::type_name::<P>()
            ))
        })?;

        let interface_id = TypeId::of::<I>();
        let mut dyn_by_type = self
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
        T: Provider<S> + 'static,
    {
        let typed = self.resolve::<T>()?;
        Some(f(typed.as_ref()))
    }

    /// Resolve a concrete service as an owned `Arc<T>` handle.
    ///
    /// This is the DI-style, high-level API: it returns a typed `Arc<T>` that
    /// points to the same underlying allocation as the internally registered
    /// provider (no new `Arc` allocation). The returned `Arc` is obtained by
    /// downcasting from a type-indexed map (`TypeId`).
    ///
    /// Returns `None` if the type is not registered.
    pub fn resolve<T>(&self) -> Option<Arc<T>>
    where
        T: Provider<S> + 'static,
    {
        let any = self
            .by_type
            .read()
            .expect("registry by_type lock poisoned")
            .get(&TypeId::of::<T>())?
            .clone();
        Arc::downcast::<T>(any).ok()
    }

    /// Resolve a registered provider's lifecycle capabilities by runtime type.
    pub fn resolve_provider(
        &self,
        type_id: TypeId,
    ) -> Option<Arc<dyn Provider<S>>> {
        self.providers
            .read()
            .expect("registry providers lock poisoned")
            .get(&type_id)
            .cloned()
    }

    /// Registered provider identities and shared lifecycle handles.
    pub fn provider_entries(&self) -> Vec<(TypeId, Arc<dyn Provider<S>>)> {
        self.provider_entries_snapshot()
            .into_iter()
            .map(|entry| (entry.type_id, entry.provider))
            .collect()
    }

    /// Resolve a trait-object capability bound with `bind_dyn`.
    pub fn resolve_dyn<I>(&self) -> Option<Arc<I>>
    where
        I: ?Sized + Send + Sync + 'static,
    {
        self.dyn_by_type
            .read()
            .expect("registry dyn_by_type lock poisoned")
            .get(&TypeId::of::<I>())?
            .downcast_ref::<Arc<I>>()
            .cloned()
    }

    /// Return a snapshot of registered providers.
    pub fn providers(&self) -> Vec<Arc<dyn Provider<S>>> {
        self.providers
            .read()
            .expect("registry providers lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn provider_entries_snapshot(&self) -> Vec<ProviderEntry<S>> {
        let providers =
            self.providers.read().expect("registry providers lock poisoned");
        self.registration_order
            .read()
            .expect("registry order lock poisoned")
            .iter()
            .enumerate()
            .filter_map(|(index, type_id)| {
                providers.get(type_id).cloned().map(|provider| ProviderEntry {
                    type_id: *type_id,
                    index,
                    provider,
                })
            })
            .collect()
    }

    /// Return the cached lifecycle plan, building it once if needed.
    ///
    /// The plan is invalidated on `insert()`. Normal lifecycle phases reuse
    /// the same known list, so reload is a boot emulation over the same
    /// provider order instead of a second ordering universe.
    fn lifecycle_plan(&self) -> Result<Vec<Arc<dyn Provider<S>>>> {
        if let Some(type_ids) = self
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .as_ref()
            .cloned()
        {
            return Ok(self.providers_from_type_ids(&type_ids));
        }

        let ordered = order_provider_entries(
            self.provider_entries_snapshot(),
            lifecycle_priority,
        )?;
        let type_ids =
            ordered.iter().map(|entry| entry.type_id).collect::<Vec<_>>();
        let providers = ordered
            .iter()
            .map(|entry| entry.provider.clone())
            .collect::<Vec<_>>();
        #[cfg(debug_assertions)]
        tracing::debug!(
            providers = ?providers.iter().map(|provider| provider.name()).collect::<Vec<_>>(),
            "provider lifecycle order"
        );
        *self
            .lifecycle_order
            .write()
            .expect("registry lifecycle order lock poisoned") = Some(type_ids);
        Ok(providers)
    }

    /// The lifecycle plan with each provider's `TypeId` alongside it, for the
    /// one caller that has to know which providers it has already handled.
    pub(crate) fn lifecycle_entries(&self) -> Result<Vec<KeyedProvider<S>>> {
        let plan = self.lifecycle_plan()?;
        let type_ids = self
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .clone()
            .unwrap_or_default();

        Ok(type_ids.into_iter().zip(plan).collect())
    }

    fn providers_from_type_ids(
        &self,
        type_ids: &[TypeId],
    ) -> Vec<Arc<dyn Provider<S>>> {
        let providers =
            self.providers.read().expect("registry providers lock poisoned");
        type_ids
            .iter()
            .filter_map(|type_id| providers.get(type_id).cloned())
            .collect()
    }

    /// Return the list of provider display names (for diagnostics only).
    pub fn list_names(&self) -> Vec<&'static str> {
        self.providers().iter().map(|c| c.name()).collect()
    }

    /// Return provider display names in lifecycle order.
    ///
    /// This is useful for diagnostics and startup logging before running
    /// `boot_all()`.
    pub fn lifecycle_names(&self) -> Result<Vec<&'static str>> {
        Ok(self
            .lifecycle_plan()?
            .iter()
            .map(|provider| provider.name())
            .collect())
    }

    /// Runnables in the order they should be started.
    ///
    /// Topological like boot, not a sort by magic number. This used to iterate
    /// `HashMap::values()` and sort by `(run_priority, name)`, so `ProviderOrder`
    /// meant something for boot and nothing for run, and a runnable that had to
    /// start after another had no way to say so. It is the same ordering pass
    /// boot uses, with `run_priority` choosing among the providers that are
    /// genuinely independent.
    pub(crate) fn runnable_entries(&self) -> Result<Vec<RunnableEntry<S>>> {
        let ordered = order_provider_entries(
            self.provider_entries_snapshot(),
            run_priority,
        )?;

        let mut runnables = Vec::new();
        for entry in ordered {
            let provider = entry.provider;
            let Some(runnable) = provider.clone().as_runnable() else {
                continue;
            };
            runnables.push(RunnableEntry {
                name: provider.name(),
                provider,
                runnable,
            });
        }

        Ok(runnables)
    }

    /// Run `validate` for every provider in lifecycle order.
    ///
    /// Lifecycle-plan failures remain outer errors. A provider validation
    /// failure does not prevent later providers from being checked; every
    /// provider failure is retained in the outcome.
    pub fn validate_all(
        &self,
        state: &S,
    ) -> Result<ValidationOutcome> {
        let mut outcome = ValidationOutcome::default();

        for provider in self.lifecycle_plan()? {
            let name = provider.name();
            match provider.validate(state) {
                Ok(()) => outcome.validated_count += 1,
                Err(error) => outcome.failures.push(ValidationFailure {
                    provider: name,
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

        // The plan is recomputed each pass rather than materialised once.
        // `insert` takes `&self`, so a provider's `boot` can register another
        // one — and a plan taken before the pass would never contain it. Such a
        // provider used to be validated and finalized but never booted, with
        // nothing said about it.
        //
        // A late arrival is booted on the next pass, which is after providers
        // that a fresh plan would have ordered behind it. That is the honest
        // limit of registering during boot, and it is still an order better
        // than never booting at all.
        loop {
            let mut progressed = false;

            for (type_id, provider) in self.lifecycle_entries()? {
                if !booted.insert(type_id) {
                    continue;
                }
                progressed = true;

                let name = provider.name();
                // Boot is a sequence of silent blocking steps, and when one
                // of them takes seconds the log shows a gap with nothing in
                // it — the next line to print gets the blame. Saying which
                // provider is being waited on, and what it cost, is what
                // turns that gap into an answer.
                tracing::debug!("⏳ booting {name}");
                let started = std::time::Instant::now();
                if let Err(e) = provider.boot(state).await {
                    return Err(e.into_boot(name));
                }
                tracing::debug!(
                    "✅ {name} booted in {} ms",
                    started.elapsed().as_millis()
                );
            }

            if !progressed {
                return Ok(());
            }
        }
    }

    /// Run `Finalizable::finalize` for every provider that exposes the
    /// capability, in reverse lifecycle order. Called by the bootstrap
    /// layer after the runnable tasks have drained. One failure does not stop
    /// later finalizers; every failure is retained in the outcome.
    pub async fn finalize_all(
        &self,
        state: &S,
    ) -> Result<FinalizeOutcome> {
        let mut providers = self.lifecycle_plan()?;
        providers.reverse();
        let mut outcome = FinalizeOutcome::default();

        for provider in providers {
            let Some(finalizable) = provider.as_finalizable() else { continue };
            let name = provider.name();
            match finalizable.finalize(state).await {
                Ok(()) => outcome.finalized_count += 1,
                Err(error) => outcome.failures.push(FinalizeFailure {
                    provider: name,
                    error: error.into_finalize(name),
                }),
            }
        }
        Ok(outcome)
    }

    pub async fn reload_one(
        &self,
        name: &str,
        state: &S,
    ) -> Result<()> {
        let Some(provider) = self
            .providers()
            .into_iter()
            .find(|provider| provider.name() == name)
        else {
            return Err(Error::msg(format!(
                "reload_by_name: no provider registered with name '{}'",
                name
            )));
        };

        let Some(reloadable) = provider.as_reloadable() else {
            return Err(Error::msg(format!(
                "reload_by_name: provider '{}' is not reloadable",
                name
            )));
        };

        let provider_name = provider.name();
        reloadable
            .reload(state)
            .await
            .map_err(|error| error.into_reload(provider_name))
    }
}

impl<S> Registry<S>
where
    S: ReloadState + 'static,
{
    /// Reload every reloadable provider, in lifecycle order.
    ///
    /// # Not atomic, and not cancel-safe
    ///
    /// Providers own their own state and the trait has no two-phase commit, so
    /// there is nothing to roll back to: a provider that fails leaves the ones
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
    /// shutdown signal between cycles, or drive providers one at a time with
    /// [`reload_one`] and stop where you choose.
    ///
    /// [`reload_one`]: Self::reload_one
    pub async fn reload_all(
        &self,
        state: &S,
    ) -> Result<ReloadOutcome> {
        let providers = self.lifecycle_plan()?;
        let reader = state.reload_config_reader();
        if let Some(reader) = reader {
            let owners: Vec<_> = providers
                .iter()
                .filter_map(|provider| provider.as_configurable())
                .collect();
            reader.prepare_owners(&owners).await?;
            // A reload re-reads program environments as it re-reads
            // configuration: the declarations from the prepared config, then
            // every file they name.
            for provider in &providers {
                let Some(environmental) = provider.as_environmental() else {
                    continue;
                };
                let owner = (**provider).type_id();
                reader.prepare_programs(
                    owner,
                    environmental.program_environments(),
                )?;
                reader.refresh_programs(owner)?;
            }
        }
        state.reload().await?;

        let mut outcome = ReloadOutcome::default();

        for provider in providers {
            let name = provider.name();
            if let Some(reloadable) = provider.as_reloadable() {
                let prepared = reader.and_then(|reader| {
                    provider
                        .as_configurable()
                        .and_then(|owner| reader.prepared_config(owner))
                });
                let result = async {
                    reloadable.reload(state).await?;
                    if let (Some(reader), Some(prepared)) = (reader, prepared) {
                        reader.confirm_applied(prepared).await?;
                    }
                    Ok::<(), Error>(())
                }
                .await;
                match result {
                    Ok(()) => outcome.reloaded_count += 1,
                    Err(error) => outcome.failures.push(ReloadFailure {
                        provider: name,
                        error: error.into_reload(name),
                    }),
                }
            }
        }

        Ok(outcome)
    }
}

struct ProviderEntry<S> {
    type_id: TypeId,
    index: usize,
    provider: Arc<dyn Provider<S>>,
}

impl<S> Clone for ProviderEntry<S> {
    fn clone(&self) -> Self {
        Self {
            type_id: self.type_id,
            index: self.index,
            provider: self.provider.clone(),
        }
    }
}

/// Topologically orders providers, choosing among the ones that are ready by
/// `priority_of`.
///
/// The priority is the tie-break, never an override: `before`/`after` are
/// constraints and a magic number cannot move a provider past one. Which
/// priority is used is the caller's — boot asks for the boot one, run for the
/// run one — so both phases mean the same thing by `ProviderOrder`.
fn order_provider_entries<S: 'static>(
    entries: Vec<ProviderEntry<S>>,
    priority_of: impl Fn(&Arc<dyn Provider<S>>) -> u8,
) -> Result<Vec<ProviderEntry<S>>> {
    let len = entries.len();
    let positions: HashMap<TypeId, usize> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.type_id, idx))
        .collect();
    let priorities: Vec<u8> =
        entries.iter().map(|entry| priority_of(&entry.provider)).collect();
    let mut outgoing: Vec<HashSet<usize>> =
        (0..len).map(|_| HashSet::new()).collect();
    let mut indegree = vec![0usize; len];

    let mut add_edge = |from: usize, to: usize| {
        if from != to && outgoing[from].insert(to) {
            indegree[to] += 1;
        }
    };

    for (idx, entry) in entries.iter().enumerate() {
        let order = entry.provider.order();
        for target in order.before_types() {
            if let Some(&target_idx) = positions.get(target) {
                add_edge(idx, target_idx);
            }
        }
        for target in order.after_types() {
            if let Some(&target_idx) = positions.get(target) {
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

    while !ready.is_empty() {
        ready.sort_by_key(|idx| {
            (
                priorities[*idx],
                entries[*idx].index,
                entries[*idx].provider.name(),
            )
        });
        let idx = ready.remove(0);
        ordered.push(idx);

        let next: Vec<_> = outgoing[idx].iter().copied().collect();
        for target in next {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }

    if ordered.len() != len {
        let blocked = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, degree)| {
                (*degree > 0).then_some(entries[idx].provider.name())
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::msg(format!(
            "provider lifecycle order cycle detected: {blocked}"
        )));
    }

    Ok(ordered.into_iter().map(|idx| entries[idx].clone()).collect())
}

fn run_priority<S: 'static>(provider: &Arc<dyn Provider<S>>) -> u8 {
    provider.run_priority().unwrap_or(priority::NORMAL)
}

fn lifecycle_priority<S: 'static>(provider: &Arc<dyn Provider<S>>) -> u8 {
    provider
        .boot_priority()
        .or_else(|| {
            provider
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
