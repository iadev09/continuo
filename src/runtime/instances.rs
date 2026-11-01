use super::*;
use crate::registry::RunnableEntry;

impl<S: SharedState> Runtime<S> {
    /// Refresh the named instances declared by prepared components.
    ///
    /// Declaration validation and new automatic admission precede mutation.
    /// Removed live instances retain their entry until their future drains.
    /// An active instance cannot be replaced by a different runnable allocation;
    /// stop it first or retain its handle and apply configuration through it.
    pub fn reconcile_instances(&mut self) -> Result<Vec<ServiceSnapshot>> {
        let state =
            self.state.as_ref().cloned().ok_or_else(|| {
                Error::msg("service runtime is not initialized")
            })?;
        if self.shutting_down || state.is_shutting_down() {
            return Err(Error::msg("service runtime is shutting down"));
        }
        let registry = self
            .registry
            .as_ref()
            .ok_or_else(|| Error::msg("service registry is not initialized"))?
            .shared();
        let declarations = registry.runnable_entries()?;
        #[cfg(feature = "fleet")]
        let declarations: Vec<_> = declarations
            .into_iter()
            .filter(|entry| {
                let scope = entry
                    .component
                    .as_scoped()
                    .map(crate::fleet::Scoped::scope)
                    .or_else(|| {
                        entry
                            .component
                            .as_dispatched()
                            .map(|placement| placement.scope())
                    })
                    .unwrap_or_default();
                scope.includes(state.process_scope())
            })
            .collect();

        let mut names = HashSet::new();
        for entry in &declarations {
            if !names.insert(entry.name.clone()) {
                return Err(Error::msg(format!(
                    "duplicate runnable service name '{}'",
                    entry.name
                )));
            }
            if let Some(current) = self.services.get(&entry.name)
                && (!Arc::ptr_eq(&current.identity, &entry.identity)
                    || !Arc::ptr_eq(&current.component, &entry.component))
                && matches!(
                    current.status,
                    ServiceStatus::Running | ServiceStatus::Stopping
                )
            {
                return Err(Error::msg(format!(
                    "stop runnable service '{}' before replacing its instance",
                    entry.name
                )));
            }
        }

        // Resolve every newly automatic instance before starting any of them.
        let mut admitted = HashMap::new();
        for entry in &declarations {
            if !self.services.contains_key(&entry.name)
                && entry.runnable.start_policy(&state)
                    == ServiceStartPolicy::Automatic
            {
                admitted.insert(
                    entry.name.clone(),
                    self.admit_instance(entry, &registry, &state)?,
                );
            }
        }

        let removed: Vec<_> = self
            .services
            .keys()
            .filter(|name| !names.contains(*name))
            .cloned()
            .collect();
        for name in removed {
            let current = self.services.get_mut(&name).unwrap();
            current.retiring = true;
            current.start_after_drain = false;
            match current.status {
                ServiceStatus::Running => {
                    current.status = ServiceStatus::Stopping;
                    current.cancellation.as_ref().unwrap().cancel();
                }
                ServiceStatus::Stopping => {}
                ServiceStatus::Stopped | ServiceStatus::Failed => {
                    self.services.remove(&name);
                }
            }
        }

        let mut updates = Vec::new();
        for entry in declarations {
            let name = entry.name.clone();
            updates.push(name.clone());
            let policy = entry.runnable.start_policy(&state);
            if let Some(current) = self.services.get_mut(&name) {
                if current.retiring {
                    // Cancellation cannot be undone. Admit the restored
                    // declaration only after the old generation drains.
                    current.start_after_drain =
                        policy == ServiceStartPolicy::Automatic;
                }
                current.retiring = false;
                current.component = entry.component;
                current.runnable = entry.runnable;
                current.identity = entry.identity;
                continue;
            }
            self.services.insert(
                name.clone(),
                ServiceEntry {
                    #[cfg(feature = "fleet")]
                    dispatch: entry
                        .component
                        .as_dispatched()
                        .map(|placement| placement.dispatch())
                        .unwrap_or_default(),
                    component: entry.component,
                    runnable: entry.runnable,
                    identity: entry.identity,
                    retiring: false,
                    status: ServiceStatus::Stopped,
                    active_executions: Arc::new(AtomicU64::new(0)),
                    generation: 0,
                    cancellation: None,
                    applied_start_policy: policy,
                    start_after_drain: false,
                    last_error: None,
                    reload_revision: 0,
                    last_reload_error: None,
                    stop_waiters: Vec::new(),
                },
            );
        }
        // Preserve graph/priority order across both new and retained instances.
        for name in updates {
            if let Some(generation) = admitted.remove(&name) {
                self.start_admitted_generation(&name, false, Some(generation))?;
            } else {
                self.reconcile_start_policy(&name)?;
            }
        }
        Ok(self.snapshots())
    }

    pub(super) fn admit_instance(
        &self,
        entry: &RunnableEntry<S>,
        registry: &Registry<S>,
        state: &S,
    ) -> Result<AdmittedGeneration<S>> {
        #[cfg(feature = "fleet")]
        if let Some(placement) = entry.component.as_dispatched()
            && placement.dispatch().is_controller_driven()
        {
            let dispatcher = self.dispatcher.as_ref().ok_or_else(|| {
                Error::msg(format!(
                    "service '{}' requires a fleet dispatcher",
                    entry.name
                ))
            })?;
            dispatcher.validate(
                &crate::fleet::DispatchRequest {
                    name: entry.name.clone(),
                    generation: 1,
                    scope: placement.scope(),
                    dispatch: placement.dispatch(),
                    policy: placement.dispatch_policy(),
                },
                state,
            )?;
        }
        let runnable = entry.runnable.clone().bind(registry)?;
        let execution = ExecutionPlan::prepare(
            entry.runnable.execution_spec(state),
            self.execution_driver.as_deref(),
        )
        .map_err(|error| error.into_run(entry.name.clone()))?;
        Ok(AdmittedGeneration { runnable, execution })
    }
}
