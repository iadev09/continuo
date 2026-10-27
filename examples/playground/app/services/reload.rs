use super::super::state::AppState;
use async_trait::async_trait;
use continuo::{
    Component, Error, HasEvents, HasRegistry, Register, ReloadRequired, Result,
    RunContext, Runnable, ServiceManager,
};
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct ReloadService;

impl ReloadService {
    async fn reload_application(
        &self,
        manager: &ServiceManager,
        state: &AppState,
    ) -> Result<()> {
        let registry = state.registry_ref();
        let services = manager
            .list()
            .await
            .map_err(|error| Error::msg(error.to_string()))?;
        let components = registry.components();
        let mut failed = 0;
        for name in registry.lifecycle_names()? {
            let component = components
                .iter()
                .find(|component| component.name() == name)
                .ok_or_else(|| {
                    Error::msg("registered component disappeared")
                })?;
            if component.as_reloadable().is_none() {
                continue;
            }
            let result = if services
                .iter()
                .any(|service| service.name() == name)
            {
                manager
                    .reload(name)
                    .await
                    .map(|_| ())
                    .map_err(|error| Error::msg(error.to_string()))
            } else {
                async {
                    if let Some(owner) = component.as_configurable() {
                        state.reader().prepare_changed_owners(&[owner]).await?;
                    }
                    let candidate =
                        component.as_configurable().and_then(|owner| {
                            state.reader().prepared_config(owner)
                        });
                    registry.reload_hook_one(name, state).await?;
                    if let Some(candidate) = candidate {
                        state.reader().confirm_applied(candidate).await?;
                    }
                    Ok(())
                }
                .await
            };
            match result {
                Ok(()) => tracing::info!(
                    component = name,
                    "application component reloaded"
                ),
                Err(error) => {
                    failed += 1;
                    tracing::error!(component = name, %error, "application component reload rejected");
                }
            }
        }
        if failed != 0 {
            return Err(Error::msg(format!(
                "{failed} component reload(s) failed"
            )));
        }
        Ok(())
    }
}

impl Register<AppState> for ReloadService {
    fn register(state: &AppState) -> Result<()> {
        state.registry_ref().register(Arc::new(Self))?;
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for ReloadService {
    // Reload is meaningless without the manager: a hard dependency.
    type Deps = (Arc<ServiceManager>,);

    fn name(&self) -> &'static str {
        "reload"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<ServiceManager>,)> for ReloadService {
    async fn run(
        self: Arc<Self>,
        (manager,): (Arc<ServiceManager>,),
        state: AppState,
        context: RunContext,
    ) -> Result<()> {
        let mut requests = state.events().subscribe::<ReloadRequired>();
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                request = requests.recv() => {
                    match request {
                        Ok(ReloadRequired) => {
                            match self.reload_application(&manager, &state).await {
                                Ok(()) => tracing::info!("application reloaded"),
                                Err(error) => tracing::error!(%error, "application reload failed"),
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "application reload requests lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
        }
    }
}
