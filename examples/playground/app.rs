//! Application composition and lifecycle; the executable only launches it.

mod config;
mod handlers;
mod providers;
mod state;

use continuo::{Error, HasRegistry, Register, Result, Runtime};
pub use providers::{CounterService, HttpService, SignalService};
use state::AppState;

pub struct Application {
    state: AppState,
    runtime: Runtime<AppState>,
}

impl Application {
    pub fn new() -> Result<Self> {
        let state = AppState::new()?;
        let runtime = Runtime::<AppState>::default();
        Ok(Self { state, runtime })
    }

    pub fn with_services<R: Register<AppState>>(mut self) -> Result<Self> {
        self.runtime = self.runtime.with_services::<R>(&self.state)?;
        Ok(self)
    }

    pub async fn run(self) -> Result<()> {
        let Self { state, mut runtime } = self;
        let registry = state.registry_ref();
        registry.boot_all(&state).await?;
        let validation = registry.validate_all(&state)?;
        for failure in validation.failures() {
            eprintln!(
                "validation failed for {}: {}",
                failure.provider(),
                failure.error()
            );
        }
        if !validation.is_valid() {
            return Err(Error::msg(format!(
                "{} provider validation(s) failed",
                validation.failed_count()
            )));
        }

        runtime.spawn_all(registry, state.clone())?;
        let runtime_result = runtime.wait_until_shutdown(&state).await;
        runtime.drain().await?;
        runtime_result?;

        let finalized = registry.finalize_all(&state).await?;
        for failure in finalized.failures() {
            eprintln!(
                "finalize failed for {}: {}",
                failure.provider(),
                failure.error()
            );
        }

        println!("all services finished gracefully");
        Ok(())
    }
}
