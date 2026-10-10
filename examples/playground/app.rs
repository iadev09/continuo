//! Application composition and lifecycle; the executable only launches it.

mod config;
mod counter;
mod environment;
mod events;
mod handlers;
mod services;
mod state;

use continuo::{Error, HasRegistry, Register, Result, Runtime};
pub use environment::environment;
pub use services::{
    CounterService, HttpService, OtherService, ReloadService, SignalService,
};
use state::AppState;

pub struct Application {
    state: AppState,
    runtime: Runtime<AppState>,
}

impl Application {
    pub async fn new(environments: dotenw::EnvStore) -> Result<Self> {
        let state = AppState::new(environments).await?;
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
        runtime.watch_configurations(&state)?;
        runtime.prepare(&state).await?;
        runtime.spawn_all(registry, state.clone())?;

        // Read a required config value using dot notation.
        let increment = state
            .config::<usize>("counter.increment")?
            .ok_or_else(|| Error::msg("counter.increment is not configured"))?;

        println!("Counter hit increments by {increment}");

        let runtime_result = runtime.wait_until_shutdown(&state).await;
        runtime.drain().await?;
        runtime_result?;

        let finalized = registry.finalize_all(&state).await?;
        for failure in finalized.failures() {
            eprintln!(
                "finalize failed for {}: {}",
                failure.component(),
                failure.error()
            );
        }

        println!("all services finished gracefully");
        Ok(())
    }
}
