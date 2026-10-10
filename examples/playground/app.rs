//! The playground application: state, services and handlers, composed in
//! `main` and run by Continuo.

mod config;
mod counter;
mod environment;
mod events;
mod handlers;
pub mod services;
mod state;

use continuo::{Register, Result, Runtime};
pub use environment::Environment;
use state::AppState;

/// Composition of the playground. Nothing happens until `run`.
#[derive(Default)]
pub struct Application {
    environment: Environment,
    runtime: Runtime<AppState>,
}

impl Application {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_environment(
        mut self,
        environment: Environment,
    ) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_services<R: Register<AppState>>(mut self) -> Self {
        self.runtime = self.runtime.with_services::<R>();
        self
    }

    /// Build the state from the selected environment, then hand the
    /// composition to Continuo: register, watch, prepare, run, finalize.
    pub async fn run(self) -> Result<()> {
        let state = AppState::new(self.environment.store()?)?;
        let finalized = self.runtime.serve(state).await?;
        for failure in finalized.failures() {
            eprintln!(
                "finalize failed for {}: {}",
                failure.component(),
                failure.error()
            );
        }
        Ok(())
    }
}
