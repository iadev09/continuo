//! Run: cargo run --example instances
//! Add or remove names in config.json while the process is running.

mod jobs;
mod state;

use async_trait::async_trait;
use continuo::{
    Component, HasRegistry, Result, RunContext, Runnable, Runtime, SharedState,
};
use jobs::{Jobs, Output};
use state::State;
use std::path::PathBuf;
use std::sync::Arc;

struct Signals;

impl Component<State> for Signals {
    type Deps = ();

    fn name(&self) -> &str {
        "signals"
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<State>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<State> for Signals {
    async fn run(
        self: Arc<Self>,
        _: (),
        state: State,
        context: RunContext,
    ) -> Result<()> {
        tokio::select! {
            _ = context.cancelled() => {}
            signal = tokio::signal::ctrl_c() => {
                signal?;
                state.initiate_shutdown();
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let source =
        std::env::args_os().nth(1).map(PathBuf::from).unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("examples/instances/config.json")
        });
    let state = State::new()?;
    state.registry_ref().insert(Arc::new(Output))?;
    state.registry_ref().register(Arc::new(Jobs::new(source)))?;
    state.registry_ref().register(Arc::new(Signals))?;
    let finalized = Runtime::default().serve(state).await?;
    for failure in finalized.failures() {
        eprintln!("{}: {}", failure.component(), failure.error());
    }
    Ok(())
}
