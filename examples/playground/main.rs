//! Run: cargo run --example playground --features config-watch
//! Open http://127.0.0.1:3000/.

mod app;
use app::{Application, CounterService, HttpService, SignalService};

#[tokio::main]
async fn main() -> continuo::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| {
            continuo::Error::msg(format!(
                "could not initialize logging: {error}"
            ))
        })?;

    Application::new()?
        .with_services::<(CounterService, HttpService, SignalService)>()?
        .run()
        .await
}
