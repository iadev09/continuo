//! Run: cargo run --example playground
//! Select an environment: cargo run --example playground -- --environment local
//! The active listening address is printed at startup.

mod app;
use app::services::{
    CounterService, HttpService, OtherService, ReloadService, SignalService,
};
use app::{Application, Environment};

#[tokio::main]
async fn main() -> continuo::Result<()> {
    Application::new()
        .with_environment(Environment::from_args()?)
        .with_services::<(
            CounterService,
            OtherService,
            HttpService,
            ReloadService,
            SignalService,
        )>()
        .run()
        .await
}
