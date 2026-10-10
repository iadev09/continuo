//! Run: cargo run --example playground
//! Select an environment: cargo run --example playground -- --environment local
//! The active listening address is printed at startup.

mod app;
use app::{
    Application, CounterService, HttpService, OtherService, ReloadService,
    SignalService,
};

fn main() -> continuo::Result<()> {
    // SAFETY: this executable owns startup; no runtime threads exist yet.
    let environments = unsafe { app::environment()? };

    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(
        async {
            Application::new(environments)
                .await?
                .with_services::<(
                    CounterService,
                    OtherService,
                    HttpService,
                    ReloadService,
                    SignalService,
                )>()?
                .run()
                .await
        },
    )
}
