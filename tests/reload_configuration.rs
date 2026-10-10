#[path = "support/atomic_parser.rs"]
mod atomic_parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use congif::{ConfigReader, ConfigRegistration, Configurable};
use continuo::{Component, Registry, ReloadState, Reloadable, Result};

#[derive(Default)]
struct State {
    reader: ConfigReader,
    reloads: AtomicUsize,
}

#[async_trait]
impl ReloadState for State {
    fn reload_config_reader(&self) -> Option<&ConfigReader> {
        Some(&self.reader)
    }

    async fn reload(&self) -> Result<()> {
        self.reloads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Default)]
struct Settings {
    candidate: Arc<AtomicUsize>,
    applied: AtomicUsize,
    reject_preparation: AtomicBool,
    reject_application: AtomicBool,
}

#[async_trait]
impl Configurable for Settings {
    type Config = usize;

    fn source(&self) -> congif::ConfigFile {
        atomic_parser::source()
    }
    async fn configure(
        &self,
        _: &usize,
    ) -> congif::Result<()> {
        Ok(())
    }
    fn validate_config(
        &self,
        config: &usize,
    ) -> congif::Result<()> {
        if self.reject_preparation.load(Ordering::Relaxed) {
            return Err(congif::Error::validation(std::io::Error::other(
                "candidate rejected",
            )));
        }
        let _ = config;
        Ok(())
    }
}

#[async_trait]
impl Component<State> for Settings {
    type Deps = ();
    fn name(&self) -> &'static str {
        "settings"
    }

    fn as_configurable(&self) -> Option<&dyn ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<State>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<State> for Settings {
    async fn reload(
        &self,
        state: &State,
    ) -> Result<()> {
        if self.reject_application.load(Ordering::Relaxed) {
            return Err(continuo::Error::msg("application rejected"));
        }
        self.applied
            .store(*state.reader.snapshot::<Self>()?, Ordering::Relaxed);
        Ok(())
    }
}

// Exercise the reload transaction directly: no boot, runnable or native watcher.
#[tokio::test]
async fn broadcast_prepares_before_hooks_and_acknowledges_only_success() {
    let state = State::default();
    let registry = Registry::new();
    let settings = Arc::new(Settings::default());
    state
        .reader
        .register(atomic_parser::AtomicParser(settings.candidate.clone()))
        .unwrap();
    registry.register(settings.clone()).unwrap();
    settings.candidate.store(1, Ordering::Relaxed);
    assert!(registry.reload_all(&state).await.unwrap().is_complete());
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);
    assert!(
        state
            .reader
            .prepare_changed_owners(&[settings.as_ref()])
            .await
            .unwrap()
            .is_empty()
    );

    settings.candidate.store(2, Ordering::Relaxed);
    settings.reject_preparation.store(true, Ordering::Relaxed);
    assert!(registry.reload_all(&state).await.is_err());
    assert_eq!(state.reloads.load(Ordering::Relaxed), 1);
    assert_eq!(*state.reader.snapshot::<Settings>().unwrap(), 1);
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);

    settings.reject_preparation.store(false, Ordering::Relaxed);
    settings.reject_application.store(true, Ordering::Relaxed);
    assert_eq!(registry.reload_all(&state).await.unwrap().failed_count(), 1);
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);
    assert_eq!(
        state
            .reader
            .prepare_changed_owners(&[settings.as_ref()])
            .await
            .unwrap()
            .len(),
        1
    );

    settings.reject_application.store(false, Ordering::Relaxed);
    assert!(registry.reload_all(&state).await.unwrap().is_complete());
    assert_eq!(settings.applied.load(Ordering::Relaxed), 2);
    assert!(
        state
            .reader
            .prepare_changed_owners(&[settings.as_ref()])
            .await
            .unwrap()
            .is_empty()
    );
}
