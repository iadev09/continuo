#[path = "../support/atomic_parser.rs"]
mod atomic_parser;
use super::apply_configured_reload;
use crate::{Component, Error, Reloadable, Result};
use async_trait::async_trait;
use congif::{ConfigReader, ConfigRegistration, Configurable};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Settings {
    source: Arc<AtomicUsize>,
    applied: AtomicUsize,
    observed: AtomicUsize,
    manual: AtomicUsize,
}

#[async_trait]
impl Configurable for Settings {
    type Config = usize;

    fn as_config_observer(
        &self
    ) -> Option<&dyn congif::ConfigObserver<Config = usize>> {
        Some(self)
    }

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
        if *config == 0 {
            return Err(congif::Error::validation(std::io::Error::other(
                "candidate rejected",
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl congif::ConfigObserver for Settings {
    async fn config_changed(
        &self,
        config: &usize,
    ) -> congif::Result<()> {
        self.observed.fetch_add(1, Ordering::Relaxed);
        self.applied.store(*config, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl Component<ConfigReader> for Settings {
    type Deps = ();
    fn name(&self) -> &'static str {
        "settings"
    }

    fn as_configurable(&self) -> Option<&dyn ConfigRegistration> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<ConfigReader> for Settings {
    async fn reload(
        &self,
        reader: &ConfigReader,
    ) -> Result<()> {
        self.manual.fetch_add(1, Ordering::Relaxed);
        let candidate = *reader.snapshot::<Self>()?;
        if candidate == 3 {
            return Err(Error::msg("application rejected"));
        }
        self.applied.store(candidate, Ordering::Relaxed);
        Ok(())
    }
}

// Test the command's transaction directly, without constructing a runtime.
#[tokio::test]
async fn targeted_reload_without_watcher_prepares_and_confirms() {
    let settings = Settings::default();
    let reader = ConfigReader::new();
    reader
        .register(atomic_parser::AtomicParser(settings.source.clone()))
        .unwrap();
    settings.source.store(1, Ordering::Relaxed);
    assert!(
        apply_configured_reload(None, &settings, &settings, &reader, false)
            .await
            .is_err()
    );
    assert_eq!(settings.applied.load(Ordering::Relaxed), 0);

    apply_configured_reload(
        Some(&reader),
        &settings,
        &settings,
        &reader,
        false,
    )
    .await
    .unwrap();
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);
    assert_eq!(settings.observed.load(Ordering::Relaxed), 0);
    assert_eq!(settings.manual.load(Ordering::Relaxed), 1);
    assert!(
        reader.prepare_changed_owners(&[&settings]).await.unwrap().is_empty()
    );

    settings.source.store(0, Ordering::Relaxed);
    assert!(
        apply_configured_reload(
            Some(&reader),
            &settings,
            &settings,
            &reader,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(*reader.snapshot::<Settings>().unwrap(), 1);
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);

    settings.source.store(3, Ordering::Relaxed);
    assert!(
        apply_configured_reload(
            Some(&reader),
            &settings,
            &settings,
            &reader,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(settings.applied.load(Ordering::Relaxed), 1);
    assert_eq!(
        reader.prepare_changed_owners(&[&settings]).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn source_prepared_reload_does_not_read_again() {
    let settings = Settings::default();
    let reader = ConfigReader::new();
    reader
        .register(atomic_parser::AtomicParser(settings.source.clone()))
        .unwrap();
    settings.source.store(2, Ordering::Relaxed);
    reader.initialize_owners(&[&settings]).await.unwrap();
    settings.source.store(0, Ordering::Relaxed);
    apply_configured_reload(Some(&reader), &settings, &settings, &reader, true)
        .await
        .unwrap();
    assert_eq!(settings.applied.load(Ordering::Relaxed), 2);
    assert_eq!(settings.observed.load(Ordering::Relaxed), 1);
    assert_eq!(settings.manual.load(Ordering::Relaxed), 0);
}
