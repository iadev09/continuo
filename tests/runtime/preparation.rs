//! Exercise construction and configuration ownership, without running services.
use super::*;
use crate::{Bootable, Component, ComponentOrder, HasEvents, ProcessEventBus};
use async_trait::async_trait;
use congif::{
    ConfigParser, ConfigReader, ConfigRegistration, Configurable,
    HasConfigReader,
};
use std::any::TypeId;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct State(Arc<Inner>);
struct Inner {
    reader: ConfigReader,
    registry: Registry<State>,
    events: ProcessEventBus,
    shutdown: CancellationToken,
}
impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }
}
impl HasRegistry for State {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }
}
impl HasEvents for State {
    fn events(&self) -> &ProcessEventBus {
        &self.0.events
    }
}
impl congif::HasConfigReader for State {
    fn config_reader(&self) -> &ConfigReader {
        &self.0.reader
    }
}

struct Parser {
    candidate: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}
#[async_trait]
impl ConfigParser for Parser {
    fn extensions(&self) -> &[&str] {
        &["test"]
    }
    async fn parse(
        &self,
        _: congif::ConfigSource<'_>,
    ) -> std::result::Result<congif::ConfigValue, congif::BoxError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(congif::ConfigValue::U64(
            self.candidate.load(Ordering::Relaxed) as u64
        ))
    }
}

#[derive(Clone)]
struct Preparation {
    configured: Arc<AtomicUsize>,
    value: Arc<AtomicUsize>,
}
#[async_trait]
impl Configurable for Preparation {
    type Config = usize;
    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/counter/valid.json"),
            format: congif::Format::Custom("test".into()),
        }
    }
    async fn configure(
        &self,
        _: &usize,
    ) -> congif::Result<()> {
        self.configured.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn as_config_observer(
        &self
    ) -> Option<&dyn congif::ConfigObserver<Config = usize>> {
        Some(self)
    }
}
#[async_trait]
impl congif::ConfigObserver for Preparation {
    async fn config_changed(
        &self,
        value: &usize,
    ) -> congif::Result<()> {
        self.value.store(*value, Ordering::Relaxed);
        Ok(())
    }
}
#[async_trait]
impl Component<State> for Preparation {
    fn name(&self) -> &'static str {
        "preparation"
    }

    fn as_configurable(&self) -> Option<&dyn ConfigRegistration> {
        Some(self)
    }
    fn as_bootable(&self) -> Option<&dyn Bootable<State>> {
        Some(self)
    }
}
#[async_trait]
impl Bootable<State> for Preparation {
    async fn boot(
        &self,
        state: &State,
    ) -> Result<()> {
        let config = state.config_reader().snapshot::<Self>()?;
        self.value.store(*config, Ordering::Relaxed);
        state.registry_ref().register(Arc::new(Ready(self.clone())))?;
        Ok(())
    }
}
struct Ready(Preparation);
#[async_trait]
impl Component<State> for Ready {
    fn name(&self) -> &'static str {
        "ready"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<Preparation>()
    }
    fn as_configurable(&self) -> Option<&dyn ConfigRegistration> {
        Some(&self.0)
    }
    fn as_reloadable(&self) -> Option<&dyn crate::Reloadable<State>> {
        Some(self)
    }
}
#[async_trait]
impl crate::Reloadable<State> for Ready {
    async fn reload(
        &self,
        state: &State,
    ) -> Result<()> {
        let value = state.config_reader().snapshot::<Preparation>()?;
        self.0.value.store(*value, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn wrapper_and_ready_service_share_one_config_owner_and_reload_target() {
    let candidate = Arc::new(AtomicUsize::new(7));
    let reads = Arc::new(AtomicUsize::new(0));
    let configured = Arc::new(AtomicUsize::new(0));
    let value = Arc::new(AtomicUsize::new(0));
    let reader = ConfigReader::new();
    reader
        .register(Parser { candidate: candidate.clone(), reads: reads.clone() })
        .unwrap();
    let state = State(Arc::new(Inner {
        reader,
        registry: Registry::new(),
        events: ProcessEventBus::new(),
        shutdown: CancellationToken::new(),
    }));
    state
        .registry_ref()
        .register_lifecycle(Arc::new(Preparation {
            configured: configured.clone(),
            value: value.clone(),
        }))
        .unwrap();
    Runtime::default().prepare(&state).await.unwrap();
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert_eq!(configured.load(Ordering::Relaxed), 1);
    assert_eq!(value.load(Ordering::Relaxed), 7);
    assert!(state.registry_ref().resolve::<Preparation>().is_none());
    let ready = state.registry_ref().resolve::<Ready>().unwrap();
    let entries = state.registry_ref().configuration_entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, TypeId::of::<Preparation>());
    assert_eq!(entries[0].1.name(), "ready");

    candidate.store(9, Ordering::Relaxed);
    apply_configured_reload(
        Some(state.config_reader()),
        ready.as_ref(),
        ready.as_ref(),
        &state,
        false,
    )
    .await
    .unwrap();
    assert_eq!(value.load(Ordering::Relaxed), 9);
    assert_eq!(reads.load(Ordering::Relaxed), 2);

    // An invalidation addressed to the ready service selects its declaration,
    // not a nonexistent Ready config snapshot or a DI-owned watcher.
    let mut watching = crate::watcher::ConfigurationWatch::new(&state).unwrap();
    candidate.store(11, Ordering::Relaxed);
    let changed = watching
        .prepare(&state, vec![crate::ConfigurationChanged::of::<Ready>()])
        .await
        .unwrap();
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].name(), "ready");
    apply_configured_reload(
        Some(state.config_reader()),
        changed[0].as_ref(),
        ready.as_ref(),
        &state,
        true,
    )
    .await
    .unwrap();
    assert_eq!(value.load(Ordering::Relaxed), 11);
    assert_eq!(
        reads.load(Ordering::Relaxed),
        3,
        "application must not parse the prepared candidate twice"
    );
    assert_eq!(configured.load(Ordering::Relaxed), 1);
}
