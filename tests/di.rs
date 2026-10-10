use std::sync::Arc;

use async_trait::async_trait;
use continuo::Registry;
use continuo::{Bootable, Component, ComponentOrder, Finalizable, Result};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Settings {
    value: usize,
}

trait Value: Send + Sync {
    fn value(&self) -> usize;
}
impl Value for Settings {
    fn value(&self) -> usize {
        self.value
    }
}

#[test]
fn ordinary_values_and_interfaces_need_no_lifecycle_traits() {
    let registry = Registry::<()>::new();
    let settings = Arc::new(Settings { value: 7 });
    registry.insert(settings.clone()).unwrap();
    assert!(Arc::ptr_eq(&settings, &registry.resolve::<Settings>().unwrap()));
    assert_eq!(
        registry.with_typed::<Settings, _>(|value| value.value),
        Some(7)
    );
    registry.bind_dyn::<dyn Value, Settings>(|value| value).unwrap();
    assert_eq!(registry.resolve_dyn::<dyn Value>().unwrap().value(), 7);
    assert!(registry.components().is_empty());
    assert!(registry.insert(Arc::new(Settings { value: 9 })).is_err());
    assert!(Arc::ptr_eq(&settings, &registry.resolve::<Settings>().unwrap()));
}

#[derive(Default)]
struct PreparationState {
    registry: Registry<Self>,
    steps: Mutex<Vec<&'static str>>,
}

// The injected service has no lifecycle traits or deferred initialization.
struct PreparedService {
    value: usize,
}
struct PreparedDependency;
struct Constructor {
    fail: bool,
}

#[async_trait]
impl Component<PreparationState> for Constructor {
    fn name(&self) -> &'static str {
        "constructor"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<PreparationState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<PreparationState> for Constructor {
    async fn boot(
        &self,
        state: &PreparationState,
    ) -> Result<()> {
        if self.fail {
            return Err(continuo::Error::msg("construction rejected"));
        }
        state.steps.lock().unwrap().push("construct");
        state.registry.insert(Arc::new(PreparedService { value: 7 }))?;
        state.registry.insert(Arc::new(PreparedDependency))?;
        state.registry.register_lifecycle(Arc::new(LateDependency))?;
        Ok(())
    }
}

struct LateDependency;
#[async_trait]
impl Component<PreparationState> for LateDependency {
    fn name(&self) -> &'static str {
        "late-dependency"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<PreparationState>> {
        Some(self)
    }
}
#[async_trait]
impl Bootable<PreparationState> for LateDependency {
    async fn boot(
        &self,
        state: &PreparationState,
    ) -> Result<()> {
        state.steps.lock().unwrap().push("dependency");
        Ok(())
    }
}
struct Consumer;
#[async_trait]
impl Component<PreparationState> for Consumer {
    fn name(&self) -> &'static str {
        "consumer"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<Constructor>().after::<LateDependency>()
    }
    fn as_bootable(&self) -> Option<&dyn Bootable<PreparationState>> {
        Some(self)
    }
}
#[async_trait]
impl Bootable<PreparationState> for Consumer {
    async fn boot(
        &self,
        state: &PreparationState,
    ) -> Result<()> {
        assert_eq!(
            state.registry.resolve::<PreparedService>().unwrap().value,
            7
        );
        state.steps.lock().unwrap().push("consumer");
        Ok(())
    }
}

#[tokio::test]
async fn construction_publishes_complete_di_values_and_replans_dependencies() {
    let state = PreparationState::default();
    state.registry.register_lifecycle(Arc::new(Consumer)).unwrap();
    state
        .registry
        .register_lifecycle(Arc::new(Constructor { fail: false }))
        .unwrap();
    assert!(state.registry.resolve::<PreparedService>().is_none());
    state.registry.boot_all(&state).await.unwrap();
    assert_eq!(
        *state.steps.lock().unwrap(),
        ["construct", "dependency", "consumer"]
    );
    assert!(state.registry.resolve::<PreparedDependency>().is_some());
    assert!(state.registry.resolve::<Constructor>().is_none());
}

#[tokio::test]
async fn rejected_construction_publishes_no_empty_service() {
    let state = PreparationState::default();
    state
        .registry
        .register_lifecycle(Arc::new(Constructor { fail: true }))
        .unwrap();
    assert!(state.registry.boot_all(&state).await.is_err());
    assert!(state.registry.resolve::<PreparedService>().is_none());
    assert!(state.registry.resolve::<PreparedDependency>().is_none());
}

// Neither the resource nor its lifecycle wrapper implements Runnable.
struct Resource(AtomicBool);
#[async_trait]
impl Finalizable<()> for Resource {
    async fn finalize(
        &self,
        _: &(),
    ) -> Result<()> {
        self.0.store(true, Ordering::Relaxed);
        Ok(())
    }
}
struct Cleanup(Arc<Resource>);
#[async_trait]
impl Component<()> for Cleanup {
    fn name(&self) -> &'static str {
        "cleanup"
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<()>> {
        Some(self.0.as_ref())
    }
}

#[tokio::test]
async fn finalization_needs_neither_a_runnable_nor_di() {
    let registry = Registry::new();
    let resource = Arc::new(Resource(AtomicBool::new(false)));
    registry.register_lifecycle(Arc::new(Cleanup(resource.clone()))).unwrap();
    assert!(registry.resolve::<Cleanup>().is_none());
    assert!(registry.resolve::<Resource>().is_none());
    assert!(
        registry
            .components()
            .iter()
            .all(|entry| entry.clone().as_runnable().is_none())
    );
    let outcome = registry.finalize_all(&()).await.unwrap();
    assert!(outcome.is_complete());
    assert_eq!(outcome.finalized_count(), 1);
    assert!(resource.0.load(Ordering::Relaxed));
}
