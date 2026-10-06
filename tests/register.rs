use std::sync::{Arc, Mutex};

use continuo::{
    Error, HasRegistry, Provider, Register, Registry, Result, Runtime,
    ServiceManager,
};

type State = Mutex<Vec<u8>>;
struct First;
struct Second;
struct Rejected;

impl Register<State> for First {
    fn register(state: &State) -> Result<()> {
        state.lock().unwrap().push(1);
        Ok(())
    }
}

impl Register<State> for Second {
    fn register(state: &State) -> Result<()> {
        state.lock().unwrap().push(2);
        Ok(())
    }
}

impl Register<State> for Rejected {
    fn register(_: &State) -> Result<()> {
        Err(Error::msg("registration rejected"))
    }
}

#[test]
fn nested_registration_preserves_order() {
    let state = State::default();
    <(First, (Second, First))>::register(&state).unwrap();
    assert_eq!(*state.lock().unwrap(), [1, 2, 1]);
}

#[test]
fn registration_stops_at_first_error() {
    let state = State::default();
    assert!(<(First, Rejected, Second)>::register(&state).is_err());
    assert_eq!(*state.lock().unwrap(), [1]);
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry<Self>>,
    capacity: usize,
    shutdown: tokio_util::sync::CancellationToken,
    #[cfg(feature = "config-watch")]
    events: continuo::ProcessEventBus,
    #[cfg(feature = "config-watch")]
    reader: Arc<congif::ConfigReader>,
}

impl continuo::SharedState for AppState {
    fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }
}

impl HasRegistry for AppState {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.registry
    }
}

#[cfg(feature = "config-watch")]
impl continuo::HasEvents for AppState {
    fn events(&self) -> &continuo::ProcessEventBus {
        &self.events
    }
}

#[cfg(feature = "config-watch")]
impl continuo::HasConfigReader for AppState {
    fn config_reader(&self) -> &congif::ConfigReader {
        &self.reader
    }
}

#[test]
fn runtime_registers_its_manager_and_service_types_without_boot() {
    let state = AppState {
        registry: Arc::new(Registry::default()),
        capacity: 7,
        shutdown: tokio_util::sync::CancellationToken::new(),
        #[cfg(feature = "config-watch")]
        events: continuo::ProcessEventBus::new(),
        #[cfg(feature = "config-watch")]
        reader: Arc::new(congif::ConfigReader::new()),
    };
    let runtime =
        Runtime::default().with_services::<(CacheService,)>(&state).unwrap();
    assert_eq!(state.registry.resolve::<CacheService>().unwrap().capacity, 7);
    let manager = state.registry.resolve::<ServiceManager>().unwrap();
    assert!(Arc::ptr_eq(&manager, &runtime.manager()));
    let runtime = runtime.with_services::<()>(&state).unwrap();
    assert!(Arc::ptr_eq(&manager, &runtime.manager()));
    assert!(Runtime::default().with_services::<()>(&state).is_err());
    assert!(Arc::ptr_eq(
        &manager,
        &state.registry.resolve::<ServiceManager>().unwrap()
    ));
}

// No Default: registration supplies this required constructor value.
struct CacheService {
    capacity: usize,
}

#[async_trait::async_trait]
impl Provider<AppState> for CacheService {}

impl Register<AppState> for CacheService {
    fn register(state: &AppState) -> Result<()> {
        state.registry.insert(Arc::new(Self { capacity: state.capacity }))?;
        Ok(())
    }
}

#[test]
fn registration_constructs_and_resolves_one_shared_instance_without_boot() {
    let state = AppState {
        registry: Arc::new(Registry::default()),
        capacity: 7,
        shutdown: tokio_util::sync::CancellationToken::new(),
        #[cfg(feature = "config-watch")]
        events: continuo::ProcessEventBus::new(),
        #[cfg(feature = "config-watch")]
        reader: Arc::new(congif::ConfigReader::new()),
    };
    <(CacheService,)>::register(&state).unwrap();
    let first = state.registry.resolve::<CacheService>().unwrap();
    let second = state.registry.resolve::<CacheService>().unwrap();
    assert_eq!(first.capacity, state.capacity);
    assert!(Arc::ptr_eq(&first, &second));
    assert!(CacheService::register(&state).is_err());
    assert!(Arc::ptr_eq(
        &first,
        &state.registry.resolve::<CacheService>().unwrap()
    ));
}

#[test]
fn empty_registration_is_a_noop() {
    let state = State::default();
    <()>::register(&state).unwrap();
    assert!(state.lock().unwrap().is_empty());
}
