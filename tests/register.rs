use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use continuo::{
    Error, HasRegistry, Register, Registry, Result, Runtime, ServiceManager,
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

#[test]
fn runtime_registers_its_manager_and_service_types_without_boot() {
    let state = AppState {
        registry: Arc::new(Registry::default()),
        capacity: 7,
        shutdown: tokio_util::sync::CancellationToken::new(),
    };
    let mut runtime = Runtime::default().with_services::<(CacheService,)>();
    // Recording registers nothing.
    assert!(state.registry.resolve::<CacheService>().is_none());
    runtime.register_services(&state).unwrap();
    assert_eq!(state.registry.resolve::<CacheService>().unwrap().capacity, 7);
    let manager = state.registry.resolve::<ServiceManager>().unwrap();
    assert!(Arc::ptr_eq(&manager, &runtime.manager()));
    // Applying again is a no-op; the manager stays.
    runtime.register_services(&state).unwrap();
    assert!(Arc::ptr_eq(&manager, &runtime.manager()));
    // Another runtime cannot take over the registry's manager.
    assert!(Runtime::default().register_services(&state).is_err());
    assert!(Arc::ptr_eq(
        &manager,
        &state.registry.resolve::<ServiceManager>().unwrap()
    ));
}

// No Default: registration supplies this required constructor value.
struct CacheService {
    capacity: usize,
}

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

struct RegistrationLog {
    fail: AtomicBool,
    seen: Mutex<Vec<u8>>,
}

impl Register<AppState> for First {
    fn register(state: &AppState) -> Result<()> {
        state
            .registry
            .resolve::<RegistrationLog>()
            .unwrap()
            .seen
            .lock()
            .unwrap()
            .push(1);
        Ok(())
    }
}

impl Register<AppState> for Second {
    fn register(state: &AppState) -> Result<()> {
        state
            .registry
            .resolve::<RegistrationLog>()
            .unwrap()
            .seen
            .lock()
            .unwrap()
            .push(2);
        Ok(())
    }
}

impl Register<AppState> for Rejected {
    fn register(state: &AppState) -> Result<()> {
        let log = state.registry.resolve::<RegistrationLog>().unwrap();
        if log.fail.load(Ordering::SeqCst) {
            return Err(Error::msg("registration rejected"));
        }
        log.seen.lock().unwrap().push(3);
        Ok(())
    }
}

#[test]
fn runtime_keeps_failed_and_pending_registrations_for_retry() {
    let state = AppState {
        registry: Arc::new(Registry::new()),
        capacity: 7,
        shutdown: tokio_util::sync::CancellationToken::new(),
    };
    let log = Arc::new(RegistrationLog {
        fail: AtomicBool::new(true),
        seen: Mutex::new(Vec::new()),
    });
    state.registry.insert(log.clone()).unwrap();
    let mut runtime = Runtime::default()
        .with_services::<First>()
        .with_services::<Rejected>()
        .with_services::<Second>();
    assert!(runtime.register_services(&state).is_err());
    assert_eq!(*log.seen.lock().unwrap(), [1]);
    assert!(runtime.register_services(&state).is_err());
    assert_eq!(*log.seen.lock().unwrap(), [1]);
    log.fail.store(false, Ordering::SeqCst);
    runtime.register_services(&state).unwrap();
    assert_eq!(*log.seen.lock().unwrap(), [1, 3, 2]);
}
