use std::sync::Mutex;

use async_trait::async_trait;

use super::*;

#[derive(Clone, Default)]
struct TestState {
    seen: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl ReloadState for TestState {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}

struct DbProvider;
struct CacheProvider;
struct ApiProvider;
struct MetricsProvider;
struct FailsValidation;

#[async_trait]
impl Provider<TestState> for DbProvider {
    fn name(&self) -> &'static str {
        "db"
    }

    fn validate(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("db");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for CacheProvider {
    fn name(&self) -> &'static str {
        "cache"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<DbProvider>()
    }

    fn validate(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("cache");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for ApiProvider {
    fn name(&self) -> &'static str {
        "api"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CacheProvider>()
    }

    fn validate(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("api");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for MetricsProvider {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().before::<ApiProvider>()
    }

    fn validate(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("metrics");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for FailsValidation {
    fn name(&self) -> &'static str {
        "fails-validation"
    }

    fn validate(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Err(Error::msg("validation rejected"))
    }
}

#[test]
fn lifecycle_order_uses_type_dependencies() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(ApiProvider))
        .expect("api registration should succeed")
        .register(Arc::new(CacheProvider))
        .expect("cache registration should succeed")
        .register(Arc::new(DbProvider))
        .expect("database registration should succeed");

    let outcome = registry.validate_all(&state).expect("validation should run");
    assert!(outcome.is_valid());
    assert_eq!(outcome.validated_count(), 3);

    let seen = state.seen.lock().expect("test log poisoned").clone();
    assert_eq!(seen, vec!["db", "cache", "api"]);
}

#[test]
fn duplicate_provider_registration_is_rejected_without_replacement() {
    let registry = Registry::<TestState>::new();
    let original = Arc::new(ApiProvider);

    registry
        .register(original.clone())
        .expect("first registration should succeed");
    let error = match registry.register(Arc::new(ApiProvider)) {
        Err(error) => error,
        Ok(_) => panic!("duplicate registration must be rejected"),
    };

    assert!(matches!(
        error,
        Error::DuplicateProvider { type_name }
            if type_name == std::any::type_name::<ApiProvider>()
    ));
    let resolved = registry
        .resolve::<ApiProvider>()
        .expect("original provider should remain");
    assert!(Arc::ptr_eq(&resolved, &original));
}

#[test]
fn lifecycle_order_supports_before_edges_and_diagnostics() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(ApiProvider))
        .expect("api registration should succeed")
        .register(Arc::new(MetricsProvider))
        .expect("metrics registration should succeed")
        .register(Arc::new(CacheProvider))
        .expect("cache registration should succeed")
        .register(Arc::new(DbProvider))
        .expect("database registration should succeed");

    let names = registry.lifecycle_names().expect("plan should build");
    assert_eq!(names, vec!["metrics", "db", "cache", "api"]);

    let outcome = registry.validate_all(&state).expect("validation should run");
    assert!(outcome.is_valid());
    assert_eq!(outcome.validated_count(), 4);

    let seen = state.seen.lock().expect("test log poisoned").clone();
    assert_eq!(seen, vec!["metrics", "db", "cache", "api"]);
}

#[test]
fn validate_all_retains_provider_failures_and_continues() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();
    registry
        .register(Arc::new(FailsValidation))
        .expect("failing validator should register")
        .register(Arc::new(DbProvider))
        .expect("successful validator should register");

    let outcome =
        registry.validate_all(&state).expect("validation should complete");

    assert!(!outcome.is_valid());
    assert_eq!(outcome.validated_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    let failure = &outcome.failures()[0];
    assert_eq!(failure.component(), "fails-validation");
    assert!(matches!(
        failure.error(),
        Error::Validate { name: "fails-validation", .. }
    ));
    assert_eq!(
        failure.error().to_string(),
        "provider 'fails-validation' failed during validate: validation rejected"
    );
    assert_eq!(
        state.seen.lock().expect("test log poisoned").as_slice(),
        &["db"]
    );
}

trait LogSink: Send + Sync {
    fn line(&self) -> &'static str;
}

struct ConsoleLogger;
struct FileLogger;

impl LogSink for ConsoleLogger {
    fn line(&self) -> &'static str {
        "console"
    }
}

impl LogSink for FileLogger {
    fn line(&self) -> &'static str {
        "file"
    }
}

#[async_trait]
impl Provider<TestState> for ConsoleLogger {
    fn name(&self) -> &'static str {
        "console-logger"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState> for ConsoleLogger {
    async fn boot(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("console-logger");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for FileLogger {
    fn name(&self) -> &'static str {
        "file-logger"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState> for FileLogger {
    async fn boot(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("file-logger");
        Ok(())
    }
}

#[tokio::test]
async fn bind_dyn_resolves_trait_object_and_allows_replacement() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(ConsoleLogger))
        .expect("console logger should register");
    registry
        .bind_dyn::<dyn LogSink, ConsoleLogger>(|logger| logger)
        .expect("dyn binding should succeed");

    let sink =
        registry.resolve_dyn::<dyn LogSink>().expect("LogSink should be bound");
    assert_eq!(sink.line(), "console");

    registry
        .register(Arc::new(FileLogger))
        .expect("file logger should register");
    registry
        .bind_dyn::<dyn LogSink, FileLogger>(|logger| logger)
        .expect("dyn binding replacement should succeed");

    let sink = registry
        .resolve_dyn::<dyn LogSink>()
        .expect("LogSink should be rebound");
    assert_eq!(sink.line(), "file");

    registry.boot_all(&state).await.expect("boot should succeed");

    let seen = state.seen.lock().expect("test log poisoned").clone();
    assert_eq!(seen, vec!["console-logger", "file-logger"]);
}

struct BootRecorder;
struct BootDependency;

#[async_trait]
impl Provider<TestState> for BootRecorder {
    fn name(&self) -> &'static str {
        "boot-recorder"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<BootDependency>()
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState> for BootRecorder {
    async fn boot(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("boot-recorder");
        Ok(())
    }
}

#[async_trait]
impl Finalizable<TestState> for BootRecorder {
    async fn finalize(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("finalize-recorder");
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for BootDependency {
    fn name(&self) -> &'static str {
        "boot-dependency"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState> for BootDependency {
    async fn boot(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("boot-dependency");
        Ok(())
    }
}

#[async_trait]
impl Finalizable<TestState> for BootDependency {
    async fn finalize(
        &self,
        state: &TestState,
    ) -> Result<()> {
        state
            .seen
            .lock()
            .expect("test log poisoned")
            .push("finalize-dependency");
        Ok(())
    }
}

#[tokio::test]
async fn boot_and_finalize_share_lifecycle_plan() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(BootRecorder))
        .expect("boot recorder should register")
        .register(Arc::new(BootDependency))
        .expect("boot dependency should register");

    registry.boot_all(&state).await.expect("boot should succeed");
    let outcome =
        registry.finalize_all(&state).await.expect("finalize should succeed");
    assert!(outcome.is_complete());
    assert_eq!(outcome.finalized_count(), 2);

    let seen = state.seen.lock().expect("test log poisoned").clone();
    assert_eq!(
        seen,
        vec![
            "boot-dependency",
            "boot-recorder",
            "finalize-recorder",
            "finalize-dependency",
        ]
    );
}

struct CycleA;
struct CycleB;

#[async_trait]
impl Provider<TestState> for CycleA {
    fn name(&self) -> &'static str {
        "cycle-a"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CycleB>()
    }
}

#[async_trait]
impl Provider<TestState> for CycleB {
    fn name(&self) -> &'static str {
        "cycle-b"
    }

    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<CycleA>()
    }
}

#[test]
fn lifecycle_order_rejects_cycles() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(CycleA))
        .expect("cycle A should register")
        .register(Arc::new(CycleB))
        .expect("cycle B should register");

    let err =
        registry.validate_all(&state).expect_err("cycle must be rejected");
    assert!(
        err.to_string().contains("provider lifecycle order cycle detected")
    );
}

struct Reloads;
struct FailsReload;
struct Finalizes;
struct FailsFinalize;

#[async_trait]
impl Provider<TestState> for Finalizes {
    fn name(&self) -> &'static str {
        "finalizes"
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Finalizable<TestState> for Finalizes {
    async fn finalize(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for FailsFinalize {
    fn name(&self) -> &'static str {
        "fails-finalize"
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Finalizable<TestState> for FailsFinalize {
    async fn finalize(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Err(Error::msg("finalize rejected"))
    }
}

#[async_trait]
impl Provider<TestState> for Reloads {
    fn name(&self) -> &'static str {
        "reloads"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<TestState> for Reloads {
    async fn reload(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Provider<TestState> for FailsReload {
    fn name(&self) -> &'static str {
        "fails-reload"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<TestState> for FailsReload {
    async fn reload(
        &self,
        _: &TestState,
    ) -> Result<()> {
        Err(Error::msg("reload rejected"))
    }
}

#[tokio::test]
async fn reload_all_retains_provider_failures_and_continues() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();
    registry
        .register(Arc::new(FailsReload))
        .expect("failing reloader should register")
        .register(Arc::new(Reloads))
        .expect("successful reloader should register");

    let outcome =
        registry.reload_all(&state).await.expect("broadcast should complete");

    assert!(!outcome.is_complete());
    assert_eq!(outcome.reloaded_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    let failure = &outcome.failures()[0];
    assert_eq!(failure.component(), "fails-reload");
    assert!(matches!(
        failure.error(),
        Error::Reload { name: "fails-reload", .. }
    ));
    assert_eq!(
        failure.error().to_string(),
        "reload of 'fails-reload' failed: reload rejected"
    );
}

#[tokio::test]
async fn finalize_all_retains_provider_failures_and_continues() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();
    registry
        .register(Arc::new(Finalizes))
        .expect("successful finalizer should register")
        .register(Arc::new(FailsFinalize))
        .expect("failing finalizer should register");

    let outcome = registry
        .finalize_all(&state)
        .await
        .expect("finalization should complete");

    assert!(!outcome.is_complete());
    assert_eq!(outcome.finalized_count(), 1);
    assert_eq!(outcome.failed_count(), 1);
    let failure = &outcome.failures()[0];
    assert_eq!(failure.component(), "fails-finalize");
    assert!(matches!(
        failure.error(),
        Error::Finalize { name: "fails-finalize", .. }
    ));
    assert_eq!(
        failure.error().to_string(),
        "finalize of 'fails-finalize' failed: finalize rejected"
    );
}
