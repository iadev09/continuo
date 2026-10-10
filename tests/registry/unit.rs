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

struct DbComponent;
struct CacheComponent;
struct ApiComponent;
struct MetricsComponent;
struct FailsValidation;

#[async_trait]
impl Component<TestState> for DbComponent {
    type Deps = ();
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
impl Component<TestState> for CacheComponent {
    type Deps = (Arc<DbComponent>,);
    fn name(&self) -> &'static str {
        "cache"
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
impl Component<TestState> for ApiComponent {
    type Deps = (Arc<CacheComponent>,);
    fn name(&self) -> &'static str {
        "api"
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
impl Component<TestState> for MetricsComponent {
    type Deps = ();
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().before::<ApiComponent>()
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
impl Component<TestState> for FailsValidation {
    type Deps = ();
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
        .register(Arc::new(ApiComponent))
        .expect("api registration should succeed")
        .register(Arc::new(CacheComponent))
        .expect("cache registration should succeed")
        .register(Arc::new(DbComponent))
        .expect("database registration should succeed");

    let outcome = registry.validate_all(&state).expect("validation should run");
    assert!(outcome.is_valid());
    assert_eq!(outcome.validated_count(), 3);

    let seen = state.seen.lock().expect("test log poisoned").clone();
    assert_eq!(seen, vec!["db", "cache", "api"]);
}

#[test]
fn duplicate_component_registration_is_rejected_without_replacement() {
    let registry = Registry::<TestState>::new();
    let original = Arc::new(ApiComponent);

    registry
        .register(original.clone())
        .expect("first registration should succeed");
    let error = match registry.register(Arc::new(ApiComponent)) {
        Err(error) => error,
        Ok(_) => panic!("duplicate registration must be rejected"),
    };

    assert!(matches!(
        error,
        Error::DuplicateComponent { type_name }
            if type_name == std::any::type_name::<ApiComponent>()
    ));
    let resolved = registry
        .resolve::<ApiComponent>()
        .expect("original component should remain");
    assert!(Arc::ptr_eq(&resolved, &original));
}

#[test]
fn lifecycle_order_supports_before_edges_and_diagnostics() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();

    registry
        .register(Arc::new(ApiComponent))
        .expect("api registration should succeed")
        .register(Arc::new(MetricsComponent))
        .expect("metrics registration should succeed")
        .register(Arc::new(CacheComponent))
        .expect("cache registration should succeed")
        .register(Arc::new(DbComponent))
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
fn validate_all_retains_component_failures_and_continues() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();
    registry
        .register(Arc::new(FailsValidation))
        .expect("failing validator should register")
        .register(Arc::new(DbComponent))
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
        "component 'fails-validation' failed during validate: validation rejected"
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
impl Component<TestState> for ConsoleLogger {
    type Deps = ();
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
        _deps: (),
        state: &TestState,
    ) -> Result<()> {
        state.seen.lock().expect("test log poisoned").push("console-logger");
        Ok(())
    }
}

#[async_trait]
impl Component<TestState> for FileLogger {
    type Deps = ();
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
        _deps: (),
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
impl Component<TestState> for BootRecorder {
    type Deps = (Arc<BootDependency>,);
    fn name(&self) -> &'static str {
        "boot-recorder"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState, Self::Deps>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState, (Arc<BootDependency>,)> for BootRecorder {
    async fn boot(
        &self,
        (dependency,): (Arc<BootDependency>,),
        state: &TestState,
    ) -> Result<()> {
        // The dependency arrives resolved and already booted.
        assert_eq!(Component::name(dependency.as_ref()), "boot-dependency");
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
impl Component<TestState> for BootDependency {
    type Deps = ();
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
        _deps: (),
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

/// A layer: never registered, only named in `order()`.
struct Network;
struct Dns;
struct Edge;

impl Component<TestState> for Dns {
    type Deps = ();
    fn name(&self) -> &'static str {
        "dns"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().before::<Network>()
    }
}

impl Component<TestState> for Edge {
    type Deps = ();
    fn name(&self) -> &'static str {
        "edge"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<Network>()
    }
}

#[test]
fn targets_order_layers_without_naming_their_members() {
    let registry = Registry::<TestState>::new();
    registry
        .register(Arc::new(Edge))
        .expect("edge should register")
        .register(Arc::new(Dns))
        .expect("dns should register");

    let names = registry.lifecycle_names().expect("plan should build");
    assert_eq!(names, vec!["dns", "edge"]);
}

struct MarkerCycle;

impl Component<TestState> for MarkerCycle {
    type Deps = ();

    fn name(&self) -> &'static str {
        "marker-cycle"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().before::<Network>().after::<Network>()
    }
}

#[test]
fn marker_cycle_returns_a_named_error_instead_of_panicking() {
    let registry = Registry::<TestState>::new();
    registry.register(Arc::new(MarkerCycle)).unwrap();
    let error = registry.lifecycle_names().unwrap_err();
    assert!(error.to_string().contains("cycle detected: marker-cycle"));
    assert!(registry.runnable_entries().is_err());
}

struct Absent;
struct NeedsAbsent;

impl Component<TestState> for NeedsAbsent {
    type Deps = (Arc<Absent>,);
    fn name(&self) -> &'static str {
        "needs-absent"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<TestState, Self::Deps>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<TestState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<TestState, (Arc<Absent>,)> for NeedsAbsent {
    async fn boot(
        &self,
        _: (Arc<Absent>,),
        _: &TestState,
    ) -> Result<()> {
        unreachable!("boot must not run without its dependency")
    }
}

#[async_trait]
impl Runnable<TestState, (Arc<Absent>,)> for NeedsAbsent {
    async fn run(
        self: Arc<Self>,
        _: (Arc<Absent>,),
        _: TestState,
        _: crate::RunContext,
    ) -> Result<()> {
        unreachable!("run must not start without its dependency")
    }
}

#[tokio::test]
async fn missing_dependency_names_the_component_and_the_type() {
    let state = TestState::default();
    let registry = Registry::<TestState>::new();
    registry.register(Arc::new(NeedsAbsent)).expect("should register");

    let error = registry.boot_all(&state).await.expect_err("boot must fail");
    assert!(matches!(error, Error::Dependency { name: "needs-absent", .. }));
    assert!(error.to_string().contains("Absent"));

    let entries =
        registry.runnable_entries().expect("discovery needs no dependencies");
    let error = entries[0]
        .runnable
        .clone()
        .bind(&registry)
        .err()
        .expect("activation must fail");
    assert!(matches!(error, Error::Dependency { name: "needs-absent", .. }));

    registry.insert(Arc::new(Absent)).expect("dependency should insert");
    entries[0]
        .runnable
        .clone()
        .bind(&registry)
        .expect("published dependency is visible");
    assert_eq!(
        registry.runnable_entries().expect("resolved").len(),
        1,
        "once the dependency exists the runnable is submitted"
    );
}

struct CycleA;
struct CycleB;

#[async_trait]
impl Component<TestState> for CycleA {
    type Deps = ();
    fn name(&self) -> &'static str {
        "cycle-a"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<CycleB>()
    }
}

#[async_trait]
impl Component<TestState> for CycleB {
    type Deps = ();
    fn name(&self) -> &'static str {
        "cycle-b"
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<CycleA>()
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
        err.to_string().contains("component lifecycle order cycle detected")
    );
}

struct Reloads;
struct FailsReload;
struct Finalizes;
struct FailsFinalize;

#[async_trait]
impl Component<TestState> for Finalizes {
    type Deps = ();
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
impl Component<TestState> for FailsFinalize {
    type Deps = ();
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
impl Component<TestState> for Reloads {
    type Deps = ();
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
impl Component<TestState> for FailsReload {
    type Deps = ();
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
async fn reload_all_retains_component_failures_and_continues() {
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
async fn finalize_all_retains_component_failures_and_continues() {
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
