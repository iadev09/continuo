use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

// =====================================================================
// Error model
// =====================================================================
//
// The error type is deliberately scoped to the `registry` module because
// it is a *registry* concern (lifecycle stages: boot / validate / reload
// / run). Other helper primitives in this crate (`gate`, `guard`) have their
// own semantics and intentionally do not share this type.
//
// Consumers return any `std::error::Error + Send + Sync + 'static`
// from their `Provider` / `Reloadable` / `Runnable` / `Finalizable`
// methods — the blanket
// `From<E>` impl wraps it into `Error::Other`. The registry then re-wraps
// `Error::Other` into the appropriate lifecycle variant (`Boot`, `Reload`,
// `Run`, `Validate`) at the call site, so downstream logs and matches see
// where the failure happened. Providers that want to emit a typed variant
// themselves can construct it directly — the registry will leave it
// untouched.

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    DuplicateProvider {
        type_name: &'static str,
    },
    Boot {
        name: &'static str,
        source: BoxError,
    },
    Validate {
        name: &'static str,
        source: BoxError,
    },
    Reload {
        name: &'static str,
        source: BoxError,
    },
    Finalize {
        name: &'static str,
        source: BoxError,
    },
    /// Fatal runnable failure (default). The runtime tears the worker
    /// down so the supervisor can respawn cleanly.
    Run {
        name: &'static str,
        source: BoxError,
    },
    /// Recoverable runnable failure. The runtime logs and keeps the
    /// worker serving — used for best-effort tasks (e.g. notify
    /// listeners, optional integrations) where a transient or
    /// configuration-driven failure shouldn't kill traffic.
    Recoverable {
        name: &'static str,
        source: BoxError,
    },
    Other(BoxError),
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Error::DuplicateProvider { type_name } => {
                write!(f, "provider type '{type_name}' is already registered")
            }
            Error::Boot { name, source } => {
                write!(f, "provider '{name}' failed during boot: {source}")
            }
            Error::Validate { name, source } => {
                write!(f, "provider '{name}' failed during validate: {source}")
            }
            Error::Reload { name, source } => {
                write!(f, "reload of '{name}' failed: {source}")
            }
            Error::Finalize { name, source } => {
                write!(f, "finalize of '{name}' failed: {source}")
            }
            Error::Run { name, source } => {
                write!(f, "runnable '{name}' failed: {source}")
            }
            Error::Recoverable { name, source } => {
                write!(f, "runnable '{name}' failed (recoverable): {source}")
            }
            Error::Other(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

// NOTE: `Error` intentionally does NOT implement `std::error::Error`.
// The blanket `From<E: Error>` below requires that `Error` itself not
// satisfy that bound (otherwise it would conflict with the core
// `From<T> for T` blanket). Consumers that need to chain `source()` can
// match on the variant and walk `BoxError` directly.

impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(e: E) -> Self {
        Error::Other(Box::new(e))
    }
}

/// Construct `Error::Other` from an arbitrary message string.
impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Other(Box::new(MsgErr(s.into())))
    }

    /// If the error is `Other`, re-wrap it as `Boot { name, source }`;
    /// otherwise leave it untouched. Used by `Registry::boot_all` to
    /// attach lifecycle context to anonymous user errors.
    fn into_boot(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Boot { name, source },
            other => other,
        }
    }
    fn into_validate(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Validate { name, source },
            other => other,
        }
    }
    /// Attach provider identity to an anonymous reload error.
    pub(crate) fn into_reload(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Reload { name, source },
            other => other,
        }
    }
    fn into_finalize(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Finalize { name, source },
            other => other,
        }
    }
    pub(crate) fn into_run(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Run { name, source },
            // Runnables that opt into recoverable failure construct
            // `Recoverable` with an empty `name`; `run_all` fills in the
            // provider name here so log lines stay attributed.
            Error::Recoverable { name: "", source } => Error::Recoverable { name, source },
            other => other,
        }
    }

    /// Build a recoverable runnable error from an arbitrary message.
    /// The runtime logs this and lets the worker keep serving instead of
    /// tearing it down. The provider `name` is filled in by `run_all`'s
    /// wrapper, so callers only supply the message.
    pub fn recoverable(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Recoverable { name: "", source: Box::new(MsgErr(s.into())) }
    }
}

/// One provider failure retained by a full validation pass.
#[derive(Debug)]
pub struct ValidationFailure {
    provider: &'static str,
    error: Error,
}

impl ValidationFailure {
    pub fn provider(&self) -> &'static str {
        self.provider
    }

    pub fn error(&self) -> &Error {
        &self.error
    }

    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of validating every provider in lifecycle order.
///
/// The outer [`Result`] returned by [`Registry::validate_all`] is reserved for
/// lifecycle-plan failures. Individual provider failures are retained here so
/// callers can report the complete invalid configuration in one pass.
#[derive(Debug, Default)]
#[must_use = "provider validation failures are reported through ValidationOutcome"]
pub struct ValidationOutcome {
    validated_count: usize,
    failures: Vec<ValidationFailure>,
}

impl ValidationOutcome {
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn validated_count(&self) -> usize {
        self.validated_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[ValidationFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<ValidationFailure> {
        self.failures
    }
}

/// One provider failure retained by a broadcast reload.
#[derive(Debug)]
pub struct ReloadFailure {
    provider: &'static str,
    error: Error,
}

impl ReloadFailure {
    pub fn provider(&self) -> &'static str {
        self.provider
    }

    pub fn error(&self) -> &Error {
        &self.error
    }

    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of a completed best-effort reload broadcast.
///
/// The outer [`Result`] returned by [`Registry::reload_all`] remains reserved
/// for failures that prevent the broadcast itself, such as state reload or
/// lifecycle-plan errors. Individual provider failures are retained here so
/// callers choose their own logging, metrics, or transport representation.
#[derive(Debug, Default)]
#[must_use = "provider reload failures are reported through ReloadOutcome"]
pub struct ReloadOutcome {
    reloaded_count: usize,
    failures: Vec<ReloadFailure>,
}

impl ReloadOutcome {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn reloaded_count(&self) -> usize {
        self.reloaded_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[ReloadFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<ReloadFailure> {
        self.failures
    }
}

/// One provider failure retained by best-effort finalization.
#[derive(Debug)]
pub struct FinalizeFailure {
    provider: &'static str,
    error: Error,
}

impl FinalizeFailure {
    pub fn provider(&self) -> &'static str {
        self.provider
    }

    pub fn error(&self) -> &Error {
        &self.error
    }

    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of a completed best-effort finalization pass.
#[derive(Debug, Default)]
#[must_use = "provider finalization failures are reported through FinalizeOutcome"]
pub struct FinalizeOutcome {
    finalized_count: usize,
    failures: Vec<FinalizeFailure>,
}

impl FinalizeOutcome {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn finalized_count(&self) -> usize {
        self.finalized_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[FinalizeFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<FinalizeFailure> {
        self.failures
    }
}

// =====================================================================
// Priority helpers
// =====================================================================

/// Shared lifecycle priority definitions for providers/reloadables.
///
/// Lower values run earlier among providers that are otherwise ready.
/// Prefer `ProviderOrder` for real dependencies; priorities are only
/// coarse tie-breakers for legacy/simple cases.
pub mod priority {
    /// Reserved floor for workspace-internal root providers.
    ///
    /// Ordinary providers should use `EARLY`, `NORMAL`, `LATE`, or explicit
    /// `ProviderOrder` edges instead of depending on this extreme value.
    #[doc(hidden)]
    pub const FIRST: u8 = 0;
    pub const EARLY: u8 = 50;
    pub const NORMAL: u8 = 100;
    pub const LATE: u8 = 150;
    /// Reserved ceiling for final workspace-internal lifecycle providers.
    ///
    /// Other providers should use `LATE` plus explicit `ProviderOrder` edges
    /// when they need to be late.
    #[doc(hidden)]
    pub const LAST: u8 = u8::MAX;
}

/// Type-based lifecycle ordering hints.
///
/// Numeric priorities still provide a coarse tie-breaker. `ProviderOrder`
/// adds explicit relationships between provider concrete types, so code can
/// say "run me before `T`" without relying on magic numbers or provider names.
#[derive(Clone, Debug, Default)]
pub struct ProviderOrder {
    before: Vec<TypeId>,
    after: Vec<TypeId>,
}

impl ProviderOrder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn before<T: 'static>(mut self) -> Self {
        self.before.push(TypeId::of::<T>());
        self
    }

    pub fn after<T: 'static>(mut self) -> Self {
        self.after.push(TypeId::of::<T>());
        self
    }

    pub fn before_types(&self) -> &[TypeId] {
        &self.before
    }

    pub fn after_types(&self) -> &[TypeId] {
        &self.after
    }
}

#[async_trait]
pub trait ReloadState: Send + Sync + Sized + 'static {
    async fn reload(&self) -> Result<()>;
}

/// Anything that can hot-reload itself when config changes.
///
/// `reload()` is the same shape as `Provider::boot()` — re-read the
/// on-disk config (use `tokio::fs`, never `std::fs` in this async path)
/// and rebuild the runtime snapshot, publishing it through an
/// `ArcSwap` so in-flight requests/connections see the swap atomically.
/// Reload must NOT change which providers are registered; it only
/// refreshes state of an already-registered provider.
#[async_trait]
pub trait Reloadable<S>: Provider<S> {
    /// Optional reload priority.
    ///
    /// Lower values run earlier among otherwise-ready providers. `None`
    /// means `priority::NORMAL`. Prefer `Provider::order()` for real
    /// dependency relationships.
    fn priority(&self) -> Option<u8> {
        None
    }

    /// Perform a synchronous reload using the current shared state.
    /// Implementations may spawn async work internally if needed.
    async fn reload(
        &self,
        state: &S,
    ) -> Result<()>;
}

/// Capability trait for providers that produce a long-running runtime task.
///
/// `run()` is the ONLY place in the lifecycle for long-running work
/// (accept loops, listeners, periodic tickers). It must NOT appear in
/// `register()` or `Provider::boot()`.
///
/// The supplied [`crate::RunContext`] belongs to one live service generation.
/// Observe its cancellation instead of only the process shutdown token: the
/// runtime uses the same path for targeted stop/restart and whole-process
/// shutdown.
///
/// Config-driven gating: if the provider is disabled at runtime (e.g.
/// an `enabled: false` config flag, or a single-instance service whose
/// pinned `worker_id` doesn't match this worker), this method MUST
/// short-circuit and return `Ok(())` immediately instead of starting the
/// long task. The provider stays registered for downstream capability
/// lookups; it just doesn't run on this process.
#[async_trait]
pub trait Runnable<S>: Provider<S> {
    /// Run one runtime-owned generation of this long-lived provider.
    ///
    /// NOTICE (convention):
    /// If this future returns `Err`, implementation should log contextual
    /// failure details itself (provider/task specific metadata).
    ///
    /// Reason:
    /// - Runtime layer handles lifecycle/control-flow only.
    /// - Runtime cannot reliably attach provider-specific business context.
    /// - Non-critical runnable errors are not centrally logged to avoid
    ///   duplicate/no-context error lines.
    async fn run(
        self: Arc<Self>,
        state: S,
        context: crate::RunContext,
    ) -> Result<()>;
}

/// Capability trait for providers that must release non-running
/// resources at the end of the process lifecycle.
///
/// `finalize()` runs after process shutdown has started and after the
/// runnable tasks have drained — runnables end their own futures (and
/// any protocol-level graceful drain) inside `Runnable::run()`;
/// `finalize()` is NOT the place to stop them.
///
/// This is not a replacement for `Drop`: it is the lifecycle point for
/// externally named resources whose stale presence can break the next
/// boot, such as shm segments or lock files. Implementations should be
/// idempotent because the finalize path may be re-entered.
#[async_trait]
pub trait Finalizable<S>: Provider<S> {
    /// Releases non-running resources after runnables drain.
    async fn finalize(
        &self,
        state: &S,
    ) -> Result<()>;
}

/// Any service that can be registered in the DI registry.
///
/// # Lifecycle convention
///
/// Each provider lives in four explicit phases. Mixing work across
/// phase boundaries is the most common bug source — keep them strict.
///
/// 1. **`register()` (free fn, outside the trait)** — synchronous, no
///    async, called once during bootstrap. Constructs the provider in
///    a placeholder/empty state and inserts it into the registry.
///
///    Allowed:
///    * Read state-level inputs (`state.run_mode()`, `state.config_dir()`)
///      to choose what to register.
///    * Read on-disk config synchronously *only* if the answer decides
///      whether to register the provider at all (e.g. feature toggles,
///      worker pinning). Use `std::fs` here — register is sync.
///
///    Forbidden:
///    * Resolving other providers from the registry (they may not exist
///      yet; ordering is settled by `Provider::order()` and coarse
///      priority, not by register order).
///    * Async I/O.
///    * Spawning tasks.
///    * Building the operational snapshot (that's `boot()`).
///
/// 2. **`boot()`** — async, called after every `register()` ran, in
///    lifecycle order. This is where the provider becomes usable.
///
///    Allowed / expected:
///    * Resolve dependencies from the registry — by now every other
///      `register()` has run.
///    * Async I/O — `tokio::fs` for config, network calls, etc. Never
///      `std::fs` (it blocks the runtime).
///    * Build the runtime snapshot and publish it via `ArcSwap` /
///      `ArcSwapOption` so concurrent readers see atomic swaps.
///    * Honor disabled-state from config: leave the snapshot empty and
///      return `Ok(())` rather than failing.
///
///    Forbidden:
///    * Spawning long-running tasks. Boot must return when state is
///      ready; the long task lives in `Runnable::run()`.
///
/// 3. **`validate()`** — synchronous readiness/invariant check after
///    boot and before runnable tasks start. Use this for cheap checks
///    that need boot-published state to exist.
///
/// 4. **`Runnable::run()`** — see that trait. The only place for
///    long-lived loops; honors disabled-state by returning `Ok(())`
///    immediately. Graceful teardown of the work started here belongs
///    here too: observe the supplied `RunContext` inside the run future,
///    drain, and return — do NOT split that into a separate hook.
///
/// 5. **`Finalizable::finalize()`** — optional capability (see that
///    trait), exposed via `as_finalizable()`. Best-effort release of
///    non-running resources after the runnable tasks have drained
///    (shm segments, lock files). Not a stop mechanism for runnables.
///
/// Reload (`Reloadable::reload()`) follows the same shape as `boot()`.
#[async_trait]
pub trait Provider<S>: Any + Send + Sync + 'static {
    /// Human-readable label for logs/diagnostics.
    fn name(&self) -> &'static str {
        "provider"
    }

    /// Optional boot priority. Lower values run earlier among otherwise-ready
    /// providers. `None` means `priority::NORMAL`. Prefer `Provider::order()`
    /// for dependency relationships; priority is only a coarse tie-breaker.
    fn boot_priority(&self) -> Option<u8> {
        None
    }

    /// Optional runtime task start priority. Lower values run earlier.
    /// `None` means `priority::NORMAL`.
    fn run_priority(&self) -> Option<u8> {
        None
    }

    /// Optional type-based boot/reload ordering hints.
    ///
    /// The registry builds one ordered lifecycle plan and uses it for boot,
    /// validate, finalize, and reload. Reload skips providers that are not
    /// `Reloadable`, but dependency relationships remain the same: reload is
    /// a boot emulation on a live process.
    fn order(&self) -> ProviderOrder {
        ProviderOrder::default()
    }

    /// Bootstrap-time async initialization. See the trait-level lifecycle
    /// convention for what belongs here vs in `register()` / `run()`.
    /// Default no-op so providers that only need `register()` insertion
    /// don't have to implement this.
    async fn boot(
        &self,
        _state: &S,
    ) -> Result<()> {
        Ok(())
    }

    /// Synchronous readiness validation after `boot()` and before runnable
    /// tasks are spawned.
    ///
    /// Use this for cheap checks over boot-published state, missing files, or
    /// conflicting settings that should fail before long-running work starts.
    fn validate(
        &self,
        _state: &S,
    ) -> Result<()> {
        Ok(())
    }

    /// Downcast hook for typed resolve APIs.
    fn as_any(&self) -> &dyn Any
    where
        Self: Sized,
    {
        self
    }

    /// Optional capability hook.
    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>> {
        None
    }

    /// Optional capability hook.
    fn as_finalizable(&self) -> Option<&dyn Finalizable<S>> {
        None
    }

    /// Optional capability hook.
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<S>>> {
        None
    }
}

/// Type-erased provider registry used for service discovery and DI-style lookup.
/// Registration happens during bootstrap and runtime access is read-only via typed resolves.
/// We keep the underlying maps behind `RwLock<HashMap<..>>` so registration stays simple while
/// lookup only holds a short-lived read lock long enough to clone the stored `Arc`.
pub struct Registry<S> {
    providers: RwLock<HashMap<TypeId, Arc<dyn Provider<S>>>>,
    by_type: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    dyn_by_type: RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    registration_order: RwLock<Vec<TypeId>>,
    lifecycle_order: RwLock<Option<Vec<TypeId>>>,
}

pub(crate) struct RunnableEntry<S> {
    pub(crate) name: &'static str,
    pub(crate) provider: Arc<dyn Provider<S>>,
    pub(crate) runnable: Arc<dyn Runnable<S>>,
}

impl<S: 'static> Registry<S> {
    /// Create the service with an empty registry. You can register later.
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
            dyn_by_type: RwLock::new(HashMap::new()),
            registration_order: RwLock::new(Vec::new()),
            lifecycle_order: RwLock::new(None),
        }
    }

    /// Register a provider into the registry.
    ///
    /// This accepts `Arc<P>` where `P: Provider`. The service is stored as a
    /// type-erased `Arc<dyn Provider>` but continues to point to the same underlying
    /// allocation (no new allocation is created).
    ///
    /// If another service with the same concrete type is already registered,
    /// registration is rejected.
    ///
    /// Returns `&Self` to allow fallible fluent chaining:
    ///
    /// ```ignore
    /// registry
    ///     .insert(dns.clone())?
    ///     .insert(ipc.clone())?;
    /// ```
    pub fn insert<P>(
        &self,
        item: Arc<P>,
    ) -> Result<&Self>
    where
        P: Provider<S> + 'static,
    {
        let type_id = TypeId::of::<P>();
        let any: Arc<dyn Any + Send + Sync> = item.clone();
        let mut by_type = self.by_type.write().expect("registry by_type lock poisoned");
        if by_type.contains_key(&type_id) {
            return Err(Error::DuplicateProvider { type_name: std::any::type_name::<P>() });
        }
        by_type.insert(type_id, any);
        drop(by_type);

        let it: Arc<dyn Provider<S>> = item;
        self.providers.write().expect("registry providers lock poisoned").insert(type_id, it);
        self.registration_order.write().expect("registry order lock poisoned").push(type_id);
        *self.lifecycle_order.write().expect("registry lifecycle order lock poisoned") = None;
        Ok(self)
    }

    /// Bind a registered concrete provider as a trait-object capability.
    ///
    /// This does not register a second lifecycle provider. It resolves the
    /// existing concrete provider `P`, casts the same `Arc<P>` into `Arc<I>`,
    /// and stores that trait-object handle under `TypeId::of::<I>()`.
    ///
    /// Returns an error if `P` has not been registered yet. If `I` already has
    /// a binding, the new binding replaces it.
    pub fn bind_dyn<I, P>(
        &self,
        cast: impl FnOnce(Arc<P>) -> Arc<I>,
    ) -> Result<&Self>
    where
        I: ?Sized + Send + Sync + 'static,
        P: Provider<S> + 'static,
    {
        let concrete = self.resolve::<P>().ok_or_else(|| {
            Error::msg(format!(
                "bind_dyn: provider type '{}' is not registered",
                std::any::type_name::<P>()
            ))
        })?;

        let interface_id = TypeId::of::<I>();
        let mut dyn_by_type = self.dyn_by_type.write().expect("registry dyn_by_type lock poisoned");
        let erased: Box<dyn Any + Send + Sync> = Box::new(cast(concrete));
        if dyn_by_type.insert(interface_id, erased).is_some() {
            tracing::debug!(
                interface = std::any::type_name::<I>(),
                "replaced dynamic capability binding"
            );
        }
        Ok(self)
    }

    /// Execute a closure with a concrete typed reference `&T` if the service is registered.
    pub fn with_typed<T, R>(
        &self,
        f: impl FnOnce(&T) -> R,
    ) -> Option<R>
    where
        T: Provider<S> + 'static,
    {
        let typed = self.resolve::<T>()?;
        Some(f(typed.as_ref()))
    }

    /// Resolve a concrete service as an owned `Arc<T>` handle.
    ///
    /// This is the DI-style, high-level API: it returns a typed `Arc<T>` that
    /// points to the same underlying allocation as the internally registered
    /// provider (no new `Arc` allocation). The returned `Arc` is obtained by
    /// downcasting from a type-indexed map (`TypeId`).
    ///
    /// Returns `None` if the type is not registered.
    pub fn resolve<T>(&self) -> Option<Arc<T>>
    where
        T: Provider<S> + 'static,
    {
        let any = self
            .by_type
            .read()
            .expect("registry by_type lock poisoned")
            .get(&TypeId::of::<T>())?
            .clone();
        Arc::downcast::<T>(any).ok()
    }

    /// Resolve a trait-object capability bound with `bind_dyn`.
    pub fn resolve_dyn<I>(&self) -> Option<Arc<I>>
    where
        I: ?Sized + Send + Sync + 'static,
    {
        self.dyn_by_type
            .read()
            .expect("registry dyn_by_type lock poisoned")
            .get(&TypeId::of::<I>())?
            .downcast_ref::<Arc<I>>()
            .cloned()
    }

    /// Return a snapshot of registered providers.
    pub fn providers(&self) -> Vec<Arc<dyn Provider<S>>> {
        self.providers.read().expect("registry providers lock poisoned").values().cloned().collect()
    }

    fn provider_entries_snapshot(&self) -> Vec<ProviderEntry<S>> {
        let providers = self.providers.read().expect("registry providers lock poisoned");
        self.registration_order
            .read()
            .expect("registry order lock poisoned")
            .iter()
            .enumerate()
            .filter_map(|(index, type_id)| {
                providers.get(type_id).cloned().map(|provider| ProviderEntry {
                    type_id: *type_id,
                    index,
                    provider,
                })
            })
            .collect()
    }

    /// Return the cached lifecycle plan, building it once if needed.
    ///
    /// The plan is invalidated on `insert()`. Normal lifecycle phases reuse
    /// the same known list, so reload is a boot emulation over the same
    /// provider order instead of a second ordering universe.
    fn lifecycle_plan(&self) -> Result<Vec<Arc<dyn Provider<S>>>> {
        if let Some(type_ids) = self
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .as_ref()
            .cloned()
        {
            return Ok(self.providers_from_type_ids(&type_ids));
        }

        let ordered = order_provider_entries(self.provider_entries_snapshot())?;
        let type_ids = ordered.iter().map(|entry| entry.type_id).collect::<Vec<_>>();
        let providers = ordered.iter().map(|entry| entry.provider.clone()).collect::<Vec<_>>();
        #[cfg(debug_assertions)]
        tracing::debug!(
            providers = ?providers.iter().map(|provider| provider.name()).collect::<Vec<_>>(),
            "provider lifecycle order"
        );
        *self.lifecycle_order.write().expect("registry lifecycle order lock poisoned") =
            Some(type_ids);
        Ok(providers)
    }

    fn providers_from_type_ids(
        &self,
        type_ids: &[TypeId],
    ) -> Vec<Arc<dyn Provider<S>>> {
        let providers = self.providers.read().expect("registry providers lock poisoned");
        type_ids.iter().filter_map(|type_id| providers.get(type_id).cloned()).collect()
    }

    /// Return the list of provider display names (for diagnostics only).
    pub fn list_names(&self) -> Vec<&'static str> {
        self.providers().iter().map(|c| c.name()).collect()
    }

    /// Return provider display names in lifecycle order.
    ///
    /// This is useful for diagnostics and startup logging before running
    /// `boot_all()`.
    pub fn lifecycle_names(&self) -> Result<Vec<&'static str>> {
        Ok(self.lifecycle_plan()?.iter().map(|provider| provider.name()).collect())
    }

    pub(crate) fn runnable_entries(&self) -> Vec<RunnableEntry<S>> {
        let mut providers = self.providers();
        providers.sort_by_key(|provider| {
            (provider.run_priority().unwrap_or(priority::NORMAL), provider.name())
        });

        let mut runnables = Vec::new();
        for provider in providers {
            let Some(runnable) = provider.clone().as_runnable() else { continue };
            runnables.push(RunnableEntry { name: provider.name(), provider, runnable });
        }
        runnables
    }

    /// Run `validate` for every provider in lifecycle order.
    ///
    /// Lifecycle-plan failures remain outer errors. A provider validation
    /// failure does not prevent later providers from being checked; every
    /// provider failure is retained in the outcome.
    pub fn validate_all(
        &self,
        state: &S,
    ) -> Result<ValidationOutcome> {
        let mut outcome = ValidationOutcome::default();

        for provider in self.lifecycle_plan()? {
            let name = provider.name();
            match provider.validate(state) {
                Ok(()) => outcome.validated_count += 1,
                Err(error) => outcome
                    .failures
                    .push(ValidationFailure { provider: name, error: error.into_validate(name) }),
            }
        }
        Ok(outcome)
    }

    pub async fn boot_all(
        &self,
        state: &S,
    ) -> Result<()> {
        for provider in self.lifecycle_plan()? {
            let name = provider.name();
            if let Err(e) = provider.boot(state).await {
                return Err(e.into_boot(name));
            }
        }
        Ok(())
    }

    /// Run `Finalizable::finalize` for every provider that exposes the
    /// capability, in reverse lifecycle order. Called by the bootstrap
    /// layer after the runnable tasks have drained. One failure does not stop
    /// later finalizers; every failure is retained in the outcome.
    pub async fn finalize_all(
        &self,
        state: &S,
    ) -> Result<FinalizeOutcome> {
        let mut providers = self.lifecycle_plan()?;
        providers.reverse();
        let mut outcome = FinalizeOutcome::default();

        for provider in providers {
            let Some(finalizable) = provider.as_finalizable() else { continue };
            let name = provider.name();
            match finalizable.finalize(state).await {
                Ok(()) => outcome.finalized_count += 1,
                Err(error) => outcome
                    .failures
                    .push(FinalizeFailure { provider: name, error: error.into_finalize(name) }),
            }
        }
        Ok(outcome)
    }

    pub async fn reload_one(
        &self,
        name: &str,
        state: &S,
    ) -> Result<()> {
        let Some(provider) = self.providers().into_iter().find(|provider| provider.name() == name)
        else {
            return Err(Error::msg(format!(
                "reload_by_name: no provider registered with name '{}'",
                name
            )));
        };

        let Some(reloadable) = provider.as_reloadable() else {
            return Err(Error::msg(format!(
                "reload_by_name: provider '{}' is not reloadable",
                name
            )));
        };

        let provider_name = provider.name();
        reloadable.reload(state).await.map_err(|error| error.into_reload(provider_name))
    }
}

impl<S> Registry<S>
where
    S: ReloadState + 'static,
{
    pub async fn reload_all(
        &self,
        state: &S,
    ) -> Result<ReloadOutcome> {
        state.reload().await?;

        let mut outcome = ReloadOutcome::default();

        for provider in self.lifecycle_plan()? {
            let name = provider.name();
            if let Some(reloadable) = provider.as_reloadable() {
                match reloadable.reload(state).await {
                    Ok(()) => outcome.reloaded_count += 1,
                    Err(error) => outcome
                        .failures
                        .push(ReloadFailure { provider: name, error: error.into_reload(name) }),
                }
            }
        }

        Ok(outcome)
    }
}

struct ProviderEntry<S> {
    type_id: TypeId,
    index: usize,
    provider: Arc<dyn Provider<S>>,
}

impl<S> Clone for ProviderEntry<S> {
    fn clone(&self) -> Self {
        Self { type_id: self.type_id, index: self.index, provider: self.provider.clone() }
    }
}

fn order_provider_entries<S: 'static>(
    entries: Vec<ProviderEntry<S>>
) -> Result<Vec<ProviderEntry<S>>> {
    let len = entries.len();
    let positions: HashMap<TypeId, usize> =
        entries.iter().enumerate().map(|(idx, entry)| (entry.type_id, idx)).collect();
    let priorities: Vec<u8> =
        entries.iter().map(|entry| lifecycle_priority(&entry.provider)).collect();
    let mut outgoing: Vec<HashSet<usize>> = (0..len).map(|_| HashSet::new()).collect();
    let mut indegree = vec![0usize; len];

    let mut add_edge = |from: usize, to: usize| {
        if from != to && outgoing[from].insert(to) {
            indegree[to] += 1;
        }
    };

    for (idx, entry) in entries.iter().enumerate() {
        let order = entry.provider.order();
        for target in order.before_types() {
            if let Some(&target_idx) = positions.get(target) {
                add_edge(idx, target_idx);
            }
        }
        for target in order.after_types() {
            if let Some(&target_idx) = positions.get(target) {
                add_edge(target_idx, idx);
            }
        }
    }

    let mut ready: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(idx, degree)| (*degree == 0).then_some(idx))
        .collect();
    let mut ordered = Vec::with_capacity(len);

    while !ready.is_empty() {
        ready.sort_by_key(|idx| {
            (priorities[*idx], entries[*idx].index, entries[*idx].provider.name())
        });
        let idx = ready.remove(0);
        ordered.push(idx);

        let next: Vec<_> = outgoing[idx].iter().copied().collect();
        for target in next {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }

    if ordered.len() != len {
        let blocked = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, degree)| (*degree > 0).then_some(entries[idx].provider.name()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::msg(format!("provider lifecycle order cycle detected: {blocked}")));
    }

    Ok(ordered.into_iter().map(|idx| entries[idx].clone()).collect())
}

fn lifecycle_priority<S: 'static>(provider: &Arc<dyn Provider<S>>) -> u8 {
    provider
        .boot_priority()
        .or_else(|| provider.as_reloadable().and_then(|reloadable| reloadable.priority()))
        .unwrap_or(priority::NORMAL)
}

impl<S: 'static> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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
            .insert(Arc::new(ApiProvider))
            .expect("api registration should succeed")
            .insert(Arc::new(CacheProvider))
            .expect("cache registration should succeed")
            .insert(Arc::new(DbProvider))
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

        registry.insert(original.clone()).expect("first registration should succeed");
        let error = match registry.insert(Arc::new(ApiProvider)) {
            Err(error) => error,
            Ok(_) => panic!("duplicate registration must be rejected"),
        };

        assert!(matches!(
            error,
            Error::DuplicateProvider { type_name }
                if type_name == std::any::type_name::<ApiProvider>()
        ));
        let resolved = registry.resolve::<ApiProvider>().expect("original provider should remain");
        assert!(Arc::ptr_eq(&resolved, &original));
    }

    #[test]
    fn lifecycle_order_supports_before_edges_and_diagnostics() {
        let state = TestState::default();
        let registry = Registry::<TestState>::new();

        registry
            .insert(Arc::new(ApiProvider))
            .expect("api registration should succeed")
            .insert(Arc::new(MetricsProvider))
            .expect("metrics registration should succeed")
            .insert(Arc::new(CacheProvider))
            .expect("cache registration should succeed")
            .insert(Arc::new(DbProvider))
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
            .insert(Arc::new(FailsValidation))
            .expect("failing validator should register")
            .insert(Arc::new(DbProvider))
            .expect("successful validator should register");

        let outcome = registry.validate_all(&state).expect("validation should complete");

        assert!(!outcome.is_valid());
        assert_eq!(outcome.validated_count(), 1);
        assert_eq!(outcome.failed_count(), 1);
        let failure = &outcome.failures()[0];
        assert_eq!(failure.provider(), "fails-validation");
        assert!(matches!(failure.error(), Error::Validate { name: "fails-validation", .. }));
        assert_eq!(
            failure.error().to_string(),
            "provider 'fails-validation' failed during validate: validation rejected"
        );
        assert_eq!(state.seen.lock().expect("test log poisoned").as_slice(), &["db"]);
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

        registry.insert(Arc::new(ConsoleLogger)).expect("console logger should register");
        registry
            .bind_dyn::<dyn LogSink, ConsoleLogger>(|logger| logger)
            .expect("dyn binding should succeed");

        let sink = registry.resolve_dyn::<dyn LogSink>().expect("LogSink should be bound");
        assert_eq!(sink.line(), "console");

        registry.insert(Arc::new(FileLogger)).expect("file logger should register");
        registry
            .bind_dyn::<dyn LogSink, FileLogger>(|logger| logger)
            .expect("dyn binding replacement should succeed");

        let sink = registry.resolve_dyn::<dyn LogSink>().expect("LogSink should be rebound");
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

        async fn boot(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("boot-recorder");
            Ok(())
        }

        fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
            Some(self)
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

        async fn boot(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("boot-dependency");
            Ok(())
        }

        fn as_finalizable(&self) -> Option<&dyn Finalizable<TestState>> {
            Some(self)
        }
    }

    #[async_trait]
    impl Finalizable<TestState> for BootDependency {
        async fn finalize(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("finalize-dependency");
            Ok(())
        }
    }

    #[tokio::test]
    async fn boot_and_finalize_share_lifecycle_plan() {
        let state = TestState::default();
        let registry = Registry::<TestState>::new();

        registry
            .insert(Arc::new(BootRecorder))
            .expect("boot recorder should register")
            .insert(Arc::new(BootDependency))
            .expect("boot dependency should register");

        registry.boot_all(&state).await.expect("boot should succeed");
        let outcome = registry.finalize_all(&state).await.expect("finalize should succeed");
        assert!(outcome.is_complete());
        assert_eq!(outcome.finalized_count(), 2);

        let seen = state.seen.lock().expect("test log poisoned").clone();
        assert_eq!(
            seen,
            vec!["boot-dependency", "boot-recorder", "finalize-recorder", "finalize-dependency",]
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
            .insert(Arc::new(CycleA))
            .expect("cycle A should register")
            .insert(Arc::new(CycleB))
            .expect("cycle B should register");

        let err = registry.validate_all(&state).expect_err("cycle must be rejected");
        assert!(err.to_string().contains("provider lifecycle order cycle detected"));
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
            .insert(Arc::new(FailsReload))
            .expect("failing reloader should register")
            .insert(Arc::new(Reloads))
            .expect("successful reloader should register");

        let outcome = registry.reload_all(&state).await.expect("broadcast should complete");

        assert!(!outcome.is_complete());
        assert_eq!(outcome.reloaded_count(), 1);
        assert_eq!(outcome.failed_count(), 1);
        let failure = &outcome.failures()[0];
        assert_eq!(failure.provider(), "fails-reload");
        assert!(matches!(failure.error(), Error::Reload { name: "fails-reload", .. }));
        assert_eq!(failure.error().to_string(), "reload of 'fails-reload' failed: reload rejected");
    }

    #[tokio::test]
    async fn finalize_all_retains_provider_failures_and_continues() {
        let state = TestState::default();
        let registry = Registry::<TestState>::new();
        registry
            .insert(Arc::new(Finalizes))
            .expect("successful finalizer should register")
            .insert(Arc::new(FailsFinalize))
            .expect("failing finalizer should register");

        let outcome = registry.finalize_all(&state).await.expect("finalization should complete");

        assert!(!outcome.is_complete());
        assert_eq!(outcome.finalized_count(), 1);
        assert_eq!(outcome.failed_count(), 1);
        let failure = &outcome.failures()[0];
        assert_eq!(failure.provider(), "fails-finalize");
        assert!(matches!(failure.error(), Error::Finalize { name: "fails-finalize", .. }));
        assert_eq!(
            failure.error().to_string(),
            "finalize of 'fails-finalize' failed: finalize rejected"
        );
    }
}
