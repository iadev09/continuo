use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use super::{Finalizable, ProviderOrder, Reloadable, Result, Runnable};

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
///    lifecycle order. The object is booted: it is built and reads its
///    configuration, and nothing more.
///
///    Allowed / expected:
///    * Resolve dependencies from the registry — by now every other
///      `register()` has run.
///    * Read configuration and state — `tokio::fs`, never `std::fs` (it
///      blocks the runtime).
///    * Build the runtime snapshot and publish it via `ArcSwap` /
///      `ArcSwapOption` so concurrent readers see atomic swaps.
///    * Honor disabled-state from config: leave the snapshot empty and
///      return `Ok(())` rather than failing.
///
///    Forbidden — anything with an effect outside the object:
///    * Binding a socket or opening a listener.
///    * Connecting to or talking to a peer.
///    * Spawning a task, long-running or not.
///
///    All of that is `Runnable::run()`. Boot runs where nothing may serve:
///    a configuration check (`--check-config`) boots, validates and
///    finalizes every provider without starting runnables, possibly as a
///    user without the rights the server has and beside a server already
///    running.
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
///    trait), exposed via `as_finalizable()`. The reverse of `boot()`:
///    boot prepares, finalize releases what was prepared — best-effort,
///    after the runnable tasks have drained (shm segments, lock files).
///    Not a stop mechanism for runnables; stopping is `run()`'s own end.
///
///    `boot` → `validate` → `run` → `finalize`: prepare, check, work,
///    release. Only `run` has effects on the world outside the object.
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

    /// Optional placement metadata for an application-owned fleet adapter.
    /// The default is treated as `Scope::Worker`. Runtime consults this before
    /// creating local runnable entries; provider/configuration registration
    /// remains intact. Scope is sampled at initial runnable submission.
    #[cfg(feature = "fleet")]
    fn as_scoped(&self) -> Option<&dyn crate::fleet::Scoped> {
        None
    }

    /// Optional dispatch metadata for a fleet execution adapter.
    /// An absent capability means `Dispatch::Each`. The adapter selects among
    /// scope-eligible processes; this hook neither arbitrates ownership nor
    /// changes local service activation. It applies to runnable execution,
    /// not provider construction, boot or configuration.
    #[cfg(feature = "fleet")]
    fn as_dispatched(&self) -> Option<&dyn crate::fleet::Dispatched> {
        None
    }

    /// Optional config-source discovery for application-owned notification.
    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        None
    }

    /// Optional declaration of the environments this provider's programs are
    /// started with. Asked after `configure` and after a reload; the reader
    /// keeps one node per program and the runtime delivers a re-read file
    /// through `Environmental::environment_changed`.
    fn as_environmental(&self) -> Option<&dyn congif::Environmental> {
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
