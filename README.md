<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

> **Runtime service composition for Rust**

`continuo` is a typed runtime registry for Rust services.

Register providers once, resolve the same shared instances by Rust type, and
let those instances join the application lifecycle when they need to.

The provider graph stays explicit and stable. Runtime behavior can still change
without recompiling: providers rebuild and publish their internal snapshots on
reload, while callers keep resolving the same typed service.

Use it when an application has several long-lived services and needs one small,
framework-agnostic place to:

- register providers once as typed shared instances
- resolve the same instances later by concrete Rust type
- boot, validate, reload, and finalize them in deterministic dependency order
- spawn long-running async runnable providers
- drain accepted work through graceful shutdown gates
- publish optional typed in-process lifecycle events

This is the whole application:

```rust
let state = AppState::new();
let mut runtime = Runtime::<AppState>::default();

// The registry stores typed providers and walks their lifecycle.
let registry = state.registry_ref();
registry
    .insert(runtime.manager())?             // same runtime control handle
    .insert(Arc::new(CounterService::new()))?
    .insert(Arc::new(HttpService::new(addr)))?
    .insert(Arc::new(SignalService))?;  // even Ctrl+C is a service

registry.boot_all(&state).await?;       // dependency-ordered
let validation = registry.validate_all(&state)?;
// The application decides how to report validation.failures().

// The runtime owns the live Runnable generations.
runtime.spawn_all(registry, state.clone())?;
runtime.wait_until_shutdown(&state).await?;
runtime.drain().await?;                 // runnables end themselves

let finalized = registry.finalize_all(&state).await?;
// The application decides how to report finalized.failures().
```

After insertion, application code resolves providers by type and the runtime
calls the hooks those providers expose. The full working version is
[`examples/harness/counter.rs`](https://github.com/iadev09/continuo/blob/main/examples/harness/counter.rs).

The core idea:

```text
Registry<AppState>            — typed instances plus lifecycle hooks
  ├─ ServiceManager -> Provider (control capability; no shadow state)
  ├─ CounterService -> Provider + Reloadable + Runnable
  ├─ HttpService    -> Provider + Runnable
  └─ SignalService  -> Provider + Runnable

Runtime<AppState>             — sole owner of live Runnable generations
  ├─ starts every provider that exposes Runnable
  └─ applies ServiceManager start / stop / restart commands
```

## Features

Default features are:

```toml
default = ["registry"]
full = ["registry", "support", "events"]
```

- `registry` adds `registry_ref()` to `SharedState` for states that carry
  `Registry<Self>`. The `Registry` type itself is always available.
- `events` enables `LifecycleBus`, adds `events()` to `SharedState`, and
  enables the `dashmap` dependency.
- `support` enables `Gate`, `Permit`, `GuardGroup`, and `Guard`.

## Your State

The crate does not own your app state. Your state only needs to implement
`SharedState`. If you want `registry.reload_all(&state).await`, implement
`ReloadState` too.

```rust
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use continuo::LifecycleBus;
use continuo::{Registry, ReloadState, Result, SharedState};

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<AppState>,
    events: LifecycleBus,
}

impl Default for AppState {
    fn default() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            events: LifecycleBus::new(),
        }))
    }
}

impl AppState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_shutdown(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.0.shutdown.cancelled()
    }
}

impl SharedState for AppState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }

    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }

    fn events(&self) -> &LifecycleBus {
        &self.0.events
    }
}

#[async_trait]
impl ReloadState for AppState {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}
```

## The Registry

`Registry<S>` stores providers as shared `Arc<T>` values keyed by concrete Rust
type. Anything with access to your state can resolve the same instance later:

```rust
let db = state.registry_ref().resolve::<DbService>().expect("DbService registered");
```

The same registered object can have two roles:

- application code resolves it by concrete type
- the runtime walks its lifecycle hooks: `validate`, `boot`, `reload`, `run`,
  `finalize`

That means a provider can be:

- a shared service resolved by application code
- a lifecycle participant with `boot()` / `validate()`
- a reload participant with `Reloadable`
- a long-running task with `Runnable`
- a post-drain cleanup participant with `Finalizable`

```rust
use std::sync::Arc;

let state = AppState::new();
state
    .registry_ref()
    .insert(Arc::new(DbService::new()))?
    .insert(Arc::new(CacheService::new()))?;

state.registry_ref().boot_all(&state).await?;
let validation = state.registry_ref().validate_all(&state)?;
```

Register once. Resolve by type. Let the same object opt into only the lifecycle
capabilities it actually needs.

`Reloadable`, `Runnable`, and `Finalizable` are provider capability traits:
each has `Provider<S>` as a supertrait. Implementing one therefore requires the
same concrete type to implement `Provider<S>`; the corresponding `as_*` hook
then exposes that capability to registry lifecycle traversal.

### How Resolve Works

Concrete resolve uses only `Any` and `TypeId` from the standard library:

```rust
use std::any::{Any, TypeId};
```

- `insert(Arc<P>)` stores the provider under `TypeId::of::<P>()`.
- the same allocation is also stored as `Arc<dyn Provider<S>>` for lifecycle
  traversal.
- `resolve::<T>()` clones the stored `Arc<dyn Any + Send + Sync>` and
  downcasts it back to `Arc<T>`.
- `resolve::<T>()` requires `T: Provider<S>`, so non-provider types do not
  compile as registry lookups.

There are no string keys, service tokens, macros, or request-path container
lookups in the concrete resolve path.

### Trait Object Capabilities

Concrete providers can also expose a trait-object capability:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use continuo::Provider;

trait Logger: Send + Sync {
    fn info(&self, message: &str);
}

struct ConsoleLogger;

impl Logger for ConsoleLogger {
    fn info(&self, message: &str) {
        println!("{message}");
    }
}

#[async_trait]
impl Provider<AppState> for ConsoleLogger {
    fn name(&self) -> &'static str {
        "console-logger"
    }
}

registry.insert(Arc::new(ConsoleLogger))?;
registry.bind_dyn::<dyn Logger, ConsoleLogger>(|logger| logger)?;

let logger = registry.resolve_dyn::<dyn Logger>().expect("Logger bound");
logger.info("ready");
```

`bind_dyn` does not add another lifecycle participant. It binds an already
registered concrete provider as a capability, and `resolve_dyn::<dyn Logger>()`
returns the same shared instance through that trait-object view.

There is one active binding for a given trait object. Calling `bind_dyn` again
for the same trait object replaces that binding; this is useful during
bootstrap when configuration decides which registered provider should be the
default capability.

If an application needs runtime backend switching without recompiling, prefer a
stable concrete provider that swaps its internal strategy during `reload()`:

```rust
// App code resolves DomainService; reload can swap GoDaddy for Namecheap
// inside the service without changing the registry graph.
let domains = registry.resolve::<DomainService>().expect("DomainService registered");
```

## Providers

A provider is a plugin-shaped runtime participant registered as a shared typed
instance. Application code can resolve it later by concrete Rust type, while
the runtime can call its lifecycle hooks.

```rust
use async_trait::async_trait;
use continuo::{Provider, Result};

pub struct DbService;

#[async_trait]
impl Provider<AppState> for DbService {
    fn name(&self) -> &'static str {
        "db"
    }

    async fn boot(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Open pools, read config, build snapshots, publish ready state.
        Ok(())
    }

    fn validate(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Cheap readiness checks after boot.
        Ok(())
    }
}
```

## Lifecycle Ordering

Lifecycle order is deterministic and type-aware. Providers can express concrete
dependencies with `ProviderOrder::before::<T>()` / `ProviderOrder::after::<T>()`
instead of relying on registration order or numeric priority conventions.

The same lifecycle plan is used for:

- `boot_all`
- `validate_all`
- `reload_all`
- `finalize_all`, in reverse order (only providers exposing `Finalizable`)

Reload skips providers that are not `Reloadable`, but it keeps the same relative
dependency order as boot. If a dependency cycle is introduced, the registry
returns an error before running the lifecycle phase.

```rust
use continuo::{Provider, ProviderOrder};

pub struct DbService;
pub struct CacheService;

impl Provider<AppState> for CacheService {
    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<DbService>()
    }
}
```

Registration order can now stay ergonomic:

```rust
use std::sync::Arc;

state.registry_ref().insert(Arc::new(CacheService))?;
state.registry_ref().insert(Arc::new(DbService))?;

let names = state.registry_ref().lifecycle_names()?;
println!("Lifecycle order: {}", names.join(" -> "));

state.registry_ref().boot_all(&state).await?;
// DbService boots before CacheService because CacheService says:
// ProviderOrder::new().after::<DbService>()
```

`boot_priority()` and `Reloadable::priority()` are still available as coarse
tie-breakers among otherwise-ready providers. Prefer `ProviderOrder` for real
dependencies. `run_priority()` controls only runtime task spawn order.

`validate_all()` checks the complete lifecycle plan even when one provider is
invalid. `ValidationOutcome` retains the successful count and every
`ValidationFailure`; the application owns reporting and the decision to abort
startup.

## Runnable Providers

Long-running loops live in `Runnable::run()`, not in `boot()`.
`Runnable::run()` is an async trait method and receives `self: Arc<Self>`, so
providers can be spawned without returning a boxed task future.

Graceful teardown belongs inside the same future. Observe the supplied
`RunContext`, drain your in-flight work, and return. Its cancellation token is
scoped to one live generation and is a child of the process shutdown token, so
targeted service stop and whole-process shutdown share one honest teardown
path.

```rust
use std::sync::Arc;
use async_trait::async_trait;
use continuo::{Provider, Result, RunContext, Runnable};

#[async_trait]
impl Runnable<AppState> for CacheService {
    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext,
    ) -> Result<()> {
        context.cancelled().await;
        Ok(())
    }
}

impl Provider<AppState> for CacheService {
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}
```

Then the runtime starts every runnable provider:

```rust
use continuo::{Runtime, SharedState};

let state = AppState::new();
let mut runtime = Runtime::<AppState>::default();
state.registry_ref().insert(runtime.manager())?;

runtime.spawn_all(state.registry_ref(), state.clone())?;
state.initiate_shutdown();
runtime.wait_until_shutdown(&state).await?;
runtime.drain().await?;
# Ok::<(), continuo::Error>(())
```

### Managing Runnable Services

`Runtime::manager()` returns the cloneable `ServiceManager` handle backed by
that exact runtime. Register the same `Arc` as a provider before boot; HTTP,
gRPC, CLI adapters, or other providers can then resolve it as an ordinary
typed capability.

```rust
let mut runtime = Runtime::<AppState>::default();
state.registry_ref().insert(runtime.manager())?;

runtime.spawn_all(state.registry_ref(), state.clone())?;

let manager = state.registry_ref().resolve::<ServiceManager>().unwrap();
let services = manager.list().await?;
manager.stop("counter").await?;       // waits until run() actually returns
manager.start("counter").await?;      // creates the next generation
manager.restart("counter").await?;   // strictly stop, then start
manager.reload("counter").await?;    // same generation, targeted Reloadable
```

The manager carries commands, not lifecycle state. `Runtime` remains the sole
owner of futures, generation counters, cancellation tokens, and status. A
successful `start` means the generation was accepted and spawned; readiness of
an application protocol remains the service's own concern.

This API deliberately does not add scheduling, concurrency, placement,
backoff, or configuration. Those belong to workload/task orchestration, not
the structural lifecycle of code-registered services.

## Reloadable Providers

Reload is a capability, not a second registry.

```rust
use async_trait::async_trait;
use continuo::{Provider, Reloadable, Result};

#[async_trait]
impl Reloadable<AppState> for DbService {
    async fn reload(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Rebuild runtime snapshot and atomically publish it.
        Ok(())
    }
}

impl Provider<AppState> for DbService {
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}
```

Use `registry.reload_one("db", &state).await` for targeted reloads or
`registry.reload_all(&state).await` for full reloads.

`reload_all` first calls `ReloadState::reload()` on your state, then walks the
same lifecycle order used by `boot_all` and calls `Reloadable::reload()` on
reloadable providers. It is a best-effort broadcast: one provider failure does
not prevent later providers from reloading. The returned `ReloadOutcome`
contains the successful provider count and each `ReloadFailure` with its
provider name and original typed error. The outer `Result` is reserved for a
state reload or lifecycle-plan failure that prevents the broadcast itself.

## Finalizable Providers

Finalize releases non-running resources **after the runnable tasks have
drained**: externally named things whose stale presence would break the next
boot, such as shm segments or lock files. It is not a stop mechanism; runnables
end their own futures inside `run()`.

```rust
use async_trait::async_trait;
use continuo::{Finalizable, Provider, Result};

#[async_trait]
impl Finalizable<AppState> for DbService {
    async fn finalize(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Releases non-running resources after runnables drain.
        Ok(())
    }
}

impl Provider<AppState> for DbService {
    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}
```

`registry.finalize_all(&state).await` walks the lifecycle plan in reverse and
calls `finalize()` on every provider that exposes the capability. One failure
does not stop the remaining finalizers. `FinalizeOutcome` retains the successful
provider count and every `FinalizeFailure`; the caller owns logging and policy.

## Gates

The `support` feature enables optional runtime support tools. They are not
required by `Registry` or `Runtime`, but they are useful when implementing
servers, connection loops, and request handlers.

`Gate` is a graceful shutdown admission/drain tool. It is inspired by axum's
server graceful-shutdown pattern, but lifted out of HTTP.

It can:

- reject new work after graceful shutdown starts
- track accepted in-flight work with `Permit`
- wait until all permits drop
- force shutdown after a grace period
- optionally apply simple max-in-flight admission control

```rust
use std::time::Duration;
use continuo::{Gate, GateDrainOutcome};

let gate = Gate::new(Some(1024), Duration::from_millis(100));
let permit = gate.enter().await?;

gate.graceful_shutdown(Some(Duration::from_secs(30)));
if let GateDrainOutcome::Forced { remaining } = gate.wait_all_done().await {
    eprintln!("forced shutdown with {remaining} permit(s) still active");
}
# Ok::<(), continuo::gate::Error>(())
```

`GuardGroup` is a smaller RAII in-flight counter for places where you only
need “count active work and wait until zero” without admission control.

## Lifecycle Events

The `events` feature enables `LifecycleBus`, a typed process-local event bus.
Use it when services need a loose in-process signal without depending on each
other directly.

```rust
use continuo::LifecycleBus;

#[derive(Clone, Debug)]
struct ConfigReloaded;

let bus = LifecycleBus::new();
let mut rx = bus.subscribe::<ConfigReloaded>();
bus.emit(ConfigReloaded);
```

## Example

[`examples/harness/counter.rs`](https://github.com/iadev09/continuo/blob/main/examples/harness/counter.rs)
shows the full flow in one file:

```sh
cargo run --example counter
curl http://127.0.0.1:3000/hit        # repeat — the count grows
curl http://127.0.0.1:3000/services   # list live runnable generations
curl -X POST http://127.0.0.1:3000/services/counter/stop
curl -X POST http://127.0.0.1:3000/services/counter/start
curl -X POST http://127.0.0.1:3000/services/counter/restart
curl -X POST http://127.0.0.1:3000/services/counter/reload
curl -L http://127.0.0.1:3000/reload  # reload over HTTP — resets to 0
kill -HUP <pid>                       # same reload, via signal
# Ctrl+C — graceful drain
```

The counter is a managed runnable. Every start, including restart, creates a
new generation and resets its state:

```text
counter started: reset to 0
http listening on http://127.0.0.1:3000/hit
http: hit #1 (client port 51422 -> its #1)
counter stopped
counter started: reset to 0
http: hit #1 (client port 51423 -> its #1)
^C
signals: initiating process shutdown
http drained and stopped
counter stopped
all services finished gracefully
```

Four providers are involved:

- **`CounterService`** is **application-scoped**: one instance for the
  process lifetime, shared by every request. Its managed `Runnable` generation
  decides whether it is active and resets the count whenever it starts.
- **`ServiceManager`** is the registered handle for the runtime-owned service
  state. The HTTP control router resolves it like any other provider.
- **`HttpService`** boots after its dependency
  (`ProviderOrder::new().after::<CounterService>()`), resolves it from
  the registry once while building the router, and serves/drains inside
  `Runnable::run()`.
- **`SignalService`** — even signal handling is a provider: Ctrl+C maps
  to shutdown, SIGHUP to `reload_all`.

axum is a dev-dependency only; the transport is an implementation detail.

## Why the name?

In baroque music, the *basso continuo* is the part that plays through the
whole piece while soloists enter and leave. `continuo` tries to be that steady
part for application services: the registry stays available, while providers
resolve, run, reload, and finalize around it.

## Dependencies

The dependency set is intentionally small and canonical for Tokio-based
services:

```toml
async-trait = "0.1"
dashmap = "6"
tokio = { version = "1", features = ["macros", "rt", "sync", "time"] }
tokio-util = "0.7"
tracing = "0.1"
```

`dashmap` is only required when the `events` feature is enabled.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
