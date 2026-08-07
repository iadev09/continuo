<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

> **The bass line for your services.**

In baroque music, the *basso continuo* is the part that plays through the
whole piece: soloists enter and leave, the continuo never stops. This crate
is that part for your application — the typed registry and lifecycle that
carries every service from the first note (`boot`) to the last (`finalize`).

The dependency injection core for multi-service Rust applications —
every framework ships one buried inside it; continuo is that core,
shipped alone. Tokio-native lifecycle and service composition, no
framework attached.

`continuo` gives your app one place to register services, boot them,
reload them, run their background tasks, resolve them by Rust type, and shut
them down gracefully:

- register typed services once
- boot / validate / reload / finalize them in deterministic dependency order
- resolve them later by concrete Rust type
- remove the need for ad-hoc runtime injection
- spawn long-running async runnable providers
- drain accepted work through graceful shutdown gates
- publish typed in-process lifecycle events with the `events` feature

This is the whole application:

```rust
let state = AppState::new();

// The registry — the score: who plays, in what order.
// It owns the provider hooks: validate / boot / reload / finalize.
let registry = state.registry_ref();
registry
    .insert(Arc::new(CounterService::new()))
    .insert(Arc::new(HttpService::new(addr)))
    .insert(Arc::new(SignalService));   // even Ctrl+C is a service

registry.validate_all(&state)?;
registry.boot_all(&state).await?;       // dependency-ordered

// The runtime — the performance: the live runnable tasks.
let mut runtime = Runtime::<AppState>::default();
runtime.spawn_all(registry, state.clone());
runtime.wait_until_shutdown(&state).await?;
runtime.drain().await?;                 // runnables end themselves

// Back to the registry for the last note.
registry.finalize_all(&state).await?;   // release named resources
```

Three inserts — everything else follows from the lifecycle contract; `main`
composes providers and walks the lifecycle, nothing more. The full working
version is [`examples/counter.rs`](https://github.com/iadev09/continuo/blob/main/examples/counter.rs).

The core idea:

```text
Registry<AppState>            — the score: hooks in dependency order
  ├─ CounterService -> Provider + Reloadable + Finalizable
  ├─ HttpService    -> Provider + Runnable
  └─ SignalService  -> Provider + Runnable

Runtime<AppState>             — the performance: live runnable tasks
  └─ spawns every provider that exposes Runnable
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

`Registry<S>` is the main piece. It stores `Arc<T>` values by `TypeId`, while
also treating them as lifecycle providers.

That means one object can be all of these at once:

- a concrete service you can resolve later: `registry.resolve::<DbService>()`
- a lifecycle participant: `boot`, `validate`
- a hot-reload participant: `Reloadable`
- a long-running background task: `Runnable`
- an end-of-life resource releaser: `Finalizable`

```rust
use std::sync::Arc;

let state = AppState::new();
state
    .registry_ref()
    .insert(Arc::new(DbService::new()))
    .insert(Arc::new(CacheService::new()));

state.registry_ref().boot_all(&state).await?;
state.registry_ref().validate_all(&state)?;

let db = state.registry_ref().resolve::<DbService>().expect("DbService registered");
```

Register once. Boot, reload, run, and resolve by type.

### The magic is boring: `Any` + `TypeId`

There is no string table, no interface token, no reflection, and no
macro behind the registry. The whole trick is two of the most
unglamorous items in the standard library:

```rust
use std::any::{Any, TypeId};
```

- **The key of a service is its type.** `insert(Arc<C>)` stores under
  `TypeId::of::<C>()` — an identifier the compiler mints, globally
  unique, impossible to typo, impossible to collide. There is no
  naming convention because there are no names.
- **One allocation, two views.** The same `Arc` is stored type-erased
  twice: as `Arc<dyn Any + Send + Sync>` for typed recovery and as
  `Arc<dyn Provider<S>>` for the lifecycle walk. No copies — both
  views share the original allocation.
- **`resolve::<T>()` is `Arc::downcast`, not reflection.** One integer
  comparison. It can only ever return the exact type that was
  inserted, or `None`. A mis-typed handle cannot exist, so the
  "wrong thing under this key, cast explodes at a distance" class of
  container bugs is deleted, not handled.
- **Asking for a non-service type does not compile.** `resolve` is
  bounded by `T: Provider<S>` — the request itself is type-checked.

Dynamic containers usually buy their flexibility with ambiguity:
string keys that drift, tokens that need registering, casts that fail
far from their cause. Rust's famously strict, famously boring type
system turns out to be the fun part — it *is* the service catalog,
and `std::any` is all the runtime it needs.

## Providers

A provider is any service that wants to join the application lifecycle.

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
        // Cheap preflight checks before boot.
        Ok(())
    }
}
```

## Lifecycle Ordering

Lifecycle order is deterministic and type-aware. Providers can express concrete
dependencies with `ProviderOrder::before::<T>()` / `ProviderOrder::after::<T>()`
instead of relying on registration order or magic priority numbers.

The same lifecycle plan is used for:

- `validate_all`
- `boot_all`
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

state.registry_ref().insert(Arc::new(CacheService));
state.registry_ref().insert(Arc::new(DbService));

let names = state.registry_ref().lifecycle_names()?;
println!("Lifecycle order: {}", names.join(" -> "));

state.registry_ref().boot_all(&state).await?;
// DbService boots before CacheService because CacheService says:
// ProviderOrder::new().after::<DbService>()
```

`boot_priority()` and `Reloadable::priority()` are still available as coarse
tie-breakers among otherwise-ready providers. Prefer `ProviderOrder` for real
dependencies. `run_priority()` controls only runtime task spawn order.

## Runnable Providers

Long-running loops live in `Runnable::run()`, not in `boot()`.
`Runnable::run()` is an async trait method and receives `self: Arc<Self>`, so
providers can be spawned without returning a boxed task future.

Graceful teardown belongs inside the same future: observe the shutdown
signal, drain your in-flight work, and return — do not split the stop path
into a separate hook.

```rust
use std::sync::Arc;
use async_trait::async_trait;
use continuo::{Provider, Result, Runnable};

#[async_trait]
impl Runnable<AppState> for CacheService {
    async fn run(
        self: Arc<Self>,
        state: AppState
    ) -> Result<()> {
        state.on_shutdown().await;
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

runtime.spawn_all(state.registry_ref(), state.clone());
state.initiate_shutdown();
runtime.wait_until_shutdown(&state).await?;
runtime.drain().await?;
# Ok::<(), continuo::Error>(())
```

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
reloadable providers.

## Finalizable Providers

Finalize is the last note: it releases non-running resources **after the
runnable tasks have drained** — externally named things whose stale presence
would break the next boot (shm segments, lock files). It is not a stop
mechanism; runnables end their own futures inside `run()`.

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
calls `finalize()` on every provider that exposes the capability. Failures are
logged and do not stop the remaining finalizers.

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
use continuo::Gate;

let gate = Gate::new(Some(1024), Duration::from_millis(100));
let permit = gate.enter().await?;

gate.graceful_shutdown(Some(Duration::from_secs(30)));
gate.wait_all_done().await;
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

One single-file example carries the whole story —
[`examples/counter.rs`](https://github.com/iadev09/continuo/blob/main/examples/counter.rs):

```sh
cargo run --example counter
curl http://127.0.0.1:3000/hit        # repeat — the count grows
curl -L http://127.0.0.1:3000/reload  # reload over HTTP — resets to 0
kill -HUP <pid>                       # same reload, via signal
# Ctrl+C — graceful drain, then finalize persists the count
```

One log tells the whole story (two browsers open — ports 51422 and
51423 each keep their own per-connection count; the application-scoped
total is shared):

```text
counter booted: starting fresh from 0 (no counter.txt yet)
signals: ready (pid 73942)
signals: reload with `kill -HUP 73942` or GET /reload — resets the counter
signals: stop with Ctrl+C or `kill -INT 73942` — drains, then finalizes
http listening on http://127.0.0.1:3000/hit (Ctrl+C to stop)
http: hit #1 (client port 51422 → its #1)
http: hit #2 (client port 51423 → its #1)
counter reloaded: reset to 0               ← SIGHUP or GET /reload
http: /reload — providers reloaded
http: hit #1 (client port 51423 → its #1)
http: hit #2 (client port 51422 → its #1)
http: hit #3 (client port 51423 → its #2)
^C
signals: Ctrl+C — initiating shutdown
http drained and stopped
counter finalize: persisted final value = 3 -> counter.txt
🏁 all services finished gracefully
```

Run it again — the count survives the process:

```text
counter booted: starting from 3 (restored from counter.txt)
http: hit #4 (client port 51425 → its #1)
```

Three providers, every capability doing real work:

- **`CounterService`** is **application-scoped**: one instance for the
  process lifetime, shared by every request. `boot()` restores the count from `counter.txt`
  with `tokio::fs`, `reload()` resets it to 0, and
  `Finalizable::finalize()` persists the final value after the
  runnables drained — the count survives restarts. It also keeps a
  per-connection count (peer address via axum's `ConnectInfo`) so the
  scope contrast is visible: connections diverge, the total is shared.
- **`HttpService`** boots after its dependency
  (`ProviderOrder::new().after::<CounterService>()`), resolves it from
  the registry **once, while building the router**, and hands it to
  handlers as an axum `Extension` — no per-request container lookups,
  no lookups on the request path. The router state is the `AppState`
  itself, so handlers like `/reload` can call `reload_all` straight
  from a request. The server serves and drains inside
  `Runnable::run()`.
- **`SignalService`** — even signal handling is a provider: Ctrl+C maps
  to shutdown, SIGHUP to `reload_all`. `main` contains no naked
  `tokio::spawn`; it only composes providers and walks the lifecycle.

axum is a dev-dependency only; the transport is an implementation
detail.

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
