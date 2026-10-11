<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

# Continuo

Typed service registry and lifecycle runtime for multi-service Rust
applications.

Continuo resolves shared values by Rust type and drives registered components
through an ordered lifecycle: boot, validation, execution, reload, drain and
finalization. A component declares what it requires as a type; the registry
resolves those requirements, derives the boot order from them and hands them
to the component. Long-running work is owned by a runtime that starts, stops,
restarts and reloads individual services while the process keeps running.

Continuo is a library, not a framework. It does not depend on an HTTP stack, a
database or a configuration format, and it does not define the application's
state type. The application implements one trait on its own state and retains
ownership of everything else.

## Layout

Composition and wiring are different concerns and live in different files.
`main.rs` names the services and the inputs that can fail. `application.rs`
holds the one type that turns those inputs into a state and hands the
composition to `Runtime`.

~~~rust
// main.rs
#[tokio::main]
async fn main() -> continuo::Result<()> {
    Application::new()
        .with_environment(Environment::from_args()?)
        .with_services::<(Listener, Metrics)>()
        .run()
        .await
}
~~~

~~~rust
// application.rs
#[derive(Default)]
pub struct Application {
    environment: Environment,
    runtime: Runtime<AppState>,
}

impl Application {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_environment(mut self, environment: Environment) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_services<R: Register<AppState>>(mut self) -> Self {
        self.runtime = self.runtime.with_services::<R>();
        self
    }

    pub async fn run(self) -> Result<()> {
        let state = AppState::new(self.environment.store()?)?;
        let finalized = self.runtime.run(state).await?;
        for failure in finalized.failures() {
            eprintln!("{}: {}", failure.component(), failure.error());
        }
        Ok(())
    }
}
~~~

`with_services` records; nothing is registered, read or started until `run`.
The command line is the only input that can fail before that point, and it is
resolved where it is read. `Environment` and `AppState` are the application's
own types; the playground has a complete version of each.

## Example

The same shape written out explicitly in one file: the state, a plain value,
a component with one dependency and one capability, and the call that runs
it.

~~~rust
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use continuo::{
    Component, HasRegistry, Register, Registry, Result, RunContext, Runnable,
    Runtime, SharedState,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct AppState(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<AppState>,
}

impl SharedState for AppState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }
}

impl HasRegistry for AppState {
    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }
}

/// A plain value, injected without implementing any trait.
struct Settings {
    listen: SocketAddr,
}

impl Settings {
    /// `LISTEN` in the process environment overrides the default address.
    fn from_env() -> Result<Self> {
        let listen = match std::env::var("LISTEN") {
            Ok(value) => value.parse()?,
            Err(std::env::VarError::NotPresent) => "127.0.0.1:8080".parse()?,
            Err(error) => return Err(error.into()),
        };
        Ok(Self { listen })
    }
}

/// A component that requires `Settings` and exposes one capability.
struct Listener;

impl Register<AppState> for Listener {
    fn register(state: &AppState) -> Result<()> {
        state.registry_ref().register(Arc::new(Listener))?;
        Ok(())
    }
}

impl Component<AppState> for Listener {
    type Deps = (Arc<Settings>,);

    fn name(&self) -> &'static str {
        "listener"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, (Arc<Settings>,)> for Listener {
    async fn run(
        self: Arc<Self>,
        (settings,): (Arc<Settings>,),
        _state: AppState,
        context: RunContext,
    ) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(settings.listen).await?;
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    let (_socket, _peer) = accepted?;
                    // Serve the connection.
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = AppState(Arc::new(Inner {
        shutdown: CancellationToken::new(),
        registry: Registry::new(),
    }));
    state.registry_ref().insert(Arc::new(Settings::from_env()?))?;

    tokio::spawn({
        let state = state.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            state.initiate_shutdown();
        }
    });

    let finalized = Runtime::default()
        .with_services::<(Listener,)>()
        .run(state)
        .await?;
    for failure in finalized.failures() {
        eprintln!("{}: {}", failure.component(), failure.error());
    }
    Ok(())
}
~~~

~~~sh
LISTEN=127.0.0.1:9000 cargo run
~~~

## Registry

`Registry<S>` stores values resolvable by type and components participating
in the lifecycle. Each registration call states which of the two an object
joins.

| Call | Resolvable by type | Lifecycle participant |
| --- | --- | --- |
| `insert(Arc<T>)` | yes | no |
| `register(Arc<T>)` | yes | yes, `T: Component<S>` |

`resolve::<T>()` returns the registered `Arc<T>` or `None`. Resolution does
not construct instances. Registering a second value of the same concrete type
is an error.

`bind_dyn` binds a trait object to an already registered concrete value;
`resolve_dyn` retrieves it. No second instance is created.

~~~rust
registry.bind_dyn::<dyn Logger, ConsoleLogger>(|logger| logger)?;
let logger = registry.resolve_dyn::<dyn Logger>();
~~~

`Register<S>` is a static registration function. An implementation builds its
instance from the application state and registers it, so a service requires
neither `Default` nor an empty constructor. Tuples compose registrations in
declaration order, eight members per tuple, nested for larger sets.
`Runtime::with_services::<(A, B)>()` records the composition; `run`, `serve`
and `prepare` apply it, together with the runtime's `ServiceManager`, before
anything boots. Recording cannot fail; registration errors surface where the
registry is built.

Registration order is not dependency order. A `register` function that
resolves another component reads a registry that may not contain it yet.
Requirements are declared in `Component::Deps` and arrive resolved in `boot`
and `run`.

## Components

`Component<S>` identifies a lifecycle participant. It declares its
dependencies as `Deps`, a diagnostic `name`, an optional `order`, and exposes
capabilities through `as_*` hooks. Capabilities are independent; a component
exposes only those it implements, either on itself or on a separate object.

### Dependencies

`Deps` is a tuple of what the component requires: `Arc<T>` for a value that
must be registered, `Option<Arc<T>>` for one that may be absent, `()` for
none. The registry resolves it before `boot` and before each run generation
and passes the result in. A registered component named in `Deps` boots before
the dependent. A missing dependency is one error naming the component and
the missing type, raised before its boot hook or admitted run generation.

Runnable discovery does not resolve dependencies. Process scope and activation
policy are checked first; a manual or unavailable service can remain observable
without resources it cannot use yet. All initial automatic generations are
admitted before any task starts. Later starts and restarts bind the current DI
handles before marking the generation running; fleet dispatch shares those bound
handles across its slots. No registry lookup is added to the execution hot path.

~~~rust
impl Component<AppState> for Cache {
    type Deps = (Arc<Pool>, Option<Arc<Metrics>>);
    ...
}
~~~

A dependency is a requirement. A component that uses another service when it
is present, and serves without it, does not declare it: it resolves the value
at the point of use and answers accordingly. The HTTP service in the
playground resolves the counter per request and returns 503 until it is
registered; it declares no dependency on it.

### Ordering

`order` places a component relative to a layer or a preparation component:

~~~rust
fn order(&self) -> ComponentOrder {
    ComponentOrder::new().after::<Network>()
}
~~~

`Network` is a marker type that nothing registers. Such a type acts as a
target: a node with no work of its own, complete once every component
ordered `before` it has completed, and releasing every component ordered
`after` it. A component can therefore follow the network layer without
naming, or depending on, the components that make it up.

When boot publishes a plain DI value, `Deps` names that value but cannot infer
which component produces it. Keep an explicit `after::<Producer>()` barrier
so the value exists before the dependent boots; the producer does not become
the value's lifecycle owner.

`boot_priority` and `run_priority` order components that are otherwise
independent: services that use one another without requiring one another.
A priority never moves a component past a dependency or an `order` edge.

The registry builds one topological plan from dependencies, targets and
priorities and uses it for boot, validation and reload; finalization
traverses it in reverse. Cycles are errors. A `Bootable` may register
additional components during boot; the plan is rebuilt after each boot so
that newly declared dependencies are honoured before later components run.

| Capability | Hook | Phase |
| --- | --- | --- |
| `Bootable<S, Deps>` | `as_bootable` | finite preparation with resolved dependencies |
| `Component::validate` | built in | synchronous readiness check after boot |
| `Runnable<S, Deps>` | `as_runnable` | long-running work with resolved dependencies |
| `Reloadable<S>` | `as_reloadable` | application of a change to a live component |
| `Finalizable<S>` | `as_finalizable` | release of named resources, reverse order |
| `Configurable` | `as_configurable` | typed configuration; feature `config` |
| `Environmental` | `as_environmental` | program environments; feature `config` |

Phases run in the order register, configure, boot, validate, spawn, run,
drain, finalize. Boot prepares and finalize releases; they are inverses.
Listeners, loops and timers belong exclusively to `run`. `finalize` is not a
stop hook: a runnable completes its own work inside `run` when its context is
cancelled.

The registry stores components as `Arc<dyn AnyComponent<S>>`, a type-erased
view with dependency resolution folded in. Application code implements
`Component`; code that inspects the registry receives `AnyComponent`.

## Runtime

`Runtime<S>` owns the future of every runnable. Each start creates a numbered
generation with its own `RunContext`, a child of the process shutdown token.
A runnable observes the same cancellation path whether the process is draining
or the service alone was stopped.

`ServiceManager` is a cloneable control handle registered as an ordinary
value. It sends commands over a bounded channel to the runtime loop and holds
no state of its own.

~~~rust
let manager = state.registry_ref().resolve::<ServiceManager>().unwrap();
let services = manager.list().await?;
manager.stop("listener").await?;
manager.start("listener").await?;
manager.reload("listener").await?;
manager.restart("listener").await?;
~~~

- `start` creates a generation. It does not assert protocol readiness.
- `stop` cancels the generation and waits for it to return.
- `restart` performs `stop` followed by `start`.
- `reload` retains the generation and advances a reload revision.

A `Runnable` declares its `start_policy`: `Automatic`, `Manual`, or
`Unavailable`, which also rejects explicit starts. Snapshots report status,
policy, generation, reload revision and the most recent errors.

Commands are applied by the runtime loop sequentially. A lifecycle hook
invoked by that loop must not issue manager commands, since the reply would
require the loop that is waiting for the hook.

`run` drives the whole lifecycle: `boot_all`, `validate_all`, `spawn_all`,
wait for shutdown, `drain`, `finalize_all`. It refuses to start when
validation fails and returns the finalize outcome for the caller to inspect.
With `config-watch`, `serve` adds `watch_configurations` and `prepare` in
front of the same sequence. `run_prepared` is the sequence after boot, for an
application that prepares the registry itself. The individual steps stay
public for applications that need a different order.

Shutdown is initiated by cancelling the state's shutdown token.
`wait_until_shutdown` returns, cancels every generation and closes the
manager. `drain` waits for the runnables to complete. After successful
preparation, finalization runs even if validation, admission or execution
fails, so named resources are released before the failure is returned.
Errors during registration, configuration or boot are returned to the
application; cleanup of partial preparation remains its responsibility.
Continuo imposes no deadline; each runnable owns its
graceful drain, and `Gate` supplies admission control and a caller-defined
deadline where one is required.

## Errors

Lifecycle hooks return `continuo::Result`. Any `std::error::Error` converts
with `?`; the registry attaches the phase and component name when reporting
the failure.

- A missing dependency fails before the component's hook runs, as
  `Error::Dependency` naming the component and the type.
- Boot stops at the first failure.
- `run` and `serve` refuse to start on validation failures and return
  `Error::Invalid` carrying the complete outcome.
- Validation, reload and finalization continue past a failing component and
  return `ValidationOutcome`, `ReloadOutcome` or `FinalizeOutcome`. The outer
  `Ok` indicates that the pass ran; the outcome carries each component
  failure and is `#[must_use]`.
- A runnable failure is fatal by default: the runtime cancels the remaining
  generations, closes the manager and returns the error. `Error::recoverable`
  marks a failure the runtime logs and survives.

`continuo::Error` does not implement `std::error::Error`; the blanket `From`
that enables `?` inside hooks precludes it. `Error::into_std` converts to a
standard error that preserves the cause chain.

## State

`SharedState` is the only required trait; it returns the shutdown token.
Further capabilities are separate traits, so enabling a feature never adds a
required method to an existing implementation.

| Trait | Exposes | Feature |
| --- | --- | --- |
| `SharedState` | shutdown `CancellationToken` | always |
| `HasRegistry` | `&Registry<Self>` | `registry` |
| `HasEvents` | `&ProcessEventBus` | `events` |
| `HasConfigReader` | `&congif::ConfigReader` | `config` |
| `ReloadState` | application hook preceding `reload_all` | always |

## Configuration

Requires the `config` feature. Configuration is provided by
[Congif](https://github.com/iadev09/congif): a typed `Config` per owner, one
declared source, validation and immutable snapshots. Continuo re-exports
`Configurable` and defines no second contract.

A component declares its configuration and exposes it through
`as_configurable`. `Runtime::configure` reads and validates every declared
owner before boot; `Runtime::prepare` performs configure followed by
`boot_all`. A config owner introduced during boot without having been declared
before configuration is rejected. Components without configuration are valid.

Two hooks apply configuration changes. They are distinct:

- `Reloadable::reload` is a lifecycle command issued by the manager,
  `reload_one` or `reload_all`. The component applies the prepared snapshot;
  Continuo acknowledges the candidate to Congif after the hook succeeds.
- `ConfigObserver::config_changed` is a typed notification carrying the new
  `Config`. With `config-watch` it is invoked for a changed source.

Both may produce the same effect, and a component may implement both. One is
a command; the other is an observation.

With `config-watch` and `Runtime::watch_configurations`:

1. One native watcher covers every source of every observing owner.
2. A changed path is mapped to its owners through the registry.
3. The affected configurations are prepared and validated as one batch.
4. A rejected batch publishes nothing; previous snapshots remain in effect.
5. A successful batch publishes, then `config_changed` runs for each owner.

The runtime loop consumes these notifications directly, without an additional
task, channel or polling interval. `ConfigurationChanged::of::<Owner>()` on
the event bus requests the same path explicitly.

A component whose programs start with their own environments, such as a PHP
application or a task process, implements `Environmental`. Congif maintains
one dotenw node per program, watches those files with the owner's sources,
and the runtime invokes `environment_changed` when one is re-read.

## Events

Requires the `events` feature. `ProcessEventBus` is a process-local typed
broadcast. Any `Clone + Send + Sync` type implementing `Event` can be emitted
and subscribed to independently.

~~~rust
#[derive(Clone)]
struct CertificateReloaded;
impl continuo::Event for CertificateReloaded {}

let mut events = state.events().subscribe::<CertificateReloaded>();
state.events().emit(CertificateReloaded);
~~~

Events are delivered only to existing subscribers. Channels are bounded and
receivers must handle lag. The bus is neither storage nor a cross-process
queue.

`subscribe_filtered::<E>(predicate)` uses the same bounded receiver contract,
but evaluates the predicate synchronously during publication, before queue
admission. Rejected events cannot consume that subscription's capacity or cause
its lag. The predicate may read a shared selection, but must be cheap,
non-blocking and must not re-enter the bus. No forwarding task or polling loop
is created; only subsequent matching publications are delivered. Dropped
receivers are removed on the next publication or filtered registration.
`subscribe_filtered_with_capacity::<E>(capacity, predicate)` lets a consumer
declare its own backlog bound without resizing other channels. Capacity is a
positive count of retained events, not event types; Tokio rounds storage up to
the next power of two. Matching overflow still reports lag.

## Admission control

Requires the `support` feature. `Gate` provides admission control for a
service loop with a capacity, an acquire timeout and a graceful deadline, all
supplied by the caller. `enter` returns an RAII `Permit`; `graceful_shutdown`
stops admission and `wait_all_done` reports whether accepted work completed
within the deadline. `GuardGroup` is the smaller primitive for in-flight
counting and waiting for an empty group.

## Placement

Requires the `fleet` feature. `Scope` and `Dispatch` describe placement in
multi-process applications: which process roles may host a runnable, and
whether it runs in each eligible process, in one, or across a pool. Components
expose them through `as_scoped` or `as_dispatched`. The runtime filters local
runnables against `SharedState::process_scope` and delegates `One` and `Pool`
executions to a `Dispatcher<S>` installed with `Runtime::with_dispatcher`.
Without a dispatcher those policies are rejected rather than executed as
`Each`. Membership, leases and transport are the adapter's responsibility.

## Playground

`examples/playground` is a complete application: an HTTP server with
server-sent events, a persisted counter, Unix signal handling, typed JSON
configuration with environment variables, watched sources with validated
reload, and start, stop, restart and reload through `ServiceManager`.

~~~sh
cargo run --example playground
cargo run --example playground -- --environment local
~~~

Its `main` is the composition:

~~~rust
#[tokio::main]
async fn main() -> continuo::Result<()> {
    Application::new()
        .with_environment(Environment::from_args()?)
        .with_services::<(
            CounterService,
            OtherService,
            HttpService,
            ReloadService,
            SignalService,
        )>()
        .run()
        .await
}
~~~

`Application` is the example's composition: it holds the selected
environment and a `Runtime`, and its `run` builds the state and calls
`Runtime::serve`. The only fallible input, the command line, is resolved
where it is read. `ReloadService` requires the `ServiceManager` and declares it in
`Deps`; the HTTP and signal services use the counter when it is present and
state a start preference through `run_priority`. The printed address serves
the counter and its controls;
`/services` lists lifecycle state, generations and reload revisions. Editing
`config/counter.json` or `config/http.json` triggers validation and reload;
the HTTP service binds a replacement listener before releasing the current
one.

~~~text
examples/playground/
├── main.rs          # compose and run
├── app.rs           # Application: environment, state, Runtime::serve
├── app/
│   ├── state.rs     # AppState and its capability traits
│   ├── config.rs    # configuration types
│   ├── counter.rs   # the counter value; no Continuo dependency
│   ├── services/    # counter, http, signals, reload, other
│   └── handlers.rs  # axum handlers
├── config/          # counter.json, http.json, signals.json
├── web/             # page, styles, script
└── storage/         # persisted counter value
~~~

## Features

~~~toml
continuo = { version = "0.8", features = ["config-watch"] }
~~~

| Feature | Enables |
| --- | --- |
| `registry` (default) | `HasRegistry`, `Runtime::with_services`, `Runtime::run` |
| `config` | Congif integration: `Configurable`, `as_configurable`, `as_environmental`, reload acknowledgement; with `registry`, `Runtime::configure` and `Runtime::prepare` |
| `config-watch` | native source watching on top of `config`; includes `registry` and `events` |
| `events` | `ProcessEventBus`, `HasEvents` |
| `support` | `Gate`, `Permit`, `GuardGroup`, `Guard` |
| `fleet` | `Scope`, `Dispatch`, `Dispatcher`, `as_scoped`, `as_dispatched` |
| `full` | all of the above |

The registry, components, runtime and manager are available with no features
enabled. Core dependencies are `async-trait`, Tokio, `tokio-util` and
`tracing`; Congif, `dashmap` and `serde` are pulled in only by their features.
Native watching supports Linux, macOS and FreeBSD. Diagnostics are emitted
through `tracing`; the executable installs the subscriber.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
