<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

# Continuo

Typed runtime dependency injection and lifecycle management for multi-service
Rust applications.

Continuo resolves `Deps` from the live registry at runtime and injects the
existing shared instances into components. It automatically derives dependency
order from registered component requirements and drives boot, validation,
execution, reload, drain and finalization. Long-running work is owned by a
runtime that starts, stops, restarts and reloads individual services while the
process keeps running.

Applications supply their own state through `SharedState`. HTTP stacks,
databases and execution drivers belong to the application.

## Layout

In the playground, `main.rs` selects services and inputs; `application.rs`
builds the state and runs the composition through `Runtime`.

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

`with_services` records the composition; `run` applies it. `Application`,
`Environment` and `AppState` are defined in the playground.

## Example

A minimal composition with a shared value and one runnable component:

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

    fn name(&self) -> &str {
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

`Registry<S>` stores shared values and lifecycle components by type.

| Call | Resolvable by type | Lifecycle participant |
| --- | --- | --- |
| `insert(Arc<T>)` | yes | no |
| `register(Arc<T>)` | yes | yes, `T: Component<S>` |

`resolve::<T>()` returns the registered `Arc<T>` or `None`. Resolution does
not construct instances. Registering a second value of the same concrete type
is an error.

`bind_dyn` exposes a registered value through a trait object;
`resolve_dyn` retrieves the same instance.

~~~rust
registry.bind_dyn::<dyn Logger, ConsoleLogger>(|logger| logger)?;
let logger = registry.resolve_dyn::<dyn Logger>();
~~~

`Register<S>::register` constructs and registers a component using application
state. Tuples compose registrations in declaration order, eight members per
tuple, nested for larger sets.
`Runtime::with_services::<(A, B)>()` records the composition; `run`, `serve`
and `prepare` apply it, together with the runtime's `ServiceManager`, before
anything boots. Registration errors are returned when the composition is applied.

Declare requirements in `Component::Deps`; their resolved values arrive in
`boot` and `run` after registration.

## Components

`Component<S>` identifies a lifecycle participant. It declares its
dependencies as `Deps`, a diagnostic `name`, an optional `order`, and exposes
capabilities through `as_*` hooks. Capabilities are independent; a component
exposes only those it implements, either on itself or on a separate object.

### Dependencies

`Deps` is a tuple of what the component requires: `Arc<T>` for a value that
must be registered, `Option<Arc<T>>` for one that may be absent, `()` for
none. Rust checks the dependency types; Continuo resolves their instances at
runtime, before `boot` and before each admitted run generation, and passes
them in. The registry also builds the dependency order at runtime: a registered
component named in `Deps` boots before its dependent, without a separate
ordering declaration. A missing dependency is one error naming the component
and the missing type, raised before its boot hook or admitted run generation.
`Option<Arc<T>>` adds an ordering edge when `T` is registered.

Process scope and activation policy are checked before dependency resolution.
All initial automatic generations are admitted before any task starts. Each
start or restart binds the current DI handles; execution uses those handles
directly, including across fleet dispatch slots.

~~~rust
impl Component<AppState> for Cache {
    type Deps = (Arc<Pool>, Option<Arc<Metrics>>);
    ...
}
~~~

### Ordering

Use `order` for additional preparation steps or groups:

~~~rust
fn order(&self) -> ComponentOrder {
    ComponentOrder::new().after::<Network>()
}
~~~

An unregistered marker such as `Network` groups components ordered `before`
and `after` it. All components before the marker complete before those after it.

When boot publishes a plain DI value, `Deps` names that value but cannot infer
which component produces it. Keep an explicit `after::<Producer>()` barrier
so the value exists before the dependent boots.

`boot_priority` and `run_priority` choose among components whose dependency
and `ComponentOrder` constraints are already satisfied. Lower values go first.
`FIRST`, `EARLY`, `NORMAL`, `LATE` and `LAST` are convenience values;
`NORMAL` is the default. Boot priority orders preparation; run priority orders
submission of runnable futures.

The registry derives lifecycle and runnable-submission plans from the same
dependencies and targets, using each phase's priorities. Boot, validation and
reload use lifecycle order; finalization traverses it in reverse. Cycles are
errors. The plan is rebuilt when boot registers additional components.

| Capability | Hook | Phase |
| --- | --- | --- |
| `Bootable<S, Deps>` | `as_bootable` | finite preparation with resolved dependencies |
| `Component::validate` | built in | synchronous readiness check after boot |
| `Runnable<S, Deps>` | `as_runnable` | long-running work with resolved dependencies |
| Named runnable instances | `runnables` | multiple operational instances from one component |
| `Reloadable<S>` | `as_reloadable` | application of a change to a live component |
| `Finalizable<S>` | `as_finalizable` | release of named resources, reverse order |
| `Configurable` | `as_configurable` | typed configuration; feature `config` |
| `Environmental` | `as_environmental` | program environments; feature `config` |

Phases run in the order register, configure, boot, validate, spawn, run,
drain, finalize. Listeners, loops and timers belong to `run`; cancellation
initiates their drain. `finalize` releases prepared resources afterward.

The registry stores components as `Arc<dyn AnyComponent<S>>`, a type-erased
view with dependency resolution folded in. Application code implements
`Component`; code that inspects the registry receives `AnyComponent`.

### Named instances

One component remains one typed DI registration and configuration owner. It
can declare several operational instances of the same `Runnable` type:

~~~rust
fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
    self.listeners.iter().map(|listener| {
        RunnableInstance::new(listener.name.clone(), listener.clone())
    }).collect()
}
~~~

Names are owned and unique across runnable instances in the process. Each
instance has its own start policy, generation, cancellation and management
entry. Its typed dependencies are resolved before admission; its own
`execution_spec` selects execution placement. Component `Deps` and `order` define
ordering between owners; instance enumeration preserves the factory's order.

Factories retain unchanged runnable allocations; enumerating them starts no
work. After configuration changes, `ServiceManager::reconcile()` admits new
automatic instances and cancels removed ones, retaining their ownership until
drain completes. Replacing a live allocation under the same name is rejected:
stop it first, or keep its handle and apply the new settings through it.
Reloading an instance applies its owner's shared configuration/reload hook.
Snapshots distinguish `name()` from the owning `component()`.

The default `runnables` implementation wraps `as_runnable` as one instance
named after its component. The [instances example](examples/instances/main.rs)
shows a configuration owner creating multiple jobs of one type. Run
`cargo run --example instances`, edit its `config.json` to add or remove a job,
then press Ctrl+C to drain all instances.

## Runtime

`Runtime<S>` owns the future of every runnable. Each start creates a numbered
generation with its own `RunContext`, a child of the process shutdown token.
A runnable observes the same cancellation path whether the process is draining
or the service alone was stopped.

`RunContext::identity()` exposes the instance's management name and generation
as a `RunIdentity`. Execution adapters, child contexts and tracked tasks receive
the same identity. Restart creates a new generation; retained contexts keep the
old one. `RunContext::new` creates an unmanaged context whose identity is `None`.
Downstream diagnostics and metrics can use `instance()` and `generation()` as
labels without resolving the manager.

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

- `start` creates a generation.
- `stop` cancels the generation and waits for it to return.
- `restart` performs `stop` followed by `start`.
- `reload` applies configuration, reconciles availability and advances a reload revision.

A `Runnable` declares its `start_policy`: `Automatic`, `Manual`, or
`Unavailable`, which also rejects explicit starts. Snapshots report status,
policy, generation, reload revision and the most recent errors.

A successful reload from `Unavailable` to `Automatic` starts the service,
including a service disabled at process startup. Becoming `Unavailable`
cancels the current generation through its normal graceful drain. Re-enabling
while it drains queues one successor, which starts only after the old generation
returns. An explicit stop clears that pending start. An unchanged policy never
restarts an operator-stopped service, and `Manual` services remain explicit-start.
Existing running generations keep their captured dispatch policy until drain.

The same reconciliation follows `Registry::reload_one`, `Registry::reload_all`
and runtime-managed configuration notifications. Registry reloads use the
registered `ServiceManager` when present, so its runtime command loop must be
running; registries without a manager remain usable independently. Scope-excluded
components do not start in this process. Failed reloads do not reconcile policy.
`reload_hook_one` remains a hook-only operation with no runtime management.

Commands are applied by the runtime loop sequentially. A lifecycle hook
invoked by that loop must not issue manager commands, since the reply would
require the loop that is waiting for the hook.

`run` drives the whole lifecycle: `boot_all`, `validate_all`, `spawn_all`,
wait for shutdown, `drain`, `finalize_all`, `finalize_execution`. It refuses to
start when validation fails and returns the finalize outcome for the caller to inspect.
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
`Gate` supplies admission control and a caller-defined deadline where required.

### Execution

`Runnable::execution_spec` selects executor placement and poll scheduling for
one generation. The default uses ambient Tokio execution. Configuration, boot,
validation, reload and finalization use the lifecycle executor.

Applications install an `ExecutionDriver` before runnable submission:

~~~rust
let runtime = Runtime::<AppState>::default().with_execution_driver(driver)?;
~~~

Return a typed specification from the runnable's `execution_spec` hook:

~~~rust
use continuo::{ExecutionDomain, ExecutionSpec, ExecutorSpec};

struct Foreground;

let execution = ExecutionSpec::default().with_executor(
    ExecutorSpec::Shared(ExecutionDomain::of::<Foreground>()),
);
~~~

Placement is `Inherit`, a typed `Shared` domain, or a `Dedicated` executor with
declared thread topology. Scheduling is inherited, priority-based, or weighted.
The installed driver implements these policies and rejects unsupported
combinations.
Application-specific policies can convert into `ExecutionSpec` with `From`.

`ExecutionDomain::of::<Marker>()` identifies a shared domain by type.
`ExecutionDomain::named::<Marker>(name)` adds an owned configuration name
within that type. Instances selecting the same domain share an execution owner.

The runtime prepares all initial automatic generations before spawning any.
Each start or restart samples a new specification; reload does not move running
work. `RunContext` retains the prepared adapter and cancellation scope, and
`child()` preserves the execution policy. Services pass the prepared adapter
to child work that needs the same custom scheduling.

Shutdown drains runnable futures and driver-owned executions first. Provider
finalization follows while shared executors remain alive for pooled I/O cleanup.
Finally, `finalize_execution` releases executor owners. `run`, `run_prepared`
and `serve` drive that sequence; manual compositions call the final step after
`Registry::finalize_all`, including when provider cleanup reports failures.
A targeted service stop or restart does not finalize shared executors.

### Tracked tasks

Use `RunContext::spawn_tracked` for child work owned by a runnable execution:

~~~rust
let task = context.spawn_tracked(|child| async move {
    child.cancelled().await;
    // Complete owned cleanup before returning.
    Ok(())
})?;
~~~

The child uses the generation's prepared execution adapter and its own
cancellation token. The adapter is reused, including its scheduling policy.
`PreparedExecution::execute` owns the root; `execute_child` submits concurrent
children. Generation-owned adapters retain their reactor through child drain.

When the runnable returns, fails or unwinds, admission closes, children receive
cancellation, and completion waits for their futures to be destroyed. Stop and
restart acknowledge that same completion boundary. Fleet slots drain their own
children independently; the generation retains all outstanding work.

Tracking uses Tokio's `TaskTracker`, created on first submission. The returned
`JoinHandle<Result<()>>` carries task results and panics; dropping the handle
keeps its work tracked. Retained contexts reject submissions after cancellation
or completion. Direct `tokio::spawn` calls remain caller-owned.

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
`Configurable`.

A component declares its configuration and exposes it through
`as_configurable`. `Runtime::configure` reads and validates every declared
owner before boot; `Runtime::prepare` performs configure followed by
`boot_all`. A config owner introduced during boot without having been declared
before configuration is rejected.

Configuration changes use two hooks:

- `Reloadable::reload` is a lifecycle command issued by the manager,
  `reload_one` or `reload_all`. The component applies the prepared snapshot;
  Continuo acknowledges the candidate to Congif after the hook succeeds.
  Registry commands obtain the catalog from `ReloadState::reload_config_reader`.
  `reload_one` requires `ReloadState` and prepares only the addressed owner;
  it does not invoke the application-wide `ReloadState::reload` hook.
  Callers that manage preparation and acknowledgement themselves can use
  `reload_hook_one`, which requires no `ReloadState` and invokes only the hook.
- `ConfigObserver::config_changed` is a typed notification carrying the new
  `Config`. With `config-watch` it is invoked for a changed source.

With `config-watch` and `Runtime::watch_configurations`:

1. One native watcher covers every source of every observing owner.
2. A changed path is mapped to its owners through the registry.
3. The affected configurations are prepared and validated as one batch.
4. A rejected batch publishes nothing; previous snapshots remain in effect.
5. A successful batch publishes, then `config_changed` runs for each owner.

The runtime loop consumes these notifications. Emit
`ConfigurationChanged::of::<Owner>()` on the event bus to request the same path.

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

Events are delivered to existing subscribers through bounded channels.
Receivers must handle lag.

`subscribe_filtered::<E>(predicate)` uses the same bounded receiver contract,
but evaluates the predicate synchronously during publication, before queue
admission. Rejected events cannot consume that subscription's capacity or cause
its lag. The predicate may read a shared selection, but must be cheap,
non-blocking and must not re-enter the bus. Dropped
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

`Dispatched::dispatch_policy()` optionally supplies the service's prepared
`DispatchPolicy`: pool capacity, lease TTL and renewal interval. `Each` needs
no policy; a lease-based adapter rejects `One`/`Pool` without one. The runtime
passes the value in `DispatchRequest` and each generation keeps its own
immutable snapshot. The policy is transport-neutral.
Task managers may retain their own task-level policy and controller.

`ServiceSnapshot::status()` describes the generation controller, not slot
ownership. `active_executions()` counts actual runnable futures, including
draining executions until they return. With `fleet`, `dispatch()` reports the
generation's placement choice. A running `One`/`Pool` controller with zero
executions is waiting. The census is generation-owned and is decremented on
return, error, panic or future cancellation; it does not claim protocol readiness.

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

`Application` holds the selected environment and a `Runtime`; its `run` builds
the state and calls `Runtime::serve`. `ReloadService` declares `ServiceManager`
in `Deps`. The HTTP and signal services set their start order through
`run_priority`. The printed address serves the counter and its controls;
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
continuo = { version = "0.11", features = ["config-watch"] }
~~~

| Feature | Enables |
| --- | --- |
| `registry` (default) | `HasRegistry`, `Runtime::with_services`, `Runtime::run` |
| `config` | Congif integration: `Configurable`, `as_configurable`, `as_environmental`, reload acknowledgement; with `registry`, `Runtime::configure` and `Runtime::prepare` |
| `config-watch` | native source watching on top of `config`; includes `registry` and `events` |
| `events` | `ProcessEventBus`, `HasEvents` |
| `support` | `Gate`, `Permit`, `GuardGroup`, `Guard` |
| `fleet` | `Scope`, `Dispatch`, `Dispatcher`, `as_scoped`, `as_dispatched` |

The registry, components, runtime and manager are available with no features
enabled. Core dependencies are `async-trait`, Tokio, `tokio-util` and
`tracing`; Congif, `dashmap` and `serde` are pulled in only by their features.
Native watching supports Linux, macOS and FreeBSD. Diagnostics are emitted
through `tracing`; the executable installs the subscriber.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
