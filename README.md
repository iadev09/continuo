<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

# Runtime service composition for Rust

Compose services once. Inject shared values by Rust type.
Manage execution and cleanup through a separate, explicit lifecycle.

Continuo supplies the registry and runtime, not an application framework.
Your application owns its state, configuration, transports and domain code.

> In [Claviron](https://github.com/iadev09/claviron), Continuo handles the core
> service lifecycle: registration, dependency ordering, startup, reload and
> shutdown. [Congif](https://github.com/iadev09/congif) handles configuration
> reading and snapshot storage. These
> responsibilities live in independent crates, not in Claviron-specific APIs.

## An application can be this small

~~~rust
// Capture the startup environment before starting runtime threads.
let environments = unsafe { app::environment()? };
Application::new(environments).await?
    .with_services::<(CounterService, OtherService, HttpService, ReloadService, SignalService)>()?
    .run()
    .await
~~~

This is the entry point of the [playground](examples/playground/main.rs).
`Application` belongs to that example: its [implementation](examples/playground/app.rs)
puts a small application API on top of Continuo. Registration and lifecycle
orchestration stay behind that API, rather than being repeated in `main.rs`.

The services demonstrate different responsibilities:

- **CounterService** accepts hits, restores its saved value during boot and saves
  it on stop. Its increment comes from configuration.
- **HttpService** serves the playground and streams live updates with SSE.
  Reloading its configuration can replace the listening address.
- **SignalService** maps configured process signals to counter hits and reloads.
  Ctrl+C initiates application shutdown.

`OtherService` observes the same source through its own config declaration;
it has no long-running work and therefore implements no `Runnable`.
`ReloadService` consumes application reload commands.

The [counter](examples/playground/app/counter.rs) is only a number. It has no
configuration, files, HTTP or lifecycle traits; `insert` makes it a plain DI
value. The existing services implement `Register` and their lifecycle
capabilities directly, without separate registration objects.

`CounterService` demonstrates runtime activation, config application and
persistence around that number. Its finite `Bootable` hook restores the value;
each run generation admits hits and saves on stop. Web editing belongs to
[handlers](examples/playground/app/handlers.rs), while client statistics and SSE
presentation belong to [application state](examples/playground/app/state.rs).
HTTP injects the same counter handle into its routes. HTTP and signals consume
the reader's validated settings when a generation starts, retain operational
settings in that generation, and hand replacements to the running loop.
No service needs `OnceLock` or an empty initialization hook.

### Try the playground

~~~sh
cargo run --example playground
~~~

Open [http://127.0.0.1:3000/](http://127.0.0.1:3000/).
Hit the counter, edit its configuration and control the service without
refreshing the page. [The service list](http://127.0.0.1:3000/services) shows
lifecycle state, generations and reload revisions.

Edit the JSON files directly, too: the same watcher handles both an editor
save and a change submitted by the page.

~~~text
examples/playground/
├── main.rs          # one environment call and application entry point
├── app.rs           # application composition and lifecycle
├── app/
│   ├── state.rs     # shared state and capabilities
│   ├── config.rs    # configuration types and environment input
│   ├── environment.rs # startup environment and diagnostics
│   ├── services.rs
│   ├── counter.rs  # ordinary number; no runtime or web dependency
│   ├── services/   # direct lifecycle integrations and live resource handoff
│   └── handlers.rs
├── config/          # counter.json, http.json and signals.json
├── web/             # HTML, CSS and JavaScript
└── storage/         # persisted counter state
~~~

The HTTP JSON configuration uses `"$HTTP_LISTEN"`. dotenw captures the process
environment at startup and prepares the example's base and service-specific
environment snapshots. `--environment NAME` selects the declared environment;
the default is `production`. Configuration watching belongs to the runtime.

## Build the services underneath

| Part | Responsibility |
| --- | --- |
| `Registry<S>` | Separate DI bindings and ordered lifecycle registrations |
| `Register<S>` | Static application composition; may register several objects |
| `Component<S>` | Lifecycle identity, dependencies and capability exposure |
| `Bootable<S>` | Finite preparation; may publish fully initialized services |
| `Runtime<S>` | Runnable generations, control commands and shutdown |
| `ServiceManager` | A control handle for that runtime |
| Congif | Configuration reading, validation and snapshot publication |

### Resolve by type

Application code asks for the service it needs, not its construction recipe.
Ordinary values need only `Any + Send + Sync`; no lifecycle trait is required:

~~~rust
registry.insert(Arc::new(settings))?; // DI only
let settings = registry.resolve::<Settings>();
~~~

For the prepared playground service:

~~~rust
let counter = state
    .registry_ref()
    .resolve::<CounterService>()
    .ok_or_else(|| continuo::Error::msg("counter is not registered"))?;
~~~

Resolution returns an `Arc<CounterService>` pointing to the registered instance.
It does not construct another counter. Missing types return `None`; inserting
a second DI value of the same concrete type is an error.

A provider can expose an interface without creating another instance.
`bind_dyn` binds an interface to an existing concrete provider, and
`resolve_dyn` retrieves that view:

~~~rust
registry.bind_dyn::<dyn Logger, ConsoleLogger>(|provider| provider)?;
let logger = registry.resolve_dyn::<dyn Logger>();
~~~

Here `Logger` and `ConsoleLogger` are application-defined types; the concrete
provider must already be registered. `with_typed` also permits a closure to
inspect a concrete provider without taking an owned handle.

### Let each service register itself

`Register<S>` is a static registration function, not a constructor requirement.
A service does not need `Default`. It can take paths, settings or dependencies
from application state when it builds its instance.

The playground binds its ordinary counter and registers its lifecycle
integration. `CounterService::register` follows this shape:

~~~rust
state.reader().bind_environment::<Self>(
    state.environment(&state.application_config().counter.env_file)?,
)?;
state.registry_ref().insert(state.counter().clone())?; // plain Counter
state.registry_ref().register(Arc::new(Self::new(
    state.application_config().counter.config_path.clone(),
    config::root().join("storage/counter.json"),
    state.counter().clone(),
    state.counter_updates().clone(),
)))?;
~~~

The [complete service](examples/playground/app/services/counter.rs) exposes
only its needed capabilities. No separate registration struct or second
provider lifecycle is involved.

These registration operations have distinct meanings:

| Operation | DI lookup | Lifecycle |
| --- | --- | --- |
| `insert(Arc<T>)` | Yes; no `Component` bound | No |
| `register(Arc<T>)` | Yes | Yes; `T: Component<S>` |
| `register_lifecycle(Arc<T>)` | No | Yes; `T: Component<S>` |

Migration: `Provider` and `ProviderOrder` remain trait/type aliases for existing
implementations. Old lifecycle calls to `insert` must become `register`;
`insert` no longer enrolls a value in lifecycle orchestration implicitly.
Every component must declare its diagnostic `name`. Preparation implements
`Bootable::boot` and is exposed through `Component::as_bootable`; there is no
boot hook on `Component`, including when using the `Provider` alias.
Failure reports identify the participant through `component()`.

Tuples compose registrations in declaration order. Each tuple supports up to
eight members; nested tuples compose larger sets. A registration error stops
that sequence without undoing providers already registered.

`Runtime::with_services` invokes those registrations and installs its own
`ServiceManager`. It does not prepare configuration, install watching or boot
the services. Enabling `config-watch` does not change its state requirements
or registration behavior.

### Keep application state yours

`SharedState` describes how the runtime observes shutdown. Other traits add
capabilities independently:

| State trait | Exposes |
| --- | --- |
| `SharedState` | A process shutdown `CancellationToken` |
| `HasRegistry` | `&Registry<Self>` |
| `HasEvents` | `&ProcessEventBus` |
| `HasConfigReader` | `&congif::ConfigReader` |
| `ReloadState` | An application-level hook before `reload_all` |

A cheap cloneable handle around `Arc<Inner>` is enough. These are separate
traits: enabling a feature does not add required methods to an existing
`SharedState` implementation.

See the complete [playground state](examples/playground/app/state.rs).

### Expose only the lifecycle hooks you need

Only lifecycle participants implement `Component<S>`. Their capability hooks
can expose themselves or separate objects. Injected services need not implement
it, and `Runnable`, `Reloadable`, `Finalizable` and `Bootable` do not inherit it.

| Trait | Capability exposed by `Component` |
| --- | --- |
| `Bootable<S>` | `as_bootable` |
| `Configurable` | `as_configurable` |
| `Environmental` | `as_environmental` |
| `Reloadable<S>` | `as_reloadable` |
| `Runnable<S>` | `as_runnable` |
| `Finalizable<S>` | `as_finalizable` |

A provider may have no running task at all. A runnable may be reloadable or
not. Implementing a capability also requires exposing it through its
corresponding `as_*` method.

`Bootable::boot` prepares dependencies and resources; `Component::validate`
checks readiness. `Runnable::run` owns its work and graceful drain.
`Reloadable::reload` applies a change; `Finalizable::finalize` releases prepared,
non-running resources. A finalizer does not require a runnable or DI binding.
The existing `Component::boot` hook remains for source compatibility; when
`as_bootable` is present, only that capability is called.

Dependencies use Rust types:

~~~rust
fn order(&self) -> continuo::ComponentOrder {
    continuo::ComponentOrder::new()
        .after::<CounterService>()
        .after::<SignalService>()
}
~~~

This is the playground HTTP provider's order. Boot, validation and reload
follow the dependency plan. Finalization reverses it. Cycles are errors;
ordering hints for absent types are ignored. Priorities provide tie-breaks, not replacements for declared
dependencies. Run priority controls spawn order separately.

### Configure once, reload through the runtime

Continuo re-exports Congif's `Configurable` trait. It does not define a second
configuration contract.

For explicit broadcast reloads, a state can expose its reader through
`ReloadState::reload_config_reader`. `Registry::reload_all` prepares and validates
all registered config owners before invoking any state or provider reload hook.
It acknowledges each exact candidate only after its provider succeeds. Rejected
preparation runs no hooks; failed resource application remains retryable. An
explicit reload still invokes every reloadable provider, even if its config is
unchanged. File notifications retain the changed-owner-only path.

`Runtime::configure` also binds the state's reader for targeted `ServiceManager`
reloads. Reading, validation and successful-application acknowledgement work
without a native watcher or the `config-watch` feature. A configurable service
cannot silently reload stale configuration when no reader has been bound;
that request returns an error. Watching only supplies automatic invalidations.

`Runtime::prepare` configures the declared owners and boots the dependency
plan. A wrapper must declare its configuration before boot; its newly created
service can expose the same owner without another read or startup callback.
Publishing an undeclared configuration owner during boot is rejected rather
than allowing an unconfigured service to run. Preparation does not spawn tasks
or validate readiness. Applications with readiness checks call `validate_all`
before `spawn_all`; this playground needs only config validation and fallible
boot construction, so it does not install empty readiness hooks.

A configurable component declares its configuration type and one source.
Congif reads and validates the batch before invoking startup callbacks:

~~~rust
#[async_trait]
impl Configurable for CounterService {
    type Config = CounterConfig;

    fn as_config_observer(&self) -> Option<&dyn congif::ConfigObserver<Config = Self::Config>> {
        Some(self)
    }

    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.config_path.clone(),
            format: congif::Format::Json,
        }
    }

    async fn configure(&self, config: &CounterConfig) -> congif::Result<()> {
        self.apply_config(config);
        Ok(())
    }

    // validate_config rejects an increment of zero.
}
~~~

The startup `configure` callback is optional. Sources consumed directly when a
generation starts, such as the playground's HTTP settings, need no empty apply
hook. Congif retains their validated snapshots.

Implement `congif::ConfigObserver::config_changed(&Self::Config)` to apply the
new typed candidate without reading sources again. Owners returning `None`
are still configurable and manually reloadable, but are not watched.

The [counter lifecycle integration](examples/playground/app/services/counter.rs)
owns this configuration and applies typed candidates directly to the
playground's display settings. The number itself does not know configuration
exists. Watching addresses the config owner, independently of DI.

Congif owns parsers, reads and configuration snapshots. Providers own their
schema, validation and runtime application. Snapshots are indexed by owner
type, so two owners can use the same configuration type independently.

With `config-watch` enabled and `Runtime::watch_configurations` called:

1. The runtime installs watches for owners exposing a config observer.
2. Congif reports a changed source; the affected owners are resolved from the registry.
3. Their configurations are prepared and validated in dependency order.
4. A successful batch publishes its configuration snapshots together.
5. The runtime invokes the affected owners' typed `config_changed` hooks.

Manual service reloads continue to call `Reloadable::reload`. A file notification
does not need an `EnvObserver`, a per-provider channel or another runtime task.

A provider whose programs start with environments of their own — a PHP
application, a task's process — implements Congif's `Environmental` and exposes
it through `as_environmental`. After `configure` and after every reload the
runtime hands its declarations to the reader, which keeps one dotenw node per
program; the program's files join the owner's watch registration, a change is
read by the same batch as a configuration change, and the runtime calls
`environment_changed(program, snapshot)` on the owner. The owner decides what
follows; nothing restarts on its own.

Watching shares native source watches; providers do not each create a polling
loop or a configuration notification channel. Runtime consumption starts after
`spawn_all`. A stopped runnable does not stop configuration watching.

An invalid configuration retains the previous snapshot catalog. Applying
runtime resources remains a provider responsibility: configuration publication
is not a transaction across every socket, pool or external resource.

The playground HTTP provider binds a replacement listener before giving up
its working listener. A rejected bind leaves the old address serving. A
successful replacement drains the old listener, including SSE connections,
and reports the new address in the console.

`Runtime::watch_configurations` installs watching explicitly after registration
and before preparation, whether using `with_services` or manual composition.
`ConfigurationChanged::of::<Owner>()` can also notify the runtime through the
typed event bus.

### Control live services

Resolve the manager from a handler or another task while the runtime's control
loop is running:

~~~rust
let manager = state
    .registry_ref()
    .resolve::<ServiceManager>()
    .ok_or_else(|| continuo::Error::msg("service manager is not registered"))?;

let services = manager.list().await?;
let stopped = manager.stop("counter").await?;
let started = manager.start("counter").await?;
let reloaded = manager.reload("counter").await?;
let restarted = manager.restart("counter").await?;
~~~

The manager sends commands to the runtime; it does not maintain a second
service registry. Do not wait for its commands from the same task that must
service the runtime control loop.

- **Start** creates a runnable generation; it is not a protocol-readiness guarantee.
- **Stop** requests cancellation and waits for that generation to return.
- **Restart** stops the current generation, then starts a new one.
- **Reload** keeps the generation and tracks a separate reload revision.

Snapshots report status, start policy, generation and reload outcome. They are
lifecycle state, not a metrics subsystem. Only managed runnables appear in the
manager's service list.

`ServiceStartPolicy` supports automatic startup, manual startup and an
unavailable state that rejects explicit starts. The runtime remains the sole
owner of running futures.

### Own the shutdown contract

A runnable receives `RunContext` with its generation's cancellation token.
It decides how to stop accepting work and finish what it already accepted.
Continuo does not invent a shutdown timeout.

The application normally boots and validates the registry, spawns runnables,
services runtime commands until shutdown, drains them, then finalizes providers.
The [playground application](examples/playground/app.rs) shows that orchestration
behind its `run` method.

Boot stops on its first failure. Validation, reload and finalization return
outcomes containing provider failures; callers must inspect those outcomes,
not treat an outer `Ok` as proof that every provider succeeded.
`reload_all` first invokes the application's `ReloadState` hook.

With `support` enabled, `Gate` adds admission control and RAII `Permit` tracking.
It can stop new admissions and wait for accepted work to finish. Capacity,
acquisition timeout and graceful deadline are explicit inputs.
`GuardGroup` is the smaller counting primitive when only in-flight tracking
and waiting for an empty group are needed.

### Publish typed events

The optional `ProcessEventBus` carries typed, cloneable events within a
process:

~~~rust
#[derive(Clone)]
struct CounterChanged {
    value: u64,
}

impl continuo::Event for CounterChanged {}

let mut changes = state.events().subscribe::<CounterChanged>();
state.events().emit(CounterChanged { value: 42 });
let event = changes.recv().await?;
~~~

Subscribe before publishing events you need to observe. Channels are bounded;
receivers must handle lag. This is an in-process bus, not durable storage or
a cross-process job queue.

### Choose how diagnostics are collected

Continuo uses `tracing` to emit diagnostics. The executable installs its
subscriber and chooses filtering, formatting and output.

The playground uses `tracing-subscriber`. A rejected configuration reload is
visible there without turning an invalid edit into application shutdown.
The library does not install a global logger for its callers.

## Features and dependencies

~~~toml
continuo = { version = "0.8", features = ["config-watch"] }
~~~

| Feature | Enables |
| --- | --- |
| `registry` (default) | The `HasRegistry` state capability |
| `events` | `ProcessEventBus` and `HasEvents` |
| `config-watch` | Runtime configuration watching; includes `registry`, `events` and Congif's watcher |
| `support` | `Gate`, `Permit`, `GuardGroup` and `Guard` |
| `fleet` | Transport-neutral placement enums/traits and provider scope/dispatch hooks; no transport dependency |
| `full` | All of the above |

The registry and lifecycle traits remain available without default features.
Native configuration watching supports Linux, macOS and FreeBSD.

Core dependencies are `async-trait`, Congif, Tokio, `tokio-util` and `tracing`.
`dashmap` is optional through `events`. Axum and `tracing-subscriber` are
example/dev dependencies; Continuo does not require an HTTP framework.

The `fleet` feature supplies `Scope`, `Dispatch`, `Scoped` and `Dispatched`,
not a fleet runtime. An application adapter owns membership, target selection
and transport; `ServiceManager` continues to manage local executions.
Providers expose scope through `as_scoped`, whose default is `None`.
Runtime filters local runnable entries against `SharedState::process_scope`
before adding management records. An absent scope means `Worker`, with
`Dispatch::Each`: master execution requires an explicit `Master` or `Any`
scope. The state's default role remains `Any`, appropriate for a combined
single-process host, so standalone hosts still execute worker-scoped work.
Excluded providers remain registered and participate in provider lifecycle;
they have no local service-list or start/stop/reload management entry.
Scope is sampled at initial runnable submission; live placement changes and
distributed dispatch are not implemented by this hook.

`Provider::as_dispatched` exposes optional `Dispatched` metadata; its default
`None` means `Dispatch::Each`. A dispatch capability also declares its scope
through `Dispatched: Scoped`. Scope-only providers may expose `as_scoped`;
dispatch-capable providers expose `as_dispatched`. The fleet adapter, not
Continuo, owns selection and coordination. No dispatch policy starts work or
changes provider configuration by itself.

Install a `fleet::Dispatcher<S>` with `Runtime::with_dispatcher` to execute
`One` or `Pool` services. Without an adapter those policies are rejected
before initial services start; they never silently fall back to `Each`.
The adapter validates its policy synchronously, then owns selection for the
whole generation through its asynchronous `run` method. It receives a
`DispatchRequest`, a cloneable `ServiceExecution<S>` capability and the
generation's `RunContext`.

Runtime owns the controller future and local start/stop/restart commands.
The adapter owns slot acquisition and renewal: acquire before executing,
cancel local work when ownership is lost, and retain ownership through drain.
It must await every child execution before returning. Pool capacity and lease
policy are explicit adapter configuration, not inferred runtime defaults.
`Running` describes the managed controller generation, even while it waits
for fleet ownership; it does not claim that this process currently owns a slot.
Transport implementations belong outside Continuo.

## Why the name?

In baroque music, the *basso continuo* plays through the whole piece while
soloists enter and leave. The registry stays available while services start,
run, reload and stop around it.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
