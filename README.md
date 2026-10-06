<p align="center">
  <img src="https://raw.githubusercontent.com/iadev09/continuo/main/assets/logo.svg" alt="continuo" width="480">
</p>

<p align="center">
  <a href="https://crates.io/crates/continuo"><img src="https://img.shields.io/crates/v/continuo.svg" alt="crates.io"></a>
  <a href="https://docs.rs/continuo"><img src="https://img.shields.io/docsrs/continuo" alt="docs.rs"></a>
  <a href="https://github.com/iadev09/continuo/blob/main/LICENSE-MIT"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="license"></a>
</p>

# Runtime service composition for Rust

Register services once. Resolve the same shared instances by Rust type.
Run, reload and stop them through an explicit lifecycle.

Continuo supplies the registry and runtime, not an application framework.
Your application owns its state, configuration, transports and domain code.

> In [Claviron](https://github.com/iadev09/claviron), Continuo handles the core
> service lifecycle: registration, dependency ordering, startup, reload and
> shutdown. [Congif](https://github.com/iadev09/congif) handles configuration
> reading and snapshot storage. These
> responsibilities live in independent crates, not in Claviron-specific APIs.

## An application can be this small

~~~rust
Application::new()?
    .with_services::<(CounterService, HttpService, SignalService)>()?
    .run()
    .await
~~~

This is the entry point of the [playground](examples/playground/main.rs).
`Application` belongs to that example: its [implementation](examples/playground/app.rs)
puts a small application API on top of Continuo. Registration and lifecycle
orchestration stay behind that API, rather than being repeated in `main.rs`.

The three services demonstrate the same model in different roles:

- **CounterService** accepts hits, restores its saved value on start and saves
  it on stop. Its increment comes from configuration.
- **HttpService** serves the playground and streams live updates with SSE.
  Reloading its configuration can replace the listening address.
- **SignalService** maps configured process signals to counter hits and reloads.
  Ctrl+C initiates application shutdown.

All three are typed providers, configurable, reloadable and runnable.

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
├── main.rs          # logging and application entry point
├── app.rs           # application composition and lifecycle
├── app/
│   ├── state.rs     # shared state and capabilities
│   ├── config.rs    # configuration types and environment input
│   ├── providers.rs
│   ├── providers/   # counter, HTTP, signals and live updates
│   └── handlers.rs
├── config/          # counter.json, http.json and signals.json
├── web/             # HTML, CSS and JavaScript
└── storage/         # persisted counter state
~~~

The HTTP configuration uses `"${HTTP_LISTEN}"`. The example reads its `.env`
and lets process environment values override it. That environment file is
not watched; configuration files are.

## Build the services underneath

| Part | Responsibility |
| --- | --- |
| `Registry<S>` | Shared provider instances, typed resolution and lifecycle ordering |
| `Register<S>` | Type-owned construction and registration |
| `Provider<S>` | Identity, dependencies and optional lifecycle capabilities |
| `Runtime<S>` | Runnable generations, control commands and shutdown |
| `ServiceManager` | A control handle for that runtime |
| Congif | Configuration reading, validation and snapshot publication |

### Resolve by type

Application code asks for the service it needs, not its construction recipe:

~~~rust
let counter = state
    .registry_ref()
    .resolve::<CounterService>()
    .ok_or_else(|| continuo::Error::msg("counter is not registered"))?;
~~~

Resolution returns an `Arc<CounterService>` pointing to the registered instance.
It does not construct another counter. Missing types return `None`; inserting
a second provider of the same concrete type is an error.

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

The playground counter registers itself with both its configuration path
and its persistence path:

~~~rust
impl Register<AppState> for CounterService {
    fn register(state: &AppState) -> continuo::Result<()> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples/playground");

        state.registry_ref().insert(Arc::new(Self::new(
            root.join("config/counter.json"),
            root.join("storage/counter.json"),
        )))?;
        Ok(())
    }
}
~~~

Tuples compose registrations in declaration order. Each tuple supports up to
eight members; nested tuples compose larger sets. A registration error stops
that sequence without undoing providers already registered.

`Runtime::with_services` invokes those registrations and installs its own
`ServiceManager`. With `config-watch` enabled, it also prepares configuration
watching. It does not boot the services.

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

Every registered service implements `Provider<S>`. Additional traits opt that
same instance into additional work:

| Trait | Capability exposed by `Provider` |
| --- | --- |
| `Configurable` | `as_configurable` |
| `Reloadable<S>` | `as_reloadable` |
| `Runnable<S>` | `as_runnable` |
| `Finalizable<S>` | `as_finalizable` |

A provider may have no running task at all. A runnable may be reloadable or
not. Implementing a capability also requires exposing it through its
corresponding `as_*` method.

`Provider::boot` prepares dependencies and resources; `validate` checks the
prepared provider. `Runnable::run` owns its long-lived work. `Reloadable::reload`
applies a change; `Finalizable::finalize` performs final cleanup.

Dependencies use Rust types:

~~~rust
fn order(&self) -> continuo::ProviderOrder {
    continuo::ProviderOrder::new()
        .after::<CounterService>()
        .after::<ServiceManager>()
        .after::<SignalService>()
}
~~~

This is the playground HTTP provider's order. Boot, validation and reload
follow the dependency plan. Finalization reverses it. Missing dependencies and
cycles are errors; priorities provide tie-breaks, not replacements for declared
dependencies. Run priority controls spawn order separately.

### Configure once, reload through the runtime

Continuo re-exports Congif's `Configurable` trait. It does not define a second
configuration contract.

A configurable owner declares its configuration type, sources and reading
logic. Validation belongs to that owner:

~~~rust
#[async_trait]
impl Configurable for CounterService {
    type Config = CounterConfig;

    fn sources(&self) -> Vec<PathBuf> {
        vec![self.config_path.clone()]
    }

    async fn configure(
        &self,
        reader: &congif::ConfigReader,
    ) -> congif::Result<Self::Config> {
        reader.read(&self.config_path).await
    }

    // validate_config rejects an increment of zero.
}
~~~

The [full implementation](examples/playground/app/providers/counter.rs)
includes validation and applies a prepared snapshot during boot and reload.

Congif owns parsers, reads and configuration snapshots. Providers own their
schema, validation and runtime application. Snapshots are indexed by owner
type, so two owners can use the same configuration type independently.

With `config-watch` enabled:

1. The runtime installs watches before provider boot.
2. Congif reports a changed source; the affected owners are resolved from the registry.
3. Their configurations are prepared and validated in dependency order.
4. A successful batch publishes its configuration snapshots together.
5. The runtime invokes the affected providers' reload capabilities.

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

For manual registry composition, `Runtime::watch_configurations` installs
watching explicitly. `ConfigurationChanged::of::<Owner>()` can also notify the
runtime through the typed event bus.

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
| `full` | All of the above |

The registry and lifecycle traits remain available without default features.
Native configuration watching supports Linux, macOS and FreeBSD.

Core dependencies are `async-trait`, Congif, Tokio, `tokio-util` and `tracing`.
`dashmap` is optional through `events`. Axum and `tracing-subscriber` are
example/dev dependencies; Continuo does not require an HTTP framework.

## Why the name?

In baroque music, the *basso continuo* plays through the whole piece while
soloists enter and leave. The registry stays available while services start,
run, reload and stop around it.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
