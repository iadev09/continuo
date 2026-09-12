use tokio_util::sync::CancellationToken;

#[cfg(feature = "events")]
use crate::ProcessEventBus;
#[cfg(feature = "registry")]
use crate::Registry;

/// Shared application state passed to providers and runtime tasks.
///
/// Applications keep ownership of their real state. Put config handles,
/// typed services, caches, event buses, and domain state wherever they belong;
/// this trait only says how the runtime observes process shutdown.
///
/// The recommended shape is a cheap cloneable handle around a private `Inner`.
/// This keeps state clones cheap while letting `registry_ref()` and `events()`
/// return ordinary references:
///
/// ```text
/// use std::sync::Arc;
/// use tokio_util::sync::CancellationToken;
/// use continuo::{ProcessEventBus, Registry, SharedState};
///
/// #[derive(Clone)]
/// pub struct AppState(Arc<Inner>);
///
/// struct Inner {
///     shutdown_token: CancellationToken,
///     registry: Registry<AppState>,
///     events: ProcessEventBus,
/// }
///
/// impl SharedState for AppState {
///     fn shutdown_token(&self) -> CancellationToken {
///         self.0.shutdown_token.clone()
///     }
/// }
///
/// impl HasRegistry for AppState {
///     fn registry_ref(&self) -> &Registry<Self> {
///         &self.0.registry
///     }
/// }
///
/// impl HasEvents for AppState {
///     fn events(&self) -> &ProcessEventBus {
///         &self.0.events
///     }
/// }
/// ```
pub trait SharedState: Clone + Send + Sync + 'static {
    fn shutdown_token(&self) -> CancellationToken;

    fn initiate_shutdown(&self) {
        self.shutdown_token().cancel();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown_token().is_cancelled()
    }
}

/// A state that owns a [`Registry`].
///
/// A trait of its own rather than a `#[cfg]`-gated method on [`SharedState`].
/// Cargo features are additive and unify across the whole graph, so a gated
/// *method* means any crate anywhere enabling `registry` adds a required method
/// to a trait other crates have already implemented, and their builds break at
/// a distance for a feature they never asked for. A gated *trait* adds a
/// capability instead: enabling the feature offers something new and takes
/// nothing away.
#[cfg(feature = "registry")]
pub trait HasRegistry: SharedState {
    fn registry_ref(&self) -> &Registry<Self>;
}

/// A state that owns a [`ProcessEventBus`]. Separate from [`SharedState`] for
/// the same reason as [`HasRegistry`].
#[cfg(feature = "events")]
pub trait HasEvents: SharedState {
    fn events(&self) -> &ProcessEventBus;
}
