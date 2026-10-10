//! # continuo
//!
//! Agnostic service composition primitives for multi-service Rust applications.
//!
//! No application state, no configuration, no HTTP, no TLS, no DB dependency.
//! Pull this crate into any Rust project with multiple services; bring your
//! own state type, implement [`state::SharedState`] and optionally
//! [`registry::ReloadState`] on it, then use [`registry::Registry`] with the
//! [`registry::Component`] / [`registry::Bootable`] / [`registry::Reloadable`] /
//! [`registry::Runnable`] / [`registry::Finalizable`] traits.
//!
//! The error model (`registry::Error`, `registry::BoxError`, `registry::Result`)
//! lives *inside* the [`registry`] module because it is a registry concern —
//! helper primitives such as [`gate`] have their own semantics and do not
//! share this error type.
#[cfg(feature = "events")]
pub mod events;
#[cfg(feature = "fleet")]
pub mod fleet;
#[cfg(feature = "support")]
pub mod gate;
#[cfg(feature = "support")]
pub mod guard;
pub mod registry;
mod runtime;
mod service;
pub mod state;
#[cfg(feature = "config-watch")]
mod watcher;

#[cfg(feature = "config")]
pub use congif::{ConfigObserver, Configurable};
#[cfg(feature = "events")]
pub use events::{Event, ProcessEventBus, ReloadRequired, ShutdownInitiated};
#[cfg(feature = "fleet")]
pub use fleet::{Dispatch, Dispatched, Scope, Scoped};
#[cfg(feature = "support")]
pub use gate::{Gate, GateDrainOutcome, Permit};
#[cfg(feature = "support")]
pub use guard::{Guard, GuardGroup};
pub use registry::{
    AnyComponent, AnyRunnable, Bootable, BoxError, Component, ComponentOrder,
    Error, Finalizable, FinalizeFailure, FinalizeOutcome, MissingDependency,
    Register, Registry, ReloadFailure, ReloadOutcome, ReloadState, Reloadable,
    Resolve, Result, Runnable, StdError, ValidationFailure, ValidationOutcome,
};
pub use runtime::Runtime;
pub use service::{
    RunContext, ServiceManager, ServiceManagerError, ServiceSnapshot,
    ServiceStartPolicy, ServiceStatus,
};
#[cfg(feature = "config")]
pub use state::HasConfigReader;
#[cfg(feature = "events")]
pub use state::HasEvents;
#[cfg(feature = "registry")]
pub use state::HasRegistry;
pub use state::SharedState;
#[cfg(feature = "config-watch")]
pub use watcher::ConfigurationChanged;
