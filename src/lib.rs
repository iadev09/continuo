//! # continuo
//!
//! Agnostic service composition primitives for multi-service Rust applications.
//!
//! No application state, no configuration, no HTTP, no TLS, no DB dependency.
//! Pull this crate into any Rust project with multiple services; bring your
//! own state type, implement [`state::SharedState`] and optionally
//! [`registry::ReloadState`] on it, then use [`registry::Registry`] with the
//! [`registry::Provider`] / [`registry::Reloadable`] /
//! [`registry::Runnable`] / [`registry::Finalizable`] traits.
//!
//! The error model (`registry::Error`, `registry::BoxError`, `registry::Result`)
//! lives *inside* the [`registry`] module because it is a registry concern —
//! helper primitives such as [`gate`] have their own semantics and do not
//! share this error type.
#[cfg(feature = "events")]
pub mod events;
#[cfg(feature = "support")]
pub mod gate;
#[cfg(feature = "support")]
pub mod guard;
pub mod registry;
mod runtime;
mod service;
pub mod state;

#[cfg(feature = "events")]
pub use events::{Event, ProcessEventBus, ShutdownInitiated};
#[cfg(feature = "support")]
pub use gate::{Gate, GateDrainOutcome, Permit};
#[cfg(feature = "support")]
pub use guard::{Guard, GuardGroup};
pub use registry::{
    BoxError, Error, Finalizable, FinalizeFailure, FinalizeOutcome, Provider, ProviderOrder,
    Registry, ReloadFailure, ReloadOutcome, ReloadState, Reloadable, Result, Runnable,
    ValidationFailure, ValidationOutcome,
};
pub use runtime::Runtime;
pub use service::{
    RunContext, ServiceManager, ServiceManagerError, ServiceSnapshot, ServiceStatus,
};
pub use state::SharedState;
