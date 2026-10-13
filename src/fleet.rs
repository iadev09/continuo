//! Fleet placement contracts, independent of execution engines and transports.

use serde::{Deserialize, Serialize};

/// Process roles eligible to host an execution.
///
/// Worker execution is the default; master or all-role execution requires an
/// explicit selection. A combined standalone host still admits worker work.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Master,
    #[default]
    #[serde(alias = "workers")]
    Worker,
    #[serde(alias = "fleet", alias = "both")]
    Any,
}

impl Scope {
    /// Compatibility spelling; a fleet is not a separate process role.
    #[allow(non_upper_case_globals)]
    pub const Fleet: Self = Self::Any;

    /// Compatibility spelling for the worker role.
    #[allow(non_upper_case_globals)]
    pub const Workers: Self = Self::Worker;

    /// Whether this scope admits a role. Hosts combining both roles supply
    /// `Any`; adapters may implement `Into<Scope>` for process identities.
    pub fn includes(
        self,
        role: impl Into<Self>,
    ) -> bool {
        let role = role.into();
        self == Self::Any || role == Self::Any || self == role
    }
}

/// Declares eligible process roles without changing component registration.
///
/// Expose this through `Component::as_scoped` (or `as_dispatched`) so Runtime
/// can filter local entries against `SharedState::process_scope`. An explicit
/// `as_scoped` capability takes precedence when both hooks are exposed; both
/// must describe the same scope. Merely implementing the trait without
/// exposing the capability does not change placement.
pub trait Scoped {
    fn scope(&self) -> Scope;
}

pub mod dispatch;
pub use dispatch::{
    Dispatch, DispatchPolicy, DispatchRequest, Dispatched, Dispatcher,
    ServiceExecution,
};

#[cfg(test)]
#[path = "../tests/fleet/scope.rs"]
mod tests;
