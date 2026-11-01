//! Lifecycle priorities.
//!
//! Lower values order components that are otherwise ready. A
//! priority orders components that use one another without requiring one
//! another: an HTTP front that answers 503 until a backing service is up
//! prefers to start after it, but does not depend on it. A requirement goes
//! in `Component::Deps`; a layer goes in `Component::order`. A priority never
//! moves a component past either. Run priority orders task submission, not
//! scheduler polls or operational readiness.

/// Earliest priority among otherwise-ready components.
pub const FIRST: u8 = 0;
/// Prefer starting before components with normal priority.
pub const EARLY: u8 = 50;
/// Default priority when a component declares none.
pub const NORMAL: u8 = 100;
/// Prefer starting after components with normal priority.
pub const LATE: u8 = 150;
/// Latest priority among otherwise-ready components.
pub const LAST: u8 = u8::MAX;
