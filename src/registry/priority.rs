//! Lifecycle priorities.
//!
//! Lower values run earlier among components that are otherwise ready. A
//! priority orders components that use one another without requiring one
//! another: an HTTP front that answers 503 until a backing service is up
//! prefers to start after it, but does not depend on it. A requirement goes
//! in `Component::Deps`; a layer goes in `Component::order`. A priority never
//! moves a component past either.

/// Reserved floor for workspace-internal root components.
///
/// Ordinary components should use `EARLY`, `NORMAL`, `LATE`, or explicit
/// `ComponentOrder` edges instead of depending on this extreme value.
#[doc(hidden)]
pub const FIRST: u8 = 0;
pub const EARLY: u8 = 50;
pub const NORMAL: u8 = 100;
pub const LATE: u8 = 150;
/// Reserved ceiling for final workspace-internal lifecycle components.
///
/// Other components should use `LATE` plus explicit `ComponentOrder` edges
/// when they need to be late.
#[doc(hidden)]
pub const LAST: u8 = u8::MAX;
