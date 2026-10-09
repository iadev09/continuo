//! Shared lifecycle priority definitions for providers/reloadables.
//!
//! Lower values run earlier among providers that are otherwise ready.
//! Prefer `ProviderOrder` for real dependencies; priorities are only
//! coarse tie-breakers for legacy/simple cases.

/// Reserved floor for workspace-internal root providers.
///
/// Ordinary providers should use `EARLY`, `NORMAL`, `LATE`, or explicit
/// `ProviderOrder` edges instead of depending on this extreme value.
#[doc(hidden)]
pub const FIRST: u8 = 0;
pub const EARLY: u8 = 50;
pub const NORMAL: u8 = 100;
pub const LATE: u8 = 150;
/// Reserved ceiling for final workspace-internal lifecycle providers.
///
/// Other providers should use `LATE` plus explicit `ProviderOrder` edges
/// when they need to be late.
#[doc(hidden)]
pub const LAST: u8 = u8::MAX;
