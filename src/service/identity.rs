use std::sync::Arc;

/// Managed runnable instance name and its local generation number.
///
/// The runtime assigns this pair at generation admission. Downstream execution
/// adapters can retain it as an owned key for diagnostics and metrics.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RunIdentity {
    instance: Arc<str>,
    generation: u64,
}

impl RunIdentity {
    pub(crate) fn new(
        instance: Arc<str>,
        generation: u64,
    ) -> Self {
        Self { instance, generation }
    }

    /// Runnable instance's management name.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// Start generation within the runtime's managed instance entry.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
