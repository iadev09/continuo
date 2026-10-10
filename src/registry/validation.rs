use super::Error;

/// One component failure retained by a full validation pass.
#[derive(Debug)]
pub struct ValidationFailure {
    pub(super) component: &'static str,
    pub(super) error: Error,
}

impl ValidationFailure {
    pub fn component(&self) -> &'static str {
        self.component
    }

    pub fn error(&self) -> &Error {
        &self.error
    }

    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of validating every component in lifecycle order.
///
/// The outer [`super::Result`] returned by [`super::Registry::validate_all`] is reserved for
/// lifecycle-plan failures. Individual component failures are retained here so
/// callers can report the complete invalid configuration in one pass.
#[derive(Debug, Default)]
#[must_use = "component validation failures are reported through ValidationOutcome"]
pub struct ValidationOutcome {
    pub(super) validated_count: usize,
    pub(super) failures: Vec<ValidationFailure>,
}

impl ValidationOutcome {
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn validated_count(&self) -> usize {
        self.validated_count
    }

    pub fn failed_count(&self) -> usize {
        self.failures.len()
    }

    pub fn failures(&self) -> &[ValidationFailure] {
        &self.failures
    }

    pub fn into_failures(self) -> Vec<ValidationFailure> {
        self.failures
    }
}
