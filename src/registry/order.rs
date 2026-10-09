use std::any::TypeId;

/// Type-based lifecycle ordering hints.
///
/// Numeric priorities still provide a coarse tie-breaker. `ProviderOrder`
/// adds explicit relationships between provider concrete types, so code can
/// say "run me before `T`" without relying on magic numbers or provider names.
#[derive(Clone, Debug, Default)]
pub struct ProviderOrder {
    before: Vec<TypeId>,
    after: Vec<TypeId>,
}

impl ProviderOrder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn before<T: 'static>(mut self) -> Self {
        self.before.push(TypeId::of::<T>());
        self
    }

    pub fn after<T: 'static>(mut self) -> Self {
        self.after.push(TypeId::of::<T>());
        self
    }

    pub fn before_types(&self) -> &[TypeId] {
        &self.before
    }

    pub fn after_types(&self) -> &[TypeId] {
        &self.after
    }
}
