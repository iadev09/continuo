use std::any::TypeId;

/// Type-based lifecycle ordering hints.
///
/// Numeric priorities still provide a coarse tie-breaker. `ComponentOrder`
/// adds explicit relationships between lifecycle component types, so code can
/// say "prepare me before `T`" without relying on magic numbers or names.
#[derive(Clone, Debug, Default)]
pub struct ComponentOrder {
    before: Vec<TypeId>,
    after: Vec<TypeId>,
}

impl ComponentOrder {
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
