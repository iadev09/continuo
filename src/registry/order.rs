use std::any::TypeId;

/// Placement of a component relative to other types.
///
/// A type named here that is a registered component becomes a direct edge.
/// A type that is not registered acts as a target: a node with no work of its
/// own that completes once every component ordered `before` it has completed,
/// and releases every component ordered `after` it. Targets let a component
/// say "after the network layer" without naming, or depending on, the
/// components that make up that layer. Requirements on specific components
/// belong in `Component::Deps`.
/// A plain DI value produced during boot does not identify its producer:
/// declare the value in `Deps` and order after its preparation component.
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
