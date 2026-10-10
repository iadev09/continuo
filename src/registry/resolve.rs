//! Typed dependencies: what a component requires in order to boot and run.

use std::any::{Any, TypeId};
use std::fmt;
use std::sync::Arc;

use super::{Registry, Result};

/// The dependencies a component declares through `Component::Deps`.
///
/// A dependency is a hard requirement: the component cannot boot or run
/// without it. `Arc<T>` requires `T` to be registered and, when `T` is itself
/// a component, orders it before the dependent. `Option<Arc<T>>` takes the
/// same ordering when `T` is present and resolves to `None` when it is not.
/// Tuples combine up to eight dependencies; `()` declares none.
///
/// A component that merely uses another when it happens to be there, and
/// serves without it, does not declare it here. It resolves the value at the
/// point of use and expresses a preferred start order through `run_priority`.
///
/// Resolution happens once before `Bootable::boot` and once before each run
/// generation, in the registry, after every dependency has had its chance to
/// be registered. A missing dependency is one error naming the component and
/// the missing type.
pub trait Resolve<S>: Clone + Send + Sync + Sized + 'static {
    /// The types this value requires. The registry turns registered
    /// components among them into ordering edges.
    fn dependencies(into: &mut Vec<TypeId>);

    /// Resolve the value from the registry.
    fn resolve(registry: &Registry<S>) -> Result<Self>;
}

/// A dependency named by `Component::Deps` that is not registered.
#[derive(Debug)]
pub struct MissingDependency {
    dependency: &'static str,
}

impl MissingDependency {
    /// The type name of the dependency.
    pub fn dependency(&self) -> &'static str {
        self.dependency
    }
}

impl fmt::Display for MissingDependency {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "`{}` is not registered", self.dependency)
    }
}

impl std::error::Error for MissingDependency {}

impl<S: 'static> Resolve<S> for () {
    fn dependencies(_: &mut Vec<TypeId>) {}

    fn resolve(_: &Registry<S>) -> Result<Self> {
        Ok(())
    }
}

impl<S: 'static, T: Any + Send + Sync> Resolve<S> for Arc<T> {
    fn dependencies(into: &mut Vec<TypeId>) {
        into.push(TypeId::of::<T>());
    }

    fn resolve(registry: &Registry<S>) -> Result<Self> {
        registry.resolve::<T>().ok_or_else(|| {
            MissingDependency { dependency: std::any::type_name::<T>() }.into()
        })
    }
}

impl<S: 'static, T: Any + Send + Sync> Resolve<S> for Option<Arc<T>> {
    fn dependencies(into: &mut Vec<TypeId>) {
        into.push(TypeId::of::<T>());
    }

    fn resolve(registry: &Registry<S>) -> Result<Self> {
        Ok(registry.resolve::<T>())
    }
}

macro_rules! resolve_tuple {
    ($($member:ident),+) => {
        impl<S: 'static, $($member: Resolve<S>),+> Resolve<S> for ($($member,)+) {
            fn dependencies(into: &mut Vec<TypeId>) {
                $($member::dependencies(into);)+
            }

            fn resolve(registry: &Registry<S>) -> Result<Self> {
                Ok(($($member::resolve(registry)?,)+))
            }
        }
    };
}

resolve_tuple!(A);
resolve_tuple!(A, B);
resolve_tuple!(A, B, C);
resolve_tuple!(A, B, C, D);
resolve_tuple!(A, B, C, D, E);
resolve_tuple!(A, B, C, D, E, F);
resolve_tuple!(A, B, C, D, E, F, G);
resolve_tuple!(A, B, C, D, E, F, G, H);
