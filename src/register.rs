use crate::Result;

/// Static, application-owned service registration.
///
/// Implementations construct providers and insert their shared instances into
/// the application's registry. No `Default` constructor is required. This is
/// not configuration or boot: dependency state is prepared in later phases.
/// `S` is application-owned and need not implement a Continuo state trait.
/// A registration failure stops a tuple; already completed registrations are
/// retained, not rolled back. Dependency order belongs to provider lifecycle,
/// not to this declaration-order registration step.
pub trait Register<S> {
    fn register(state: &S) -> Result<()>;
}

impl<S> Register<S> for () {
    fn register(_: &S) -> Result<()> {
        Ok(())
    }
}

macro_rules! register_tuple {
    ($($member:ident),+) => {
        impl<S, $($member: Register<S>),+> Register<S> for ($($member,)+) {
            fn register(state: &S) -> Result<()> {
                $($member::register(state)?;)+
                Ok(())
            }
        }
    };
}

// Declaration order is preserved; an error stops subsequent registrations.
register_tuple!(A);
register_tuple!(A, B);
register_tuple!(A, B, C);
register_tuple!(A, B, C, D);
register_tuple!(A, B, C, D, E);
register_tuple!(A, B, C, D, E, F);
register_tuple!(A, B, C, D, E, F, G);
register_tuple!(A, B, C, D, E, F, G, H);
