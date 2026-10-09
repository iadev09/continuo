// =====================================================================
// Error model
// =====================================================================
//
// The error type is deliberately scoped to the `registry` module because
// it is a *registry* concern (lifecycle stages: boot / validate / reload
// / run). Other helper primitives in this crate (`gate`, `guard`) have their
// own semantics and intentionally do not share this type.
//
// Consumers return any `std::error::Error + Send + Sync + 'static`
// from their `Provider` / `Reloadable` / `Runnable` / `Finalizable`
// methods — the blanket
// `From<E>` impl wraps it into `Error::Other`. The registry then re-wraps
// `Error::Other` into the appropriate lifecycle variant (`Boot`, `Reload`,
// `Run`, `Validate`) at the call site, so downstream logs and matches see
// where the failure happened. Providers that want to emit a typed variant
// themselves can construct it directly — the registry will leave it
// untouched.

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    DuplicateProvider {
        type_name: &'static str,
    },
    Boot {
        name: &'static str,
        source: BoxError,
    },
    Validate {
        name: &'static str,
        source: BoxError,
    },
    Reload {
        name: &'static str,
        source: BoxError,
    },
    Finalize {
        name: &'static str,
        source: BoxError,
    },
    /// Fatal runnable failure (default). The runtime tears the worker
    /// down so the supervisor can respawn cleanly.
    Run {
        name: &'static str,
        source: BoxError,
    },
    /// Recoverable runnable failure. The runtime logs and keeps the
    /// worker serving — used for best-effort tasks (e.g. notify
    /// listeners, optional integrations) where a transient or
    /// configuration-driven failure shouldn't kill traffic.
    Recoverable {
        name: &'static str,
        source: BoxError,
    },
    Other(BoxError),
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Error::DuplicateProvider { type_name } => {
                write!(f, "provider type '{type_name}' is already registered")
            }
            Error::Boot { name, source } => {
                write!(f, "provider '{name}' failed during boot: {source}")
            }
            Error::Validate { name, source } => {
                write!(f, "provider '{name}' failed during validate: {source}")
            }
            Error::Reload { name, source } => {
                write!(f, "reload of '{name}' failed: {source}")
            }
            Error::Finalize { name, source } => {
                write!(f, "finalize of '{name}' failed: {source}")
            }
            Error::Run { name, source } => {
                write!(f, "runnable '{name}' failed: {source}")
            }
            Error::Recoverable { name, source } => {
                write!(f, "runnable '{name}' failed (recoverable): {source}")
            }
            Error::Other(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

// `Error` deliberately does NOT implement `std::error::Error`, and cannot while
// the blanket `From<E: Error>` below exists: the two overlap through core's
// reflexive `From<T> for T` the moment `Error: std::error::Error` holds. It is
// a real trade and worth naming rather than discovering.
//
// The blanket is what makes `?` work on an io, serde or driver error inside a
// lifecycle hook, which is most of what implementors write; removing it would
// put an explicit conversion on every one of those call sites. Keeping it costs
// interop — no `source()` chain, no `Box<dyn Error>`, nothing `anyhow` or
// `eyre` will take, and not returnable from `main`.
//
// `into_std` below is the way out where interop is what you need: it hands back
// a `std::error::Error` that keeps this error's `Display` and exposes the
// `BoxError` a variant was carrying as its `source()`, so the chain survives
// the crossing.

impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(e: E) -> Self {
        Error::Other(Box::new(e))
    }
}

impl Error {
    /// Converts into a `std::error::Error` for handing to something that
    /// requires one — `anyhow`, `eyre`, `Box<dyn Error>`, `fn main`.
    ///
    /// This type cannot implement that trait itself; see the note above the
    /// blanket `From` impl for why. The returned error keeps this one's
    /// `Display` and reports the boxed cause a lifecycle variant was carrying
    /// as its `source()`, so a chain that arrived here survives leaving.
    pub fn into_std(self) -> StdError {
        StdError(self)
    }
}

/// [`Error`] as a `std::error::Error`, from [`Error::into_std`].
#[derive(Debug)]
pub struct StdError(Error);

impl StdError {
    /// The error this was made from.
    pub fn into_inner(self) -> Error {
        self.0
    }
}

impl std::fmt::Display for StdError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for StdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            Error::Boot { source, .. }
            | Error::Validate { source, .. }
            | Error::Reload { source, .. }
            | Error::Finalize { source, .. }
            | Error::Other(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<Error> for StdError {
    fn from(error: Error) -> Self {
        error.into_std()
    }
}

/// Construct `Error::Other` from an arbitrary message string.
impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Other(Box::new(MsgErr(s.into())))
    }

    /// If the error is `Other`, re-wrap it as `Boot { name, source }`;
    /// otherwise leave it untouched. Used by `Registry::boot_all` to
    /// attach lifecycle context to anonymous user errors.
    pub(super) fn into_boot(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Boot { name, source },
            other => other,
        }
    }
    pub(super) fn into_validate(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Validate { name, source },
            other => other,
        }
    }
    /// Attach provider identity to an anonymous reload error.
    pub(crate) fn into_reload(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Reload { name, source },
            other => other,
        }
    }
    pub(super) fn into_finalize(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Finalize { name, source },
            other => other,
        }
    }
    pub(crate) fn into_run(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Run { name, source },
            // Runnables that opt into recoverable failure construct
            // `Recoverable` with an empty `name`; `run_all` fills in the
            // provider name here so log lines stay attributed.
            Error::Recoverable { name: "", source } => {
                Error::Recoverable { name, source }
            }
            other => other,
        }
    }

    /// Build a recoverable runnable error from an arbitrary message.
    /// The runtime logs this and lets the worker keep serving instead of
    /// tearing it down. The provider `name` is filled in by `run_all`'s
    /// wrapper, so callers only supply the message.
    pub fn recoverable(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Recoverable { name: "", source: Box::new(MsgErr(s.into())) }
    }
}
