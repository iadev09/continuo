//! Startup environment: which `.env` layer this run uses and the process
//! environment it starts from. Belongs to the executable, not to Continuo or
//! to any service.
use continuo::{Error, Result};

/// The environment selected for this run.
pub struct Environment {
    name: String,
}

impl Environment {
    /// `--environment NAME`; `production` when absent.
    pub fn from_args() -> Result<Self> {
        Ok(Self { name: environment_name(std::env::args().skip(1))? })
    }

    /// Read the process environment into a dotenw store under this
    /// environment's name and install diagnostics.
    ///
    /// The example reads the environment and leaves it in place. A deployment
    /// that must hide secrets from later `getenv` calls uses
    /// `EnvStore::take_process_env` at its exclusive startup boundary instead.
    pub(super) fn store(&self) -> Result<dotenw::EnvStore> {
        let store = dotenw::EnvStore::from_base(dotenw::Snapshot::new(
            std::env::vars().collect(),
        ))
        .and_then(|store| store.with_environment(self.name.clone()))
        .map_err(congif::Error::from)?;
        let base = store.base();
        let filter = tracing_subscriber::EnvFilter::try_new(
            base.get("RUST_LOG").map(String::as_str).unwrap_or("info"),
        )
        .map_err(|error| Error::msg(error.to_string()))?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_writer(std::io::stderr)
            .try_init()
            .map_err(|error| {
                Error::msg(format!("could not initialize logging: {error}"))
            })?;
        Ok(store)
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self { name: "production".to_owned() }
    }
}

fn environment_name(
    arguments: impl IntoIterator<Item = String>
) -> Result<String> {
    let mut arguments = arguments.into_iter();
    let name = match arguments.next().as_deref() {
        None => "production".to_owned(),
        Some("--environment") => arguments
            .next()
            .ok_or_else(|| Error::msg("--environment requires a name"))?,
        Some(_) => {
            return Err(Error::msg("usage: playground [--environment NAME]"));
        }
    };
    if arguments.next().is_some() {
        return Err(Error::msg("usage: playground [--environment NAME]"));
    }
    Ok(name)
}

#[cfg(test)]
#[path = "../../../tests/playground/environment.rs"]
mod tests;
