//! Process environment and diagnostics belong to the example's startup,
//! not to Continuo or to each service.
use continuo::{Error, Result};

/// Capture the example's startup environment with dotenw, select the explicit
/// environment argument, and install application-owned diagnostics.
///
/// # Safety
/// Call only before runtime or other threads start. dotenw captures and scrubs
/// the process environment at this exclusive boundary; services use snapshots.
pub unsafe fn environment() -> Result<dotenw::EnvStore> {
    let name = environment_name(std::env::args().skip(1))?;
    // SAFETY: the executable caller owns the exclusive startup boundary.
    let store = unsafe { dotenw::EnvStore::take_process_env() }
        .and_then(|store| store.with_environment(name))
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
