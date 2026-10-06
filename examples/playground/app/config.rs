//! Startup-only environment input. Watching/reloading `.env` is not implied.
use std::path::Path;

use congif::Variables;
use continuo::{Error, Result};

pub fn variables(path: &Path) -> Result<Variables> {
    let mut variables = Variables::new();
    match dotenvy::from_filename_iter(path) {
        Ok(entries) => {
            for entry in entries {
                let (key, value) =
                    entry.map_err(|error| Error::msg(error.to_string()))?;
                variables.insert(key, value);
            }
        }
        Err(dotenvy::Error::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::msg(error.to_string())),
    }
    // Explicit process environment wins. No unsafe mutation of process-global
    // environment is needed, even when this runs inside an async runtime.
    for (key, value) in std::env::vars_os() {
        if let (Ok(key), Ok(value)) = (key.into_string(), value.into_string()) {
            variables.insert(key, value);
        }
    }
    Ok(variables)
}
