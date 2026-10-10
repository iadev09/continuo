//! Each configuration uses a separate environment in the reader's shared store.
use congif::ConfigReader;
use continuo::Result;
use dotenw::{EnvHandle, Environment, MergeMode};
use std::path::PathBuf;

#[derive(Clone, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CounterConfig {
    pub(crate) increment: u64,
}

impl CounterConfig {
    pub fn validate(&self) -> congif::Result<()> {
        if self.increment == 0 {
            return Err(congif::Error::validation(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "counter increment must be greater than zero",
            )));
        }
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CounterState {
    pub value: u64,
}

pub fn atomic_write(
    path: &std::path::Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("file has no parent"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|error| error.error)?;
    Ok(())
}

pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/playground")
}

pub fn environment(
    reader: &ConfigReader,
    source: impl AsRef<std::path::Path>,
    base: Option<&EnvHandle>,
) -> Result<EnvHandle> {
    let mut environment = Environment::new()
        .with_environment_file(root().join(source), MergeMode::Override);
    if let Some(base) = base {
        environment = environment.with_base(base);
    }
    reader
        .environments()
        .register(environment)
        .map_err(congif::Error::from)
        .map_err(Into::into)
}
