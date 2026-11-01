use super::state::State;
use async_trait::async_trait;
use congif::{ConfigObserver, Configurable};
use continuo::{
    Component, HasConfigReader, Reloadable, Result, RunContext, Runnable,
    RunnableInstance,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct Output;

impl Output {
    fn report(
        &self,
        name: &str,
        action: &str,
    ) {
        println!("{name} {action}");
    }
}

pub struct Jobs {
    source: PathBuf,
    jobs: RwLock<Vec<Arc<Job>>>,
}

impl Jobs {
    pub fn new(source: PathBuf) -> Self {
        Self { source, jobs: RwLock::new(Vec::new()) }
    }

    fn apply(
        &self,
        config: &JobsConfig,
    ) {
        let mut jobs = self.jobs.write().expect("job declarations poisoned");
        // Unchanged declarations retain their operational identity.
        *jobs = config
            .instances
            .iter()
            .map(|name| {
                jobs.iter()
                    .find(|job| job.name.as_ref() == name)
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(Job { name: name.as_str().into() })
                    })
            })
            .collect();
    }
}

impl Component<State> for Jobs {
    type Deps = (Arc<Output>,);

    fn name(&self) -> &str {
        "jobs"
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<State>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<State>> {
        self.jobs
            .read()
            .expect("job declarations poisoned")
            .iter()
            .map(|job| RunnableInstance::new(job.name.clone(), job.clone()))
            .collect()
    }
}

#[derive(serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobsConfig {
    instances: Vec<String>,
}

#[async_trait]
impl Configurable for Jobs {
    type Config = JobsConfig;

    fn source(&self) -> congif::ConfigFile {
        congif::ConfigFile {
            path: self.source.clone(),
            format: congif::Format::Json,
        }
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = JobsConfig>> {
        Some(self)
    }

    fn validate_config(
        &self,
        config: &JobsConfig,
    ) -> congif::Result<()> {
        let mut names = HashSet::new();
        for name in &config.instances {
            if name.is_empty() || name == "signals" || !names.insert(name) {
                return Err(congif::Error::validation(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "job names must be nonempty, unique and different from 'signals'",
                )));
            }
        }
        Ok(())
    }

    async fn configure(
        &self,
        config: &JobsConfig,
    ) -> congif::Result<()> {
        self.apply(config);
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for Jobs {
    async fn config_changed(
        &self,
        config: &JobsConfig,
    ) -> congif::Result<()> {
        self.apply(config);
        Ok(())
    }
}

#[async_trait]
impl Reloadable<State> for Jobs {
    async fn reload(
        &self,
        state: &State,
    ) -> Result<()> {
        self.apply(state.config_reader().snapshot::<Self>()?.as_ref());
        Ok(())
    }
}

struct Job {
    name: Arc<str>,
}

#[async_trait]
impl Runnable<State, (Arc<Output>,)> for Job {
    async fn run(
        self: Arc<Self>,
        (output,): (Arc<Output>,),
        _: State,
        context: RunContext,
    ) -> Result<()> {
        output.report(&self.name, "started");
        context.cancelled().await;
        output.report(&self.name, "stopped");
        Ok(())
    }
}
