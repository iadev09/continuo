use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct AtomicParser(pub Arc<AtomicUsize>);

#[async_trait::async_trait]
impl congif::ConfigParser for AtomicParser {
    fn extensions(&self) -> &[&str] {
        &["test"]
    }

    async fn parse(
        &self,
        _: congif::ConfigSource<'_>,
    ) -> std::result::Result<congif::ConfigValue, congif::BoxError> {
        Ok(congif::ConfigValue::U64(self.0.load(Ordering::Relaxed) as u64))
    }
}

pub fn source() -> congif::ConfigFile {
    congif::ConfigFile {
        path: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/counter/valid.json"),
        format: congif::Format::Custom("test".into()),
    }
}
