use super::*;
use std::path::PathBuf;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/http/valid.json")
}

#[tokio::test]
async fn source_has_complete_settings_without_a_registration_wrapper() {
    let reader = congif::ConfigReader::new().with_builtin_parsers().unwrap();
    let service = HttpService::new(fixture());
    reader.initialize_owners(&[&service]).await.unwrap();
    let config = reader.prepare(&service).await.unwrap();
    assert_eq!(config.listen, "192.0.2.1:3000".parse().unwrap());
    assert_eq!(config.max_connections, NonZeroUsize::new(2));
    assert!(!service.live.is_running());
    service.apply_config(config).await.unwrap();
    assert_eq!(reader.snapshot::<HttpService>().unwrap().listen, config.listen);
}
