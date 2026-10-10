use std::path::PathBuf;

use super::SignalService;
use congif::ConfigReader;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/signals")
        .join(name)
}

#[tokio::test]
async fn signal_commands_follow_the_typed_configuration() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let service = SignalService::new(fixture("valid.json"));
    let config = reader.prepare(&service).await.unwrap();
    assert_eq!(
        config.commands(42),
        ("kill -USR2 42".into(), "kill -HUP 42".into())
    );
}

#[tokio::test]
async fn invalid_signal_candidates_preserve_the_published_mapping() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let valid = reader
        .prepare(&SignalService::new(fixture("valid.json")))
        .await
        .unwrap();
    let store = congif::ConfigStore::new(valid);
    let before = store.snapshot();
    let duplicate: super::config::SignalConfig =
        reader.read(&fixture("duplicate.json")).await.unwrap();
    assert!(
        store
            .replace(duplicate, super::config::SignalConfig::validate)
            .is_err()
    );
    assert!(std::ptr::eq(before.as_ref(), store.snapshot().as_ref()));
    for file in ["duplicate.json", "unsupported.json", "unknown.json"] {
        assert!(
            reader.prepare(&SignalService::new(fixture(file))).await.is_err()
        );
    }
}
