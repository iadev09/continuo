use std::path::PathBuf;

use super::CounterService;
use congif::{ConfigReader, parsers::JsonParser};
use std::sync::atomic::Ordering;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/counter")
        .join(name)
}

#[tokio::test]
async fn hits_notify_every_subscriber_and_signal_requests_page_reload() {
    let mut reader = ConfigReader::new();
    reader.register("json", JsonParser).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let provider = CounterService::new(
        fixture("valid.json"),
        directory.path().join("state.json"),
    );
    provider.apply_config(&reader).await.unwrap();
    provider.active.store(true, Ordering::Release);
    let mut first = provider.subscribe();
    let mut second = provider.subscribe();
    assert_eq!(provider.hit("192.0.2.1:1234".parse().unwrap()), Some((3, 3)));
    for receiver in [&mut first, &mut second] {
        assert!(receiver.has_changed().unwrap());
        let view = receiver.borrow_and_update();
        assert_eq!(view.value, 3);
        assert_eq!(view.page_revision, 0);
    }
    assert_eq!(provider.hit_from_signal(), Some(6));
    for receiver in [&mut first, &mut second] {
        assert!(receiver.has_changed().unwrap());
        let view = receiver.borrow_and_update();
        assert_eq!(view.value, 6);
        assert_eq!(view.page_revision, 1);
    }
    // A newly opened page adopts the current revision instead of reload looping.
    assert_eq!(provider.subscribe().borrow().page_revision, 1);
    provider.active.store(false, Ordering::Release);
    assert_eq!(provider.hit_from_signal(), None);
    assert!(!first.has_changed().unwrap());
    assert!(!second.has_changed().unwrap());
}

#[tokio::test]
async fn counter_configuration_preserves_snapshot_on_invalid_candidate() {
    let mut reader = ConfigReader::new();
    reader.register("json", JsonParser).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.json");
    tokio::fs::copy(fixture("valid.json"), &path).await.unwrap();
    let state_path = directory.path().join("state.json");
    let provider = CounterService::new(path.clone(), state_path.clone());
    provider.apply_config(&reader).await.unwrap();
    let store = provider.config.get().unwrap();
    let before = store.snapshot();
    assert_eq!(before.increment, 3);
    assert_eq!(provider.hits.load(Ordering::Relaxed), 0);

    provider.active.store(true, Ordering::Release);
    let client = "192.0.2.1:1234".parse().unwrap();
    assert_eq!(provider.hit(client), Some((3, 3)));
    // Duplicate native notifications must not reset a running counter.
    provider.apply_config(&reader).await.unwrap();
    assert_eq!(provider.hits.load(Ordering::Relaxed), 3);
    assert!(!state_path.exists(), "reload must not write counter state");

    tokio::fs::copy(fixture("invalid.json"), &path).await.unwrap();
    let error = provider.apply_config(&reader).await.err().unwrap();
    assert!(error.to_string().contains("greater than zero"));
    assert!(std::ptr::eq(before.as_ref(), store.snapshot().as_ref()));
    assert_eq!(provider.hits.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn counter_update_validates_before_replacing_file() {
    let mut reader = ConfigReader::new();
    reader.register("json", JsonParser).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.json");
    tokio::fs::copy(fixture("valid.json"), &path).await.unwrap();
    let provider =
        CounterService::new(path.clone(), directory.path().join("state.json"));
    let before = tokio::fs::read(&path).await.unwrap();
    let invalid =
        tokio::fs::read_to_string(fixture("invalid.json")).await.unwrap();
    assert!(provider.update_config(&reader, &invalid).await.is_err());
    assert_eq!(tokio::fs::read(&path).await.unwrap(), before);

    let valid = tokio::fs::read_to_string(fixture("valid.json")).await.unwrap();
    provider.update_config(&reader, &valid).await.unwrap();
    let parsed = reader.prepare(&provider).await.unwrap();
    assert_eq!(parsed.increment, 3);
}

#[tokio::test]
async fn counter_configuration_rejects_unknown_fields() {
    let mut reader = ConfigReader::new();
    reader.register("json", JsonParser).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let provider = CounterService::new(
        fixture("unknown.json"),
        directory.path().join("state.json"),
    );
    let error = reader.prepare(&provider).await.err().unwrap();
    assert!(matches!(error, congif::Error::Deserialize { .. }));
}

#[tokio::test]
async fn counter_state_is_saved_separately_and_restored() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.json");
    let provider = CounterService::new(fixture("valid.json"), path.clone());
    provider.restore_state().await.unwrap();
    assert_eq!(provider.hits.load(Ordering::Relaxed), 0);
    assert!(!path.exists());
    provider.hits.store(42, Ordering::Relaxed);
    provider.save_state().await.unwrap();
    let restored = CounterService::new(fixture("valid.json"), path);
    restored.restore_state().await.unwrap();
    assert_eq!(restored.hits.load(Ordering::Relaxed), 42);
}
