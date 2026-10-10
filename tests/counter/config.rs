use super::*;
use congif::ConfigReader;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/counter")
        .join(name)
}

fn service(
    path: PathBuf,
    state_path: PathBuf,
) -> CounterService {
    CounterService::new(
        path,
        state_path,
        Arc::new(Counter::default()),
        watch::channel(CounterView {
            value: 0,
            increment: 0,
            active: false,
            page_revision: 0,
        })
        .0,
    )
}

#[tokio::test]
async fn configuration_notifies_views_without_changing_the_number() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let service =
        service(fixture("valid.json"), directory.path().join("state.json"));
    reader.initialize_owners(&[&service]).await.unwrap();
    reader.configure_registered(&service).await.unwrap();
    service.counter.set(42);
    let mut updates = service.updates.subscribe();
    service.config_changed(&CounterConfig { increment: 7 }).await.unwrap();
    assert!(updates.has_changed().unwrap());
    assert_eq!(updates.borrow_and_update().increment, 7);
    assert_eq!(service.counter.value(), 42);
    service.config_changed(&CounterConfig { increment: 7 }).await.unwrap();
    assert!(!updates.has_changed().unwrap());
}

#[tokio::test]
async fn invalid_configuration_preserves_published_snapshot() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.json");
    tokio::fs::copy(fixture("valid.json"), &path).await.unwrap();
    let service = service(path.clone(), directory.path().join("state.json"));
    reader.initialize_owners(&[&service]).await.unwrap();
    reader.configure_registered(&service).await.unwrap();
    let before = reader.snapshot::<CounterService>().unwrap();
    service.counter.set(42);
    tokio::fs::copy(fixture("invalid.json"), &path).await.unwrap();
    let error = reader.prepare_owners(&[&service]).await.err().unwrap();
    assert!(error.to_string().contains("greater than zero"));
    assert!(std::ptr::eq(
        before.as_ref(),
        reader.snapshot::<CounterService>().unwrap().as_ref()
    ));
    assert_eq!(service.counter.value(), 42);
    assert!(!service.state_path.exists());
}

#[tokio::test]
async fn web_editor_validates_before_replacing_the_file() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.json");
    tokio::fs::copy(fixture("valid.json"), &path).await.unwrap();
    let service = service(path.clone(), directory.path().join("state.json"));
    reader.initialize_owners(&[&service]).await.unwrap();
    let before = tokio::fs::read(&path).await.unwrap();
    let invalid =
        tokio::fs::read_to_string(fixture("invalid.json")).await.unwrap();
    assert!(
        super::super::super::handlers::save_counter_config(
            &reader, &service, &invalid
        )
        .await
        .is_err()
    );
    assert_eq!(tokio::fs::read(&path).await.unwrap(), before);
    let valid = tokio::fs::read_to_string(fixture("valid.json")).await.unwrap();
    super::super::super::handlers::save_counter_config(
        &reader, &service, &valid,
    )
    .await
    .unwrap();
    assert_eq!(reader.prepare(&service).await.unwrap().increment, 3);
}

#[tokio::test]
async fn configuration_rejects_unknown_fields() {
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let error = reader
        .prepare(&service(
            fixture("unknown.json"),
            directory.path().join("state.json"),
        ))
        .await
        .err()
        .unwrap();
    assert!(matches!(error, congif::Error::Deserialize { .. }));
}

#[tokio::test]
async fn saved_number_is_restored_without_config_or_web_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.json");
    let service = service(fixture("valid.json"), path.clone());
    service.restore_state().await.unwrap();
    assert_eq!(service.counter.value(), 0);
    assert!(!path.exists());
    service.counter.set(42);
    service.save_state().await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &tokio::fs::read(&path).await.unwrap()
        )
        .unwrap(),
        serde_json::json!({"value": 42})
    );
    service.counter.set(0);
    service.restore_state().await.unwrap();
    assert_eq!(service.counter.value(), 42);
}
