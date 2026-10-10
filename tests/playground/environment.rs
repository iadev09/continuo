use super::environment_name;

#[test]
fn selects_default_or_explicit_environment_without_process_mutation() {
    assert_eq!(environment_name([]).unwrap(), "production");
    assert_eq!(
        environment_name(["--environment".into(), "local".into()]).unwrap(),
        "local"
    );
    assert!(environment_name(["--environment".into()]).is_err());
    assert!(environment_name(["--unknown".into()]).is_err());
    assert!(
        environment_name([
            "--environment".into(),
            "local".into(),
            "extra".into()
        ])
        .is_err()
    );
}
