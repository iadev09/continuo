use super::{Dispatch, Scope};

#[test]
fn implicit_placement_excludes_master_but_admits_workers_and_standalone() {
    let scope = Scope::default();
    assert!(!scope.includes(Scope::Master));
    assert!(scope.includes(Scope::Worker));
    // A standalone host supplies Any as its combined process role.
    assert!(scope.includes(Scope::Any));
    assert_eq!(Dispatch::default(), Dispatch::Each);
}

#[test]
fn explicit_all_role_aliases_still_admit_master() {
    for spelling in ["any", "fleet", "both"] {
        let scope: Scope =
            serde_json::from_value(serde_json::json!(spelling)).unwrap();
        assert!(scope.includes(Scope::Master));
        assert!(scope.includes(Scope::Worker));
    }
}
