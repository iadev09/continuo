use super::*;

struct FirstDomain;
struct SecondDomain;

#[test]
fn named_domains_own_runtime_names_and_remain_type_scoped() {
    let name: Arc<str> = String::from("configured-instance").into();
    let retained = Arc::downgrade(&name);
    let domain = ExecutionDomain::named::<FirstDomain>(name.clone());
    drop(name);
    assert!(retained.upgrade().is_some());
    assert_eq!(
        domain,
        ExecutionDomain::named::<FirstDomain>("configured-instance")
    );
    assert_ne!(
        domain,
        ExecutionDomain::named::<SecondDomain>("configured-instance")
    );
    assert_ne!(domain, ExecutionDomain::of::<FirstDomain>());
    drop(domain);
    assert!(retained.upgrade().is_none());
}

#[test]
fn domains_share_by_marker_type_not_by_service_identity() {
    assert_eq!(
        ExecutionDomain::of::<FirstDomain>(),
        ExecutionDomain::of::<FirstDomain>()
    );
    assert_ne!(
        ExecutionDomain::of::<FirstDomain>(),
        ExecutionDomain::of::<SecondDomain>()
    );
}

#[test]
fn application_policy_converts_without_domain_names_in_continuo() {
    struct BatchPolicy;
    impl From<BatchPolicy> for ExecutionSpec {
        fn from(_: BatchPolicy) -> Self {
            Self::default()
                .with_executor(ExecutorSpec::Shared(ExecutionDomain::of::<
                    FirstDomain,
                >()))
                .with_scheduling(SchedulingPolicy::Priority(PollPriority::Low))
        }
    }
    let spec: ExecutionSpec = BatchPolicy.into();
    assert_eq!(
        spec.executor,
        ExecutorSpec::Shared(ExecutionDomain::of::<FirstDomain>())
    );
    assert_eq!(spec.scheduling, SchedulingPolicy::Priority(PollPriority::Low));
    assert!(ExecutionPlan::prepare(spec.clone(), None).is_err());
}

#[test]
fn direct_context_retains_ambient_execution_and_cancellation() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    assert!(context.identity().is_none());
    assert!(context.child().identity().is_none());
    assert_eq!(context.execution_spec(), ExecutionSpec::default());
    assert!(context.execution_executor().is_none());
    cancellation.cancel();
    assert!(context.clone().is_cancelled());
}

#[test]
fn scheduling_and_placement_each_require_explicit_support() {
    let base = ExecutionSpec::default();
    for spec in [
        base.clone().with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::CurrentThread,
        )),
        base.clone().with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::MultiThread {
                threads: NonZeroUsize::new(2).unwrap(),
            },
        )),
        base.clone()
            .with_scheduling(SchedulingPolicy::Priority(PollPriority::Normal)),
        base.clone().with_scheduling(SchedulingPolicy::Weighted(
            NonZeroUsize::new(2).unwrap(),
        )),
    ] {
        assert!(ExecutionPlan::prepare(spec.clone(), None).is_err());
    }
}
