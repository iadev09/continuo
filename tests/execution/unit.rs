use super::*;

struct FirstDomain;
struct SecondDomain;

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
    assert!(ExecutionPlan::prepare(spec, None).is_err());
}

#[test]
fn direct_context_retains_ambient_execution_and_cancellation() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = RunContext::new(cancellation.clone());
    assert_eq!(context.execution_spec(), ExecutionSpec::default());
    assert!(context.execution_executor().is_none());
    cancellation.cancel();
    assert!(context.clone().is_cancelled());
}

#[test]
fn scheduling_and_placement_each_require_explicit_support() {
    let base = ExecutionSpec::default();
    for spec in [
        base.with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::CurrentThread,
        )),
        base.with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::MultiThread {
                threads: NonZeroUsize::new(2).unwrap(),
            },
        )),
        base.with_scheduling(SchedulingPolicy::Priority(PollPriority::Normal)),
        base.with_scheduling(SchedulingPolicy::Weighted(
            NonZeroUsize::new(2).unwrap(),
        )),
    ] {
        assert!(ExecutionPlan::prepare(spec, None).is_err());
    }
}
