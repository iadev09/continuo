use super::Counter;

#[test]
fn number_can_be_read_set_and_added_without_a_runtime() {
    let counter = Counter::default();
    assert_eq!(counter.value(), 0);
    assert_eq!(counter.add(3), 3);
    counter.set(42);
    assert_eq!(counter.add(2), 44);
}

#[test]
fn additions_preserve_the_playgrounds_saturating_behavior() {
    let counter = Counter::default();
    counter.set(u64::MAX - 1);
    assert_eq!(counter.add(2), u64::MAX);
    assert_eq!(counter.add(1), u64::MAX);
}

#[test]
fn concurrent_additions_are_not_lost() {
    let counter = Counter::default();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let counter = &counter;
            scope.spawn(move || {
                for _ in 0..100 {
                    counter.add(1);
                }
            });
        }
    });
    assert_eq!(counter.value(), 400);
}
