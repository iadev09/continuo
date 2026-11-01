use std::sync::Arc;

use continuo::{Component, ComponentOrder, Registry};

struct Required;
impl Component<()> for Required {
    type Deps = Arc<Self>;
    fn name(&self) -> &str {
        "required-self"
    }
}

struct Optional;
impl Component<()> for Optional {
    type Deps = Option<Arc<Self>>;
    fn name(&self) -> &str {
        "optional-self"
    }
}

struct Ordered<const BEFORE: bool>;
impl<const BEFORE: bool> Component<()> for Ordered<BEFORE> {
    type Deps = ();
    fn name(&self) -> &str {
        "ordered-self"
    }
    fn order(&self) -> ComponentOrder {
        if BEFORE {
            ComponentOrder::new().before::<Self>()
        } else {
            ComponentOrder::new().after::<Self>()
        }
    }
}

fn assert_self_cycle<C: Component<()>>(component: C) {
    let name = component.name().to_owned();
    let registry = Registry::<()>::new();
    registry.register(Arc::new(component)).unwrap();
    let error = registry.lifecycle_names().unwrap_err().to_string();
    assert!(error.contains("cycle"), "{error}");
    assert!(error.contains(&name), "{error}");
}

#[test]
fn self_dependencies_and_order_are_cycles() {
    assert_self_cycle(Required);
    assert_self_cycle(Optional);
    assert_self_cycle(Ordered::<true>);
    assert_self_cycle(Ordered::<false>);
}
