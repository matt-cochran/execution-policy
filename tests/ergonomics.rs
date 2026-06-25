//! Proves all four methods, state injection, `!Send` ops, and `?` interop.

use std::rc::Rc;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionPolicyBuilder, Retry};

struct Deps {
    base: u32,
}

#[tokio::test]
async fn all_four_methods() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .build_with(core);

    let a = policy.run(async || Ok::<_, &str>(1u32)).await.unwrap();

    let deps = Deps { base: 10 };
    let b = policy
        .run_with(&deps, async |d: &Deps| Ok::<_, &str>(d.base))
        .await
        .unwrap();

    let c = policy
        .execute(async |at| Ok::<_, &str>(at.number()))
        .await
        .unwrap();

    let d = policy
        .execute_with(&deps, async |dep: &Deps, at| {
            Ok::<_, &str>(dep.base + at.number())
        })
        .await
        .unwrap();

    assert_eq!((a, b, c, d), (1, 10, 1, 11));
}

#[tokio::test]
async fn accepts_non_send_operation() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .build_with(core);
    let shared = Rc::new(41u32);
    let out = policy
        .run(async || {
            let v = Rc::clone(&shared);
            Ok::<_, &str>(*v + 1)
        })
        .await
        .unwrap();
    assert_eq!(out, 42);
}

#[tokio::test]
async fn question_mark_into_caller_error() -> Result<(), Box<dyn std::error::Error>> {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, std::io::Error>::new()
        .retry(Retry::none())
        .build_with(core);
    let v: u32 = policy
        .run(async || Ok::<u32, std::io::Error>(5))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    assert_eq!(v, 5);
    Ok(())
}
