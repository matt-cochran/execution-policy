#![cfg(feature = "test-util")]
//! Integration: the `on_event` observability hook.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{Event, ExecutionPolicyBuilder, Retry};

fn recorder() -> (
    Arc<Mutex<Vec<Event>>>,
    impl Fn(&Event) + Send + Sync + 'static,
) {
    let sink: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let inner = Arc::clone(&sink);
    (sink, move |e: &Event| inner.lock().unwrap().push(e.clone()))
}

#[tokio::test]
async fn emits_attempt_failed_and_succeeded() {
    let (events, hook) = recorder();
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::fixed(Duration::ZERO).max_attempts(3))
        .on_event(hook)
        .build_with(core);

    let calls = std::sync::atomic::AtomicU32::new(0);
    let r = policy
        .execute(async |a| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if a.number() < 3 {
                Err("transient")
            } else {
                Ok(1u32)
            }
        })
        .await;
    assert!(r.is_ok());

    let log = events.lock().unwrap();
    assert!(log.contains(&Event::AttemptFailed { attempt: 1 }));
    assert!(log.contains(&Event::AttemptFailed { attempt: 2 }));
    assert!(log.contains(&Event::Succeeded { attempts: 3 }));
    assert!(
        log.iter()
            .any(|e| matches!(e, Event::RetryScheduled { .. }))
    );
}

#[tokio::test]
async fn emits_gave_up_on_exhaustion() {
    let (events, hook) = recorder();
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::fixed(Duration::ZERO).max_attempts(2))
        .on_event(hook)
        .build_with(core);

    let _ = policy.execute(async |_a| Err::<u32, _>("always")).await;
    let log = events.lock().unwrap();
    assert!(log.contains(&Event::GaveUp { attempts: 2 }));
}

#[tokio::test]
async fn no_hook_means_no_overhead_path() {
    // Smoke test that the no-hook path still works (events guarded by Option).
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .build_with(core);
    assert_eq!(policy.run(async || Ok::<_, &str>(5u32)).await.unwrap(), 5);
}
