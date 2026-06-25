//! Integration: circuit breaker, bounded concurrency, and retry budget through
//! the full `ExecutionPolicy` surface.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{
    BreakerState, CircuitBreaker, ConcurrencyLimit, ExecutionPolicyBuilder, Retry, RetryBudget,
};

#[tokio::test]
async fn circuit_opens_and_rejects() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .circuit_breaker(CircuitBreaker::consecutive_failures(2).open_for(Duration::from_secs(30)))
        .build_with(core);

    // Two failures trip the breaker (no retries, so one failure per call).
    assert!(policy.execute(async |_a| Err::<u32, _>("x")).await.is_err());
    assert!(policy.execute(async |_a| Err::<u32, _>("x")).await.is_err());
    assert_eq!(policy.circuit_state(), Some(BreakerState::Open));

    // Next call is rejected outright with CircuitOpen (operation not invoked).
    let calls = AtomicU32::new(0);
    let err = policy
        .execute(async |_a| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<u32, &str>(1)
        })
        .await
        .unwrap_err();
    assert!(err.is_circuit_open());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "operation must not run while open"
    );
}

#[tokio::test]
async fn circuit_half_opens_then_closes() {
    let clock = ManualClock::new();
    let core = TestCore::new(clock.clone());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .circuit_breaker(
            CircuitBreaker::consecutive_failures(1)
                .open_for(Duration::from_secs(5))
                .half_open_max_calls(1),
        )
        .build_with(core);

    assert!(policy.execute(async |_a| Err::<u32, _>("x")).await.is_err());
    assert_eq!(policy.circuit_state(), Some(BreakerState::Open));

    clock.advance(Duration::from_secs(6)); // open_for elapses

    // Half-open probe succeeds → circuit closes.
    let ok = policy.execute(async |_a| Ok::<_, &str>(7u32)).await;
    assert_eq!(ok.unwrap(), 7);
    assert_eq!(policy.circuit_state(), Some(BreakerState::Closed));
}

#[tokio::test]
async fn concurrency_reject_sheds_load() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .concurrency_limit(ConcurrencyLimit::operations(1).reject())
        .build_with(core);

    // Hold the single permit with a long-running op, then a second call should
    // be rejected immediately.
    let occupy = async {
        policy
            .execute(async |_a| {
                // Yield a few times to stay "in flight" while the other call races.
                for _ in 0..5 {
                    tokio::task::yield_now().await;
                }
                Ok::<u32, &str>(1)
            })
            .await
    };
    let intrude = async {
        tokio::task::yield_now().await; // let `occupy` take the permit first
        policy.execute(async |_a| Ok::<u32, &str>(2)).await
    };
    let (a, b) = tokio::join!(occupy, intrude);
    assert!(a.is_ok());
    assert!(
        b.unwrap_err().is_rejected(),
        "second call should be load-shed"
    );
}

#[tokio::test]
async fn retry_budget_caps_storms() {
    let core = TestCore::new(ManualClock::new());
    // Budget allows a burst of exactly 1 retry, no replenishment.
    let budget = RetryBudget::new(0.0, 1);
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::fixed(Duration::ZERO).max_attempts(10).budget(budget))
        .build_with(core);

    let calls = AtomicU32::new(0);
    let err = policy
        .execute(async |_a| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<u32, _>("always")
        })
        .await
        .unwrap_err();
    // Attempt 1 (deposit), one retry token spent → attempt 2, then budget empty.
    assert_eq!(calls.load(Ordering::SeqCst), 2, "budget bounded the storm");
    assert!(matches!(err, e if e.context().attempts == 2));
}
