//! Integration: retry, backoff timing, and total-timeout behavior on a virtual clock.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionPolicyBuilder, Retry};

#[tokio::test]
async fn backoff_waits_on_the_virtual_clock() {
    let clock = ManualClock::new();
    let core = TestCore::new(clock.clone());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(
            Retry::exponential()
                .max_attempts(3)
                .base_delay(Duration::from_millis(100))
                .max_delay(Duration::from_secs(1)),
        )
        .build_with(core);

    let calls = AtomicU32::new(0);
    let driver = async {
        policy
            .execute(async |a| {
                calls.fetch_add(1, Ordering::SeqCst);
                if a.number() < 3 {
                    Err("transient")
                } else {
                    Ok(99u32)
                }
            })
            .await
    };

    tokio::pin!(driver);

    let advancer = async {
        // Release each backoff once the driver has armed its sleep.
        for _ in 0..6 {
            tokio::task::yield_now().await;
        }
        clock.advance(Duration::from_millis(100));
        for _ in 0..6 {
            tokio::task::yield_now().await;
        }
        clock.advance(Duration::from_millis(200));
    };

    let (res, _) = tokio::join!(driver, advancer);
    assert_eq!(res.unwrap(), 99);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn total_timeout_fires() {
    let clock = ManualClock::new();
    let core = TestCore::new(clock.clone());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::fixed(Duration::from_millis(50)).max_attempts(100))
        .total_timeout(Duration::from_millis(120))
        .build_with(core);

    let driver = async { policy.execute(async |_a| Err::<u32, _>("always")).await };
    tokio::pin!(driver);
    let advancer = async {
        for _ in 0..20 {
            tokio::task::yield_now().await;
            clock.advance(Duration::from_millis(50));
        }
    };
    let (res, _) = tokio::join!(driver, advancer);
    let err = res.unwrap_err();
    assert!(err.is_timeout(), "expected total timeout, got {err}");
}

#[tokio::test]
async fn when_predicate_stops_on_permanent() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, i32>::new()
        .retry(
            Retry::fixed(Duration::ZERO)
                .max_attempts(5)
                .when(|e: &i32| *e >= 500),
        )
        .build_with(core);

    let calls = AtomicU32::new(0);
    let res = policy
        .execute(async |_a| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<u32, _>(404)
        })
        .await;
    assert!(res.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
