//! Load-balancing behavior — the pressure-test. Every strategy test is
//! *discriminating*: it would FAIL under `Pick::first_healthy()` (which sends
//! everything to the first target), so passing proves the strategy actually
//! reshapes the distribution.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{
    ExecutionPolicy, ExecutionPolicyBuilder, Member, Meter, Pick, Retry, RouterPolicy,
};

fn policy(clock: &ManualClock) -> ExecutionPolicy<TestCore, u32, u16> {
    ExecutionPolicyBuilder::<u32, u16>::new()
        .retry(Retry::exponential().max_attempts(1))
        .build_with(TestCore::new(clock.clone()))
}

fn member(clock: &ManualClock, id: &str) -> Member<String, TestCore, u32, u16> {
    Member::new(id.to_string(), policy(clock))
}

/// Round-robin (least global `pick_count`) spreads sequential calls evenly.
/// `first_healthy` would put all 30 on "a".
#[tokio::test]
async fn round_robin_spreads_evenly() {
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(member(&clock, "a"))
        .target(member(&clock, "b"))
        .target(member(&clock, "c"))
        .select(Pick::round_robin())
        .advance_when(|_e: &u16| true)
        .build_with(TestCore::new(clock.clone()));

    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..30 {
        let s = router
            .run(|_id: &String| async { Ok::<u32, u16>(1) })
            .await
            .unwrap();
        *counts.entry(s.target).or_default() += 1;
    }
    let max = *counts.values().max().unwrap();
    let min = *counts.values().min().unwrap();
    assert_eq!(counts.len(), 3, "all three members used: {counts:?}");
    assert!(max - min <= 1, "round-robin spread too wide: {counts:?}");
}

/// P2C is deterministic under a fixed seed (two identically-seeded routers pick
/// alike), samples DISTINCT indices, and does not collapse to one target.
#[tokio::test]
async fn p2c_is_deterministic_distinct_and_non_degenerate() {
    fn build(clock: &ManualClock) -> RouterPolicy<String, TestCore, u32, u16> {
        RouterPolicy::builder()
            .target(member(clock, "a"))
            .target(member(clock, "b"))
            .target(member(clock, "c"))
            .target(member(clock, "d"))
            .select(Pick::p2c())
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()))
    }
    let (c1, c2) = (ManualClock::new(), ManualClock::new());
    let (r1, r2) = (build(&c1), build(&c2));
    let mut seen = HashSet::new();
    for _ in 0..20 {
        let a = r1
            .run(|_i: &String| async { Ok::<u32, u16>(1) })
            .await
            .unwrap()
            .target;
        let b = r2
            .run(|_i: &String| async { Ok::<u32, u16>(1) })
            .await
            .unwrap()
            .target;
        assert_eq!(a, b, "same seed must pick alike");
        seen.insert(a);
    }
    assert!(
        seen.len() > 1,
        "p2c must not collapse to one target: {seen:?}"
    );
}

/// Peak-EWMA sheds share from a member whose *successful* calls are slow.
/// `first_healthy`/`round_robin` would not react to latency.
#[tokio::test]
async fn peak_ewma_sheds_share_from_slow_member() {
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(member(&clock, "fast"))
        .target(member(&clock, "slow"))
        .select(Pick::peak_ewma())
        .meter(Meter::peak_ewma(Duration::from_secs(10)))
        .advance_when(|_e: &u16| true)
        .build_with(TestCore::new(clock.clone()));

    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..12 {
        let s = router
            .run(async |id: &String| {
                // The op drives virtual time: "slow" takes 500ms, "fast" 10ms.
                clock.advance(if id == "slow" {
                    Duration::from_millis(500)
                } else {
                    Duration::from_millis(10)
                });
                Ok::<u32, u16>(1)
            })
            .await
            .unwrap();
        *counts.entry(s.target).or_default() += 1;
    }
    let fast = counts.get("fast").copied().unwrap_or(0);
    let slow = counts.get("slow").copied().unwrap_or(0);
    assert!(
        fast > slow,
        "peak_ewma should shed the slow member: {counts:?}"
    );
}

/// A member registered in two routers shares one in-flight signal: while
/// `router_a` holds a call on shared member "m", `router_b` (over "m" and "n",
/// least-in-flight) routes to "n" — which only happens if it sees m's shared
/// in-flight. If state were per-router, router_b would pick "m" (tie → index 0).
#[tokio::test]
async fn cross_pool_shares_in_flight_signal() {
    let clock = ManualClock::new();
    let shared = member(&clock, "m");

    let router_a = RouterPolicy::builder()
        .target(shared.clone())
        .select(Pick::least_in_flight())
        .advance_when(|_e: &u16| true)
        .build_with(TestCore::new(clock.clone()));
    let router_b = RouterPolicy::builder()
        .target(shared) // same member, shared Arc<MemberState>
        .target(member(&clock, "n"))
        .select(Pick::least_in_flight())
        .advance_when(|_e: &u16| true)
        .build_with(TestCore::new(clock.clone()));

    let notify = tokio::sync::Notify::new();
    let hold = router_a.run(async |_id: &String| {
        notify.notified().await;
        Ok::<u32, u16>(1)
    });
    let probe = async {
        tokio::task::yield_now().await; // let router_a take m's in-flight first
        let served = router_b
            .run(|_id: &String| async { Ok::<u32, u16>(2) })
            .await
            .unwrap();
        notify.notify_one();
        served.target
    };
    let (held, target_b) = tokio::join!(hold, probe);
    held.unwrap();
    assert_eq!(
        target_b, "n",
        "router_b must route away from the shared in-flight member"
    );
}

/// Weighted least-in-flight fills a weight-2 member ~2:1 vs a weight-1 member
/// under concurrent load. Unweighted LIF would fill 3:3; `first_healthy` 6:0.
#[tokio::test]
async fn weighted_least_in_flight_favors_heavier_under_load() {
    let clock = ManualClock::new();
    let heavy = Member::new("heavy".to_string(), policy(&clock)).weight(2.0);
    let light = Member::new("light".to_string(), policy(&clock));
    let router = RouterPolicy::builder()
        .target(heavy)
        .target(light)
        .select(Pick::weighted_least_in_flight())
        .advance_when(|_e: &u16| true)
        .build_with(TestCore::new(clock.clone()));

    let notify = tokio::sync::Notify::new();
    let call = || {
        router.run(async |_id: &String| {
            notify.notified().await;
            Ok::<u32, u16>(1)
        })
    };
    // Six concurrent in-flight calls; the fill order under a single-threaded
    // runtime is deterministic (heavy, heavy, light, heavy, heavy, light).
    let releaser = async {
        tokio::task::yield_now().await;
        notify.notify_waiters();
    };
    let (r1, r2, r3, r4, r5, r6, ()) =
        tokio::join!(call(), call(), call(), call(), call(), call(), releaser);

    let mut counts: HashMap<String, u32> = HashMap::new();
    for t in [r1, r2, r3, r4, r5, r6] {
        *counts.entry(t.unwrap().target).or_default() += 1;
    }
    assert_eq!(
        counts.get("heavy").copied().unwrap_or(0),
        4,
        "counts={counts:?}"
    );
    assert_eq!(
        counts.get("light").copied().unwrap_or(0),
        2,
        "counts={counts:?}"
    );
}
