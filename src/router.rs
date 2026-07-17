//! Generic multi-target **router** over N single-target [`ExecutionPolicy`]s.
//!
//! One router over N members, each with its own breaker, selected by a composable
//! [`Pick`] strategy over pollable health + load, advancing to the next member
//! only on a **classified** transient failure, and reporting either the served
//! member's provenance ([`Served`]) or a park hint spanning all members
//! ([`RouterError::AllUnavailable`]).
//!
//! Ordered failover is just `Pick::first_healthy()` (score = index); load
//! balancing is the same router with a different `Pick`. The router deliberately
//! does **not** enforce the cross-member `deadline` or park itself: it surfaces
//! `AllUnavailable { next_available_at }` and exposes [`RouterPolicy::deadline`]
//! so the durable caller enforces the wall-clock budget at its own decision
//! points. The crate stays a pure reliability primitive; durability lives above.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use crate::classify::ErrorPredicate;
use crate::core::Core;
#[cfg(feature = "tokio")]
use crate::core::DefaultCore;
use crate::error::{BreakerState, ExecutionError};
use crate::member::{Member, MemberState};
use crate::meter::{Meter, Sample};
use crate::pick::{Candidate, Pick};
use crate::policy::ExecutionPolicy;

/// Provenance of a successful route: which member served, and after how many
/// members were attempted (`1` == the first attempted member served).
#[derive(Debug, Clone)]
pub struct Served<Id, T> {
    pub value: T,
    pub target: Id,
    pub attempts: u32,
}

/// Terminal failure of a router run.
#[derive(Debug)]
pub enum RouterError<Id, E> {
    /// Every candidate member's breaker is open — nothing was attempted.
    /// `next_available_at` is the soonest a member leaves cooling, so the caller
    /// can park until then. `None` when no cooling member reported an instant.
    AllUnavailable { next_available_at: Option<Instant> },
    /// Members were attempted and none served; carries the last operation error.
    Exhausted(E),
    /// A score closure returned a NaN/infinite value for `id` (F7). A score bug
    /// surfaces loudly rather than as a silent mis-route.
    Score { id: Id, value: f64 },
}

struct Target<Id, C, T, E> {
    id: Id,
    policy: Arc<ExecutionPolicy<C, T, E>>,
    weight: f64,
    state: Arc<MemberState>,
}

/// A router over N members. Cheap to hold; `run` borrows it.
pub struct RouterPolicy<Id, C, T, E> {
    core: C,
    targets: Vec<Target<Id, C, T, E>>,
    select: Pick<Id>,
    meter: Option<Meter>,
    advance_when: ErrorPredicate<E>,
    deadline: Duration,
}

/// Fluent builder for [`RouterPolicy`].
pub struct RouterPolicyBuilder<Id, C, T, E> {
    targets: Vec<Target<Id, C, T, E>>,
    select: Pick<Id>,
    meter: Option<Meter>,
    advance_when: Option<ErrorPredicate<E>>,
    deadline: Duration,
}

impl<Id: std::fmt::Debug, C, T, E> std::fmt::Debug for RouterPolicy<Id, C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterPolicy")
            .field(
                "targets",
                &self.targets.iter().map(|t| &t.id).collect::<Vec<_>>(),
            )
            .field("select", &self.select)
            .field("meter", &self.meter)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl<Id: std::fmt::Debug, C, T, E> std::fmt::Debug for RouterPolicyBuilder<Id, C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterPolicyBuilder")
            .field(
                "targets",
                &self.targets.iter().map(|t| &t.id).collect::<Vec<_>>(),
            )
            .field("select", &self.select)
            .field("meter", &self.meter)
            .field("advance_when", &self.advance_when.as_ref().map(|_| "<fn>"))
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl<Id, C, T, E> RouterPolicy<Id, C, T, E> {
    /// Start configuring a router. Default selection is `Pick::first_healthy()`.
    pub fn builder() -> RouterPolicyBuilder<Id, C, T, E> {
        RouterPolicyBuilder {
            targets: Vec::new(),
            select: Pick::first_healthy(),
            meter: None,
            advance_when: None,
            deadline: Duration::from_secs(3600),
        }
    }

    /// The cross-member budget. The router does not enforce it (see module docs);
    /// the caller reads it to bound waiting at its own park/resume decision points.
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

impl<Id, C, T, E> RouterPolicyBuilder<Id, C, T, E> {
    /// Register a member. Members are `Clone`; register `member.clone()` into every
    /// pool the member serves so its breaker + load state are shared.
    pub fn target(mut self, m: Member<Id, C, T, E>) -> Self {
        self.targets.push(Target {
            id: m.id,
            policy: m.policy,
            weight: m.weight,
            state: m.state,
        });
        self
    }

    /// Choose the selection strategy (default `Pick::first_healthy()`).
    pub fn select(mut self, pick: Pick<Id>) -> Self {
        self.select = pick;
        self
    }

    /// Attach a load meter (required by latency-aware strategies like `peak_ewma`).
    pub fn meter(mut self, m: Meter) -> Self {
        self.meter = Some(m);
        self
    }

    /// Classify which operation errors are transient (advance to the next member).
    /// A non-matching error is permanent: the router fails fast, never burning the
    /// remaining members. **Required** — there is no advance-on-everything default
    /// (F6); pass `|_| true` explicitly to advance on all errors.
    pub fn advance_when(mut self, f: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        self.advance_when = Some(Box::new(f));
        self
    }

    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = d;
        self
    }

    /// Validate and build with a custom [`Core`]. **Panics** on mis-config (§15).
    pub fn build_with(self, core: C) -> RouterPolicy<Id, C, T, E>
    where
        Id: Eq + Hash + std::fmt::Debug,
    {
        // Build-time poka-yoke (§15): every mis-config fails fast, in all builds.
        assert!(
            !self.targets.is_empty(),
            "a RouterPolicy needs at least one target"
        );
        let mut seen = HashSet::with_capacity(self.targets.len());
        for t in &self.targets {
            assert!(seen.insert(&t.id), "duplicate member id: {:?}", t.id); // F3
        }
        assert!(
            self.select.sample_is_valid(),
            "by_sampled_score sample size must be >= 1"
        );
        assert!(
            !(self.select.requires_meter && self.meter.is_none()),
            "selected strategy requires a meter, but none configured; add .meter(Meter::peak_ewma(..))"
        ); // F4
        let advance_when = self.advance_when.expect(
            "advance_when is required — pass |e| ... classifying transient errors, or |_| true to advance on all",
        ); // F6

        RouterPolicy {
            core,
            targets: self.targets,
            select: self.select,
            meter: self.meter,
            advance_when,
            deadline: self.deadline,
        }
    }
}

#[cfg(feature = "tokio")]
impl<Id, T, E> RouterPolicyBuilder<Id, DefaultCore, T, E> {
    /// Validate and build with the default core. **Panics** on mis-config (§15).
    pub fn build(self) -> RouterPolicy<Id, DefaultCore, T, E>
    where
        Id: Eq + Hash + std::fmt::Debug,
    {
        self.build_with(DefaultCore::new())
    }
}

/// Increments in-flight on construction, decrements on drop (panic/early-return
/// safe). Scoped to ONE attempt so a failed-over member is released before the
/// next member is scored (F5).
struct InFlightGuard {
    state: Arc<MemberState>,
}
impl InFlightGuard {
    fn acquire(state: &Arc<MemberState>) -> Self {
        state.in_flight.fetch_add(1, AtomicOrdering::SeqCst);
        Self {
            state: Arc::clone(state),
        }
    }
}
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

/// Draw `k` distinct indices in `0..n` via a partial Fisher–Yates over the
/// `Core` RNG (deterministic under `TestCore`).
fn sample_distinct<C: Core>(n: usize, k: usize, core: &C) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    for i in 0..k {
        let j = i + (core.next_u64() as usize) % (n - i);
        idx.swap(i, j);
    }
    idx.truncate(k);
    idx
}

impl<Id, C, T, E> RouterPolicy<Id, C, T, E>
where
    C: Core,
    Id: Clone,
{
    /// Choose one healthy member: argmin of the `Pick` score (via `total_cmp`,
    /// NaN/inf → `RouterError::Score`), optionally over `k` distinct random draws,
    /// tie-broken by ascending insertion index. Returns the chosen target index.
    fn choose(&self, healthy: &[usize]) -> Result<usize, RouterError<Id, E>> {
        let has_meter = self.meter.is_some();
        let cands: Vec<Candidate<'_, Id>> = healthy
            .iter()
            .map(|&ti| {
                let t = &self.targets[ti];
                Candidate {
                    id: &t.id,
                    index: ti,
                    in_flight: t.state.in_flight.load(AtomicOrdering::SeqCst),
                    weight: t.weight,
                    pick_count: t.state.pick_count.load(AtomicOrdering::SeqCst),
                    latency: if has_meter {
                        Some(
                            t.state
                                .meter
                                .lock()
                                .unwrap()
                                .map(|m| Duration::from_secs_f64(m.value_secs))
                                .unwrap_or(Duration::ZERO),
                        )
                    } else {
                        None
                    },
                }
            })
            .collect();

        // Sampling is WITHOUT replacement (F10); `k >= len` (or `None`) ⇒ all.
        let positions: Vec<usize> = match self.select.sample {
            Some(k) if k < cands.len() => sample_distinct(cands.len(), k, &self.core),
            _ => (0..cands.len()).collect(),
        };

        let mut best: Option<(usize, f64)> = None;
        for &p in &positions {
            let s = (self.select.score)(&cands[p]);
            if !s.is_finite() {
                return Err(RouterError::Score {
                    id: cands[p].id.clone(),
                    value: s,
                }); // F7
            }
            let take = match best {
                None => true,
                Some((bp, bs)) => {
                    s.total_cmp(&bs) == Ordering::Less
                        || (s == bs && cands[p].index < cands[bp].index)
                }
            };
            if take {
                best = Some((p, s));
            }
        }
        Ok(cands[best.expect("healthy set is non-empty").0].index)
    }

    /// Run `op` against members chosen by the `Pick` strategy. `op` receives the
    /// chosen member id by reference.
    ///
    /// - A breaker-open member is skipped (its cooldown feeds the park hint).
    /// - `Ok` → [`Served`] with the member id and how many members were attempted.
    /// - A transient failure (per `advance_when`), timeout, breaker trip, or
    ///   rejection advances to the next-best member.
    /// - A **permanent** operation error fails fast with [`RouterError::Exhausted`].
    /// - Every member cooling → [`RouterError::AllUnavailable`].
    pub async fn run<F>(&self, mut op: F) -> Result<Served<Id, T>, RouterError<Id, E>>
    where
        F: AsyncFnMut(&Id) -> Result<T, E>,
    {
        // Healthy = breaker not Open; collect the soonest cooldown for the park hint.
        let mut soonest: Option<Instant> = None;
        let mut healthy: Vec<usize> = Vec::with_capacity(self.targets.len());
        for (i, t) in self.targets.iter().enumerate() {
            if matches!(t.policy.circuit_state(), Some(BreakerState::Open)) {
                if let Some(until) = t.policy.cooling_until() {
                    soonest = Some(soonest.map_or(until, |s| s.min(until)));
                }
            } else {
                healthy.push(i);
            }
        }
        if healthy.is_empty() {
            return Err(RouterError::AllUnavailable {
                next_available_at: soonest,
            });
        }

        let mut attempts = 0u32;
        let mut last_err: Option<E> = None;

        while !healthy.is_empty() {
            let chosen = self.choose(&healthy)?;
            let target = &self.targets[chosen];
            attempts += 1;

            let attempt_start = self.core.now();
            let guard = InFlightGuard::acquire(&target.state);
            let id_ref = &target.id;
            let outcome = target.policy.run(async || op(id_ref).await).await;

            // Meter fold on completion (F2/F9: only the fold decides what ok means).
            if let Some(meter) = &self.meter {
                let now = self.core.now();
                let mut cell = target.state.meter.lock().unwrap();
                let last = cell.map(|c| c.last_update).unwrap_or(now);
                let sample = Sample {
                    latency: now.saturating_duration_since(attempt_start),
                    at: now,
                    last_update: last,
                    in_flight: target.state.in_flight.load(AtomicOrdering::SeqCst),
                    ok: outcome.is_ok(),
                };
                *cell = Some(meter.fold(*cell, &sample));
            }
            target.state.pick_count.fetch_add(1, AtomicOrdering::SeqCst); // F11
            drop(guard); // decrement BEFORE any advance (F5)

            match outcome {
                Ok(value) => {
                    return Ok(Served {
                        value,
                        target: target.id.clone(),
                        attempts,
                    });
                }
                Err(e) => match &e {
                    // Permanent operation error → fail fast, do not burn the pool.
                    ExecutionError::Operation { source, .. } if !(self.advance_when)(source) => {
                        return Err(RouterError::Exhausted(
                            e.into_inner().expect("Operation variant carries a source"),
                        ));
                    }
                    // Transient / timeout / breaker trip / rejection: advance.
                    _ => {
                        last_err = e.into_inner().or(last_err);
                    }
                },
            }
            healthy.retain(|&i| i != chosen);
        }

        match last_err {
            Some(e) => Err(RouterError::Exhausted(e)),
            None => Err(RouterError::AllUnavailable {
                next_available_at: soonest,
            }),
        }
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::breaker::CircuitBreaker;
    use crate::builder::ExecutionPolicyBuilder;
    use crate::core::{ManualClock, TestCore};
    use crate::retry::Retry;

    fn policy(clock: &ManualClock) -> ExecutionPolicy<TestCore, u32, u16> {
        ExecutionPolicyBuilder::<u32, u16>::new()
            .retry(Retry::exponential().max_attempts(1))
            .build_with(TestCore::new(clock.clone()))
    }

    fn tripping_policy(clock: &ManualClock) -> ExecutionPolicy<TestCore, u32, u16> {
        ExecutionPolicyBuilder::<u32, u16>::new()
            .retry(Retry::exponential().max_attempts(1))
            .circuit_breaker(
                CircuitBreaker::consecutive_failures(1).open_for(Duration::from_secs(47)),
            )
            .build_with(TestCore::new(clock.clone()))
    }

    fn member(clock: &ManualClock, id: &str) -> Member<String, TestCore, u32, u16> {
        Member::new(id.to_string(), policy(clock))
    }

    #[tokio::test]
    async fn first_healthy_serves_and_records_provenance() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(member(&clock, "deepseek"))
            .target(member(&clock, "glm"))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429 || *e >= 500)
            .build_with(TestCore::new(clock.clone()));
        let served = router
            .run(async |id: &String| {
                assert_eq!(id, "deepseek");
                Ok::<u32, u16>(7)
            })
            .await
            .expect("served");
        assert_eq!(served.value, 7);
        assert_eq!(served.target, "deepseek".to_string());
        assert_eq!(served.attempts, 1);
    }

    #[tokio::test]
    async fn ordered_failover_advances_on_transient() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(member(&clock, "primary"))
            .target(member(&clock, "secondary"))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429 || *e >= 500)
            .build_with(TestCore::new(clock.clone()));
        let served = router
            .run(async |id: &String| {
                if id == "primary" {
                    Err::<u32, u16>(503)
                } else {
                    Ok(9)
                }
            })
            .await
            .expect("secondary serves");
        assert_eq!(served.target, "secondary".to_string());
        assert_eq!(served.attempts, 2);
    }

    #[tokio::test]
    async fn permanent_error_does_not_advance() {
        let clock = ManualClock::new();
        let b_called = std::sync::atomic::AtomicBool::new(false);
        let router = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .target(member(&clock, "b"))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429 || *e >= 500) // 400 is permanent
            .build_with(TestCore::new(clock.clone()));
        let out = router
            .run(async |id: &String| {
                if id == "b" {
                    b_called.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                Err::<u32, u16>(400)
            })
            .await;
        assert!(matches!(out, Err(RouterError::Exhausted(400))));
        assert!(!b_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn all_open_returns_park_hint() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(Member::new("a".to_string(), tripping_policy(&clock)))
            .target(Member::new("b".to_string(), tripping_policy(&clock)))
            .select(Pick::first_healthy())
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
        let _ = router
            .run(|_t: &String| async { Err::<u32, u16>(500) })
            .await;
        let out = router.run(|_t: &String| async { Ok::<u32, u16>(1) }).await;
        assert!(matches!(
            out,
            Err(RouterError::AllUnavailable {
                next_available_at: Some(_)
            })
        ));
    }

    #[tokio::test]
    async fn generic_id_carries_typed_provenance() {
        #[derive(Clone, PartialEq, Eq, Hash, Debug)]
        struct Mid(u32);
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(Member::new(Mid(7), policy(&clock)))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429)
            .build_with(TestCore::new(clock.clone()));
        let served = router
            .run(async |id: &Mid| {
                assert_eq!(id, &Mid(7));
                Ok::<u32, u16>(1)
            })
            .await
            .expect("served");
        assert_eq!(served.target, Mid(7));
    }

    #[tokio::test]
    async fn nan_score_fails_fast() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .select(Pick::by_score(|_c| f64::NAN))
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
        let out = router.run(|_id: &String| async { Ok::<u32, u16>(1) }).await;
        assert!(matches!(out, Err(RouterError::Score { .. })), "got {out:?}");
    }

    #[tokio::test]
    async fn in_flight_returns_to_zero_before_failover() {
        let clock = ManualClock::new();
        let a = member(&clock, "a");
        let a_state = Arc::clone(&a.state);
        let router = RouterPolicy::builder()
            .target(a)
            .target(member(&clock, "b"))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429)
            .build_with(TestCore::new(clock.clone()));
        let served = router
            .run(async |id: &String| {
                if id == "a" {
                    assert_eq!(a_state.in_flight.load(AtomicOrdering::SeqCst), 1);
                    Err::<u32, u16>(429)
                } else {
                    assert_eq!(a_state.in_flight.load(AtomicOrdering::SeqCst), 0);
                    Ok(1)
                }
            })
            .await
            .expect("b serves");
        assert_eq!(served.target, "b".to_string());
        assert_eq!(a_state.in_flight.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    #[should_panic(expected = "at least one target")]
    fn zero_targets_panics() {
        let clock = ManualClock::new();
        let _ = RouterPolicy::<String, TestCore, u32, u16>::builder()
            .select(Pick::first_healthy())
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
    }

    #[test]
    #[should_panic(expected = "duplicate member id")]
    fn duplicate_id_panics() {
        let clock = ManualClock::new();
        let _ = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .target(member(&clock, "a"))
            .select(Pick::first_healthy())
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
    }

    #[test]
    #[should_panic(expected = "advance_when is required")]
    fn missing_advance_when_panics() {
        let clock = ManualClock::new();
        let _ = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .select(Pick::first_healthy())
            .build_with(TestCore::new(clock.clone()));
    }

    #[test]
    #[should_panic(expected = "sample size must be >= 1")]
    fn zero_sample_panics() {
        let clock = ManualClock::new();
        let _ = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .select(Pick::by_sampled_score(0, |c| c.in_flight() as f64))
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
    }

    #[test]
    #[should_panic(expected = "requires a meter")]
    fn peak_ewma_without_meter_panics() {
        let clock = ManualClock::new();
        let _ = RouterPolicy::builder()
            .target(member(&clock, "a"))
            .select(Pick::peak_ewma())
            .advance_when(|_e: &u16| true)
            .build_with(TestCore::new(clock.clone()));
    }
}
