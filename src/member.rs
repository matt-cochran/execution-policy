//! The registrable member handle and its `Arc`-shared per-member state.
//!
//! A `Member` bundles a target id, its single-target [`ExecutionPolicy`], a load
//! weight, and an `Arc<MemberState>`. Cloning a `Member` shares the state, so the
//! same member registered in multiple routers shares one breaker (via the
//! policy's `Arc<Plan>`) and one load signal (via `Arc<MemberState>`) — see the
//! router's cross-pool invariant.

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use crate::meter::PeakEwmaState;
use crate::policy::ExecutionPolicy;

/// Per-member load/health state shared across every router the member joins.
// Fields consumed by RouterPolicy's choose() + meter fold (router deliverable, next cohort).
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemberState {
    /// Outstanding calls right now (a *signal*, never a cap).
    pub(crate) in_flight: AtomicUsize,
    /// Total times this member was chosen, across ALL pools (global — F11).
    pub(crate) pick_count: AtomicU64,
    /// Latency meter cell; `None` until the first successful fold (F9 — folds
    /// happen under this `Mutex`, off the hot path).
    pub(crate) meter: Mutex<Option<PeakEwmaState>>,
}

impl MemberState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicUsize::new(0),
            pick_count: AtomicU64::new(0),
            meter: Mutex::new(None),
        })
    }
}

/// Rejected non-finite / non-positive weight (F1).
#[derive(Debug, Clone, Copy)]
pub struct WeightError(pub f64);

impl std::fmt::Display for WeightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "member weight must be finite and > 0, got {}", self.0)
    }
}
impl std::error::Error for WeightError {}

/// A registrable member: id + single-target policy + weight + shared state.
///
/// The policy is held behind an `Arc` so `Member: Clone` needs no `C: Clone`
/// bound and every clone shares the *same* `ExecutionPolicy` instance — hence the
/// same breaker — which is the cross-pool health-sharing mechanism (alongside the
/// shared `Arc<MemberState>` for load).
pub struct Member<Id, C, T, E> {
    // Read by RouterPolicy when registering/serving (router deliverable, next cohort).
    #[allow(dead_code)]
    pub(crate) id: Id,
    // Wraps each attempt (router deliverable, next cohort).
    #[allow(dead_code)]
    pub(crate) policy: Arc<ExecutionPolicy<C, T, E>>,
    pub(crate) weight: f64,
    pub(crate) state: Arc<MemberState>,
}

impl<Id: Clone, C, T, E> Clone for Member<Id, C, T, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            policy: Arc::clone(&self.policy), // same policy instance ⇒ shared breaker
            weight: self.weight,
            state: Arc::clone(&self.state), // shared — the cross-pool load signal
        }
    }
}

impl<Id, C, T, E> std::fmt::Debug for Member<Id, C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Member")
            .field("weight", &self.weight)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<Id, C, T, E> Member<Id, C, T, E> {
    /// A new member with weight `1.0` and fresh shared state.
    pub fn new(id: Id, policy: ExecutionPolicy<C, T, E>) -> Self {
        Self {
            id,
            policy: Arc::new(policy),
            weight: 1.0,
            state: MemberState::new(),
        }
    }

    /// Set the load weight. **Panics** on non-finite / non-positive (F1) — a
    /// mis-weighted member corrupts every weighted score, so it fails fast.
    pub fn weight(self, w: f64) -> Self {
        self.try_weight(w).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Fallible weight setter for callers that want a `Result`.
    pub fn try_weight(mut self, w: f64) -> Result<Self, WeightError> {
        if !w.is_finite() || w <= 0.0 {
            return Err(WeightError(w));
        }
        self.weight = w;
        Ok(self)
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::builder::ExecutionPolicyBuilder;
    use crate::core::{ManualClock, TestCore};
    use crate::retry::Retry;

    fn pol() -> ExecutionPolicy<TestCore, u32, u16> {
        ExecutionPolicyBuilder::<u32, u16>::new()
            .retry(Retry::exponential().max_attempts(1))
            .build_with(TestCore::new(ManualClock::new()))
    }

    #[test]
    fn new_member_defaults_to_weight_one_and_clone_shares_state() {
        let m = Member::new("a".to_string(), pol());
        assert_eq!(m.weight, 1.0);
        let c = m.clone();
        assert!(
            Arc::ptr_eq(&m.state, &c.state),
            "clone must share the Arc<MemberState>"
        );
    }

    #[test]
    fn weight_rejects_zero_negative_and_non_finite() {
        assert!(Member::new("a".to_string(), pol()).try_weight(0.0).is_err());
        assert!(
            Member::new("a".to_string(), pol())
                .try_weight(-1.0)
                .is_err()
        );
        assert!(
            Member::new("a".to_string(), pol())
                .try_weight(f64::NAN)
                .is_err()
        );
        assert!(
            Member::new("a".to_string(), pol())
                .try_weight(f64::INFINITY)
                .is_err()
        );
        assert_eq!(
            Member::new("a".to_string(), pol())
                .try_weight(2.5)
                .unwrap()
                .weight,
            2.5
        );
    }

    #[test]
    #[should_panic(expected = "weight must be finite and > 0")]
    fn weight_panics_on_invalid() {
        let _ = Member::new("a".to_string(), pol()).weight(0.0);
    }

    #[test]
    fn fresh_member_state_starts_zeroed() {
        let m = Member::new("a".to_string(), pol());
        use std::sync::atomic::Ordering;
        assert_eq!(m.state.in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(m.state.pick_count.load(Ordering::SeqCst), 0);
        assert!(m.state.meter.lock().unwrap().is_none());
    }
}
