//! Latency meter seam. A meter is a pure fold applied on call completion, so it
//! is deterministic and unit-testable without a live clock. The built-in
//! peak-EWMA folds ONLY successful calls (F2) — a fast failure is the breaker's
//! concern, never a reason to route MORE traffic to a throttling member.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// One completed-call observation handed to a meter fold.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Observed wall time of the call.
    pub latency: Duration,
    /// `Core::now()` at completion.
    pub at: Instant,
    /// When this member's meter last folded (equals `at` on the first fold).
    pub last_update: Instant,
    /// Outstanding calls at completion (for peak-style meters).
    pub in_flight: usize,
    /// `Ok` vs `Err` outcome — the fold decides what a failure means.
    pub ok: bool,
}

/// Per-member peak-EWMA state. Stored in `MemberState` behind a `Mutex` (F9);
/// `None` until the first successful fold.
#[derive(Debug, Clone, Copy)]
pub struct PeakEwmaState {
    pub(crate) value_secs: f64,
    pub(crate) last_update: Instant,
}

type FoldFn = Arc<dyn Fn(Option<PeakEwmaState>, &Sample) -> PeakEwmaState + Send + Sync>;

/// A latency meter: a pure fold plus whether any selection strategy reading
/// `Candidate::latency()` needs it. Clone-cheap (shares an `Arc`'d fold).
#[derive(Clone)]
pub struct Meter {
    // Read by `fold()`; both are consumed by RouterPolicy (router deliverable, next cohort).
    #[allow(dead_code)]
    fold: FoldFn,
    /// True for meters that drive a latency-aware score; lets the router require
    /// a meter when such a strategy is selected (see the router's build check).
    pub(crate) reads_latency: bool,
}

impl Meter {
    /// Time-decayed peak-EWMA with the given half-life. Folds **only** `ok`
    /// samples: a failure never lowers the reading, so a fast rejection cannot
    /// make a throttling member look "fast" and pull more traffic (F2).
    pub fn peak_ewma(half_life: Duration) -> Self {
        let tau = half_life.as_secs_f64().max(f64::MIN_POSITIVE);
        Self {
            reads_latency: true,
            fold: Arc::new(move |prev, s| {
                if !s.ok {
                    // Failure is the breaker's domain — leave the reading as-is
                    // (seed neutral if this member has never had a successful call).
                    return prev.unwrap_or(PeakEwmaState {
                        value_secs: 0.0,
                        last_update: s.at,
                    });
                }
                let observed = s.latency.as_secs_f64();
                match prev {
                    None => PeakEwmaState {
                        value_secs: observed,
                        last_update: s.at,
                    },
                    Some(p) => {
                        let dt = s.at.saturating_duration_since(p.last_update).as_secs_f64();
                        let w = (-(dt / tau)).exp(); // 1.0 at dt=0, → 0 over time
                        let ewma = p.value_secs * w + observed * (1.0 - w);
                        PeakEwmaState {
                            value_secs: ewma.max(observed), // "peak" term
                            last_update: s.at,
                        }
                    }
                }
            }),
        }
    }

    /// A custom fold. `reads_latency` is true so a latency-aware strategy still
    /// forces a meter to be present; a custom fold that no strategy reads simply
    /// goes unrequired.
    pub fn custom(
        f: impl Fn(Option<PeakEwmaState>, &Sample) -> PeakEwmaState + Send + Sync + 'static,
    ) -> Self {
        Self {
            reads_latency: true,
            fold: Arc::new(f),
        }
    }

    // Consumed by RouterPolicy on call completion (router deliverable, next cohort).
    #[allow(dead_code)]
    pub(crate) fn fold(&self, prev: Option<PeakEwmaState>, s: &Sample) -> PeakEwmaState {
        (self.fold)(prev, s)
    }
}

impl std::fmt::Debug for Meter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meter")
            .field("reads_latency", &self.reads_latency)
            .finish()
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;

    #[test]
    fn peak_ewma_ignores_failed_calls() {
        let m = Meter::peak_ewma(Duration::from_secs(10));
        let t0 = Instant::now();
        // A slow SUCCESS raises the value.
        let st = m.fold(
            None,
            &Sample {
                latency: Duration::from_millis(800),
                at: t0,
                last_update: t0,
                in_flight: 1,
                ok: true,
            },
        );
        let after_success = st.value_secs;
        assert!(after_success >= 0.8, "success should register ~0.8s");
        // A fast FAILURE must NOT lower it (F2).
        let st2 = m.fold(
            Some(st),
            &Sample {
                latency: Duration::from_millis(5),
                at: t0,
                last_update: st.last_update,
                in_flight: 1,
                ok: false,
            },
        );
        assert!(
            st2.value_secs >= after_success,
            "a fast failure must not make the member look fast"
        );
    }

    #[test]
    fn peak_ewma_rises_with_slow_success_and_takes_the_peak() {
        let m = Meter::peak_ewma(Duration::from_secs(10));
        let t0 = Instant::now();
        let s1 = m.fold(
            None,
            &Sample {
                latency: Duration::from_millis(100),
                at: t0,
                last_update: t0,
                in_flight: 1,
                ok: true,
            },
        );
        // A much slower success right after: the peak term keeps value >= observed.
        let t1 = t0 + Duration::from_millis(10);
        let s2 = m.fold(
            Some(s1),
            &Sample {
                latency: Duration::from_millis(900),
                at: t1,
                last_update: s1.last_update,
                in_flight: 1,
                ok: true,
            },
        );
        assert!(
            s2.value_secs >= 0.9,
            "peak term keeps value at/above observed"
        );
    }

    #[test]
    fn first_failure_seeds_neutral_zero() {
        let m = Meter::peak_ewma(Duration::from_secs(10));
        let t0 = Instant::now();
        let st = m.fold(
            None,
            &Sample {
                latency: Duration::from_millis(5),
                at: t0,
                last_update: t0,
                in_flight: 1,
                ok: false,
            },
        );
        assert_eq!(
            st.value_secs, 0.0,
            "a never-succeeded member reads neutral 0"
        );
    }
}
