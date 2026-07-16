//! Generic multi-target fallback over N single-target [`ExecutionPolicy`]s.
//!
//! `execution-policy`'s single-target primitive does retry/backoff/breaker for
//! **one call to one target**; multi-target *selection* and *fallback* were left
//! to `.or_else` at the call site (README non-goal). This module brings that
//! abstraction into the crate: one router over N targets, each with its own
//! breaker, selected by pollable health, advancing to the next target only on a
//! **classified** transient failure, and reporting either the served target's
//! provenance ([`Served`]) or a park hint spanning all targets
//! ([`FallbackError::AllUnavailable`]).
//!
//! The router deliberately does **not** enforce the cross-target `deadline` or
//! park itself: it surfaces `AllUnavailable { next_available_at }` and exposes
//! [`FallbackPolicy::deadline`] so the caller (which owns the durable queue) can
//! park work and enforce the wall-clock budget at its own decision points. The
//! crate stays a pure reliability primitive; durability lives above it.

use std::time::{Duration, Instant};

use crate::classify::ErrorPredicate;
use crate::core::Core;
use crate::error::{BreakerState, ExecutionError};
use crate::policy::ExecutionPolicy;

/// Provenance of a successful fallback: which target served, and after how many
/// targets were attempted (`1` == the first attempted target served).
#[derive(Debug, Clone)]
pub struct Served<T> {
    pub value: T,
    pub target: String,
    pub attempts: u32,
}

/// Terminal failure of a fallback run.
#[derive(Debug)]
pub enum FallbackError<E> {
    /// Every candidate target's breaker is open — nothing was attempted.
    /// `next_available_at` is the soonest a target leaves cooling, so the caller
    /// can park until then. `None` when no cooling target reported an instant
    /// (e.g. no breakers configured).
    AllUnavailable { next_available_at: Option<Instant> },
    /// Targets were attempted and none served; carries the last operation error.
    Exhausted(E),
}

/// How a target is chosen. One strategy today; the enum leaves room to grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// The first target, in insertion order, whose breaker is not open.
    FirstHealthy,
}

struct Target<C, T, E> {
    id: String,
    policy: ExecutionPolicy<C, T, E>,
}

/// A router over N [`ExecutionPolicy`] targets. Cheap to hold; `run` borrows it.
pub struct FallbackPolicy<C, T, E> {
    targets: Vec<Target<C, T, E>>,
    selection: Selection,
    advance_when: ErrorPredicate<E>,
    deadline: Duration,
}

/// Fluent builder for [`FallbackPolicy`].
pub struct FallbackPolicyBuilder<C, T, E> {
    targets: Vec<Target<C, T, E>>,
    selection: Selection,
    advance_when: Option<ErrorPredicate<E>>,
    deadline: Duration,
}

impl<C, T, E> std::fmt::Debug for FallbackPolicy<C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallbackPolicy")
            .field(
                "targets",
                &self.targets.iter().map(|t| &t.id).collect::<Vec<_>>(),
            )
            .field("selection", &self.selection)
            .field("advance_when", &"<fn>")
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl<C, T, E> std::fmt::Debug for FallbackPolicyBuilder<C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallbackPolicyBuilder")
            .field(
                "targets",
                &self.targets.iter().map(|t| &t.id).collect::<Vec<_>>(),
            )
            .field("selection", &self.selection)
            .field("advance_when", &self.advance_when.as_ref().map(|_| "<fn>"))
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl<C, T, E> FallbackPolicy<C, T, E> {
    /// Start configuring a router.
    pub fn builder() -> FallbackPolicyBuilder<C, T, E> {
        FallbackPolicyBuilder {
            targets: Vec::new(),
            selection: Selection::FirstHealthy,
            advance_when: None,
            deadline: Duration::from_secs(3600),
        }
    }

    /// The configured selection strategy.
    pub fn selection(&self) -> Selection {
        self.selection
    }

    /// The cross-target budget. The router does not enforce it (see module docs);
    /// the caller reads it to bound waiting at its own park/resume decision points.
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

impl<C, T, E> FallbackPolicyBuilder<C, T, E> {
    /// Add a target with a stable id and its per-target reliability policy.
    pub fn target(mut self, id: impl Into<String>, policy: ExecutionPolicy<C, T, E>) -> Self {
        self.targets.push(Target {
            id: id.into(),
            policy,
        });
        self
    }
    pub fn select(mut self, s: Selection) -> Self {
        self.selection = s;
        self
    }
    /// Classify which operation errors are transient (advance to the next target).
    /// A non-matching error is permanent: the router fails fast, never burning the
    /// remaining targets. Without this, the default advances on every error.
    pub fn advance_when(mut self, f: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        self.advance_when = Some(Box::new(f));
        self
    }
    pub fn deadline(mut self, d: Duration) -> Self {
        self.deadline = d;
        self
    }
    pub fn build(self) -> FallbackPolicy<C, T, E> {
        FallbackPolicy {
            targets: self.targets,
            selection: self.selection,
            advance_when: self.advance_when.unwrap_or_else(|| Box::new(|_| true)),
            deadline: self.deadline,
        }
    }
}

impl<C, T, E> FallbackPolicy<C, T, E>
where
    C: Core,
{
    /// Run `op` against targets in `Selection` order. `op` receives the target id.
    ///
    /// - A breaker-open target is skipped (its cooldown feeds the park hint).
    /// - `Ok` → [`Served`] with the target id and how many targets were attempted.
    /// - A transient operation error (per `advance_when`), or a timeout / breaker
    ///   trip / rejection, advances to the next target.
    /// - A **permanent** operation error fails fast with [`FallbackError::Exhausted`]
    ///   — it never advances the chain.
    /// - Every target skipped (all cooling) → [`FallbackError::AllUnavailable`].
    pub async fn run<F>(&self, mut op: F) -> Result<Served<T>, FallbackError<E>>
    where
        F: AsyncFnMut(&str) -> Result<T, E>,
    {
        let Selection::FirstHealthy = self.selection;
        let mut attempts = 0u32;
        let mut soonest: Option<Instant> = None;
        let mut last_err: Option<E> = None;

        for target in &self.targets {
            if matches!(target.policy.circuit_state(), Some(BreakerState::Open)) {
                if let Some(until) = target.policy.cooling_until() {
                    soonest = Some(soonest.map_or(until, |s| s.min(until)));
                }
                continue;
            }
            attempts += 1;
            let id = target.id.clone();
            match target.policy.run(async || op(id.as_str()).await).await {
                Ok(value) => {
                    return Ok(Served {
                        value,
                        target: id,
                        attempts,
                    });
                }
                Err(e) => match &e {
                    // Permanent operation error → fail fast, do not burn the chain.
                    ExecutionError::Operation { source, .. } if !(self.advance_when)(source) => {
                        return Err(FallbackError::Exhausted(
                            e.into_inner().expect("Operation variant carries a source"),
                        ));
                    }
                    // Transient operation error, timeout, breaker trip, rejection,
                    // budget: advance to the next target, remembering any source.
                    _ => {
                        last_err = e.into_inner().or(last_err);
                    }
                },
            }
        }

        match last_err {
            Some(e) => Err(FallbackError::Exhausted(e)),
            None => Err(FallbackError::AllUnavailable {
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
    use std::sync::atomic::{AtomicBool, Ordering};

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

    #[tokio::test]
    async fn first_healthy_target_serves_and_records_provenance() {
        let clock = ManualClock::new();
        let router = FallbackPolicy::builder()
            .target("deepseek", policy(&clock))
            .target("glm", policy(&clock))
            .select(Selection::FirstHealthy)
            .advance_when(|e: &u16| *e == 429 || *e >= 500)
            .deadline(Duration::from_secs(3600))
            .build();

        let served = router
            .run(async |target: &str| {
                assert_eq!(target, "deepseek"); // first healthy target
                Ok::<u32, u16>(7)
            })
            .await
            .expect("served");
        assert_eq!(served.value, 7);
        assert_eq!(served.target, "deepseek");
        assert_eq!(served.attempts, 1);
    }

    #[tokio::test]
    async fn permanent_error_does_not_advance_the_chain() {
        let clock = ManualClock::new();
        let router = FallbackPolicy::builder()
            .target("a", policy(&clock))
            .target("b", policy(&clock))
            .select(Selection::FirstHealthy)
            .advance_when(|e: &u16| *e == 429 || *e >= 500) // 400 is NOT transient
            .deadline(Duration::from_secs(3600))
            .build();

        let b_called = AtomicBool::new(false);
        let out = router
            .run(async |target: &str| {
                if target == "b" {
                    b_called.store(true, Ordering::SeqCst);
                }
                Err::<u32, u16>(400) // permanent
            })
            .await;
        assert!(matches!(out, Err(FallbackError::Exhausted(400))));
        assert!(
            !b_called.load(Ordering::SeqCst),
            "permanent error on 'a' must NOT advance to 'b'"
        );
    }

    #[tokio::test]
    async fn all_targets_open_returns_park_hint() {
        let clock = ManualClock::new();
        let router = FallbackPolicy::builder()
            .target("a", tripping_policy(&clock))
            .target("b", tripping_policy(&clock))
            .select(Selection::FirstHealthy)
            .advance_when(|_e: &u16| true)
            .deadline(Duration::from_secs(3600))
            .build();

        // First run trips both breakers (consecutive_failures(1)).
        let _ = router.run(|_t: &str| async { Err::<u32, u16>(500) }).await;

        // Now both are Open; the next run short-circuits to AllUnavailable with
        // the soonest cooldown instant as the park hint.
        let out = router.run(|_t: &str| async { Ok::<u32, u16>(1) }).await;
        match out {
            Err(FallbackError::AllUnavailable {
                next_available_at: Some(_),
            }) => {}
            other => panic!("expected AllUnavailable with a hint, got {other:?}"),
        }
    }
}
