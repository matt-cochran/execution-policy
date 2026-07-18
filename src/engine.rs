//! The execution engine.
//!
//! Composition order (outermost → innermost), per spec §5:
//! `total_timeout( concurrency( breaker( retry( attempt_timeout( op ) ) ) ) )`.
//!
//! `run_pipeline` applies the concurrency gate (once, for operations scope) and
//! the breaker gate/record around `drive`, the retry loop. Both layers race
//! against the same total deadline. Each attempt's operation future is
//! stack-pinned and raced against the deadlines via a hand-rolled `poll_fn`
//! select — the cancellation seam.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::attempt::Attempt;
use crate::classify::RetryDecision;
use crate::concurrency::{CompiledConcurrency, Permit, Scope};
use crate::core::Core;
use crate::error::{BreakerState, ErrorContext, ExecutionError};
use crate::event::{Event, emit};
use crate::plan::Plan;

enum AttemptOutcome<T, E> {
    Completed(Result<T, E>),
    AttemptTimeout,
    TotalTimeout,
}

fn context(
    attempts: u32,
    start: Instant,
    now: Instant,
    last_delay: Option<Duration>,
    breaker_state: BreakerState,
) -> Box<ErrorContext> {
    Box::new(ErrorContext {
        attempts,
        elapsed: now.duration_since(start),
        last_delay,
        breaker_state,
    })
}

/// Full pipeline: concurrency + breaker layered around the retry loop.
pub(crate) async fn run_pipeline<C, F, T, E>(
    core: &C,
    plan: &Plan<T, E>,
    op: F,
) -> Result<T, ExecutionError<E>>
where
    C: Core,
    F: AsyncFnMut(Attempt) -> Result<T, E>,
{
    let start = core.now();
    let total_deadline = plan.total_timeout.map(|t| start + t);

    // --- Concurrency layer: operations scope acquires one permit per call. ---
    let _op_permit = match &plan.concurrency {
        Some(c) if c.scope == Scope::Operations => {
            match acquire_permit(core, c, total_deadline).await {
                Ok(p) => Some(p),
                Err(()) => {
                    emit(&plan.on_event, || Event::ConcurrencyRejected);
                    return Err(ExecutionError::ConcurrencyRejected {
                        context: context(0, start, core.now(), None, BreakerState::Disabled),
                    });
                }
            }
        }
        _ => None,
    };

    // --- Breaker gate (before running). ---
    let gate_state = if let Some(b) = &plan.breaker {
        match b.runtime.gate(core.now()) {
            Ok(s) => {
                if s == BreakerState::HalfOpen {
                    emit(&plan.on_event, || Event::CircuitStateChanged {
                        to: BreakerState::HalfOpen,
                    });
                }
                s
            }
            Err(()) => {
                return Err(ExecutionError::CircuitOpen {
                    context: context(0, start, core.now(), None, BreakerState::Open),
                });
            }
        }
    } else {
        BreakerState::Disabled
    };

    // --- Retry loop. ---
    let result = drive(core, plan, start, total_deadline, gate_state, op).await;

    // --- Breaker record: one vote on the final pipeline outcome. ---
    if let Some(b) = &plan.breaker {
        let now = core.now();
        let transition = match &result {
            Ok(_) => b.runtime.record_success(now),
            Err(e) if e.is_timeout() => b.runtime.record_failure(now),
            Err(ExecutionError::Operation { source, .. }) => {
                let counts = b.record_when.as_ref().map(|p| p(source)).unwrap_or(true);
                if counts {
                    b.runtime.record_failure(now)
                } else {
                    b.runtime.record_success(now)
                }
            }
            Err(_) => None, // CircuitOpen / Rejected / BudgetExhausted: not an op outcome
        };
        if let Some(to) = transition {
            emit(&plan.on_event, || Event::CircuitStateChanged { to });
        }
    }

    result
}

/// Acquire a concurrency permit honoring the saturation policy and deadline.
async fn acquire_permit<C: Core>(
    core: &C,
    c: &CompiledConcurrency,
    total_deadline: Option<Instant>,
) -> Result<Permit, ()> {
    use crate::concurrency::SaturationPolicy;

    if let Some(p) = c.sem.try_acquire() {
        return Ok(p);
    }
    match c.saturation {
        SaturationPolicy::Reject => Err(()),
        SaturationPolicy::Wait {
            max_queued,
            queue_timeout,
        } => {
            if c.sem.queued() >= max_queued {
                return Err(());
            }
            let acq = c.sem.acquire();
            let mut acq = pin!(acq);
            let mut qto = queue_timeout.map(|d| core.sleep(d));
            let mut dl =
                total_deadline.map(|d| core.sleep(d.saturating_duration_since(core.now())));

            poll_fn(|cx| {
                if let Poll::Ready(p) = acq.as_mut().poll(cx) {
                    return Poll::Ready(Ok(p));
                }
                if let Some(s) = qto.as_mut() {
                    if Pin::new(s).poll(cx).is_ready() {
                        return Poll::Ready(Err(()));
                    }
                }
                if let Some(s) = dl.as_mut() {
                    if Pin::new(s).poll(cx).is_ready() {
                        return Poll::Ready(Err(()));
                    }
                }
                Poll::Pending
            })
            .await
        }
    }
}

/// The retry loop: attempt-timeout, classification, backoff, budget, and
/// attempts-scope concurrency.
#[allow(clippy::too_many_arguments)]
async fn drive<C, F, T, E>(
    core: &C,
    plan: &Plan<T, E>,
    start: Instant,
    total_deadline: Option<Instant>,
    breaker_state: BreakerState,
    mut op: F,
) -> Result<T, ExecutionError<E>>
where
    C: Core,
    F: AsyncFnMut(Attempt) -> Result<T, E>,
{
    let max_attempts = plan.retry.max_attempts_value();
    let budget = plan.retry.budget_ref();
    if let Some(b) = budget {
        b.deposit();
    }

    let mut last_delay: Option<Duration> = None;
    let mut last_error: Option<E> = None;
    let mut attempt_no: u32 = 1;

    loop {
        let now = core.now();
        let attempt = Attempt::new(attempt_no, start, now);

        // Attempts-scope concurrency: hold a permit for this attempt only.
        let _attempt_permit = match &plan.concurrency {
            Some(c) if c.scope == Scope::Attempts => {
                match acquire_permit(core, c, total_deadline).await {
                    Ok(p) => Some(p),
                    Err(()) => {
                        emit(&plan.on_event, || Event::ConcurrencyRejected);
                        return Err(ExecutionError::ConcurrencyRejected {
                            context: context(
                                attempt_no,
                                start,
                                core.now(),
                                last_delay,
                                breaker_state,
                            ),
                        });
                    }
                }
            }
            _ => None,
        };

        let op_fut = op(attempt);
        let mut op_fut = pin!(op_fut);

        // Timers are armed lazily — only once the operation first pends — so a
        // fast success allocates no timer futures even with timeouts configured.
        let mut at_sleep: Option<crate::core::BoxFuture<'_, ()>> = None;
        let mut tot_sleep: Option<crate::core::BoxFuture<'_, ()>> = None;
        let mut armed = false;

        let outcome = poll_fn(|cx| {
            if let Poll::Ready(r) = op_fut.as_mut().poll(cx) {
                return Poll::Ready(AttemptOutcome::Completed(r));
            }
            if !armed {
                armed = true;
                at_sleep = plan.attempt_timeout.map(|t| core.sleep(t));
                tot_sleep =
                    total_deadline.map(|d| core.sleep(d.saturating_duration_since(core.now())));
            }
            if let Some(s) = at_sleep.as_mut() {
                if Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::AttemptTimeout);
                }
            }
            if let Some(s) = tot_sleep.as_mut() {
                if Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::TotalTimeout);
                }
            }
            Poll::Pending
        })
        .await;

        drop(_attempt_permit); // release before backoff sleep

        let now = core.now();
        match outcome {
            AttemptOutcome::TotalTimeout => {
                emit(&plan.on_event, || Event::GaveUp {
                    attempts: attempt_no,
                });
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, now, last_delay, breaker_state),
                });
            }
            AttemptOutcome::Completed(result) => match plan.retry.decide(&result) {
                RetryDecision::Stop => {
                    return match result {
                        Ok(v) => {
                            emit(&plan.on_event, || Event::Succeeded {
                                attempts: attempt_no,
                            });
                            Ok(v)
                        }
                        Err(e) => {
                            emit(&plan.on_event, || Event::GaveUp {
                                attempts: attempt_no,
                            });
                            Err(ExecutionError::Operation {
                                source: e,
                                context: context(attempt_no, start, now, last_delay, breaker_state),
                            })
                        }
                    };
                }
                RetryDecision::Retry => match result {
                    Ok(v) if attempt_no >= max_attempts => {
                        emit(&plan.on_event, || Event::Succeeded {
                            attempts: attempt_no,
                        });
                        return Ok(v);
                    }
                    Err(e) if attempt_no >= max_attempts => {
                        emit(&plan.on_event, || Event::GaveUp {
                            attempts: attempt_no,
                        });
                        return Err(ExecutionError::Operation {
                            source: e,
                            context: context(attempt_no, start, now, last_delay, breaker_state),
                        });
                    }
                    Ok(_) => {}
                    Err(e) => {
                        emit(&plan.on_event, || Event::AttemptFailed {
                            attempt: attempt_no,
                        });
                        last_error = Some(e);
                    }
                },
            },
            AttemptOutcome::AttemptTimeout => {
                emit(&plan.on_event, || Event::AttemptTimedOut {
                    attempt: attempt_no,
                });
                if attempt_no >= max_attempts {
                    emit(&plan.on_event, || Event::GaveUp {
                        attempts: attempt_no,
                    });
                    return Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    });
                }
            }
        }

        // max_elapsed guard.
        if let Some(max_el) = plan.retry.max_elapsed_value() {
            if now.duration_since(start) >= max_el {
                emit(&plan.on_event, || Event::GaveUp {
                    attempts: attempt_no,
                });
                return match last_error.take() {
                    Some(e) => Err(ExecutionError::Operation {
                        source: e,
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    }),
                    None => Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    }),
                };
            }
        }

        // Retry budget: deny a retry storm even if attempts remain.
        if let Some(b) = budget {
            if !b.try_withdraw() {
                emit(&plan.on_event, || Event::RetryBudgetExhausted {
                    attempts: attempt_no,
                });
                return Err(ExecutionError::RetryBudgetExhausted {
                    context: context(attempt_no, start, now, last_delay, breaker_state),
                });
            }
        }

        // Backoff before the next attempt, capped by the total deadline.
        let raw = plan.retry.delay(attempt_no, core, last_error.as_ref());
        last_delay = Some(raw);

        // When a retry-after hint pushes the required delay past the remaining
        // total_timeout budget, stop rather than overshoot.
        if let Some(deadline) = total_deadline {
            let remaining = deadline.saturating_duration_since(core.now());
            // If the hint alone (before jitter reduction) would exceed the
            // remaining budget, bail out now.
            if let Some(ref err) = last_error {
                if let Some(hint) = plan.retry.retry_after_hint(err) {
                    if hint > remaining {
                        emit(&plan.on_event, || Event::GaveUp {
                            attempts: attempt_no,
                        });
                        return Err(ExecutionError::TotalTimeout {
                            context: context(
                                attempt_no,
                                start,
                                core.now(),
                                last_delay,
                                breaker_state,
                            ),
                        });
                    }
                }
            }
        }

        let delay = match total_deadline {
            Some(d) => raw.min(d.saturating_duration_since(core.now())),
            None => raw,
        };
        emit(&plan.on_event, || Event::RetryScheduled {
            attempt: attempt_no,
            delay,
        });
        if !delay.is_zero() {
            core.sleep(delay).await;
        }
        if let Some(d) = total_deadline {
            if core.now() >= d {
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, core.now(), last_delay, breaker_state),
                });
            }
        }

        attempt_no += 1;
    }
}

/// Boxed-op twin of [`run_pipeline`]: identical pipeline, but the op is a
/// `FnMut(Attempt) -> Fut` producing an OWNED future (`Fut: Future`, in practice a
/// [`crate::core::BoxFuture<'static, _>`]) instead of an `AsyncFnMut` whose future
/// borrows the closure. That single (non-HRTB) future type is what lets the engine
/// future stay `Send` for a caller whose own future must be `Send` (a router behind
/// `#[async_trait]` / `tokio::spawn`). Behavior MUST stay identical to `run_pipeline`;
/// the `boxed_pipeline_matches_asyncfnmut_pipeline` differential test guards drift.
/// Full pipeline: concurrency + breaker layered around the retry loop.
pub(crate) async fn run_pipeline_boxed<C, F, Fut, T, E>(
    core: &C,
    plan: &Plan<T, E>,
    op: F,
) -> Result<T, ExecutionError<E>>
where
    C: Core,
    F: FnMut(Attempt) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let start = core.now();
    let total_deadline = plan.total_timeout.map(|t| start + t);

    // --- Concurrency layer: operations scope acquires one permit per call. ---
    let _op_permit = match &plan.concurrency {
        Some(c) if c.scope == Scope::Operations => {
            match acquire_permit(core, c, total_deadline).await {
                Ok(p) => Some(p),
                Err(()) => {
                    emit(&plan.on_event, || Event::ConcurrencyRejected);
                    return Err(ExecutionError::ConcurrencyRejected {
                        context: context(0, start, core.now(), None, BreakerState::Disabled),
                    });
                }
            }
        }
        _ => None,
    };

    // --- Breaker gate (before running). ---
    let gate_state = if let Some(b) = &plan.breaker {
        match b.runtime.gate(core.now()) {
            Ok(s) => {
                if s == BreakerState::HalfOpen {
                    emit(&plan.on_event, || Event::CircuitStateChanged {
                        to: BreakerState::HalfOpen,
                    });
                }
                s
            }
            Err(()) => {
                return Err(ExecutionError::CircuitOpen {
                    context: context(0, start, core.now(), None, BreakerState::Open),
                });
            }
        }
    } else {
        BreakerState::Disabled
    };

    // --- Retry loop. ---
    let result = drive_boxed(core, plan, start, total_deadline, gate_state, op).await;

    // --- Breaker record: one vote on the final pipeline outcome. ---
    if let Some(b) = &plan.breaker {
        let now = core.now();
        let transition = match &result {
            Ok(_) => b.runtime.record_success(now),
            Err(e) if e.is_timeout() => b.runtime.record_failure(now),
            Err(ExecutionError::Operation { source, .. }) => {
                let counts = b.record_when.as_ref().map(|p| p(source)).unwrap_or(true);
                if counts {
                    b.runtime.record_failure(now)
                } else {
                    b.runtime.record_success(now)
                }
            }
            Err(_) => None, // CircuitOpen / Rejected / BudgetExhausted: not an op outcome
        };
        if let Some(to) = transition {
            emit(&plan.on_event, || Event::CircuitStateChanged { to });
        }
    }

    result
}

/// Boxed-op twin of [`drive`] — see [`run_pipeline_boxed`]. Kept byte-for-byte in
/// step with `drive` except the op bound.
/// The retry loop: attempt-timeout, classification, backoff, budget, and
/// attempts-scope concurrency.
#[allow(clippy::too_many_arguments)]
async fn drive_boxed<C, F, Fut, T, E>(
    core: &C,
    plan: &Plan<T, E>,
    start: Instant,
    total_deadline: Option<Instant>,
    breaker_state: BreakerState,
    mut op: F,
) -> Result<T, ExecutionError<E>>
where
    C: Core,
    F: FnMut(Attempt) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let max_attempts = plan.retry.max_attempts_value();
    let budget = plan.retry.budget_ref();
    if let Some(b) = budget {
        b.deposit();
    }

    let mut last_delay: Option<Duration> = None;
    let mut last_error: Option<E> = None;
    let mut attempt_no: u32 = 1;

    loop {
        let now = core.now();
        let attempt = Attempt::new(attempt_no, start, now);

        // Attempts-scope concurrency: hold a permit for this attempt only.
        let _attempt_permit = match &plan.concurrency {
            Some(c) if c.scope == Scope::Attempts => {
                match acquire_permit(core, c, total_deadline).await {
                    Ok(p) => Some(p),
                    Err(()) => {
                        emit(&plan.on_event, || Event::ConcurrencyRejected);
                        return Err(ExecutionError::ConcurrencyRejected {
                            context: context(
                                attempt_no,
                                start,
                                core.now(),
                                last_delay,
                                breaker_state,
                            ),
                        });
                    }
                }
            }
            _ => None,
        };

        let op_fut = op(attempt);
        let mut op_fut = pin!(op_fut);

        // Timers are armed lazily — only once the operation first pends — so a
        // fast success allocates no timer futures even with timeouts configured.
        let mut at_sleep: Option<crate::core::BoxFuture<'_, ()>> = None;
        let mut tot_sleep: Option<crate::core::BoxFuture<'_, ()>> = None;
        let mut armed = false;

        let outcome = poll_fn(|cx| {
            if let Poll::Ready(r) = op_fut.as_mut().poll(cx) {
                return Poll::Ready(AttemptOutcome::Completed(r));
            }
            if !armed {
                armed = true;
                at_sleep = plan.attempt_timeout.map(|t| core.sleep(t));
                tot_sleep =
                    total_deadline.map(|d| core.sleep(d.saturating_duration_since(core.now())));
            }
            if let Some(s) = at_sleep.as_mut() {
                if Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::AttemptTimeout);
                }
            }
            if let Some(s) = tot_sleep.as_mut() {
                if Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::TotalTimeout);
                }
            }
            Poll::Pending
        })
        .await;

        drop(_attempt_permit); // release before backoff sleep

        let now = core.now();
        match outcome {
            AttemptOutcome::TotalTimeout => {
                emit(&plan.on_event, || Event::GaveUp {
                    attempts: attempt_no,
                });
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, now, last_delay, breaker_state),
                });
            }
            AttemptOutcome::Completed(result) => match plan.retry.decide(&result) {
                RetryDecision::Stop => {
                    return match result {
                        Ok(v) => {
                            emit(&plan.on_event, || Event::Succeeded {
                                attempts: attempt_no,
                            });
                            Ok(v)
                        }
                        Err(e) => {
                            emit(&plan.on_event, || Event::GaveUp {
                                attempts: attempt_no,
                            });
                            Err(ExecutionError::Operation {
                                source: e,
                                context: context(attempt_no, start, now, last_delay, breaker_state),
                            })
                        }
                    };
                }
                RetryDecision::Retry => match result {
                    Ok(v) if attempt_no >= max_attempts => {
                        emit(&plan.on_event, || Event::Succeeded {
                            attempts: attempt_no,
                        });
                        return Ok(v);
                    }
                    Err(e) if attempt_no >= max_attempts => {
                        emit(&plan.on_event, || Event::GaveUp {
                            attempts: attempt_no,
                        });
                        return Err(ExecutionError::Operation {
                            source: e,
                            context: context(attempt_no, start, now, last_delay, breaker_state),
                        });
                    }
                    Ok(_) => {}
                    Err(e) => {
                        emit(&plan.on_event, || Event::AttemptFailed {
                            attempt: attempt_no,
                        });
                        last_error = Some(e);
                    }
                },
            },
            AttemptOutcome::AttemptTimeout => {
                emit(&plan.on_event, || Event::AttemptTimedOut {
                    attempt: attempt_no,
                });
                if attempt_no >= max_attempts {
                    emit(&plan.on_event, || Event::GaveUp {
                        attempts: attempt_no,
                    });
                    return Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    });
                }
            }
        }

        // max_elapsed guard.
        if let Some(max_el) = plan.retry.max_elapsed_value() {
            if now.duration_since(start) >= max_el {
                emit(&plan.on_event, || Event::GaveUp {
                    attempts: attempt_no,
                });
                return match last_error.take() {
                    Some(e) => Err(ExecutionError::Operation {
                        source: e,
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    }),
                    None => Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay, breaker_state),
                    }),
                };
            }
        }

        // Retry budget: deny a retry storm even if attempts remain.
        if let Some(b) = budget {
            if !b.try_withdraw() {
                emit(&plan.on_event, || Event::RetryBudgetExhausted {
                    attempts: attempt_no,
                });
                return Err(ExecutionError::RetryBudgetExhausted {
                    context: context(attempt_no, start, now, last_delay, breaker_state),
                });
            }
        }

        // Backoff before the next attempt, capped by the total deadline.
        let raw = plan.retry.delay(attempt_no, core, last_error.as_ref());
        last_delay = Some(raw);

        // When a retry-after hint pushes the required delay past the remaining
        // total_timeout budget, stop rather than overshoot.
        if let Some(deadline) = total_deadline {
            let remaining = deadline.saturating_duration_since(core.now());
            // If the hint alone (before jitter reduction) would exceed the
            // remaining budget, bail out now.
            if let Some(ref err) = last_error {
                if let Some(hint) = plan.retry.retry_after_hint(err) {
                    if hint > remaining {
                        emit(&plan.on_event, || Event::GaveUp {
                            attempts: attempt_no,
                        });
                        return Err(ExecutionError::TotalTimeout {
                            context: context(
                                attempt_no,
                                start,
                                core.now(),
                                last_delay,
                                breaker_state,
                            ),
                        });
                    }
                }
            }
        }

        let delay = match total_deadline {
            Some(d) => raw.min(d.saturating_duration_since(core.now())),
            None => raw,
        };
        emit(&plan.on_event, || Event::RetryScheduled {
            attempt: attempt_no,
            delay,
        });
        if !delay.is_zero() {
            core.sleep(delay).await;
        }
        if let Some(d) = total_deadline {
            if core.now() >= d {
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, core.now(), last_delay, breaker_state),
                });
            }
        }

        attempt_no += 1;
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};
    use crate::retry::Retry;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn plan(
        retry: Retry<u32, &'static str>,
        attempt_to: Option<Duration>,
        total_to: Option<Duration>,
    ) -> Plan<u32, &'static str> {
        Plan {
            retry,
            attempt_timeout: attempt_to,
            total_timeout: total_to,
            breaker: None,
            concurrency: None,
            on_event: None,
        }
    }

    #[tokio::test]
    async fn succeeds_first_try() {
        let core = TestCore::new(ManualClock::new());
        let p = plan(Retry::none(), None, None);
        let r = run_pipeline(&core, &p, async |_a| Ok::<_, &str>(7u32)).await;
        assert_eq!(r.unwrap(), 7);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let core = TestCore::new(ManualClock::new());
        let p = plan(Retry::fixed(Duration::ZERO).max_attempts(3), None, None);
        let calls = AtomicU32::new(0);
        let r = run_pipeline(&core, &p, async |a| {
            calls.fetch_add(1, Ordering::SeqCst);
            if a.number() < 3 {
                Err("transient")
            } else {
                Ok(42u32)
            }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_and_reports_last_error() {
        let core = TestCore::new(ManualClock::new());
        let p = plan(Retry::fixed(Duration::ZERO).max_attempts(2), None, None);
        let r: Result<u32, _> = run_pipeline(&core, &p, async |_a| Err::<u32, _>("always")).await;
        let e = r.unwrap_err();
        assert!(e.is_exhausted());
        assert_eq!(e.context().attempts, 2);
        assert_eq!(e.into_inner(), Some("always"));
    }

    /// Drift guard: `run_pipeline_boxed` is a hand-maintained twin of `run_pipeline`
    /// (only the op bound differs — boxed owned future vs `AsyncFnMut`). Drive the
    /// SAME scenarios through both and assert byte-identical outcomes so the two
    /// retry loops can never silently diverge.
    #[tokio::test]
    async fn boxed_pipeline_matches_asyncfnmut_pipeline() {
        use std::sync::Arc;

        // Scenario A — retries then succeeds: same value + same attempt count.
        let core = TestCore::new(ManualClock::new());
        let pa = plan(Retry::fixed(Duration::ZERO).max_attempts(3), None, None);
        let calls_a = AtomicU32::new(0);
        let ra = run_pipeline(&core, &pa, async |a| {
            calls_a.fetch_add(1, Ordering::SeqCst);
            if a.number() < 3 {
                Err("transient")
            } else {
                Ok(42u32)
            }
        })
        .await;

        let core_b = TestCore::new(ManualClock::new());
        let pb = plan(Retry::fixed(Duration::ZERO).max_attempts(3), None, None);
        let calls_b = Arc::new(AtomicU32::new(0));
        let rb = run_pipeline_boxed(&core_b, &pb, |a: Attempt| {
            let calls = Arc::clone(&calls_b);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if a.number() < 3 {
                    Err("transient")
                } else {
                    Ok(42u32)
                }
            }) as crate::core::BoxFuture<'static, Result<u32, &'static str>>
        })
        .await;

        assert_eq!(ra.unwrap(), rb.unwrap());
        assert_eq!(
            calls_a.load(Ordering::SeqCst),
            calls_b.load(Ordering::SeqCst)
        );
        assert_eq!(calls_b.load(Ordering::SeqCst), 3);

        // Scenario B — exhaustion: same last error + same attempt count.
        let core_c = TestCore::new(ManualClock::new());
        let pc = plan(Retry::fixed(Duration::ZERO).max_attempts(2), None, None);
        let ea: Result<u32, _> =
            run_pipeline(&core_c, &pc, async |_a| Err::<u32, _>("always")).await;
        let core_d = TestCore::new(ManualClock::new());
        let pd = plan(Retry::fixed(Duration::ZERO).max_attempts(2), None, None);
        let eb: Result<u32, _> = run_pipeline_boxed(&core_d, &pd, |_a: Attempt| {
            Box::pin(async { Err::<u32, &'static str>("always") })
                as crate::core::BoxFuture<'static, Result<u32, &'static str>>
        })
        .await;
        let (ea, eb) = (ea.unwrap_err(), eb.unwrap_err());
        assert_eq!(ea.context().attempts, eb.context().attempts);
        assert_eq!(ea.into_inner(), eb.into_inner());
    }

    // ---- retry-after hint tests ----

    /// A minimal error type that optionally carries a server-supplied retry-after
    /// duration — mimics what a caller would extract from an HTTP or gRPC error.
    #[derive(Debug, Clone)]
    struct HintedError {
        hint: Option<Duration>,
    }

    fn hinted_plan(
        retry: Retry<u32, HintedError>,
        total_to: Option<Duration>,
    ) -> Plan<u32, HintedError> {
        Plan {
            retry,
            attempt_timeout: None,
            total_timeout: total_to,
            breaker: None,
            concurrency: None,
            on_event: None,
        }
    }

    /// The delay() method floors to the hint when it exceeds the raw backoff.
    /// This is a pure unit test — no engine, no sleeping.
    #[test]
    fn delay_method_floors_to_hint() {
        let core = TestCore::new(ManualClock::new());
        let retry = Retry::<u32, HintedError>::fixed(Duration::from_millis(100))
            .max_attempts(3)
            .retry_after(|e: &HintedError| e.hint);

        let err = HintedError {
            hint: Some(Duration::from_secs(2)),
        };
        // Raw backoff is 100 ms; hint is 2 s → delay must be ≥ 2 s (before jitter).
        let d = retry.delay(1, &core, Some(&err));
        assert!(
            d >= Duration::from_secs(2),
            "delay {d:?} should be >= hint 2s"
        );
    }

    /// When the hint is smaller than the raw backoff, the backoff wins.
    #[test]
    fn delay_method_keeps_backoff_when_larger_than_hint() {
        let core = TestCore::new(ManualClock::new());
        let retry = Retry::<u32, HintedError>::fixed(Duration::from_secs(5))
            .max_attempts(3)
            .retry_after(|e: &HintedError| e.hint);

        let err = HintedError {
            hint: Some(Duration::from_millis(100)), // hint < backoff
        };
        // Raw backoff is 5 s; hint is 100 ms → backoff wins.
        let d = retry.delay(1, &core, Some(&err));
        assert_eq!(d, Duration::from_secs(5));
    }

    /// When the hint exceeds the remaining total_timeout budget, the engine
    /// stops (TotalTimeout) rather than sleeping past the deadline. The hint
    /// check is synchronous, so no clock advancement is needed.
    #[tokio::test]
    async fn retry_after_hint_exceeding_budget_stops() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());

        let retry = Retry::<u32, HintedError>::fixed(Duration::from_millis(100))
            .max_attempts(5)
            .retry_after(|e: &HintedError| e.hint);

        // total budget: 1 s; hint: 2 s → engine must stop, not sleep past deadline
        let p = hinted_plan(retry, Some(Duration::from_secs(1)));

        let result = run_pipeline(&core, &p, async |_a| {
            Err::<u32, _>(HintedError {
                hint: Some(Duration::from_secs(2)),
            })
        })
        .await;

        // The hint (2 s) exceeds remaining budget (1 s); engine returns TotalTimeout.
        assert!(
            result.unwrap_err().is_timeout(),
            "expected TotalTimeout when hint exceeds remaining budget"
        );
    }

    /// When no extractor is registered, behavior is identical to plain backoff
    /// (zero delay in this case, so no clock advancement is needed).
    #[tokio::test]
    async fn no_extractor_ignores_hint_in_error() {
        let core = TestCore::new(ManualClock::new());

        // No .retry_after() — plain zero-delay fixed backoff.
        let retry = Retry::<u32, HintedError>::fixed(Duration::ZERO).max_attempts(2);
        let p = hinted_plan(retry, None);

        let result = run_pipeline(&core, &p, async |a| {
            if a.number() < 2 {
                // Error carries a huge hint, but no extractor is set.
                Err(HintedError {
                    hint: Some(Duration::from_secs(999)),
                })
            } else {
                Ok(1u32)
            }
        })
        .await;

        // No extractor → hint ignored, zero delay, succeeds normally.
        assert_eq!(result.unwrap(), 1);
    }

    /// When the extractor returns `None`, behavior is identical to plain backoff.
    #[tokio::test]
    async fn none_hint_falls_back_to_plain_backoff() {
        let core = TestCore::new(ManualClock::new());

        // Extractor present but always returns None → zero-delay plain backoff.
        let retry = Retry::<u32, HintedError>::fixed(Duration::ZERO)
            .max_attempts(2)
            .retry_after(|e: &HintedError| e.hint);
        let p = hinted_plan(retry, None);

        let result = run_pipeline(&core, &p, async |a| {
            if a.number() < 2 {
                Err(HintedError { hint: None }) // extractor returns None
            } else {
                Ok(7u32)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 7);
    }
}
