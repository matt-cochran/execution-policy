//! Observability hook. Dependency-free: `on_event` is a plain synchronous
//! callback. `Event` values are constructed **only** when a hook is registered,
//! so the happy path stays allocation-free when no hook is set.

use std::sync::Arc;
use std::time::Duration;

use crate::error::BreakerState;

/// A lifecycle event emitted by the engine when a hook is registered.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// An attempt's operation returned an error that will be classified.
    AttemptFailed { attempt: u32 },
    /// An attempt exceeded `attempt_timeout`.
    AttemptTimedOut { attempt: u32 },
    /// A retry was scheduled after `delay`.
    RetryScheduled { attempt: u32, delay: Duration },
    /// The operation eventually succeeded.
    Succeeded { attempts: u32 },
    /// Retries were exhausted (attempt cap, max_elapsed, or total timeout).
    GaveUp { attempts: u32 },
    /// The circuit breaker changed state.
    CircuitStateChanged { to: BreakerState },
    /// A call was rejected by the concurrency limit.
    ConcurrencyRejected,
    /// A retry was denied because the shared retry budget was exhausted.
    RetryBudgetExhausted { attempts: u32 },
    /// The fallback handler was invoked after a terminal failure.
    FallbackInvoked { attempts: u32 },
}

/// A registered event callback. Synchronous and cheap by contract; a panicking
/// hook is the caller's bug and is **not** caught (fail fast).
pub(crate) type EventHook = Arc<dyn Fn(&Event) + Send + Sync>;

/// Emit `make()`'s event to `hook` — but only construct the event if a hook is
/// present. This is the zero-cost-when-absent guard.
#[inline]
pub(crate) fn emit(hook: &Option<EventHook>, make: impl FnOnce() -> Event) {
    if let Some(h) = hook {
        h(&make());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn emit_skips_when_no_hook() {
        let none: Option<EventHook> = None;
        // make() must not be called — use a side effect to prove it.
        let called = std::cell::Cell::new(false);
        emit(&none, || {
            called.set(true);
            Event::Succeeded { attempts: 1 }
        });
        assert!(
            !called.get(),
            "event must not be constructed without a hook"
        );
    }

    #[test]
    fn emit_invokes_hook() {
        let seen: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let hook: Option<EventHook> = Some(Arc::new(move |e: &Event| {
            sink.lock().unwrap().push(e.clone())
        }));
        emit(&hook, || Event::Succeeded { attempts: 3 });
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[Event::Succeeded { attempts: 3 }]
        );
    }
}
