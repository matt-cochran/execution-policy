//! Circuit breaker: a fast lock-free closed-state gate check (atomic state load)
//! with the bookkeeping mutex taken only on the once-per-call record path.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use crate::classify::ErrorPredicate;
use crate::error::BreakerState;

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;

/// What trips the breaker.
#[derive(Debug, Clone, Copy)]
enum Trip {
    Consecutive {
        threshold: u32,
    },
    FailureRatio {
        ratio: f64,
        min_throughput: u32,
        window: Duration,
    },
}

/// Circuit breaker configuration + (after `build`) live state.
///
/// Build with [`CircuitBreaker::consecutive_failures`] or
/// [`CircuitBreaker::failure_ratio`]. `record_when` (optional) decides which
/// operation errors count as breaker faults; by default every error counts.
pub struct CircuitBreaker<E> {
    trip: Trip,
    open_for: Duration,
    half_open_max_calls: u32,
    record_when: Option<ErrorPredicate<E>>,
}

impl<E> std::fmt::Debug for CircuitBreaker<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("trip", &self.trip)
            .field("open_for", &self.open_for)
            .field("half_open_max_calls", &self.half_open_max_calls)
            .field("record_when", &self.record_when.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl<E> CircuitBreaker<E> {
    /// Trip after `n` consecutive failures.
    pub fn consecutive_failures(n: u32) -> Self {
        Self {
            trip: Trip::Consecutive {
                threshold: n.max(1),
            },
            open_for: Duration::from_secs(30),
            half_open_max_calls: 1,
            record_when: None,
        }
    }

    /// Trip when the failure ratio over a sampling window is exceeded.
    pub fn failure_ratio() -> Self {
        Self {
            trip: Trip::FailureRatio {
                ratio: 0.5,
                min_throughput: 10,
                window: Duration::from_secs(30),
            },
            open_for: Duration::from_secs(30),
            half_open_max_calls: 1,
            record_when: None,
        }
    }

    pub fn failure_ratio_value(mut self, ratio: f64) -> Self {
        if let Trip::FailureRatio { ratio: r, .. } = &mut self.trip {
            *r = ratio.clamp(0.0, 1.0);
        }
        self
    }
    /// Alias matching the spec's fluent name `.failure_ratio(0.5)`.
    pub fn ratio(self, ratio: f64) -> Self {
        self.failure_ratio_value(ratio)
    }
    pub fn minimum_throughput(mut self, n: u32) -> Self {
        if let Trip::FailureRatio { min_throughput, .. } = &mut self.trip {
            *min_throughput = n;
        }
        self
    }
    pub fn sampling_window(mut self, d: Duration) -> Self {
        if let Trip::FailureRatio { window, .. } = &mut self.trip {
            *window = d;
        }
        self
    }
    pub fn open_for(mut self, d: Duration) -> Self {
        self.open_for = d;
        self
    }
    pub fn half_open_max_calls(mut self, n: u32) -> Self {
        self.half_open_max_calls = n.max(1);
        self
    }
    pub fn record_when(mut self, pred: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        self.record_when = Some(Box::new(pred));
        self
    }

    /// Compile into a live runtime state machine.
    pub(crate) fn compile(self) -> (BreakerRuntime, Option<ErrorPredicate<E>>) {
        let rt = BreakerRuntime::new(self.trip, self.open_for, self.half_open_max_calls);
        (rt, self.record_when)
    }
}

/// Live breaker state shared behind an `Arc`.
#[derive(Debug)]
pub(crate) struct BreakerRuntime {
    state: AtomicU8,
    trip: Trip,
    open_for: Duration,
    half_open_max_calls: u32,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    open_until: Option<Instant>,
    consecutive_failures: u32,
    half_open_in_flight: u32,
    half_open_successes: u32,
    window: Window,
}

impl BreakerRuntime {
    fn new(trip: Trip, open_for: Duration, half_open_max_calls: u32) -> Self {
        let buckets = match trip {
            Trip::FailureRatio { window, .. } => Window::new(window),
            Trip::Consecutive { .. } => Window::new(Duration::from_secs(1)),
        };
        Self {
            state: AtomicU8::new(CLOSED),
            trip,
            open_for,
            half_open_max_calls,
            inner: Mutex::new(Inner {
                open_until: None,
                consecutive_failures: 0,
                half_open_in_flight: 0,
                half_open_successes: 0,
                window: buckets,
            }),
        }
    }

    /// Current public state, as last committed by a call (`gate`/`record_*`).
    ///
    /// This is the raw latched state and does NOT account for a cooldown that
    /// has elapsed without an intervening call — prefer [`state_at`] for a
    /// clock-accurate view when polling for a healthy target.
    ///
    /// [`state_at`]: Self::state_at
    #[cfg(test)]
    pub(crate) fn state(&self) -> BreakerState {
        match self.state.load(Ordering::Acquire) {
            OPEN => BreakerState::Open,
            HALF_OPEN => BreakerState::HalfOpen,
            _ => BreakerState::Closed,
        }
    }

    /// Reported state as a pure function of `now`.
    ///
    /// Unlike [`state`], this reports `HalfOpen` as soon as an open breaker's
    /// cooldown has elapsed — without waiting for a call to arrive and drive
    /// the lazy transition in [`gate`]. This makes breaker health *schedulable*:
    /// a poller selecting a recovered target sees the change on time. The actual
    /// state transition (and half-open probe accounting) still happens in
    /// [`gate`]; this method never mutates.
    ///
    /// [`state`]: Self::state
    /// [`gate`]: Self::gate
    pub(crate) fn state_at(&self, now: Instant) -> BreakerState {
        match self.state.load(Ordering::Acquire) {
            OPEN => {
                let inner = self.inner.lock().unwrap();
                match inner.open_until {
                    Some(t) if now >= t => BreakerState::HalfOpen,
                    _ => BreakerState::Open,
                }
            }
            HALF_OPEN => BreakerState::HalfOpen,
            _ => BreakerState::Closed,
        }
    }

    /// The instant at which the breaker stops cooling (leaves `Open`), while it
    /// is still cooling. Returns `None` when closed, half-open, or when the
    /// cooldown has already elapsed (i.e. ready to probe).
    pub(crate) fn cooling_until(&self, now: Instant) -> Option<Instant> {
        if self.state.load(Ordering::Acquire) == OPEN {
            let inner = self.inner.lock().unwrap();
            inner.open_until.filter(|t| now < *t)
        } else {
            None
        }
    }

    /// Gate a call. `Ok(state)` allows it; `Err(())` means reject (circuit open).
    /// Lock-free fast path when closed.
    pub(crate) fn gate(&self, now: Instant) -> Result<BreakerState, ()> {
        if self.state.load(Ordering::Acquire) == CLOSED {
            return Ok(BreakerState::Closed);
        }
        let mut inner = self.inner.lock().unwrap();
        match self.state.load(Ordering::Acquire) {
            CLOSED => Ok(BreakerState::Closed),
            OPEN => {
                let ready = inner.open_until.map(|t| now >= t).unwrap_or(true);
                if ready {
                    self.state.store(HALF_OPEN, Ordering::Release);
                    inner.half_open_in_flight = 1;
                    inner.half_open_successes = 0;
                    Ok(BreakerState::HalfOpen)
                } else {
                    Err(())
                }
            }
            _ => {
                // HALF_OPEN — admit up to half_open_max_calls probes.
                if inner.half_open_in_flight < self.half_open_max_calls {
                    inner.half_open_in_flight += 1;
                    Ok(BreakerState::HalfOpen)
                } else {
                    Err(())
                }
            }
        }
    }

    /// Record a success. Returns the new state if a transition occurred.
    pub(crate) fn record_success(&self, now: Instant) -> Option<BreakerState> {
        let mut inner = self.inner.lock().unwrap();
        match self.state.load(Ordering::Acquire) {
            HALF_OPEN => {
                inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
                inner.half_open_successes += 1;
                if inner.half_open_successes >= self.half_open_max_calls {
                    self.close(&mut inner);
                    return Some(BreakerState::Closed);
                }
                None
            }
            _ => {
                inner.consecutive_failures = 0;
                inner.window.record(now, false);
                None
            }
        }
    }

    /// Record a failure. Returns the new state if a transition occurred.
    pub(crate) fn record_failure(&self, now: Instant) -> Option<BreakerState> {
        let mut inner = self.inner.lock().unwrap();
        match self.state.load(Ordering::Acquire) {
            HALF_OPEN => {
                inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
                self.open(&mut inner, now);
                Some(BreakerState::Open)
            }
            _ => {
                inner.consecutive_failures += 1;
                inner.window.record(now, true);
                if self.should_trip(&mut inner, now) {
                    self.open(&mut inner, now);
                    Some(BreakerState::Open)
                } else {
                    None
                }
            }
        }
    }

    fn should_trip(&self, inner: &mut Inner, now: Instant) -> bool {
        match self.trip {
            Trip::Consecutive { threshold } => inner.consecutive_failures >= threshold,
            Trip::FailureRatio {
                ratio,
                min_throughput,
                ..
            } => {
                let (failures, total) = inner.window.totals(now);
                total >= min_throughput as u64 && (failures as f64 / total as f64) >= ratio
            }
        }
    }

    fn open(&self, inner: &mut Inner, now: Instant) {
        self.state.store(OPEN, Ordering::Release);
        inner.open_until = Some(now + self.open_for);
        inner.half_open_successes = 0;
        inner.half_open_in_flight = 0;
    }

    fn close(&self, inner: &mut Inner) {
        self.state.store(CLOSED, Ordering::Release);
        inner.consecutive_failures = 0;
        inner.half_open_successes = 0;
        inner.half_open_in_flight = 0;
        inner.window.clear();
    }
}

/// A time-bucketed sliding window of (failure, total) counts.
#[derive(Debug)]
struct Window {
    span: Duration,
    bucket: Duration,
    buckets: Vec<(Instant, u64, u64)>, // (bucket_start, failures, total)
}

impl Window {
    fn new(span: Duration) -> Self {
        let n = 10u32;
        let bucket = (span / n).max(Duration::from_millis(1));
        Self {
            span,
            bucket,
            buckets: Vec::with_capacity(n as usize + 1),
        }
    }

    fn clear(&mut self) {
        self.buckets.clear();
    }

    fn record(&mut self, now: Instant, failure: bool) {
        self.evict(now);
        let start = now;
        match self.buckets.last_mut() {
            Some((b_start, fails, total)) if now.duration_since(*b_start) < self.bucket => {
                *total += 1;
                if failure {
                    *fails += 1;
                }
            }
            _ => {
                self.buckets.push((start, u64::from(failure), 1));
            }
        }
    }

    fn totals(&mut self, now: Instant) -> (u64, u64) {
        self.evict(now);
        self.buckets
            .iter()
            .fold((0, 0), |(f, t), (_, fails, total)| (f + fails, t + total))
    }

    fn evict(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.span);
        if let Some(cutoff) = cutoff {
            self.buckets.retain(|(start, _, _)| *start >= cutoff);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn consecutive_trips_after_threshold() {
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(3)
            .open_for(Duration::from_secs(10))
            .compile();
        let now = t0();
        assert_eq!(rt.gate(now), Ok(BreakerState::Closed));
        rt.record_failure(now);
        rt.record_failure(now);
        assert_eq!(rt.state(), BreakerState::Closed);
        rt.record_failure(now); // 3rd → trip
        assert_eq!(rt.state(), BreakerState::Open);
        assert_eq!(rt.gate(now), Err(())); // rejected while open
    }

    #[test]
    fn success_resets_consecutive() {
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(3).compile();
        let now = t0();
        rt.record_failure(now);
        rt.record_failure(now);
        rt.record_success(now);
        rt.record_failure(now);
        rt.record_failure(now);
        assert_eq!(rt.state(), BreakerState::Closed); // never hit 3 in a row
    }

    #[test]
    fn half_open_probe_then_close_on_success() {
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(2)
            .open_for(Duration::from_secs(5))
            .half_open_max_calls(1)
            .compile();
        let now = t0();
        rt.record_failure(now);
        rt.record_failure(now);
        assert_eq!(rt.state(), BreakerState::Open);
        // Before open_for elapses: still rejected.
        assert_eq!(rt.gate(now + Duration::from_secs(1)), Err(()));
        // After open_for: half-open probe admitted.
        let later = now + Duration::from_secs(6);
        assert_eq!(rt.gate(later), Ok(BreakerState::HalfOpen));
        rt.record_success(later);
        assert_eq!(rt.state(), BreakerState::Closed);
    }

    #[test]
    fn half_open_failure_reopens() {
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(1)
            .open_for(Duration::from_secs(5))
            .compile();
        let now = t0();
        rt.record_failure(now);
        assert_eq!(rt.state(), BreakerState::Open);
        let later = now + Duration::from_secs(6);
        assert_eq!(rt.gate(later), Ok(BreakerState::HalfOpen));
        rt.record_failure(later);
        assert_eq!(rt.state(), BreakerState::Open);
    }

    #[test]
    fn failure_ratio_trips() {
        let (rt, _) = CircuitBreaker::<()>::failure_ratio()
            .ratio(0.5)
            .minimum_throughput(4)
            .sampling_window(Duration::from_secs(10))
            .compile();
        let now = t0();
        // 2 ok, 2 fail over 4 calls = 50% ratio, throughput 4 → trip.
        rt.record_success(now);
        rt.record_success(now);
        rt.record_failure(now);
        rt.record_failure(now);
        assert_eq!(rt.state(), BreakerState::Open);
    }

    #[test]
    fn failure_ratio_respects_min_throughput() {
        let (rt, _) = CircuitBreaker::<()>::failure_ratio()
            .ratio(0.5)
            .minimum_throughput(10)
            .compile();
        let now = t0();
        rt.record_failure(now);
        rt.record_failure(now); // 100% but only 2 calls < min 10
        assert_eq!(rt.state(), BreakerState::Closed);
    }

    #[test]
    fn state_reports_half_open_after_cooldown_without_a_call() {
        use crate::core::{Core, ManualClock, TestCore};
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(1)
            .open_for(Duration::from_secs(5))
            .compile();
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let now = core.now();
        rt.record_failure(now);
        assert_eq!(rt.state_at(now), BreakerState::Open);
        // Advance past the cooldown with NO gate()/record call in between.
        clock.advance(Duration::from_secs(6));
        let later = core.now();
        assert_eq!(
            rt.state_at(later),
            BreakerState::HalfOpen,
            "breaker must report HalfOpen once cooldown elapses, without a call arriving"
        );
        // The lazy atomic hasn't transitioned yet — `state_at` is a pure clock fn.
        assert_eq!(rt.state(), BreakerState::Open);
    }

    #[test]
    fn cooling_until_tracks_open_window() {
        use crate::core::{Core, ManualClock, TestCore};
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(1)
            .open_for(Duration::from_secs(5))
            .compile();
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let now = core.now();
        // Closed → not cooling.
        assert_eq!(rt.cooling_until(now), None);
        rt.record_failure(now); // trip → Open
        assert_eq!(rt.cooling_until(now), Some(now + Duration::from_secs(5)));
        // Once the window elapses it is no longer cooling (ready to probe).
        clock.advance(Duration::from_secs(6));
        assert_eq!(rt.cooling_until(core.now()), None);
    }

    #[test]
    fn cooling_until_none_while_closed() {
        let (rt, _) = CircuitBreaker::<()>::consecutive_failures(3).compile();
        let now = t0();
        assert_eq!(rt.cooling_until(now), None);
        rt.record_failure(now); // below threshold — still Closed
        assert_eq!(rt.cooling_until(now), None);
    }

    #[test]
    fn record_when_compiles_predicate() {
        let cb = CircuitBreaker::<i32>::consecutive_failures(1).record_when(|e: &i32| *e >= 500);
        let (_rt, record_when) = cb.compile();
        let p = record_when.expect("predicate present");
        assert!(p(&503));
        assert!(!p(&404));
    }
}
