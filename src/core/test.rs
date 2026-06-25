use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use super::rng::SplitMix64;
use super::{BoxFuture, Core};

struct ClockState {
    base: Instant,
    offset: Duration,
    wakers: Vec<(Duration, Waker)>,
}

/// A virtual clock for deterministic tests. `now()` advances only via
/// [`ManualClock::advance`]; sleeps registered on it resolve when crossed.
#[derive(Clone)]
pub struct ManualClock(Arc<Mutex<ClockState>>);

impl std::fmt::Debug for ManualClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManualClock")
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ManualClock {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ClockState {
            base: Instant::now(),
            offset: Duration::ZERO,
            wakers: Vec::new(),
        })))
    }

    /// Advance virtual time, waking any sleeps whose deadline is now crossed.
    pub fn advance(&self, dur: Duration) {
        let mut st = self.0.lock().unwrap();
        st.offset += dur;
        let now = st.offset;
        let mut ready = Vec::new();
        st.wakers.retain(|(deadline, w)| {
            if *deadline <= now {
                ready.push(w.clone());
                false
            } else {
                true
            }
        });
        drop(st);
        for w in ready {
            w.wake();
        }
    }

    fn now(&self) -> Instant {
        let st = self.0.lock().unwrap();
        st.base + st.offset
    }

    fn register(&self, deadline: Duration, waker: Waker) {
        self.0.lock().unwrap().wakers.push((deadline, waker));
    }

    fn offset(&self) -> Duration {
        self.0.lock().unwrap().offset
    }
}

struct ManualSleep {
    clock: ManualClock,
    deadline: Duration,
}

impl Future for ManualSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.clock.offset() >= self.deadline {
            Poll::Ready(())
        } else {
            self.clock.register(self.deadline, cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Deterministic [`Core`] for tests: virtual clock + seeded RNG, no real sleeps.
#[derive(Debug)]
pub struct TestCore {
    clock: ManualClock,
    rng: Mutex<SplitMix64>,
}

impl TestCore {
    pub fn new(clock: ManualClock) -> Self {
        Self::with_seed(clock, 0xDEAD_BEEF)
    }

    pub fn with_seed(clock: ManualClock, seed: u64) -> Self {
        Self {
            clock,
            rng: Mutex::new(SplitMix64::new(seed)),
        }
    }
}

impl Core for TestCore {
    fn now(&self) -> Instant {
        self.clock.now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        let deadline = self.clock.offset() + dur;
        Box::pin(ManualSleep {
            clock: self.clock.clone(),
            deadline,
        })
    }

    fn next_u64(&self) -> u64 {
        self.rng.lock().unwrap().next_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_advances_only_on_advance() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let t0 = core.now();
        clock.advance(Duration::from_secs(5));
        assert_eq!(core.now().duration_since(t0), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn sleep_resolves_when_clock_crosses_deadline() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let mut fut = Box::pin(core.sleep(Duration::from_secs(10)));
        let poll = noop_poll(&mut fut);
        assert!(poll.is_pending());
        clock.advance(Duration::from_secs(10));
        fut.await;
    }

    fn noop_poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        Pin::new(f).poll(&mut cx)
    }
}
