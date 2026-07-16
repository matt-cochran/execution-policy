//! Generic dead-time watchdog: fire on *silence*, never on a slow-but-live op.
//!
//! `attempt_timeout` (see [`crate::engine`]) drops the whole attempt at a fixed
//! wall-clock duration — it cannot tell "deep in a long reasoning trace" from
//! "the connection is dead". `stall_timeout` measures *silence*: the operation
//! ticks a [`Progress`] handle on each unit of forward progress (e.g. each
//! streamed chunk), and the watchdog fires only after `budget` elapses with no
//! tick. A live operation that keeps ticking runs to completion regardless of
//! total duration. Generic to any streaming or long-running op — no LLM concept
//! lives here; the loop/quality policy that decides "is this output usable" is
//! the caller's job.
//!
//! Racing the operation against the silence timer uses the same hand-rolled
//! `poll_fn` seam as the engine (no `tokio::select!`), so it is `Core`-driven
//! and deterministic in virtual time.

use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::Duration;

use crate::core::{BoxFuture, Core};

/// A cheap, clonable "I made contact" handle handed to a watched operation.
///
/// The operation calls [`Progress::tick`] on each unit of forward progress. The
/// watchdog samples a monotonic counter (not a timestamp) so `Progress` stays
/// `Core`-agnostic — the watchdog owns the clock.
#[derive(Clone, Debug)]
pub struct Progress {
    ticks: Arc<AtomicU64>,
}

impl Progress {
    fn new() -> Self {
        Self {
            ticks: Arc::new(AtomicU64::new(0)),
        }
    }
    /// Record that the operation just made progress (e.g. a chunk arrived).
    pub fn tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
    fn sample(&self) -> u64 {
        self.ticks.load(Ordering::Relaxed)
    }
}

/// The outcome of a stall-guarded operation.
#[derive(Debug, PartialEq, Eq)]
pub enum StallError<E> {
    /// The operation went silent for at least `idle` (no `tick` within budget).
    Stalled { idle: Duration },
    /// The operation itself returned an error.
    Operation(E),
}

/// Run `op`, dropping it if it goes silent for longer than `budget`.
///
/// `op` receives a [`Progress`] handle and must `tick()` it on each unit of
/// forward progress. The silence deadline is reset to `now + budget` every time
/// a tick is observed; if the deadline is reached with no new tick the operation
/// is considered stalled and dropped with [`StallError::Stalled`]. A live op
/// that ticks within every `budget` window runs to completion regardless of
/// total duration — this is the "correctness over latency" guarantee: a long
/// legitimate stream is never killed for being slow, only for going silent.
pub async fn stall_timeout<C, T, E, F>(
    core: &C,
    budget: Duration,
    op: F,
) -> Result<T, StallError<E>>
where
    C: Core,
    F: AsyncFnOnce(Progress) -> Result<T, E>,
{
    let progress = Progress::new();
    let op_fut = op(progress.clone());
    let mut op_fut = pin!(op_fut);

    let mut last_ticks = progress.sample();
    let mut deadline = core.now() + budget;
    // Armed lazily; rebuilt whenever a tick pushes the deadline forward.
    let mut sleep: Option<BoxFuture<'_, ()>> = None;

    poll_fn(|cx| {
        if let Poll::Ready(r) = op_fut.as_mut().poll(cx) {
            return Poll::Ready(r.map_err(StallError::Operation));
        }
        // Observe forward progress; a new tick resets the silence deadline.
        let now_ticks = progress.sample();
        if now_ticks != last_ticks {
            last_ticks = now_ticks;
            deadline = core.now() + budget;
            sleep = None; // re-arm to the new deadline
        }
        // Arm/poll the silence timer. Ready ⇒ `budget` elapsed with no tick.
        let s =
            sleep.get_or_insert_with(|| core.sleep(deadline.saturating_duration_since(core.now())));
        if Pin::new(s).poll(cx).is_ready() {
            return Poll::Ready(Err(StallError::Stalled { idle: budget }));
        }
        Poll::Pending
    })
    .await
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};

    /// A slow operation that keeps ticking within budget must run to completion,
    /// even though its total duration (200ms) far exceeds the budget (100ms).
    /// The op advances the ManualClock to model elapsed time between chunks and
    /// yields so the watchdog gets a chance to observe each tick.
    #[tokio::test]
    async fn slow_but_ticking_op_is_not_killed() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let out: Result<u32, StallError<()>> =
            stall_timeout(&core, Duration::from_millis(100), async |p: Progress| {
                for _ in 0..5 {
                    clock.advance(Duration::from_millis(40)); // < budget
                    p.tick();
                    tokio::task::yield_now().await;
                }
                Ok(7)
            })
            .await;
        assert_eq!(out, Ok(7));
    }

    /// A silent operation (time passes, no tick) is dropped once the budget is
    /// exceeded. The op advances the clock past the budget then parks forever;
    /// the watchdog observes the elapsed silence deadline and returns Stalled.
    #[tokio::test]
    async fn silent_op_is_killed() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let out: Result<u32, StallError<()>> =
            stall_timeout(&core, Duration::from_millis(100), async |_p: Progress| {
                clock.advance(Duration::from_millis(150)); // silence past budget
                std::future::pending::<()>().await;
                Ok(0)
            })
            .await;
        assert_eq!(
            out,
            Err(StallError::Stalled {
                idle: Duration::from_millis(100)
            })
        );
    }
}
