//! Bounded concurrency: a runtime-agnostic async semaphore (no tokio dependency)
//! plus the explicit saturation policy. Permits release on drop and wake the
//! next waiter.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// What happens when all permits are taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaturationPolicy {
    /// Queue up to `max_queued` callers, each waiting at most `queue_timeout`.
    Wait {
        max_queued: usize,
        queue_timeout: Option<Duration>,
    },
    /// Reject immediately (load-shed).
    Reject,
}

/// Whether the limit counts whole operations or individual attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Operations,
    Attempts,
}

/// Bounded-concurrency configuration.
#[derive(Debug, Clone)]
pub struct ConcurrencyLimit {
    permits: usize,
    scope: Scope,
    saturation: SaturationPolicy,
}

impl ConcurrencyLimit {
    /// Limit concurrent *operations* (one permit per top-level call).
    pub fn operations(n: usize) -> Self {
        Self::new(n, Scope::Operations)
    }
    /// Limit concurrent *attempts* (one permit per attempt, retries included).
    pub fn attempts(n: usize) -> Self {
        Self::new(n, Scope::Attempts)
    }

    fn new(permits: usize, scope: Scope) -> Self {
        Self {
            permits: permits.max(1),
            scope,
            saturation: SaturationPolicy::Wait {
                max_queued: usize::MAX,
                queue_timeout: None,
            },
        }
    }

    /// Cap the number of waiting callers (excess is rejected).
    pub fn max_queued(mut self, n: usize) -> Self {
        self.saturation = match self.saturation {
            SaturationPolicy::Wait { queue_timeout, .. } => SaturationPolicy::Wait {
                max_queued: n,
                queue_timeout,
            },
            SaturationPolicy::Reject => SaturationPolicy::Reject,
        };
        self
    }
    /// Maximum time a caller waits in the queue before being rejected.
    pub fn queue_timeout(mut self, d: Duration) -> Self {
        self.saturation = match self.saturation {
            SaturationPolicy::Wait { max_queued, .. } => SaturationPolicy::Wait {
                max_queued,
                queue_timeout: Some(d),
            },
            SaturationPolicy::Reject => SaturationPolicy::Reject,
        };
        self
    }
    /// Reject immediately when saturated instead of queueing.
    pub fn reject(mut self) -> Self {
        self.saturation = SaturationPolicy::Reject;
        self
    }

    pub(crate) fn build(&self) -> Arc<Semaphore> {
        Semaphore::new(self.permits)
    }

    /// Compile into the live shared semaphore + policy used by the engine.
    pub(crate) fn compile(&self) -> CompiledConcurrency {
        CompiledConcurrency {
            sem: self.build(),
            saturation: self.saturation,
            scope: self.scope,
        }
    }
}

/// Live, shared concurrency gate stored in the compiled `Plan`.
#[derive(Debug)]
pub(crate) struct CompiledConcurrency {
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) saturation: SaturationPolicy,
    pub(crate) scope: Scope,
}

impl From<usize> for ConcurrencyLimit {
    /// `n` is shorthand for `ConcurrencyLimit::operations(n)`.
    fn from(n: usize) -> Self {
        ConcurrencyLimit::operations(n)
    }
}

impl From<u32> for ConcurrencyLimit {
    fn from(n: u32) -> Self {
        ConcurrencyLimit::operations(n as usize)
    }
}

#[derive(Debug)]
struct SemState {
    permits: usize,
    waiters: VecDeque<Waker>,
}

/// A runtime-agnostic counting semaphore.
#[derive(Debug)]
pub(crate) struct Semaphore {
    state: Mutex<SemState>,
}

impl Semaphore {
    fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SemState {
                permits,
                waiters: VecDeque::new(),
            }),
        })
    }

    /// Number of callers currently queued.
    pub(crate) fn queued(self: &Arc<Self>) -> usize {
        self.state.lock().unwrap().waiters.len()
    }

    /// Try to take a permit without waiting.
    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut st = self.state.lock().unwrap();
        if st.permits > 0 {
            st.permits -= 1;
            Some(Permit {
                sem: Arc::clone(self),
            })
        } else {
            None
        }
    }

    /// Wait for a permit.
    pub(crate) fn acquire(self: &Arc<Self>) -> Acquire {
        Acquire {
            sem: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut st = self.state.lock().unwrap();
        st.permits += 1;
        if let Some(w) = st.waiters.pop_front() {
            w.wake();
        }
    }
}

/// A held concurrency permit; releases on drop.
#[derive(Debug)]
pub(crate) struct Permit {
    sem: Arc<Semaphore>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.sem.release();
    }
}

/// Future returned by [`Semaphore::acquire`].
pub(crate) struct Acquire {
    sem: Arc<Semaphore>,
}

impl Future for Acquire {
    type Output = Permit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Permit> {
        let mut st = self.sem.state.lock().unwrap();
        if st.permits > 0 {
            st.permits -= 1;
            return Poll::Ready(Permit {
                sem: Arc::clone(&self.sem),
            });
        }
        // Register our waker to be woken on the next release.
        st.waiters.push_back(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn permit_release_lets_next_in() {
        let cfg = ConcurrencyLimit::operations(1);
        let sem = cfg.build();
        let p1 = sem.try_acquire().expect("first permit");
        assert!(sem.try_acquire().is_none(), "saturated");

        // Acquire should pend until p1 drops.
        let acq = sem.acquire();
        tokio::pin!(acq);
        let waiter = async { (&mut acq).await };
        tokio::pin!(waiter);

        let releaser = async {
            tokio::task::yield_now().await;
            drop(p1); // releases → wakes the waiter
        };
        let (_p2, ()) = tokio::join!(waiter, releaser);
        // _p2 now holds the only permit.
        assert!(sem.try_acquire().is_none());
    }

    #[test]
    fn queued_counts_waiters() {
        let sem = ConcurrencyLimit::operations(1).build();
        let _p = sem.try_acquire().unwrap();
        assert_eq!(sem.queued(), 0);
    }

    #[test]
    fn saturation_builders() {
        let c = ConcurrencyLimit::attempts(8)
            .max_queued(4)
            .queue_timeout(Duration::from_millis(10))
            .compile();
        assert_eq!(c.scope, Scope::Attempts);
        match c.saturation {
            SaturationPolicy::Wait {
                max_queued,
                queue_timeout,
            } => {
                assert_eq!(max_queued, 4);
                assert_eq!(queue_timeout, Some(Duration::from_millis(10)));
            }
            _ => panic!("expected Wait"),
        }
        assert_eq!(
            ConcurrencyLimit::operations(2)
                .reject()
                .compile()
                .saturation,
            SaturationPolicy::Reject
        );
    }
}
