use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::rng::SplitMix64;
use super::{BoxFuture, Core};

/// Default production [`Core`]: tokio timers + a fast non-crypto RNG for jitter.
#[derive(Debug)]
pub struct TokioCore {
    rng_state: AtomicU64,
}

impl Default for TokioCore {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioCore {
    pub fn new() -> Self {
        // Seed from process-relative nanos; jitter quality only, not security.
        let seed = Instant::now().elapsed().as_nanos() as u64 ^ 0x2545_F491_4F6C_DD1D;
        Self {
            rng_state: AtomicU64::new(seed.max(1)),
        }
    }
}

impl Core for TokioCore {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(dur))
    }

    fn next_u64(&self) -> u64 {
        let s = self
            .rng_state
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        SplitMix64::new(s).next_u64()
    }
}
