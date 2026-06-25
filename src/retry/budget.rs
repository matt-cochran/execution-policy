//! Shared retry budget: a lock-free token bucket that bounds retries to a
//! fraction of total traffic, protecting dependencies from retry storms.
//!
//! Each top-level call deposits tokens; each retry withdraws one. When the
//! bucket is empty, retries are denied even if attempts remain.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

const SCALE: i64 = 1000;

#[derive(Debug)]
struct Inner {
    tokens: AtomicI64,
    max_tokens: i64,
    deposit: i64,
    retry_cost: i64,
}

/// A shared retry budget. Clone it into multiple policies to share one budget.
#[derive(Debug, Clone)]
pub struct RetryBudget(Arc<Inner>);

impl RetryBudget {
    /// Allow retries up to `ratio` of total calls (e.g. `0.2` = 20%), with a
    /// burst of up to `burst` retries when the bucket is full.
    pub fn new(ratio: f64, burst: u32) -> Self {
        let ratio = ratio.clamp(0.0, 1.0);
        let deposit = (ratio * SCALE as f64) as i64;
        let max_tokens = (burst.max(1) as i64) * SCALE;
        Self(Arc::new(Inner {
            tokens: AtomicI64::new(max_tokens),
            max_tokens,
            deposit,
            retry_cost: SCALE,
        }))
    }

    /// Sensible default: 20% retry ratio, burst of 10.
    pub fn standard() -> Self {
        Self::new(0.2, 10)
    }

    /// Record a top-level call (deposits tokens, capped at the burst max).
    pub(crate) fn deposit(&self) {
        let inner = &self.0;
        let mut cur = inner.tokens.load(Ordering::Relaxed);
        loop {
            let next = (cur + inner.deposit).min(inner.max_tokens);
            match inner
                .tokens
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Try to spend one retry token. Returns `false` if the budget is exhausted.
    pub(crate) fn try_withdraw(&self) -> bool {
        let inner = &self.0;
        let mut cur = inner.tokens.load(Ordering::Relaxed);
        loop {
            if cur < inner.retry_cost {
                return false;
            }
            match inner.tokens.compare_exchange_weak(
                cur,
                cur - inner.retry_cost,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => cur = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_budget_allows_burst_then_denies() {
        let b = RetryBudget::new(0.0, 3); // no replenishment, burst 3
        assert!(b.try_withdraw());
        assert!(b.try_withdraw());
        assert!(b.try_withdraw());
        assert!(!b.try_withdraw(), "burst exhausted with no deposits");
    }

    #[test]
    fn deposits_replenish_at_ratio() {
        let b = RetryBudget::new(0.5, 1); // 50% ratio, small burst
        // Drain the burst.
        assert!(b.try_withdraw());
        assert!(!b.try_withdraw());
        // Two calls deposit 0.5 + 0.5 = 1.0 token → one more retry allowed.
        b.deposit();
        b.deposit();
        assert!(b.try_withdraw());
        assert!(!b.try_withdraw());
    }

    #[test]
    fn shared_clones_share_one_bucket() {
        let a = RetryBudget::new(0.0, 1);
        let b = a.clone();
        assert!(a.try_withdraw());
        assert!(!b.try_withdraw(), "clone sees the same drained bucket");
    }

    #[test]
    fn deposit_caps_at_max() {
        let b = RetryBudget::new(1.0, 2); // deposit 1 token/call, max 2
        for _ in 0..100 {
            b.deposit();
        }
        assert!(b.try_withdraw());
        assert!(b.try_withdraw());
        assert!(
            !b.try_withdraw(),
            "capped at burst max regardless of deposits"
        );
    }
}
