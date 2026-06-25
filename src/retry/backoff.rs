use std::time::Duration;

/// Delay schedule between attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    /// Constant delay.
    Fixed(Duration),
    /// `base * 2^(attempt-1)`, clamped to `max`.
    Exponential { base: Duration, max: Duration },
}

impl Backoff {
    pub fn fixed(d: Duration) -> Self {
        Backoff::Fixed(d)
    }
    pub fn exponential(base: Duration, max: Duration) -> Self {
        Backoff::Exponential { base, max }
    }

    /// Raw (pre-jitter) delay to wait *after* `attempt` (1-based) before the next try.
    pub(crate) fn raw_delay(&self, attempt: u32) -> Duration {
        match self {
            Backoff::Fixed(d) => *d,
            Backoff::Exponential { base, max } => {
                let shift = attempt.saturating_sub(1).min(63);
                let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
                let scaled = base
                    .checked_mul(u32::try_from(factor).unwrap_or(u32::MAX))
                    .unwrap_or(*max);
                scaled.min(*max)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_is_constant() {
        let b = Backoff::fixed(Duration::from_millis(50));
        assert_eq!(b.raw_delay(1), Duration::from_millis(50));
        assert_eq!(b.raw_delay(5), Duration::from_millis(50));
    }

    #[test]
    fn exponential_doubles_then_clamps() {
        let b = Backoff::exponential(Duration::from_millis(50), Duration::from_millis(400));
        assert_eq!(b.raw_delay(1), Duration::from_millis(50));
        assert_eq!(b.raw_delay(2), Duration::from_millis(100));
        assert_eq!(b.raw_delay(3), Duration::from_millis(200));
        assert_eq!(b.raw_delay(4), Duration::from_millis(400));
        assert_eq!(b.raw_delay(5), Duration::from_millis(400));
        assert_eq!(b.raw_delay(40), Duration::from_millis(400));
    }
}
