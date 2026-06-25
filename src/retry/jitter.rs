use std::time::Duration;

/// Randomization applied to a backoff delay to de-correlate retriers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Jitter {
    /// No jitter — use the raw delay.
    None,
    /// Uniform random in `[0, base]` (AWS "full jitter").
    Full,
    /// `base/2 + uniform(0, base/2)` (AWS "equal jitter").
    Equal,
}

impl Jitter {
    /// Apply jitter to `base` using one random `u64`.
    pub(crate) fn apply(&self, base: Duration, rng: u64) -> Duration {
        if base.is_zero() {
            return base;
        }
        let nanos = base.as_nanos() as u64;
        // Fraction of `span` from the high bits of `rng`, in `[0, span)`.
        let frac = |span: u64| -> u64 {
            if span == 0 {
                0
            } else {
                (((rng >> 11) as u128 * span as u128) >> 53) as u64
            }
        };
        match self {
            Jitter::None => base,
            Jitter::Full => Duration::from_nanos(frac(nanos)),
            Jitter::Equal => {
                let half = nanos / 2;
                Duration::from_nanos(half + frac(half))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_identity() {
        let d = Duration::from_millis(100);
        assert_eq!(Jitter::None.apply(d, u64::MAX), d);
    }

    #[test]
    fn full_is_within_bounds() {
        let d = Duration::from_millis(100);
        for rng in [0u64, 1, 12345, u64::MAX / 2, u64::MAX] {
            let j = Jitter::Full.apply(d, rng);
            assert!(j <= d, "full jitter {j:?} exceeded base {d:?}");
        }
    }

    #[test]
    fn equal_is_in_upper_half() {
        let d = Duration::from_millis(100);
        for rng in [0u64, 999, u64::MAX] {
            let j = Jitter::Equal.apply(d, rng);
            assert!(
                j >= d / 2 && j <= d,
                "equal jitter {j:?} out of [50ms,100ms]"
            );
        }
    }

    #[test]
    fn zero_base_stays_zero() {
        assert_eq!(Jitter::Full.apply(Duration::ZERO, 42), Duration::ZERO);
    }
}
