//! Per-attempt metadata handed to operation closures.

use std::marker::PhantomData;
use std::time::{Duration, Instant};

/// Metadata for the current attempt. `number()` is 1-based.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct Attempt<'a> {
    number: u32,
    start: Instant,
    now: Instant,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Attempt<'a> {
    pub(crate) fn new(number: u32, start: Instant, now: Instant) -> Self {
        Self {
            number,
            start,
            now,
            _borrow: PhantomData,
        }
    }

    /// 1-based attempt index (first attempt returns 1).
    pub fn number(&self) -> u32 {
        self.number
    }

    /// Time elapsed since the first attempt began.
    pub fn elapsed(&self) -> Duration {
        self.now.duration_since(self.start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_is_one_based() {
        let t = Instant::now();
        let a = Attempt::new(1, t, t);
        assert_eq!(a.number(), 1);
    }

    #[test]
    fn elapsed_reflects_clock() {
        let start = Instant::now();
        let now = start + Duration::from_millis(250);
        let a = Attempt::new(2, start, now);
        assert_eq!(a.elapsed(), Duration::from_millis(250));
    }
}
