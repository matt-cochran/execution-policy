//! Per-attempt metadata handed to operation closures.

use std::time::{Duration, Instant};

/// Metadata for the current attempt. `number()` is 1-based.
///
/// Intentionally lifetime-free and fully owned (`Copy`). An earlier design carried
/// a `PhantomData<&'a ()>` "reserved borrow"; it held nothing, but the `Attempt<'a>`
/// lifetime forced every op-closure bound to be higher-ranked
/// (`for<'a> …(Attempt<'a>)`), which — together with `AsyncFnMut`'s own HRTB — made
/// the engine future fail `Send` inference for any caller whose future must be
/// `Send` (a router behind `#[async_trait]`, `tokio::spawn`, …). Dropping the
/// phantom lifetime is one half of removing that obstruction (the other is the
/// engine taking `FnMut(Attempt) -> Fut` instead of `AsyncFnMut(Attempt)`); a real
/// borrow can be reintroduced deliberately later if ever needed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct Attempt {
    number: u32,
    start: Instant,
    now: Instant,
}

impl Attempt {
    pub(crate) fn new(number: u32, start: Instant, now: Instant) -> Self {
        Self { number, start, now }
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
