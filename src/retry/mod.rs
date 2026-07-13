//! Retry policy, backoff schedules, and jitter.

pub mod backoff;
pub mod budget;
pub mod jitter;

pub use backoff::Backoff;
pub use budget::RetryBudget;
pub use jitter::Jitter;

use std::time::Duration;

use crate::classify::{Classifier, RetryAfterExtractor, RetryDecision};
use crate::core::Core;

/// Retry configuration: attempt cap, backoff, jitter, classification, budget.
pub struct Retry<T, E> {
    max_attempts: u32,
    max_elapsed: Option<Duration>,
    backoff: Backoff,
    jitter: Jitter,
    classifier: Classifier<T, E>,
    budget: Option<RetryBudget>,
    retry_after: Option<RetryAfterExtractor<E>>,
}

impl<T, E> std::fmt::Debug for Retry<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retry")
            .field("max_attempts", &self.max_attempts)
            .field("max_elapsed", &self.max_elapsed)
            .field("backoff", &self.backoff)
            .field("jitter", &self.jitter)
            .field("classifier", &self.classifier)
            .field("budget", &self.budget)
            .field("retry_after", &self.retry_after.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl<T, E> Retry<T, E> {
    fn base(max_attempts: u32, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            max_elapsed: None,
            backoff,
            jitter: Jitter::None,
            classifier: Classifier::RetryAll,
            budget: None,
            retry_after: None,
        }
    }

    /// No retries — a single attempt.
    pub fn none() -> Self {
        Self::base(1, Backoff::fixed(Duration::ZERO))
    }
    /// Constant-delay retries (default 3 attempts).
    pub fn fixed(delay: Duration) -> Self {
        Self::base(3, Backoff::fixed(delay))
    }
    /// Exponential backoff (default 3 attempts, 100ms base, 10s max, no jitter).
    pub fn exponential() -> Self {
        Self::base(
            3,
            Backoff::exponential(Duration::from_millis(100), Duration::from_secs(10)),
        )
    }
    /// Sensible default schedule: exponential + full jitter, 4 attempts.
    /// Pair with `.when(..)` for transient-only classification (see spec §6).
    pub fn standard() -> Self {
        Self::base(
            4,
            Backoff::exponential(Duration::from_millis(100), Duration::from_secs(2)),
        )
        .jitter(Jitter::Full)
    }

    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }
    pub fn max_elapsed(mut self, d: Duration) -> Self {
        self.max_elapsed = Some(d);
        self
    }
    pub fn base_delay(mut self, d: Duration) -> Self {
        self.backoff = match self.backoff {
            Backoff::Exponential { max, .. } => Backoff::Exponential { base: d, max },
            Backoff::Fixed(_) => Backoff::Fixed(d),
        };
        self
    }
    pub fn max_delay(mut self, d: Duration) -> Self {
        if let Backoff::Exponential { base, .. } = self.backoff {
            self.backoff = Backoff::Exponential { base, max: d };
        }
        self
    }
    pub fn jitter(mut self, j: Jitter) -> Self {
        self.jitter = j;
        self
    }
    /// Attach a shared [`RetryBudget`] to bound retries across calls.
    pub fn budget(mut self, budget: RetryBudget) -> Self {
        self.budget = Some(budget);
        self
    }
    pub fn when(mut self, pred: impl Fn(&E) -> bool + Send + Sync + 'static) -> Self {
        self.classifier = Classifier::WhenErr(Box::new(pred));
        self
    }
    pub fn when_outcome(
        mut self,
        f: impl Fn(&Result<T, E>) -> RetryDecision + Send + Sync + 'static,
    ) -> Self {
        self.classifier = Classifier::WhenOutcome(Box::new(f));
        self
    }

    /// Extract an explicit delay hint from an error (e.g. a server-supplied
    /// `Retry-After` duration). When the extractor returns `Some(hint)`, the
    /// next delay is `max(backoff, hint)` — the hint acts as a **floor**.
    /// The result is still capped by `max_backoff` (if set) and will not
    /// exceed the remaining `total_timeout` budget (the engine stops instead
    /// of overshooting). Jitter applies only to the backoff component before
    /// the floor, so the hint is a *hard* lower bound the delay never dips below.
    ///
    /// No HTTP, gRPC, or any other dependency is introduced — the closure
    /// receives `&E` and you parse whatever field you need.
    pub fn retry_after(
        mut self,
        f: impl Fn(&E) -> Option<Duration> + Send + Sync + 'static,
    ) -> Self {
        self.retry_after = Some(Box::new(f));
        self
    }

    pub(crate) fn max_attempts_value(&self) -> u32 {
        self.max_attempts
    }
    pub(crate) fn max_elapsed_value(&self) -> Option<Duration> {
        self.max_elapsed
    }
    pub(crate) fn decide(&self, outcome: &Result<T, E>) -> RetryDecision {
        self.classifier.decide(outcome)
    }
    /// Compute the next backoff delay.
    ///
    /// `last_err` is passed through to the retry-after extractor (if any).
    /// Jitter is applied to the raw backoff first; when the extractor returns
    /// `Some(hint)`, the jittered backoff is then floored to `hint` so the hint
    /// is a hard lower bound. The cap at `max_backoff` (encoded in
    /// `Backoff::Exponential`) already happens inside `raw_delay`; the
    /// `total_timeout` budget check is done by the caller in `engine.rs`.
    pub(crate) fn delay(&self, attempt: u32, core: &dyn Core, last_err: Option<&E>) -> Duration {
        let backoff_raw = self.backoff.raw_delay(attempt);
        // Jitter de-correlates the *backoff* component only. The retry-after
        // hint is then applied as a hard floor, so jitter can never undercut a
        // server-supplied Retry-After (with `Jitter::Full` it otherwise could
        // sample far below the hint and hammer a throttled provider).
        let jittered = self.jitter.apply(backoff_raw, core.next_u64());
        let hint = last_err
            .zip(self.retry_after.as_deref())
            .and_then(|(e, f)| f(e));
        match hint {
            Some(h) if h > jittered => h,
            _ => jittered,
        }
    }
    pub(crate) fn budget_ref(&self) -> Option<&RetryBudget> {
        self.budget.as_ref()
    }
    /// Returns the retry-after extractor, if any, for use in budget-stop checks.
    pub(crate) fn retry_after_hint(&self, err: &E) -> Option<Duration> {
        self.retry_after.as_deref().and_then(|f| f(err))
    }
}

#[cfg(all(test, feature = "test-util"))]
mod retry_tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};

    #[test]
    fn presets_have_expected_attempt_caps() {
        assert_eq!(Retry::<(), ()>::none().max_attempts_value(), 1);
        assert_eq!(
            Retry::<(), ()>::fixed(Duration::ZERO).max_attempts_value(),
            3
        );
        assert_eq!(Retry::<(), ()>::exponential().max_attempts_value(), 3);
        assert_eq!(Retry::<(), ()>::standard().max_attempts_value(), 4);
    }

    #[test]
    fn builder_overrides_schedule() {
        let r = Retry::<u32, &str>::exponential()
            .max_attempts(5)
            .base_delay(Duration::from_millis(20))
            .max_delay(Duration::from_millis(80));
        assert_eq!(r.max_attempts_value(), 5);
        let core = TestCore::new(ManualClock::new());
        assert_eq!(r.delay(1, &core, None), Duration::from_millis(20));
        assert_eq!(r.delay(3, &core, None), Duration::from_millis(80));
        assert_eq!(r.delay(9, &core, None), Duration::from_millis(80));
    }

    #[test]
    fn retry_after_floors_delay_across_seeds() {
        // `standard()` uses `Jitter::Full`. The Retry-After hint must be a hard
        // floor: jitter may only shrink the backoff component, never undercut
        // the server-supplied hint. Drive many RNG seeds × attempts.
        let r = Retry::<(), ()>::standard().retry_after(|_: &()| Some(Duration::from_secs(47)));
        let floor = Duration::from_secs(47);
        for seed in 0..2000u64 {
            let core = TestCore::with_seed(ManualClock::new(), seed);
            for attempt in 1..=6 {
                let d = r.delay(attempt, &core, Some(&()));
                assert!(
                    d >= floor,
                    "seed {seed}, attempt {attempt}: delay {d:?} undercut Retry-After floor {floor:?}"
                );
            }
        }
    }

    #[test]
    fn no_hint_leaves_full_jitter_untouched() {
        // Without a Retry-After hint, `Jitter::Full` still applies to the raw
        // backoff and may fall below the backoff cap — behavior must be
        // identical to before the floor fix.
        let r = Retry::<(), ()>::standard();
        let cap = Duration::from_secs(2); // standard() max backoff
        let mut saw_below_cap = false;
        for seed in 0..256u64 {
            let core = TestCore::with_seed(ManualClock::new(), seed);
            let d = r.delay(6, &core, None); // attempt 6 → backoff clamps to cap
            assert!(d <= cap, "seed {seed}: delay {d:?} exceeded backoff cap {cap:?}");
            if d < cap {
                saw_below_cap = true;
            }
        }
        assert!(
            saw_below_cap,
            "Full jitter never reduced the delay below the cap"
        );
    }

    #[test]
    fn when_predicate_controls_decision() {
        let r = Retry::<u32, i32>::exponential().when(|e: &i32| *e >= 500);
        assert_eq!(r.decide(&Err(503)), RetryDecision::Retry);
        assert_eq!(r.decide(&Err(404)), RetryDecision::Stop);
        assert_eq!(r.decide(&Ok(1)), RetryDecision::Stop);
    }
}
