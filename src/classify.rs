//! Failure classification, kept independent from retry/breaker policy.

/// Coarse classification of an outcome (used by the circuit breaker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Success,
    Retryable,
    Permanent,
    Ignored,
}

/// Whether the engine should retry after an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    Stop,
}

pub(crate) type ErrorPredicate<E> = Box<dyn Fn(&E) -> bool + Send + Sync>;
pub(crate) type OutcomeClassifier<T, E> = Box<dyn Fn(&Result<T, E>) -> RetryDecision + Send + Sync>;

/// An optional closure that extracts an explicit retry-after delay hint from
/// an error value. Used by [`crate::retry::Retry::retry_after`].
pub type RetryAfterExtractor<E> = Box<dyn Fn(&E) -> Option<std::time::Duration> + Send + Sync>;

/// How an outcome maps to a retry decision. `Result<T, E>` *is* the outcome —
/// there is no separate `Outcome` type.
pub(crate) enum Classifier<T, E> {
    /// Default: retry any `Err`, stop on any `Ok`.
    RetryAll,
    /// Retry when the error predicate returns true.
    WhenErr(ErrorPredicate<E>),
    /// Full control, inspects `Ok` values too.
    WhenOutcome(OutcomeClassifier<T, E>),
}

impl<T, E> Classifier<T, E> {
    pub(crate) fn decide(&self, outcome: &Result<T, E>) -> RetryDecision {
        match self {
            Self::RetryAll => match outcome {
                Ok(_) => RetryDecision::Stop,
                Err(_) => RetryDecision::Retry,
            },
            Self::WhenErr(pred) => match outcome {
                Ok(_) => RetryDecision::Stop,
                Err(e) => {
                    if pred(e) {
                        RetryDecision::Retry
                    } else {
                        RetryDecision::Stop
                    }
                }
            },
            Self::WhenOutcome(f) => f(outcome),
        }
    }
}

impl<T, E> std::fmt::Debug for Classifier<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::RetryAll => "RetryAll",
            Self::WhenErr(_) => "WhenErr",
            Self::WhenOutcome(_) => "WhenOutcome",
        };
        f.debug_tuple("Classifier").field(&name).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_all_retries_errors_stops_on_ok() {
        let c: Classifier<u32, &str> = Classifier::RetryAll;
        assert_eq!(c.decide(&Ok(1)), RetryDecision::Stop);
        assert_eq!(c.decide(&Err("x")), RetryDecision::Retry);
    }

    #[test]
    fn when_err_uses_predicate() {
        let c: Classifier<u32, i32> = Classifier::WhenErr(Box::new(|e: &i32| *e == 503));
        assert_eq!(c.decide(&Err(503)), RetryDecision::Retry);
        assert_eq!(c.decide(&Err(400)), RetryDecision::Stop);
    }

    #[test]
    fn when_outcome_can_retry_on_ok() {
        let c: Classifier<u32, &str> =
            Classifier::WhenOutcome(Box::new(|o: &Result<u32, &str>| match o {
                Ok(503) => RetryDecision::Retry,
                _ => RetryDecision::Stop,
            }));
        assert_eq!(c.decide(&Ok(503)), RetryDecision::Retry);
        assert_eq!(c.decide(&Ok(200)), RetryDecision::Stop);
    }
}
