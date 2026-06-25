//! Fluent builder for [`ExecutionPolicy`].

use std::sync::Arc;
use std::time::Duration;

use crate::breaker::CircuitBreaker;
use crate::concurrency::ConcurrencyLimit;
use crate::core::Core;
#[cfg(feature = "tokio")]
use crate::core::DefaultCore;
use crate::error::ExecutionError;
use crate::event::{Event, EventHook};
use crate::plan::{CompiledBreaker, FallbackFn, Plan};
use crate::policy::ExecutionPolicy;
use crate::retry::Retry;

/// Returned by [`ExecutionPolicyBuilder::try_build`] on invalid config.
#[derive(Debug, Clone)]
pub struct BuildError(pub(crate) String);

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid execution policy: {}", self.0)
    }
}
impl std::error::Error for BuildError {}

/// Fluent builder for an [`ExecutionPolicy`].
pub struct ExecutionPolicyBuilder<T, E> {
    retry: Option<Retry<T, E>>,
    attempt_timeout: Option<Duration>,
    total_timeout: Option<Duration>,
    breaker: Option<CircuitBreaker<E>>,
    concurrency: Option<ConcurrencyLimit>,
    on_event: Option<EventHook>,
    fallback: Option<FallbackFn<T, E>>,
}

impl<T, E> std::fmt::Debug for ExecutionPolicyBuilder<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPolicyBuilder")
            .field("retry", &self.retry)
            .field("attempt_timeout", &self.attempt_timeout)
            .field("total_timeout", &self.total_timeout)
            .field("breaker", &self.breaker)
            .field("concurrency", &self.concurrency)
            .field("on_event", &self.on_event.as_ref().map(|_| "<fn>"))
            .field("fallback", &self.fallback.as_ref().map(|_| "<async fn>"))
            .finish()
    }
}

impl<T, E> Default for ExecutionPolicyBuilder<T, E> {
    fn default() -> Self {
        Self {
            retry: None,
            attempt_timeout: None,
            total_timeout: None,
            breaker: None,
            concurrency: None,
            on_event: None,
            fallback: None,
        }
    }
}

impl<T, E> ExecutionPolicyBuilder<T, E> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry(mut self, retry: Retry<T, E>) -> Self {
        debug_assert!(self.retry.is_none(), "retry configured twice (last-wins)");
        self.retry = Some(retry);
        self
    }
    pub fn attempt_timeout(mut self, d: Duration) -> Self {
        self.attempt_timeout = Some(d);
        self
    }
    pub fn total_timeout(mut self, d: Duration) -> Self {
        self.total_timeout = Some(d);
        self
    }
    pub fn circuit_breaker(mut self, breaker: CircuitBreaker<E>) -> Self {
        debug_assert!(
            self.breaker.is_none(),
            "circuit_breaker configured twice (last-wins)"
        );
        self.breaker = Some(breaker);
        self
    }
    pub fn concurrency_limit(mut self, limit: impl Into<ConcurrencyLimit>) -> Self {
        debug_assert!(
            self.concurrency.is_none(),
            "concurrency_limit configured twice (last-wins)"
        );
        self.concurrency = Some(limit.into());
        self
    }
    /// Register a synchronous, cheap event hook. Not `catch_unwind`-wrapped — a
    /// panicking hook surfaces as a bug (fail fast).
    pub fn on_event(mut self, hook: impl Fn(&Event) + Send + Sync + 'static) -> Self {
        self.on_event = Some(Arc::new(hook));
        self
    }
    /// Bridge events to `tracing` at debug level.
    #[cfg(feature = "tracing")]
    pub fn with_tracing(self) -> Self {
        self.on_event(|e: &Event| tracing::debug!(event = ?e, "execution-policy"))
    }

    /// Register an async fallback of last resort.
    ///
    /// Invoked when the bundled policy yields a **terminal** `ExecutionError<E>` —
    /// after retries, the circuit breaker, timeouts, and concurrency limits have
    /// all run. The closure receives the final error so you can discriminate by
    /// failure class (`is_timeout()`, `is_circuit_open()`, `is_rejected()`,
    /// `is_exhausted()`, `into_inner()`).
    ///
    /// **Async** so the fallback can do I/O — fetch a cached value, query an
    /// alternate endpoint, return a sentinel. Return `Ok(T)` to recover; return
    /// `Err(E)` to propagate the fallback's own failure (wrapped into
    /// `ExecutionError::Operation` preserving the original diagnostic context).
    ///
    /// # Composition
    ///
    /// Sits **outside** the retry/breaker/timeout/concurrency stack. Those
    /// policies run unchanged; the fallback fires only when they all give up.
    /// No fallback set → unchanged behavior (additive, non-breaking).
    pub fn fallback<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&ExecutionError<E>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
    {
        self.fallback = Some(Box::new(move |e| Box::pin(f(e))));
        self
    }

    fn validate(&self) -> Result<(), BuildError> {
        if let Some(r) = &self.retry {
            if r.max_attempts_value() == 0 {
                return Err(BuildError("max_attempts must be >= 1".into()));
            }
        }
        if let (Some(a), Some(t)) = (self.attempt_timeout, self.total_timeout) {
            if a > t {
                return Err(BuildError(format!(
                    "attempt_timeout ({a:?}) must be <= total_timeout ({t:?})"
                )));
            }
        }
        Ok(())
    }

    fn compile(self) -> Plan<T, E> {
        let breaker = self.breaker.map(|b| {
            let (runtime, record_when) = b.compile();
            CompiledBreaker {
                runtime: std::sync::Arc::new(runtime),
                record_when,
            }
        });
        let concurrency = self.concurrency.as_ref().map(|c| c.compile());
        Plan {
            retry: self.retry.unwrap_or_else(Retry::none),
            attempt_timeout: self.attempt_timeout,
            total_timeout: self.total_timeout,
            breaker,
            concurrency,
            on_event: self.on_event,
            fallback: self.fallback,
        }
    }

    /// Validate and build with the default core. Panics on invalid config.
    #[cfg(feature = "tokio")]
    pub fn build(self) -> ExecutionPolicy<DefaultCore, T, E> {
        self.build_with(DefaultCore::new())
    }

    /// Validate and build with the default core, returning an error on bad config.
    #[cfg(feature = "tokio")]
    pub fn try_build(self) -> Result<ExecutionPolicy<DefaultCore, T, E>, BuildError> {
        self.validate()?;
        Ok(ExecutionPolicy::from_parts(
            DefaultCore::new(),
            Arc::new(self.compile()),
        ))
    }

    /// Validate and build with a custom [`Core`]. Panics on invalid config.
    pub fn build_with<C: Core>(self, core: C) -> ExecutionPolicy<C, T, E> {
        if let Err(e) = self.validate() {
            panic!("{e}");
        }
        ExecutionPolicy::from_parts(core, Arc::new(self.compile()))
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};

    #[test]
    #[should_panic(expected = "max_attempts must be >= 1")]
    fn build_panics_on_zero_attempts() {
        let _ = ExecutionPolicyBuilder::<u32, &str>::new()
            .retry(Retry::exponential().max_attempts(0))
            .build_with(TestCore::new(ManualClock::new()));
    }

    #[test]
    fn try_build_reports_timeout_inversion() {
        let err = ExecutionPolicyBuilder::<u32, &str>::new()
            .attempt_timeout(Duration::from_secs(5))
            .total_timeout(Duration::from_secs(2))
            .validate();
        assert!(err.is_err());
    }
}
