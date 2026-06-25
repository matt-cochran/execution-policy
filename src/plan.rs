//! Compiled, immutable policy configuration shared behind an `Arc`.

use std::sync::Arc;
use std::time::Duration;

use crate::breaker::BreakerRuntime;
use crate::classify::ErrorPredicate;
use crate::concurrency::CompiledConcurrency;
use crate::event::EventHook;
use crate::retry::Retry;

/// A breaker compiled into live state plus its fault classifier.
pub(crate) struct CompiledBreaker<E> {
    pub(crate) runtime: Arc<BreakerRuntime>,
    pub(crate) record_when: Option<ErrorPredicate<E>>,
}

impl<E> std::fmt::Debug for CompiledBreaker<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledBreaker")
            .field("runtime", &self.runtime)
            .field("record_when", &self.record_when.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

pub(crate) struct Plan<T, E> {
    pub(crate) retry: Retry<T, E>,
    pub(crate) attempt_timeout: Option<Duration>,
    pub(crate) total_timeout: Option<Duration>,
    pub(crate) breaker: Option<CompiledBreaker<E>>,
    pub(crate) concurrency: Option<CompiledConcurrency>,
    pub(crate) on_event: Option<EventHook>,
}

impl<T, E> std::fmt::Debug for Plan<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plan")
            .field("retry", &self.retry)
            .field("attempt_timeout", &self.attempt_timeout)
            .field("total_timeout", &self.total_timeout)
            .field("breaker", &self.breaker)
            .field("concurrency", &self.concurrency)
            .field("on_event", &self.on_event.as_ref().map(|_| "<fn>"))
            .finish()
    }
}
