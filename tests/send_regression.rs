//! Regression guard for the `Send`-ness of the engine future.
//!
//! `BoxFuture` — and therefore `ExecutionPolicy::run`/`execute` — must be `Send`, so a policy can
//! be `.await`ed inside `async-trait` `Send` futures and `tokio::spawn`ed tasks (the common case
//! when a policy wraps an HTTP/LLM call behind a `Send` trait method).
//!
//! If `BoxFuture` ever loses its `+ Send` bound, the engine future becomes `!Send` and these tests
//! **stop compiling** — a loud, build-breaking regression rather than a silent ergonomic loss.
//!
//! Style: one declarative behavioral assertion per test.

use execution_policy::BoxFuture;

#[cfg(feature = "tokio")]
use execution_policy::{CircuitBreaker, ExecutionPolicyBuilder, Jitter, Retry};
#[cfg(feature = "tokio")]
use std::time::Duration;

/// The `BoxFuture` alias the engine threads through `run`/`execute` is `Send` — the root property.
#[test]
fn box_future_is_send() {
    fn require_send<T: Send>() {}
    require_send::<BoxFuture<'static, ()>>();
}

/// A bare policy's `run` future is `Send`: spawned onto the runtime, it yields the operation value.
#[cfg(feature = "tokio")]
#[tokio::test]
async fn run_future_is_send() {
    let policy = ExecutionPolicyBuilder::<i32, ()>::new().build();
    let outcome = tokio::spawn(async move { policy.run(async || Ok::<i32, ()>(42)).await })
        .await
        .expect("spawned task joins");
    assert_eq!(outcome.ok(), Some(42));
}

/// A policy's `execute` future (the attempt-aware entry point) is `Send`.
#[cfg(feature = "tokio")]
#[tokio::test]
async fn execute_future_is_send() {
    let policy = ExecutionPolicyBuilder::<i32, ()>::new().build();
    let outcome =
        tokio::spawn(async move { policy.execute(async |_attempt| Ok::<i32, ()>(7)).await })
            .await
            .expect("spawned task joins");
    assert_eq!(outcome.ok(), Some(7));
}

/// A fully-composed policy (retry + attempt/total timeout + circuit breaker) keeps its `run`
/// future `Send`: the composed engine future also survives `tokio::spawn`.
#[cfg(feature = "tokio")]
#[tokio::test]
async fn composed_policy_run_future_is_send() {
    let policy = ExecutionPolicyBuilder::<i32, ()>::new()
        .retry(Retry::exponential().max_attempts(2).jitter(Jitter::Full))
        .attempt_timeout(Duration::from_secs(1))
        .total_timeout(Duration::from_secs(2))
        .circuit_breaker(CircuitBreaker::consecutive_failures(3))
        .build();
    let outcome = tokio::spawn(async move { policy.run(async || Ok::<i32, ()>(9)).await })
        .await
        .expect("spawned task joins");
    assert_eq!(outcome.ok(), Some(9));
}
