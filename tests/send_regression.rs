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

/// A `RouterPolicy::run_boxed` future is `Send` — a router bridging its op into each
/// member's `ExecutionPolicy::run` survives `tokio::spawn`. This is the exact generic
/// op-composition that `run`/`run_owned` cannot make `Send` (issue #7); `run_boxed`'s
/// concrete `BoxFuture` op erases the generic future type so the composed future is
/// `Send` on stable Rust. If this regresses, the test stops compiling.
#[cfg(feature = "tokio")]
#[tokio::test]
async fn router_run_boxed_future_is_send() {
    use execution_policy::{BoxFuture, Member, Pick, RouterPolicy};
    let router = RouterPolicy::builder()
        .target(Member::new(
            "a".to_string(),
            ExecutionPolicyBuilder::<i32, ()>::new().build(),
        ))
        .select(Pick::first_healthy())
        .advance_when(|_e: &()| true)
        .build();
    let served = tokio::spawn(async move {
        router
            .run_boxed(|_id: String| -> BoxFuture<'static, Result<i32, ()>> {
                Box::pin(async { Ok::<i32, ()>(11) })
            })
            .await
    })
    .await
    .expect("spawned task joins");
    assert_eq!(served.ok().map(|s| s.value), Some(11));
}

/// The PRECISE issue-#7 failure mode: a `Send` future that BORROWS `&self` across
/// the `run_boxed` await — an `#[async_trait]` method (here hand-desugared to
/// `-> Pin<Box<dyn Future + Send + '_>>`). `tokio::spawn` above only proves the
/// `'static` case; this proves the borrowed-`&self` case that actually broke a
/// downstream consumer (praxec's `LlmExecutor::execute`). If `run_boxed`'s future
/// were not `Send` for ALL lifetimes, the `Box<dyn … + Send + '_>` coercion below
/// fails to compile with "implementation of `Send` is not general enough".
#[cfg(feature = "tokio")]
#[tokio::test]
async fn router_run_boxed_composes_in_send_borrowed_context() {
    use execution_policy::core::DefaultCore;
    use execution_policy::{BoxFuture, Member, Pick, RouterPolicy};
    use std::future::Future;
    use std::pin::Pin;

    struct Consumer {
        router: RouterPolicy<String, DefaultCore, i32, ()>,
    }
    impl Consumer {
        // Mirrors what `#[async_trait]` generates: a `Send` future borrowing `&self`.
        fn drive(&self) -> Pin<Box<dyn Future<Output = Option<i32>> + Send + '_>> {
            Box::pin(async move {
                self.router
                    .run_boxed(|_id: String| -> BoxFuture<'static, Result<i32, ()>> {
                        Box::pin(async { Ok::<i32, ()>(11) })
                    })
                    .await
                    .ok()
                    .map(|s| s.value)
            })
        }
    }

    let consumer = Consumer {
        router: RouterPolicy::builder()
            .target(Member::new(
                "a".to_string(),
                ExecutionPolicyBuilder::<i32, ()>::new().build(),
            ))
            .select(Pick::first_healthy())
            .advance_when(|_e: &()| true)
            .build(),
    };
    assert_eq!(consumer.drive().await, Some(11));
}
