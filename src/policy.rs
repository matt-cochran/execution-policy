//! The hero type: a reusable, cheaply-cloneable reliability policy.

use std::sync::Arc;

use crate::attempt::Attempt;
use crate::builder::ExecutionPolicyBuilder;
use crate::core::Core;
use crate::engine::{run_pipeline, run_pipeline_boxed};
use crate::error::ExecutionError;
use crate::plan::Plan;

/// A reusable reliability policy. Cheap to clone (shares a compiled `Plan`).
pub struct ExecutionPolicy<C, T, E> {
    core: C,
    plan: Arc<Plan<T, E>>,
}

impl<C: std::fmt::Debug, T, E> std::fmt::Debug for ExecutionPolicy<C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPolicy")
            .field("core", &self.core)
            .field("plan", &self.plan)
            .finish()
    }
}

impl<C: Clone, T, E> Clone for ExecutionPolicy<C, T, E> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            plan: Arc::clone(&self.plan),
        }
    }
}

impl ExecutionPolicy<(), (), ()> {
    /// Start configuring a policy. The `T`/`E` types are inferred at the first
    /// `retry(..)` or execute call site.
    pub fn builder<T, E>() -> ExecutionPolicyBuilder<T, E> {
        ExecutionPolicyBuilder::new()
    }
}

impl<C, T, E> ExecutionPolicy<C, T, E> {
    pub(crate) fn from_parts(core: C, plan: Arc<Plan<T, E>>) -> Self {
        Self { core, plan }
    }
}

impl<C, T, E> ExecutionPolicy<C, T, E>
where
    C: Core,
{
    /// Current circuit-breaker state, or `None` if no breaker is configured.
    ///
    /// The state is reported against the [`Core`] clock, so a breaker whose
    /// cooldown has elapsed reads as `HalfOpen` immediately — without waiting
    /// for a call to arrive and drive the transition. This makes breaker health
    /// pollable when selecting a healthy target.
    pub fn circuit_state(&self) -> Option<crate::error::BreakerState> {
        let now = self.core.now();
        self.plan.breaker.as_ref().map(|b| b.runtime.state_at(now))
    }

    /// The instant at which the breaker stops cooling (leaves `Open`), while it
    /// is currently cooling. Returns `None` when no breaker is configured, when
    /// it is closed or half-open, or when the cooldown has already elapsed.
    pub fn cooling_until(&self) -> Option<std::time::Instant> {
        let now = self.core.now();
        self.plan
            .breaker
            .as_ref()
            .and_then(|b| b.runtime.cooling_until(now))
    }

    /// Run an operation that needs neither application state nor attempt metadata.
    pub async fn run<F>(&self, mut op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut() -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, async move |_attempt: Attempt| {
            op().await
        })
        .await
    }

    /// Run an operation that wants attempt metadata.
    pub async fn execute<F>(&self, op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut(Attempt) -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, op).await
    }

    /// `Send`-general variant of [`run`](Self::run): the op is a plain
    /// `FnMut` returning an OWNED, boxed future
    /// ([`BoxFuture<'static, _>`](crate::core::BoxFuture)) rather than an
    /// `AsyncFnMut` whose future borrows the closure.
    ///
    /// Same pipeline as `run` (retry, timeouts, breaker, concurrency), but because
    /// the op's future is a single concrete `Send` type — not `AsyncFnMut`'s
    /// higher-ranked `CallRefFuture<'a>` — the future this method returns is `Send`
    /// for ALL lifetimes. That is what lets a router compose it inside a caller
    /// whose own future must be `Send` (behind `#[async_trait]`, `tokio::spawn`),
    /// which `run`/`execute` cannot (issue #7). The price is one boxed allocation
    /// per attempt — negligible next to the network/LLM op it wraps.
    pub async fn run_boxed<F>(&self, mut op: F) -> Result<T, ExecutionError<E>>
    where
        F: FnMut() -> crate::core::BoxFuture<'static, Result<T, E>>,
    {
        run_pipeline_boxed(&self.core, &self.plan, move |_attempt: Attempt| op()).await
    }

    /// Run an operation with injected application state.
    pub async fn run_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S) -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, async move |_attempt: Attempt| {
            op(state).await
        })
        .await
    }

    /// Run an operation with injected state and attempt metadata.
    pub async fn execute_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S, Attempt) -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, async move |attempt: Attempt| {
            op(state, attempt).await
        })
        .await
    }
}
