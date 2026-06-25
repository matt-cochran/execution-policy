//! The hero type: a reusable, cheaply-cloneable reliability policy.

use std::sync::Arc;

use crate::attempt::Attempt;
use crate::builder::ExecutionPolicyBuilder;
use crate::core::Core;
use crate::engine::run_pipeline;
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

    /// Current circuit-breaker state, or `None` if no breaker is configured.
    pub fn circuit_state(&self) -> Option<crate::error::BreakerState> {
        self.plan.breaker.as_ref().map(|b| b.runtime.state())
    }
}

impl<C, T, E> ExecutionPolicy<C, T, E>
where
    C: Core,
{
    /// Run an operation that needs neither application state nor attempt metadata.
    pub async fn run<F>(&self, mut op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut() -> Result<T, E>,
    {
        run_pipeline(
            &self.core,
            &self.plan,
            async move |_attempt: Attempt<'_>| op().await,
        )
        .await
    }

    /// Run an operation that wants attempt metadata.
    pub async fn execute<F>(&self, op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut(Attempt<'_>) -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, op).await
    }

    /// Run an operation with injected application state.
    pub async fn run_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S) -> Result<T, E>,
    {
        run_pipeline(
            &self.core,
            &self.plan,
            async move |_attempt: Attempt<'_>| op(state).await,
        )
        .await
    }

    /// Run an operation with injected state and attempt metadata.
    pub async fn execute_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S, Attempt<'_>) -> Result<T, E>,
    {
        run_pipeline(&self.core, &self.plan, async move |attempt: Attempt<'_>| {
            op(state, attempt).await
        })
        .await
    }
}
