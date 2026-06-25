//! `execution-policy` — closure-first reliability policies for any async operation.
//!
//! Wrap any async operation with retry, backoff, jitter, and per-attempt / total
//! timeouts (circuit breaking, bounded concurrency, and retry budgets build on
//! this foundation). The operation is a **factory** — re-invoked per attempt — so
//! requests are freshly constructed and never need `Clone`, and `!Send`
//! operations are accepted.
//!
//! ```
//! # #[cfg(feature = "tokio")]
//! # {
//! use std::time::Duration;
//! use execution_policy::{ExecutionPolicyBuilder, Retry};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let policy = ExecutionPolicyBuilder::<u32, &str>::new()
//!     .retry(Retry::exponential().max_attempts(3))
//!     .attempt_timeout(Duration::from_secs(2))
//!     .build();
//!
//! let value = policy.run(async || Ok::<_, &str>(7u32)).await.unwrap();
//! assert_eq!(value, 7);
//! # }
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod attempt;
pub mod breaker;
pub mod builder;
pub mod classify;
pub mod concurrency;
pub mod core;
pub mod error;
pub mod event;
pub mod policy;
pub mod retry;

pub(crate) mod engine;
pub(crate) mod plan;

pub use crate::attempt::Attempt;
pub use crate::breaker::CircuitBreaker;
pub use crate::builder::{BuildError, ExecutionPolicyBuilder};
pub use crate::classify::{FailureClass, RetryDecision};
pub use crate::concurrency::{ConcurrencyLimit, SaturationPolicy};
#[cfg(feature = "tokio")]
pub use crate::core::DefaultCore;
pub use crate::core::{BoxFuture, Core};
pub use crate::error::{BreakerState, ErrorContext, ExecutionError};
pub use crate::event::Event;
pub use crate::policy::ExecutionPolicy;
pub use crate::retry::{Backoff, Jitter, Retry, RetryBudget};
