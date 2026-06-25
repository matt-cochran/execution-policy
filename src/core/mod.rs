//! Runtime abstraction: clock, sleeping, and RNG behind one object-safe trait.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

#[cfg(any(feature = "tokio", feature = "test-util"))]
pub(crate) mod rng;

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use tokio::TokioCore;

#[cfg(feature = "test-util")]
mod test;
#[cfg(feature = "test-util")]
pub use test::{ManualClock, TestCore};

/// A boxed future. [`Core::sleep`] returns this so the trait stays object-safe
/// (`Arc<dyn Core>` works); the box is on the cold backoff path only.
///
/// `+ Send` so the engine future (and therefore `ExecutionPolicy::run`/`execute`)
/// is `Send` and can be `.await`ed inside `async-trait` `Send` futures and
/// `tokio::spawn`. Both cores already produce `Send` sleeps (Tokio's `Sleep` is
/// `Send`; `TestCore`'s `ManualSleep` is `Arc<Mutex<…>>`-backed).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The policy engine's access to time, sleeping, and randomness.
///
/// Object-safe by construction. Implementors: [`TokioCore`] (default) and
/// [`TestCore`] (deterministic, for tests).
pub trait Core {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
    /// A future that completes after `dur` of this `Core`'s time.
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()>;
    /// Next pseudo-random `u64` (used for jitter). Not cryptographic.
    fn next_u64(&self) -> u64;
}

/// The `Core` used by `ExecutionPolicy::builder().build()`.
#[cfg(feature = "tokio")]
pub type DefaultCore = TokioCore;
