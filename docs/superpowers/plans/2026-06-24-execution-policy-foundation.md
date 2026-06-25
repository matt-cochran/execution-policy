# execution-policy — Plan 1: Foundation + Retry/Timeout Pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A shippable `execution-policy` crate that wraps any async operation with retry, exponential/fixed backoff, jitter, per-attempt and total timeouts, behind the four-method ergonomic API (`run`/`run_with`/`execute`/`execute_with`).

**Architecture:** A compiled, `Arc`-shared `Plan` holds immutable policy config. A single non-generic-as-possible `drive` loop (the engine) races each operation future against attempt/total deadlines using a hand-rolled `poll_fn` select (the cancellation seam — no `tokio::select!`, so the engine stays runtime-agnostic). Time, sleeping, and RNG are abstracted behind an object-safe `Core` trait; `TokioCore` is the default impl and `TestCore`/`ManualClock` give deterministic, no-real-sleep tests.

**Tech Stack:** Rust 2024 edition, async closures (`AsyncFnMut`), `tokio` (default feature, time only), no `rand`/`futures` deps (RNG and select are hand-rolled).

## Global Constraints

- **MSRV:** Rust 1.85. **Edition:** 2024. (Copy into `Cargo.toml` as `rust-version = "1.85"`, `edition = "2024"`.)
- **License:** BSD-3-Clause (Polly-semantics-inspired; not a port — do not name as official Polly).
- **No `Send`/`'static` bound** on user operations — the engine drives futures in place, never `spawn`s them.
- **Zero per-attempt heap allocation on the happy path** — operation futures are stack-pinned (`std::pin::pin!`); no `Box::pin` of the operation.
- **`--no-default-features` must compile** — all primitives are runtime-agnostic; only `TokioCore`/`DefaultCore` sit behind the `tokio` feature.
- **Object-safe `Core`** — `sleep` returns `Pin<Box<dyn Future>>` so `Arc<dyn Core>` is usable.
- **Naming (locked):** `attempt_timeout`/`total_timeout` (never bare `timeout`); `max_attempts` (never `max_retries`); `ExecutionPolicy`, `Retry`, `Backoff`, `Jitter`, `Attempt`, `ExecutionError`.
- **`Attempt::number()` is 1-based** (first attempt = 1).
- **No real sleeps in unit tests** — use `TestCore`/`ManualClock`.
- **TDD throughout; commit after every green step.**

---

## File Structure (Plan 1 scope)

```
Cargo.toml                       # package, features, deps
src/lib.rs                       # crate docs + re-exports
src/core/mod.rs                  # trait Core, BoxFuture; DefaultCore alias
src/core/rng.rs                  # SplitMix64 (internal, dep-free)
src/core/tokio.rs                # TokioCore                (feature = "tokio")
src/core/test.rs                 # TestCore, ManualClock     (feature = "test-util")
src/error.rs                     # ExecutionError, ErrorContext, BreakerState
src/classify.rs                  # FailureClass, RetryDecision, Classifier
src/retry/mod.rs                 # Retry (builder + presets)
src/retry/backoff.rs             # Backoff (fixed, exponential)
src/retry/jitter.rs              # Jitter (None, Full, Equal)
src/attempt.rs                   # Attempt<'_>
src/plan.rs                      # Plan (compiled config)
src/engine.rs                    # drive loop + poll_fn select + AttemptOutcome
src/builder.rs                   # ExecutionPolicyBuilder
src/policy.rs                    # ExecutionPolicy<C> + the four methods
tests/retry_pipeline.rs          # integration: retry + timeout behavior
tests/ergonomics.rs              # the four methods compile & run
```

---

### Task 1: Project scaffold + object-safe `Core` trait + RNG

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/core/mod.rs`
- Create: `src/core/rng.rs`
- Test: unit tests inside `src/core/rng.rs`

**Interfaces:**
- Produces:
  - `pub type BoxFuture<'a, T> = core::pin::Pin<Box<dyn core::future::Future<Output = T> + 'a>>;`
  - `pub trait Core { fn now(&self) -> std::time::Instant; fn sleep(&self, dur: std::time::Duration) -> BoxFuture<'_, ()>; fn next_u64(&self) -> u64; }`
  - `pub(crate) struct SplitMix64(u64);` with `fn new(seed: u64) -> Self` and `fn next_u64(&mut self) -> u64`.

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "execution-policy"
version = "0.0.0"
edition = "2024"
rust-version = "1.85"
license = "BSD-3-Clause"
description = "Closure-first, runtime-light reliability policies (retry, timeout, circuit breaking, bounded concurrency) for any async Rust operation."
repository = "https://github.com/matt-cochran/execution-policy"

[features]
default = ["tokio"]
tokio = ["dep:tokio"]
test-util = []
tracing = ["dep:tracing"]

[dependencies]
tokio = { version = "1", features = ["time", "sync"], optional = true }
tracing = { version = "0.1", optional = true }

[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt", "time"] }

[lints.rust]
missing_debug_implementations = "warn"
```

- [ ] **Step 2: Write the failing RNG test** in `src/core/rng.rs`

```rust
//! Internal dependency-free PRNG for jitter. Not cryptographic.

#[derive(Debug, Clone)]
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_same_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        let seq_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b);
    }

    #[test]
    fn differs_across_seeds_and_advances() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
        let first = SplitMix64::new(1).next_u64();
        let mut c = SplitMix64::new(1);
        let _ = c.next_u64();
        assert_ne!(first, c.next_u64()); // sequence advances
    }
}
```

- [ ] **Step 3: Run, verify it fails**

Run: `cargo test --lib rng -- --nocapture`
Expected: FAIL — `unimplemented!()` panics.

- [ ] **Step 4: Implement `next_u64`** (replace the `unimplemented!()` body)

```rust
    pub(crate) fn next_u64(&mut self) -> u64 {
        // SplitMix64 — public-domain reference algorithm.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
```

- [ ] **Step 5: Write `src/core/mod.rs`**

```rust
//! Runtime abstraction: clock, sleeping, and RNG behind one object-safe trait.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

pub(crate) mod rng;

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use tokio::TokioCore;

#[cfg(feature = "test-util")]
mod test;
#[cfg(feature = "test-util")]
pub use test::{ManualClock, TestCore};

/// A boxed future. `Core::sleep` returns this so the trait stays object-safe
/// (`Arc<dyn Core>` works); the box is on the cold backoff path only.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

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
```

- [ ] **Step 6: Write `src/lib.rs`**

```rust
//! `execution-policy` — closure-first reliability policies for any async operation.
//!
//! See the design spec under `docs/superpowers/specs/`.

#![forbid(unsafe_code)]

pub mod core;

pub use crate::core::{BoxFuture, Core};
#[cfg(feature = "tokio")]
pub use crate::core::DefaultCore;
```

- [ ] **Step 7: Run tests + no-default-features compile check**

Run: `cargo test --lib`
Expected: PASS (2 rng tests).
Run: `cargo build --no-default-features`
Expected: compiles clean (Core trait + rng have no tokio dependency).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml src/lib.rs src/core/mod.rs src/core/rng.rs
git commit -m "feat: object-safe Core trait + dep-free SplitMix64 rng"
```

---

### Task 2: `TokioCore` + `TestCore`/`ManualClock`

**Files:**
- Create: `src/core/tokio.rs`
- Create: `src/core/test.rs`
- Test: unit tests inside `src/core/test.rs`

**Interfaces:**
- Consumes: `Core`, `BoxFuture`, `rng::SplitMix64`.
- Produces:
  - `pub struct TokioCore;` impl `Core` (+ `TokioCore::new()`).
  - `pub struct ManualClock { .. }` with `fn new() -> Self`, `fn advance(&self, dur: Duration)`, `Clone`.
  - `pub struct TestCore { .. }` with `fn new(clock: ManualClock) -> Self`, `fn with_seed(clock: ManualClock, seed: u64) -> Self`, impl `Core`.

- [ ] **Step 1: Write `src/core/tokio.rs`**

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::rng::SplitMix64;
use super::{BoxFuture, Core};

/// Default production [`Core`]: tokio timers + a fast non-crypto RNG for jitter.
#[derive(Debug, Default)]
pub struct TokioCore {
    rng_state: AtomicU64,
}

impl TokioCore {
    pub fn new() -> Self {
        // Seed from process-relative nanos; jitter quality only, not security.
        let seed = Instant::now().elapsed().as_nanos() as u64 ^ 0x2545_F491_4F6C_DD1D;
        Self { rng_state: AtomicU64::new(seed.max(1)) }
    }
}

impl Core for TokioCore {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(dur))
    }

    fn next_u64(&self) -> u64 {
        let s = self.rng_state.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        SplitMix64::new(s).next_u64()
    }
}
```

- [ ] **Step 2: Write the failing `TestCore` tests** in `src/core/test.rs`

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use super::rng::SplitMix64;
use super::{BoxFuture, Core};

struct ClockState {
    base: Instant,
    offset: Duration,
    wakers: Vec<(Duration, Waker)>,
}

/// A virtual clock for deterministic tests. `now()` advances only via
/// [`ManualClock::advance`]; sleeps registered on it resolve when crossed.
#[derive(Clone)]
pub struct ManualClock(Arc<Mutex<ClockState>>);

impl std::fmt::Debug for ManualClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManualClock")
    }
}

impl ManualClock {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ClockState {
            base: Instant::now(),
            offset: Duration::ZERO,
            wakers: Vec::new(),
        })))
    }

    pub fn advance(&self, dur: Duration) {
        let mut st = self.0.lock().unwrap();
        st.offset += dur;
        let now = st.offset;
        let mut ready = Vec::new();
        st.wakers.retain(|(deadline, w)| {
            if *deadline <= now {
                ready.push(w.clone());
                false
            } else {
                true
            }
        });
        drop(st);
        for w in ready {
            w.wake();
        }
    }

    fn now(&self) -> Instant {
        let st = self.0.lock().unwrap();
        st.base + st.offset
    }

    fn register(&self, deadline: Duration, waker: Waker) {
        self.0.lock().unwrap().wakers.push((deadline, waker));
    }

    fn offset(&self) -> Duration {
        self.0.lock().unwrap().offset
    }
}

struct ManualSleep {
    clock: ManualClock,
    deadline: Duration,
}

impl Future for ManualSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.clock.offset() >= self.deadline {
            Poll::Ready(())
        } else {
            self.clock.register(self.deadline, cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Deterministic [`Core`] for tests: virtual clock + seeded RNG, no real sleeps.
#[derive(Debug)]
pub struct TestCore {
    clock: ManualClock,
    rng: Mutex<SplitMix64>,
}

impl TestCore {
    pub fn new(clock: ManualClock) -> Self {
        Self::with_seed(clock, 0xDEAD_BEEF)
    }

    pub fn with_seed(clock: ManualClock, seed: u64) -> Self {
        Self { clock, rng: Mutex::new(SplitMix64::new(seed)) }
    }
}

impl Core for TestCore {
    fn now(&self) -> Instant {
        self.clock.now()
    }

    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        let deadline = self.clock.offset() + dur;
        Box::pin(ManualSleep { clock: self.clock.clone(), deadline })
    }

    fn next_u64(&self) -> u64 {
        self.rng.lock().unwrap().next_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_advances_only_on_advance() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let t0 = core.now();
        clock.advance(Duration::from_secs(5));
        assert_eq!(core.now().duration_since(t0), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn sleep_resolves_when_clock_crosses_deadline() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let mut fut = Box::pin(core.sleep(Duration::from_secs(10)));
        // Not ready yet.
        let poll = futures_noop_poll(&mut fut);
        assert!(poll.is_pending());
        clock.advance(Duration::from_secs(10));
        fut.await; // now resolves
    }

    // Minimal manual poll helper (avoids a `futures` dependency).
    fn futures_noop_poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VT)
        }
        static VT: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VT)) };
        let mut cx = Context::from_waker(&waker);
        Pin::new(f).poll(&mut cx)
    }
}
```

- [ ] **Step 3: Run, verify it fails then passes**

Run: `cargo test --features test-util --lib core::test`
Expected: PASS (both tests). If the file didn't compile before the impls existed, that's the "fail" — fix until green.

- [ ] **Step 4: Wire the modules** — confirm `src/core/mod.rs` already `mod tokio;`/`mod test;` gates (done in Task 1). Run:

Run: `cargo build` and `cargo build --features test-util` and `cargo build --no-default-features --features test-util`
Expected: all compile (test-util has no tokio dependency).

- [ ] **Step 5: Commit**

```bash
git add src/core/tokio.rs src/core/test.rs
git commit -m "feat: TokioCore + deterministic TestCore/ManualClock"
```

---

### Task 3: Error model (`ExecutionError`, `ErrorContext`, `BreakerState`)

**Files:**
- Create: `src/error.rs`
- Modify: `src/lib.rs` (add `pub mod error;` + re-exports)
- Test: unit tests inside `src/error.rs`

**Interfaces:**
- Produces:
  - `pub enum BreakerState { Disabled, Closed, Open, HalfOpen }` (Plan 1 only ever sets `Disabled`).
  - `pub struct ErrorContext { pub attempts: u32, pub elapsed: Duration, pub last_delay: Option<Duration>, pub breaker_state: BreakerState }`
  - `#[non_exhaustive] pub enum ExecutionError<E> { Operation(E), AttemptTimeout, TotalTimeout, CircuitOpen, ConcurrencyRejected, RetryBudgetExhausted }`
  - Each variant carries context via a shared `Box<ErrorContext>`: actual shape is `Operation { source: E, context: Box<ErrorContext> }`, etc. Accessor `fn context(&self) -> &ErrorContext`, `fn into_inner(self) -> Option<E>`, predicates `is_timeout/is_circuit_open/is_rejected/is_exhausted`.
  - `impl<E: std::error::Error + 'static> std::error::Error for ExecutionError<E>` with `Operation`'s `source`.

- [ ] **Step 1: Write the failing tests** in `src/error.rs`

```rust
//! Typed failure outcomes with rich, fail-fast diagnostic context.

use std::time::Duration;

/// Circuit-breaker state at the moment of failure. In Plan 1 always `Disabled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Disabled,
    Closed,
    Open,
    HalfOpen,
}

/// Diagnostic context attached to every [`ExecutionError`].
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub attempts: u32,
    pub elapsed: Duration,
    pub last_delay: Option<Duration>,
    pub breaker_state: BreakerState,
}

/// Why an execution failed. Boxed context keeps the hot `Result` small.
#[non_exhaustive]
#[derive(Debug)]
pub enum ExecutionError<E> {
    Operation { source: E, context: Box<ErrorContext> },
    AttemptTimeout { context: Box<ErrorContext> },
    TotalTimeout { context: Box<ErrorContext> },
    CircuitOpen { context: Box<ErrorContext> },
    ConcurrencyRejected { context: Box<ErrorContext> },
    RetryBudgetExhausted { context: Box<ErrorContext> },
}

impl<E> ExecutionError<E> {
    pub fn context(&self) -> &ErrorContext {
        match self {
            Self::Operation { context, .. }
            | Self::AttemptTimeout { context }
            | Self::TotalTimeout { context }
            | Self::CircuitOpen { context }
            | Self::ConcurrencyRejected { context }
            | Self::RetryBudgetExhausted { context } => context,
        }
    }

    pub fn into_inner(self) -> Option<E> {
        match self {
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::AttemptTimeout { .. } | Self::TotalTimeout { .. })
    }
    pub fn is_circuit_open(&self) -> bool {
        matches!(self, Self::CircuitOpen { .. })
    }
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::ConcurrencyRejected { .. })
    }
    pub fn is_exhausted(&self) -> bool {
        matches!(self, Self::Operation { .. } | Self::RetryBudgetExhausted { .. })
    }
}

impl<E: std::fmt::Display> std::fmt::Display for ExecutionError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ctx = self.context();
        match self {
            Self::Operation { source, .. } => write!(
                f,
                "operation failed after {} attempt(s) in {:?}: {source}",
                ctx.attempts, ctx.elapsed
            ),
            Self::AttemptTimeout { .. } => write!(f, "attempt timed out (attempt {})", ctx.attempts),
            Self::TotalTimeout { .. } => {
                write!(f, "total timeout after {:?} ({} attempts)", ctx.elapsed, ctx.attempts)
            }
            Self::CircuitOpen { .. } => write!(f, "circuit open"),
            Self::ConcurrencyRejected { .. } => write!(f, "concurrency limit rejected the call"),
            Self::RetryBudgetExhausted { .. } => write!(f, "retry budget exhausted"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ExecutionError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Box<ErrorContext> {
        Box::new(ErrorContext {
            attempts: 3,
            elapsed: Duration::from_millis(120),
            last_delay: Some(Duration::from_millis(50)),
            breaker_state: BreakerState::Disabled,
        })
    }

    #[test]
    fn predicates_and_context() {
        let e: ExecutionError<std::io::Error> = ExecutionError::TotalTimeout { context: ctx() };
        assert!(e.is_timeout());
        assert!(!e.is_circuit_open());
        assert_eq!(e.context().attempts, 3);
    }

    #[test]
    fn into_inner_recovers_operation_error() {
        let src = std::io::Error::other("boom");
        let e = ExecutionError::Operation { source: src, context: ctx() };
        assert_eq!(e.into_inner().unwrap().to_string(), "boom");
    }

    #[test]
    fn error_source_chains() {
        use std::error::Error;
        let src = std::io::Error::other("io fail");
        let e = ExecutionError::Operation { source: src, context: ctx() };
        assert!(e.source().is_some());
    }
}
```

- [ ] **Step 2: Run, verify it fails** (module not declared yet)

Run: `cargo test --lib error`
Expected: FAIL — `error` module not found.

- [ ] **Step 3: Declare the module** in `src/lib.rs`

```rust
pub mod error;

pub use crate::error::{BreakerState, ErrorContext, ExecutionError};
```

- [ ] **Step 4: Run, verify it passes**

Run: `cargo test --lib error`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/error.rs src/lib.rs
git commit -m "feat: ExecutionError with boxed context, Error+source, predicates"
```

---

### Task 4: Classification (`FailureClass`, `RetryDecision`, `Classifier`)

**Files:**
- Create: `src/classify.rs`
- Modify: `src/lib.rs`
- Test: unit tests inside `src/classify.rs`

**Interfaces:**
- Produces:
  - `pub enum FailureClass { Success, Retryable, Permanent, Ignored }`
  - `#[derive(PartialEq)] pub enum RetryDecision { Retry, Stop }`
  - `pub(crate) type ErrorPredicate<E> = Box<dyn Fn(&E) -> bool + Send + Sync>;`
  - `pub(crate) type OutcomeClassifier<T, E> = Box<dyn Fn(&Result<T, E>) -> RetryDecision + Send + Sync>;`
  - `pub(crate) enum Classifier<T, E> { RetryAll, WhenErr(ErrorPredicate<E>), WhenOutcome(OutcomeClassifier<T, E>) }` with `fn decide(&self, outcome: &Result<T, E>) -> RetryDecision`.

Note: `Result<T, E>` *is* the outcome — there is no separate `Outcome` type (per spec §6).

- [ ] **Step 1: Write the failing tests** in `src/classify.rs`

```rust
//! Failure classification, kept independent from retry/breaker policy.

/// Coarse classification of an outcome (reserved for breaker use in Plan 2).
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

/// How an outcome maps to a retry decision.
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
        // e.g. an Ok(503) HTTP response is retryable
        let c: Classifier<u32, &str> =
            Classifier::WhenOutcome(Box::new(|o: &Result<u32, &str>| match o {
                Ok(503) => RetryDecision::Retry,
                _ => RetryDecision::Stop,
            }));
        assert_eq!(c.decide(&Ok(503)), RetryDecision::Retry);
        assert_eq!(c.decide(&Ok(200)), RetryDecision::Stop);
    }
}
```

- [ ] **Step 2: Run, verify it fails** (module not declared)

Run: `cargo test --lib classify`
Expected: FAIL — module not found.

- [ ] **Step 3: Declare module + re-export public types** in `src/lib.rs`

```rust
pub mod classify;

pub use crate::classify::{FailureClass, RetryDecision};
```

- [ ] **Step 4: Run, verify it passes**

Run: `cargo test --lib classify`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/classify.rs src/lib.rs
git commit -m "feat: outcome classification over &Result (no parallel Outcome type)"
```

---

### Task 5: `Jitter` + `Backoff` (delay math)

**Files:**
- Create: `src/retry/jitter.rs`
- Create: `src/retry/backoff.rs`
- Create: `src/retry/mod.rs` (stub declaring submodules, expanded in Task 6)
- Modify: `src/lib.rs`
- Test: unit tests inside each file

**Interfaces:**
- Produces:
  - `pub enum Jitter { None, Full, Equal }` with `pub(crate) fn apply(&self, base: Duration, rng: u64) -> Duration`.
  - `pub enum Backoff { Fixed(Duration), Exponential { base: Duration, max: Duration } }` with constructors `Backoff::fixed(d)`, `Backoff::exponential(base, max)`, and `pub(crate) fn raw_delay(&self, attempt: u32) -> Duration` (attempt is 1-based; delay applies *after* attempt N before attempt N+1).

- [ ] **Step 1: Write `src/retry/jitter.rs` with failing tests**

```rust
use std::time::Duration;

/// Randomization applied to a backoff delay to de-correlate retriers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Jitter {
    /// No jitter — use the raw delay.
    None,
    /// Uniform random in `[0, base]` (AWS "full jitter").
    Full,
    /// `base/2 + uniform(0, base/2)` (AWS "equal jitter").
    Equal,
}

impl Jitter {
    /// Apply jitter to `base` using one random `u64`.
    pub(crate) fn apply(&self, base: Duration, rng: u64) -> Duration {
        if base.is_zero() {
            return base;
        }
        let nanos = base.as_nanos() as u64;
        // Fraction in [0, 1) scaled from the high bits of rng.
        let frac = |span: u64| -> u64 {
            if span == 0 { 0 } else { ((rng >> 11) as u128 * span as u128 >> 53) as u64 }
        };
        match self {
            Jitter::None => base,
            Jitter::Full => Duration::from_nanos(frac(nanos)),
            Jitter::Equal => {
                let half = nanos / 2;
                Duration::from_nanos(half + frac(half))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_identity() {
        let d = Duration::from_millis(100);
        assert_eq!(Jitter::None.apply(d, u64::MAX), d);
    }

    #[test]
    fn full_is_within_bounds() {
        let d = Duration::from_millis(100);
        for rng in [0u64, 1, 12345, u64::MAX / 2, u64::MAX] {
            let j = Jitter::Full.apply(d, rng);
            assert!(j <= d, "full jitter {j:?} exceeded base {d:?}");
        }
    }

    #[test]
    fn equal_is_in_upper_half() {
        let d = Duration::from_millis(100);
        for rng in [0u64, 999, u64::MAX] {
            let j = Jitter::Equal.apply(d, rng);
            assert!(j >= d / 2 && j <= d, "equal jitter {j:?} out of [50ms,100ms]");
        }
    }

    #[test]
    fn zero_base_stays_zero() {
        assert_eq!(Jitter::Full.apply(Duration::ZERO, 42), Duration::ZERO);
    }
}
```

- [ ] **Step 2: Run jitter tests**

Run: `cargo test --lib jitter`
Expected: PASS (4 tests). (If a bound assert fails, fix `apply` until green.)

- [ ] **Step 3: Write `src/retry/backoff.rs` with failing tests**

```rust
use std::time::Duration;

/// Delay schedule between attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    /// Constant delay.
    Fixed(Duration),
    /// `base * 2^(attempt-1)`, clamped to `max`.
    Exponential { base: Duration, max: Duration },
}

impl Backoff {
    pub fn fixed(d: Duration) -> Self {
        Backoff::Fixed(d)
    }
    pub fn exponential(base: Duration, max: Duration) -> Self {
        Backoff::Exponential { base, max }
    }

    /// Raw (pre-jitter) delay to wait *after* `attempt` (1-based) before the next try.
    pub(crate) fn raw_delay(&self, attempt: u32) -> Duration {
        match self {
            Backoff::Fixed(d) => *d,
            Backoff::Exponential { base, max } => {
                let shift = attempt.saturating_sub(1).min(63);
                let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
                base.checked_mul(factor as u32).unwrap_or(*max).min(*max)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_is_constant() {
        let b = Backoff::fixed(Duration::from_millis(50));
        assert_eq!(b.raw_delay(1), Duration::from_millis(50));
        assert_eq!(b.raw_delay(5), Duration::from_millis(50));
    }

    #[test]
    fn exponential_doubles_then_clamps() {
        let b = Backoff::exponential(Duration::from_millis(50), Duration::from_millis(400));
        assert_eq!(b.raw_delay(1), Duration::from_millis(50));
        assert_eq!(b.raw_delay(2), Duration::from_millis(100));
        assert_eq!(b.raw_delay(3), Duration::from_millis(200));
        assert_eq!(b.raw_delay(4), Duration::from_millis(400));
        assert_eq!(b.raw_delay(5), Duration::from_millis(400)); // clamped
        assert_eq!(b.raw_delay(40), Duration::from_millis(400)); // no overflow
    }
}
```

- [ ] **Step 4: Write `src/retry/mod.rs` stub + wire lib**

```rust
//! Retry policy, backoff schedules, and jitter.

pub mod backoff;
pub mod jitter;

pub use backoff::Backoff;
pub use jitter::Jitter;
```

In `src/lib.rs` add:

```rust
pub mod retry;

pub use crate::retry::{Backoff, Jitter};
```

- [ ] **Step 5: Run all retry math tests + overflow check**

Run: `cargo test --lib retry`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/retry/jitter.rs src/retry/backoff.rs src/retry/mod.rs src/lib.rs
git commit -m "feat: Backoff (fixed/exponential, overflow-safe) + Jitter (full/equal)"
```

---

### Task 6: `Retry` config + presets + delay composition

**Files:**
- Modify: `src/retry/mod.rs`
- Test: unit tests inside `src/retry/mod.rs`

**Interfaces:**
- Consumes: `Backoff`, `Jitter`, `classify::{Classifier, ErrorPredicate}`, `Core`.
- Produces `Retry<T, E>`:
  - Presets: `Retry::none()`, `Retry::fixed(Duration)`, `Retry::exponential()`, `Retry::standard()`.
  - Builder methods (consume-self): `max_attempts(u32)`, `max_elapsed(Duration)`, `base_delay(Duration)`, `max_delay(Duration)`, `jitter(Jitter)`, `when(impl Fn(&E)->bool + Send + Sync + 'static)`, `when_outcome(impl Fn(&Result<T,E>)->RetryDecision + Send + Sync + 'static)`.
  - `pub(crate) fn max_attempts_value(&self) -> u32`, `pub(crate) fn max_elapsed_value(&self) -> Option<Duration>`, `pub(crate) fn decide(&self, outcome: &Result<T,E>) -> RetryDecision`, `pub(crate) fn delay(&self, attempt: u32, core: &dyn Core) -> Duration` (raw_delay → jitter via `core.next_u64()`).

- [ ] **Step 1: Append the failing tests + skeleton** to `src/retry/mod.rs`

```rust
use std::time::Duration;

use crate::classify::{Classifier, RetryDecision};
use crate::core::Core;

/// Retry configuration: attempt cap, backoff, jitter, and classification.
pub struct Retry<T, E> {
    max_attempts: u32,
    max_elapsed: Option<Duration>,
    backoff: Backoff,
    jitter: Jitter,
    classifier: Classifier<T, E>,
}

impl<T, E> std::fmt::Debug for Retry<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retry")
            .field("max_attempts", &self.max_attempts)
            .field("max_elapsed", &self.max_elapsed)
            .field("backoff", &self.backoff)
            .field("jitter", &self.jitter)
            .field("classifier", &self.classifier)
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
        Self::base(3, Backoff::exponential(Duration::from_millis(100), Duration::from_secs(10)))
    }
    /// Sensible transient-only default: exponential + full jitter, 4 attempts.
    /// (Transient-only classifier is applied by callers via `.when(..)`; this
    /// preset only sets the schedule — see spec §6.)
    pub fn standard() -> Self {
        Self::base(4, Backoff::exponential(Duration::from_millis(100), Duration::from_secs(2)))
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

    pub(crate) fn max_attempts_value(&self) -> u32 {
        self.max_attempts
    }
    pub(crate) fn max_elapsed_value(&self) -> Option<Duration> {
        self.max_elapsed
    }
    pub(crate) fn decide(&self, outcome: &Result<T, E>) -> RetryDecision {
        self.classifier.decide(outcome)
    }
    pub(crate) fn delay(&self, attempt: u32, core: &dyn Core) -> Duration {
        let raw = self.backoff.raw_delay(attempt);
        self.jitter.apply(raw, core.next_u64())
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};

    #[test]
    fn presets_have_expected_attempt_caps() {
        assert_eq!(Retry::<(), ()>::none().max_attempts_value(), 1);
        assert_eq!(Retry::<(), ()>::fixed(Duration::ZERO).max_attempts_value(), 3);
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
        // base 20 -> 40 -> 80 -> clamp
        let core = TestCore::new(ManualClock::new());
        // Jitter::None by default, so delay == raw.
        assert_eq!(r.delay(1, &core), Duration::from_millis(20));
        assert_eq!(r.delay(3, &core), Duration::from_millis(80));
        assert_eq!(r.delay(9, &core), Duration::from_millis(80));
    }

    #[test]
    fn when_predicate_controls_decision() {
        let r = Retry::<u32, i32>::exponential().when(|e: &i32| *e >= 500);
        assert_eq!(r.decide(&Err(503)), RetryDecision::Retry);
        assert_eq!(r.decide(&Err(404)), RetryDecision::Stop);
        assert_eq!(r.decide(&Ok(1)), RetryDecision::Stop);
    }
}
```

- [ ] **Step 2: Run, verify failure then green**

Run: `cargo test --features test-util --lib retry`
Expected: PASS (3 new tests + the 6 math tests).

- [ ] **Step 3: Re-export `Retry`** in `src/retry/mod.rs` top and `src/lib.rs`

In `src/retry/mod.rs` (top, after submodule decls): `pub use self::Retry;` is implicit; just ensure `Retry` is defined in this module (it is). In `src/lib.rs`:

```rust
pub use crate::retry::Retry;
```

- [ ] **Step 4: Confirm build**

Run: `cargo build` then `cargo build --no-default-features --features test-util`
Expected: both compile.

- [ ] **Step 5: Commit**

```bash
git add src/retry/mod.rs src/lib.rs
git commit -m "feat: Retry config + presets (none/fixed/exponential/standard) + classification"
```

---

### Task 7: `Attempt` + `Plan`

**Files:**
- Create: `src/attempt.rs`
- Create: `src/plan.rs`
- Modify: `src/lib.rs`
- Test: unit tests inside each file

**Interfaces:**
- Produces:
  - `#[non_exhaustive] pub struct Attempt<'a> { .. }` with `pub fn number(&self) -> u32` (1-based) and `pub fn elapsed(&self) -> Duration`; constructed via `pub(crate) fn new(number: u32, start: Instant, now: Instant) -> Self`. The `'a` lifetime is reserved for future borrowed metadata (keeps the signature stable).
  - `pub(crate) struct Plan<T, E> { pub retry: Retry<T, E>, pub attempt_timeout: Option<Duration>, pub total_timeout: Option<Duration> }`.

- [ ] **Step 1: Write `src/attempt.rs` with tests**

```rust
//! Per-attempt metadata handed to operation closures.

use std::marker::PhantomData;
use std::time::{Duration, Instant};

/// Metadata for the current attempt. `number()` is 1-based.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct Attempt<'a> {
    number: u32,
    start: Instant,
    now: Instant,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Attempt<'a> {
    pub(crate) fn new(number: u32, start: Instant, now: Instant) -> Self {
        Self { number, start, now, _borrow: PhantomData }
    }

    /// 1-based attempt index (first attempt returns 1).
    pub fn number(&self) -> u32 {
        self.number
    }

    /// Time elapsed since the first attempt began.
    pub fn elapsed(&self) -> Duration {
        self.now.duration_since(self.start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_is_one_based() {
        let t = Instant::now();
        let a = Attempt::new(1, t, t);
        assert_eq!(a.number(), 1);
    }

    #[test]
    fn elapsed_reflects_clock() {
        let start = Instant::now();
        let now = start + Duration::from_millis(250);
        let a = Attempt::new(2, start, now);
        assert_eq!(a.elapsed(), Duration::from_millis(250));
    }
}
```

- [ ] **Step 2: Write `src/plan.rs`**

```rust
//! Compiled, immutable policy configuration shared behind an `Arc`.

use std::time::Duration;

use crate::retry::Retry;

pub(crate) struct Plan<T, E> {
    pub(crate) retry: Retry<T, E>,
    pub(crate) attempt_timeout: Option<Duration>,
    pub(crate) total_timeout: Option<Duration>,
}

impl<T, E> std::fmt::Debug for Plan<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plan")
            .field("retry", &self.retry)
            .field("attempt_timeout", &self.attempt_timeout)
            .field("total_timeout", &self.total_timeout)
            .finish()
    }
}
```

- [ ] **Step 3: Declare modules + re-export `Attempt`** in `src/lib.rs`

```rust
pub mod attempt;
pub(crate) mod plan;

pub use crate::attempt::Attempt;
```

- [ ] **Step 4: Run**

Run: `cargo test --lib attempt`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/attempt.rs src/plan.rs src/lib.rs
git commit -m "feat: 1-based Attempt + compiled Plan"
```

---

### Task 8: Engine — `poll_fn` select + `drive` loop (the spine)

**Files:**
- Create: `src/engine.rs`
- Modify: `src/lib.rs` (`pub(crate) mod engine;`)
- Test: unit tests inside `src/engine.rs` (using `TestCore`)

**Interfaces:**
- Consumes: `Core`, `Plan`, `Attempt`, `ExecutionError`, `ErrorContext`, `BreakerState`, `RetryDecision`.
- Produces:
  - `pub(crate) enum AttemptOutcome<T, E> { Completed(Result<T, E>), AttemptTimeout, TotalTimeout }`
  - `pub(crate) async fn drive<C, F, T, E>(core: &C, plan: &Plan<T, E>, op: F) -> Result<T, ExecutionError<E>> where C: Core + ?Sized, F: AsyncFnMut(Attempt<'_>) -> Result<T, E>`

This is the single execution path; all four public methods delegate here.

- [ ] **Step 1: Write the engine with the select helper + drive loop**

```rust
//! The execution engine: one `drive` loop racing the operation against
//! attempt/total deadlines via a hand-rolled `poll_fn` select. The select is
//! the cancellation seam — a future cooperative-token branch slots in here.

use std::future::poll_fn;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::attempt::Attempt;
use crate::core::Core;
use crate::error::{BreakerState, ErrorContext, ExecutionError};
use crate::classify::RetryDecision;
use crate::plan::Plan;

pub(crate) enum AttemptOutcome<T, E> {
    Completed(Result<T, E>),
    AttemptTimeout,
    TotalTimeout,
}

fn context(attempts: u32, start: Instant, now: Instant, last_delay: Option<Duration>) -> Box<ErrorContext> {
    Box::new(ErrorContext {
        attempts,
        elapsed: now.duration_since(start),
        last_delay,
        breaker_state: BreakerState::Disabled,
    })
}

pub(crate) async fn drive<C, F, T, E>(
    core: &C,
    plan: &Plan<T, E>,
    mut op: F,
) -> Result<T, ExecutionError<E>>
where
    C: Core + ?Sized,
    F: AsyncFnMut(Attempt<'_>) -> Result<T, E>,
{
    let start = core.now();
    let max_attempts = plan.retry.max_attempts_value();
    let total_deadline = plan.total_timeout.map(|t| start + t);
    let mut last_delay: Option<Duration> = None;

    let mut attempt_no: u32 = 1;
    loop {
        let now = core.now();
        let attempt = Attempt::new(attempt_no, start, now);

        // Drive the operation, racing attempt + total deadlines. Operation
        // future is stack-pinned — no per-attempt heap allocation.
        let op_fut = op(attempt);
        let mut op_fut = pin!(op_fut);

        let attempt_timeout = plan.attempt_timeout;
        let remaining_total = total_deadline.map(|d| d.saturating_duration_since(core.now()));

        let mut at_sleep = attempt_timeout.map(|t| core.sleep(t));
        let mut tot_sleep = remaining_total.map(|t| core.sleep(t));

        let outcome = poll_fn(|cx| {
            if let Poll::Ready(r) = op_fut.as_mut().poll(cx) {
                return Poll::Ready(AttemptOutcome::Completed(r));
            }
            if let Some(s) = at_sleep.as_mut() {
                if std::pin::Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::AttemptTimeout);
                }
            }
            if let Some(s) = tot_sleep.as_mut() {
                if std::pin::Pin::new(s).poll(cx).is_ready() {
                    return Poll::Ready(AttemptOutcome::TotalTimeout);
                }
            }
            Poll::Pending
        })
        .await;

        let now = core.now();
        match outcome {
            AttemptOutcome::TotalTimeout => {
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, now, last_delay),
                });
            }
            AttemptOutcome::Completed(result) => {
                let decision = plan.retry.decide(&result);
                match (result, decision) {
                    (Ok(v), RetryDecision::Stop) => return Ok(v),
                    (Err(e), RetryDecision::Stop) => {
                        return Err(ExecutionError::Operation {
                            source: e,
                            context: context(attempt_no, start, now, last_delay),
                        });
                    }
                    // Retry requested (Ok-retry or Err-retry): fall through to backoff.
                    (Ok(v), RetryDecision::Retry) if attempt_no >= max_attempts => return Ok(v),
                    (Err(e), RetryDecision::Retry) if attempt_no >= max_attempts => {
                        return Err(ExecutionError::Operation {
                            source: e,
                            context: context(attempt_no, start, now, last_delay),
                        });
                    }
                    _ => { /* schedule backoff below */ }
                }
            }
            AttemptOutcome::AttemptTimeout => {
                // An attempt timeout is a retryable failure; exhaust like any other.
                if attempt_no >= max_attempts {
                    return Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay),
                    });
                }
            }
        }

        // max_elapsed guard
        if let Some(max_el) = plan.retry.max_elapsed_value() {
            if now.duration_since(start) >= max_el {
                return Err(ExecutionError::Operation {
                    // Best-effort: max_elapsed without a stored error surfaces as
                    // AttemptTimeout-style exhaustion is wrong; use a dedicated path.
                    source: unreachable_marker(),
                    context: context(attempt_no, start, now, last_delay),
                });
            }
        }

        // Backoff before the next attempt, capped by the total deadline.
        let delay = plan.retry.delay(attempt_no, core);
        last_delay = Some(delay);
        let delay = match total_deadline {
            Some(d) => delay.min(d.saturating_duration_since(core.now())),
            None => delay,
        };
        if !delay.is_zero() {
            core.sleep(delay).await;
        }
        // If the total deadline has now passed, report it.
        if let Some(d) = total_deadline {
            if core.now() >= d {
                return Err(ExecutionError::TotalTimeout {
                    context: context(attempt_no, start, core.now(), last_delay),
                });
            }
        }

        attempt_no += 1;
    }
}

// `max_elapsed` needs to surface the *last* error. Plan 1 keeps the last error
// by value across iterations; see Step 3 refinement. This marker is replaced there.
fn unreachable_marker<E>() -> E {
    unreachable!("max_elapsed path is refined in Step 3 to carry the last error")
}
```

- [ ] **Step 2: Run — verify it compiles and the obvious paths fail correctly**

Run: `cargo build --features test-util`
Expected: compiles. (The `unreachable_marker` is a known TODO refined in Step 3 — do not ship it.)

- [ ] **Step 3: Refine `drive` to carry the last error for `max_elapsed`**

Replace the loop's error handling so the most recent `Err` is retained and reused by the `max_elapsed` guard. Apply this diff to the body: track `let mut last_error: Option<E> = None;` near `last_delay`; in the `(Err(e), RetryDecision::Retry)` non-exhausted arm, store `last_error = Some(e)` *before* falling through (clone-free: move it, then it's overwritten next attempt); change the `max_elapsed` guard to:

```rust
        if let Some(max_el) = plan.retry.max_elapsed_value() {
            if now.duration_since(start) >= max_el {
                return match last_error.take() {
                    Some(e) => Err(ExecutionError::Operation {
                        source: e,
                        context: context(attempt_no, start, now, last_delay),
                    }),
                    None => Err(ExecutionError::AttemptTimeout {
                        context: context(attempt_no, start, now, last_delay),
                    }),
                };
            }
        }
```

Delete `unreachable_marker`. Adjust the retry arm:

```rust
                    (Err(e), RetryDecision::Retry) => {
                        if attempt_no >= max_attempts {
                            return Err(ExecutionError::Operation {
                                source: e,
                                context: context(attempt_no, start, now, last_delay),
                            });
                        }
                        last_error = Some(e);
                    }
                    (Ok(v), RetryDecision::Retry) => {
                        if attempt_no >= max_attempts {
                            return Ok(v);
                        }
                    }
                    (Ok(v), RetryDecision::Stop) => return Ok(v),
                    (Err(e), RetryDecision::Stop) => {
                        return Err(ExecutionError::Operation {
                            source: e,
                            context: context(attempt_no, start, now, last_delay),
                        });
                    }
```

- [ ] **Step 4: Add engine unit tests** (append to `src/engine.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};
    use crate::retry::Retry;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn plan(retry: Retry<u32, &'static str>, attempt_to: Option<Duration>, total_to: Option<Duration>) -> Plan<u32, &'static str> {
        Plan { retry, attempt_timeout: attempt_to, total_timeout: total_to }
    }

    #[tokio::test]
    async fn succeeds_first_try() {
        let core = TestCore::new(ManualClock::new());
        let p = plan(Retry::none(), None, None);
        let r = drive(&core, &p, async |_a| Ok::<_, &str>(7u32)).await;
        assert_eq!(r.unwrap(), 7);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let clock = ManualClock::new();
        let core = TestCore::new(clock.clone());
        let p = plan(Retry::fixed(Duration::ZERO).max_attempts(3), None, None);
        let calls = AtomicU32::new(0);
        let r = drive(&core, &p, async |a| {
            calls.fetch_add(1, Ordering::SeqCst);
            if a.number() < 3 { Err("transient") } else { Ok(42u32) }
        })
        .await;
        assert_eq!(r.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_and_reports_last_error() {
        let core = TestCore::new(ManualClock::new());
        let p = plan(Retry::fixed(Duration::ZERO).max_attempts(2), None, None);
        let r: Result<u32, _> = drive(&core, &p, async |_a| Err::<u32, _>("always")).await;
        let e = r.unwrap_err();
        assert!(e.is_exhausted());
        assert_eq!(e.context().attempts, 2);
        assert_eq!(e.into_inner(), Some("always"));
    }
}
```

- [ ] **Step 5: Run engine tests**

Run: `cargo test --features test-util --lib engine`
Expected: PASS (3 tests). Backoff is `Duration::ZERO` so no clock advance is needed; for non-zero delays, tests advance `clock` (covered in Task 10 integration tests).

- [ ] **Step 6: Commit**

```bash
git add src/engine.rs src/lib.rs
git commit -m "feat: engine drive loop with poll_fn select (cancellation seam)"
```

---

### Task 9: `ExecutionPolicy` + builder + the four methods

**Files:**
- Create: `src/builder.rs`
- Create: `src/policy.rs`
- Modify: `src/lib.rs`
- Test: unit tests inside `src/builder.rs`

**Interfaces:**
- Consumes: `Core`, `DefaultCore`, `Plan`, `Retry`, `engine::drive`, `Attempt`, `ExecutionError`.
- Produces:
  - `pub struct ExecutionPolicyBuilder<T, E> { .. }` with `retry(Retry<T,E>)`, `attempt_timeout(Duration)`, `total_timeout(Duration)`, `build() -> ExecutionPolicy<DefaultCore, T, E>`, `build_with<C: Core>(C) -> ExecutionPolicy<C, T, E>`, `try_build() -> Result<ExecutionPolicy<DefaultCore,T,E>, BuildError>`.
  - `pub struct BuildError(String);` (Display).
  - `pub struct ExecutionPolicy<C, T, E> { core: C, plan: Arc<Plan<T,E>> }` with `builder()`, and `run`/`run_with`/`execute`/`execute_with`.

Note: the `T, E` type parameters are carried by the policy because the classifier is typed. This is invisible at most call sites via inference.

- [ ] **Step 1: Write `src/builder.rs`** with validation tests

```rust
use std::sync::Arc;
use std::time::Duration;

use crate::core::Core;
#[cfg(feature = "tokio")]
use crate::core::DefaultCore;
use crate::plan::Plan;
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
}

impl<T, E> std::fmt::Debug for ExecutionPolicyBuilder<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPolicyBuilder")
            .field("retry", &self.retry)
            .field("attempt_timeout", &self.attempt_timeout)
            .field("total_timeout", &self.total_timeout)
            .finish()
    }
}

impl<T, E> Default for ExecutionPolicyBuilder<T, E> {
    fn default() -> Self {
        Self { retry: None, attempt_timeout: None, total_timeout: None }
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
        Plan {
            retry: self.retry.unwrap_or_else(Retry::none),
            attempt_timeout: self.attempt_timeout,
            total_timeout: self.total_timeout,
        }
    }

    /// Validate and build with the default core. Panics on invalid config.
    #[cfg(feature = "tokio")]
    pub fn build(self) -> ExecutionPolicy<DefaultCore, T, E> {
        self.build_with(DefaultCore::new())
    }

    /// Validate and build, returning an error instead of panicking.
    #[cfg(feature = "tokio")]
    pub fn try_build(self) -> Result<ExecutionPolicy<DefaultCore, T, E>, BuildError> {
        self.validate()?;
        Ok(ExecutionPolicy::from_parts(DefaultCore::new(), Arc::new(self.compile())))
    }

    /// Validate and build with a custom [`Core`]. Panics on invalid config.
    pub fn build_with<C: Core>(self, core: C) -> ExecutionPolicy<C, T, E> {
        if let Err(e) = self.validate() {
            panic!("{e}");
        }
        ExecutionPolicy::from_parts(core, Arc::new(self.compile()))
    }
}

#[cfg(test)]
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
```

- [ ] **Step 2: Write `src/policy.rs`** with the four methods

```rust
use std::sync::Arc;

use crate::attempt::Attempt;
use crate::builder::ExecutionPolicyBuilder;
use crate::core::Core;
#[cfg(feature = "tokio")]
use crate::core::DefaultCore;
use crate::engine::drive;
use crate::error::ExecutionError;
use crate::plan::Plan;

/// A reusable, cheaply-cloneable reliability policy.
pub struct ExecutionPolicy<C, T, E> {
    core: C,
    plan: Arc<Plan<T, E>>,
}

impl<C: std::fmt::Debug, T, E> std::fmt::Debug for ExecutionPolicy<C, T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPolicy").field("core", &self.core).field("plan", &self.plan).finish()
    }
}

impl<C: Clone, T, E> Clone for ExecutionPolicy<C, T, E> {
    fn clone(&self) -> Self {
        Self { core: self.core.clone(), plan: Arc::clone(&self.plan) }
    }
}

#[cfg(feature = "tokio")]
impl ExecutionPolicy<DefaultCore, (), ()> {
    /// Start configuring a policy. The `T`/`E` types are inferred at the first
    /// `retry(..)`/execute call site.
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
    /// Run an operation that needs neither application state nor attempt metadata.
    pub async fn run<F>(&self, mut op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut() -> Result<T, E>,
    {
        drive(&self.core, &self.plan, async move |_attempt: Attempt<'_>| op().await).await
    }

    /// Run an operation that wants attempt metadata.
    pub async fn execute<F>(&self, op: F) -> Result<T, ExecutionError<E>>
    where
        F: AsyncFnMut(Attempt<'_>) -> Result<T, E>,
    {
        drive(&self.core, &self.plan, op).await
    }

    /// Run an operation with injected application state.
    pub async fn run_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S) -> Result<T, E>,
    {
        drive(&self.core, &self.plan, async move |_attempt: Attempt<'_>| op(state).await).await
    }

    /// Run an operation with injected state and attempt metadata.
    pub async fn execute_with<S, F>(&self, state: &S, mut op: F) -> Result<T, ExecutionError<E>>
    where
        S: Sync + ?Sized,
        F: AsyncFnMut(&S, Attempt<'_>) -> Result<T, E>,
    {
        drive(&self.core, &self.plan, async move |attempt: Attempt<'_>| op(state, attempt).await).await
    }
}
```

- [ ] **Step 3: Wire `src/lib.rs`**

```rust
pub mod builder;
pub mod policy;

pub use crate::builder::{BuildError, ExecutionPolicyBuilder};
pub use crate::policy::ExecutionPolicy;
```

- [ ] **Step 4: Run builder + full lib tests**

Run: `cargo test --features test-util`
Expected: PASS (all unit tests including the two builder validation tests).

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs src/policy.rs src/lib.rs
git commit -m "feat: ExecutionPolicy + builder (validate/try_build) + four-method API"
```

---

### Task 10: Integration tests — pipeline behavior + ergonomics

**Files:**
- Create: `tests/retry_pipeline.rs`
- Create: `tests/ergonomics.rs`

**Interfaces:**
- Consumes the full public API + `TestCore`/`ManualClock` (requires `test-util`).

- [ ] **Step 1: Write `tests/retry_pipeline.rs`**

```rust
//! Integration: retry, backoff timing, and total-timeout behavior on a virtual clock.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionPolicyBuilder, Retry};

#[tokio::test]
async fn backoff_waits_on_the_virtual_clock() {
    let clock = ManualClock::new();
    let core = TestCore::new(clock.clone());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(
            Retry::exponential()
                .max_attempts(3)
                .base_delay(Duration::from_millis(100))
                .max_delay(Duration::from_secs(1)),
        )
        .build_with(core);

    let calls = AtomicU32::new(0);
    let driver = async {
        policy
            .execute(async |a| {
                calls.fetch_add(1, Ordering::SeqCst);
                if a.number() < 3 { Err("transient") } else { Ok(99u32) }
            })
            .await
    };

    tokio::pin!(driver);

    // Attempt 1 runs immediately and fails; engine schedules a 100ms backoff.
    // Poll once to reach the first sleep, then advance the clock to release each backoff.
    let advancer = async {
        // Give the driver a chance to make the first attempt and arm the sleep.
        tokio::task::yield_now().await;
        clock.advance(Duration::from_millis(100)); // release backoff after attempt 1
        tokio::task::yield_now().await;
        clock.advance(Duration::from_millis(200)); // release backoff after attempt 2
    };

    let (res, _) = tokio::join!(driver, advancer);
    assert_eq!(res.unwrap(), 99);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn total_timeout_fires() {
    let clock = ManualClock::new();
    let core = TestCore::new(clock.clone());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::fixed(Duration::from_millis(50)).max_attempts(100))
        .total_timeout(Duration::from_millis(120))
        .build_with(core);

    let driver = async {
        policy.execute(async |_a| Err::<u32, _>("always")).await
    };
    tokio::pin!(driver);
    let advancer = async {
        for _ in 0..10 {
            tokio::task::yield_now().await;
            clock.advance(Duration::from_millis(50));
        }
    };
    let (res, _) = tokio::join!(driver, advancer);
    let err = res.unwrap_err();
    assert!(err.is_timeout(), "expected total timeout, got {err}");
}

#[tokio::test]
async fn when_predicate_stops_on_permanent() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, i32>::new()
        .retry(Retry::fixed(Duration::ZERO).max_attempts(5).when(|e: &i32| *e >= 500))
        .build_with(core);

    let calls = AtomicU32::new(0);
    let res = policy
        .execute(async |_a| {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<u32, _>(404) // permanent — must not retry
        })
        .await;
    assert!(res.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
```

- [ ] **Step 2: Run pipeline tests**

Run: `cargo test --features test-util --test retry_pipeline`
Expected: PASS (3 tests). If the timing tests are racy, the `yield_now`/`advance` interleave may need an extra `yield_now()` — adjust until deterministic (no real sleeps involved).

- [ ] **Step 3: Write `tests/ergonomics.rs`** — proves all four methods + state injection + `!Send` op compile and run

```rust
use std::rc::Rc;
use std::time::Duration;

use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionPolicyBuilder, Retry};

struct Deps {
    base: u32,
}

#[tokio::test]
async fn all_four_methods() {
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new()
        .retry(Retry::none())
        .build_with(core);

    let a = policy.run(async || Ok::<_, &str>(1u32)).await.unwrap();

    let deps = Deps { base: 10 };
    let b = policy.run_with(&deps, async |d: &Deps| Ok::<_, &str>(d.base)).await.unwrap();

    let c = policy.execute(async |at| Ok::<_, &str>(at.number())).await.unwrap();

    let d = policy
        .execute_with(&deps, async |dep: &Deps, at| Ok::<_, &str>(dep.base + at.number()))
        .await
        .unwrap();

    assert_eq!((a, b, c, d), (1, 10, 1, 11));
}

#[tokio::test]
async fn accepts_non_send_operation() {
    // Rc is !Send — proves the engine drives futures in place without a Send bound.
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, &str>::new().retry(Retry::none()).build_with(core);
    let shared = Rc::new(41u32);
    let out = policy
        .run(async || {
            let v = Rc::clone(&shared);
            Ok::<_, &str>(*v + 1)
        })
        .await
        .unwrap();
    assert_eq!(out, 42);
}

#[tokio::test]
async fn question_mark_into_caller_error() -> Result<(), Box<dyn std::error::Error>> {
    // ExecutionError<E>: Error, so `?` flows into a boxed error.
    let core = TestCore::new(ManualClock::new());
    let policy = ExecutionPolicyBuilder::<u32, std::io::Error>::new()
        .retry(Retry::none())
        .build_with(core);
    let _v: u32 = policy
        .run(async || Ok::<u32, std::io::Error>(5))
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    let _ = Duration::from_secs(1);
    Ok(())
}
```

- [ ] **Step 4: Run ergonomics tests**

Run: `cargo test --features test-util --test ergonomics`
Expected: PASS (3 tests).

- [ ] **Step 5: Full suite + clippy + no-default-features**

Run: `cargo test --features test-util`
Run: `cargo clippy --all-targets --features test-util -- -D warnings`
Run: `cargo build --no-default-features --features test-util`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add tests/retry_pipeline.rs tests/ergonomics.rs
git commit -m "test: integration coverage for retry/timeout pipeline + four-method ergonomics"
```

---

### Task 11: README hero example + crate docs + push

**Files:**
- Create: `README.md`
- Modify: `src/lib.rs` (crate-level doc example)

**Interfaces:** none new — documents the existing API.

- [ ] **Step 1: Write `README.md`** with the simple + state-injection examples

````markdown
# execution-policy

Closure-first, runtime-light reliability policies for any async Rust operation:
retry · backoff · jitter · attempt/total timeouts (circuit breaking & bounded
concurrency land in the next release).

```rust
use std::time::Duration;
use execution_policy::{ExecutionPolicyBuilder, Retry, Jitter};

let policy = ExecutionPolicyBuilder::<_, reqwest::Error>::new()
    .retry(
        Retry::exponential()
            .max_attempts(4)
            .base_delay(Duration::from_millis(100))
            .max_delay(Duration::from_secs(2))
            .jitter(Jitter::Full)
            .when(|e: &reqwest::Error| e.is_timeout() || e.is_connect()),
    )
    .attempt_timeout(Duration::from_secs(2))
    .total_timeout(Duration::from_secs(8))
    .build();

let body = policy
    .execute_with(&client, async |client, attempt| {
        client.get("https://example.com")
            .header("x-attempt", attempt.number().to_string())
            .send().await?.error_for_status()
    })
    .await?;
```

The operation is a **factory**: it is re-invoked per attempt, so requests are
freshly built and never need `Clone`. `!Send` operations are accepted.
````

- [ ] **Step 2: Add a crate-level doc example** to the top of `src/lib.rs` (compiles under `cargo test --doc`)

```rust
//! ```
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
//! ```
```

- [ ] **Step 3: Run doc tests + full verification**

Run: `cargo test --doc`
Run: `cargo test --features test-util`
Expected: all green.

- [ ] **Step 4: Commit + push**

```bash
git add README.md src/lib.rs
git commit -m "docs: README hero example + crate doc test"
git push -u origin main
```

---

## Self-Review (against the spec, Plan 1 scope)

- **§3 structure:** Tasks 1–9 create every Plan-1 file; concurrency/breaker/events/budget modules are intentionally deferred to Plans 2–3. ✓
- **§4 API:** four methods (Task 9), no `Send`/`'static` bound (ergonomics test, Task 10), `build`/`build_with`/`try_build` (Task 9). ✓
- **§5 composition order:** Plan 1 covers `total_timeout`→`retry`→`attempt_timeout`→operation; concurrency/breaker layers (the outer two) arrive in Plan 2 and slot around the `drive` loop. ✓ (noted)
- **§6 classification:** `when`/`when_outcome` over `&Result` (Tasks 4, 6); `record_when` is breaker-only → Plan 2. ✓
- **§7 Core/seam:** object-safe Core (Task 1), `poll_fn` select seam (Task 8). `Core::acquire` deferred to Plan 2 (concurrency). ✓
- **§8 errors:** boxed context, `Error`+`source`, predicates, `into_inner` (Task 3). ✓
- **§10 perf:** stack-pinned op future, no per-attempt heap alloc (Task 8). Benches → Plan 3. ✓ (noted)
- **§11 testing:** TestCore/no-real-sleep unit + integration; `!Send` + `?` ergonomics tests; `--no-default-features` build check each task. `trybuild`/loom → later. ✓
- **§12 build order:** matches steps 1–3 of the spec's order; budget + failure-ratio breaker explicitly Plan 2. ✓

**Type-consistency check:** `Plan<T,E>`, `Retry<T,E>`, `Classifier<T,E>`, `ExecutionPolicy<C,T,E>`, `drive<C,F,T,E>` thread the same `T,E` throughout; `Attempt::number()` is `u32` everywhere; `ErrorContext` fields are referenced consistently. ✓

**Deferred to later plans (explicit, not gaps):** `CircuitBreaker`, `ConcurrencyLimit`/`SaturationPolicy`, `RetryBudget`, `failure_ratio` breaker, `Event`/`on_event`, `tracing` bridge, criterion benches, `loom`/`trybuild`, CI matrix, `record_when`.
