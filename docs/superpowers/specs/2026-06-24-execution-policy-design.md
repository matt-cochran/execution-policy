# execution-policy — Design Spec

**Date:** 2026-06-24
**Status:** Approved (brainstorming) → ready for implementation plan
**Repo:** `https://github.com/matt-cochran/execution-policy.git`

## 1. Summary

`execution-policy` is a closure-first, runtime-light reliability crate for arbitrary
Rust `async` operations. It provides Polly-like usability — retry, backoff, jitter,
timeouts, circuit breaking, bounded concurrency, retry budgets — with explicit Rust
ownership, cancellation semantics, deadlines, and deterministic testing.

It is **not** a Polly port and **not** a Tower-coupled middleware stack. Tower users are
served by `tower-resilience`; this crate targets *any* async function, job, DB call, or
non-Tower client through an operation-factory model.

**Two non-negotiable goals:** (1) a maximally ergonomic, fluent API; (2) a
high-performing implementation (no per-attempt heap allocation on the happy path).

## 2. Goals / Non-Goals

### Goals
- Wrap any `async` operation with composable reliability policies.
- Operation **factory** model: the closure is re-invoked per attempt, so requests/futures
  are freshly constructed and never require `Clone`.
- Ergonomic gradient: `run` / `run_with` / `execute` / `execute_with`.
- Separate **classification** from **policy** (retry vs. circuit-breaker decisions are
  independent; `Ok`-wrapped failures like HTTP 503 are classifiable).
- Deterministic testing via an injectable `Core` (clock + sleep + RNG); **no real sleeps**
  in unit tests.
- High performance: stack-pinned operation futures, zero per-attempt heap allocation on the
  happy path, criterion benches gating CI.

### Non-Goals (v1)
- Tower `Service`/`Layer` adapters (future sibling crate `execution-policy-tower`).
- HTTP-specific classification / `Retry-After` (future sibling `execution-policy-http`).
- Cooperative `CancellationToken` surface — **deferred**, but the engine seam is built now
  (see §7).
- Distributed/shared-across-process circuit-breaker or budget state.

## 3. Crate Structure (single crate, feature-gated)

```
execution-policy/
├── Cargo.toml          # features: default=["tokio"]; tokio, test-util, tracing
├── src/
│   ├── lib.rs          # re-exports, crate docs, hero example, MSRV note
│   ├── policy.rs       # ExecutionPolicy<C>, the 4 execute methods
│   ├── builder.rs      # ExecutionPolicyBuilder, build()/build_with()
│   ├── plan.rs         # compiled immutable Plan (Arc'd); composition order
│   ├── engine.rs       # select!-based execution loop (cancellation seam)
│   ├── attempt.rs      # Attempt<'_> (#[non_exhaustive]): number/elapsed/deadline
│   ├── error.rs        # ExecutionError<E> (Box<ErrorContext>); Error+source; predicates
│   ├── classify.rs     # FailureClass, RetryDecision; classifiers over &Result<T,E>
│   ├── retry/
│   │   ├── mod.rs      # Retry builder + presets (none/fixed/exponential/standard)
│   │   ├── backoff.rs  # Backoff (fixed, exponential w/ base+max)
│   │   ├── jitter.rs   # Jitter::{None, Full, Equal}
│   │   └── budget.rs   # RetryBudget (shared, atomic token bucket)
│   ├── breaker.rs      # CircuitBreaker: lock-free closed path; ring window; half-open
│   ├── concurrency.rs  # ConcurrencyLimit *config* + SaturationPolicy; gate via Core::acquire
│   ├── core/
│   │   ├── mod.rs      # trait Core (object-safe: clock + boxed sleep + rng + acquire)
│   │   ├── tokio.rs    # TokioCore                 (feature = "tokio")
│   │   └── test.rs     # TestCore, ManualClock, seeded RNG (feature = "test-util")
│   └── event.rs        # Event enum + on_event hook (dependency-free; tracing optional)
├── tests/              # integration: composition order, full pipeline (TestCore)
├── benches/            # criterion: happy-path overhead, retry path
└── docs/superpowers/specs/2026-06-24-execution-policy-design.md
```

Dependency inversion: policy primitives (retry math, breaker state) know nothing about
tokio. The engine and `TokioCore` depend on the `Core` trait. All core types compile with
`--no-default-features`.

**MSRV:** Rust 1.85 (async closures / `AsyncFnMut` stabilized Feb 2025). Edition 2024.

## 4. Public API

### 4.1 Construction

```rust
let policy = ExecutionPolicy::builder()
    .retry(
        Retry::exponential()
            .max_attempts(4)
            .base_delay(Duration::from_millis(50))
            .max_delay(Duration::from_secs(1))
            .jitter(Jitter::Full)
            .when(is_retryable),                 // error-only classification
    )
    .attempt_timeout(Duration::from_secs(2))
    .total_timeout(Duration::from_secs(6))
    .circuit_breaker(
        CircuitBreaker::failure_ratio()
            .failure_ratio(0.5)
            .minimum_throughput(20)
            .sampling_window(Duration::from_secs(30))
            .open_for(Duration::from_secs(15))
            .half_open_max_calls(2)
            .record_when(counts_as_breaker_failure),
    )
    .concurrency_limit(
        ConcurrencyLimit::attempts(32)
            .max_queued(64)
            .queue_timeout(Duration::from_millis(250)),
    )
    .on_event(|event| tracing::debug!(?event))
    .build();                                    // DefaultCore; .build_with(core) to override
```

`build()` uses `DefaultCore` (= `TokioCore` under the default `tokio` feature).
`build_with(core)` is the advanced/testing escape hatch (`TestCore`, custom embedding).

**Builder validation (fail-fast).** `build()` validates the assembled config and **panics
with a rich message naming the offending field and value** on nonsensical input:
`attempt_timeout > total_timeout`, `failure_ratio ∉ (0.0, 1.0]`, `max_attempts == 0`,
`max_queued == 0`, `minimum_throughput == 0`. Config is known at startup, so a panic is the
idiomatic builder behavior; `try_build() -> Result<ExecutionPolicy, BuildError>` is provided
for callers that must handle it without unwinding. Re-setting a stage (e.g. `.retry(..)`
twice) is **last-wins**, with a `debug_assert!` flagging the re-set.

### 4.2 Execution — four-method ergonomic gradient

```rust
policy.run(async || do_work().await).await?;                       // no state, no attempt
policy.run_with(&deps, async |deps| deps.worker.go().await).await?;// state, no attempt
policy.execute(async |attempt| work(attempt.number()).await).await?;          // attempt
policy.execute_with(&deps, async |deps, attempt| {                 // state + attempt
    deps.client.get(deps.url.clone())
        .header("x-attempt", attempt.number().to_string())
        .send().await?.error_for_status()
}).await?;
```

Signatures (conceptual):

```rust
pub struct ExecutionPolicy<C = DefaultCore> { core: C, plan: Arc<Plan> }

impl ExecutionPolicy { pub fn builder() -> ExecutionPolicyBuilder { .. } }

impl<C: Core> ExecutionPolicy<C> {
    pub async fn run<F, T, E>(&self, op: F) -> Result<T, ExecutionError<E>>
        where F: AsyncFnMut() -> Result<T, E>;
    pub async fn execute<F, T, E>(&self, op: F) -> Result<T, ExecutionError<E>>
        where F: AsyncFnMut(Attempt<'_>) -> Result<T, E>;
    pub async fn run_with<S: Sync + ?Sized, F, T, E>(&self, state: &S, op: F) -> Result<T, ExecutionError<E>>
        where F: AsyncFnMut(&S) -> Result<T, E>;
    pub async fn execute_with<S: Sync + ?Sized, F, T, E>(&self, state: &S, op: F) -> Result<T, ExecutionError<E>>
        where F: AsyncFnMut(&S, Attempt<'_>) -> Result<T, E>;
}
```

**No `Send`/`'static` bound on the operation.** The engine drives operation futures in
place (it never `spawn`s them), so `!Send` operations — those capturing `Rc`, `RefCell`,
etc. — are accepted. A `!Send`-capturing closure is proven to compile via a `trybuild` case.

`Attempt::number()` is **1-based** (first attempt returns `1`), documented on the accessor
and asserted by a test, to avoid off-by-one in headers/logs.

### 4.3 Naming discipline (locked)
- `ExecutionPolicy`, `Retry`, `Backoff`, `Jitter`, `CircuitBreaker`, `ConcurrencyLimit`,
  `Attempt`, `ExecutionError`.
- `attempt_timeout` / `total_timeout` — never bare `timeout`.
- `max_attempts` — never `max_retries` (avoids off-by-one).
- `concurrency_limit` — never `bulkhead`.
- `ConcurrencyLimit::attempts(n)` vs `::operations(n)` — scope is explicit.

### 4.4 Presets
`Retry::{none, fixed(Duration), exponential, standard}` ·
`CircuitBreaker::{consecutive_failures(n), failure_ratio()}` ·
`ConcurrencyLimit::{attempts(n), operations(n)}` ·
`Jitter::{None, Full, Equal}`.

`Retry::standard()` = exponential with a sensible **transient-only** default classifier
(see §6).

## 5. Composition Order (fixed & documented)

One canonical nesting, outermost → innermost:

```
total_timeout( concurrency_limit( circuit_breaker( retry( attempt_timeout( operation ) ) ) ) )
```

- **Concurrency gate acquired once per call** (outside retry) — a queued caller does not
  re-queue on every attempt.
- **Breaker outside retry** — by default it records one vote per pipeline outcome; the
  recording granularity is exposed so "record every attempt" is reachable.
- **`attempt_timeout` innermost** — bounds each individual try.
- **`total_timeout` outermost** — bounds everything including backoff sleeps and queue wait.

This ordering is **asserted by integration tests**, not merely documented.

## 6. Classification (separate from policy)

```rust
pub enum FailureClass { Success, Retryable, Permanent, Ignored }
pub enum RetryDecision { Retry, Stop }
```

There is **no separate `Outcome<T,E>` type** — `Result<T, E>` already *is* the outcome, and
introducing a twin would be a parallel abstraction requiring a conversion on every call.
Result-aware classifiers receive `&Result<T, E>` directly, which lets them inspect
`Ok`-wrapped failures (e.g. an HTTP 503 inside `Ok(Response)`).

Three ergonomic tiers; retry and breaker classification are **independent**:
- `.when(|&E| bool)` — simple, error-type only.
- `.record_when(|&E| bool)` — breaker-specific (e.g. 429 retryable but not a breaker fault;
  caller cancellation not a fault; timeout *is* a fault).
- `.when_outcome(|&Result<T,E>| RetryDecision)` — full power, inspects `Ok` values.

**Default classification:** a bare `Retry::exponential().max_attempts(n)` with no `.when()`
retries **all `Err`** (matches user intent of "retry"). The footgun (retrying permanent /
non-idempotent failures) is documented; `Retry::standard()` provides a transient-only
classifier for safe out-of-the-box use.

## 7. Engine, Cancellation Seam & Core

- The engine loop is **`select!`-based**: it races the operation future against the
  attempt timeout, the total deadline, and (reserved) a future cancellation signal. Building
  the loop this way now means cooperative `CancellationToken` is a one-branch, non-breaking
  addition later. `Attempt<'_>` is `#[non_exhaustive]` and exposes data via accessor methods
  so new metadata is additive.
- **Drop ≠ abort.** A timeout drops the operation future; remote/blocking work may continue.
  Documented prominently. `AttemptTimeout` and `TotalTimeout` are distinct error variants.
- `Core` trait = `{ now() -> Instant, sleep(Duration) -> Pin<Box<dyn Future<Output=()> + '_>>,
  next_u64() -> u64, acquire(permits) -> … }`. The engine never touches `tokio::time`,
  `rand`, or `tokio::sync` directly. `sleep` returns a **boxed** future deliberately: it keeps
  `Core` **object-safe**, so the `Arc<dyn Core>` escape hatch promised for type-parameter
  ergonomics is real, and the one box per backoff sleep is on the cold path (negligible).
  `TokioCore` (feature `tokio`) and `TestCore`/`ManualClock` (feature `test-util`) implement
  it. Time type: `std::time::Instant`.
- **Concurrency primitive lives behind `Core`/the runtime feature**, not in the
  runtime-agnostic core. The core crate holds only the `ConcurrencyLimit` *config value*; the
  async semaphore/gate is provided by `Core::acquire` (tokio impl behind the `tokio` feature).
  This keeps `--no-default-features` compiling (§11) and avoids hard-coupling concurrency to
  tokio.

## 8. Error Model (fail-fast, rich context)

```rust
#[non_exhaustive]
pub enum ExecutionError<E> {
    Operation(E),               // last error after retries exhausted
    AttemptTimeout,
    TotalTimeout,
    CircuitOpen,
    ConcurrencyRejected,
    RetryBudgetExhausted,
}
```

Every `ExecutionError` carries a **`Box<ErrorContext>`** (`{ attempts, elapsed, last_delay,
breaker_state }`), surfaced via `Display` and an accessor. Boxing keeps `ExecutionError<E>`
— and therefore the hot `Result<T, ExecutionError<E>>` — small, so the success path moves a
lean value; the context is only allocated on the cold error path. This satisfies the
fail-fast, rich-diagnostic-context requirement: a failure tells you how many attempts ran,
how long it took, the last backoff delay, and the breaker state at failure.

**Error trait & `?` story.** `ExecutionError<E>: std::error::Error where E: Error`, with the
operation error exposed as the `#[source]` so `?`-chaining and `anyhow`/`thiserror`
interop work end-to-end; `into_inner() -> Option<E>` recovers the operation error. A doctest
demonstrates a caller fn using `?` from `execute_with` straight into its own error type.

**Ergonomic predicates** (so `#[non_exhaustive]` never forces a `match`):
`is_timeout()`, `is_circuit_open()`, `is_rejected()`, `is_exhausted()`.

## 9. Observability

`event.rs` defines an `Event` enum (retry scheduled, attempt failed, circuit state changed,
concurrency rejected, attempt/total timeout, budget exhausted). `on_event` is a
**synchronous, cheap, dependency-free** hook — it is **not** `catch_unwind`-wrapped (a
panicking hook is the caller's bug and should surface, per fail-fast). A `tracing` feature
adds an opt-in bridge; the core crate never requires `tracing`.

**Zero-cost when absent.** The hook is stored as an `Option`; when no hook is registered the
engine **does not construct `Event` values at all** (no string formatting, no allocation),
so the happy path stays allocation-free per §10.

## 10. Performance Requirements

- Operation futures are **stack-pinned** (`std::pin::pin!`); the engine is generic over the
  future with **no `Box::pin` on the happy path**.
- **Zero per-attempt heap allocation** on the success path.
- `Plan` is compiled once and `Arc`-shared; execution clones only the `Arc`.
- **Minimal monomorphized surface.** The four generic methods are thin shims that drive the
  operation future; the runtime-invariant logic (outcome → classification → backoff schedule
  → breaker/budget update) lives in a **non-generic inner driver** operating over erased
  values, so `S/F/T/E/C` don't multiply the heavy code. No boxing on the hot path.
- **Circuit-breaker hot path is lock-free in the closed state**: an atomic state load plus a
  bucketed atomic counter; a lock (or CAS) is taken **only** on a state transition, never on
  the common closed-circuit call. The sliding window is a bucketed ring, not a mutex-guarded
  list — so high-concurrency throughput isn't serialized by the breaker.
- **criterion benches** measure happy-path overhead (asserting 0 allocs and bounded
  `size_of::<Result<…>>()`), the retry path, and breaker throughput under contention; a
  regression gate plus `cargo bloat` run in CI.

## 11. Testing Strategy (TDD throughout)

- **Unit:** backoff sequences; jitter bounds (seeded RNG); breaker state transitions;
  budget bucket math — all on `TestCore`/`ManualClock`, **no real sleeps**.
- **Concurrency correctness (mandated):** `CircuitBreaker` half-open races and `RetryBudget`
  contention get dedicated stress tests; the breaker state machine is verified with `loom`
  before it is considered done.
- **Integration:** composition-order assertions; full pipeline behavior on `TestCore`.
- **Ergonomics:** `trybuild` compile tests proving real-world closures (capturing refs and
  owned state, plus a `!Send` `Rc`-capturing closure) compile across all four methods; a
  doctest proving end-to-end `?` into a caller's own error type; doctests as living examples.
- **CI matrix:** `--no-default-features`, `--features tokio` (default), `--all-features`;
  criterion regression gate.

## 12. Build Order (within full-surface v1)

Each layer green before the next:

1. `Core` + `TestCore`/`ManualClock` + `error.rs` (`ExecutionError`, `ErrorContext`, `Outcome`).
2. `Retry` + `Backoff` + `Jitter` (no budget yet).
3. Timeouts + `engine` `select!` loop + `Attempt`.
4. `CircuitBreaker` (consecutive_failures first).
5. `ConcurrencyLimit` + `SaturationPolicy`.
6. `RetryBudget` (shared atomic) — **built late, behind stress tests**.
7. `failure_ratio` breaker (sliding window) — **built late, behind stress tests**.
8. Classification wiring (`when` / `record_when` / `when_outcome`).
9. `event.rs` + `on_event`; optional `tracing` bridge.
10. Four execute methods polish, README hero example, criterion benches.

## 13. Accepted Residual Risk

**Idempotency footgun** — retrying non-idempotent operations is domain-inherent and cannot
be removed by a general-purpose crate without becoming a framework. Mitigated by the explicit
factory model (each attempt visibly re-runs the closure) and prominent documentation.
Accepted as residual **Medium**.

## 14. Design Provenance

API surface converged in prior design discussion (operation-factory model, four-method
gradient, classification split, `Core` injection with `build()`/`build_with()`). The seven
reinforcements below came from an FMECA / poka-yoke / TRIZ validation pass and do **not**
change the public API:

1. `ExecutionError` carries `ErrorContext`.
2. Performance is first-class (stack-pin, zero happy-path alloc, criterion CI gate).
3. Concurrency correctness mandated for `CircuitBreaker` + `RetryBudget` (loom + stress).
4. `Retry::standard()` transient-only preset alongside retry-all default.
5. CI matrix includes `--no-default-features`.
6. `RetryBudget` + `failure_ratio` breaker built last, behind their stress tests.
7. Events layer dependency-free; `tracing` only behind its feature.

A **second FMECA pass over the written spec** (performance + ergonomics lens) added twelve
reinforcements — none expand the public API beyond *removing* one type and *adding* helpers:

8. Removed the `Outcome<T,E>` parallel abstraction; classify over `&Result<T,E>`.
9. Concurrency primitive moved behind `Core`/`tokio` feature (fixes `--no-default-features`).
10. Circuit-breaker closed-state hot path is lock-free (atomics + bucketed ring window).
11. `ExecutionError<E>: std::error::Error` with `#[source]` + `into_inner()` for clean `?`.
12. Events are zero-cost when no hook is registered (no `Event` construction).
13. `Box<ErrorContext>` keeps the hot `Result` small.
14. Non-generic inner driver behind the thin generic methods; no hot-path boxing.
15. No `Send`/`'static` bound on the operation (`!Send` ops accepted).
16. `Attempt::number()` is 1-based, documented + tested.
17. `build()` validates and panics with rich context; `try_build() -> Result` provided.
18. `Core` is object-safe (boxed `sleep`) → `Arc<dyn Core>` escape hatch is real.
19. Predicate helpers on `ExecutionError` (`is_timeout`/`is_circuit_open`/`is_rejected`/`is_exhausted`).
