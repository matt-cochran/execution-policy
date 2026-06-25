# execution-policy — Plan 2: Circuit Breaking, Bounded Concurrency, Retry Budgets

> **For agentic workers:** Builds on Plan 1. Steps use checkbox (`- [ ]`) syntax. This plan was executed; it documents the as-built design and the TDD task order followed.

**Goal:** Add the three remaining resilience layers — circuit breaker, bounded concurrency, and shared retry budget — composed around Plan 1's retry loop in the canonical order.

**Architecture:** A runtime-agnostic async semaphore (Mutex + waker queue) provides bounded concurrency with **no `Core` change**. The breaker uses an atomic state for a lock-free closed-path gate check and a mutex only on the once-per-call record path. The retry budget is a shared lock-free token bucket (CAS). The engine is refactored into `run_pipeline` (concurrency → breaker gate → retry → breaker record) wrapping the inner `drive` loop.

**Tech Stack:** Same as Plan 1. No new runtime dependencies (semaphore and budget are hand-rolled with `std::sync` atomics/mutex).

## Global Constraints

Inherits all of Plan 1's. Additionally:
- **Lock-free breaker closed path:** the gate check when closed is a single atomic load; the bookkeeping mutex is taken only on `record_*` (once per call) and state transitions.
- **Concurrency is runtime-agnostic:** the semaphore must not depend on tokio (keeps `--no-default-features` building).
- **Composition order asserted by tests**, not just documented.
- **Concurrency-correctness mandate:** breaker half-open races and budget contention get dedicated tests.

---

## File Structure (Plan 2 additions)

```
src/breaker.rs        # CircuitBreaker<E> config + BreakerRuntime state machine + Window
src/concurrency.rs    # ConcurrencyLimit + SaturationPolicy + Semaphore/Permit/Acquire + CompiledConcurrency
src/retry/budget.rs   # RetryBudget (shared token bucket)
src/plan.rs           # extended: CompiledBreaker<E>, CompiledConcurrency fields
src/engine.rs         # refactored: run_pipeline + acquire_permit + drive(start, deadline, ...)
src/builder.rs        # extended: .circuit_breaker(), .concurrency_limit()
src/policy.rs         # circuit_state() introspection; methods call run_pipeline
tests/resilience.rs   # integration: open/reject, half-open→close, load-shed, budget cap
```

---

### Task 1: Circuit breaker state machine

**Files:** Create `src/breaker.rs`; wire into `lib.rs`.

**Interfaces produced:**
- `CircuitBreaker<E>` — `consecutive_failures(n)` / `failure_ratio()` presets; builders `ratio`, `minimum_throughput`, `sampling_window`, `open_for`, `half_open_max_calls`, `record_when`; `compile() -> (BreakerRuntime, Option<ErrorPredicate<E>>)`.
- `BreakerRuntime` — `state()`, `gate(now) -> Result<BreakerState,()>`, `record_success(now) -> Option<BreakerState>`, `record_failure(now) -> Option<BreakerState>`. Atomic `state` for the fast closed-path check; `Mutex<Inner>` for transitions + sliding `Window`.

**TDD steps:** failing tests for consecutive trip, success-resets-consecutive, half-open probe→close, half-open failure→reopen, failure-ratio trip, min-throughput gate, record_when predicate → implement state machine → green (7 tests).

- [ ] Write failing breaker tests → implement → green → commit.

---

### Task 2: Bounded concurrency (runtime-agnostic semaphore)

**Files:** Create `src/concurrency.rs`; wire into `lib.rs`.

**Interfaces produced:**
- `SaturationPolicy::{Wait { max_queued, queue_timeout }, Reject}`.
- `ConcurrencyLimit` — `operations(n)` / `attempts(n)`; builders `max_queued`, `queue_timeout`, `reject`; `From<usize>`/`From<u32>` → `operations(n)`; `compile() -> CompiledConcurrency`.
- Internal `Semaphore` (`try_acquire`, `acquire`, `queued`) + `Permit` (releases on drop, wakes next waiter) + `Acquire` future.

**TDD steps:** failing tests for permit-release-wakes-next, queued count, saturation builders → implement Mutex+waker-queue semaphore → green (3 tests).

- [ ] Write failing concurrency tests → implement → green → commit.

---

### Task 3: Shared retry budget

**Files:** Create `src/retry/budget.rs`; wire into `retry/mod.rs` + `lib.rs`; add `Retry::budget()`.

**Interfaces produced:**
- `RetryBudget` (`Arc`-cloneable) — `new(ratio, burst)`, `standard()`; `deposit()` per call, `try_withdraw() -> bool` per retry. Lock-free CAS token bucket, scale = 1000.

**TDD steps:** failing tests for burst-then-deny, deposits-replenish-at-ratio, shared-clones-share-bucket, deposit-caps-at-max → implement → green (4 tests).

- [ ] Write failing budget tests → implement → green → commit.

---

### Task 4: Integrate into the pipeline engine

**Files:** Modify `src/plan.rs`, `src/engine.rs`, `src/builder.rs`, `src/policy.rs`.

**Interfaces:**
- `Plan` gains `breaker: Option<CompiledBreaker<E>>`, `concurrency: Option<CompiledConcurrency>`.
- `engine::run_pipeline` = concurrency acquire (operations scope, raced against deadline + saturation) → breaker `gate` → `drive(core, plan, start, total_deadline, breaker_state, op)` → breaker `record_*` on final outcome.
- `drive` gains: per-attempt concurrency permit (attempts scope), `RetryBudget` deposit/withdraw, `breaker_state` threaded into `ErrorContext`.
- Builder: `.circuit_breaker(CircuitBreaker<E>)`, `.concurrency_limit(impl Into<ConcurrencyLimit>)`.
- `ExecutionPolicy::circuit_state() -> Option<BreakerState>`.

**Composition order (asserted by tests):**
`total_timeout( concurrency( circuit_breaker( retry( attempt_timeout( op ) ) ) ) )`.

Breaker recording rules: `Ok` → success; timeout error → failure; `Operation{source}` → `record_when(source)` (default counts); other ExecutionError variants → no vote.

- [ ] Refactor engine + plan + builder + policy → `tests/resilience.rs` (open/reject, half-open→close, reject load-shed, budget cap) → green → clippy + feature matrix → commit.

---

## Self-Review (Plan 2 vs spec)

- §5 composition order: enforced in `run_pipeline`; concurrency once-per-call (operations), breaker outside retry, recorded once. ✓
- §6 `record_when`: breaker-specific classifier wired. ✓
- §8 `ExecutionError`: `CircuitOpen`/`ConcurrencyRejected`/`RetryBudgetExhausted` populated with `breaker_state` context. ✓
- §10 perf: lock-free breaker closed path; runtime-agnostic semaphore; budget lock-free. ✓
- §12 build order: budget + failure_ratio breaker built within this plan, behind their tests. ✓

**Deferred to Plan 3:** events/`on_event`, `tracing` bridge, benches, CI matrix, README.
