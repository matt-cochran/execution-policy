# execution-policy — Plan 3: Observability & Production Polish

> **For agentic workers:** Builds on Plans 1–2. Steps use checkbox (`- [ ]`) syntax. This plan was executed; it documents the as-built work.

**Goal:** Make the crate observable and production-ready — a dependency-free event hook, an optional `tracing` bridge, performance benches that guard the perf claims, a CI matrix, and complete docs.

**Architecture:** Events are emitted through an `Option<EventHook>` guard so values are constructed **only** when a hook is registered (zero cost when absent). The breaker reports state transitions up to the engine for `CircuitStateChanged`. Timers are armed lazily so the success path is allocation-free even with timeouts configured.

**Tech Stack:** `criterion` (dev), optional `tracing`.

## Global Constraints

Inherits Plans 1–2. Additionally:
- **Events zero-cost when absent:** no `Event` construction, no allocation, without a hook.
- **Hooks fail fast:** synchronous, not `catch_unwind`-wrapped.
- **`tracing` only behind its feature;** core never requires it.
- **Benches guard perf:** success-path overhead + `size_of` of `ExecutionError`/`Result`.

---

## File Structure (Plan 3 additions)

```
src/event.rs                  # Event enum + EventHook + emit() guard
src/plan.rs                   # + on_event: Option<EventHook>
src/builder.rs                # + on_event(), with_tracing() (feature = tracing)
src/engine.rs                 # emit() calls at lifecycle points; lazy timer arming
src/breaker.rs                # record_* return Option<BreakerState> (transitions)
tests/events.rs               # hook integration
benches/overhead.rs           # criterion happy-path + size guards
.github/workflows/ci.yml      # feature matrix, fmt+clippy, MSRV, doctests
README.md, LICENSE            # docs + BSD-3-Clause
```

---

### Task 1: Event system

**Interfaces produced:**
- `#[non_exhaustive] Event` — `AttemptFailed`, `AttemptTimedOut`, `RetryScheduled { attempt, delay }`, `Succeeded { attempts }`, `GaveUp { attempts }`, `CircuitStateChanged { to }`, `ConcurrencyRejected`, `RetryBudgetExhausted { attempts }`.
- `EventHook = Arc<dyn Fn(&Event) + Send + Sync>`.
- `emit(&Option<EventHook>, impl FnOnce() -> Event)` — the zero-cost-when-absent guard.

**TDD:** test that `emit` does **not** call its closure when the hook is `None`, and does when `Some`. → implement → green (2 tests).

- [ ] Write failing emit tests → implement → green.

---

### Task 2: Wire events through the engine

- `Plan` + `ExecutionPolicyBuilder` gain `on_event: Option<EventHook>`; builder `.on_event(hook)` and `.with_tracing()` (feature-gated).
- `breaker::record_success/record_failure` return `Option<BreakerState>`; engine emits `CircuitStateChanged` on transitions and on half-open admission.
- Engine emits at: attempt-failed, attempt-timed-out, retry-scheduled (before backoff), succeeded, gave-up, concurrency-rejected, budget-exhausted.

**TDD:** `tests/events.rs` — assert the failed/succeeded/retry-scheduled sequence on a retrying call, and gave-up on exhaustion. → green (3 tests).

- [ ] Wire emit points → events integration tests → green → commit.

---

### Task 3: Performance — lazy timer arming + benches

- **Lazy arming:** in `drive`, create `attempt_timeout`/`total_timeout` sleep futures only after the operation first returns `Pending`. A fast success allocates no timer futures.
- **Benches** (`benches/overhead.rs`, criterion): `run/success/no-policy`, `execute/success/full-policy`; plus a `structural_sizes` guard asserting `size_of::<ExecutionError<io::Error>>() <= 32` and `Result <= 32` (measured: 24/24).

**Verification:** `cargo bench --features test-util` — full-policy success path dropped from ~240 ns to ~84 ns after lazy arming.

- [ ] Implement lazy arming → re-run tests green → add benches → run → commit.

---

### Task 4: CI, license, docs

- `.github/workflows/ci.yml`: matrix over `--no-default-features`, `--no-default-features --features test-util`, `--features test-util`, `--all-features`; `fmt --check` + `clippy -D warnings` (all-features and no-default); MSRV 1.85 build; `cargo test --doc`.
- `LICENSE` (BSD-3-Clause); `README.md` full feature tour + crate-level doctest.

- [ ] Add CI + LICENSE + README → `cargo fmt` → full verification → commit + push.

---

## Self-Review (Plan 3 vs spec)

- §9 observability: dependency-free hook, zero-cost when absent, `tracing` behind feature. ✓
- §10 perf: lazy timer arming makes the success path allocation-free; benches guard `Result` size + overhead; CI runs them buildable. ✓
- §11 testing: CI feature matrix incl. `--no-default-features`; doctests. ✓ (`loom`/`trybuild` noted as future hardening.)
- §14 deltas: all 19 reinforcements implemented across Plans 1–3. ✓

## Known future work (explicitly out of scope, not gaps)

- `loom` model-checking of the breaker state machine and `trybuild` compile-fail cases (the `!Send` + `?` happy cases are covered by `tests/ergonomics.rs`).
- Cooperative `CancellationToken` (engine seam is in place).
- Sibling adapter crates `execution-policy-tower` / `execution-policy-http`.
