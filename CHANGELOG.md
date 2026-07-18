# Changelog

## [0.0.6] — 2026-07-18

### Added
- `RouterPolicy::run_owned` / `run_boxed` and `ExecutionPolicy::run_boxed` — a
  **`Send`-general** execution path whose op returns an OWNED future (a concrete
  `BoxFuture<'static, _>`) rather than an `AsyncFnMut` whose future borrows the
  closure. This is what lets a router compose inside a caller whose own future
  must be `Send` — e.g. a member router driven from an `#[async_trait]` method or
  a `tokio::spawn`ed task (issue #7). The ergonomic `AsyncFnMut` `run`/`execute`/
  `run_with`/`execute_with` are unchanged (zero-alloc retry with borrowed state);
  the boxed path costs one box per attempt.
- `Member::from_arc` — register a member over an existing `Arc<ExecutionPolicy>`.

### Changed
- `Attempt` is now lifetime-free (was a `PhantomData<&'a ()>` that reserved
  nothing). Removing it drops a `for<'a>` from every op-closure bound — one half
  of making the boxed path `Send`-general; the other half is the new
  `FnMut(Attempt) -> Fut` boxed engine (`run_pipeline_boxed`/`drive_boxed`), a
  drift-guarded twin of the `AsyncFnMut` pipeline (identical retry/timeout/breaker
  semantics; see `boxed_pipeline_matches_asyncfnmut_pipeline`).

## [0.0.5] — 2026-07-17

### Changed (breaking, pre-1.0 clean cutover)
- `FallbackPolicy` → `RouterPolicy<Id, C, T, E>` — a composable load-balancing
  router with a generic, opaque target id.
- `Selection` enum → `Pick<Id>` scoring seam: `by_score` / `by_sampled_score`
  plus `first_healthy` / `round_robin` / `least_in_flight` /
  `weighted_least_in_flight` / `p2c` / `peak_ewma`.
- `.target(id, policy)` → `.target(Member)`; `Member::new(id, policy).weight(w)`.
- `advance_when` is now **required** (no advance-on-every-error default).

### Added
- Load balancing over N members — one router, no per-algorithm branch (every
  named strategy composes over the `Pick` seam).
- `Member` with `Arc`-shared per-member state: one member joins many routers,
  sharing its breaker + in-flight + latency signal.
- `Meter` seam (`peak_ewma`, `custom`); in-flight `AtomicUsize` signal (never a cap).

### Fixed / hardened (FMECA vet)
- NaN/inf scores fail fast (`RouterError::Score`); weight `> 0` validated;
  latency-aware strategies hard-error without a meter; `peak_ewma` ignores
  failed-call latency; in-flight released before failover; empty / duplicate-id /
  zero-sample rejected at build.

## [0.0.4] — 2026-06

### Added
- `FailureClass` wiring, `stall_timeout`, and the multi-target `FallbackPolicy`
  (superseded by `RouterPolicy` in 0.0.5).

## [0.0.3] — 2026-06-25

### Added
- `retry_after` hint extractor on `Retry` — honor a server-supplied delay (e.g.
  HTTP `Retry-After`, gRPC `RetryInfo`) as a **floor** on the next backoff delay.

### Fixed
- `cargo test --no-default-features` now passes: integration test files gated
  behind `#![cfg(feature = "test-util")]` so they are skipped without the feature.

## [0.0.2] — initial public release

Retry · exponential backoff · jitter · attempt/total timeouts · circuit
breaking · bounded concurrency · retry budgets · event hooks · tracing bridge.
