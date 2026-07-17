# Changelog

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
