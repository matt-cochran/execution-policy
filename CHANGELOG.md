# Changelog

## [0.0.3] — 2026-06-25

### Added
- `retry_after` hint extractor on `Retry` — honor a server-supplied delay (e.g.
  HTTP `Retry-After`, gRPC `RetryInfo`) as a **floor** on the next backoff delay.
- `fallback` chain on `ExecutionPolicyBuilder` — additive async recovery links
  invoked after all retries, timeouts, the circuit breaker, and concurrency
  limits give up. Each `.fallback(...)` call appends a link; links run in
  registration order (first `Ok` wins). All-decline → original `ExecutionError`
  propagates. Each link receives the original error for failure-class
  discrimination. Emits `Event::FallbackInvoked` per link that fires.

### Fixed
- `cargo test --no-default-features` now passes: integration test files gated
  behind `#![cfg(feature = "test-util")]` so they are skipped without the feature.

## [0.0.2] — initial public release

Retry · exponential backoff · jitter · attempt/total timeouts · circuit
breaking · bounded concurrency · retry budgets · event hooks · tracing bridge.
