# execution-policy

[![CI](https://github.com/matt-cochran/execution-policy/actions/workflows/ci.yml/badge.svg)](https://github.com/matt-cochran/execution-policy/actions/workflows/ci.yml)

Closure-first, runtime-light **reliability policies** for any async Rust operation:
**retry · backoff · jitter · attempt/total timeouts · circuit breaking · bounded
concurrency · retry budgets.**

A fluent, closure-first API with explicit Rust ownership, real deadlines, and
deterministic testing. It wraps *any* async function, job, DB call, or HTTP
client — you bring a closure, it brings the resilience. (Building a Tower
`Service` stack instead? Reach for `tower-resilience`; this crate is for
everything that isn't a Tower service.)

## Quick start

Start tiny and add policy as you need it — every example below is the same
builder, just with more layers.

**1. Retry a flaky call.** The closure is re-run on each attempt.

```rust
use execution_policy::{ExecutionPolicyBuilder, Retry};

let policy = ExecutionPolicyBuilder::<_, MyError>::new()
    .retry(Retry::exponential().max_attempts(3))
    .build();

let value = policy.run(async || fetch_widget().await).await?;
```

**2. Only retry transient errors, and add jitter.** Classification keeps you from
retrying a `404` forever.

```rust
use std::time::Duration;
use execution_policy::{ExecutionPolicyBuilder, Jitter, Retry};

let policy = ExecutionPolicyBuilder::<_, reqwest::Error>::new()
    .retry(
        Retry::exponential()
            .max_attempts(4)
            .base_delay(Duration::from_millis(100))
            .jitter(Jitter::Full)
            .when(|e: &reqwest::Error| e.is_timeout() || e.is_connect()),
    )
    .build();

let resp = policy.run(async || client.get(&url).send().await?.error_for_status()).await?;
```

**3. Bound how long it can take.** A per-attempt cap and an overall deadline; use
`execute` when you want the attempt number (e.g. for a header or log).

```rust
let policy = ExecutionPolicyBuilder::<_, reqwest::Error>::new()
    .retry(Retry::exponential().max_attempts(4).jitter(Jitter::Full))
    .attempt_timeout(Duration::from_secs(2))
    .total_timeout(Duration::from_secs(8))
    .build();

let resp = policy
    .execute(async |attempt| {
        client.get(&url)
            .header("x-attempt", attempt.number().to_string())
            .send().await?.error_for_status()
    })
    .await?;
```

**4. The full picture.** Add a circuit breaker and a concurrency limit, and inject
your client as state so the closure borrows it cleanly across retries.

```rust
use execution_policy::{CircuitBreaker, ConcurrencyLimit, ExecutionPolicyBuilder, Jitter, Retry};

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
    .circuit_breaker(CircuitBreaker::consecutive_failures(5).open_for(Duration::from_secs(30)))
    .concurrency_limit(ConcurrencyLimit::operations(32))
    .build();

let body = policy
    .execute_with(&client, async |client, attempt| {
        client.get("https://example.com")
            .header("x-attempt", attempt.number().to_string())
            .send().await?.error_for_status()
    })
    .await?;
```

## Why it's different

- **Operation factory, not a future.** The closure is re-invoked per attempt, so
  every retry builds a fresh request/future — **no `Clone` bound** on your types.
- **`!Send` friendly.** The engine drives futures in place (never `spawn`s), so
  operations capturing `Rc`/`RefCell` work fine.
- **Classification separate from policy.** Retry and circuit-breaker decisions are
  independent; you can inspect `Ok`-wrapped failures (e.g. an HTTP 503 inside
  `Ok(Response)`).
- **Deterministic tests.** Inject a `TestCore`/`ManualClock` — no real sleeps,
  reproducible jitter and breaker windows.
- **Fast.** ~65 ns success overhead; **zero heap allocation on the success path**
  (timers are armed lazily, only once an operation actually pends).

## The four-method ergonomic gradient

```rust
policy.run(async || do_work().await).await?;                          // no state, no metadata
policy.run_with(&deps, async |deps| deps.go().await).await?;          // state
policy.execute(async |attempt| work(attempt.number()).await).await?;  // attempt metadata
policy.execute_with(&deps, async |deps, attempt| { /* … */ }).await?; // both
```

## Composition order (fixed & documented)

```
total_timeout( concurrency( circuit_breaker( retry( attempt_timeout( operation ) ) ) ) )
```

The concurrency gate is acquired **once** per call; the breaker records **one
vote per pipeline outcome**; `attempt_timeout` bounds each try; `total_timeout`
bounds everything including backoff.

## Errors

`ExecutionError<E>` implements `std::error::Error` (the operation error is its
`source`, so `?` chains cleanly) and carries diagnostic context — attempts made,
elapsed time, last backoff delay, breaker state. Predicates avoid matching:
`is_timeout()`, `is_circuit_open()`, `is_rejected()`, `is_exhausted()`;
`into_inner()` recovers the operation error.

## Observability

Register a synchronous hook — zero cost when absent (events aren't even
constructed without a hook):

```rust
let policy = ExecutionPolicyBuilder::<u32, &str>::new()
    .retry(Retry::exponential().max_attempts(4))
    .on_event(|e| eprintln!("{e:?}"))
    .build();
```

Enable the `tracing` feature and call `.with_tracing()` to bridge events to
`tracing` automatically.

## Honor a server's retry-after hint

Some servers tell you exactly how long to wait — HTTP `Retry-After`, gRPC
`RetryInfo`, database backpressure headers, queue throttle responses. Pass a
closure to `.retry_after(f)` and it acts as a **floor** on the next backoff
delay: the engine uses `max(backoff, hint)`. The hint is still capped by
`max_backoff` (if set), and if honoring it would overshoot the `total_timeout`
budget, the engine stops rather than overshooting.

**This crate has NO http/reqwest dependency.** Your closure receives `&E` —
you extract whatever field carries the hint, and the crate stays transport-agnostic.

```rust
use std::time::Duration;
use execution_policy::{ExecutionPolicyBuilder, Retry};

// Your error type — could be an HTTP, gRPC, DB, or queue error.
struct ApiError {
    retry_after_secs: Option<u64>,
}

let policy = ExecutionPolicyBuilder::<_, ApiError>::new()
    .retry(
        Retry::exponential()
            .max_attempts(5)
            .retry_after(|e: &ApiError| {
                e.retry_after_secs.map(Duration::from_secs)
            }),
    )
    .total_timeout(Duration::from_secs(30))
    .build();
```

**HTTP example** — parse the `Retry-After` header in the consumer:

```rust
// (reqwest is YOUR dependency, not this crate's)
let policy = ExecutionPolicyBuilder::<_, reqwest::Error>::new()
    .retry(
        Retry::exponential()
            .max_attempts(4)
            .when(|e: &reqwest::Error| e.status().map_or(false, |s| s == 429 || s.is_server_error()))
            .retry_after(|_e: &reqwest::Error| {
                // Caller parses the response header before returning the error.
                // Example: store the hint in a thread-local or a wrapper type.
                None // replace with your parsed Duration
            }),
    )
    .build();
```

The same mechanism applies equally to **gRPC** (`RetryInfo` delay), **databases**
(connection-pool saturation hints), and **message queues** (throttle backoff
directives) — any transport that embeds an explicit delay in its error type.

## Retry budgets

Bound retry storms across calls with a shared token bucket:

```rust
use execution_policy::RetryBudget;

let budget = RetryBudget::standard(); // 20% retry ratio, burst 10
let policy = ExecutionPolicyBuilder::<u32, &str>::new()
    .retry(Retry::exponential().max_attempts(4).budget(budget.clone()))
    .build();
```

## Features

| feature     | default | enables                                            |
|-------------|:-------:|----------------------------------------------------|
| `tokio`     | ✅      | `TokioCore` / `DefaultCore` (production timers)     |
| `test-util` | ✅      | `TestCore` / `ManualClock` (no extra deps)         |
| `tracing`   |         | `.with_tracing()` event bridge                     |

`test-util` is default-on so tests/benches/examples work out of the box — it pulls
in **no** dependencies. For a lean production build:

```toml
execution-policy = { version = "*", default-features = false, features = ["tokio"] }
```

The core even compiles with `--no-default-features` (no runtime); supply your own
`Core` to run anywhere.

## Cancellation

A timeout drops the operation future — it does **not** abort remote or blocking
work the operation started. The engine is built around a `select!`-style seam, so
cooperative `CancellationToken` support can be added without breaking the API.

## Why not a composable strategy pipeline?

The bundled builder already covers ~90 % of real-world composition needs. In
particular it provides **both timeout orderings** through separate knobs —
`attempt_timeout` (per-attempt, inside the retry loop) and `total_timeout`
(overall deadline including backoff), plus retry · retry-after hints ·
circuit breaker · concurrency · fallback in a sensible fixed order.

A composable *pipeline* in the style of Polly v8 or Tower would add:

- A `Strategy` trait (or similar abstraction layer)
- Per-layer boxed futures and explicit ordering semantics
- Combinatorial tests for every valid and invalid ordering

…for mostly long-tail value: custom third-party strategies, exotic orderings,
request hedging. That overlap is already served by `tower` and
`tower-resilience`, which this crate deliberately is NOT — it targets
anything that isn't a Tower `Service`.

**Migration path if ever needed:** introduce a `Strategy` trait and keep the
builder as a facade that assembles a default pipeline — a non-breaking,
opt-in evolution with no API churn for existing callers.

## License

BSD-3-Clause.
