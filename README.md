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

## Recovery / fallback at the call site

The policy returns `Result<T, ExecutionError<E>>` — resilience ends there. Recovery (deciding what to return on failure) is business logic that belongs at the call site, not inside a reusable resilience policy; keeping it out via `or_else` also gives you full call context and keeps the policy pure.

```rust,ignore
let value = policy
    .run(op)
    .await
    .or_else(|e| if e.is_circuit_open() { cache.get_stale() } else { Err(e) })
    .or_else(|e| if e.is_exhausted()   { replica.get()      } else { Err(e) })
    .or_else(|e| if e.is_timeout()     { Ok(sentinel())     } else { Err(e) })
    .or_else(|_| Ok(default()))?;
```

Links run in order; the first `Ok` wins — native Rust, no special API. Each closure can discriminate via `e.is_circuit_open()` / `e.is_timeout()` / `e.is_exhausted()` / `e.into_inner()`.

### HTTP reqwest + retry-after + cache recovery

`execution-policy` has **no http/reqwest dependency** — header parsing lives in your crate. Parse the `Retry-After` header into your error type; `.retry_after(f)` honors it as a floor on the next backoff delay inside the policy. Cache recovery is a plain `.or_else(...)` after `policy.run(...)`.

```rust,ignore
use std::time::Duration;
use execution_policy::{ExecutionPolicyBuilder, ExecutionError, Retry};
use reqwest::header::RETRY_AFTER;

// Your error type wraps reqwest and carries the parsed hint.
struct ApiError {
    inner: reqwest::Error,
    retry_after: Option<Duration>,
}

impl ApiError {
    fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }
}

// In your HTTP layer: parse the header before returning the error.
async fn call(client: &reqwest::Client, url: &str) -> Result<String, ApiError> {
    let resp = client.get(url).send().await.map_err(|e| ApiError { inner: e, retry_after: None })?;
    if resp.status() == 429 || resp.status().is_server_error() {
        let hint = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);
        return Err(ApiError { inner: resp.error_for_status().unwrap_err(), retry_after: hint });
    }
    resp.text().await.map_err(|e| ApiError { inner: e, retry_after: None })
}

let policy = ExecutionPolicyBuilder::<_, ApiError>::new()
    .retry(
        Retry::exponential()
            .max_attempts(5)
            .when(|e: &ApiError| e.inner.status().map_or(false, |s| s == 429 || s.is_server_error()))
            .retry_after(|e: &ApiError| e.retry_after()),
    )
    .total_timeout(Duration::from_secs(30))
    .build();

// Recovery is call-site composition — serve from cache while the upstream is throttling or down.
let body = policy
    .run(async || call(&client, "https://api.example.com/data").await)
    .await
    .or_else(|_e: ExecutionError<ApiError>| cache.get_stale())?;
```

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

## Design — guardrails, what you own, and non-goals

### Guardrails (what the crate protects you from)

- **Bounded retries** — `max_attempts` + `total_timeout` + retry **budgets** (shared token bucket) ensure a failing dependency can't trigger a retry storm.
- **Jitter** (`Jitter::Full` / `Jitter::Equal`) spreads retries across callers, avoiding synchronized thundering-herd bursts.
- **Circuit breaker** sheds load off a failing dependency and recovers automatically via half-open probing.
- **`retry_after`** honors server-provided backpressure (HTTP `Retry-After`, gRPC `RetryInfo`, queue throttle headers) as a floor on the next backoff delay.
- **Typed `ExecutionError<E>`** preserves the failure signal — timeout / circuit-open / rejected / exhausted — and never collapses it to a generic string. Predicates (`is_timeout()`, `is_circuit_open()`, etc.) let you discriminate without matching.

### What you own (deliberately NOT in the policy)

- **Idempotency** — the policy never assumes a call is safe to retry. YOU classify what's retryable via `.when(...)`. This is the single most important footgun the crate refuses to guess for you: retrying a non-idempotent write causes duplicates.
- **Recovery / fallback** — handled at the call site with `.or_else(...)` on the returned `Result<T, ExecutionError<E>>`, not a policy knob. Keeping business logic out keeps the policy a reusable resilience primitive. See [Recovery / fallback at the call site](#recovery--fallback-at-the-call-site) for the composable `.or_else` pattern.
- **Transport** — HTTP/gRPC/queue header parsing lives in your crate. The crate has no `http`/`reqwest` dependency; your closure receives `&E` and returns a `Duration`.

### Non-goals & possible future directions

These are deliberately out of scope today. Each note states the trigger that would make it worth adding and the rough approach — not a promise or roadmap.

- **Composable strategy pipeline** (Polly-v8 / Tower style) — *Trigger:* need for third-party custom strategies, caller-controlled exotic ordering, or multiple same-type layers (e.g. two circuit breakers). *Approach:* a `Strategy` trait, with the existing builder kept as a facade that assembles a default pipeline — non-breaking, opt-in, no API churn for existing callers. *Why not now:* the builder already covers ~90 % of real-world needs including both timeout orderings (`attempt_timeout` inside the loop, `total_timeout` as overall deadline); the remaining long-tail overlaps `tower`/`tower-resilience`, which this crate deliberately is not.
- **Hedging** (fire a speculative backup attempt for slow requests; take whichever finishes first; cancel the loser) — *Trigger:* idempotent reads hitting multiple replicas/backends with tail-latency (p99) SLOs. *Approach:* concurrent attempts + cancellation, gated to idempotent ops only via the `.when(...)` predicate. *Why not now:* dangerous for non-idempotent ops, and pointless against a single backend (it just doubles load rather than improving latency).
- **Cross-hop deadline propagation** (thread the remaining time budget across service calls, like `grpc-timeout`) — *Trigger:* multi-hop synchronous call chains where A→B→C each consume part of an inbound deadline. *Approach:* expose a `remaining()` accessor on the policy so the consumer seeds `total_timeout` from an inbound deadline and writes the outbound deadline header; header read/write stays in the consumer/transport layer and the crate remains transport-agnostic. *Why not now:* single-hop usage doesn't need it, and deadline propagation is a transport/context concern rather than a per-operation policy concern.

## License

BSD-3-Clause.
