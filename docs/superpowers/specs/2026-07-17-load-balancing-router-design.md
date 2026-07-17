# execution-policy 0.0.5 — Load-Balancing Router (design)

**Status:** design, approved to implement
**Date:** 2026-07-17
**Scope:** execution-policy crate only. The praxec consumer (capability-driven
resolution, behavior specs, tagged member pools) is a *separate* spec and is
explicitly out of scope here — see "Non-goals" and "Consumer boundary".

---

## 1. Summary

`execution-policy` 0.0.4 ships single-target reliability (retry/backoff/breaker/
concurrency/stall) plus `FallbackPolicy` — an N-target router that serves the
**first healthy** target and advances on classified-transient failure.

0.0.5 generalizes that router from *ordered failover only* into a **composable
load balancer**: the same one router, with a pluggable **selection strategy**
(`Pick`) built from a small scoring seam, so round-robin, least-in-flight,
weighted least-in-flight, power-of-two-choices (P2C), peak-EWMA, **and** the
existing ordered-failover all fall out of the *same* primitive with no
per-algorithm branch. It adds a per-member **load meter** seam (latency-EWMA by
default) and makes per-member reliability/load state `Arc`-shareable so one
member can participate in many routers while sharing a single breaker + load
signal.

The crate stays domain-blind. Target identity becomes a **generic `Id`** the
crate treats as opaque — no knowledge of providers, models, accounts, tags, or
effort. Those are entirely the consumer's concern.

### The unifying insight

Almost every load-balancing discipline is **argmin of a score over the healthy
candidates**, optionally over a **random k-sample**:

| discipline                     | score per candidate                | sampling  |
|--------------------------------|------------------------------------|-----------|
| ordered failover (0.0.4)       | `index`                            | all       |
| round-robin                    | `pick_count` (least-recently-used) | all       |
| least-in-flight                | `in_flight`                        | all       |
| weighted least-in-flight       | `(in_flight + 1) / weight`         | all       |
| power-of-two-choices           | `in_flight`                        | random 2  |
| peak-EWMA (latency-aware)      | `latency * (in_flight + 1)`        | random 2  |

Since **`FirstHealthy` is just `score = index`**, the fallback router and the
balancer are the *same object*. We upgrade the one router; we do not ship two.

---

## 2. Rename & generic id

- `FallbackPolicy<C, T, E>` → **`RouterPolicy<Id, C, T, E>`** where
  `Id: Clone + Eq + Hash + Debug + Send + Sync + 'static`.
- `Selection` enum → a **`Pick<Id>`** strategy value (see §3).
- `Served<T>` → **`Served<Id, T>`**: `{ value: T, target: Id, attempts: u32 }`.
- `FallbackError<E>` → **`RouterError<E>`**: same variants
  (`AllUnavailable { next_available_at }`, `Exhausted(E)`).
- `.run(async |id: &Id| ...)` — the closure receives the chosen target id by
  reference; the run shape is otherwise unchanged.
- New **`Member<Id, C, T, E>`** handle — the registrable unit (see §5). A member
  bundles `{ id, policy: ExecutionPolicy, weight, state: Arc<MemberState> }`,
  is `Clone` (cloning shares the `Arc`'d state), and is registered with
  `.target(member)`. This replaces 0.0.4's `.target(id, policy)` two-arg form:
  the id and policy now travel inside the handle so a member can be shared across
  routers (§5). Single-router use is just `.target(Member::new(id, policy))`.

Greenfield cutover: **no deprecation shim, no type alias**. execution-policy has
zero external consumers (the first, praxec, has not yet taken a dependency), so
we rename cleanly. `advance_when`, `deadline`, and the per-target
`ExecutionPolicy` wiring carry over verbatim.

### Why generic `Id` and not `String`

The consumer's member identity is a compound key (in praxec:
`(provider, model, account)`). A generic id lets the consumer pass its **typed**
key straight through — no format/parse round-trip, no stringly bugs — and get
typed provenance back in `Served { target: Id }` and typed per-member breaker
keys. The crate never inspects the id; it only needs `Eq + Hash` (breaker/state
keying) and `Clone` (provenance).

---

## 3. The `Pick` seam — one primitive, named constructors on top

`Pick<Id>` is the selection strategy. Its two primitive constructors are the
whole seam; the named strategies are thin wrappers over them (which is precisely
what makes them the correctness pressure-test — §8).

```rust
// The seam (bring-your-own):
Pick::by_score(|c: &Candidate<'_, Id>| -> f64 { ... })            // argmin score
Pick::by_sampled_score(k: usize, |c: &Candidate<'_, Id>| -> f64)  // argmin over k random draws

// Named constructors — implemented purely via the two above:
Pick::first_healthy()             // by_score(|c| c.index() as f64)
Pick::round_robin()               // by_score(|c| c.pick_count() as f64)
Pick::least_in_flight()           // by_score(|c| c.in_flight() as f64)
Pick::weighted_least_in_flight()  // by_score(|c| (c.in_flight() as f64 + 1.0) / c.weight())
Pick::p2c()                       // by_sampled_score(2, |c| c.in_flight() as f64)
Pick::peak_ewma()                 // by_sampled_score(2, |c| c.latency().as_secs_f64() * (c.in_flight() as f64 + 1.0))
```

Rules:
- **Argmin**, with **deterministic tie-break by ascending `index`** (insertion
  order). Ties never depend on hashmap iteration order or wall clock.
- Candidates handed to the closure are **already filtered to breaker-healthy**
  (breaker not `Open`). A score closure therefore *cannot* route to an open
  breaker — poka-yoke. (An empty healthy set short-circuits to
  `RouterError::AllUnavailable` before any score runs.)
- **Sampling** draws indices from `Core::next_u64` — deterministic under
  `TestCore`. `by_sampled_score(k, ..)` with `k >= healthy_len` degenerates to
  scanning all healthy candidates (no wasted RNG, identical result to `by_score`).
- `Pick` holds only the closure(s) + `k`; it is `Clone` and cheap. Round-robin's
  cursor is **not** in `Pick` — it is the per-member `pick_count` in shared state
  (§5), so round-robin composes as a pure score with no special router path.

The builder gains `.select(Pick<Id>)` replacing `.select(Selection)`. Default
remains `Pick::first_healthy()` (0.0.4 behavior preserved by default).

---

## 4. `Candidate` — the read-only signal snapshot

The score closure sees an immutable snapshot, taken at pick time, of each healthy
candidate:

```rust
pub struct Candidate<'a, Id> { /* borrows shared state */ }

impl<'a, Id> Candidate<'a, Id> {
    pub fn id(&self) -> &Id;
    pub fn index(&self) -> usize;      // insertion order — the ordered-failover score & tie-break
    pub fn in_flight(&self) -> usize;  // current outstanding calls (shared, live)
    pub fn weight(&self) -> f64;       // configured capacity weight (default 1.0)
    pub fn pick_count(&self) -> u64;   // times chosen — the round-robin score
    pub fn latency(&self) -> Duration; // current meter reading; Duration::ZERO if no meter
}
```

`weight` is configured on the `Member` handle (`Member::new(id, policy)` is
weight 1.0; `.weight(w)` overrides). Everything else is derived from shared
per-member state (§5).

---

## 5. Per-member shared state (the cross-pool invariant)

**Invariant:** a member's reliability + load state — breaker, in-flight counter,
meter reading, pick counter — is owned by an `Arc`-shared cell keyed by the
member id, so the **same member registered in multiple `RouterPolicy` instances
shares one breaker and one load signal.**

Rationale (from the consumer): one member can serve multiple capability pools; a
throttle observed via one pool must be visible to all, and true in-flight is the
*sum* across pools.

Mechanism — a **shared `Member` handle**:
- `ExecutionPolicy` is `Clone` and shares an `Arc<Plan>`; the breaker runtime
  inside is already `Arc`'d. So cloning a member's policy shares breaker health
  today, no change.
- 0.0.5 wraps the **new** per-member state in an `Arc<MemberState>` holding
  `{ in_flight: AtomicUsize, pick_count: AtomicU64, meter: <cell> }`, and bundles
  it with the policy + id + weight into a `Clone` `Member` handle whose `clone()`
  shares that `Arc`. Registering `member.clone()` into a second router is the
  sharing mechanism — both the breaker (via the policy's `Arc<Plan>`) and the load
  signal (via `Arc<MemberState>`) are shared. The router keys on `id`.

```rust
let m = Member::new(id, policy).weight(2.0);      // one Arc<MemberState>
let router_a = RouterPolicy::builder().target(m.clone())./*…*/build();
let router_b = RouterPolicy::builder().target(m.clone())./*…*/build();
// a call in-flight through router_a is visible to router_b's score; a breaker
// trip via a reads Open via b.
```

**Test (acceptance):** two `RouterPolicy` instances sharing one member — a
concurrent call through router A increments the in-flight the score sees in router
B; a breaker trip via A is `Open` when polled via B.

---

## 6. In-flight accounting — atomic, orthogonal to capping

In-flight is a plain **`AtomicUsize` per member**, incremented when the router
selects the member and begins the call, decremented on completion via an **RAII
guard** (`Drop` decrements — panic-safe, early-return-safe).

**In-flight is a *signal*, not a *cap*.** It never blocks. Balancing (spreading
load by reading in-flight) is kept **orthogonal** to bounding concurrency: if a
caller also wants a hard concurrency ceiling, that is the existing
`ConcurrencyLimit`/`Semaphore` on the per-member `ExecutionPolicy`, opted into
independently. We deliberately do **not** reuse the `Semaphore` as the in-flight
counter, because the `Semaphore` *blocks when full* — the opposite of what a
balancer wants (spread, don't queue).

The RAII guard also carries the timing start so the meter can observe latency on
drop (§7).

---

## 7. The meter seam — per-member load signal

A meter turns completed calls into a per-member scalar the score can read
(latency-EWMA is the built-in). It is a **pure fold** applied on call completion,
so it is deterministic and unit-testable without a live clock.

```rust
.meter(Meter::peak_ewma(half_life: Duration))   // built-in default
.meter(Meter::custom(|prev: f64, s: &Sample| -> f64 { ... }))  // same closure pattern
```

```rust
pub struct Sample {
    pub latency: Duration,     // observed wall time of the call
    pub at: Instant,           // Core::now() at completion
    pub last_update: Instant,  // when this member's meter last folded
    pub in_flight: usize,      // outstanding at completion (for peak-style meters)
    pub ok: bool,              // Ok vs Err outcome
}
```

- `Meter::peak_ewma(half_life)` decays the stored latency toward the observed
  sample with a **time-based weight** (`half_life` via `at - last_update`), and
  takes the max of decayed-vs-observed (the "peak" term) so a cold or briefly
  slow member is not instantly forgotten. All times from `Core::now`.
- The router owns the per-member meter cell (in `MemberState`, §5), reads it into
  `Candidate::latency()`, and folds a new `Sample` on each call completion.
- **No meter configured** ⇒ `Candidate::latency()` returns `Duration::ZERO`.
  Latency-aware scores (`peak_ewma`) then degenerate to a health-only tie, resolved
  by index — documented, not surprising. (You would not select `peak_ewma` without
  a meter; a debug-assert warns if you do.)

The meter is a router-level config (one meter definition applied per member).
YAGNI: ship exactly one built-in (`peak_ewma`) behind the seam; the closure form
covers anything else without a crate change.

---

## 8. Router mechanics

Per `run` call:

1. **Snapshot** candidates from the registered targets + shared `MemberState`.
2. **Filter to healthy** — drop targets whose breaker is `Open` (record their
   `cooling_until` for the park hint).
3. If none healthy ⇒ `RouterError::AllUnavailable { next_available_at: soonest }`.
4. **`Pick`** selects one healthy candidate (argmin score, or sampled argmin).
5. **Acquire in-flight guard** on the chosen member (increment; RAII).
6. **Run** the user closure against the chosen `Id`, wrapped by that member's
   `ExecutionPolicy` (its own retry/breaker/timeout apply).
7. On completion: **meter folds** the `Sample`; **guard drops** (decrement);
   **`pick_count` increments**.
8. On success ⇒ `Served { value, target: id, attempts }`.
9. On a **classified-transient** failure (per `advance_when`) ⇒ mark this member
   attempted, return to step 2 over the *remaining* eligible members (this is
   where "focus-in on survivors when throttled" emerges — no special code).
10. On a **permanent** operation error ⇒ fail fast with `RouterError::Exhausted`,
    never burning the rest of the pool (0.0.4 semantics preserved).
11. All members attempted-and-failed ⇒ `RouterError::Exhausted(last_err)`.

`deadline` is surfaced (not self-enforced) exactly as in 0.0.4 — the durable
caller enforces the wall-clock budget at its park/resume points.

Determinism: every time source is `Core::now`; every random draw is
`Core::next_u64`. `TestCore` makes selection, sampling, meter decay, and breaker
transitions fully reproducible.

---

## 9. Correctness gate — the pressure-test

Acceptance criterion for 0.0.5: **every named strategy is implemented *and
tested* purely as a composition of `by_score` / `by_sampled_score`** — no
per-algorithm branch in the router, no escape hatch. If any strategy needs
special-casing, the seam is wrong and we revisit the decomposition before
shipping.

Required strategy tests (each against `TestCore` with `ManualClock` + seeded RNG):
- **ordered failover** = `first_healthy()`: first target serves; on transient it
  advances in index order; equals 0.0.4 behavior byte-for-byte.
- **round-robin** = `round_robin()`: N calls across M healthy members distribute
  evenly by `pick_count`; a member going unhealthy drops out and rejoins on
  cooldown.
- **least-in-flight** = `least_in_flight()`: with skewed live in-flight, the next
  pick is the least-loaded; ties break by index.
- **weighted-LIF** = `weighted_least_in_flight()`: a weight-2 member sustains ~2×
  the share of a weight-1 member at steady state.
- **P2C** = `p2c()`: seeded RNG picks 2, routes to the lesser-loaded; verified
  deterministic under a fixed seed; `k >= len` degenerates to full-scan LIF.
- **peak-EWMA** = `peak_ewma()`: a member with rising measured latency sheds share
  even while its in-flight is low.
- **custom score**: an arbitrary `by_score` closure (e.g. static priority) selects
  as specified — proves the seam is open.

Plus the cross-pool sharing test (§5) and the meter-fold determinism test (§7).

---

## 10. Ergonomic consumption (the target)

```rust
use execution_policy::{RouterPolicy, Member, Pick, Meter};

// Consumer's typed member id — opaque to the crate.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Mid { provider: ProviderId, model: String, account: String }

let a = Member::new(Mid { provider: Fireworks,  model: "accounts/fireworks/models/qwen3-coder".into(), account: "acct-a".into() }, policy_a).weight(2.0);
let b = Member::new(Mid { provider: Openrouter, model: "qwen/qwen3-coder".into(),                      account: "acct-b".into() }, policy_b);

let router = RouterPolicy::builder()
    .target(a.clone())                            // clone into as many pools as this member serves
    .target(b.clone())
    .select(Pick::weighted_least_in_flight())     // or .p2c(), .round_robin(), .peak_ewma(), .by_score(|c| ...)
    .meter(Meter::peak_ewma(Duration::from_secs(10)))
    .advance_when(|e| e.is_throttle_or_5xx())
    .deadline(Duration::from_secs(3600))
    .build();

let served = router.run(async |m: &Mid| call_provider(m).await).await?;
// served.target: Mid   served.attempts: u32
```

---

## 11. API-change / migration impact

- Symbols renamed: `FallbackPolicy`→`RouterPolicy`, `FallbackPolicyBuilder`→
  `RouterPolicyBuilder`, `Selection`→`Pick`, `Served`→`Served<Id,T>`,
  `FallbackError`→`RouterError`. Re-exports in `lib.rs` updated.
- `Selection::FirstHealthy` → `Pick::first_healthy()` (default unchanged).
- `.target(id, policy)` (two args) → `.target(Member)`; `Member::new(id, policy)`
  + `.weight(w)`. Members are `Clone` and shared across routers (§5).
- New public: `Member`, `Pick`, `Candidate`, `Meter`, `Sample`, `.meter`,
  `.select(Pick)`.
- Single-target `ExecutionPolicy` and its builder are **untouched**.
- CHANGELOG entry; minor-version bump to **0.0.5** (greenfield 0.0.x; breaking
  renames are acceptable pre-1.0 with a clean cutover).

---

## 12. Non-goals (this cut)

- **No consumer domain concepts** — providers, models, accounts, capability tags,
  behavior/effort specs, tiers. All praxec-side, in the separate resolution spec.
- **No distributed/cross-process load state** — per-process only.
- **No adaptive weight auto-tuning** — weights are static config.
- **No request hedging** (parallel-race the same request across members).
- **Only one built-in meter** (`peak_ewma`); custom folds cover the rest.

## 13. Consumer boundary (informative)

praxec will hold one canonical `ExecutionPolicy` + `MemberState` per
`(provider, model, account)` member, tag members by capability, and — per
request — materialize a `RouterPolicy` over the members satisfying the requested
behavior spec, cloning the shared per-member policies in. Effort/behavior params
live *inside* the run-closure (`call_provider`), never in the crate. That design
is out of scope here and specified separately.

---

## 14. Behavioral assertions (TDD checklist)

1. `first_healthy()` reproduces 0.0.4 fallback behavior exactly (regression).
2. Healthy-set filter: an `Open`-breaker member is never handed to a score.
3. Empty healthy set ⇒ `AllUnavailable` with the soonest `cooling_until`.
4. Argmin ties break by ascending index, deterministically.
5. `round_robin` distributes evenly by `pick_count` over M healthy members.
6. `least_in_flight` picks the least-loaded under skewed in-flight.
7. `weighted_least_in_flight` holds ~weight-proportional share at steady state.
8. `p2c` is deterministic under a fixed seed; `k>=len` == full-scan LIF.
9. `peak_ewma` sheds share from a member with rising measured latency.
10. In-flight guard decrements on success, on `Err`, and on panic (RAII).
11. In-flight is a signal, never a block (a saturated member still gets scored).
12. Cross-pool: two routers sharing a member share in-flight + breaker state.
13. Meter fold is deterministic under `ManualClock`; no meter ⇒ `latency()==0`.
14. Permanent op error fails fast (`Exhausted`), does not burn remaining members.
15. `by_score` custom closure selects exactly as specified.

## 15. Risks (FMECA-style, prevent → detect → fail-fast)

| # | Failure mode | Effect | Mitigation |
|---|--------------|--------|------------|
| R1 | Score closure reads stale in-flight (race) | Mild imbalance | Snapshot is per-pick and monotonic; imbalance self-corrects next pick. Atomics, no torn reads. Accept — balancing is best-effort by nature. |
| R2 | A custom score routes to an unhealthy member | Wasted attempt on a dead target | **Prevent**: candidates are pre-filtered to healthy; the score cannot see `Open` members. |
| R3 | `peak_ewma` selected with no meter | Silent all-ties | **Detect**: debug-assert on build; documented `latency()==0` degeneration to index order. |
| R4 | In-flight leaks on a non-drop path | Member looks permanently loaded, starves | **Prevent**: RAII guard; test asserts decrement on success/Err/panic (assertion 10). |
| R5 | Two routers key the same member differently | State not shared | **Prevent**: sharing is by `Arc<MemberState>` handle identity + `Id` key; cross-pool test (12) is the gate. |
| R6 | Sampling RNG non-deterministic in tests | Flaky P2C tests | **Prevent**: draws via `Core::next_u64`; `TestCore` seeded; assertion 8. |
| R7 | Rename misses a re-export / doctest | Build break for first consumer | **Detect**: `cargo build --all-features` + doctests + CHANGELOG review in the plan's final gate. |
