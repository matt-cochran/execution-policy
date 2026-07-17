# execution-policy 0.0.5 — Load-Balancing Router (design)

**Status:** design, FMECA-vetted (iteration 1 — all risks Low), approved to implement
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
Pick::peak_ewma()                 // by_sampled_score(2, |c| lat(c) * (c.in_flight() as f64 + 1.0))
                                  //   where lat(c) = c.latency().expect("requires_meter").as_secs_f64()
                                  //   — peak_ewma is flagged requires_meter, so build() guarantees Some (§15).
```

Rules:
- **Argmin via `f64::total_cmp`**, with **deterministic tie-break by ascending
  `index`** (insertion order). Ties never depend on hashmap iteration order or
  wall clock.
- **NaN / non-finite scores fail fast** (F7). Argmin does **not** use
  `partial_cmp().unwrap()` (panics) and does **not** silently treat NaN as
  largest. A NaN or infinite score returns `RouterError::Score { id, value }`
  naming the offending member and value — a score bug surfaces loudly, never as a
  quiet mis-route.
- Candidates handed to the closure are **already filtered to breaker-healthy**
  (breaker not `Open`). A score closure therefore *cannot* route to an open
  breaker — poka-yoke. (An empty healthy set short-circuits to
  `RouterError::AllUnavailable` before any score runs.)
- **Sampling is without replacement** (F10): `by_sampled_score(k, ..)` draws `k`
  **distinct** indices from `Core::next_u64` (deterministic under `TestCore`).
  `k >= healthy_len` degenerates to scanning all healthy candidates (identical to
  `by_score`); `k == 0` is rejected at build (§15).
- `Pick` holds **only** the closure(s) + `k` — it carries **no `StrategyKind`
  enum or discriminant** (F8). The engine has nothing to `match` on, so a
  per-algorithm branch is *structurally impossible*: it will not compile because
  there is no variant to switch over. This is the composability guarantee enforced
  by the type, not by discipline. Round-robin's cursor is likewise **not** in
  `Pick` — it is the per-member `pick_count` in shared state (§5).

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
    pub fn weight(&self) -> f64;       // configured capacity weight (finite, > 0)
    pub fn pick_count(&self) -> u64;   // GLOBAL times chosen across all pools (§5, F11)
    pub fn latency(&self) -> Option<Duration>; // meter reading; None if no meter configured
}
```

`weight` is configured on the `Member` handle and is **validated finite and > 0
at construction** (F1) — `Member::new(id, policy)` is weight 1.0, `.weight(w)`
rejects non-finite/≤0 with an error naming the id and value. So a score never
divides by zero or a negative.

`latency()` returns **`Option<Duration>`** (F4), not a silent `Duration::ZERO`:
a custom latency-aware score must confront `None` explicitly rather than be
handed a fake "0ms = infinitely fast" reading when no meter is configured. The
built-in `peak_ewma` strategy cannot reach `None` because it *requires* a meter
(enforced at build, §15).

`pick_count` and `in_flight` are **global per member** (summed across every pool
the member belongs to), not per-router — see §5/F11. Everything except `index`
and `weight` is derived from shared per-member state (§5).

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

**Split-brain prevention (F3).** The invariant holds only if the *same* `Arc`'d
state backs a given id. Two failure surfaces:
- **Within one router**: registering an id twice (even with the same handle) is a
  config error — `build()` fail-fasts with `RouterError`/`BuildError`
  `duplicate member id: {id}`. A member appears at most once per router.
- **Across routers**: the crate cannot see two routers at once, so it cannot
  detect a consumer that mistakenly calls `Member::new` twice for one id instead
  of cloning one handle. This is an explicit **consumer contract**: *hold exactly
  one canonical `Member` per id and clone it into each pool.* In praxec that
  canonical registry is owned by the resolution layer (spec #2). (TRIZ — Local
  Quality: enforce inside the crate's visibility; delegate the cross-router case
  to the one place that can see all pools.)

**Global load semantics (F11).** Because `in_flight` and `pick_count` are shared
per member, `round_robin()` / least-loaded selection rotate by a member's **total
load across all pools**, not per-pool. This is *intended and correct*: a member
already saturated by pool B should be de-prioritized by pool A. Documented as
"least-recently-used (global)"; a shared-pool test pins the semantic so it can't
silently regress.

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
- **`peak_ewma` folds only `ok == true` samples** (F2). A fast *failure* (e.g. a
  100ms 429 rejection) must never make a throttling member read as "fast" and pull
  *more* traffic — that would route toward exactly the members the balancer should
  shed. Failures are the **breaker's** domain, not the latency meter's. `Sample.ok`
  remains exposed so a custom fold can choose its own policy, but the built-in is
  correct-by-construction here.
- **Concurrency**: the per-member meter state lives in `MemberState` behind a
  `Mutex<PeakEwmaState>` (F9), folded on completion (off the hot path); a data race
  on the shared `f64`+`Instant` is thereby impossible. Reads take the short lock.
- **No meter configured** ⇒ `Candidate::latency()` is `None` (F4), not a fake zero.
  A latency-*reading* named strategy (`peak_ewma`) is flagged `requires_meter` and
  **`build()` hard-errors in all builds** if no meter is set (§15) — never a
  `debug_assert` that vanishes in release. Load-only strategies never call
  `latency()` and are unaffected.

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
5. **Acquire per-attempt in-flight guard** on the chosen member (increment; RAII).
   The guard's scope is **this single attempt**, never the whole `run` (F5).
6. **Run** the user closure against the chosen `Id`, wrapped by that member's
   `ExecutionPolicy` (its own retry/breaker/timeout apply).
7. On attempt completion, **in this order**: **meter folds** the `Sample`
   (ok/latency); **`pick_count` increments**; **the guard drops** (decrement).
   The decrement therefore happens **before** any advance (step 9), so a
   failed-over member is *not* still counted as in-flight while the next member is
   scored — no cumulative inflation across a multi-hop failover (F5).
8. On success ⇒ `Served { value, target: id, attempts }`.
9. On a **classified-transient** failure (per the **required** `advance_when` —
   §15, F6) ⇒ mark this member attempted, return to step 2 over the *remaining*
   eligible members (this is where "focus-in on survivors when throttled" emerges
   — no special code).
10. On a **permanent** operation error ⇒ fail fast with `RouterError::Exhausted`,
    never burning the rest of the pool (0.0.4 semantics preserved).
11. All members attempted-and-failed ⇒ `RouterError::Exhausted(last_err)`.

`deadline` is surfaced (not self-enforced) exactly as in 0.0.4 — the durable
caller enforces the wall-clock budget at its park/resume points. The advance loop
is bounded (each member is attempted at most once per `run`), so it cannot spin.

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
- **`advance_when` is now required** (F6) — the permissive 0.0.4 default (advance
  on *every* error) is removed; `build()` errors if it is unset. Callers who
  genuinely want "advance on all" pass `|_| true` explicitly. This is a deliberate
  0.0.4 behavior change: burning the whole pool on a deterministic permanent error
  must be an explicit choice, never a silent default.
- New public: `Member`, `Pick`, `Candidate`, `Meter`, `Sample`, `.meter`,
  `.select(Pick)`; `Candidate::latency()` is `Option<Duration>`; new
  `RouterError::Score` and `BuildError` cases (§15).
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

Every *strategy* assertion is **discriminating** (F12): it must **fail if the
strategy is swapped for `first_healthy()`** — a built-in mutation check baked into
the test — so no assertion can pass tautologically.

Selection & composition:
1. `first_healthy()` reproduces 0.0.4 fallback behavior exactly (regression).
2. Healthy-set filter: an `Open`-breaker member is never handed to a score.
3. Empty healthy set ⇒ `AllUnavailable` with the soonest `cooling_until`.
4. Argmin ties break by ascending index, deterministically.
5. `round_robin`: over `k·M` calls to M equal healthy members, max−min `pick_count`
   spread ≤ 1.
6. `least_in_flight`: with skewed in-flight, picks the strictly-lower member.
7. `weighted_least_in_flight`: weight-2 vs weight-1 holds a 2:1 share within ±15%
   over ≥ 200 steady-state calls.
8. `p2c`: deterministic under a fixed seed; two draws are **distinct** indices;
   `k ≥ len` == full-scan LIF; result is not always index-0 (not degenerate).
9. `peak_ewma`: a member with rising *successful*-call latency sheds share even
   while its in-flight is low.
10. `by_score` custom closure selects exactly as specified (seam is open).

Numeric & config hygiene (poka-yoke, §15):
11. `Member::weight(w)` rejects `0.0`, negative, and non-finite `w` (F1).
12. A `by_score` closure returning `NaN`/`inf` ⇒ `RouterError::Score`, not a panic
    or arbitrary pick (F7).
13. `build()` fail-fasts on: zero targets; duplicate member id; `by_sampled_score`
    `k == 0`; unset `advance_when`; a `requires_meter` strategy with no meter (§15).

State, lifecycle & concurrency:
14. In-flight guard decrements on success, on `Err`, and on panic (RAII).
15. Failover: a member's in-flight returns to 0 **before** the next member is
    scored (guard drops pre-advance, F5).
16. In-flight is a signal, never a block (a saturated member still gets scored).
17. Cross-pool: two routers sharing a member share in-flight + breaker state (F3).
18. Duplicate id within one router ⇒ `build()` error (F3).
19. Global load: a member's `pick_count`/`in_flight` reflects picks from *all*
    pools it belongs to (F11).
20. `peak_ewma` does **not** fold `ok == false` samples — a fast failure does not
    lower measured latency (F2).
21. Meter fold is deterministic under `ManualClock`; no meter ⇒ `latency() == None`.
22. Permanent op error fails fast (`Exhausted`), does not burn remaining members.

## 15. Build-time validation (poka-yoke)

`RouterPolicyBuilder::build()` (and `try_build`) **fail fast** on every mis-config
below, each with an actionable message naming the offending element. None is a
`debug_assert` — all hold in release.

| Check | Trigger | Rationale (FM) |
|-------|---------|----------------|
| ≥ 1 target | zero members registered | an empty router can only ever return `AllUnavailable` — a silent dead router (config error) |
| unique ids | same id registered twice | split-brain / ambiguous member (F3) |
| `advance_when` set | not configured | forces explicit transient classification; no "advance on everything" default (F6) |
| meter present | strategy is `requires_meter` (e.g. `peak_ewma`) and no `.meter(...)` | latency-aware selection with no latency signal is a silent no-op (F4) |
| `k ≥ 1` | `by_sampled_score(0, ..)` | zero-sample selection is undefined |
| finite `w > 0` | `Member::weight` (at construction, not build) | prevents inf/NaN/negative scores (F1) |

## 16. FMECA vet record (iteration 1, 2026-07-17)

Vetted with the reliability-engineering methodology (FMECA → poka-yoke →
prevent/detect/fail-fast → TRIZ-if-trade-off). 12 failure modes across UX,
runtime, architecture, and delivery; **all reduced to residual Low.** Two TRIZ
resolutions, both *Local Quality* (segment the property to where it is correct):
(a) split-brain enforcement inside the crate's single-router visibility, consumer
registry for the cross-router case (F3); (b) global-per-member load as the
*correct* shared-pool semantic rather than a bug to hide (F11).

Key hardening applied to this spec vs. the pre-vet draft:
- Numeric hygiene: `weight > 0` at construction; NaN/inf scores → `RouterError::Score`
  (total-order argmin, no `unwrap` panic).
- Silent-degradation removed: `latency() → Option`; `requires_meter` strategies
  hard-error at build (no `debug_assert` that vanishes in release).
- Meter correctness: `peak_ewma` folds only successful calls (a fast failure never
  reads as fast); meter state behind a `Mutex` (no data race).
- Lifecycle: per-attempt in-flight guard drops **before** failover advance (no
  cumulative inflation).
- Fail-fast defaults: `advance_when` required; empty/duplicate/`k==0` rejected at
  build.
- Design-integrity: `Pick` carries no discriminant → per-algorithm branching is
  structurally impossible.
- Delivery: strategy assertions are discriminating (must fail if swapped for
  `first_healthy`) with concrete tolerances.

Stop condition met: **all High/Medium risks mitigated to Low in one iteration**
(no residual High/Medium to justify iteration 2). Systemic check — accuracy: no
fabricated figures (tolerances are labeled targets for the tests to pin);
complexity: additions are small fail-fast guards that *remove* silent-failure
classes, net utility gain; capability: no strategy or seam removed.
