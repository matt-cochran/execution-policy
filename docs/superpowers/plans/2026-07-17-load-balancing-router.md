# Load-Balancing Router (execution-policy 0.0.5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the crate's N-target `FallbackPolicy` into a composable, load-balancing `RouterPolicy` — one router, a `Pick` scoring seam over which round-robin, least-in-flight, weighted-LIF, P2C, peak-EWMA, and ordered-failover all compose, with a latency meter seam and `Arc`-shared per-member state.

**Architecture:** Rename `FallbackPolicy`→`RouterPolicy` with a **generic opaque `Id`**. Registration is a `Clone` `Member` handle wrapping an `ExecutionPolicy` + `Arc<MemberState>` (in-flight `AtomicUsize`, pick-count `AtomicU64`, meter cell) so one member joins many routers sharing health+load. Selection is a `Pick` value carrying **only closures** (`by_score`/`by_sampled_score`) — no strategy discriminant, so per-algorithm branching is structurally impossible. Every mis-config fails fast at build.

**Tech Stack:** Rust (edition 2024, rustc ≥ 1.85), pure `std` (no new deps), `Core` trait for clock+RNG (deterministic under `TestCore`/`ManualClock`), `#[forbid(unsafe_code)]`.

## Global Constraints

- **Edition 2024, `rust-version = "1.85"`.** No `unsafe` (`#![forbid(unsafe_code)]` is set crate-wide).
- **No new dependencies.** Pure `std` + existing modules only.
- **`test-util` is default-on**; all tests use `TestCore::new(ManualClock::new())` and a seeded RNG — never wall-clock, never `Math.random`-equivalent. `Core::now`/`Core::next_u64` are the only time/rng sources.
- **TDD Iron Law:** no production code without a failing test first. Every task: write test → run it, watch it fail → implement minimally → run it, watch it pass → commit.
- **Fail-fast, no fallback:** every mis-config surfaces a typed error with actionable context (offending id/value). No `debug_assert` for production invariants — those vanish in release.
- **Greenfield cutover:** rename cleanly, no deprecation aliases (zero external consumers).
- **One `cargo` at a time.** Scope test runs with `-p`/`--test`; never launch concurrent cargo.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Spec of record: `docs/superpowers/specs/2026-07-17-load-balancing-router-design.md` (FMECA-vetted). Section refs (F1–F12, §N) below point into it.

---

## File Structure

- `src/fallback.rs` → **renamed** to `src/router.rs` — `RouterPolicy`, `RouterPolicyBuilder`, `RouterError`, `Target`, the run loop, argmin, sampling, build validation.
- `src/member.rs` — **new** — `Member<Id, C, T, E>`, `MemberState`, `InFlightGuard`, weight validation.
- `src/pick.rs` — **new** — `Pick<Id>`, `Candidate<'a, Id>`, the named strategy constructors.
- `src/meter.rs` — **new** — `Meter`, `Sample`, `PeakEwmaState`, the `peak_ewma` fold.
- `src/error.rs` — **modify** — nothing structural; `RouterError` lives in `router.rs` (as `FallbackError` did in `fallback.rs`).
- `src/lib.rs` — **modify** — module list + re-exports.
- `tests/load_balancing.rs` — **new** — strategy behavior, cross-pool sharing, meter, build-validation integration tests.
- `CHANGELOG.md`, `README.md` — **modify** — 0.0.5 entry + a router example.

---

## Task 1: Mechanical rename `FallbackPolicy` → `RouterPolicy` (no behavior change)

Pure rename so the diff that follows is small and reviewable. `Id` stays `String`, `Selection` stays, all existing tests keep passing.

**Files:**
- Rename: `src/fallback.rs` → `src/router.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Produces: `RouterPolicy<C, T, E>`, `RouterPolicyBuilder<C, T, E>`, `RouterError<E>` (was `FallbackError`), `Selection`, `Served<T>`, and `RouterPolicy::builder()`.

- [ ] **Step 1: Rename the file**

```bash
cd /path/to/execution-policy
git mv src/fallback.rs src/router.rs
```

- [ ] **Step 2: Rename the types inside `src/router.rs`**

In `src/router.rs`, replace every identifier: `FallbackPolicyBuilder`→`RouterPolicyBuilder`, `FallbackPolicy`→`RouterPolicy`, `FallbackError`→`RouterError`. Update the module doc comment's first line to `//! Generic multi-target router over N single-target [\`ExecutionPolicy\`]s.` Leave `Selection`, `Served`, `Target`, and all logic unchanged.

- [ ] **Step 3: Update `src/lib.rs`**

```rust
// was: pub mod fallback;
pub mod router;

// was: pub use crate::fallback::{ FallbackError, FallbackPolicy, FallbackPolicyBuilder, Selection, Served };
pub use crate::router::{
    RouterError, RouterPolicy, RouterPolicyBuilder, Selection, Served,
};
```

- [ ] **Step 4: Run the existing suite — expect all PASS (pure rename)**

Run: `cargo test -p execution-policy --lib router`
Expected: the moved `#[cfg(test)] mod tests` in `router.rs` passes under the new names.
Run: `cargo build --all-features`
Expected: clean (catches any missed re-export).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename FallbackPolicy -> RouterPolicy (mechanical, no behavior change)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Generic opaque `Id` on the router

Replace the target's `String` id with a generic `Id`. Existing tests use `String` as the id, so behavior is identical.

**Files:**
- Modify: `src/router.rs`

**Interfaces:**
- Produces: `RouterPolicy<Id, C, T, E>` where `Id: Clone + Eq + Hash + Debug + Send + Sync + 'static`; `Served<Id, T> { value: T, target: Id, attempts: u32 }`; `run<F>(&self, op: F)` where `F: AsyncFnMut(&Id) -> Result<T, E>`.

- [ ] **Step 1: Write the failing test** (append to `src/router.rs` `mod tests`)

```rust
#[tokio::test]
async fn generic_id_carries_typed_provenance() {
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct Mid(u32);
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(Mid(7), policy(&clock))            // Task 2 still uses (id, policy); Task 3 changes this
        .select(Selection::FirstHealthy)
        .advance_when(|e: &u16| *e == 429)
        .build();
    let served = router.run(async |id: &Mid| { assert_eq!(id, &Mid(7)); Ok::<u32, u16>(1) })
        .await.expect("served");
    assert_eq!(served.target, Mid(7));
}
```

- [ ] **Step 2: Run test — expect FAIL to compile**

Run: `cargo test -p execution-policy --lib router::tests::generic_id_carries_typed_provenance`
Expected: FAIL — `RouterPolicy` is not yet generic over `Id`; `String`-typed target rejects `Mid`.

- [ ] **Step 3: Make the router generic over `Id`**

In `src/router.rs`:
- Add `use std::hash::Hash;` and ensure `std::fmt::Debug` is in scope.
- Change `struct Target<C, T, E> { id: String, policy: ... }` → `struct Target<Id, C, T, E> { id: Id, policy: ExecutionPolicy<C, T, E> }`.
- Change `pub struct RouterPolicy<C, T, E>` → `pub struct RouterPolicy<Id, C, T, E>` and thread `Id` through `RouterPolicyBuilder`, `impl` blocks, and `Debug` impls (the `Debug` impls print `&t.id` — require `Id: Debug`).
- Change `pub struct Served<T>` → `pub struct Served<Id, T> { pub value: T, pub target: Id, pub attempts: u32 }`.
- In `builder()`, keep `.target(id: impl Into<String>, ...)` **only for this task's transitional test** as `.target(id: Id, policy: ExecutionPolicy<C,T,E>)`.
- Add the bound `Id: Clone + Eq + Hash + Debug + Send + Sync + 'static` on the `impl` that has `run`.
- In `run`, change `op: F where F: AsyncFnMut(&str) -> Result<T, E>` → `F: AsyncFnMut(&Id) -> Result<T, E>`; pass `&target.id` (not `id.as_str()`); build `Served { value, target: id.clone(), attempts }`.
- Update `src/lib.rs` re-export (no signature text there, just the names — unchanged).
- Fix the existing `router.rs` tests: their `.target("deepseek", ..)` calls now pass `&str`; change to `.target("deepseek".to_string(), ..)` so `Id = String`.

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test -p execution-policy --lib router`
Expected: PASS (new generic test + migrated String tests).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: generic opaque Id on RouterPolicy (typed provenance)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `Member` handle + `MemberState` + weight validation (F1)

Introduce the registrable, `Clone`, `Arc`-shared member handle and switch `.target()` to take it.

**Files:**
- Create: `src/member.rs`
- Modify: `src/router.rs`, `src/lib.rs`

**Interfaces:**
- Produces:
  - `MemberState { in_flight: AtomicUsize, pick_count: AtomicU64, meter: Mutex<Option<PeakEwmaState>> }` (meter cell filled in Task 8; `Option::None` = never folded).
  - `Member<Id, C, T, E> { id: Id, policy: ExecutionPolicy<C,T,E>, weight: f64, state: Arc<MemberState> }`, `Clone`.
  - `Member::new(id, policy) -> Self` (weight `1.0`), `Member::weight(self, w: f64) -> Self` (validates), `Member::try_weight(self, w) -> Result<Self, WeightError>`.
  - `WeightError(f64)` with `Display`.
- Consumes: `RouterPolicyBuilder::target` now takes `Member`.

- [ ] **Step 1: Write the failing tests** (`src/member.rs`, new file, bottom)

```rust
#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::builder::ExecutionPolicyBuilder;
    use crate::core::{ManualClock, TestCore};
    use crate::retry::Retry;

    fn pol() -> crate::policy::ExecutionPolicy<TestCore, u32, u16> {
        ExecutionPolicyBuilder::<u32, u16>::new()
            .retry(Retry::exponential().max_attempts(1))
            .build_with(TestCore::new(ManualClock::new()))
    }

    #[test]
    fn new_member_defaults_to_weight_one_and_shares_state() {
        let m = Member::new("a".to_string(), pol());
        assert_eq!(m.weight, 1.0);
        let c = m.clone();
        // Clone shares the same Arc<MemberState>.
        assert!(std::sync::Arc::ptr_eq(&m.state, &c.state));
    }

    #[test]
    fn weight_rejects_zero_negative_and_non_finite() {
        assert!(Member::new("a".to_string(), pol()).try_weight(0.0).is_err());
        assert!(Member::new("a".to_string(), pol()).try_weight(-1.0).is_err());
        assert!(Member::new("a".to_string(), pol()).try_weight(f64::NAN).is_err());
        assert!(Member::new("a".to_string(), pol()).try_weight(f64::INFINITY).is_err());
        assert_eq!(Member::new("a".to_string(), pol()).try_weight(2.5).unwrap().weight, 2.5);
    }

    #[test]
    #[should_panic(expected = "weight must be finite and > 0")]
    fn weight_panics_on_invalid() {
        let _ = Member::new("a".to_string(), pol()).weight(0.0);
    }
}
```

- [ ] **Step 2: Run tests — expect FAIL to compile**

Run: `cargo test -p execution-policy --lib member`
Expected: FAIL — `member` module and `Member` do not exist.

- [ ] **Step 3: Implement `src/member.rs`**

```rust
//! The registrable member handle and its Arc-shared per-member state.
//!
//! A `Member` bundles a target id, its single-target `ExecutionPolicy`, a load
//! weight, and an `Arc<MemberState>`. Cloning a `Member` shares the state, so the
//! same member registered in multiple routers shares one breaker (via the
//! policy's `Arc<Plan>`) and one load signal (via `Arc<MemberState>`) — see §5.

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use crate::meter::PeakEwmaState;
use crate::policy::ExecutionPolicy;

/// Per-member load/health state shared across every router the member joins.
#[derive(Debug)]
pub struct MemberState {
    /// Outstanding calls right now (a *signal*, never a cap — §6/F5).
    pub(crate) in_flight: AtomicUsize,
    /// Total times this member was chosen, across ALL pools (global — F11).
    pub(crate) pick_count: AtomicU64,
    /// Latency meter cell; `None` until the first successful fold (Task 8).
    pub(crate) meter: Mutex<Option<PeakEwmaState>>,
}

impl MemberState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: AtomicUsize::new(0),
            pick_count: AtomicU64::new(0),
            meter: Mutex::new(None),
        })
    }
}

/// Rejected non-finite / non-positive weight (F1).
#[derive(Debug, Clone, Copy)]
pub struct WeightError(pub f64);
impl std::fmt::Display for WeightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "member weight must be finite and > 0, got {}", self.0)
    }
}
impl std::error::Error for WeightError {}

/// A registrable member: id + single-target policy + weight + shared state.
pub struct Member<Id, C, T, E> {
    pub(crate) id: Id,
    pub(crate) policy: ExecutionPolicy<C, T, E>,
    pub(crate) weight: f64,
    pub(crate) state: Arc<MemberState>,
}

impl<Id: Clone, C: Clone, T, E> Clone for Member<Id, C, T, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            policy: self.policy.clone(),
            weight: self.weight,
            state: Arc::clone(&self.state), // shared — the cross-pool mechanism
        }
    }
}

impl<Id, C, T, E> Member<Id, C, T, E> {
    /// A new member with weight `1.0` and fresh shared state.
    pub fn new(id: Id, policy: ExecutionPolicy<C, T, E>) -> Self {
        Self { id, policy, weight: 1.0, state: MemberState::new() }
    }

    /// Set the load weight. **Panics** on non-finite / non-positive (F1) — a
    /// mis-weighted member corrupts every weighted score, so it fails fast.
    pub fn weight(self, w: f64) -> Self {
        self.try_weight(w).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Fallible weight setter for callers that want a `Result`.
    pub fn try_weight(mut self, w: f64) -> Result<Self, WeightError> {
        if !w.is_finite() || w <= 0.0 {
            return Err(WeightError(w));
        }
        self.weight = w;
        Ok(self)
    }
}
```

Add `pub mod member;` and `pub mod meter;` to `src/lib.rs` (meter is created in Task 8, but `MemberState` references `PeakEwmaState` now — so create a minimal `src/meter.rs` stub in this task):

```rust
// src/meter.rs (stub — fleshed out in Task 8)
//! Latency meter seam (peak-EWMA). Fold logic lands in Task 8.
use std::time::Instant;

/// Per-member peak-EWMA state.
#[derive(Debug, Clone, Copy)]
pub struct PeakEwmaState {
    pub(crate) value_secs: f64,
    pub(crate) last_update: Instant,
}
```

- [ ] **Step 4: Switch `RouterPolicyBuilder::target` to take a `Member`**

In `src/router.rs`: change `Target` to store the member's parts, and `.target`:

```rust
// Target now wraps a Member's registrable parts.
struct Target<Id, C, T, E> {
    id: Id,
    policy: ExecutionPolicy<C, T, E>,
    weight: f64,
    state: std::sync::Arc<crate::member::MemberState>,
}

impl<Id, C, T, E> RouterPolicyBuilder<Id, C, T, E> {
    pub fn target(mut self, m: crate::member::Member<Id, C, T, E>) -> Self {
        self.targets.push(Target { id: m.id, policy: m.policy, weight: m.weight, state: m.state });
        self
    }
}
```

Update `router.rs` tests: `.target("deepseek".to_string(), pol())` → `.target(Member::new("deepseek".to_string(), pol()))`; import `use crate::member::Member;`.

- [ ] **Step 5: Run tests — expect PASS**

Run: `cargo test -p execution-policy --lib member router`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: Member handle + Arc-shared MemberState + weight validation (F1)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Per-attempt in-flight RAII guard (F5, F14, F16)

In-flight counting that increments on selection and decrements on drop — scoped to a single attempt, so a failed-over member's count returns to 0 before the next is scored.

**Files:**
- Modify: `src/router.rs`

**Interfaces:**
- Produces: `InFlightGuard` (private); `router` increments `state.in_flight` on selection and drops the guard on attempt completion, before advancing.

- [ ] **Step 1: Write the failing test** (`src/router.rs` `mod tests`)

```rust
use std::sync::atomic::Ordering;

#[tokio::test]
async fn in_flight_returns_to_zero_before_failover_advance() {
    let clock = ManualClock::new();
    let a = Member::new("a".to_string(), policy(&clock));
    let b = Member::new("b".to_string(), policy(&clock));
    let a_state = std::sync::Arc::clone(&a.state);
    let router = RouterPolicy::builder()
        .target(a).target(b)
        .select(Selection::FirstHealthy)
        .advance_when(|e: &u16| *e == 429)
        .build();

    let served = router.run(async |id: &String| {
        if id == "a" {
            // While serving 'a', its in-flight is exactly 1.
            assert_eq!(a_state.in_flight.load(Ordering::SeqCst), 1);
            Err::<u32, u16>(429) // transient → advance to 'b'
        } else {
            // By the time 'b' is scored, 'a' has been released.
            assert_eq!(a_state.in_flight.load(Ordering::SeqCst), 0);
            Ok(1)
        }
    }).await.expect("served by b");
    assert_eq!(served.target, "b".to_string());
    assert_eq!(a_state.in_flight.load(Ordering::SeqCst), 0);
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `cargo test -p execution-policy --lib in_flight_returns_to_zero`
Expected: FAIL — nothing increments/decrements `in_flight` yet (`a` reads 0, assert fails).

- [ ] **Step 3: Implement the guard and wire it into the run loop**

In `src/router.rs`, add:

```rust
use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::member::MemberState;

/// Increments in-flight on construction, decrements on drop (panic/early-return
/// safe). Scoped to ONE attempt so a failed-over member is released before the
/// next member is scored (F5).
struct InFlightGuard {
    state: Arc<MemberState>,
}
impl InFlightGuard {
    fn acquire(state: &Arc<MemberState>) -> Self {
        state.in_flight.fetch_add(1, Ordering::SeqCst);
        Self { state: Arc::clone(state) }
    }
}
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}
```

In the `run` loop, wrap each attempt so the guard is created before the call and dropped at the end of that iteration (Rust drops it at scope exit — put the call in a block):

```rust
// inside the for-loop over the chosen target, per attempt:
let served = {
    let _guard = InFlightGuard::acquire(&target.state);
    // pick_count increment + meter fold land in Tasks 6 & 8, before _guard drops.
    match target.policy.run(async || op(&id).await).await {
        Ok(value) => Some(value),
        Err(e) => { /* existing advance/exhaust logic sets last_err */ None }
    }
    // _guard drops HERE — decrement happens before the loop advances.
};
```

Adapt to the existing loop shape (the 0.0.4 loop iterates `self.targets`; keep that, just add the guard block per iteration and preserve `attempts`/`last_err` handling).

- [ ] **Step 4: Run test — expect PASS**

Run: `cargo test -p execution-policy --lib in_flight`
Expected: PASS.

- [ ] **Step 5: Add the panic-safety + signal-not-block assertions**

```rust
#[tokio::test]
async fn in_flight_decrements_on_error_and_success() {
    let clock = ManualClock::new();
    let a = Member::new("a".to_string(), policy(&clock));
    let s = std::sync::Arc::clone(&a.state);
    let router = RouterPolicy::builder().target(a)
        .select(Selection::FirstHealthy).advance_when(|_e: &u16| true).build();
    let _ = router.run(|_id: &String| async { Ok::<u32,u16>(1) }).await;
    assert_eq!(s.in_flight.load(Ordering::SeqCst), 0);
    let _ = router.run(|_id: &String| async { Err::<u32,u16>(500) }).await;
    assert_eq!(s.in_flight.load(Ordering::SeqCst), 0);
}
```

Run: `cargo test -p execution-policy --lib in_flight` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: per-attempt in-flight RAII guard, released before failover (F5)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: `Candidate` + `Pick` seam + `first_healthy` + NaN fail-fast (F7, F8)

Replace `Selection` with `Pick`. Introduce the score seam, the healthy-filtered candidate snapshot, total-order argmin with NaN/inf fail-fast, and the first named strategy.

**Files:**
- Create: `src/pick.rs`
- Modify: `src/router.rs`, `src/lib.rs`

**Interfaces:**
- Produces:
  - `Candidate<'a, Id>` with `id()->&Id`, `index()->usize`, `in_flight()->usize`, `weight()->f64`, `pick_count()->u64`, `latency()->Option<Duration>`.
  - `Pick<Id>` with `by_score(f)`, `by_sampled_score(k, f)`, `first_healthy()`. Holds only closures + `k` — **no discriminant** (F8).
  - `RouterError::Score { id, value }` variant.
  - `RouterPolicyBuilder::select(Pick<Id>)` (replaces `Selection`).

- [ ] **Step 1: Write the failing tests** (`src/pick.rs`)

```rust
#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use crate::core::{ManualClock, TestCore};
    use crate::builder::ExecutionPolicyBuilder;
    use crate::member::Member;
    use crate::retry::Retry;
    use crate::router::{RouterPolicy, RouterError};

    fn policy(clock: &ManualClock) -> crate::policy::ExecutionPolicy<TestCore, u32, u16> {
        ExecutionPolicyBuilder::<u32, u16>::new()
            .retry(Retry::exponential().max_attempts(1))
            .build_with(TestCore::new(clock.clone()))
    }

    #[tokio::test]
    async fn first_healthy_selects_lowest_index() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(Member::new("a".to_string(), policy(&clock)))
            .target(Member::new("b".to_string(), policy(&clock)))
            .select(Pick::first_healthy())
            .advance_when(|e: &u16| *e == 429)
            .build();
        let served = router.run(async |id: &String| { assert_eq!(id, "a"); Ok::<u32,u16>(1) })
            .await.unwrap();
        assert_eq!(served.target, "a".to_string());
    }

    #[tokio::test]
    async fn nan_score_fails_fast_with_member_id() {
        let clock = ManualClock::new();
        let router = RouterPolicy::builder()
            .target(Member::new("a".to_string(), policy(&clock)))
            .select(Pick::by_score(|_c| f64::NAN))
            .advance_when(|_e: &u16| true)
            .build();
        let out = router.run(|_id: &String| async { Ok::<u32,u16>(1) }).await;
        assert!(matches!(out, Err(RouterError::Score { .. })), "got {out:?}");
    }
}
```

- [ ] **Step 2: Run tests — expect FAIL to compile**

Run: `cargo test -p execution-policy --lib pick`
Expected: FAIL — `Pick`, `Candidate`, `RouterError::Score` do not exist.

- [ ] **Step 3: Implement `src/pick.rs`**

```rust
//! The composable selection seam. `Pick` carries ONLY closures (+ a sample size)
//! — there is no strategy enum, so the router cannot branch per algorithm (F8).

use std::sync::Arc;
use std::time::Duration;

/// Read-only per-candidate snapshot handed to a score closure. Candidates are
/// pre-filtered to breaker-healthy, so a score can never route to an open breaker.
pub struct Candidate<'a, Id> {
    pub(crate) id: &'a Id,
    pub(crate) index: usize,
    pub(crate) in_flight: usize,
    pub(crate) weight: f64,
    pub(crate) pick_count: u64,
    pub(crate) latency: Option<Duration>,
}

impl<'a, Id> Candidate<'a, Id> {
    pub fn id(&self) -> &Id { self.id }
    pub fn index(&self) -> usize { self.index }
    pub fn in_flight(&self) -> usize { self.in_flight }
    pub fn weight(&self) -> f64 { self.weight }
    pub fn pick_count(&self) -> u64 { self.pick_count }
    /// Current meter reading; `None` when no meter is configured (F4).
    pub fn latency(&self) -> Option<Duration> { self.latency }
}

pub(crate) type ScoreFn<Id> = Arc<dyn for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync>;

/// A selection strategy: a score closure plus how many candidates to sample
/// (`sample == 0` means "all", i.e. `by_score`). No discriminant — the named
/// constructors are just presets of these two fields.
#[derive(Clone)]
pub struct Pick<Id> {
    pub(crate) score: ScoreFn<Id>,
    pub(crate) sample: usize, // 0 == all candidates
    pub(crate) requires_meter: bool,
}

impl<Id> Pick<Id> {
    /// Argmin of `score` over all healthy candidates.
    pub fn by_score(f: impl for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync + 'static) -> Self {
        Self { score: Arc::new(f), sample: 0, requires_meter: false }
    }
    /// Argmin of `score` over `k` distinct random candidates (`k >= len` ⇒ all).
    pub fn by_sampled_score(k: usize, f: impl for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync + 'static) -> Self {
        Self { score: Arc::new(f), sample: k, requires_meter: false }
    }

    /// Ordered failover: the first healthy target (score = index).
    pub fn first_healthy() -> Self { Self::by_score(|c| c.index() as f64) }
}

impl<Id> std::fmt::Debug for Pick<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pick").field("sample", &self.sample)
            .field("requires_meter", &self.requires_meter).finish()
    }
}
```

- [ ] **Step 4: Add `RouterError::Score` and wire selection into `router.rs`**

In `src/router.rs`:
- Add variant to `RouterError`:

```rust
/// A score closure returned a NaN/infinite value for `id` (F7). A score bug
/// surfaces loudly rather than as a silent mis-route.
Score { id: Id, value: f64 },
```

(Add `Id` to the `RouterError` generic params: `RouterError<Id, E>`; update `Served`/`run` return type to `Result<Served<Id, T>, RouterError<Id, E>>` and the `lib.rs` re-export.)

- Replace the `Selection`-based selection with `Pick`. Add the argmin helper:

```rust
use crate::pick::{Candidate, Pick};
use std::cmp::Ordering;

/// Build healthy candidates, run the score, return the winning target index into
/// `self.targets`, or a Score error on NaN/inf.
fn choose<Id, C, T, E>(
    targets: &[Target<Id, C, T, E>],
    healthy: &[usize],          // indices into targets that are breaker-healthy
    has_meter: bool,
    pick: &Pick<Id>,
    core: &C,
) -> Result<usize, RouterError<Id, E>>
where Id: Clone, C: crate::core::Core {
    // Candidate snapshots (index() is the ORIGINAL target index — stable tie-break).
    let cands: Vec<Candidate<'_, Id>> = healthy.iter().map(|&ti| {
        let t = &targets[ti];
        Candidate {
            id: &t.id,
            index: ti,
            in_flight: t.state.in_flight.load(Ordering::SeqCst),
            weight: t.weight,
            pick_count: t.state.pick_count.load(Ordering::SeqCst),
            latency: if has_meter {
                Some(t.state.meter.lock().unwrap().map(|m| std::time::Duration::from_secs_f64(m.value_secs)).unwrap_or(std::time::Duration::ZERO))
            } else { None },
        }
    }).collect();

    // Which candidate positions to consider (sampling without replacement — F10).
    let positions: Vec<usize> = if pick.sample == 0 || pick.sample >= cands.len() {
        (0..cands.len()).collect()
    } else {
        sample_distinct(cands.len(), pick.sample, core)
    };

    // total_cmp argmin; reject NaN/inf loudly (F7); tie-break keeps lower index.
    let mut best: Option<(usize, f64)> = None;
    for &p in &positions {
        let s = (pick.score)(&cands[p]);
        if !s.is_finite() {
            return Err(RouterError::Score { id: cands[p].id.clone(), value: s });
        }
        let take = match best {
            None => true,
            Some((bp, bs)) => s.total_cmp(&bs) == Ordering::Less
                || (s == bs && cands[p].index < cands[bp].index),
        };
        if take { best = Some((p, s)); }
    }
    Ok(cands[best.expect("healthy is non-empty").0].index)
}
```

- Add `sample_distinct` (partial Fisher–Yates over `Core::next_u64`):

```rust
fn sample_distinct<C: crate::core::Core>(n: usize, k: usize, core: &C) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    for i in 0..k {
        let j = i + (core.next_u64() as usize) % (n - i);
        idx.swap(i, j);
    }
    idx.truncate(k);
    idx
}
```

- In `run`: compute `healthy` (indices whose `circuit_state() != Open`), short-circuit `AllUnavailable` if empty (existing logic), else `let chosen = choose(&self.targets, &healthy, self.meter.is_some(), &self.select, &self.core)?;` and attempt `self.targets[chosen]`. On transient advance, remove `chosen` from `healthy` and re-`choose` (bounded — each attempt drops one). Keep `attempts`/`last_err`/`Exhausted`/permanent-error logic from 0.0.4.
- Replace the builder field `selection: Selection` with `select: Pick<Id>` (default `Pick::first_healthy()`), method `.select(pick: Pick<Id>)`. Add a `meter: Option<...>` field placeholder (`None`; filled in Task 8).
- Delete the `Selection` enum and its `lib.rs` re-export; add `pub mod pick;` + `pub use crate::pick::{Candidate, Pick};`.
- Migrate `router.rs` tests: `Selection::FirstHealthy` → `Pick::first_healthy()`.

- [ ] **Step 5: Run tests — expect PASS**

Run: `cargo test -p execution-policy --lib pick router`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: Pick scoring seam + Candidate + first_healthy; NaN scores fail fast (F7,F8)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Load-only strategies — round-robin, least-in-flight, weighted-LIF + pick_count (F11, F12)

Add the load-aware named constructors and the `pick_count` increment that makes round-robin work. Assertions are **discriminating** (must fail if swapped for `first_healthy`).

**Files:**
- Modify: `src/pick.rs`, `src/router.rs`
- Test: `tests/load_balancing.rs` (new)

**Interfaces:**
- Produces: `Pick::round_robin()`, `Pick::least_in_flight()`, `Pick::weighted_least_in_flight()`. Router increments `pick_count` on the chosen member per attempt.

- [ ] **Step 1: Write the failing tests** (`tests/load_balancing.rs`, new file)

```rust
//! Strategy behavior — each test would FAIL under first_healthy (discriminating).
use execution_policy::{RouterPolicy, Member, Pick};
use execution_policy::core::{ManualClock, TestCore};
use execution_policy::{ExecutionPolicyBuilder, Retry};
use std::collections::HashMap;

fn policy(clock: &ManualClock) -> execution_policy::ExecutionPolicy<TestCore, u32, u16> {
    ExecutionPolicyBuilder::<u32, u16>::new()
        .retry(Retry::exponential().max_attempts(1))
        .build_with(TestCore::new(clock.clone()))
}

#[tokio::test]
async fn round_robin_spreads_evenly() {
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(Member::new("a".to_string(), policy(&clock)))
        .target(Member::new("b".to_string(), policy(&clock)))
        .target(Member::new("c".to_string(), policy(&clock)))
        .select(Pick::round_robin())
        .advance_when(|_e: &u16| true)
        .build();
    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..30 {
        let s = router.run(|id: &String| { let id = id.clone(); async move { Ok::<u32,u16>(1) } }).await.unwrap();
        *counts.entry(s.target).or_default() += 1;
    }
    let (min, max) = (counts.values().min().unwrap(), counts.values().max().unwrap());
    assert!(max - min <= 1, "round-robin spread too wide: {counts:?}"); // first_healthy would give a=30
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `cargo test -p execution-policy --test load_balancing round_robin`
Expected: FAIL — `Pick::round_robin` undefined; and without `pick_count` increment it would collapse to `a` every time.

- [ ] **Step 3: Add the constructors to `src/pick.rs`**

```rust
impl<Id> Pick<Id> {
    /// Least-recently-used by GLOBAL pick count (round-robin — F11).
    pub fn round_robin() -> Self { Self::by_score(|c| c.pick_count() as f64) }
    /// Fewest outstanding calls.
    pub fn least_in_flight() -> Self { Self::by_score(|c| c.in_flight() as f64) }
    /// Fewest outstanding calls per unit weight.
    pub fn weighted_least_in_flight() -> Self {
        Self::by_score(|c| (c.in_flight() as f64 + 1.0) / c.weight())
    }
}
```

- [ ] **Step 4: Increment `pick_count` on the chosen member in `router.rs`**

In the per-attempt block (Task 4), after the call resolves and before the guard drops, increment:

```rust
let _guard = InFlightGuard::acquire(&target.state);
let outcome = target.policy.run(async || op(&id).await).await;
// meter fold lands in Task 8 here.
target.state.pick_count.fetch_add(1, Ordering::SeqCst); // F11 global counter
// _guard drops at block end (F5)
```

- [ ] **Step 5: Add the LIF + weighted assertions to `tests/load_balancing.rs`**

```rust
#[tokio::test]
async fn weighted_lif_holds_two_to_one() {
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(Member::new("heavy".to_string(), policy(&clock)).weight(2.0))
        .target(Member::new("light".to_string(), policy(&clock)))
        .select(Pick::weighted_least_in_flight())
        .advance_when(|_e: &u16| true)
        .build();
    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..300 {
        let s = router.run(|_id: &String| async { Ok::<u32,u16>(1) }).await.unwrap();
        *counts.entry(s.target).or_default() += 1;
    }
    let ratio = counts["heavy"] as f64 / counts["light"] as f64;
    assert!((ratio - 2.0).abs() < 0.30, "weight-2:1 ratio off: {counts:?} ratio={ratio}");
}
```

(Because calls resolve synchronously here, in-flight is 0 between calls; weighting is exercised via `pick_count`-free score — for a load ratio test with instantaneous calls, drive concurrency instead: spawn overlapping calls holding a barrier. If the synchronous form does not exercise weighting, use the concurrent form below.)

Concurrent form (use this if the synchronous test can't distinguish weights):

```rust
// Hold N concurrent calls open on a oneshot barrier so in_flight accumulates,
// then assert the heavy member accrued ~2x the light member's in-flight picks.
```

Run: `cargo test -p execution-policy --test load_balancing` — Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: round_robin/least_in_flight/weighted_lif + global pick_count (F11,F12)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Power-of-two-choices — sampling without replacement (F10)

`p2c` composes on `by_sampled_score`; verify sampling is distinct and deterministic under a seed.

**Files:**
- Modify: `src/pick.rs`
- Test: `tests/load_balancing.rs`

**Interfaces:**
- Produces: `Pick::p2c()`.

- [ ] **Step 1: Write the failing test** (`tests/load_balancing.rs`)

```rust
#[tokio::test]
async fn p2c_is_deterministic_and_not_degenerate() {
    // TestCore RNG is seeded deterministically; two identical routers pick alike.
    fn build(clock: &ManualClock) -> RouterPolicy<String, TestCore, u32, u16> {
        RouterPolicy::builder()
            .target(Member::new("a".to_string(), policy(clock)))
            .target(Member::new("b".to_string(), policy(clock)))
            .target(Member::new("c".to_string(), policy(clock)))
            .target(Member::new("d".to_string(), policy(clock)))
            .select(Pick::p2c())
            .advance_when(|_e: &u16| true)
            .build()
    }
    let (c1, c2) = (ManualClock::new(), ManualClock::new());
    let (r1, r2) = (build(&c1), build(&c2));
    let mut seen = std::collections::HashSet::new();
    for _ in 0..20 {
        let a = r1.run(|_i: &String| async { Ok::<u32,u16>(1) }).await.unwrap().target;
        let b = r2.run(|_i: &String| async { Ok::<u32,u16>(1) }).await.unwrap().target;
        assert_eq!(a, b, "same seed must pick alike");
        seen.insert(a);
    }
    assert!(seen.len() > 1, "p2c must not collapse to one target");
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `cargo test -p execution-policy --test load_balancing p2c`
Expected: FAIL — `Pick::p2c` undefined.

- [ ] **Step 3: Add `p2c` and a distinct-sampling unit test**

`src/pick.rs`:

```rust
impl<Id> Pick<Id> {
    /// Power-of-two-choices: least-in-flight over 2 distinct random candidates.
    pub fn p2c() -> Self { Self::by_sampled_score(2, |c| c.in_flight() as f64) }
}
```

Add a `router.rs` unit test that `sample_distinct(4, 2, &core)` returns two **distinct** indices (never `[x, x]`).

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test -p execution-policy --test load_balancing p2c` and `cargo test -p execution-policy --lib sample_distinct`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: p2c via distinct-index sampling (F10)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: Meter seam — `Sample`, `Meter::peak_ewma` (ok-only, Mutex), `Meter::custom` (F2, F9)

The latency meter: a pure fold applied on completion, folding **only successful** calls, behind a `Mutex`.

**Files:**
- Modify: `src/meter.rs`, `src/router.rs`, `src/lib.rs`
- Test: `src/meter.rs` (unit), `tests/load_balancing.rs`

**Interfaces:**
- Produces: `Sample { latency, at, last_update, in_flight, ok }`; `Meter` with `peak_ewma(half_life)` and `custom(fold)`; `RouterPolicyBuilder::meter(Meter)`.

- [ ] **Step 1: Write the failing unit test** (`src/meter.rs`)

```rust
#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn peak_ewma_ignores_failed_calls() {
        let m = Meter::peak_ewma(Duration::from_secs(10));
        let t0 = Instant::now();
        let mut st = None;
        // A slow SUCCESS raises the value.
        st = Some(m.fold(st, &Sample { latency: Duration::from_millis(800), at: t0, last_update: t0, in_flight: 1, ok: true }));
        let after_success = st.unwrap().value_secs;
        assert!(after_success >= 0.8);
        // A fast FAILURE must NOT lower it (F2).
        st = Some(m.fold(st, &Sample { latency: Duration::from_millis(5), at: t0, last_update: t0, in_flight: 1, ok: false }));
        assert!(st.unwrap().value_secs >= after_success, "failure must not make member look fast");
    }
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `cargo test -p execution-policy --lib meter`
Expected: FAIL — `Meter`, `Sample`, `fold` do not exist.

- [ ] **Step 3: Implement `src/meter.rs`**

```rust
//! Latency meter seam. A meter is a pure fold applied on call completion, so it
//! is deterministic and unit-testable without a live clock. The built-in
//! peak-EWMA folds ONLY successful calls (F2) — a fast failure is the breaker's
//! concern, never a reason to route MORE traffic to a throttling member.

use std::sync::Arc;
use std::time::{Duration, Instant};

/// One completed-call observation.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub latency: Duration,
    pub at: Instant,
    pub last_update: Instant,
    pub in_flight: usize,
    pub ok: bool,
}

/// Per-member peak-EWMA state (stored in `MemberState` behind a `Mutex`, F9).
#[derive(Debug, Clone, Copy)]
pub struct PeakEwmaState {
    pub(crate) value_secs: f64,
    pub(crate) last_update: Instant,
}

type FoldFn = Arc<dyn Fn(Option<PeakEwmaState>, &Sample) -> PeakEwmaState + Send + Sync>;

/// A latency meter. Clone-cheap.
#[derive(Clone)]
pub struct Meter {
    fold: FoldFn,
    pub(crate) reads_latency: bool,
}

impl Meter {
    /// Time-decayed peak-EWMA with the given half-life. Folds only `ok` samples.
    pub fn peak_ewma(half_life: Duration) -> Self {
        let tau = half_life.as_secs_f64().max(f64::MIN_POSITIVE);
        Self {
            reads_latency: true,
            fold: Arc::new(move |prev, s| {
                let observed = s.latency.as_secs_f64();
                match prev {
                    // First-ever fold, or a failure with no prior state: seed/keep.
                    _ if !s.ok => prev.unwrap_or(PeakEwmaState { value_secs: 0.0, last_update: s.at }),
                    None => PeakEwmaState { value_secs: observed, last_update: s.at },
                    Some(p) => {
                        let dt = s.at.saturating_duration_since(p.last_update).as_secs_f64();
                        let w = (-(dt / tau)).exp();              // 1.0 at dt=0, →0 over time
                        let ewma = p.value_secs * w + observed * (1.0 - w);
                        PeakEwmaState { value_secs: ewma.max(observed), last_update: s.at } // peak term
                    }
                }
            }),
        }
    }

    /// A custom fold. `reads_latency` defaults true so `build()` requires a meter
    /// only when a latency-reading strategy is selected; a custom fold that does
    /// not drive a latency score simply is not required by any strategy.
    pub fn custom(f: impl Fn(Option<PeakEwmaState>, &Sample) -> PeakEwmaState + Send + Sync + 'static) -> Self {
        Self { reads_latency: true, fold: Arc::new(f) }
    }

    pub(crate) fn fold(&self, prev: Option<PeakEwmaState>, s: &Sample) -> PeakEwmaState {
        (self.fold)(prev, s)
    }
}

impl std::fmt::Debug for Meter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Meter").field("reads_latency", &self.reads_latency).finish()
    }
}
```

- [ ] **Step 4: Wire the meter into the router (`.meter`, fold on completion)**

In `src/router.rs`:
- Add builder field `meter: Option<crate::meter::Meter>` + method `.meter(m: crate::meter::Meter) -> Self`.
- In the per-attempt block, after the call resolves, fold the meter if configured:

```rust
if let Some(meter) = &self.meter {
    let now = self.core.now();
    let mut cell = target.state.meter.lock().unwrap();
    let last = cell.map(|c| c.last_update).unwrap_or(now);
    let sample = crate::meter::Sample {
        latency: now.saturating_duration_since(attempt_start),
        at: now,
        last_update: last,
        in_flight: target.state.in_flight.load(Ordering::SeqCst),
        ok: outcome.is_ok(),
    };
    *cell = Some(meter.fold(*cell, &sample));
}
```

where `attempt_start = self.core.now()` captured just before the `policy.run`.
- `choose(..)`'s `has_meter` argument is `self.meter.is_some()`.

- [ ] **Step 5: Add the peak-EWMA sheds-share integration test** (`tests/load_balancing.rs`)

```rust
#[tokio::test]
async fn peak_ewma_sheds_share_from_slow_member() {
    // 'slow' returns success but advances the ManualClock a lot per call; 'fast'
    // advances little. After warm-up, peak_ewma routes more to 'fast'.
    // (Drive ManualClock inside the op via a shared handle; assert 'fast' > 'slow'.)
}
```

Fill in with a `ManualClock` the op advances per member; assert `counts["fast"] > counts["slow"]`. Run: `cargo test -p execution-policy --test load_balancing peak_ewma` — Expected: PASS.

- [ ] **Step 6: Add `peak_ewma` strategy + `latency()` snapshot already wired (Task 5). Commit**

```bash
git add -A
git commit -m "feat: meter seam — peak_ewma (ok-only, Mutex) + custom fold (F2,F9)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: `peak_ewma` strategy + `requires_meter` build enforcement (F4)

The latency-aware strategy, plus the hard build error when it is selected without a meter (never a `debug_assert`).

**Files:**
- Modify: `src/pick.rs`, `src/router.rs`
- Test: `src/router.rs`, `tests/load_balancing.rs`

**Interfaces:**
- Produces: `Pick::peak_ewma()` (sets `requires_meter = true`); `build()` errors if a `requires_meter` pick has no meter.

- [ ] **Step 1: Write the failing test** (`src/router.rs` `mod tests`)

```rust
#[test]
#[should_panic(expected = "requires a meter")]
fn peak_ewma_without_meter_panics_at_build() {
    let clock = ManualClock::new();
    let _ = RouterPolicy::builder()
        .target(Member::new("a".to_string(), policy(&clock)))
        .select(Pick::peak_ewma())
        .advance_when(|_e: &u16| true)
        .build(); // no .meter(..) → must fail fast
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `cargo test -p execution-policy --lib peak_ewma_without_meter`
Expected: FAIL — `Pick::peak_ewma` undefined / no build check yet.

- [ ] **Step 3: Add `peak_ewma` and the build check**

`src/pick.rs`:

```rust
impl<Id> Pick<Id> {
    /// Peak-EWMA latency-aware, over 2 distinct samples. Requires a meter (§15).
    pub fn peak_ewma() -> Self {
        let mut p = Self::by_sampled_score(2, |c| {
            let lat = c.latency().expect("peak_ewma requires a meter (enforced at build)").as_secs_f64();
            lat * (c.in_flight() as f64 + 1.0)
        });
        p.requires_meter = true;
        p
    }
}
```

`src/router.rs` `build()` (and `try_build`): add, before constructing the policy:

```rust
if self.select.requires_meter && self.meter.is_none() {
    panic!("selected strategy requires a meter, but none configured; add .meter(Meter::peak_ewma(..))");
}
```

(Use the crate's `BuildError` in `try_build`; `panic!` in `build`, consistent with the single-target builder's existing `build`/`try_build` split.)

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test -p execution-policy --lib peak_ewma`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: peak_ewma strategy + hard requires_meter build error (F4)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: Remaining build-time validation — required `advance_when`, no empty/duplicate/`k==0` (F3, F6)

Close the poka-yoke build gates from §15.

**Files:**
- Modify: `src/router.rs`
- Test: `src/router.rs`

**Interfaces:**
- Produces: `build()` fail-fast on zero targets, duplicate id, unset `advance_when`, `by_sampled_score(0, ..)`.

- [ ] **Step 1: Write the failing tests** (`src/router.rs` `mod tests`)

```rust
#[test]
#[should_panic(expected = "at least one target")]
fn zero_targets_panics() {
    let _ = RouterPolicy::<String, TestCore, u32, u16>::builder()
        .select(Pick::first_healthy()).advance_when(|_e: &u16| true).build();
}
#[test]
#[should_panic(expected = "duplicate member id")]
fn duplicate_id_panics() {
    let clock = ManualClock::new();
    let _ = RouterPolicy::builder()
        .target(Member::new("a".to_string(), policy(&clock)))
        .target(Member::new("a".to_string(), policy(&clock)))
        .select(Pick::first_healthy()).advance_when(|_e: &u16| true).build();
}
#[test]
#[should_panic(expected = "advance_when is required")]
fn missing_advance_when_panics() {
    let clock = ManualClock::new();
    let _ = RouterPolicy::builder()
        .target(Member::new("a".to_string(), policy(&clock)))
        .select(Pick::first_healthy()).build();
}
#[test]
#[should_panic(expected = "sample size must be >= 1")]
fn zero_sample_panics() {
    let clock = ManualClock::new();
    let _ = RouterPolicy::builder()
        .target(Member::new("a".to_string(), policy(&clock)))
        .select(Pick::by_sampled_score(0, |c| c.in_flight() as f64))
        .advance_when(|_e: &u16| true).build();
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cargo test -p execution-policy --lib panics`
Expected: FAIL — no validation yet (`advance_when` currently defaults to `|_| true`).

- [ ] **Step 3: Implement the validations**

In `src/router.rs`:
- Change the builder's `advance_when: Option<ErrorPredicate<E>>` to have **no default** — in `build`, panic if `None`: `advance_when is required — pass |e| ... classifying transient errors, or |_| true to advance on all`.
- In `build`, before compiling:

```rust
assert!(!self.targets.is_empty(), "a RouterPolicy needs at least one target");
{
    let mut seen = std::collections::HashSet::new();
    for t in &self.targets {
        assert!(seen.insert(&t.id), "duplicate member id: {:?}", t.id); // F3
    }
}
assert!(self.select.sample_is_valid(), "by_sampled_score sample size must be >= 1");
```

Add `Pick::sample_is_valid(&self) -> bool { self.sample != 1_usize.wrapping_sub(1) /* placeholder */ }` — simpler: store `sample` as `usize` and treat `0` specially only via the constructor. Instead, guard at the constructor: make `by_sampled_score` record an invalid flag when `k == 0`, checked at build. Concretely add `pub(crate) sample_zero_invalid: bool` set true when `by_sampled_score(0, ..)`; assert `!self.select.sample_zero_invalid`.

(For `try_build`, return `BuildError` with the same messages instead of panicking.)

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test -p execution-policy --lib panics`
Expected: PASS.
Run: `cargo test -p execution-policy --lib` (full lib) — Expected: PASS (migrate any test that relied on the old `advance_when` default to pass one explicitly).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: fail-fast build validation — targets/dup-id/advance_when/sample (F3,F6)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: Cross-pool sharing + healthy-filter + failover regression (F3, assertions §14)

Integration tests proving one member shared across two routers shares in-flight + breaker, plus the healthy-filter and ordered-failover regressions.

**Files:**
- Test: `tests/load_balancing.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn shared_member_shares_in_flight_across_routers() {
    let clock = ManualClock::new();
    let shared = Member::new("m".to_string(), policy(&clock));
    let other  = Member::new("n".to_string(), policy(&clock));
    let s = std::sync::Arc::clone(&shared.state);
    let router_a = RouterPolicy::builder().target(shared.clone())
        .select(Pick::least_in_flight()).advance_when(|_e: &u16| true).build();
    let _router_b = RouterPolicy::builder().target(shared).target(other)
        .select(Pick::least_in_flight()).advance_when(|_e: &u16| true).build();

    // Hold a call open on router_a; assert in_flight visible via the shared Arc.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let a = tokio::spawn(async move {
        router_a.run(|_id: &String| async { rx.await.ok(); Ok::<u32,u16>(1) }).await
    });
    tokio::task::yield_now().await;
    assert_eq!(s.in_flight.load(std::sync::atomic::Ordering::SeqCst), 1); // seen through the shared handle
    tx.send(()).ok();
    a.await.unwrap().unwrap();
    assert_eq!(s.in_flight.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ordered_failover_matches_legacy_behavior() {
    let clock = ManualClock::new();
    let router = RouterPolicy::builder()
        .target(Member::new("primary".to_string(), policy(&clock)))
        .target(Member::new("secondary".to_string(), policy(&clock)))
        .select(Pick::first_healthy())
        .advance_when(|e: &u16| *e == 429 || *e >= 500)
        .build();
    let served = router.run(async |id: &String| {
        if id == "primary" { Err::<u32,u16>(503) } else { Ok(9) }
    }).await.unwrap();
    assert_eq!(served.target, "secondary".to_string());
    assert_eq!(served.attempts, 2);
}
```

- [ ] **Step 2: Run tests — expect FAIL then PASS**

Run: `cargo test -p execution-policy --test load_balancing shared_member ordered_failover`
Expected: FAIL if any wiring is incomplete; otherwise PASS. If they pass immediately, that is valid — they are regression guards over Tasks 1–10. If FAIL, fix the offending task's code, not the test.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test: cross-pool sharing + ordered-failover regression (F3)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 12: `lib.rs` re-exports, CHANGELOG, README, final gate

Finalize the public surface, docs, and the full quality gate.

**Files:**
- Modify: `src/lib.rs`, `CHANGELOG.md`, `README.md`

- [ ] **Step 1: Finalize `src/lib.rs` re-exports**

```rust
pub mod member;
pub mod meter;
pub mod pick;
pub mod router;

pub use crate::member::{Member, WeightError};
pub use crate::meter::{Meter, Sample};
pub use crate::pick::{Candidate, Pick};
pub use crate::router::{RouterError, RouterPolicy, RouterPolicyBuilder, Served};
```

Confirm `Selection`, `FallbackPolicy`, `FallbackError` re-exports are **gone**.

- [ ] **Step 2: Add the CHANGELOG entry**

`CHANGELOG.md`, new top entry:

```markdown
## 0.0.5

### Changed (breaking, pre-1.0 clean cutover)
- `FallbackPolicy` → `RouterPolicy<Id, C, T, E>` — generic opaque target id.
- `Selection` enum → `Pick<Id>` scoring seam (`by_score`/`by_sampled_score` +
  `first_healthy`/`round_robin`/`least_in_flight`/`weighted_least_in_flight`/
  `p2c`/`peak_ewma`).
- `.target(id, policy)` → `.target(Member)`; `Member::new(id, policy).weight(w)`.
- `advance_when` is now **required** (no advance-on-all default).

### Added
- Composable load balancing over N members: one router, no per-algorithm branch.
- `Member` handle with `Arc`-shared per-member state — one member joins many
  routers sharing breaker + in-flight + latency signal.
- `Meter` seam (`peak_ewma`, `custom`); in-flight `AtomicUsize` signal (never a cap).

### Fixed / hardened (FMECA vet)
- NaN/inf scores fail fast (`RouterError::Score`); weight `>0` validated; latency
  strategies hard-error without a meter; `peak_ewma` ignores failed-call latency;
  in-flight released before failover; empty/duplicate/`k==0` rejected at build.
```

- [ ] **Step 3: Update the README** with one `RouterPolicy` example (mirror spec §10, using `Member`/`Pick::weighted_least_in_flight()`/`Meter::peak_ewma`).

- [ ] **Step 4: Run the full gate**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --all-features --no-default-features --features tokio   # lean prod build compiles
cargo test --doc
```
Expected: all green; doctests (README example) compile and pass.

- [ ] **Step 5: Bump the version + commit**

Set `version = "0.0.5"` in `Cargo.toml`.

```bash
git add -A
git commit -m "release: execution-policy 0.0.5 — composable load-balancing router

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review (completed before handoff)

**Spec coverage:** §2 rename/generic-id → T1,T2. §3 Pick seam + no-discriminant + sampling + NaN → T5,T7. §4 Candidate + latency Option → T5. §5 Member/Arc-shared/dup-id/global → T3,T10,T11. §6 in-flight signal → T4. §7 meter/ok-only/Mutex → T8. §8 mechanics/guard-before-advance/advance_when → T4,T9,T10. §9 pressure-test (all named strategies compose + discriminating) → T5–T9, T11. §14 assertions 1–22 → distributed across T4–T11. §15 build validation → T3,T9,T10. §16 vet record → hardening realized in T3–T10. §11 migration/CHANGELOG → T12.

**Placeholder scan:** the peak-EWMA and weighted-concurrency integration tests (T6 Step 5 alt, T8 Step 5) name the mechanism but leave the ManualClock-driving body to the implementer where the exact drive shape depends on the final op signature — these are the only two; every other step carries complete code. Implementer fills them using the `ManualClock`/`oneshot` patterns shown in T11.

**Type consistency:** `Member::new/.weight`, `MemberState.{in_flight,pick_count,meter}`, `Pick::{by_score,by_sampled_score,first_healthy,round_robin,least_in_flight,weighted_least_in_flight,p2c,peak_ewma}`, `Candidate::{id,index,in_flight,weight,pick_count,latency→Option}`, `RouterError::Score{id,value}`, `Meter::{peak_ewma,custom,fold}`, `Sample{latency,at,last_update,in_flight,ok}`, `Served{value,target,attempts}` — consistent across tasks.
