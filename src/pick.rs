//! The composable selection seam. `Pick` carries ONLY closures (+ a sample size)
//! — there is no strategy enum, so the router cannot branch per algorithm (F8).
//! Every named strategy is a preset of `by_score` / `by_sampled_score`, which is
//! what makes them the correctness pressure-test: if they all compose here with
//! no escape hatch, the decomposition is right.

use std::sync::Arc;
use std::time::Duration;

/// Read-only per-candidate snapshot handed to a score closure. Candidates are
/// pre-filtered to breaker-healthy by the router, so a score can never route to
/// an open breaker.
pub struct Candidate<'a, Id> {
    pub(crate) id: &'a Id,
    pub(crate) index: usize,
    pub(crate) in_flight: usize,
    pub(crate) weight: f64,
    pub(crate) pick_count: u64,
    pub(crate) latency: Option<Duration>,
}

impl<'a, Id> Candidate<'a, Id> {
    pub fn id(&self) -> &Id {
        self.id
    }
    /// Insertion order — the ordered-failover score and the argmin tie-break.
    pub fn index(&self) -> usize {
        self.index
    }
    /// Outstanding calls right now (shared across every pool — global).
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }
    /// Configured capacity weight (finite, > 0).
    pub fn weight(&self) -> f64 {
        self.weight
    }
    /// Times chosen across every pool this member belongs to (global — F11).
    pub fn pick_count(&self) -> u64 {
        self.pick_count
    }
    /// Current meter reading; `None` when no meter is configured (F4). A custom
    /// latency-aware score must handle `None` rather than be handed a fake zero.
    pub fn latency(&self) -> Option<Duration> {
        self.latency
    }
}

impl<'a, Id> std::fmt::Debug for Candidate<'a, Id> {
    // Deliberately omits `id`: `Candidate` is generic over an unbounded `Id`, and
    // the load signals are what a score reasons about anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Candidate")
            .field("index", &self.index)
            .field("in_flight", &self.in_flight)
            .field("weight", &self.weight)
            .field("pick_count", &self.pick_count)
            .field("latency", &self.latency)
            .finish()
    }
}

pub(crate) type ScoreFn<Id> = Arc<dyn for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync>;

/// A selection strategy: a score closure plus how many candidates to sample
/// (`None` = all healthy, i.e. `by_score`; `Some(k)` = `k` distinct random
/// draws). **No discriminant** — the router has nothing to `match` on, so a
/// per-algorithm branch is structurally impossible (F8).
#[derive(Clone)]
pub struct Pick<Id> {
    pub(crate) score: ScoreFn<Id>,
    pub(crate) sample: Option<usize>,
    pub(crate) requires_meter: bool,
}

impl<Id> Pick<Id> {
    /// Argmin of `score` over all healthy candidates.
    pub fn by_score(f: impl for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync + 'static) -> Self {
        Self {
            score: Arc::new(f),
            sample: None,
            requires_meter: false,
        }
    }

    /// Argmin of `score` over `k` distinct random candidates (`k >= len` ⇒ all,
    /// `k == 0` is rejected at build).
    pub fn by_sampled_score(
        k: usize,
        f: impl for<'a> Fn(&Candidate<'a, Id>) -> f64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            score: Arc::new(f),
            sample: Some(k),
            requires_meter: false,
        }
    }

    /// Ordered failover: the first healthy target (score = index).
    pub fn first_healthy() -> Self {
        Self::by_score(|c| c.index() as f64)
    }

    /// Least-recently-used by GLOBAL pick count (round-robin — F11).
    pub fn round_robin() -> Self {
        Self::by_score(|c| c.pick_count() as f64)
    }

    /// Fewest outstanding calls.
    pub fn least_in_flight() -> Self {
        Self::by_score(|c| c.in_flight() as f64)
    }

    /// Fewest outstanding calls per unit weight.
    pub fn weighted_least_in_flight() -> Self {
        Self::by_score(|c| (c.in_flight() as f64 + 1.0) / c.weight())
    }

    /// Power-of-two-choices: least-in-flight over 2 distinct random candidates.
    pub fn p2c() -> Self {
        Self::by_sampled_score(2, |c| c.in_flight() as f64)
    }

    /// Peak-EWMA latency-aware, over 2 distinct samples. Requires a meter — the
    /// router's build check enforces it, so `latency()` is always `Some` here.
    pub fn peak_ewma() -> Self {
        let mut p = Self::by_sampled_score(2, |c| {
            let lat = c
                .latency()
                .expect("peak_ewma requires a meter (enforced at build)")
                .as_secs_f64();
            lat * (c.in_flight() as f64 + 1.0)
        });
        p.requires_meter = true;
        p
    }

    /// Whether the configured sample size is valid (`Some(0)` is not — F10/§15).
    pub(crate) fn sample_is_valid(&self) -> bool {
        !matches!(self.sample, Some(0))
    }
}

impl<Id> std::fmt::Debug for Pick<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pick")
            .field("sample", &self.sample)
            .field("requires_meter", &self.requires_meter)
            .finish()
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;

    fn cand<'a>(
        id: &'a &'a str,
        index: usize,
        in_flight: usize,
        weight: f64,
        pick_count: u64,
        latency: Option<Duration>,
    ) -> Candidate<'a, &'a str> {
        Candidate {
            id,
            index,
            in_flight,
            weight,
            pick_count,
            latency,
        }
    }

    #[test]
    fn named_strategies_are_compositions_with_the_expected_scores() {
        let name = "a";
        let c = cand(&name, 3, 5, 2.0, 7, Some(Duration::from_millis(100)));

        assert_eq!((Pick::first_healthy().score)(&c), 3.0);
        assert_eq!((Pick::round_robin().score)(&c), 7.0);
        assert_eq!((Pick::least_in_flight().score)(&c), 5.0);
        assert!(((Pick::weighted_least_in_flight().score)(&c) - (6.0 / 2.0)).abs() < 1e-9);
        assert!(((Pick::peak_ewma().score)(&c) - (0.1 * 6.0)).abs() < 1e-9);
    }

    #[test]
    fn sampling_sizes_and_meter_flags_are_set_by_the_constructor() {
        assert_eq!(Pick::<&str>::first_healthy().sample, None);
        assert_eq!(Pick::<&str>::p2c().sample, Some(2));
        assert_eq!(Pick::<&str>::peak_ewma().sample, Some(2));
        assert!(!Pick::<&str>::least_in_flight().requires_meter);
        assert!(Pick::<&str>::peak_ewma().requires_meter);
    }

    #[test]
    fn zero_sample_is_flagged_invalid() {
        assert!(!Pick::<&str>::by_sampled_score(0, |c| c.in_flight() as f64).sample_is_valid());
        assert!(Pick::<&str>::by_sampled_score(2, |c| c.in_flight() as f64).sample_is_valid());
        assert!(Pick::<&str>::by_score(|c| c.index() as f64).sample_is_valid());
    }

    #[test]
    fn no_meter_gives_none_latency_for_custom_scores() {
        let name = "a";
        let c = cand(&name, 0, 0, 1.0, 0, None);
        assert_eq!(c.latency(), None);
    }
}
