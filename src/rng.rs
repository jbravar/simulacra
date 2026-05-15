//! Deterministic random number generation.
//!
//! All randomness in Simulacra is seeded and reproducible.

use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::time::Duration;

/// A deterministic random number generator for simulations.
///
/// Uses ChaCha8 for good statistical properties and reproducibility.
/// All operations are deterministic given the same seed.
#[derive(Debug, Clone)]
pub struct SimRng {
    rng: ChaCha8Rng,
}

impl SimRng {
    /// Creates a new RNG with the given seed.
    pub fn new(seed: u64) -> Self {
        SimRng {
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Creates a new RNG from a byte seed.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        SimRng {
            rng: ChaCha8Rng::from_seed(seed),
        }
    }

    /// Generates a random boolean with the given probability of being true.
    #[inline]
    pub fn bool(&mut self, probability: f64) -> bool {
        self.rng.random_bool(probability)
    }

    /// Generates a random u64 in the range [0, max).
    #[inline]
    pub fn u64(&mut self, max: u64) -> u64 {
        self.rng.random_range(0..max)
    }

    /// Generates a random u64 in the range [min, max).
    #[inline]
    pub fn u64_range(&mut self, min: u64, max: u64) -> u64 {
        self.rng.random_range(min..max)
    }

    /// Generates a random usize in the range [0, max).
    #[inline]
    pub fn usize(&mut self, max: usize) -> usize {
        self.rng.random_range(0..max)
    }

    /// Generates a random f64 in the range [0.0, 1.0).
    #[inline]
    pub fn f64(&mut self) -> f64 {
        self.rng.random()
    }

    /// Generates a random f64 in the range [min, max).
    #[inline]
    pub fn f64_range(&mut self, min: f64, max: f64) -> f64 {
        self.rng.random_range(min..max)
    }

    /// Generates jitter as a duration within [-max_jitter, +max_jitter].
    ///
    /// Returns a signed jitter value that can be added to a base duration.
    /// The jitter is uniformly distributed.
    pub fn jitter(&mut self, max_jitter: Duration) -> i64 {
        let max = max_jitter.as_nanos() as i64;
        if max == 0 {
            return 0;
        }
        self.rng.random_range(-max..=max)
    }

    /// Generates a duration with jitter applied.
    ///
    /// The result is clamped to be non-negative.
    pub fn duration_with_jitter(&mut self, base: Duration, max_jitter: Duration) -> Duration {
        let jitter = self.jitter(max_jitter);
        let base_nanos = base.as_nanos() as i64;
        let result = (base_nanos + jitter).max(0) as u64;
        Duration::from_nanos(result)
    }

    /// Selects a random element from a slice.
    ///
    /// Panics if the slice is empty.
    pub fn choice<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        assert!(!items.is_empty(), "cannot choose from empty slice");
        &items[self.usize(items.len())]
    }

    /// Shuffles a slice in place using Fisher-Yates algorithm.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.rng.random_range(0..=i);
            items.swap(i, j);
        }
    }

    /// Creates a child RNG with a derived seed.
    ///
    /// Useful for creating independent RNG streams for different concerns
    /// (e.g., separate streams for jitter, failures, workload generation).
    pub fn fork(&mut self) -> SimRng {
        let mut seed = [0u8; 32];
        self.rng.fill(&mut seed);
        SimRng::from_seed(seed)
    }
}

impl Default for SimRng {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_sequence() {
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(42);

        for _ in 0..100 {
            assert_eq!(rng1.u64(1000), rng2.u64(1000));
        }
    }

    #[test]
    fn different_seeds_different_results() {
        let mut rng1 = SimRng::new(1);
        let mut rng2 = SimRng::new(2);

        let seq1: Vec<_> = (0..10).map(|_| rng1.u64(1000)).collect();
        let seq2: Vec<_> = (0..10).map(|_| rng2.u64(1000)).collect();

        assert_ne!(seq1, seq2);
    }

    #[test]
    fn jitter_within_bounds() {
        let mut rng = SimRng::new(42);
        let max_jitter = Duration::from_millis(10);

        for _ in 0..1000 {
            let jitter = rng.jitter(max_jitter);
            assert!(jitter >= -10_000_000);
            assert!(jitter <= 10_000_000);
        }
    }

    #[test]
    fn duration_with_jitter_non_negative() {
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(5);
        let max_jitter = Duration::from_millis(10);

        for _ in 0..1000 {
            let result = rng.duration_with_jitter(base, max_jitter);
            // `as_nanos()` returns u64 so non-negativity is implicit; this
            // test is really checking the call does not panic.
            let _ = result.as_nanos();
        }
    }

    #[test]
    fn choice_selects_from_slice() {
        let mut rng = SimRng::new(42);
        let items = [1, 2, 3, 4, 5];

        let mut seen = [false; 5];
        for _ in 0..1000 {
            let choice = *rng.choice(&items);
            seen[choice - 1] = true;
        }

        assert!(
            seen.iter().all(|&x| x),
            "should see all elements eventually"
        );
    }

    #[test]
    fn shuffle_is_deterministic() {
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(42);

        let mut items1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut items2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        rng1.shuffle(&mut items1);
        rng2.shuffle(&mut items2);

        assert_eq!(items1, items2);
    }

    #[test]
    fn fork_produces_independent_streams() {
        let mut rng = SimRng::new(42);
        let mut child1 = rng.fork();
        let mut child2 = rng.fork();

        // Children should produce different sequences from each other
        let seq1: Vec<_> = (0..10).map(|_| child1.u64(1000)).collect();
        let seq2: Vec<_> = (0..10).map(|_| child2.u64(1000)).collect();

        assert_ne!(seq1, seq2);
    }

    #[test]
    fn fork_is_deterministic() {
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(42);

        let mut child1 = rng1.fork();
        let mut child2 = rng2.fork();

        let seq1: Vec<_> = (0..10).map(|_| child1.u64(1000)).collect();
        let seq2: Vec<_> = (0..10).map(|_| child2.u64(1000)).collect();

        assert_eq!(seq1, seq2);
    }

    /// Cross-version RNG-stream guardrail.
    ///
    /// Every value Simulacra derives from a seed flows through the
    /// `rand`/`rand_chacha` distribution layer (`random_range`, `random`,
    /// `random_bool`, `fill`). `ChaCha8`'s raw bytes are algorithmically
    /// stable, but that distribution layer is reworked at `rand`
    /// minor/major boundaries — so a dependency bump can silently change
    /// what every seed produces (jitter, packet loss, `choice`, `shuffle`,
    /// forked sub-streams). Nothing else in the suite pins this: the
    /// integration determinism test is only self-consistent within one
    /// binary.
    ///
    /// These literals are the stream for seed 42. If a `rand`/`rand_chacha`
    /// upgrade makes this test fail, that is a **deliberate, reviewable**
    /// stream change: confirm the new values are sane (in range,
    /// deterministic, distinct), re-bless the literals in the same commit,
    /// and note it in `CHANGELOG.md`. Pinned on `rand` 0.10 / `rand_chacha`
    /// 0.10.
    #[test]
    fn golden_stream_is_stable_across_dep_versions() {
        let mut rng = SimRng::new(42);

        let u: Vec<u64> = (0..8).map(|_| rng.u64(1_000_000)).collect();
        assert_eq!(
            u,
            [
                681896, 950275, 427516, 627360, 288593, 149958, 308040, 803872
            ],
            "u64 range stream (random_range over integers) changed"
        );

        let f: Vec<u64> = (0..4).map(|_| rng.f64().to_bits()).collect();
        assert_eq!(
            f,
            [
                4605122010988943809,
                4597763960352646552,
                4602740670442056384,
                4606297940404477195
            ],
            "f64 stream (random::<f64>() bit pattern) changed"
        );

        let b: u32 = (0..16).fold(0u32, |acc, i| acc | ((rng.bool(0.5) as u32) << i));
        assert_eq!(b, 0x3be8, "bool(0.5) stream (random_bool) changed");

        let j: Vec<i64> = (0..4)
            .map(|_| rng.jitter(Duration::from_millis(10)))
            .collect();
        assert_eq!(
            j,
            [3440136, 3642442, 3485106, -8003166],
            "signed jitter stream (random_range over a signed range) changed"
        );

        let mut sh = [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        rng.shuffle(&mut sh);
        assert_eq!(
            sh,
            [4, 7, 9, 5, 3, 0, 8, 6, 1, 2],
            "Fisher-Yates shuffle order changed"
        );

        // fork() derives the child seed via `fill()` — the surface most
        // likely to shift on a rand major bump.
        let mut child = SimRng::new(42).fork();
        let c: Vec<u64> = (0..4).map(|_| child.u64(1_000_000)).collect();
        assert_eq!(
            c,
            [999698, 281675, 181238, 337832],
            "forked sub-stream changed (fill()-based seed derivation)"
        );
    }

    #[test]
    fn bool_respects_probability() {
        let mut rng = SimRng::new(42);

        // With probability 0, should never be true
        for _ in 0..100 {
            assert!(!rng.bool(0.0));
        }

        // With probability 1, should always be true
        for _ in 0..100 {
            assert!(rng.bool(1.0));
        }

        // With probability 0.5, should be roughly half true
        let count = (0..10000).filter(|_| rng.bool(0.5)).count();
        assert!(
            count > 4500 && count < 5500,
            "expected ~5000, got {}",
            count
        );
    }
}
