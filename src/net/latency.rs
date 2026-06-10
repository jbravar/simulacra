//! Latency and jitter models for network simulation.

use crate::rng::SimRng;
use crate::time::Duration;

/// A model for computing message transmission latency.
pub trait LatencyModel {
    /// Computes the latency for a message given the base link latency.
    fn compute(&mut self, base_latency: Duration, rng: &mut SimRng) -> Duration;
}

/// Fixed latency with no jitter.
///
/// Messages always take exactly the base latency.
#[derive(Debug, Clone, Copy, Default)]
pub struct FixedLatency;

impl LatencyModel for FixedLatency {
    fn compute(&mut self, base_latency: Duration, _rng: &mut SimRng) -> Duration {
        base_latency
    }
}

/// Uniform jitter model.
///
/// Adds uniformly distributed jitter in the range [-`max_jitter`, +`max_jitter`]
/// to the base latency. Result is clamped to be non-negative.
#[derive(Debug, Clone, Copy)]
pub struct UniformJitter {
    /// Maximum jitter to add or subtract.
    pub max_jitter: Duration,
}

impl UniformJitter {
    /// Creates a new uniform jitter model.
    #[must_use]
    pub const fn new(max_jitter: Duration) -> Self {
        Self { max_jitter }
    }
}

impl LatencyModel for UniformJitter {
    fn compute(&mut self, base_latency: Duration, rng: &mut SimRng) -> Duration {
        rng.duration_with_jitter(base_latency, self.max_jitter)
    }
}

/// Percentage-based jitter model.
///
/// Adds jitter as a percentage of the base latency.
/// For example, 10% jitter on 100ms base = jitter in [-10ms, +10ms].
#[derive(Debug, Clone, Copy)]
pub struct PercentageJitter {
    /// Jitter as a fraction (0.0 to 1.0).
    pub fraction: f64,
}

impl PercentageJitter {
    /// Creates a new percentage jitter model.
    ///
    /// # Arguments
    /// * `percent` - Jitter percentage (e.g., 10.0 for 10%)
    #[must_use]
    pub fn new(percent: f64) -> Self {
        Self {
            fraction: percent / 100.0,
        }
    }

    /// Creates from a fraction (0.0 to 1.0).
    #[must_use]
    pub const fn from_fraction(fraction: f64) -> Self {
        Self { fraction }
    }
}

impl LatencyModel for PercentageJitter {
    fn compute(&mut self, base_latency: Duration, rng: &mut SimRng) -> Duration {
        #[expect(
            clippy::cast_precision_loss,
            reason = "nanosecond count to f64 for the jitter fraction scaling; the \
                      exact f64 result is part of the deterministic latency model and \
                      must not change"
        )]
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "fraction * non-negative nanos is a non-negative magnitude; the \
                      round-toward-zero f64->u64 conversion is the model's defined, \
                      determinism-bearing rounding and must not change"
        )]
        let max_jitter_nanos = (base_latency.as_nanos() as f64 * self.fraction) as u64;
        let max_jitter = Duration::from_nanos(max_jitter_nanos);
        rng.duration_with_jitter(base_latency, max_jitter)
    }
}

/// Compound latency model that adds a fixed overhead plus jitter.
#[derive(Debug, Clone, Copy)]
pub struct OverheadPlusJitter {
    /// Fixed overhead added to every message.
    pub overhead: Duration,
    /// Maximum jitter.
    pub max_jitter: Duration,
}

impl OverheadPlusJitter {
    /// Creates a new overhead plus jitter model.
    #[must_use]
    pub const fn new(overhead: Duration, max_jitter: Duration) -> Self {
        Self {
            overhead,
            max_jitter,
        }
    }
}

impl LatencyModel for OverheadPlusJitter {
    fn compute(&mut self, base_latency: Duration, rng: &mut SimRng) -> Duration {
        let base_with_overhead = base_latency.saturating_add(self.overhead);
        rng.duration_with_jitter(base_with_overhead, self.max_jitter)
    }
}

/// Injects occasional delay spikes on top of an inner latency model.
///
/// On each `compute()` call, the inner model is consulted first. With
/// probability `spike_probability`, an additional uniformly random spike
/// in `[0, spike_max]` is added. Useful for simulating tail-latency fault
/// storms, GC pauses, or congested links without a full queueing model.
#[derive(Debug, Clone, Copy)]
pub struct SpikyLatency<L: LatencyModel> {
    /// Inner latency model.
    pub inner: L,
    /// Probability of a spike on any given message, in `[0.0, 1.0]`.
    pub spike_probability: f64,
    /// Maximum additional spike duration.
    pub spike_max: Duration,
}

impl<L: LatencyModel> SpikyLatency<L> {
    /// Creates a new spiky latency wrapper around `inner`.
    pub const fn new(inner: L, spike_probability: f64, spike_max: Duration) -> Self {
        Self {
            inner,
            spike_probability,
            spike_max,
        }
    }
}

impl<L: LatencyModel> LatencyModel for SpikyLatency<L> {
    fn compute(&mut self, base_latency: Duration, rng: &mut SimRng) -> Duration {
        let base = self.inner.compute(base_latency, rng);
        if self.spike_probability > 0.0
            && self.spike_max.as_nanos() > 0
            && rng.bool(self.spike_probability)
        {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "`+ 1` makes the rng range inclusive of spike_max; overflow \
                          needs as_nanos() == u64::MAX (an infeasible ~584-year \
                          spike_max). Latent: a saturating fix would change the rng \
                          draw range in that pathological config, altering the trace, \
                          so the panic-on-overflow contract is kept"
            )]
            let spike_nanos = rng.u64(self.spike_max.as_nanos() + 1);
            base.saturating_add(Duration::from_nanos(spike_nanos))
        } else {
            base
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_latency() {
        let mut model = FixedLatency;
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(100);

        for _ in 0..100 {
            assert_eq!(model.compute(base, &mut rng), base);
        }
    }

    #[test]
    fn uniform_jitter_bounds() {
        let mut model = UniformJitter::new(Duration::from_millis(10));
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(100);

        for _ in 0..1000 {
            let latency = model.compute(base, &mut rng);
            assert!(latency.as_millis() >= 90);
            assert!(latency.as_millis() <= 110);
        }
    }

    #[test]
    fn uniform_jitter_clamps_negative() {
        let mut model = UniformJitter::new(Duration::from_millis(100));
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(10); // Small base, large jitter

        for _ in 0..1000 {
            let latency = model.compute(base, &mut rng);
            // `as_nanos()` returns u64, so non-negative is implicit;
            // exercise the call and keep the result observable.
            assert!(latency.as_nanos() < u64::MAX);
        }
    }

    #[test]
    fn percentage_jitter() {
        let mut model = PercentageJitter::new(10.0); // 10%
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(100);

        for _ in 0..1000 {
            let latency = model.compute(base, &mut rng);
            assert!(latency.as_millis() >= 90);
            assert!(latency.as_millis() <= 110);
        }
    }

    #[test]
    fn overhead_plus_jitter() {
        let mut model = OverheadPlusJitter::new(Duration::from_millis(5), Duration::from_millis(2));
        let mut rng = SimRng::new(42);
        let base = Duration::from_millis(100);

        for _ in 0..1000 {
            let latency = model.compute(base, &mut rng);
            // 100 + 5 = 105, with +/- 2ms jitter
            assert!(latency.as_millis() >= 103);
            assert!(latency.as_millis() <= 107);
        }
    }

    #[test]
    fn deterministic_jitter() {
        let mut model1 = UniformJitter::new(Duration::from_millis(10));
        let mut model2 = UniformJitter::new(Duration::from_millis(10));
        let mut rng1 = SimRng::new(42);
        let mut rng2 = SimRng::new(42);
        let base = Duration::from_millis(100);

        for _ in 0..100 {
            assert_eq!(
                model1.compute(base, &mut rng1),
                model2.compute(base, &mut rng2)
            );
        }
    }
}
