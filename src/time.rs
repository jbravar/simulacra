//! Simulated time representation.
//!
//! Time in Simulacra is represented as integer ticks, providing deterministic
//! behavior without floating-point precision issues.

// The `checked_*(...).expect("… overflow")` pattern below *is* the documented
// panic-on-overflow determinism contract (DESIGN.md / README) — the kernel's
// time arithmetic deliberately panics rather than wrap. This is the same
// design-contract carve-out applied to `arithmetic_side_effects` in the other
// kernel modules; here the mechanism is `expect` on `checked_*`.
#![expect(
    clippy::expect_used,
    reason = "documented panic-on-overflow determinism contract; see DESIGN.md"
)]

use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};

/// A point in simulated time, represented as nanoseconds from simulation start.
///
/// Using nanoseconds as the base unit provides sufficient precision for most
/// network simulations while keeping the representation simple.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time(u64);

impl Time {
    /// The zero point of simulated time (simulation start).
    pub const ZERO: Self = Self(0);

    /// The maximum representable time.
    pub const MAX: Self = Self(u64::MAX);

    /// Creates a new `Time` from raw nanoseconds.
    #[inline]
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Creates a new `Time` from microseconds.
    #[inline]
    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros.saturating_mul(1_000))
    }

    /// Creates a new `Time` from milliseconds.
    #[inline]
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    /// Creates a new `Time` from seconds.
    #[inline]
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// Returns the time as raw nanoseconds.
    #[inline]
    #[must_use]
    pub const fn as_nanos(&self) -> u64 {
        self.0
    }

    /// Returns the time as microseconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_micros(&self) -> u64 {
        self.0 / 1_000
    }

    /// Returns the time as milliseconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_millis(&self) -> u64 {
        self.0 / 1_000_000
    }

    /// Returns the time as seconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_secs(&self) -> u64 {
        self.0 / 1_000_000_000
    }

    /// Returns the duration since `earlier`, or `Duration::ZERO` if `earlier` is after `self`.
    #[inline]
    #[must_use]
    pub const fn saturating_duration_since(&self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Adds a duration to this time, saturating at `Time::MAX`.
    #[inline]
    #[must_use]
    pub const fn saturating_add(&self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration.as_nanos()))
    }

    /// Subtracts a duration from this time, saturating at `Time::ZERO`.
    #[inline]
    #[must_use]
    pub const fn saturating_sub(&self, duration: Duration) -> Self {
        Self(self.0.saturating_sub(duration.as_nanos()))
    }
}

impl Add<Duration> for Time {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Duration) -> Self {
        Self(self.0.checked_add(rhs.as_nanos()).expect("time overflow"))
    }
}

impl AddAssign<Duration> for Time {
    #[inline]
    fn add_assign(&mut self, rhs: Duration) {
        self.0 = self.0.checked_add(rhs.as_nanos()).expect("time overflow");
    }
}

impl Sub<Duration> for Time {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0.checked_sub(rhs.as_nanos()).expect("time underflow"))
    }
}

impl SubAssign<Duration> for Time {
    #[inline]
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 = self.0.checked_sub(rhs.as_nanos()).expect("time underflow");
    }
}

impl Sub<Self> for Time {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: Self) -> Duration {
        Duration::from_nanos(self.0.checked_sub(rhs.0).expect("time underflow"))
    }
}

impl fmt::Debug for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Time({}ns)", self.0)
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= 1_000_000_000 {
            write!(
                f,
                "{}.{:09}s",
                self.0 / 1_000_000_000,
                self.0 % 1_000_000_000
            )
        } else if self.0 >= 1_000_000 {
            write!(f, "{}.{:06}ms", self.0 / 1_000_000, self.0 % 1_000_000)
        } else if self.0 >= 1_000 {
            write!(f, "{}.{:03}µs", self.0 / 1_000, self.0 % 1_000)
        } else {
            write!(f, "{}ns", self.0)
        }
    }
}

/// A duration of simulated time.
///
/// Like `Time`, this is represented as nanoseconds for precision and simplicity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(u64);

impl Duration {
    /// A zero-length duration.
    pub const ZERO: Self = Self(0);

    /// The maximum representable duration.
    pub const MAX: Self = Self(u64::MAX);

    /// Creates a new `Duration` from raw nanoseconds.
    #[inline]
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Creates a new `Duration` from microseconds.
    #[inline]
    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros.saturating_mul(1_000))
    }

    /// Creates a new `Duration` from milliseconds.
    #[inline]
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    /// Creates a new `Duration` from seconds.
    #[inline]
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// Returns the duration as raw nanoseconds.
    #[inline]
    #[must_use]
    pub const fn as_nanos(&self) -> u64 {
        self.0
    }

    /// Returns the duration as microseconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_micros(&self) -> u64 {
        self.0 / 1_000
    }

    /// Returns the duration as milliseconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_millis(&self) -> u64 {
        self.0 / 1_000_000
    }

    /// Returns the duration as seconds (truncated).
    #[inline]
    #[must_use]
    pub const fn as_secs(&self) -> u64 {
        self.0 / 1_000_000_000
    }

    /// Returns true if this duration is zero.
    #[inline]
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.0 == 0
    }

    /// Adds two durations, saturating at `Duration::MAX`.
    #[inline]
    #[must_use]
    pub const fn saturating_add(&self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    /// Subtracts a duration, saturating at `Duration::ZERO`.
    #[inline]
    #[must_use]
    pub const fn saturating_sub(&self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    /// Multiplies the duration by a scalar, saturating at `Duration::MAX`.
    #[inline]
    #[must_use]
    pub const fn saturating_mul(&self, n: u64) -> Self {
        Self(self.0.saturating_mul(n))
    }
}

impl Add for Duration {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self {
        Self(self.0.checked_add(rhs.0).expect("duration overflow"))
    }
}

impl AddAssign for Duration {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.checked_add(rhs.0).expect("duration overflow");
    }
}

impl Sub for Duration {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self {
        Self(self.0.checked_sub(rhs.0).expect("duration underflow"))
    }
}

impl SubAssign for Duration {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = self.0.checked_sub(rhs.0).expect("duration underflow");
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Duration({}ns)", self.0)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= 1_000_000_000 {
            write!(
                f,
                "{}.{:09}s",
                self.0 / 1_000_000_000,
                self.0 % 1_000_000_000
            )
        } else if self.0 >= 1_000_000 {
            write!(f, "{}.{:06}ms", self.0 / 1_000_000, self.0 % 1_000_000)
        } else if self.0 >= 1_000 {
            write!(f, "{}.{:03}µs", self.0 / 1_000, self.0 % 1_000)
        } else {
            write!(f, "{}ns", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_arithmetic() {
        let t1 = Time::from_millis(100);
        let t2 = Time::from_millis(200);
        let d = Duration::from_millis(50);

        assert_eq!(t1 + d, Time::from_millis(150));
        assert_eq!(t2 - d, Time::from_millis(150));
        assert_eq!(t2 - t1, Duration::from_millis(100));
    }

    #[test]
    fn time_ordering() {
        let times = [
            Time::from_nanos(100),
            Time::from_nanos(50),
            Time::from_nanos(200),
        ];
        let mut sorted = times;
        sorted.sort();
        assert_eq!(
            sorted,
            [
                Time::from_nanos(50),
                Time::from_nanos(100),
                Time::from_nanos(200),
            ]
        );
    }

    #[test]
    fn duration_arithmetic() {
        let d1 = Duration::from_millis(100);
        let d2 = Duration::from_millis(50);

        assert_eq!(d1 + d2, Duration::from_millis(150));
        assert_eq!(d1 - d2, Duration::from_millis(50));
    }

    #[test]
    fn saturating_operations() {
        assert_eq!(Time::MAX.saturating_add(Duration::from_nanos(1)), Time::MAX);
        assert_eq!(
            Time::ZERO.saturating_sub(Duration::from_nanos(1)),
            Time::ZERO
        );
        assert_eq!(
            Duration::MAX.saturating_add(Duration::from_nanos(1)),
            Duration::MAX
        );
    }

    #[test]
    fn unit_conversions() {
        let t = Time::from_secs(1);
        assert_eq!(t.as_millis(), 1_000);
        assert_eq!(t.as_micros(), 1_000_000);
        assert_eq!(t.as_nanos(), 1_000_000_000);

        let d = Duration::from_millis(1_500);
        assert_eq!(d.as_secs(), 1);
        assert_eq!(d.as_millis(), 1_500);
    }
}
