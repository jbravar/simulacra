//! Event tracing and replay for simulation observability.
//!
//! This module provides tools for recording simulation events, exporting traces,
//! and validating deterministic replay.
//!
//! # JSON Export
//!
//! Enable the `serde` feature to get JSON import/export:
//!
//! ```toml
//! simulacra = { version = "0.1", features = ["serde"] }
//! ```

use std::fmt;

use crate::time::Time;

/// A recorded event in a trace.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TraceEvent<E> {
    /// Simulated time when the event occurred (nanoseconds).
    pub time_ns: u64,
    /// Sequence number (order of processing).
    pub sequence: u64,
    /// The event data.
    pub event: E,
}

impl<E> TraceEvent<E> {
    /// Creates a new trace event.
    pub fn new(time: Time, sequence: u64, event: E) -> Self {
        TraceEvent {
            time_ns: time.as_nanos(),
            sequence,
            event,
        }
    }

    /// Returns the time as a `Time` value.
    pub fn time(&self) -> Time {
        Time::from_nanos(self.time_ns)
    }
}

/// A complete trace of a simulation run.
///
/// Captures all events in processing order, along with metadata about the run.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Trace<E> {
    /// Seed used for this simulation run.
    pub seed: u64,
    /// All events in processing order.
    events: Vec<TraceEvent<E>>,
    /// Final simulation time (nanoseconds).
    pub final_time_ns: u64,
}

impl<E> Trace<E> {
    /// Creates a new empty trace with the given seed.
    pub fn new(seed: u64) -> Self {
        Trace {
            seed,
            events: Vec::new(),
            final_time_ns: 0,
        }
    }

    /// Creates a trace with pre-allocated capacity.
    pub fn with_capacity(seed: u64, capacity: usize) -> Self {
        Trace {
            seed,
            events: Vec::with_capacity(capacity),
            final_time_ns: 0,
        }
    }

    /// Records an event.
    pub fn record(&mut self, time: Time, event: E) {
        let sequence = self.events.len() as u64;
        self.events.push(TraceEvent::new(time, sequence, event));
        self.final_time_ns = time.as_nanos();
    }

    /// Returns the final simulation time.
    pub fn final_time(&self) -> Time {
        Time::from_nanos(self.final_time_ns)
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns true if no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns an iterator over all trace events.
    pub fn events(&self) -> impl Iterator<Item = &TraceEvent<E>> {
        self.events.iter()
    }

    /// Returns the event at the given index.
    pub fn get(&self, index: usize) -> Option<&TraceEvent<E>> {
        self.events.get(index)
    }

    /// Consumes the trace and returns the events.
    pub fn into_events(self) -> Vec<TraceEvent<E>> {
        self.events
    }
}

#[cfg(feature = "serde")]
impl<E: serde::Serialize> Trace<E> {
    /// Exports the trace to a JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Exports the trace to a pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Writes the trace to a JSON file.
    pub fn write_json(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer(file, self)?;
        Ok(())
    }

    /// Writes the trace to a pretty-printed JSON file.
    pub fn write_json_pretty(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}

#[cfg(feature = "serde")]
impl<E: serde::de::DeserializeOwned> Trace<E> {
    /// Parses a trace from a JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Reads a trace from a JSON file.
    pub fn read_json(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let trace = serde_json::from_reader(file)?;
        Ok(trace)
    }
}

impl<E: PartialEq> Trace<E> {
    /// Compares two traces for equality.
    ///
    /// Returns `Ok(())` if traces match, or `Err` with details about the first mismatch.
    pub fn compare(&self, other: &Trace<E>) -> Result<(), TraceMismatch> {
        if self.seed != other.seed {
            return Err(TraceMismatch::SeedMismatch {
                expected: self.seed,
                actual: other.seed,
            });
        }

        if self.events.len() != other.events.len() {
            return Err(TraceMismatch::LengthMismatch {
                expected: self.events.len(),
                actual: other.events.len(),
            });
        }

        for (i, (a, b)) in self.events.iter().zip(other.events.iter()).enumerate() {
            if a.time_ns != b.time_ns {
                return Err(TraceMismatch::TimeMismatch {
                    index: i,
                    expected: Time::from_nanos(a.time_ns),
                    actual: Time::from_nanos(b.time_ns),
                });
            }
            if a.event != b.event {
                return Err(TraceMismatch::EventMismatch { index: i });
            }
        }

        Ok(())
    }

    /// Validates that this trace matches a replay.
    ///
    /// This is a convenience wrapper around `compare()` that provides
    /// a more descriptive error message for replay validation.
    pub fn validate_replay(&self, replay: &Trace<E>) -> Result<(), ReplayError> {
        self.compare(replay).map_err(|mismatch| ReplayError {
            seed: self.seed,
            mismatch,
        })
    }
}

/// Describes a mismatch between two traces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceMismatch {
    /// Seeds don't match.
    SeedMismatch { expected: u64, actual: u64 },
    /// Different number of events.
    LengthMismatch { expected: usize, actual: usize },
    /// Event times differ at given index.
    TimeMismatch {
        index: usize,
        expected: Time,
        actual: Time,
    },
    /// Event data differs at given index.
    EventMismatch { index: usize },
}

impl fmt::Display for TraceMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceMismatch::SeedMismatch { expected, actual } => {
                write!(f, "seed mismatch: expected {}, got {}", expected, actual)
            }
            TraceMismatch::LengthMismatch { expected, actual } => {
                write!(
                    f,
                    "length mismatch: expected {} events, got {}",
                    expected, actual
                )
            }
            TraceMismatch::TimeMismatch {
                index,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "time mismatch at event {}: expected {}, got {}",
                    index, expected, actual
                )
            }
            TraceMismatch::EventMismatch { index } => {
                write!(f, "event data mismatch at index {}", index)
            }
        }
    }
}

impl std::error::Error for TraceMismatch {}

/// Error returned when replay validation fails.
#[derive(Debug, Clone)]
pub struct ReplayError {
    /// The seed that was used.
    pub seed: u64,
    /// The mismatch that was detected.
    pub mismatch: TraceMismatch,
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "replay validation failed (seed {}): {}",
            self.seed, self.mismatch
        )
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.mismatch)
    }
}

/// A recorder that captures events during simulation.
///
/// Use this with `Simulation::run()` to build a trace.
#[derive(Debug)]
pub struct TraceRecorder<E> {
    trace: Trace<E>,
}

impl<E> TraceRecorder<E> {
    /// Creates a new trace recorder.
    pub fn new(seed: u64) -> Self {
        TraceRecorder {
            trace: Trace::new(seed),
        }
    }

    /// Creates a recorder with pre-allocated capacity.
    pub fn with_capacity(seed: u64, capacity: usize) -> Self {
        TraceRecorder {
            trace: Trace::with_capacity(seed, capacity),
        }
    }

    /// Records an event at the given time.
    pub fn record(&mut self, time: Time, event: E) {
        self.trace.record(time, event);
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.trace.len()
    }

    /// Returns true if no events recorded.
    pub fn is_empty(&self) -> bool {
        self.trace.is_empty()
    }

    /// Finishes recording and returns the trace.
    pub fn finish(self) -> Trace<E> {
        self.trace
    }
}

impl<E: Clone> TraceRecorder<E> {
    /// Records an event, cloning it for the trace.
    ///
    /// Useful when you need to keep the original event.
    pub fn record_clone(&mut self, time: Time, event: &E) {
        self.trace.record(time, event.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    enum TestEvent {
        A(u32),
        B(String),
    }

    #[test]
    fn basic_recording() {
        let mut recorder = TraceRecorder::new(42);
        recorder.record(Time::from_millis(100), TestEvent::A(1));
        recorder.record(Time::from_millis(200), TestEvent::B("hello".into()));

        let trace = recorder.finish();
        assert_eq!(trace.seed, 42);
        assert_eq!(trace.len(), 2);
        assert_eq!(trace.final_time(), Time::from_millis(200));

        let events: Vec<_> = trace.events().collect();
        assert_eq!(events[0].time(), Time::from_millis(100));
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);
    }

    #[test]
    fn trace_comparison_equal() {
        let mut t1 = Trace::new(42);
        t1.record(Time::from_millis(100), TestEvent::A(1));
        t1.record(Time::from_millis(200), TestEvent::A(2));

        let mut t2 = Trace::new(42);
        t2.record(Time::from_millis(100), TestEvent::A(1));
        t2.record(Time::from_millis(200), TestEvent::A(2));

        assert!(t1.compare(&t2).is_ok());
    }

    #[test]
    fn trace_comparison_seed_mismatch() {
        let t1 = Trace::<TestEvent>::new(42);
        let t2 = Trace::<TestEvent>::new(43);

        let err = t1.compare(&t2).unwrap_err();
        assert!(matches!(err, TraceMismatch::SeedMismatch { .. }));
    }

    #[test]
    fn trace_comparison_length_mismatch() {
        let mut t1 = Trace::new(42);
        t1.record(Time::from_millis(100), TestEvent::A(1));

        let mut t2 = Trace::new(42);
        t2.record(Time::from_millis(100), TestEvent::A(1));
        t2.record(Time::from_millis(200), TestEvent::A(2));

        let err = t1.compare(&t2).unwrap_err();
        assert!(matches!(
            err,
            TraceMismatch::LengthMismatch {
                expected: 1,
                actual: 2
            }
        ));
    }

    #[test]
    fn trace_comparison_time_mismatch() {
        let mut t1 = Trace::new(42);
        t1.record(Time::from_millis(100), TestEvent::A(1));

        let mut t2 = Trace::new(42);
        t2.record(Time::from_millis(150), TestEvent::A(1));

        let err = t1.compare(&t2).unwrap_err();
        assert!(matches!(err, TraceMismatch::TimeMismatch { index: 0, .. }));
    }

    #[test]
    fn trace_comparison_event_mismatch() {
        let mut t1 = Trace::new(42);
        t1.record(Time::from_millis(100), TestEvent::A(1));

        let mut t2 = Trace::new(42);
        t2.record(Time::from_millis(100), TestEvent::A(2));

        let err = t1.compare(&t2).unwrap_err();
        assert!(matches!(err, TraceMismatch::EventMismatch { index: 0 }));
    }

    #[test]
    fn into_events() {
        let mut trace = Trace::new(42);
        trace.record(Time::from_millis(100), TestEvent::A(1));
        trace.record(Time::from_millis(200), TestEvent::A(2));

        let events = trace.into_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, TestEvent::A(1));
        assert_eq!(events[1].event, TestEvent::A(2));
    }

    #[test]
    fn validate_replay() {
        let mut original = Trace::new(42);
        original.record(Time::from_millis(100), TestEvent::A(1));

        let mut replay = Trace::new(42);
        replay.record(Time::from_millis(100), TestEvent::A(1));

        assert!(original.validate_replay(&replay).is_ok());

        let mut bad_replay = Trace::new(42);
        bad_replay.record(Time::from_millis(100), TestEvent::A(2));

        let err = original.validate_replay(&bad_replay).unwrap_err();
        assert_eq!(err.seed, 42);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_roundtrip() {
        let mut trace = Trace::new(42);
        trace.record(Time::from_millis(100), TestEvent::A(1));
        trace.record(Time::from_millis(200), TestEvent::B("hello".into()));

        let json = trace.to_json().unwrap();
        let parsed: Trace<TestEvent> = Trace::from_json(&json).unwrap();

        assert!(trace.compare(&parsed).is_ok());
    }
}
