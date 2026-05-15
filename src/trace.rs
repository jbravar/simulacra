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

/// Current serialized trace-schema version.
///
/// Embedded in every JSON trace. **Bump this on any change to the
/// serialized shape** — the [`Trace`] envelope *or* the wire shape of a
/// known event type. [`Trace::from_json`] / [`Trace::read_json`] reject
/// traces whose version differs, so a bump is a deliberate, reviewable
/// break (same discipline as the RNG-stream golden test).
pub const TRACE_SCHEMA_VERSION: u32 = 1;

/// Format tag written into every serialized trace so foreign or
/// hand-rolled JSON is rejected with a clear error instead of being
/// silently mis-parsed into a structurally-similar `Trace`.
#[cfg(feature = "serde")]
const TRACE_FORMAT: &str = "simulacra-trace";

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

/// Borrowed view serialized in place of `Trace`, so every emitted trace
/// carries the format tag + schema version without those constants
/// polluting the in-memory type or `compare()`.
#[cfg(feature = "serde")]
#[derive(serde::Serialize)]
struct TraceEnvelopeRef<'a, E> {
    format: &'static str,
    schema_version: u32,
    seed: u64,
    events: &'a [TraceEvent<E>],
    final_time_ns: u64,
}

/// Owned shape parsed on the way in, before validation. `format` /
/// `schema_version` default so a foreign document yields a clear
/// `NotASimulacraTrace` / `SchemaVersion` rather than a raw serde error.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct TraceEnvelopeOwned<E> {
    #[serde(default)]
    format: String,
    #[serde(default)]
    schema_version: u32,
    seed: u64,
    events: Vec<TraceEvent<E>>,
    final_time_ns: u64,
}

/// Error returned when a trace fails to load.
///
/// Loading is strict: a foreign document or a different schema version is
/// a hard error, never a silent best-effort parse.
#[cfg(feature = "serde")]
#[derive(Debug)]
pub enum TraceLoadError {
    /// Not valid JSON, or did not match the trace shape.
    Json(serde_json::Error),
    /// I/O failure reading the trace file.
    Io(std::io::Error),
    /// Valid JSON, but not a Simulacra trace (missing/wrong format tag).
    NotASimulacraTrace,
    /// A Simulacra trace, but written by an incompatible schema version.
    SchemaVersion { expected: u32, found: u32 },
}

#[cfg(feature = "serde")]
impl fmt::Display for TraceLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceLoadError::Json(e) => write!(f, "invalid trace JSON: {e}"),
            TraceLoadError::Io(e) => write!(f, "could not read trace file: {e}"),
            TraceLoadError::NotASimulacraTrace => write!(
                f,
                "not a Simulacra trace (missing or wrong \"{TRACE_FORMAT}\" format tag)"
            ),
            TraceLoadError::SchemaVersion { expected, found } => write!(
                f,
                "unsupported trace schema version: file is v{found}, \
                 this build expects v{expected}"
            ),
        }
    }
}

#[cfg(feature = "serde")]
impl std::error::Error for TraceLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TraceLoadError::Json(e) => Some(e),
            TraceLoadError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "serde")]
impl From<serde_json::Error> for TraceLoadError {
    fn from(e: serde_json::Error) -> Self {
        TraceLoadError::Json(e)
    }
}

#[cfg(feature = "serde")]
impl From<std::io::Error> for TraceLoadError {
    fn from(e: std::io::Error) -> Self {
        TraceLoadError::Io(e)
    }
}

#[cfg(feature = "serde")]
impl<E: serde::Serialize> Trace<E> {
    fn envelope(&self) -> TraceEnvelopeRef<'_, E> {
        TraceEnvelopeRef {
            format: TRACE_FORMAT,
            schema_version: TRACE_SCHEMA_VERSION,
            seed: self.seed,
            events: &self.events,
            final_time_ns: self.final_time_ns,
        }
    }

    /// Exports the trace to a JSON string (format-tagged + versioned).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.envelope())
    }

    /// Exports the trace to a pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.envelope())
    }

    /// Writes the trace to a JSON file.
    pub fn write_json(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer(file, &self.envelope())?;
        Ok(())
    }

    /// Writes the trace to a pretty-printed JSON file.
    pub fn write_json_pretty(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, &self.envelope())?;
        Ok(())
    }
}

#[cfg(feature = "serde")]
impl<E: serde::de::DeserializeOwned> Trace<E> {
    fn from_envelope(env: TraceEnvelopeOwned<E>) -> Result<Self, TraceLoadError> {
        if env.format != TRACE_FORMAT {
            return Err(TraceLoadError::NotASimulacraTrace);
        }
        if env.schema_version != TRACE_SCHEMA_VERSION {
            return Err(TraceLoadError::SchemaVersion {
                expected: TRACE_SCHEMA_VERSION,
                found: env.schema_version,
            });
        }
        Ok(Trace {
            seed: env.seed,
            events: env.events,
            final_time_ns: env.final_time_ns,
        })
    }

    /// Parses a trace from a JSON string.
    ///
    /// Strict: rejects non-Simulacra JSON
    /// ([`TraceLoadError::NotASimulacraTrace`]) and traces from a
    /// different [`TRACE_SCHEMA_VERSION`]
    /// ([`TraceLoadError::SchemaVersion`]) rather than mis-parsing them.
    pub fn from_json(json: &str) -> Result<Self, TraceLoadError> {
        Self::from_envelope(serde_json::from_str(json)?)
    }

    /// Reads a trace from a JSON file, with the same strict validation as
    /// [`Trace::from_json`].
    pub fn read_json(path: impl AsRef<std::path::Path>) -> Result<Self, TraceLoadError> {
        let file = std::fs::File::open(path)?;
        Self::from_envelope(serde_json::from_reader(file)?)
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

    #[cfg(feature = "serde")]
    #[test]
    fn json_carries_format_tag_and_version() {
        let mut trace = Trace::new(7);
        trace.record(Time::from_millis(1), TestEvent::A(1));
        let json = trace.to_json().unwrap();

        assert!(json.contains(r#""format":"simulacra-trace""#));
        assert!(json.contains(&format!(r#""schema_version":{TRACE_SCHEMA_VERSION}"#)));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn from_json_rejects_foreign_document() {
        // Structurally trace-like, but no format tag.
        let foreign = r#"{"seed":1,"events":[],"final_time_ns":0}"#;
        let err = Trace::<TestEvent>::from_json(foreign).unwrap_err();
        assert!(matches!(err, TraceLoadError::NotASimulacraTrace));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn from_json_rejects_other_schema_version() {
        let future = format!(
            r#"{{"format":"simulacra-trace","schema_version":{},"seed":1,"events":[],"final_time_ns":0}}"#,
            TRACE_SCHEMA_VERSION + 1
        );
        let err = Trace::<TestEvent>::from_json(&future).unwrap_err();
        match err {
            TraceLoadError::SchemaVersion { expected, found } => {
                assert_eq!(expected, TRACE_SCHEMA_VERSION);
                assert_eq!(found, TRACE_SCHEMA_VERSION + 1);
            }
            other => panic!("expected SchemaVersion, got {other:?}"),
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn from_json_rejects_garbage() {
        let err = Trace::<TestEvent>::from_json("not json at all").unwrap_err();
        assert!(matches!(err, TraceLoadError::Json(_)));
    }
}
