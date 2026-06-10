//! Simulacra: A deterministic discrete-event simulation engine.
//!
//! Simulacra is designed for modeling message flow across large computer networks
//! with pluggable latency, jitter, and failure models.
//!
//! # Core Concepts
//!
//! - **Time**: Simulated time advances by events, not wall clock time
//! - **Events**: The atomic units of causality, ordered by time and sequence
//! - **Determinism**: Given the same seed and inputs, simulations produce identical results
//!
//! # Example
//!
//! ```
//! use simulacra::{Simulation, Time, Duration};
//!
//! // Define your event type
//! #[derive(Debug)]
//! enum Event {
//!     Tick(u32),
//! }
//!
//! // Create and run a simulation
//! let mut sim = Simulation::new();
//! sim.schedule(Time::from_millis(100), Event::Tick(1));
//! sim.schedule(Time::from_millis(200), Event::Tick(2));
//!
//! let stats = sim.run(|sim, event| {
//!     println!("At {}: {:?}", sim.now(), event);
//! });
//!
//! println!("Processed {} events", stats.events_processed);
//! ```
//!
//! # Deterministic Random Numbers
//!
//! ```
//! use simulacra::SimRng;
//!
//! let mut rng = SimRng::new(42);
//!
//! // Same seed produces identical sequences
//! let mut rng2 = SimRng::new(42);
//! assert_eq!(rng.u64(100), rng2.u64(100));
//! ```

pub mod net;
pub mod parallel;
mod queue;
mod rng;
pub mod scenario;
mod sim;
pub mod task;
mod time;
pub mod trace;

pub use queue::{EventQueue, Scheduled};
pub use rng::SimRng;
pub use scenario::Scenario;
pub use sim::{Simulation, SimulationStats};
pub use time::{Duration, Time};
#[cfg(feature = "serde")]
pub use trace::TraceLoadError;
pub use trace::{
    ReplayError, TRACE_SCHEMA_VERSION, Trace, TraceEvent, TraceMismatch, TraceRecorder,
};

// Re-export commonly used network types at crate root for convenience
pub use net::{
    DropPolicy, DropReason, FixedLatency, LatencyModel, Message, MessageId, NetConfig, NetEvent,
    NetTraceDropReason, NetTraceEvent, Network, NetworkStats, NodeId, OverheadPlusJitter,
    PercentageJitter, Route, RunContext, SpikyLatency, Topology, TopologyBuilder, TracedNetwork,
    UniformJitter,
};

// Re-export task types
pub use task::{
    CancellationToken, Either, Envelope, FailureAction, NodeContext, Recv, RecvTimeout, Select2,
    SendFut, Sleep, TaskId, TaskSim, TaskSimBuilder, TaskSimStats, TaskTraceDropReason,
    TaskTraceEvent, select2,
};
