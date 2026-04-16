//! Traced network simulation support.
//!
//! Provides helpers for recording traces of network simulations.

use crate::net::{DropReason, LatencyModel, NetEvent, Network, NetworkStats, NodeId, Topology};
use crate::time::Time;
use crate::trace::{Trace, TraceRecorder};

/// A simplified trace event for network simulations.
///
/// This captures the essential information about message delivery
/// without requiring the payload to be Clone or Serialize.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NetTraceEvent {
    /// A message was delivered.
    Delivered {
        /// Message ID.
        message_id: u64,
        /// Source node.
        src: u32,
        /// Destination node.
        dst: u32,
    },
    /// A message was dropped.
    Dropped {
        /// Message ID.
        message_id: u64,
        /// Source node.
        src: u32,
        /// Destination node.
        dst: u32,
        /// Reason for dropping.
        reason: NetTraceDropReason,
    },
}

/// Serializable version of DropReason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NetTraceDropReason {
    /// No route to destination.
    NoRoute,
    /// Random packet loss.
    PacketLoss,
    /// Active network partition between source and destination.
    Partitioned,
}

impl From<DropReason> for NetTraceDropReason {
    fn from(reason: DropReason) -> Self {
        match reason {
            DropReason::NoRoute => NetTraceDropReason::NoRoute,
            DropReason::PacketLoss => NetTraceDropReason::PacketLoss,
            DropReason::Partitioned => NetTraceDropReason::Partitioned,
        }
    }
}

/// A network simulation that records a trace.
pub struct TracedNetwork<P, L: LatencyModel> {
    network: Network<P, L>,
    seed: u64,
}

impl<P> TracedNetwork<P, crate::net::FixedLatency> {
    /// Creates a new traced network simulation.
    pub fn new(topology: Topology, seed: u64) -> Self {
        TracedNetwork {
            network: Network::new(topology, seed),
            seed,
        }
    }
}

impl<P, L: LatencyModel> TracedNetwork<P, L> {
    /// Creates a new traced network with a custom latency model.
    pub fn with_latency_model(topology: Topology, latency_model: L, seed: u64) -> Self {
        TracedNetwork {
            network: Network::with_latency_model(topology, latency_model, seed),
            seed,
        }
    }

    /// Installs a partition between `a` and `b`. See [`Network::partition`].
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.network.partition(a, b);
    }

    /// Sets the bandwidth cap for `(src, dst)`. See
    /// [`Network::set_bandwidth`].
    pub fn set_bandwidth(&mut self, src: NodeId, dst: NodeId, bps: u64) {
        self.network.set_bandwidth(src, dst, bps);
    }

    /// Sends a sized message. See [`Network::send_sized`].
    pub fn send_sized(
        &mut self,
        src: NodeId,
        dst: NodeId,
        payload: P,
        size_bytes: u64,
    ) -> Option<crate::net::MessageId> {
        self.network.send_sized(src, dst, payload, size_bytes)
    }

    /// Removes a partition. See [`Network::heal`].
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.network.heal(a, b);
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.network.is_partitioned(a, b)
    }

    /// Returns the current simulated time.
    pub fn now(&self) -> Time {
        self.network.now()
    }

    /// Returns a reference to the topology.
    pub fn topology(&self) -> &Topology {
        self.network.topology()
    }

    /// Returns a mutable reference to the topology.
    pub fn topology_mut(&mut self) -> &mut Topology {
        self.network.topology_mut()
    }

    /// Sends a message.
    pub fn send(&mut self, src: NodeId, dst: NodeId, payload: P) -> Option<crate::net::MessageId> {
        self.network.send(src, dst, payload)
    }

    /// Runs the simulation with tracing, returning both stats and trace.
    pub fn run_traced<F>(self, mut handler: F) -> (NetworkStats, Trace<NetTraceEvent>)
    where
        F: FnMut(&mut crate::net::RunContext<P, L>, &NetEvent<P>),
    {
        let seed = self.seed;
        let mut recorder = TraceRecorder::new(seed);

        let stats = self.network.run(|ctx, event| {
            // Record the trace event with the actual simulation time
            let trace_event = match &event {
                NetEvent::Deliver(msg) => NetTraceEvent::Delivered {
                    message_id: msg.id.as_u64(),
                    src: msg.src.as_u32(),
                    dst: msg.dst.as_u32(),
                },
                NetEvent::Drop { message, reason } => NetTraceEvent::Dropped {
                    message_id: message.id.as_u64(),
                    src: message.src.as_u32(),
                    dst: message.dst.as_u32(),
                    reason: (*reason).into(),
                },
            };

            recorder.record(ctx.now(), trace_event);

            handler(ctx, &event);
        });

        // Update final time from stats
        let mut trace = recorder.finish();
        trace.final_time_ns = stats.final_time.as_nanos();

        (stats, trace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Duration;
    use crate::net::TopologyBuilder;

    #[test]
    fn traced_network_basic() {
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .build();

        let mut net = TracedNetwork::new(topo, 42);
        net.send(NodeId(0), NodeId(2), "hello");

        let (stats, trace) = net.run_traced(|_ctx, _event| {});

        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(trace.seed, 42);
        assert_eq!(trace.len(), 1);

        let event = &trace.get(0).unwrap().event;
        assert!(matches!(
            event,
            NetTraceEvent::Delivered { src: 0, dst: 2, .. }
        ));
    }

    #[test]
    fn traced_network_dropped() {
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            // Node 2 disconnected
            .build();

        let mut net = TracedNetwork::new(topo, 42);
        net.send(NodeId(0), NodeId(2), "unreachable");

        let (stats, trace) = net.run_traced(|_ctx, _event| {});

        assert_eq!(stats.messages_dropped, 1);
        assert_eq!(trace.len(), 1);

        let event = &trace.get(0).unwrap().event;
        assert!(matches!(
            event,
            NetTraceEvent::Dropped {
                reason: NetTraceDropReason::NoRoute,
                ..
            }
        ));
    }

    #[test]
    fn trace_comparison() {
        fn run_sim() -> Trace<NetTraceEvent> {
            let topo = TopologyBuilder::new(3)
                .link(0u32, 1u32, Duration::from_millis(10))
                .link(1u32, 2u32, Duration::from_millis(10))
                .build();

            let mut net = TracedNetwork::new(topo, 42);
            net.send(NodeId(0), NodeId(1), ());
            net.send(NodeId(1), NodeId(2), ());

            let (_stats, trace) = net.run_traced(|_ctx, _event| {});
            trace
        }

        let trace1 = run_sim();
        let trace2 = run_sim();

        assert!(trace1.compare(&trace2).is_ok());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn trace_json_export() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = TracedNetwork::new(topo, 42);
        net.send(NodeId(0), NodeId(1), "test");

        let (_stats, trace) = net.run_traced(|_ctx, _event| {});

        let json = trace.to_json().unwrap();
        let parsed: Trace<NetTraceEvent> = Trace::from_json(&json).unwrap();

        assert!(trace.compare(&parsed).is_ok());
    }
}
