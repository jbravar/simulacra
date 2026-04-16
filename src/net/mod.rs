//! Network simulation module.
//!
//! Provides a complete network simulation layer on top of the core simulation engine,
//! with topology, routing, latency models, and message delivery.

mod latency;
mod node;
mod topology;
mod traced;

pub use latency::{
    FixedLatency, LatencyModel, OverheadPlusJitter, PercentageJitter, SpikyLatency, UniformJitter,
};
pub use node::{MessageId, NodeId};
pub use topology::{Link, Route, Topology, TopologyBuilder};
pub use traced::{NetTraceDropReason, NetTraceEvent, TracedNetwork};

use crate::rng::SimRng;
use crate::sim::Simulation;
use crate::time::{Duration, Time};

/// A message being sent through the network.
#[derive(Debug, Clone)]
pub struct Message<P> {
    /// Unique message identifier.
    pub id: MessageId,
    /// Source node.
    pub src: NodeId,
    /// Destination node.
    pub dst: NodeId,
    /// User-defined payload.
    pub payload: P,
}

impl<P> Message<P> {
    /// Creates a new message.
    pub fn new(id: MessageId, src: NodeId, dst: NodeId, payload: P) -> Self {
        Message {
            id,
            src,
            dst,
            payload,
        }
    }
}

/// Network-level events.
#[derive(Debug, Clone)]
pub enum NetEvent<P> {
    /// A message is delivered to its destination.
    Deliver(Message<P>),
    /// A message was dropped (e.g., no route, link failure).
    Drop {
        message: Message<P>,
        reason: DropReason,
    },
}

/// Reason why a message was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// No route exists to the destination.
    NoRoute,
    /// Random packet loss.
    PacketLoss,
    /// The source and destination are separated by an active network partition.
    Partitioned,
}

/// Configuration for a network simulation.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Probability of packet loss (0.0 to 1.0).
    pub packet_loss: f64,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig { packet_loss: 0.0 }
    }
}

/// A network simulation built on top of the core simulation engine.
///
/// Manages topology, routing, and message delivery with configurable
/// latency models.
#[derive(Debug)]
pub struct Network<P, L: LatencyModel = FixedLatency> {
    /// The underlying simulation.
    sim: Simulation<NetEvent<P>>,
    /// Network topology.
    topology: Topology,
    /// Latency model for computing transmission delays.
    latency_model: L,
    /// Random number generator.
    rng: SimRng,
    /// Next message ID.
    next_message_id: u64,
    /// Configuration.
    config: NetConfig,
    /// Active partitions: unordered pairs of nodes that cannot reach each
    /// other. A partition affects the full `(src, dst)` pair, independent of
    /// the route. This is a coarse model, but matches what most callers want
    /// when simulating network failures.
    partitions: std::collections::HashSet<(NodeId, NodeId)>,
    /// Per-direction end-to-end bandwidth caps, with serialization queueing.
    ///
    /// A `(src, dst)` pair present in this map has a bandwidth limit; each
    /// sized send between that pair blocks the next one until the link
    /// finishes "transmitting" the current message. Pairs not in the map
    /// are uncapped (classic `send()` behavior).
    bandwidths: std::collections::HashMap<(NodeId, NodeId), BandwidthState>,
}

/// Per-`(src, dst)` bandwidth state for the optional end-to-end cap.
#[derive(Debug, Clone, Copy)]
struct BandwidthState {
    /// Bandwidth in bytes per second. Must be > 0.
    bps: u64,
    /// Earliest time the link is free to start transmitting a new message.
    link_free_at: Time,
}

/// Computes the transmission time for a message of `size_bytes` bytes at
/// `bps` bytes per second, rounded up to the next nanosecond.
fn transmission_duration(bps: u64, size_bytes: u64) -> Duration {
    if bps == 0 || size_bytes == 0 {
        return Duration::ZERO;
    }
    // size_bytes * 1e9 / bps, rounded up.
    let numerator = size_bytes.saturating_mul(1_000_000_000);
    let nanos = numerator.div_ceil(bps);
    Duration::from_nanos(nanos)
}

fn partition_key(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a.as_u32() <= b.as_u32() {
        (a, b)
    } else {
        (b, a)
    }
}

impl<P> Network<P, FixedLatency> {
    /// Creates a new network simulation with the given topology.
    pub fn new(topology: Topology, seed: u64) -> Self {
        Network {
            sim: Simulation::new(),
            topology,
            latency_model: FixedLatency,
            rng: SimRng::new(seed),
            next_message_id: 0,
            config: NetConfig::default(),
            partitions: std::collections::HashSet::new(),
            bandwidths: std::collections::HashMap::new(),
        }
    }
}

impl<P, L: LatencyModel> Network<P, L> {
    /// Creates a new network simulation with a custom latency model.
    pub fn with_latency_model(topology: Topology, latency_model: L, seed: u64) -> Self {
        Network {
            sim: Simulation::new(),
            topology,
            latency_model,
            rng: SimRng::new(seed),
            next_message_id: 0,
            config: NetConfig::default(),
            partitions: std::collections::HashSet::new(),
            bandwidths: std::collections::HashMap::new(),
        }
    }

    /// Installs an end-to-end bandwidth cap (in bytes per second) for
    /// messages flowing from `src` to `dst`.
    ///
    /// Only affects [`Network::send_sized`] / [`Network::send_at_sized`]
    /// calls between this pair; plain [`Network::send`] treats payload size
    /// as 0 and is unaffected by the cap.
    ///
    /// Bandwidth is directional: set both `(a, b)` and `(b, a)` separately
    /// if you want a symmetric pipe.
    ///
    /// Passing `bps == 0` clears the cap for that direction.
    pub fn set_bandwidth(&mut self, src: NodeId, dst: NodeId, bps: u64) {
        if bps == 0 {
            self.bandwidths.remove(&(src, dst));
        } else {
            self.bandwidths
                .entry((src, dst))
                .and_modify(|s| s.bps = bps)
                .or_insert(BandwidthState {
                    bps,
                    link_free_at: Time::ZERO,
                });
        }
    }

    /// Returns the configured bandwidth (bytes/sec) for `(src, dst)`, or
    /// `None` if uncapped.
    pub fn bandwidth(&self, src: NodeId, dst: NodeId) -> Option<u64> {
        self.bandwidths.get(&(src, dst)).map(|s| s.bps)
    }

    /// Installs a symmetric partition between `a` and `b`.
    ///
    /// All subsequent `send(a, b, _)` and `send(b, a, _)` calls will be
    /// dropped with [`DropReason::Partitioned`] until [`Network::heal`] is
    /// called. Partitioning is done at the `(src, dst)` pair granularity;
    /// messages routed between other node pairs are unaffected.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert(partition_key(a, b));
    }

    /// Removes a previously installed partition between `a` and `b`.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&partition_key(a, b));
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&partition_key(a, b))
    }

    /// Sets the network configuration.
    pub fn with_config(mut self, config: NetConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the current simulated time.
    #[inline]
    pub fn now(&self) -> Time {
        self.sim.now()
    }

    /// Returns a reference to the topology.
    #[inline]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Returns a mutable reference to the topology.
    #[inline]
    pub fn topology_mut(&mut self) -> &mut Topology {
        &mut self.topology
    }

    /// Sends a message from `src` to `dst` with the given payload.
    ///
    /// No bandwidth cap is applied (the message is treated as zero-sized).
    /// Use [`Network::send_sized`] to include a message size and have any
    /// configured bandwidth cap apply serialization delay + queueing.
    ///
    /// Returns the message ID, or `None` if no route exists.
    pub fn send(&mut self, src: NodeId, dst: NodeId, payload: P) -> Option<MessageId> {
        self.enqueue_send(self.sim.now(), src, dst, payload, 0)
    }

    /// Sends a message with an explicit size in bytes.
    ///
    /// If a bandwidth cap has been installed via [`Network::set_bandwidth`]
    /// for `(src, dst)`, this send will be delayed by
    /// `size_bytes / bps` seconds of transmission time, plus any
    /// serialization queueing behind messages already scheduled on the link.
    pub fn send_sized(
        &mut self,
        src: NodeId,
        dst: NodeId,
        payload: P,
        size_bytes: u64,
    ) -> Option<MessageId> {
        self.enqueue_send(self.sim.now(), src, dst, payload, size_bytes)
    }

    /// Sends a message at a specific time (no bandwidth cap).
    pub fn send_at(
        &mut self,
        time: Time,
        src: NodeId,
        dst: NodeId,
        payload: P,
    ) -> Option<MessageId> {
        self.enqueue_send(time, src, dst, payload, 0)
    }

    /// Sends a sized message at a specific time.
    pub fn send_at_sized(
        &mut self,
        time: Time,
        src: NodeId,
        dst: NodeId,
        payload: P,
        size_bytes: u64,
    ) -> Option<MessageId> {
        self.enqueue_send(time, src, dst, payload, size_bytes)
    }

    /// Core send path shared by `send`, `send_sized`, `send_at`, and
    /// `send_at_sized`. Applies partition, loss, routing, latency, and
    /// optional bandwidth serialization in a single place.
    fn enqueue_send(
        &mut self,
        base_time: Time,
        src: NodeId,
        dst: NodeId,
        payload: P,
        size_bytes: u64,
    ) -> Option<MessageId> {
        let id = MessageId::new(self.next_message_id);
        self.next_message_id += 1;

        let message = Message::new(id, src, dst, payload);

        // Check for an active partition first.
        if self.partitions.contains(&partition_key(src, dst)) {
            self.sim.schedule(
                base_time,
                NetEvent::Drop {
                    message,
                    reason: DropReason::Partitioned,
                },
            );
            return Some(id);
        }

        // Check for packet loss.
        if self.config.packet_loss > 0.0 && self.rng.bool(self.config.packet_loss) {
            self.sim.schedule(
                base_time,
                NetEvent::Drop {
                    message,
                    reason: DropReason::PacketLoss,
                },
            );
            return Some(id);
        }

        // Find route.
        let route = match self.topology.route(src, dst) {
            Some(r) => r,
            None => {
                self.sim.schedule(
                    base_time,
                    NetEvent::Drop {
                        message,
                        reason: DropReason::NoRoute,
                    },
                );
                return Some(id);
            }
        };

        // Compute latency with model.
        let latency = self
            .latency_model
            .compute(route.total_latency, &mut self.rng);

        // Apply bandwidth serialization if configured for this direction.
        let (departure_time, transmission) = match self.bandwidths.get_mut(&(src, dst)) {
            Some(state) if size_bytes > 0 => {
                let earliest = base_time.max(state.link_free_at);
                let tx = transmission_duration(state.bps, size_bytes);
                let free_at = earliest + tx;
                state.link_free_at = free_at;
                (earliest, tx)
            }
            _ => (base_time, Duration::ZERO),
        };

        let delivery_time = departure_time + transmission + latency;
        self.sim.schedule(delivery_time, NetEvent::Deliver(message));
        Some(id)
    }

    /// Schedules a raw network event.
    pub fn schedule(&mut self, time: Time, event: NetEvent<P>) -> u64 {
        self.sim.schedule(time, event)
    }

    /// Schedules a raw network event after a delay.
    pub fn schedule_in(&mut self, delay: Duration, event: NetEvent<P>) -> u64 {
        self.sim.schedule_in(delay, event)
    }

    /// Returns the total number of messages sent.
    pub fn messages_sent(&self) -> u64 {
        self.next_message_id
    }

    /// Runs the simulation to completion.
    pub fn run<F>(self, handler: F) -> NetworkStats
    where
        F: FnMut(&mut RunContext<P, L>, NetEvent<P>),
    {
        let Network {
            sim,
            topology,
            latency_model,
            rng,
            next_message_id,
            config,
            partitions,
            bandwidths,
        } = self;

        let mut ctx = RunContext {
            now: Time::ZERO,
            topology,
            latency_model,
            rng,
            next_message_id,
            config,
            partitions,
            bandwidths,
            delivered: 0,
            dropped: 0,
            pending_sends: Vec::new(),
        };

        let mut handler = handler;

        let stats = sim.run(|sim, event| {
            // Update current time
            ctx.now = sim.now();

            // Track stats
            match &event {
                NetEvent::Deliver(_) => ctx.delivered += 1,
                NetEvent::Drop { .. } => ctx.dropped += 1,
            }

            handler(&mut ctx, event);

            // Process pending sends after handler completes
            for (src, dst, payload, size_bytes) in ctx.pending_sends.drain(..) {
                let id = MessageId::new(ctx.next_message_id);
                ctx.next_message_id += 1;

                let message = Message::new(id, src, dst, payload);

                if ctx.partitions.contains(&partition_key(src, dst)) {
                    sim.schedule(
                        sim.now(),
                        NetEvent::Drop {
                            message,
                            reason: DropReason::Partitioned,
                        },
                    );
                    continue;
                }

                if ctx.config.packet_loss > 0.0 && ctx.rng.bool(ctx.config.packet_loss) {
                    sim.schedule(
                        sim.now(),
                        NetEvent::Drop {
                            message,
                            reason: DropReason::PacketLoss,
                        },
                    );
                    continue;
                }

                let route = match ctx.topology.route(src, dst) {
                    Some(r) => r,
                    None => {
                        sim.schedule(
                            sim.now(),
                            NetEvent::Drop {
                                message,
                                reason: DropReason::NoRoute,
                            },
                        );
                        continue;
                    }
                };

                let latency = ctx.latency_model.compute(route.total_latency, &mut ctx.rng);

                let base_time = sim.now();
                let (departure_time, transmission) = match ctx.bandwidths.get_mut(&(src, dst)) {
                    Some(state) if size_bytes > 0 => {
                        let earliest = base_time.max(state.link_free_at);
                        let tx = transmission_duration(state.bps, size_bytes);
                        let free_at = earliest + tx;
                        state.link_free_at = free_at;
                        (earliest, tx)
                    }
                    _ => (base_time, Duration::ZERO),
                };
                let delivery_time = departure_time + transmission + latency;
                sim.schedule(delivery_time, NetEvent::Deliver(message));
            }
        });

        NetworkStats {
            events_processed: stats.events_processed,
            final_time: stats.final_time,
            messages_sent: ctx.next_message_id,
            messages_delivered: ctx.delivered,
            messages_dropped: ctx.dropped,
        }
    }
}

/// Context available during simulation run for sending messages.
#[derive(Debug)]
pub struct RunContext<P, L: LatencyModel> {
    now: Time,
    topology: Topology,
    latency_model: L,
    rng: SimRng,
    next_message_id: u64,
    config: NetConfig,
    partitions: std::collections::HashSet<(NodeId, NodeId)>,
    bandwidths: std::collections::HashMap<(NodeId, NodeId), BandwidthState>,
    delivered: u64,
    dropped: u64,
    pending_sends: Vec<(NodeId, NodeId, P, u64)>,
}

impl<P, L: LatencyModel> RunContext<P, L> {
    /// Returns the current simulated time.
    pub fn now(&self) -> Time {
        self.now
    }

    /// Queues a message to be sent. Message size is treated as 0, so any
    /// configured bandwidth cap has no effect.
    ///
    /// The message will be processed after the current event handler returns.
    pub fn send(&mut self, src: NodeId, dst: NodeId, payload: P) {
        self.pending_sends.push((src, dst, payload, 0));
    }

    /// Queues a sized message to be sent. If a bandwidth cap is configured
    /// for `(src, dst)`, it applies serialization delay + queueing.
    pub fn send_sized(&mut self, src: NodeId, dst: NodeId, payload: P, size_bytes: u64) {
        self.pending_sends.push((src, dst, payload, size_bytes));
    }

    /// Installs a bandwidth cap (bytes/sec) for `(src, dst)`, or clears it
    /// if `bps == 0`. See [`Network::set_bandwidth`].
    pub fn set_bandwidth(&mut self, src: NodeId, dst: NodeId, bps: u64) {
        if bps == 0 {
            self.bandwidths.remove(&(src, dst));
        } else {
            self.bandwidths
                .entry((src, dst))
                .and_modify(|s| s.bps = bps)
                .or_insert(BandwidthState {
                    bps,
                    link_free_at: Time::ZERO,
                });
        }
    }

    /// Returns the configured bandwidth (bytes/sec) for `(src, dst)`, or
    /// `None` if uncapped.
    pub fn bandwidth(&self, src: NodeId, dst: NodeId) -> Option<u64> {
        self.bandwidths.get(&(src, dst)).map(|s| s.bps)
    }

    /// Installs a partition between `a` and `b` in-flight.
    ///
    /// Subsequent sends across this pair will drop with
    /// [`DropReason::Partitioned`].
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert(partition_key(a, b));
    }

    /// Heals a previously installed in-flight partition.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&partition_key(a, b));
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&partition_key(a, b))
    }

    /// Returns a reference to the topology.
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Returns a mutable reference to the topology.
    pub fn topology_mut(&mut self) -> &mut Topology {
        &mut self.topology
    }

    /// Returns the RNG for use in handlers.
    pub fn rng(&mut self) -> &mut SimRng {
        &mut self.rng
    }
}

/// Statistics from a network simulation run.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total events processed.
    pub events_processed: u64,
    /// Final simulated time.
    pub final_time: Time,
    /// Total messages sent.
    pub messages_sent: u64,
    /// Messages successfully delivered.
    pub messages_delivered: u64,
    /// Messages dropped.
    pub messages_dropped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_message_delivery() {
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(20))
            .build();

        let mut net = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(2), "hello");

        let mut delivered = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                delivered.push((msg.src, msg.dst, msg.payload));
            }
        });

        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0], (NodeId(0), NodeId(2), "hello"));
        // Latency should be 30ms (10 + 20)
        assert_eq!(stats.final_time, Time::from_millis(30));
    }

    #[test]
    fn no_route_drops_message() {
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            // Node 2 is disconnected
            .build();

        let mut net = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(2), "unreachable");

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { message, reason } = event {
                dropped.push((message.dst, reason));
            }
        });

        assert_eq!(stats.messages_dropped, 1);
        assert_eq!(dropped[0], (NodeId(2), DropReason::NoRoute));
    }

    #[test]
    fn jitter_model() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(100))
            .build();

        let latency_model = UniformJitter::new(Duration::from_millis(10));
        let mut net = Network::with_latency_model(topo, latency_model, 42);

        // Send multiple messages
        for _ in 0..10 {
            net.send(NodeId(0), NodeId(1), ());
        }

        net.run(|_ctx, event| {
            if let NetEvent::Deliver(_) = event {
                // Times are tracked by the stats
            }
        });

        // With jitter, messages shouldn't all arrive at exactly 100ms
        // (This is implicitly tested by the latency model tests)
    }

    #[test]
    fn packet_loss() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let config = NetConfig { packet_loss: 0.5 };
        let mut net = Network::new(topo, 42).with_config(config);

        // Send many messages
        for _ in 0..100 {
            net.send(NodeId(0), NodeId(1), ());
        }

        let stats = net.run(|_ctx, _event| {});

        // With 50% loss, we should have some delivered and some dropped
        assert!(stats.messages_delivered > 0);
        assert!(stats.messages_dropped > 0);
        assert_eq!(stats.messages_delivered + stats.messages_dropped, 100);
    }

    #[test]
    fn reply_in_handler() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(1), "ping");

        let mut messages = Vec::new();
        let stats = net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                messages.push(msg.payload);
                if msg.payload == "ping" {
                    ctx.send(msg.dst, msg.src, "pong");
                }
            }
        });

        assert_eq!(stats.messages_delivered, 2);
        assert_eq!(messages, vec!["ping", "pong"]);
    }

    #[test]
    fn transmission_duration_helper() {
        // 1 MB at 1 MB/s = 1 second exactly.
        assert_eq!(
            transmission_duration(1_000_000, 1_000_000),
            Duration::from_secs(1)
        );
        // Rounding: 1 byte at 3 B/s should round up to ceil(1e9 / 3) ns.
        let d = transmission_duration(3, 1);
        assert_eq!(d.as_nanos(), 333_333_334);
        // Zero size or zero bps = zero duration.
        assert_eq!(transmission_duration(0, 100), Duration::ZERO);
        assert_eq!(transmission_duration(100, 0), Duration::ZERO);
    }

    #[test]
    fn unsized_send_ignores_bandwidth() {
        // Even with a tight cap, plain send() treats payload as 0-sized and
        // therefore should not be delayed.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 1); // 1 byte/sec

        net.send(NodeId(0), NodeId(1), 0);
        net.send(NodeId(0), NodeId(1), 0);

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 2);
        // Both arrive at base latency 10ms; bandwidth is not consulted.
        assert_eq!(stats.final_time, Time::from_millis(10));
    }

    #[test]
    fn send_sized_delays_by_transmission_time() {
        // 1000 bytes @ 1 MB/s = 1ms of tx time, on top of the 10ms link.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 1_000_000);
        assert_eq!(net.bandwidth(NodeId(0), NodeId(1)), Some(1_000_000));

        net.send_sized(NodeId(0), NodeId(1), 0, 1_000);

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        // 10ms (latency) + 1ms (tx) = 11ms.
        assert_eq!(stats.final_time, Time::from_millis(11));
    }

    #[test]
    fn bandwidth_serializes_concurrent_sends() {
        // Three 1 MB messages over a 1 MB/s link: each takes 1s tx.
        // They queue, so deliveries are at 1s, 2s, 3s (all + latency).
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(0))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 1_000_000);

        net.send_sized(NodeId(0), NodeId(1), 0, 1_000_000);
        net.send_sized(NodeId(0), NodeId(1), 1, 1_000_000);
        net.send_sized(NodeId(0), NodeId(1), 2, 1_000_000);

        let mut arrivals = Vec::new();
        net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                arrivals.push((msg.payload, ctx.now()));
            }
        });

        arrivals.sort_by_key(|(_, t)| *t);
        assert_eq!(arrivals.len(), 3);
        assert_eq!(arrivals[0], (0, Time::from_secs(1)));
        assert_eq!(arrivals[1], (1, Time::from_secs(2)));
        assert_eq!(arrivals[2], (2, Time::from_secs(3)));
    }

    #[test]
    fn set_bandwidth_zero_clears_cap() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(0))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 1_000_000);
        assert!(net.bandwidth(NodeId(0), NodeId(1)).is_some());

        net.set_bandwidth(NodeId(0), NodeId(1), 0);
        assert!(net.bandwidth(NodeId(0), NodeId(1)).is_none());

        // With the cap cleared, send_sized should behave like send.
        net.send_sized(NodeId(0), NodeId(1), 0, 1_000_000);
        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.final_time, Time::ZERO);
    }

    #[test]
    fn bandwidth_is_directional() {
        // Cap A->B but leave B->A uncapped.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(0))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 1_000_000); // 1 MB/s forward

        net.send_sized(NodeId(0), NodeId(1), 0, 1_000_000); // 1s tx
        net.send_sized(NodeId(1), NodeId(0), 1, 1_000_000); // no cap, 0s tx

        let mut arrivals = Vec::new();
        net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                arrivals.push((msg.payload, ctx.now()));
            }
        });

        arrivals.sort_by_key(|(p, _)| *p);
        assert_eq!(arrivals[0], (0, Time::from_secs(1)));
        assert_eq!(arrivals[1], (1, Time::ZERO));
    }

    #[test]
    fn partition_drops_messages() {
        let topo = TopologyBuilder::new(3)
            .full_mesh(Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.partition(NodeId(0), NodeId(1));
        assert!(net.is_partitioned(NodeId(0), NodeId(1)));
        assert!(net.is_partitioned(NodeId(1), NodeId(0)));
        assert!(!net.is_partitioned(NodeId(0), NodeId(2)));

        net.send(NodeId(0), NodeId(1), "nope");
        net.send(NodeId(1), NodeId(0), "also nope");
        net.send(NodeId(0), NodeId(2), "this goes through");

        let mut dropped_pairs = Vec::new();
        let mut delivered_pairs = Vec::new();
        let stats = net.run(|_ctx, event| match event {
            NetEvent::Deliver(msg) => delivered_pairs.push((msg.src, msg.dst)),
            NetEvent::Drop { message, reason } => {
                dropped_pairs.push((message.src, message.dst, reason));
            }
        });

        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.messages_dropped, 2);
        assert!(
            dropped_pairs
                .iter()
                .all(|(.., r)| *r == DropReason::Partitioned)
        );
        assert_eq!(delivered_pairs, vec![(NodeId(0), NodeId(2))]);
    }

    #[test]
    fn heal_restores_delivery() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(5))
            .build();

        let mut net = Network::new(topo, 42);
        net.partition(NodeId(0), NodeId(1));
        net.send(NodeId(0), NodeId(1), "blocked");
        net.heal(NodeId(0), NodeId(1));
        net.send(NodeId(0), NodeId(1), "through");

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn asymmetric_link_latency() {
        let topo = TopologyBuilder::new(2)
            .link_asymmetric(
                0u32,
                1u32,
                Duration::from_millis(5),
                Duration::from_millis(20),
            )
            .build();

        let mut net = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(1), "fast");
        net.send_at(Time::from_millis(100), NodeId(1), NodeId(0), "slow");

        let mut arrivals = Vec::new();
        net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                arrivals.push((msg.payload, ctx.now()));
            }
        });

        arrivals.sort_by_key(|(_, t)| *t);
        assert_eq!(arrivals.len(), 2);
        assert_eq!(arrivals[0], ("fast", Time::from_millis(5)));
        assert_eq!(arrivals[1], ("slow", Time::from_millis(120)));
    }

    #[test]
    fn spiky_latency_sometimes_adds_extra() {
        use crate::net::SpikyLatency;
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        // 100% spike probability so we can deterministically observe a spike.
        let model = SpikyLatency::new(FixedLatency, 1.0, Duration::from_millis(50));
        let mut net = Network::with_latency_model(topo, model, 42);

        for _ in 0..20 {
            net.send(NodeId(0), NodeId(1), ());
        }

        let mut max_time = Time::ZERO;
        net.run(|ctx, event| {
            if let NetEvent::Deliver(_) = event
                && ctx.now() > max_time
            {
                max_time = ctx.now();
            }
        });

        // Some message was delayed beyond the base 10ms.
        assert!(
            max_time > Time::from_millis(10),
            "expected at least one spike beyond base latency, got max_time = {}",
            max_time
        );
    }

    #[test]
    fn deterministic_simulation() {
        fn run_sim() -> (Vec<(Time, NodeId)>, NetworkStats) {
            let topo = TopologyBuilder::new(4)
                .ring(Duration::from_millis(10))
                .build();

            let latency_model = UniformJitter::new(Duration::from_millis(2));
            let mut net = Network::with_latency_model(topo, latency_model, 42);

            net.send(NodeId(0), NodeId(2), ());
            net.send(NodeId(1), NodeId(3), ());
            net.send(NodeId(2), NodeId(0), ());

            let mut deliveries = Vec::new();
            let stats = net.run(|_ctx, event| {
                if let NetEvent::Deliver(msg) = event {
                    deliveries.push((Time::ZERO, msg.dst)); // We'd need sim.now() access
                }
            });
            (deliveries, stats)
        }

        let (d1, s1) = run_sim();
        let (d2, s2) = run_sim();

        assert_eq!(d1, d2);
        assert_eq!(s1.final_time, s2.final_time);
    }
}
