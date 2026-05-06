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

/// Per-link drop policy applied at admission time when the link is
/// congested.
///
/// Default is [`DropPolicy::TailDrop`], which only sheds load once the
/// buffer is exhausted (identical to the classic `buffer_bytes` behavior).
/// [`DropPolicy::RandomEarlyDetection`] models active queue management by
/// probabilistically dropping incoming messages once queue occupancy
/// crosses a configurable threshold, before the hard buffer limit is hit.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum DropPolicy {
    /// Admit while the pending byte count fits under the link's buffer.
    /// This is the classic FIFO / tail-drop behavior.
    #[default]
    TailDrop,
    /// Random Early Detection (RED).
    ///
    /// * Below `min_bytes` of pending work: never drop (still subject to
    ///   the hard buffer cap).
    /// * In `[min_bytes, max_bytes)`: drop with probability linearly
    ///   interpolated from `0.0` at `min_bytes` up to `max_prob` at
    ///   `max_bytes`.
    /// * At or above `max_bytes`: always drop.
    ///
    /// `max_prob` is in `(0.0, 1.0]`. The hard `buffer_bytes` cap still
    /// applies; RED just introduces earlier drops.
    RandomEarlyDetection {
        /// Pending bytes below which RED never drops.
        min_bytes: u64,
        /// Pending bytes at or above which RED always drops.
        max_bytes: u64,
        /// Drop probability at `max_bytes`.
        max_prob: f64,
    },
}

impl DropPolicy {
    /// Constructs a validated `RandomEarlyDetection` policy.
    ///
    /// Panics on clearly-wrong inputs to fail fast at topology build time
    /// rather than silently admitting every message.
    pub fn red(min_bytes: u64, max_bytes: u64, buffer_bytes: u64, max_prob: f64) -> Self {
        assert!(
            min_bytes <= max_bytes,
            "DropPolicy::red: min_bytes ({min_bytes}) must be <= max_bytes ({max_bytes})"
        );
        assert!(
            max_bytes <= buffer_bytes,
            "DropPolicy::red: max_bytes ({max_bytes}) must be <= buffer_bytes ({buffer_bytes})"
        );
        assert!(
            max_prob > 0.0 && max_prob <= 1.0,
            "DropPolicy::red: max_prob ({max_prob}) must be in (0.0, 1.0]"
        );
        DropPolicy::RandomEarlyDetection {
            min_bytes,
            max_bytes,
            max_prob,
        }
    }
}

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
    /// A per-link buffer on the message's path was full at admission time.
    ///
    /// Only produced by sized sends (`send_sized`/`send_at_sized`) that
    /// traverse at least one link configured with `buffer_bytes`.
    BufferOverflow,
}

/// Configuration for a network simulation.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Probability of packet loss (0.0 to 1.0).
    pub packet_loss: f64,
    /// When `true`, calls to [`Network::partition`], [`Network::fail_link`],
    /// [`Network::fail_node`] (and their `RunContext` equivalents) walk the
    /// pending event queue and rewrite any in-flight `Deliver` events
    /// whose `(src, dst)` no longer routes — replacing them with `Drop`
    /// events at the time of the failure call. Defaults to `false`,
    /// matching the historic "in-flight messages survive" semantics.
    pub drop_in_flight_on_failure: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig {
            packet_loss: 0.0,
            drop_in_flight_on_failure: false,
        }
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
    /// are uncapped (classic `send()` behavior). This is an end-to-end
    /// model; it is layered on top of any per-physical-link bandwidth caps
    /// declared on the `Topology`.
    bandwidths: std::collections::HashMap<(NodeId, NodeId), BandwidthState>,
    /// Earliest time each directed physical link is free to start
    /// transmitting a new message. Populated lazily for links that
    /// participate in a `send_sized` call. Models realistic per-link
    /// serialization (e.g., multiple flows sharing a bottleneck WAN link).
    link_busy: std::collections::HashMap<(NodeId, NodeId), Time>,
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

/// Walks all pending `Deliver` events on `sim` and rewrites any whose
/// `(src, dst)` no longer routes against the current topology + partitions
/// into `Drop` events firing at `now`. `Drop` events already in the queue
/// and other event payloads are left untouched.
///
/// This is the in-flight-drop sweep invoked from a failure mutator
/// (`partition`, `fail_link`, `fail_node`, etc.) when the
/// [`NetConfig::drop_in_flight_on_failure`] flag is set. Without the
/// flag, in-flight messages keep their original delivery time, matching
/// the historic semantics.
fn sweep_failure_drops<P>(
    sim: &mut Simulation<NetEvent<P>>,
    topology: &mut Topology,
    partitions: &std::collections::HashSet<(NodeId, NodeId)>,
    now: Time,
) {
    sim.rewrite_queue(|s| match s.event {
        NetEvent::Deliver(msg) => {
            if partitions.contains(&partition_key(msg.src, msg.dst)) {
                Some((
                    now,
                    NetEvent::Drop {
                        message: msg,
                        reason: DropReason::Partitioned,
                    },
                ))
            } else if topology.route(msg.src, msg.dst).is_none() {
                Some((
                    now,
                    NetEvent::Drop {
                        message: msg,
                        reason: DropReason::NoRoute,
                    },
                ))
            } else {
                Some((s.time, NetEvent::Deliver(msg)))
            }
        }
        other => Some((s.time, other)),
    });
}

fn partition_key(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a.as_u32() <= b.as_u32() {
        (a, b)
    } else {
        (b, a)
    }
}

/// Returns an approximation of the bytes still queued on a link at time
/// `now`, given that the link is known to be busy until `busy_until` at
/// `bps` bytes/sec. Uses u128 intermediate math to avoid overflow and
/// floors the result — a small one-byte undercount is acceptable for
/// admission control (it matches the intuition that exactly-drained means
/// zero queue).
fn pending_bytes_at(bps: u64, busy_until: Time, now: Time) -> u64 {
    if bps == 0 || busy_until <= now {
        return 0;
    }
    let remaining_ns = busy_until.as_nanos().saturating_sub(now.as_nanos());
    let bytes = (remaining_ns as u128 * bps as u128) / 1_000_000_000u128;
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// Outcome of walking the shortest path for per-link bandwidth admission.
#[derive(Debug, Clone, Copy)]
enum PerLinkOutcome {
    /// Admitted along the entire path; extra delay added by per-link tx +
    /// queueing beyond what the latency model already accounts for.
    Admitted(Duration),
    /// Admission blocked because the buffer on some hop was full.
    Overflow,
}

/// Evaluates a per-link `DropPolicy` against the currently pending queue
/// depth. Returns `true` if the policy says the incoming message should
/// be dropped *before* consulting the hard buffer cap. The hard cap is
/// always enforced separately by the caller.
fn policy_wants_drop(policy: DropPolicy, pending: u64, rng: &mut SimRng) -> bool {
    match policy {
        DropPolicy::TailDrop => false,
        DropPolicy::RandomEarlyDetection {
            min_bytes,
            max_bytes,
            max_prob,
        } => {
            if pending >= max_bytes {
                true
            } else if pending < min_bytes || max_bytes == min_bytes {
                false
            } else {
                // Linear ramp from 0.0 at min_bytes to max_prob at max_bytes.
                let span = (max_bytes - min_bytes) as f64;
                let over = (pending - min_bytes) as f64;
                let prob = max_prob * (over / span);
                rng.bool(prob)
            }
        }
    }
}

/// Walks the shortest path from `src` to `dst` hop-by-hop, applying per-link
/// bandwidth serialization for a message of `size_bytes` bytes.
///
/// Returns an `Admitted` delay representing only the *extra* delay induced
/// by bandwidth (link latencies themselves are assumed accounted for by
/// the caller), or `Overflow` if any hop's `buffer_bytes` capacity would
/// be exceeded by admission or its [`DropPolicy`] chooses to drop.
///
/// `link_busy` is only mutated when the outcome is `Admitted`: the walk
/// collects per-hop admission decisions into a local scratch buffer and
/// commits them only if every hop passes admission, so an overflowing
/// message never "burns" capacity on earlier hops along its path.
///
/// `arrival_at_src_now` is the time at which the message's first byte is
/// ready to enter the network at `src` (i.e., after any pair-level
/// serialization has already run).
fn walk_per_link_bandwidth(
    topology: &mut Topology,
    link_busy: &mut std::collections::HashMap<(NodeId, NodeId), Time>,
    rng: &mut SimRng,
    src: NodeId,
    dst: NodeId,
    size_bytes: u64,
    arrival_at_src_now: Time,
) -> PerLinkOutcome {
    let mut current = src;
    let mut hop_time = arrival_at_src_now;
    let mut accumulated_link_latency = Duration::ZERO;
    // Collected but-not-yet-committed `link_busy` updates. Expected to be
    // tiny (bounded by the path length), so inline allocation here is fine.
    let mut admissions: Vec<((NodeId, NodeId), Time)> = Vec::new();

    while current != dst {
        let next = match topology.route(current, dst) {
            Some(r) if r.hop_count > 0 => r.next_hop,
            _ => return PerLinkOutcome::Admitted(Duration::ZERO),
        };

        let link = topology.link_between(current, next).copied();
        let link_latency = link.map(|l| l.latency).unwrap_or(Duration::ZERO);
        let link_bandwidth = link.and_then(|l| l.bandwidth);
        let link_buffer = link.and_then(|l| l.buffer_bytes);
        let link_policy = link.map(|l| l.drop_policy).unwrap_or_default();

        if let Some(bps) = link_bandwidth {
            let busy = link_busy
                .get(&(current, next))
                .copied()
                .unwrap_or(Time::ZERO);

            // Admission check against the per-link buffer + drop policy.
            // Evaluated as of `hop_time` — the time the message's first
            // byte is ready to enter this hop.
            if let Some(buf) = link_buffer {
                let pending = pending_bytes_at(bps, busy, hop_time);
                // Policy-driven early drop (AQM) is only meaningful when
                // the link has a buffer configured; a policy without a
                // buffer has no queue depth to reason about.
                if policy_wants_drop(link_policy, pending, rng) {
                    return PerLinkOutcome::Overflow;
                }
                if pending.saturating_add(size_bytes) > buf {
                    return PerLinkOutcome::Overflow;
                }
            }

            let tx = transmission_duration(bps, size_bytes);
            let departure = hop_time.max(busy);
            let free_at = departure + tx;
            admissions.push(((current, next), free_at));
            hop_time = free_at + link_latency;
        } else {
            hop_time += link_latency;
        }

        accumulated_link_latency += link_latency;
        current = next;
    }

    // Commit admissions atomically now that the full path cleared.
    for (key, t) in admissions {
        link_busy.insert(key, t);
    }

    let walk_elapsed = hop_time.saturating_duration_since(arrival_at_src_now);
    PerLinkOutcome::Admitted(walk_elapsed.saturating_sub(accumulated_link_latency))
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
            link_busy: std::collections::HashMap::new(),
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
            link_busy: std::collections::HashMap::new(),
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
    ///
    /// Pending `Deliver` events already on the queue are unaffected by
    /// default. To also drop in-flight messages whose route is now broken,
    /// set [`NetConfig::drop_in_flight_on_failure`] before calling.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert(partition_key(a, b));
        self.maybe_sweep_in_flight();
    }

    /// Removes a previously installed partition between `a` and `b`.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&partition_key(a, b));
    }

    /// If the config opts in, walks the queue and converts now-unroutable
    /// `Deliver` events into `Drop` events at the current simulated time.
    /// No-op when the flag is off.
    fn maybe_sweep_in_flight(&mut self) {
        if self.config.drop_in_flight_on_failure {
            let now = self.sim.now();
            sweep_failure_drops(&mut self.sim, &mut self.topology, &self.partitions, now);
        }
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&partition_key(a, b))
    }

    /// Fails the bidirectional link between `a` and `b` at the topology
    /// level. Subsequent routing decisions will exclude the failed edges,
    /// so sends whose shortest path required this link will either reroute
    /// around it or drop with [`DropReason::NoRoute`].
    ///
    /// Pending `Deliver` events already on the queue are unaffected by
    /// default; set [`NetConfig::drop_in_flight_on_failure`] to also drop
    /// them. For an asymmetric failure, use [`Network::fail_link_directed`].
    pub fn fail_link(&mut self, a: NodeId, b: NodeId) {
        self.topology.fail_link(a, b);
        self.maybe_sweep_in_flight();
    }

    /// Restores a previously-failed bidirectional link between `a` and `b`.
    pub fn heal_link(&mut self, a: NodeId, b: NodeId) {
        self.topology.heal_link(a, b);
    }

    /// Returns `true` if both directions of the link between `a` and `b`
    /// are currently failed.
    pub fn is_link_failed(&self, a: NodeId, b: NodeId) -> bool {
        self.topology.is_link_failed(a, b)
    }

    /// Fails a single direction of the link between `src` and `dst`.
    pub fn fail_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.topology.fail_link_directed(src, dst);
        self.maybe_sweep_in_flight();
    }

    /// Restores a single direction of the link between `src` and `dst`.
    pub fn heal_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.topology.heal_link_directed(src, dst);
    }

    /// Returns `true` if the directed edge `src -> dst` is failed.
    pub fn is_link_failed_directed(&self, src: NodeId, dst: NodeId) -> bool {
        self.topology.is_link_failed_directed(src, dst)
    }

    /// Marks `node` as failed at the topology level. All routes whose
    /// source, destination, or any intermediate hop is failed will return
    /// no route, dropping affected sends with [`DropReason::NoRoute`].
    ///
    /// Pending `Deliver` events already on the queue are unaffected by
    /// default; set [`NetConfig::drop_in_flight_on_failure`] to also drop
    /// them.
    pub fn fail_node(&mut self, node: NodeId) {
        self.topology.fail_node(node);
        self.maybe_sweep_in_flight();
    }

    /// Restores a previously-failed node.
    pub fn heal_node(&mut self, node: NodeId) {
        self.topology.heal_node(node);
    }

    /// Returns `true` if `node` is currently marked as failed.
    pub fn is_node_failed(&self, node: NodeId) -> bool {
        self.topology.is_node_failed(node)
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

        // Apply pair-level bandwidth serialization (end-to-end model).
        let (departure_time, pair_tx) = match self.bandwidths.get_mut(&(src, dst)) {
            Some(state) if size_bytes > 0 => {
                let earliest = base_time.max(state.link_free_at);
                let tx = transmission_duration(state.bps, size_bytes);
                let free_at = earliest + tx;
                state.link_free_at = free_at;
                (earliest, tx)
            }
            _ => (base_time, Duration::ZERO),
        };

        // Apply per-physical-link bandwidth serialization by walking the
        // path. Each link may have its own cap + buffer, shared across
        // flows that traverse it. Returns the extra delay beyond `latency`
        // that per-link transmission + queueing contributed, or signals
        // buffer overflow in which case the message is dropped.
        let link_bandwidth_delay = if size_bytes > 0 {
            match self.apply_per_link_bandwidth(src, dst, size_bytes, departure_time + pair_tx) {
                PerLinkOutcome::Admitted(d) => d,
                PerLinkOutcome::Overflow => {
                    self.sim.schedule(
                        base_time,
                        NetEvent::Drop {
                            message,
                            reason: DropReason::BufferOverflow,
                        },
                    );
                    return Some(id);
                }
            }
        } else {
            Duration::ZERO
        };

        let delivery_time = departure_time + pair_tx + latency + link_bandwidth_delay;
        self.sim.schedule(delivery_time, NetEvent::Deliver(message));
        Some(id)
    }

    /// Walks the shortest path from `src` to `dst`, accumulating per-link
    /// transmission + serialization delay for a message of `size_bytes`.
    /// See [`walk_per_link_bandwidth`] for the underlying algorithm.
    fn apply_per_link_bandwidth(
        &mut self,
        src: NodeId,
        dst: NodeId,
        size_bytes: u64,
        arrival_at_src_now: Time,
    ) -> PerLinkOutcome {
        walk_per_link_bandwidth(
            &mut self.topology,
            &mut self.link_busy,
            &mut self.rng,
            src,
            dst,
            size_bytes,
            arrival_at_src_now,
        )
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
            link_busy,
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
            link_busy,
            delivered: 0,
            dropped: 0,
            pending_sends: Vec::new(),
            needs_failure_sweep: false,
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

            // If the handler called fail_link / fail_node / partition and
            // the config opted in, rewrite the queue *before* draining
            // pending sends so any newly-scheduled sends from this handler
            // route against the post-failure topology fresh.
            if ctx.needs_failure_sweep && ctx.config.drop_in_flight_on_failure {
                ctx.needs_failure_sweep = false;
                let now = sim.now();
                sweep_failure_drops(sim, &mut ctx.topology, &ctx.partitions, now);
            } else {
                // Reset unconditionally so the flag never accumulates between
                // events when the config is off.
                ctx.needs_failure_sweep = false;
            }

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
                let (departure_time, pair_tx) = match ctx.bandwidths.get_mut(&(src, dst)) {
                    Some(state) if size_bytes > 0 => {
                        let earliest = base_time.max(state.link_free_at);
                        let tx = transmission_duration(state.bps, size_bytes);
                        let free_at = earliest + tx;
                        state.link_free_at = free_at;
                        (earliest, tx)
                    }
                    _ => (base_time, Duration::ZERO),
                };

                // Walk the path for per-link bandwidth.
                let link_bandwidth_delay = if size_bytes > 0 {
                    match walk_per_link_bandwidth(
                        &mut ctx.topology,
                        &mut ctx.link_busy,
                        &mut ctx.rng,
                        src,
                        dst,
                        size_bytes,
                        departure_time + pair_tx,
                    ) {
                        PerLinkOutcome::Admitted(d) => d,
                        PerLinkOutcome::Overflow => {
                            sim.schedule(
                                base_time,
                                NetEvent::Drop {
                                    message,
                                    reason: DropReason::BufferOverflow,
                                },
                            );
                            continue;
                        }
                    }
                } else {
                    Duration::ZERO
                };

                let delivery_time = departure_time + pair_tx + latency + link_bandwidth_delay;
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
    link_busy: std::collections::HashMap<(NodeId, NodeId), Time>,
    delivered: u64,
    dropped: u64,
    pending_sends: Vec<(NodeId, NodeId, P, u64)>,
    /// Set by mid-run failure mutators (`partition`, `fail_link`,
    /// `fail_node`, ...) to request that the run loop sweep the event
    /// queue once the handler returns. Combined with
    /// [`NetConfig::drop_in_flight_on_failure`].
    needs_failure_sweep: bool,
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
    /// [`DropReason::Partitioned`]. If
    /// [`NetConfig::drop_in_flight_on_failure`] is enabled, pending
    /// `Deliver` events whose `(src, dst)` matches the partition are
    /// rewritten into `Drop` events at the current simulated time, after
    /// the current handler returns.
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitions.insert(partition_key(a, b));
        self.needs_failure_sweep = true;
    }

    /// Heals a previously installed in-flight partition.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitions.remove(&partition_key(a, b));
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&partition_key(a, b))
    }

    /// Fails the bidirectional link between `a` and `b` mid-run. See
    /// [`Network::fail_link`] for semantics.
    pub fn fail_link(&mut self, a: NodeId, b: NodeId) {
        self.topology.fail_link(a, b);
        self.needs_failure_sweep = true;
    }

    /// Restores a previously-failed bidirectional link between `a` and `b`.
    pub fn heal_link(&mut self, a: NodeId, b: NodeId) {
        self.topology.heal_link(a, b);
    }

    /// Returns `true` if both directions of the link between `a` and `b`
    /// are currently failed.
    pub fn is_link_failed(&self, a: NodeId, b: NodeId) -> bool {
        self.topology.is_link_failed(a, b)
    }

    /// Fails a single direction of the link between `src` and `dst`.
    pub fn fail_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.topology.fail_link_directed(src, dst);
        self.needs_failure_sweep = true;
    }

    /// Restores a single direction of the link between `src` and `dst`.
    pub fn heal_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.topology.heal_link_directed(src, dst);
    }

    /// Returns `true` if the directed edge `src -> dst` is failed.
    pub fn is_link_failed_directed(&self, src: NodeId, dst: NodeId) -> bool {
        self.topology.is_link_failed_directed(src, dst)
    }

    /// Marks `node` as failed mid-run. See [`Network::fail_node`].
    pub fn fail_node(&mut self, node: NodeId) {
        self.topology.fail_node(node);
        self.needs_failure_sweep = true;
    }

    /// Restores a previously-failed node.
    pub fn heal_node(&mut self, node: NodeId) {
        self.topology.heal_node(node);
    }

    /// Returns `true` if `node` is currently marked as failed.
    pub fn is_node_failed(&self, node: NodeId) -> bool {
        self.topology.is_node_failed(node)
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

        let config = NetConfig {
            packet_loss: 0.5,
            ..Default::default()
        };
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
    fn per_link_bandwidth_on_direct_edge() {
        // Same scenario as send_sized_delays_by_transmission_time, but the
        // cap is declared on the physical link instead of pair-level.
        let topo = TopologyBuilder::new(2)
            .link_with_capacity(0u32, 1u32, Duration::from_millis(10), 1_000_000)
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.send_sized(NodeId(0), NodeId(1), 0, 1_000);

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        // 10ms latency + 1ms tx = 11ms.
        assert_eq!(stats.final_time, Time::from_millis(11));
    }

    #[test]
    fn per_link_bandwidth_bottleneck_middle_of_path() {
        // 3 nodes linear: 0 -- 1 -- 2.
        // 0->1: uncapped, 1ms. 1->2: 1 MB/s cap, 1ms latency.
        // Send a 1 MB message from 0 to 2. Expected timing:
        //   - 0->1: 1ms (latency, no tx)
        //   - 1->2: 1s tx + 1ms latency
        //   Total: 1.002s latency + 0 bandwidth delay from 0->1 + 1s bw from 1->2.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(1))
            .link_with_capacity(1u32, 2u32, Duration::from_millis(1), 1_000_000)
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.send_sized(NodeId(0), NodeId(2), 0, 1_000_000);

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        // 2ms link latency + 1s tx on the bottleneck = 1.002s.
        assert_eq!(stats.final_time, Time::from_millis(1002));
    }

    #[test]
    fn per_link_bandwidth_shared_across_flows() {
        // Two senders both flow through a shared bottleneck link 1->2.
        // Topology: 0 --uncapped-- 1 --1 MB/s-- 2
        //           3 --uncapped--/
        // Each sends a 500 KB message at t=0 to node 2. The bottleneck
        // serializes them: first arrives at ~501ms, second at ~1001ms.
        let topo = TopologyBuilder::new(4)
            .link(0u32, 1u32, Duration::from_millis(1))
            .link(3u32, 1u32, Duration::from_millis(1))
            .link_with_capacity(1u32, 2u32, Duration::from_millis(0), 1_000_000)
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.send_sized(NodeId(0), NodeId(2), 0, 500_000);
        net.send_sized(NodeId(3), NodeId(2), 1, 500_000);

        let mut arrivals = Vec::new();
        net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                arrivals.push((msg.payload, ctx.now()));
            }
        });
        arrivals.sort_by_key(|(_, t)| *t);

        assert_eq!(arrivals.len(), 2);
        // First message: 1ms ingress + 500ms bw = 501ms.
        assert_eq!(arrivals[0].1, Time::from_millis(501));
        // Second message: queued behind the first on link 1->2 — departs
        // the link at 1s instead of 500ms, so arrival is 1001ms.
        assert_eq!(arrivals[1].1, Time::from_millis(1001));
    }

    #[test]
    fn per_link_and_pair_bandwidth_stack() {
        // Pair-level cap 2 MB/s at endpoints + per-link cap 1 MB/s on the
        // wire. The tighter cap (per-link) dominates for the tx time, plus
        // the pair adds its own serialization at the source.
        let topo = TopologyBuilder::new(2)
            .link_with_capacity(0u32, 1u32, Duration::from_millis(0), 1_000_000)
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.set_bandwidth(NodeId(0), NodeId(1), 2_000_000);

        // Send 1 MB. Pair tx = 0.5s. Per-link tx = 1s.
        // Delivery = 0 + 0.5s (pair) + 1s (link tx) + 0 latency = 1.5s.
        net.send_sized(NodeId(0), NodeId(1), 0, 1_000_000);

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.final_time, Time::from_millis(1500));
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
    fn unbounded_buffer_matches_prior_behavior() {
        // A link with bandwidth but no buffer: any number of enqueued
        // sends must be admitted (serialized, never dropped).
        let topo = TopologyBuilder::new(2)
            .link_with_capacity(0u32, 1u32, Duration::from_millis(0), 1_000_000)
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        for i in 0..5 {
            net.send_sized(NodeId(0), NodeId(1), i, 1_000_000);
        }
        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 5);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn buffer_overflow_drops_excess_sends() {
        // 1 MB/s link with a 2 MB buffer: admits one in-flight + one
        // queued (2 MB total un-transmitted); the third concurrent send
        // would push the buffer to 3 MB and is therefore dropped.
        let topo = TopologyBuilder::new(2)
            .link_with_capacity_and_buffer(
                0u32,
                1u32,
                Duration::from_millis(0),
                1_000_000,
                2_000_000,
            )
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        let a = net.send_sized(NodeId(0), NodeId(1), 0, 1_000_000);
        let b = net.send_sized(NodeId(0), NodeId(1), 1, 1_000_000);
        let c = net.send_sized(NodeId(0), NodeId(1), 2, 1_000_000);
        assert!(a.is_some() && b.is_some() && c.is_some());

        let mut delivered = Vec::new();
        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| match event {
            NetEvent::Deliver(msg) => delivered.push(msg.payload),
            NetEvent::Drop { message, reason } => dropped.push((message.payload, reason)),
        });

        assert_eq!(stats.messages_delivered, 2);
        assert_eq!(stats.messages_dropped, 1);
        assert_eq!(delivered, vec![0, 1]);
        assert_eq!(dropped, vec![(2, DropReason::BufferOverflow)]);
    }

    #[test]
    fn buffer_drains_over_time() {
        // 1 MB/s link with a tight 1.5 MB buffer: sending a 1 MB payload
        // every 2 seconds should always be admitted because the queue
        // drains fully between sends (1 s of tx + 1 s idle).
        let topo = TopologyBuilder::new(2)
            .link_with_capacity_and_buffer(
                0u32,
                1u32,
                Duration::from_millis(0),
                1_000_000,
                1_500_000,
            )
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        for i in 0..4 {
            net.send_at_sized(
                Time::from_secs(2 * i),
                NodeId(0),
                NodeId(1),
                i as u32,
                1_000_000,
            );
        }
        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 4);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn overflow_on_shared_bottleneck_does_not_consume_upstream_capacity() {
        // Topology: 0 --uncapped-- 1 --1 MB/s, 2 MB buffer-- 2
        //           3 --uncapped--/
        //
        // Three 1 MB concurrent flows onto the bottleneck: one in flight,
        // one queued, the third must overflow. Admission is atomic, so
        // the overflow must not have mutated any link's state — a
        // delivered-counts assertion is sufficient for the externally
        // visible guarantee.
        let topo = TopologyBuilder::new(4)
            .link(0u32, 1u32, Duration::from_millis(1))
            .link(3u32, 1u32, Duration::from_millis(1))
            .link_with_capacity_and_buffer(
                1u32,
                2u32,
                Duration::from_millis(0),
                1_000_000,
                2_000_000,
            )
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.send_sized(NodeId(0), NodeId(2), 0, 1_000_000);
        net.send_sized(NodeId(3), NodeId(2), 1, 1_000_000);
        net.send_sized(NodeId(0), NodeId(2), 2, 1_000_000);

        let mut delivered = Vec::new();
        let mut dropped = Vec::new();
        net.run(|_ctx, event| match event {
            NetEvent::Deliver(msg) => delivered.push(msg.payload),
            NetEvent::Drop { message, reason } => dropped.push((message.payload, reason)),
        });

        // Exactly one overflow drop; the other two arrive.
        assert_eq!(delivered.len(), 2);
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::BufferOverflow);
    }

    #[test]
    fn red_never_drops_below_min_threshold() {
        // RED thresholds live well above where our queue occupancy stays.
        // Three 1 MB messages produce pending values {0, 1M, 2M}, all
        // strictly below min=5M, so all three must be admitted.
        let topo = TopologyBuilder::new(2)
            .link_with_policy(
                0u32,
                1u32,
                Duration::from_millis(0),
                1_000_000,
                10_000_000,
                DropPolicy::red(5_000_000, 8_000_000, 10_000_000, 1.0),
            )
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        for i in 0..3 {
            net.send_sized(NodeId(0), NodeId(1), i, 1_000_000);
        }
        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 3);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn red_hard_drops_above_max_threshold() {
        // RED: buffer=10M, min=2M, max=3M, max_prob=1.0.
        // Burst of five concurrent 1 MB sends at t=0 produces pending
        // values {0, 1M, 2M, 3M, 3M}. Admissions happen in
        // sequence, so the first three admit (below max) and the fourth
        // / fifth sit at >= max and must drop unconditionally.
        //
        // Note pending=2M is AT min: ramp probability is 0 (numerator
        // zero), which is a deterministic admit with no RNG draw.
        let topo = TopologyBuilder::new(2)
            .link_with_policy(
                0u32,
                1u32,
                Duration::from_millis(0),
                1_000_000,
                10_000_000,
                DropPolicy::red(2_000_000, 3_000_000, 10_000_000, 1.0),
            )
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        for i in 0..5 {
            net.send_sized(NodeId(0), NodeId(1), i, 1_000_000);
        }
        let mut delivered = Vec::new();
        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| match event {
            NetEvent::Deliver(msg) => delivered.push(msg.payload),
            NetEvent::Drop { message, reason } => dropped.push((message.payload, reason)),
        });
        assert_eq!(stats.messages_delivered, 3);
        assert_eq!(stats.messages_dropped, 2);
        assert_eq!(delivered, vec![0, 1, 2]);
        assert!(
            dropped
                .iter()
                .all(|(_, r)| *r == DropReason::BufferOverflow)
        );
    }

    #[test]
    fn red_ramp_drops_are_deterministic_and_nontrivial() {
        // A steep RED ramp across the whole buffer: min=0, max=5M,
        // prob=1.0. A 20-message burst will certainly produce *some*
        // early RED drops (ramp fires as soon as pending > 0), *some*
        // hard drops (once pending reaches 5M = max), and *some*
        // deliveries. With a fixed seed, the outcome must be exactly
        // reproducible across runs.
        fn run() -> (u64, u64) {
            let topo = TopologyBuilder::new(2)
                .link_with_policy(
                    0u32,
                    1u32,
                    Duration::from_millis(0),
                    1_000_000,
                    10_000_000,
                    DropPolicy::red(0, 5_000_000, 10_000_000, 1.0),
                )
                .build();

            let mut net: Network<u32> = Network::new(topo, 0xC0FFEE);
            for i in 0..20 {
                net.send_sized(NodeId(0), NodeId(1), i, 1_000_000);
            }
            let stats = net.run(|_ctx, _event| {});
            (stats.messages_delivered, stats.messages_dropped)
        }

        let (d1, x1) = run();
        let (d2, x2) = run();
        assert_eq!((d1, x1), (d2, x2), "RED admission must be deterministic");
        assert_eq!(d1 + x1, 20);
        assert!(d1 > 0, "expected some deliveries, got {d1}");
        assert!(x1 > 0, "expected some RED drops, got {x1}");
    }

    #[test]
    fn red_drops_more_than_tail_under_sustained_load() {
        // Same traffic, same link parameters — only the policy differs.
        // RED should shed more than tail drop under sustained congestion
        // because it starts dropping before the buffer is full.
        fn run_with_policy(red: bool) -> u64 {
            let topo = if red {
                TopologyBuilder::new(2).link_with_policy(
                    0u32,
                    1u32,
                    Duration::from_millis(0),
                    1_000_000,
                    10_000_000,
                    DropPolicy::red(2_000_000, 8_000_000, 10_000_000, 1.0),
                )
            } else {
                TopologyBuilder::new(2).link_with_capacity_and_buffer(
                    0u32,
                    1u32,
                    Duration::from_millis(0),
                    1_000_000,
                    10_000_000,
                )
            }
            .build();

            let mut net: Network<u32> = Network::new(topo, 7);
            for i in 0..30 {
                net.send_sized(NodeId(0), NodeId(1), i, 1_000_000);
            }
            let stats = net.run(|_ctx, _event| {});
            stats.messages_dropped
        }

        let red_drops = run_with_policy(true);
        let tail_drops = run_with_policy(false);
        assert!(
            red_drops > tail_drops,
            "expected RED ({red_drops}) to drop strictly more than TailDrop ({tail_drops})"
        );
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
    fn fail_link_drops_with_no_route_when_no_alternate() {
        // Line 0 -- 1 -- 2; failing 0-1 isolates 2 from 0.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_link(NodeId(0), NodeId(1));
        net.send(NodeId(0), NodeId(2), "stuck");

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { reason, .. } = event {
                dropped.push(reason);
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
        assert_eq!(dropped, vec![DropReason::NoRoute]);
    }

    #[test]
    fn fail_link_reroutes_via_alternate_path() {
        // Triangle: direct 0-2 = 100ms, indirect 0-1-2 = 20ms. Normal
        // routing prefers the indirect path; failing 0-1 forces fallback
        // to the direct edge and the message must still arrive.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .link(0u32, 2u32, Duration::from_millis(100))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_link(NodeId(0), NodeId(1));
        net.send(NodeId(0), NodeId(2), "long way");

        let mut arrivals = Vec::new();
        let stats = net.run(|ctx, event| {
            if let NetEvent::Deliver(_) = event {
                arrivals.push(ctx.now());
            }
        });

        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(arrivals, vec![Time::from_millis(100)]);
    }

    #[test]
    fn fail_link_directed_blocks_only_that_direction() {
        // Bidirectional link 0<->1 with 0->1 failed only.
        // Send 0->1 must drop NoRoute; send 1->0 must arrive.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_link_directed(NodeId(0), NodeId(1));
        net.send(NodeId(0), NodeId(1), "blocked");
        net.send(NodeId(1), NodeId(0), "ok");

        let mut delivered = Vec::new();
        let mut dropped = Vec::new();
        net.run(|_ctx, event| match event {
            NetEvent::Deliver(msg) => delivered.push(msg.payload),
            NetEvent::Drop { message, reason } => dropped.push((message.payload, reason)),
        });

        assert_eq!(delivered, vec!["ok"]);
        assert_eq!(dropped, vec![("blocked", DropReason::NoRoute)]);
    }

    #[test]
    fn run_context_can_fail_and_heal_mid_run() {
        // Three reply messages issued from inside the handler — these go
        // through `pending_sends` and route against the *current* topology,
        // so fail/heal toggled mid-run takes effect. Use distinct timed
        // self-ticks at t={0, 20, 40}ms to drive the fail/heal sequence.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(0u32, 2u32, Duration::from_millis(10))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        // Self-ticks on node 0 at distinct times to drive mid-run actions.
        net.send_at(Time::from_millis(0), NodeId(0), NodeId(0), 100);
        net.send_at(Time::from_millis(20), NodeId(0), NodeId(0), 200);
        net.send_at(Time::from_millis(40), NodeId(0), NodeId(0), 300);

        let mut outcomes = Vec::new();
        net.run(|ctx, event| match event {
            NetEvent::Deliver(msg) if msg.dst == NodeId(0) => match msg.payload {
                100 => {
                    // First tick: send to node 1, link is healthy — must deliver.
                    ctx.send(NodeId(0), NodeId(1), 1);
                }
                200 => {
                    // Second tick: fail the link, then send to node 1 — must drop.
                    ctx.fail_link(NodeId(0), NodeId(1));
                    ctx.send(NodeId(0), NodeId(1), 2);
                }
                300 => {
                    // Third tick: heal, then send to node 1 — must deliver again.
                    ctx.heal_link(NodeId(0), NodeId(1));
                    ctx.send(NodeId(0), NodeId(1), 3);
                }
                _ => {}
            },
            NetEvent::Deliver(msg) if msg.dst == NodeId(1) => {
                outcomes.push((msg.payload, "delivered"));
            }
            NetEvent::Drop {
                message,
                reason: DropReason::NoRoute,
            } => {
                outcomes.push((message.payload, "no_route"));
            }
            _ => {}
        });

        assert_eq!(
            outcomes,
            vec![(1, "delivered"), (2, "no_route"), (3, "delivered")]
        );
    }

    #[test]
    fn fail_link_does_not_affect_in_flight_messages() {
        // A message scheduled for delivery survives a mid-flight link failure.
        // Send 0->1 (10ms in flight), schedule a self-tick on node 1 at t=5ms,
        // and use that tick to fail the 0-1 link mid-flight. The in-flight
        // message must still arrive at t=10ms.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net: Network<&'static str> = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(1), "in_flight");
        net.send_at(Time::from_millis(5), NodeId(1), NodeId(1), "tick");

        let mut delivered = Vec::new();
        net.run(|ctx, event| {
            if let NetEvent::Deliver(msg) = event {
                if msg.payload == "tick" {
                    ctx.fail_link(NodeId(0), NodeId(1));
                }
                delivered.push((msg.payload, ctx.now()));
            }
        });

        assert!(
            delivered.contains(&("in_flight", Time::from_millis(10))),
            "in-flight message must survive mid-flight fail; got {delivered:?}"
        );
    }

    #[test]
    fn fail_node_drops_with_no_route_when_no_alternate() {
        // Line 0 -- 1 -- 2; failing node 1 isolates 2 from 0.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_node(NodeId(1));
        assert!(net.is_node_failed(NodeId(1)));
        net.send(NodeId(0), NodeId(2), "stuck");

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { reason, .. } = event {
                dropped.push(reason);
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(dropped, vec![DropReason::NoRoute]);
    }

    #[test]
    fn fail_node_reroutes_via_alternate_path() {
        // Diamond: 0 -> {1, 2} -> 3, where 1 is fast and 2 is slow.
        // Failing node 1 forces traffic through 2.
        let topo = TopologyBuilder::new(4)
            .link(0u32, 1u32, Duration::from_millis(5))
            .link(1u32, 3u32, Duration::from_millis(5))
            .link(0u32, 2u32, Duration::from_millis(50))
            .link(2u32, 3u32, Duration::from_millis(50))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_node(NodeId(1));
        net.send(NodeId(0), NodeId(3), "long way");

        let mut arrivals = Vec::new();
        let stats = net.run(|ctx, event| {
            if let NetEvent::Deliver(_) = event {
                arrivals.push(ctx.now());
            }
        });

        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(arrivals, vec![Time::from_millis(100)]);
    }

    #[test]
    fn fail_node_blocks_sends_targeting_failed_node() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.fail_node(NodeId(1));
        net.send(NodeId(0), NodeId(1), "to_dead_node");
        net.send(NodeId(1), NodeId(0), "from_dead_node");

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { message, reason } = event {
                dropped.push((message.payload, reason));
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 2);
        assert!(dropped.iter().all(|(_, r)| *r == DropReason::NoRoute));
    }

    #[test]
    fn run_context_can_fail_and_heal_node_mid_run() {
        // Diamond like the reroute test, but driven from inside the run loop.
        // Tick 1: send through fast node 1 — must deliver.
        // Tick 2: fail node 1, send — must reroute via slow node 2.
        // Tick 3: heal node 1, send — must deliver via fast path again.
        let topo = TopologyBuilder::new(4)
            .link(0u32, 1u32, Duration::from_millis(5))
            .link(1u32, 3u32, Duration::from_millis(5))
            .link(0u32, 2u32, Duration::from_millis(50))
            .link(2u32, 3u32, Duration::from_millis(50))
            .build();

        let mut net: Network<u32> = Network::new(topo, 42);
        net.send_at(Time::from_millis(0), NodeId(0), NodeId(0), 100);
        net.send_at(Time::from_millis(200), NodeId(0), NodeId(0), 200);
        net.send_at(Time::from_millis(400), NodeId(0), NodeId(0), 300);

        let mut hops_taken = Vec::new();
        net.run(|ctx, event| match event {
            NetEvent::Deliver(msg) if msg.dst == NodeId(0) => match msg.payload {
                100 => {
                    ctx.send(NodeId(0), NodeId(3), 1);
                }
                200 => {
                    ctx.fail_node(NodeId(1));
                    ctx.send(NodeId(0), NodeId(3), 2);
                }
                300 => {
                    ctx.heal_node(NodeId(1));
                    ctx.send(NodeId(0), NodeId(3), 3);
                }
                _ => {}
            },
            NetEvent::Deliver(msg) if msg.dst == NodeId(3) => {
                hops_taken.push((msg.payload, ctx.now()));
            }
            _ => {}
        });

        // First and third deliver via fast path (10ms after their dispatch);
        // second reroutes via slow path (100ms after its dispatch).
        // Dispatches are 0ms, 200ms, 400ms; arrivals are 10, 300, 410.
        assert_eq!(hops_taken.len(), 3);
        assert_eq!(hops_taken[0], (1, Time::from_millis(10)));
        assert_eq!(hops_taken[1], (2, Time::from_millis(300)));
        assert_eq!(hops_taken[2], (3, Time::from_millis(410)));
    }

    fn drop_in_flight_config() -> NetConfig {
        NetConfig {
            drop_in_flight_on_failure: true,
            ..Default::default()
        }
    }

    #[test]
    fn drop_in_flight_default_off_preserves_in_flight_messages() {
        // Without the opt-in flag, an in-flight message that is later
        // partitioned away must still deliver — this is the historic
        // behavior that existing tests depend on.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42);
        net.send(NodeId(0), NodeId(1), "in_flight");
        // Partition installed *after* the send; default config keeps the
        // already-scheduled Deliver event intact.
        net.partition(NodeId(0), NodeId(1));

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn drop_in_flight_partition_rewrites_pending_delivery() {
        // With the flag on, partitioning after the send must rewrite the
        // queued Deliver event into a Drop with reason Partitioned, fired
        // at the time of the partition call (now == 0 here).
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42).with_config(drop_in_flight_config());
        net.send(NodeId(0), NodeId(1), "in_flight");
        net.partition(NodeId(0), NodeId(1));

        let mut dropped = Vec::new();
        let stats = net.run(|ctx, event| {
            if let NetEvent::Drop { message, reason } = event {
                dropped.push((message.payload, reason, ctx.now()));
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
        assert_eq!(
            dropped,
            vec![("in_flight", DropReason::Partitioned, Time::ZERO)]
        );
    }

    #[test]
    fn drop_in_flight_link_failure_drops_when_no_alternate() {
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42).with_config(drop_in_flight_config());
        net.send(NodeId(0), NodeId(2), "doomed");
        net.fail_link(NodeId(0), NodeId(1));

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { message, reason } = event {
                dropped.push((message.payload, reason));
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(dropped, vec![("doomed", DropReason::NoRoute)]);
    }

    #[test]
    fn drop_in_flight_link_failure_preserves_when_alternate_exists() {
        // Triangle: failing 0-1 still leaves 0->2 directly via the slow edge.
        // The in-flight 0->2 message (originally taking the fast 0-1-2 path)
        // is still routable, so the sweep must leave it alone.
        let topo = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(10))
            .link(1u32, 2u32, Duration::from_millis(10))
            .link(0u32, 2u32, Duration::from_millis(100))
            .build();

        let mut net = Network::new(topo, 42).with_config(drop_in_flight_config());
        net.send(NodeId(0), NodeId(2), "still_ok");
        net.fail_link(NodeId(0), NodeId(1));

        let stats = net.run(|_ctx, _event| {});
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn drop_in_flight_node_failure_drops_when_node_is_dst() {
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net = Network::new(topo, 42).with_config(drop_in_flight_config());
        net.send(NodeId(0), NodeId(1), "to_dead");
        net.fail_node(NodeId(1));

        let mut dropped = Vec::new();
        let stats = net.run(|_ctx, event| {
            if let NetEvent::Drop { message, reason } = event {
                dropped.push((message.payload, reason));
            }
        });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(dropped, vec![("to_dead", DropReason::NoRoute)]);
    }

    #[test]
    fn drop_in_flight_works_mid_run_via_run_context() {
        // Send a message at t=0 (delivers at t=10ms). At t=5ms, a self-tick
        // causes the handler to fail the link. The sweep on the post-handler
        // hook must rewrite the in-flight message into a Drop.
        let topo = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut net: Network<&'static str> =
            Network::new(topo, 42).with_config(drop_in_flight_config());
        net.send(NodeId(0), NodeId(1), "doomed");
        net.send_at(Time::from_millis(5), NodeId(1), NodeId(1), "tick");

        let mut events_seen = Vec::new();
        net.run(|ctx, event| match event {
            NetEvent::Deliver(msg) if msg.payload == "tick" => {
                ctx.fail_link(NodeId(0), NodeId(1));
                events_seen.push(("tick_at", ctx.now()));
            }
            NetEvent::Drop { message, reason } => {
                events_seen.push((message.payload, ctx.now()));
                assert_eq!(reason, DropReason::NoRoute);
            }
            _ => {}
        });

        // Tick fired at 5ms; doomed message dropped at 5ms (time of failure),
        // not at its original 10ms delivery time.
        assert_eq!(
            events_seen,
            vec![
                ("tick_at", Time::from_millis(5)),
                ("doomed", Time::from_millis(5))
            ]
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
