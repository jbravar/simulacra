//! Network topology representation.
//!
//! A topology is a graph of nodes connected by links, where each link
//! has an associated latency.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use super::node::NodeId;
use crate::net::DropPolicy;
use crate::time::Duration;

/// A link between two nodes with associated latency and optional bandwidth.
#[derive(Debug, Clone, Copy)]
pub struct Link {
    /// The target node of this link.
    pub target: NodeId,
    /// Base latency for messages traversing this link.
    pub latency: Duration,
    /// Optional bandwidth cap in bytes per second. `None` means uncapped.
    pub bandwidth: Option<u64>,
    /// Optional per-link buffer capacity in bytes.
    ///
    /// Only meaningful when `bandwidth` is set. Represents the total
    /// number of *un-transmitted* bytes the link can hold at any instant,
    /// including the message currently being serialized plus all messages
    /// waiting behind it. Incoming sends whose admission would push this
    /// total past `buffer_bytes` are dropped with
    /// `DropReason::BufferOverflow`. `None` means an unbounded queue (the
    /// default for backwards compatibility).
    pub buffer_bytes: Option<u64>,
    /// Drop policy that governs admission when the link is congested.
    ///
    /// Defaults to [`DropPolicy::TailDrop`], which only drops once the
    /// buffer is actually full (current behavior). Other variants (e.g.
    /// [`DropPolicy::RandomEarlyDetection`]) can shed load probabilistically
    /// ahead of buffer exhaustion.
    pub drop_policy: DropPolicy,
}

/// Dijkstra priority-queue entry. Defined at module level so that scratch
/// `BinaryHeap`s can be stored on `Topology` without leaking the type.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DijkstraState {
    cost: u64,
    node: NodeId,
}

impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap on cost, tie-break by node id for determinism.
        other
            .cost
            .cmp(&self.cost)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A network topology represented as an adjacency list.
///
/// Supports both directed and undirected links, shortest-path routing,
/// and configurable link latencies.
#[derive(Debug, Clone)]
pub struct Topology {
    /// Number of nodes in the topology.
    node_count: usize,
    /// Adjacency list: for each node, a list of outgoing links.
    adjacency: Vec<Vec<Link>>,
    /// Dense route cache indexed by `src * node_count + dst`. Lazily
    /// allocated on the first successful `route()` call and torn down
    /// (set back to `None`) whenever the topology is mutated. Individual
    /// unreachable pairs are left as `None` within the cache since
    /// recomputing them is cheap and avoids a three-state enum.
    routes: Option<Vec<Option<Route>>>,
    /// Directed edges that are currently failed and must be excluded from
    /// routing. Stored as `(src, dst)` pairs; symmetric failures hold both
    /// orderings. Membership is treated as advisory by Dijkstra — a failed
    /// link that doesn't physically exist is simply a no-op for routing,
    /// which lets callers "pre-fail" links before adding them.
    failed_links: HashSet<(NodeId, NodeId)>,
    /// Nodes that are currently down. A failed node is excluded from
    /// routing entirely: any route with a failed src, dst, or intermediate
    /// hop returns `None`. This is orthogonal to `failed_links` — healing
    /// a node does not implicitly heal individual link failures, and vice
    /// versa.
    failed_nodes: HashSet<NodeId>,
    /// Scratch buffer for Dijkstra distances, sized to `node_count`.
    dist_scratch: Vec<u64>,
    /// Scratch buffer for Dijkstra predecessors, sized to `node_count`.
    prev_scratch: Vec<Option<NodeId>>,
    /// Scratch priority queue for Dijkstra, reused across `route()` calls.
    heap_scratch: BinaryHeap<DijkstraState>,
}

/// A computed route between two nodes.
#[derive(Debug, Clone, Copy)]
pub struct Route {
    /// The next hop on the path (first node after source).
    pub next_hop: NodeId,
    /// Total latency along the shortest path.
    pub total_latency: Duration,
    /// Number of hops in the path.
    pub hop_count: u32,
}

impl Topology {
    /// Creates a new topology with the given number of nodes and no links.
    ///
    /// The route cache is not allocated here; it is created lazily on the
    /// first successful `route()` call. This keeps construction and
    /// mutation (`add_link`, `add_bidi_link`) O(1) with respect to
    /// `node_count` even though the cache itself is O(N²).
    pub fn new(node_count: usize) -> Self {
        Topology {
            node_count,
            adjacency: vec![Vec::new(); node_count],
            routes: None,
            failed_links: HashSet::new(),
            failed_nodes: HashSet::new(),
            dist_scratch: Vec::with_capacity(node_count),
            prev_scratch: Vec::with_capacity(node_count),
            heap_scratch: BinaryHeap::new(),
        }
    }

    #[inline]
    fn route_idx(&self, src: NodeId, dst: NodeId) -> usize {
        src.as_usize() * self.node_count + dst.as_usize()
    }

    /// Returns the number of nodes in the topology.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Returns an iterator over all node IDs.
    pub fn nodes(&self) -> impl Iterator<Item = NodeId> {
        (0..self.node_count).map(|i| NodeId::new(i as u32))
    }

    /// Adds a directed link from `src` to `dst` with the given latency and
    /// no bandwidth cap.
    ///
    /// Clears cached routes since the topology has changed.
    pub fn add_link(&mut self, src: NodeId, dst: NodeId, latency: Duration) {
        self.add_link_full(src, dst, latency, None, None);
    }

    /// Adds a directed link with an optional bandwidth cap (bytes/sec).
    pub fn add_link_with_bandwidth(
        &mut self,
        src: NodeId,
        dst: NodeId,
        latency: Duration,
        bandwidth: Option<u64>,
    ) {
        self.add_link_full(src, dst, latency, bandwidth, None);
    }

    /// Adds a directed link with an optional bandwidth cap and an optional
    /// per-link buffer in bytes. A buffer only has an effect when
    /// `bandwidth` is also set. Uses [`DropPolicy::TailDrop`] by default.
    pub fn add_link_full(
        &mut self,
        src: NodeId,
        dst: NodeId,
        latency: Duration,
        bandwidth: Option<u64>,
        buffer_bytes: Option<u64>,
    ) {
        self.add_link_full_with_policy(
            src,
            dst,
            latency,
            bandwidth,
            buffer_bytes,
            DropPolicy::TailDrop,
        );
    }

    /// Adds a directed link with an explicit drop policy. Like
    /// [`Topology::add_link_full`] but lets callers plug in AQM variants
    /// such as [`DropPolicy::RandomEarlyDetection`].
    pub fn add_link_full_with_policy(
        &mut self,
        src: NodeId,
        dst: NodeId,
        latency: Duration,
        bandwidth: Option<u64>,
        buffer_bytes: Option<u64>,
        drop_policy: DropPolicy,
    ) {
        self.clear_route_cache();
        if src.as_usize() < self.node_count && dst.as_usize() < self.node_count {
            self.adjacency[src.as_usize()].push(Link {
                target: dst,
                latency,
                bandwidth,
                buffer_bytes,
                drop_policy,
            });
        }
    }

    /// Adds a bidirectional link between `a` and `b` with the given latency.
    ///
    /// This is equivalent to adding two directed links.
    pub fn add_bidi_link(&mut self, a: NodeId, b: NodeId, latency: Duration) {
        self.add_link(a, b, latency);
        self.add_link(b, a, latency);
    }

    /// Returns the first directed `Link` from `src` to `dst`, if one exists.
    ///
    /// Used by the network layer to look up per-hop properties (latency,
    /// bandwidth) while walking a route. If the graph has parallel edges,
    /// this returns the one that was inserted first.
    pub fn link_between(&self, src: NodeId, dst: NodeId) -> Option<&Link> {
        self.adjacency
            .get(src.as_usize())?
            .iter()
            .find(|l| l.target == dst)
    }

    /// Returns the outgoing links from a node.
    pub fn links_from(&self, node: NodeId) -> &[Link] {
        self.adjacency
            .get(node.as_usize())
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Computes and returns the shortest-path route from `src` to `dst`.
    ///
    /// Returns `None` if there is no path between the nodes.
    /// Routes are cached for subsequent lookups. The cache is allocated
    /// lazily on the first call, so topologies that never route don't pay
    /// the O(N²) allocation cost.
    pub fn route(&mut self, src: NodeId, dst: NodeId) -> Option<Route> {
        if src == dst {
            return Some(Route {
                next_hop: dst,
                total_latency: Duration::ZERO,
                hop_count: 0,
            });
        }

        let idx = self.route_idx(src, dst);

        // Fast path: cache populated and hit.
        if let Some(cache) = self.routes.as_ref()
            && let Some(route) = cache[idx]
        {
            return Some(route);
        }

        // Slow path: compute and (lazily) allocate the cache.
        let route = self.compute_route(src, dst)?;
        let n = self.node_count;
        let cache = self.routes.get_or_insert_with(|| vec![None; n * n]);
        cache[idx] = Some(route);
        Some(route)
    }

    /// Computes the route without caching, reusing the scratch buffers on
    /// `self`.
    fn compute_route(&mut self, src: NodeId, dst: NodeId) -> Option<Route> {
        let n = self.node_count;

        // Fast paths: a failed source can't transmit and a failed
        // destination can't receive, so no route exists either way.
        if self.failed_nodes.contains(&src) || self.failed_nodes.contains(&dst) {
            return None;
        }

        // Reset scratch buffers without freeing their allocations.
        self.dist_scratch.clear();
        self.dist_scratch.resize(n, u64::MAX);
        self.prev_scratch.clear();
        self.prev_scratch.resize(n, None);
        self.heap_scratch.clear();

        self.dist_scratch[src.as_usize()] = 0;
        self.heap_scratch.push(DijkstraState { cost: 0, node: src });

        while let Some(DijkstraState { cost, node }) = self.heap_scratch.pop() {
            if node == dst {
                break;
            }

            if cost > self.dist_scratch[node.as_usize()] {
                continue;
            }

            for link in &self.adjacency[node.as_usize()] {
                if self.failed_links.contains(&(node, link.target))
                    || self.failed_nodes.contains(&link.target)
                {
                    continue;
                }
                let next_cost = cost.saturating_add(link.latency.as_nanos());
                if next_cost < self.dist_scratch[link.target.as_usize()] {
                    self.dist_scratch[link.target.as_usize()] = next_cost;
                    self.prev_scratch[link.target.as_usize()] = Some(node);
                    self.heap_scratch.push(DijkstraState {
                        cost: next_cost,
                        node: link.target,
                    });
                }
            }
        }

        // No path found
        self.prev_scratch[dst.as_usize()]?;

        // Reconstruct path to find next_hop and hop_count. Path length is
        // bounded by the diameter of the graph (typically small), so we
        // allocate a fresh Vec here rather than maintaining a scratch.
        let mut path = Vec::with_capacity(8);
        path.push(dst);
        let mut current = dst;
        while let Some(p) = self.prev_scratch[current.as_usize()] {
            path.push(p);
            current = p;
            if current == src {
                break;
            }
        }

        if current != src {
            return None;
        }

        path.reverse();
        let next_hop = path[1]; // First node after src
        let hop_count = (path.len() - 1) as u32;

        Some(Route {
            next_hop,
            total_latency: Duration::from_nanos(self.dist_scratch[dst.as_usize()]),
            hop_count,
        })
    }

    /// Marks the directed edge `src -> dst` as failed.
    ///
    /// Failed edges are excluded from routing: subsequent `route()` calls
    /// behave as if the edge does not exist (the route may reroute around
    /// it, or return `None` if no alternative path remains).
    ///
    /// Failure is recorded as a `(src, dst)` pair regardless of whether such
    /// a link physically exists in the adjacency list — pre-failing a link
    /// before it's added is a no-op for routing but is still tracked, so a
    /// later `add_link` followed by a `route()` will see the link as failed.
    ///
    /// Use [`Topology::fail_link`] for the symmetric (bidirectional) case.
    /// In-flight messages already scheduled for delivery are not affected;
    /// only future routing decisions are.
    pub fn fail_link_directed(&mut self, src: NodeId, dst: NodeId) {
        if self.failed_links.insert((src, dst)) {
            self.clear_route_cache();
        }
    }

    /// Restores a previously-failed directed edge `src -> dst`.
    pub fn heal_link_directed(&mut self, src: NodeId, dst: NodeId) {
        if self.failed_links.remove(&(src, dst)) {
            self.clear_route_cache();
        }
    }

    /// Returns `true` if the directed edge `src -> dst` is currently marked
    /// as failed.
    pub fn is_link_failed_directed(&self, src: NodeId, dst: NodeId) -> bool {
        self.failed_links.contains(&(src, dst))
    }

    /// Marks the link between `a` and `b` as failed in both directions.
    ///
    /// Convenience wrapper that calls `fail_link_directed` for both
    /// `(a, b)` and `(b, a)`. For asymmetric failures, use
    /// [`Topology::fail_link_directed`].
    pub fn fail_link(&mut self, a: NodeId, b: NodeId) {
        self.fail_link_directed(a, b);
        self.fail_link_directed(b, a);
    }

    /// Restores a previously-failed bidirectional link between `a` and `b`.
    pub fn heal_link(&mut self, a: NodeId, b: NodeId) {
        self.heal_link_directed(a, b);
        self.heal_link_directed(b, a);
    }

    /// Returns `true` if both directions of the link between `a` and `b`
    /// are currently failed. For an asymmetric check, use
    /// [`Topology::is_link_failed_directed`].
    pub fn is_link_failed(&self, a: NodeId, b: NodeId) -> bool {
        self.is_link_failed_directed(a, b) && self.is_link_failed_directed(b, a)
    }

    /// Marks `node` as failed. A failed node is excluded from routing
    /// entirely: any route whose source, destination, or any intermediate
    /// hop is failed returns `None`, and consequently sends targeting it
    /// drop with [`DropReason::NoRoute`](crate::net::DropReason::NoRoute).
    ///
    /// Node failure is orthogonal to link failure; healing a node does not
    /// implicitly heal any individual link failures involving it. In-flight
    /// messages already scheduled for delivery are not affected.
    pub fn fail_node(&mut self, node: NodeId) {
        if self.failed_nodes.insert(node) {
            self.clear_route_cache();
        }
    }

    /// Restores a previously-failed node.
    pub fn heal_node(&mut self, node: NodeId) {
        if self.failed_nodes.remove(&node) {
            self.clear_route_cache();
        }
    }

    /// Returns `true` if `node` is currently marked as failed.
    pub fn is_node_failed(&self, node: NodeId) -> bool {
        self.failed_nodes.contains(&node)
    }

    /// Precomputes all shortest paths (Floyd-Warshall style, but using Dijkstra from each node).
    ///
    /// Call this after building the topology if you want to avoid lazy computation costs.
    pub fn precompute_routes(&mut self) {
        for src in 0..self.node_count {
            for dst in 0..self.node_count {
                if src != dst {
                    let _ = self.route(NodeId::new(src as u32), NodeId::new(dst as u32));
                }
            }
        }
    }

    /// Clears the route cache.
    ///
    /// Drops the entire cache allocation (O(1)). The next `route()` call
    /// will reallocate lazily.
    pub fn clear_route_cache(&mut self) {
        self.routes = None;
    }
}

/// Builder for creating common topology patterns.
#[derive(Debug)]
pub struct TopologyBuilder {
    topology: Topology,
}

impl TopologyBuilder {
    /// Creates a new builder with the given number of nodes.
    pub fn new(node_count: usize) -> Self {
        TopologyBuilder {
            topology: Topology::new(node_count),
        }
    }

    /// Adds a bidirectional link.
    pub fn link(mut self, a: impl Into<NodeId>, b: impl Into<NodeId>, latency: Duration) -> Self {
        self.topology.add_bidi_link(a.into(), b.into(), latency);
        self
    }

    /// Adds a directed link.
    pub fn directed_link(
        mut self,
        src: impl Into<NodeId>,
        dst: impl Into<NodeId>,
        latency: Duration,
    ) -> Self {
        self.topology.add_link(src.into(), dst.into(), latency);
        self
    }

    /// Adds a bidirectional link with different latencies in each direction.
    ///
    /// `lat_ab` is the latency from `a` to `b`; `lat_ba` is the latency from
    /// `b` to `a`. Useful for modeling asymmetric paths (e.g., different
    /// up/down bandwidth, satellite links).
    pub fn link_asymmetric(
        mut self,
        a: impl Into<NodeId>,
        b: impl Into<NodeId>,
        lat_ab: Duration,
        lat_ba: Duration,
    ) -> Self {
        let a = a.into();
        let b = b.into();
        self.topology.add_link(a, b, lat_ab);
        self.topology.add_link(b, a, lat_ba);
        self
    }

    /// Adds a bidirectional link with a symmetric bandwidth cap (bytes/sec).
    ///
    /// Per-link bandwidth is applied by [`Network::send_sized`](crate::net::Network::send_sized)
    /// and stacks independently per direction (each direction has its own
    /// serialization queue in the network runtime).
    pub fn link_with_capacity(
        mut self,
        a: impl Into<NodeId>,
        b: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
    ) -> Self {
        let a = a.into();
        let b = b.into();
        let bw = Some(bandwidth_bps);
        self.topology.add_link_with_bandwidth(a, b, latency, bw);
        self.topology.add_link_with_bandwidth(b, a, latency, bw);
        self
    }

    /// Adds a directed link with a bandwidth cap in bytes/sec.
    pub fn directed_link_with_capacity(
        mut self,
        src: impl Into<NodeId>,
        dst: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
    ) -> Self {
        self.topology
            .add_link_with_bandwidth(src.into(), dst.into(), latency, Some(bandwidth_bps));
        self
    }

    /// Adds a bidirectional link with a bandwidth cap and a per-link buffer
    /// (in bytes) applied in each direction.
    ///
    /// Messages whose arrival would push the in-flight byte count over
    /// `buffer_bytes` are dropped with
    /// [`DropReason::BufferOverflow`](crate::net::DropReason::BufferOverflow).
    pub fn link_with_capacity_and_buffer(
        mut self,
        a: impl Into<NodeId>,
        b: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
        buffer_bytes: u64,
    ) -> Self {
        let a = a.into();
        let b = b.into();
        let bw = Some(bandwidth_bps);
        let buf = Some(buffer_bytes);
        self.topology.add_link_full(a, b, latency, bw, buf);
        self.topology.add_link_full(b, a, latency, bw, buf);
        self
    }

    /// Adds a directed link with a bandwidth cap and a per-link buffer.
    pub fn directed_link_with_capacity_and_buffer(
        mut self,
        src: impl Into<NodeId>,
        dst: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
        buffer_bytes: u64,
    ) -> Self {
        self.topology.add_link_full(
            src.into(),
            dst.into(),
            latency,
            Some(bandwidth_bps),
            Some(buffer_bytes),
        );
        self
    }

    /// Adds a bidirectional link with a bandwidth cap, a per-link buffer,
    /// and an explicit [`DropPolicy`].
    ///
    /// Use [`DropPolicy::red`] to construct a validated RED configuration,
    /// or [`DropPolicy::TailDrop`] for the default FIFO behavior
    /// (equivalent to [`TopologyBuilder::link_with_capacity_and_buffer`]).
    pub fn link_with_policy(
        mut self,
        a: impl Into<NodeId>,
        b: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
        buffer_bytes: u64,
        policy: DropPolicy,
    ) -> Self {
        let a = a.into();
        let b = b.into();
        self.topology.add_link_full_with_policy(
            a,
            b,
            latency,
            Some(bandwidth_bps),
            Some(buffer_bytes),
            policy,
        );
        self.topology.add_link_full_with_policy(
            b,
            a,
            latency,
            Some(bandwidth_bps),
            Some(buffer_bytes),
            policy,
        );
        self
    }

    /// Directed version of [`TopologyBuilder::link_with_policy`].
    pub fn directed_link_with_policy(
        mut self,
        src: impl Into<NodeId>,
        dst: impl Into<NodeId>,
        latency: Duration,
        bandwidth_bps: u64,
        buffer_bytes: u64,
        policy: DropPolicy,
    ) -> Self {
        self.topology.add_link_full_with_policy(
            src.into(),
            dst.into(),
            latency,
            Some(bandwidth_bps),
            Some(buffer_bytes),
            policy,
        );
        self
    }

    /// Creates a ring topology where each node connects to its neighbors.
    pub fn ring(mut self, latency: Duration) -> Self {
        let n = self.topology.node_count;
        for i in 0..n {
            let next = (i + 1) % n;
            self.topology
                .add_bidi_link(NodeId::new(i as u32), NodeId::new(next as u32), latency);
        }
        self
    }

    /// Creates a fully connected mesh where every node connects to every other node.
    pub fn full_mesh(mut self, latency: Duration) -> Self {
        let n = self.topology.node_count;
        for i in 0..n {
            for j in (i + 1)..n {
                self.topology
                    .add_bidi_link(NodeId::new(i as u32), NodeId::new(j as u32), latency);
            }
        }
        self
    }

    /// Creates a star topology with node 0 as the hub.
    pub fn star(mut self, latency: Duration) -> Self {
        let n = self.topology.node_count;
        let hub = NodeId::new(0);
        for i in 1..n {
            self.topology
                .add_bidi_link(hub, NodeId::new(i as u32), latency);
        }
        self
    }

    /// Creates a line topology (chain).
    pub fn line(mut self, latency: Duration) -> Self {
        let n = self.topology.node_count;
        for i in 0..(n - 1) {
            self.topology.add_bidi_link(
                NodeId::new(i as u32),
                NodeId::new((i + 1) as u32),
                latency,
            );
        }
        self
    }

    /// Builds the topology.
    pub fn build(self) -> Topology {
        self.topology
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_topology() {
        let topo = Topology::new(5);
        assert_eq!(topo.node_count(), 5);
        assert!(topo.links_from(NodeId(0)).is_empty());
    }

    #[test]
    fn add_links() {
        let mut topo = Topology::new(3);
        topo.add_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(20));

        assert_eq!(topo.links_from(NodeId(0)).len(), 1);
        assert_eq!(topo.links_from(NodeId(1)).len(), 1); // only to 2
        assert_eq!(topo.links_from(NodeId(2)).len(), 1); // back to 1
    }

    #[test]
    fn direct_route() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(20));

        let route = topo.route(NodeId(0), NodeId(1)).unwrap();
        assert_eq!(route.next_hop, NodeId(1));
        assert_eq!(route.total_latency, Duration::from_millis(10));
        assert_eq!(route.hop_count, 1);
    }

    #[test]
    fn multi_hop_route() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(20));

        let route = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(route.next_hop, NodeId(1));
        assert_eq!(route.total_latency, Duration::from_millis(30));
        assert_eq!(route.hop_count, 2);
    }

    #[test]
    fn shortest_path_selection() {
        // Triangle: 0-1 (10ms), 1-2 (10ms), 0-2 (100ms)
        // Shortest 0->2 should go through 1
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(0), NodeId(2), Duration::from_millis(100));

        let route = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(route.next_hop, NodeId(1)); // Goes through 1, not direct
        assert_eq!(route.total_latency, Duration::from_millis(20));
    }

    #[test]
    fn no_route() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        // Node 2 is disconnected

        assert!(topo.route(NodeId(0), NodeId(2)).is_none());
    }

    #[test]
    fn self_route() {
        let mut topo = Topology::new(3);
        let route = topo.route(NodeId(1), NodeId(1)).unwrap();
        assert_eq!(route.hop_count, 0);
        assert_eq!(route.total_latency, Duration::ZERO);
    }

    #[test]
    fn builder_ring() {
        let mut topo = TopologyBuilder::new(4)
            .ring(Duration::from_millis(10))
            .build();

        // In a ring: 0-1-2-3-0
        // Route 0->2 should be 2 hops
        let route = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(route.hop_count, 2);
        assert_eq!(route.total_latency, Duration::from_millis(20));
    }

    #[test]
    fn builder_star() {
        let mut topo = TopologyBuilder::new(5)
            .star(Duration::from_millis(10))
            .build();

        // All paths go through hub (node 0)
        let route = topo.route(NodeId(1), NodeId(3)).unwrap();
        assert_eq!(route.next_hop, NodeId(0)); // Must go through hub
        assert_eq!(route.hop_count, 2);
    }

    #[test]
    fn builder_full_mesh() {
        let mut topo = TopologyBuilder::new(4)
            .full_mesh(Duration::from_millis(10))
            .build();

        // Every node directly connected
        for i in 0..4 {
            for j in 0..4 {
                if i != j {
                    let route = topo.route(NodeId::new(i), NodeId::new(j)).unwrap();
                    assert_eq!(route.hop_count, 1);
                }
            }
        }
    }

    #[test]
    fn route_caching() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(20));

        // First call computes
        let route1 = topo.route(NodeId(0), NodeId(2)).unwrap();
        // Second call uses cache
        let route2 = topo.route(NodeId(0), NodeId(2)).unwrap();

        assert_eq!(route1.total_latency, route2.total_latency);
    }

    #[test]
    fn fail_link_reroutes_around_failure() {
        // Triangle: 0-1 (10ms), 1-2 (10ms), 0-2 (100ms).
        // Direct 0->2 normally takes the 0-1-2 path (20ms). Fail the 0-1
        // link and routing must fall back to the slower direct edge.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(0), NodeId(2), Duration::from_millis(100));

        assert_eq!(
            topo.route(NodeId(0), NodeId(2)).unwrap().total_latency,
            Duration::from_millis(20)
        );

        topo.fail_link(NodeId(0), NodeId(1));
        assert!(topo.is_link_failed(NodeId(0), NodeId(1)));

        let rerouted = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(rerouted.next_hop, NodeId(2));
        assert_eq!(rerouted.total_latency, Duration::from_millis(100));
    }

    #[test]
    fn fail_link_drops_to_no_route_when_no_alt() {
        // Line: 0 -- 1 -- 2. Failing 0-1 isolates 2 from 0 entirely.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        assert!(topo.route(NodeId(0), NodeId(2)).is_some());
        topo.fail_link(NodeId(0), NodeId(1));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());
    }

    #[test]
    fn heal_link_restores_route() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_link(NodeId(0), NodeId(1));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());

        topo.heal_link(NodeId(0), NodeId(1));
        assert!(!topo.is_link_failed(NodeId(0), NodeId(1)));
        let route = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(route.total_latency, Duration::from_millis(20));
    }

    #[test]
    fn fail_link_directed_is_asymmetric() {
        // Symmetric link 0-1 with directed-only failure of 0->1.
        // 1->0 must still route directly; 0->1 falls through to NoRoute.
        let mut topo = Topology::new(2);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));

        topo.fail_link_directed(NodeId(0), NodeId(1));
        assert!(topo.is_link_failed_directed(NodeId(0), NodeId(1)));
        assert!(!topo.is_link_failed_directed(NodeId(1), NodeId(0)));
        // Symmetric query reports false because only one direction is down.
        assert!(!topo.is_link_failed(NodeId(0), NodeId(1)));

        assert!(topo.route(NodeId(0), NodeId(1)).is_none());
        assert_eq!(
            topo.route(NodeId(1), NodeId(0)).unwrap().total_latency,
            Duration::from_millis(10)
        );
    }

    #[test]
    fn fail_link_invalidates_route_cache() {
        // Without cache invalidation, the second route() would still return
        // the pre-failure path. Verify by routing first (populating the
        // cache), then failing, then routing again.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(0), NodeId(2), Duration::from_millis(100));

        let _warm = topo.route(NodeId(0), NodeId(2)).unwrap();
        topo.fail_link(NodeId(0), NodeId(1));
        assert_eq!(
            topo.route(NodeId(0), NodeId(2)).unwrap().total_latency,
            Duration::from_millis(100)
        );
    }

    #[test]
    fn fail_node_blocks_all_routing_through_it() {
        // Diamond: 0 -> {1, 2} -> 3, where node 1 is the cheap intermediate
        // (5ms each) and node 2 is the expensive one (50ms each). Failing
        // node 1 must reroute traffic via node 2.
        let mut topo = Topology::new(4);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(5));
        topo.add_bidi_link(NodeId(1), NodeId(3), Duration::from_millis(5));
        topo.add_bidi_link(NodeId(0), NodeId(2), Duration::from_millis(50));
        topo.add_bidi_link(NodeId(2), NodeId(3), Duration::from_millis(50));

        assert_eq!(
            topo.route(NodeId(0), NodeId(3)).unwrap().total_latency,
            Duration::from_millis(10)
        );

        topo.fail_node(NodeId(1));
        assert!(topo.is_node_failed(NodeId(1)));

        let rerouted = topo.route(NodeId(0), NodeId(3)).unwrap();
        assert_eq!(rerouted.next_hop, NodeId(2));
        assert_eq!(rerouted.total_latency, Duration::from_millis(100));
    }

    #[test]
    fn fail_node_isolates_when_no_alternate() {
        // Line 0 -- 1 -- 2 with node 1 as the only intermediate.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_node(NodeId(1));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());
    }

    #[test]
    fn fail_node_blocks_routes_to_and_from_node() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_node(NodeId(1));
        // Routing to a failed node fails.
        assert!(topo.route(NodeId(0), NodeId(1)).is_none());
        // Routing from a failed node fails.
        assert!(topo.route(NodeId(1), NodeId(0)).is_none());
    }

    #[test]
    fn heal_node_restores_routing() {
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_node(NodeId(1));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());

        topo.heal_node(NodeId(1));
        assert!(!topo.is_node_failed(NodeId(1)));
        assert_eq!(
            topo.route(NodeId(0), NodeId(2)).unwrap().total_latency,
            Duration::from_millis(20)
        );
    }

    #[test]
    fn fail_node_invalidates_route_cache() {
        // Cache must invalidate on fail/heal so a stale route doesn't keep
        // routing through a failed node.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        let _warm = topo.route(NodeId(0), NodeId(2)).unwrap();
        topo.fail_node(NodeId(1));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());
    }

    #[test]
    fn node_and_link_failures_are_orthogonal() {
        // Failing a node and then healing it must not also clear a link
        // failure that was independently set.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_link(NodeId(0), NodeId(1));
        topo.fail_node(NodeId(1));
        topo.heal_node(NodeId(1));

        // Link failure must still hold even though node is healed.
        assert!(topo.is_link_failed(NodeId(0), NodeId(1)));
        assert!(topo.route(NodeId(0), NodeId(2)).is_none());
    }

    #[test]
    fn fail_nonexistent_link_is_noop_for_existing_routes() {
        // Failing an edge that doesn't exist in the adjacency list must not
        // affect any actual routing decisions.
        let mut topo = Topology::new(3);
        topo.add_bidi_link(NodeId(0), NodeId(1), Duration::from_millis(10));
        topo.add_bidi_link(NodeId(1), NodeId(2), Duration::from_millis(10));

        topo.fail_link(NodeId(0), NodeId(2));
        assert!(topo.is_link_failed(NodeId(0), NodeId(2)));

        let route = topo.route(NodeId(0), NodeId(2)).unwrap();
        assert_eq!(route.total_latency, Duration::from_millis(20));
    }
}
