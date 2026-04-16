//! Network topology representation.
//!
//! A topology is a graph of nodes connected by links, where each link
//! has an associated latency.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use super::node::NodeId;
use crate::time::Duration;

/// A link between two nodes with associated latency.
#[derive(Debug, Clone, Copy)]
pub struct Link {
    /// The target node of this link.
    pub target: NodeId,
    /// Base latency for messages traversing this link.
    pub latency: Duration,
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
    /// Cached shortest paths (lazily computed).
    /// Maps (src, dst) -> (next_hop, total_latency)
    routes: HashMap<(NodeId, NodeId), Route>,
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
    pub fn new(node_count: usize) -> Self {
        Topology {
            node_count,
            adjacency: vec![Vec::new(); node_count],
            routes: HashMap::new(),
        }
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

    /// Adds a directed link from `src` to `dst` with the given latency.
    ///
    /// Clears cached routes since the topology has changed.
    pub fn add_link(&mut self, src: NodeId, dst: NodeId, latency: Duration) {
        self.routes.clear();
        if src.as_usize() < self.node_count && dst.as_usize() < self.node_count {
            self.adjacency[src.as_usize()].push(Link {
                target: dst,
                latency,
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
    /// Routes are cached for subsequent lookups.
    pub fn route(&mut self, src: NodeId, dst: NodeId) -> Option<Route> {
        if src == dst {
            return Some(Route {
                next_hop: dst,
                total_latency: Duration::ZERO,
                hop_count: 0,
            });
        }

        // Check cache first
        if let Some(&route) = self.routes.get(&(src, dst)) {
            return Some(route);
        }

        // Compute shortest path using Dijkstra's algorithm
        let route = self.compute_route(src, dst)?;
        self.routes.insert((src, dst), route);
        Some(route)
    }

    /// Computes the route without caching.
    fn compute_route(&self, src: NodeId, dst: NodeId) -> Option<Route> {
        #[derive(Clone, Copy, Eq, PartialEq)]
        struct State {
            cost: u64,
            node: NodeId,
        }

        impl Ord for State {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .cost
                    .cmp(&self.cost)
                    .then_with(|| self.node.cmp(&other.node))
            }
        }

        impl PartialOrd for State {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let n = self.node_count;
        let mut dist = vec![u64::MAX; n];
        let mut prev: Vec<Option<NodeId>> = vec![None; n];
        let mut heap = BinaryHeap::new();

        dist[src.as_usize()] = 0;
        heap.push(State { cost: 0, node: src });

        while let Some(State { cost, node }) = heap.pop() {
            if node == dst {
                break;
            }

            if cost > dist[node.as_usize()] {
                continue;
            }

            for link in &self.adjacency[node.as_usize()] {
                let next_cost = cost.saturating_add(link.latency.as_nanos());
                if next_cost < dist[link.target.as_usize()] {
                    dist[link.target.as_usize()] = next_cost;
                    prev[link.target.as_usize()] = Some(node);
                    heap.push(State {
                        cost: next_cost,
                        node: link.target,
                    });
                }
            }
        }

        // No path found
        prev[dst.as_usize()]?;

        // Reconstruct path to find next_hop and hop_count
        let mut path = vec![dst];
        let mut current = dst;
        while let Some(p) = prev[current.as_usize()] {
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
            total_latency: Duration::from_nanos(dist[dst.as_usize()]),
            hop_count,
        })
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
    pub fn clear_route_cache(&mut self) {
        self.routes.clear();
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
}
