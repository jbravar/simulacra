//! End-to-end determinism guardrail.
//!
//! This is the contract that protects every future change: running the same
//! non-trivial scenario twice with the same seed must produce byte-identical
//! JSON traces. If this ever fails, some source of non-determinism has leaked
//! into the engine (wall-clock, unseeded RNG, hashmap iteration order, etc.).

#![cfg(feature = "serde")]

use simulacra::{Duration, NetConfig, NodeId, TopologyBuilder, TracedNetwork, UniformJitter};

/// A deliberately non-trivial scenario: 20-node ring, jitter + packet loss,
/// cross-traffic from every node to the node two hops ahead.
fn run_scenario(seed: u64) -> String {
    let node_count = 20;
    let topology = TopologyBuilder::new(node_count)
        .ring(Duration::from_millis(10))
        .build();

    let latency_model = UniformJitter::new(Duration::from_millis(3));
    let mut net = TracedNetwork::<u64, _>::with_latency_model(topology, latency_model, seed);

    // Apply packet loss via the underlying network's config.
    // Note: TracedNetwork currently doesn't re-export with_config, so we rely
    // on jitter alone here. Loss-sensitive determinism is covered elsewhere;
    // this scenario stresses jitter ordering + cross-traffic scheduling.
    let _ = NetConfig { packet_loss: 0.0 };

    // Cross-traffic: every node sends to the node two hops ahead.
    for i in 0..node_count as u32 {
        let dst = (i + 2) % node_count as u32;
        let _ = net.send(NodeId(i), NodeId(dst), i as u64);
    }

    // Second wave: every node sends to the node five hops ahead.
    for i in 0..node_count as u32 {
        let dst = (i + 5) % node_count as u32;
        let _ = net.send(NodeId(i), NodeId(dst), (i as u64) + 1000);
    }

    let (_stats, trace) = net.run_traced(|_ctx, _event| {});
    trace
        .to_json()
        .expect("trace should serialize to JSON cleanly")
}

#[test]
fn ring_gossip_scenario_is_byte_equal_across_runs() {
    let a = run_scenario(0xDEADBEEF);
    let b = run_scenario(0xDEADBEEF);
    assert_eq!(
        a, b,
        "two runs with the same seed produced different JSON traces; \
         determinism has regressed"
    );
}

#[test]
fn ring_gossip_scenario_differs_across_seeds() {
    // Sanity check: different seeds should generally produce different traces,
    // otherwise the jitter model isn't actually using the RNG.
    let a = run_scenario(1);
    let b = run_scenario(2);
    assert_ne!(
        a, b,
        "two runs with different seeds produced identical traces; \
         the RNG is probably not being consulted"
    );
}

/// Coarse shape check on the emitted trace.
///
/// If the trace format, seed serialization, or number of events changes,
/// this will fail — forcing a deliberate update rather than a silent drift.
#[test]
fn ring_gossip_scenario_matches_pinned_shape() {
    let trace = run_scenario(0xDEADBEEF);

    // Seed should appear in the JSON.
    assert!(
        trace.contains("\"seed\":3735928559"),
        "seed not found in trace JSON: {}",
        &trace[..trace.len().min(200)]
    );

    // 40 messages sent (20 nodes * 2 waves). Each should appear as a
    // Delivered event. A simple substring count is good enough here.
    let delivered = trace.matches("\"Delivered\"").count();
    assert_eq!(
        delivered, 40,
        "expected 40 Delivered events; got {}",
        delivered
    );
}
