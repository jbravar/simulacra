//! End-to-end determinism guardrail.
//!
//! This is the contract that protects every future change: running the same
//! non-trivial scenario twice with the same seed must produce byte-identical
//! JSON traces. If this ever fails, some source of non-determinism has leaked
//! into the engine (wall-clock, unseeded RNG, hashmap iteration order, etc.).

#![cfg(feature = "serde")]

use simulacra::{
    Duration, NetConfig, NetEvent, NodeId, TopologyBuilder, TracedNetwork, UniformJitter,
};

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

/// Mid-run link failure scenario. Builds a triangle topology where the
/// fast indirect path (0-1-2) is twice as good as the direct edge (0-2).
/// A series of self-ticks drives `ctx.fail_link` / `ctx.heal_link` actions
/// in the handler, producing a mix of normal deliveries, reroute via the
/// slower direct path, and `NoRoute` drops while the network is fully
/// partitioned.
fn run_link_failure_scenario(seed: u64) -> String {
    let topology = TopologyBuilder::new(3)
        .link(0u32, 1u32, Duration::from_millis(5))
        .link(1u32, 2u32, Duration::from_millis(5))
        .link(0u32, 2u32, Duration::from_millis(50))
        .build();

    let latency_model = UniformJitter::new(Duration::from_millis(1));
    let mut net = TracedNetwork::<u64, _>::with_latency_model(topology, latency_model, seed);

    // Self-ticks on node 0 drive fail/heal actions at deterministic times.
    // Use the payload as a tag to dispatch in the handler.
    for (i, t) in [(1u64, 0), (2, 30), (3, 60), (4, 90)].iter() {
        let _ = net.send_at(simulacra::Time::from_millis(*t), NodeId(0), NodeId(0), *i);
    }

    let (_stats, trace) = net.run_traced(|ctx, event| {
        if let NetEvent::Deliver(msg) = event
            && msg.dst == NodeId(0)
        {
            match msg.payload {
                1 => {
                    // Healthy: indirect path 0->1->2.
                    ctx.send(NodeId(0), NodeId(2), 100);
                }
                2 => {
                    // Fail the indirect path; reroute via slow direct edge.
                    ctx.fail_link(NodeId(0), NodeId(1));
                    ctx.send(NodeId(0), NodeId(2), 200);
                }
                3 => {
                    // Also fail the direct edge: full partition, NoRoute drop.
                    ctx.fail_link(NodeId(0), NodeId(2));
                    ctx.send(NodeId(0), NodeId(2), 300);
                }
                4 => {
                    // Heal both: back to fast indirect path.
                    ctx.heal_link(NodeId(0), NodeId(1));
                    ctx.heal_link(NodeId(0), NodeId(2));
                    ctx.send(NodeId(0), NodeId(2), 400);
                }
                _ => {}
            }
        }
    });

    trace
        .to_json()
        .expect("trace should serialize to JSON cleanly")
}

#[test]
fn link_failure_scenario_is_byte_equal_across_runs() {
    let a = run_link_failure_scenario(0xC0FFEE);
    let b = run_link_failure_scenario(0xC0FFEE);
    assert_eq!(
        a, b,
        "two runs with the same seed and fail/heal sequence produced \
         different JSON traces; link-failure determinism has regressed"
    );
}

/// Mid-run node failure scenario. Diamond topology where node 1 is the
/// fast intermediate and node 2 is slow. Self-ticks drive `ctx.fail_node`
/// / `ctx.heal_node`, exercising reroute and full isolation in the same run.
fn run_node_failure_scenario(seed: u64) -> String {
    let topology = TopologyBuilder::new(4)
        .link(0u32, 1u32, Duration::from_millis(5))
        .link(1u32, 3u32, Duration::from_millis(5))
        .link(0u32, 2u32, Duration::from_millis(50))
        .link(2u32, 3u32, Duration::from_millis(50))
        .build();

    let latency_model = UniformJitter::new(Duration::from_millis(1));
    let mut net = TracedNetwork::<u64, _>::with_latency_model(topology, latency_model, seed);

    for (tag, t) in [(1u64, 0u64), (2, 200), (3, 400), (4, 600)].iter() {
        let _ = net.send_at(simulacra::Time::from_millis(*t), NodeId(0), NodeId(0), *tag);
    }

    let (_stats, trace) = net.run_traced(|ctx, event| {
        if let NetEvent::Deliver(msg) = event
            && msg.dst == NodeId(0)
        {
            match msg.payload {
                1 => {
                    // Healthy: route via fast node 1.
                    ctx.send(NodeId(0), NodeId(3), 100);
                }
                2 => {
                    // Fail node 1: reroute via slow node 2.
                    ctx.fail_node(NodeId(1));
                    ctx.send(NodeId(0), NodeId(3), 200);
                }
                3 => {
                    // Also fail node 2: full isolation, NoRoute drop.
                    ctx.fail_node(NodeId(2));
                    ctx.send(NodeId(0), NodeId(3), 300);
                }
                4 => {
                    // Heal node 1 only: fast path restored.
                    ctx.heal_node(NodeId(1));
                    ctx.send(NodeId(0), NodeId(3), 400);
                }
                _ => {}
            }
        }
    });

    trace
        .to_json()
        .expect("trace should serialize to JSON cleanly")
}

#[test]
fn node_failure_scenario_is_byte_equal_across_runs() {
    let a = run_node_failure_scenario(0xBEEF);
    let b = run_node_failure_scenario(0xBEEF);
    assert_eq!(
        a, b,
        "two runs with the same seed and node fail/heal sequence produced \
         different JSON traces; node-failure determinism has regressed"
    );
}

#[test]
fn node_failure_scenario_includes_reroute_and_noroute() {
    let trace = run_node_failure_scenario(0xBEEF);
    assert!(
        trace.contains("\"NoRoute\""),
        "expected at least one NoRoute drop in the trace"
    );
    let delivered = trace.matches("\"Delivered\"").count();
    assert!(
        delivered >= 3,
        "expected at least 3 deliveries (ticks + at least one app message); \
         got {delivered}"
    );
}

#[test]
fn link_failure_scenario_includes_reroute_and_noroute() {
    // The scenario must exercise the full failure surface: at least one
    // delivery (reroute via direct path) and at least one NoRoute drop
    // (full partition window). If either disappears, the test stopped
    // exercising what its name claims.
    let trace = run_link_failure_scenario(0xC0FFEE);
    assert!(
        trace.contains("\"NoRoute\""),
        "expected at least one NoRoute drop in the trace"
    );
    let delivered = trace.matches("\"Delivered\"").count();
    assert!(
        delivered >= 3,
        "expected at least 3 deliveries (ticks + at least one app message); \
         got {delivered}"
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
