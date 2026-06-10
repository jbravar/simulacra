//! End-to-end determinism guardrail.
//!
//! This is the contract that protects every future change: running the same
//! non-trivial scenario twice with the same seed must produce byte-identical
//! JSON traces. If this ever fails, some source of non-determinism has leaked
//! into the engine (wall-clock, unseeded RNG, hashmap iteration order, etc.).

#![cfg(feature = "serde")]
// Integration test harness, not library code. clippy.toml already exempts
// tests from panic/unwrap/expect/indexing/dbg; this extends the same stance
// to the remaining restriction-group lints that are pure noise in test
// scaffolding (fixed seeds, bounded loop arithmetic, trace string slicing).
#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::unreadable_literal,
    clippy::string_slice,
    reason = "integration test harness; mirrors clippy.toml allow-*-in-tests"
)]

use simulacra::{
    Duration, NetConfig, NetEvent, NodeId, TaskSimBuilder, TopologyBuilder, TracedNetwork,
    UniformJitter,
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

    // This scenario stresses jitter ordering + cross-traffic scheduling.
    // Loss-sensitive determinism is covered elsewhere; we leave packet_loss
    // at its default to keep the trace shape simple.
    let _ = NetConfig::default();

    // Cross-traffic: every node sends to the node two hops ahead.
    for i in 0..node_count as u32 {
        let dst = (i + 2) % node_count as u32;
        let _ = net.send(NodeId(i), NodeId(dst), u64::from(i));
    }

    // Second wave: every node sends to the node five hops ahead.
    for i in 0..node_count as u32 {
        let dst = (i + 5) % node_count as u32;
        let _ = net.send(NodeId(i), NodeId(dst), u64::from(i) + 1000);
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
    for (i, t) in &[(1u64, 0), (2, 30), (3, 60), (4, 90)] {
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

    for (tag, t) in &[(1u64, 0u64), (2, 200), (3, 400), (4, 600)] {
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

/// In-flight drop scenario. With the opt-in `drop_in_flight_on_failure`
/// flag, partitioning a pair mid-run rewrites already-scheduled `Deliver`
/// events into `Drop` events at the time of the partition. The scenario
/// exercises the rewrite path end-to-end through `TracedNetwork`.
fn run_drop_in_flight_scenario(seed: u64) -> String {
    use simulacra::NetConfig;

    let topology = TopologyBuilder::new(3)
        .link(0u32, 1u32, Duration::from_millis(50))
        .link(1u32, 2u32, Duration::from_millis(50))
        .build();

    let latency_model = UniformJitter::new(Duration::from_millis(2));
    let mut net = TracedNetwork::<u64, _>::with_latency_model(topology, latency_model, seed)
        .with_config(NetConfig {
            drop_in_flight_on_failure: true,
            ..Default::default()
        });

    // Five 0->2 sends staggered slightly. Each takes ~100ms (50+50) plus
    // jitter. A self-tick at t=20ms triggers the partition before any of
    // them deliver, so all five must end up as Drops at t=20ms.
    for i in 0..5u64 {
        let _ = net.send_at(simulacra::Time::from_millis(i), NodeId(0), NodeId(2), i);
    }
    let _ = net.send_at(simulacra::Time::from_millis(20), NodeId(0), NodeId(0), 999);

    let (_stats, trace) = net.run_traced(|ctx, event| {
        if let NetEvent::Deliver(msg) = event
            && msg.payload == 999
        {
            ctx.partition(NodeId(0), NodeId(2));
        }
    });

    trace
        .to_json()
        .expect("trace should serialize to JSON cleanly")
}

#[test]
fn drop_in_flight_scenario_is_byte_equal_across_runs() {
    let a = run_drop_in_flight_scenario(0xFEED);
    let b = run_drop_in_flight_scenario(0xFEED);
    assert_eq!(
        a, b,
        "two runs with the same seed and partition sequence produced \
         different JSON traces"
    );
}

#[test]
fn drop_in_flight_scenario_actually_drops_messages() {
    // Sanity check: confirm the rewrite path is wired up in TracedNetwork.
    // 5 sends at t={0..5}ms heading to node 2; partition at t=20ms.
    // Expected: 5 Partitioned drops + 1 self-tick delivered = 5 drops + 1
    // delivery in the trace.
    let trace = run_drop_in_flight_scenario(0xFEED);
    assert_eq!(trace.matches("\"Partitioned\"").count(), 5);
    assert_eq!(trace.matches("\"Delivered\"").count(), 1);
}

/// Task-layer (async `TaskSim`) determinism scenario. The async facade has
/// its own event queue and trace path, separate from `TracedNetwork`; this is
/// the guardrail for that path. A single driver node (0) on a triangle drives
/// the whole failure surface from inside its task: a healthy delivery, a
/// mid-run `fail_link` pair that forces a `NoRoute` drop, then a `heal_link`
/// pair that restores delivery — recorded via `TaskSimBuilder::with_trace`.
fn run_task_scenario(seed: u64) -> String {
    let topology = TopologyBuilder::new(3)
        .link(0u32, 1u32, Duration::from_millis(5))
        .link(1u32, 2u32, Duration::from_millis(5))
        .link(0u32, 2u32, Duration::from_millis(50))
        .build();

    let sim = TaskSimBuilder::<u64>::new(topology, seed)
        .with_trace()
        .build(|ctx| async move {
            // Only node 0 acts; the others' tasks complete immediately but
            // still receive (and so get recorded as) deliveries.
            if ctx.id().as_u32() == 0 {
                // Healthy: 0->2 routes via the fast 0-1-2 path (10ms).
                ctx.send(NodeId(2), 100).await;
                ctx.sleep(Duration::from_millis(20)).await;

                // Fail both of node 0's edges: 0->2 now has no route.
                ctx.fail_link(NodeId(0), NodeId(1));
                ctx.fail_link(NodeId(0), NodeId(2));
                ctx.send(NodeId(2), 200).await; // NoRoute drop at t=20ms

                ctx.sleep(Duration::from_millis(20)).await;

                // Heal both edges: 0->2 is deliverable again.
                ctx.heal_link(NodeId(0), NodeId(1));
                ctx.heal_link(NodeId(0), NodeId(2));
                ctx.send(NodeId(2), 300).await; // delivered at t=50ms
            }
        });

    let (_stats, trace) = sim.run_traced();
    trace
        .to_json()
        .expect("task trace should serialize to JSON cleanly")
}

#[test]
fn task_scenario_is_byte_equal_across_runs() {
    let a = run_task_scenario(0xABCD);
    let b = run_task_scenario(0xABCD);
    assert_eq!(
        a, b,
        "two task-sim runs with the same seed produced different JSON traces; \
         task-layer determinism has regressed"
    );
}

#[test]
fn task_scenario_includes_delivery_and_noroute_drop() {
    // The scenario must exercise both sides of the task trace: real
    // deliveries and a NoRoute drop while node 0 is fully cut off.
    let trace = run_task_scenario(0xABCD);
    assert!(
        trace.contains("\"NoRoute\""),
        "expected a NoRoute drop after both of node 0's links failed: {trace}"
    );
    let delivered = trace.matches("\"Delivered\"").count();
    assert_eq!(
        delivered, 2,
        "expected exactly 2 deliveries (messages 100 and 300); got {delivered}"
    );
}

/// Task-layer in-flight-drop scenario. Node 0 fires four 0->2 messages (each
/// ~100ms in flight), then partitions the pair at t=20ms before any deliver.
/// With `drop_in_flight_on_failure`, the sweep rewrites all four pending
/// `Deliver` events into `Partitioned` drops — exercising the task-layer
/// `EventQueue` rewrite path under tracing.
fn run_task_drop_in_flight_scenario(seed: u64) -> String {
    let topology = TopologyBuilder::new(3)
        .link(0u32, 1u32, Duration::from_millis(50))
        .link(1u32, 2u32, Duration::from_millis(50))
        .build();

    let sim = TaskSimBuilder::<u64>::new(topology, seed)
        .with_trace()
        .drop_in_flight_on_failure()
        .build(|ctx| async move {
            if ctx.id() == NodeId(0) {
                for i in 0..4u64 {
                    ctx.send(NodeId(2), i).await;
                }
                ctx.sleep(Duration::from_millis(20)).await;
                ctx.partition(NodeId(0), NodeId(2));
            }
        });

    let (_stats, trace) = sim.run_traced();
    trace
        .to_json()
        .expect("task drop-in-flight trace should serialize to JSON cleanly")
}

#[test]
fn task_drop_in_flight_is_byte_equal_across_runs() {
    let a = run_task_drop_in_flight_scenario(0x1234);
    let b = run_task_drop_in_flight_scenario(0x1234);
    assert_eq!(
        a, b,
        "two task-sim drop-in-flight runs with the same seed produced \
         different JSON traces; the rewrite path is non-deterministic"
    );
}

#[test]
fn task_drop_in_flight_drops_all_pending() {
    let trace = run_task_drop_in_flight_scenario(0x1234);
    assert_eq!(
        trace.matches("\"Partitioned\"").count(),
        4,
        "expected all four in-flight messages swept into Partitioned drops"
    );
    assert_eq!(
        trace.matches("\"Delivered\"").count(),
        0,
        "no message should have been delivered after the partition"
    );
}

/// Task-layer scheduled-failure scenario. Drives the whole failure surface
/// declaratively via `TaskSim::schedule_failure` (the engine under
/// `Scenario::fail_at`) instead of from inside node tasks: both of node 0's
/// edges fail at t=10ms and heal at t=40ms. Node 0 sends before the failure
/// (delivered), during the outage (`NoRoute` drop), and after the heal
/// (delivered).
fn run_task_scheduled_failure_scenario(seed: u64) -> String {
    use simulacra::{FailureAction, Time};

    let topology = TopologyBuilder::new(3)
        .link(0u32, 1u32, Duration::from_millis(5))
        .link(1u32, 2u32, Duration::from_millis(5))
        .link(0u32, 2u32, Duration::from_millis(50))
        .build();

    let mut sim = TaskSimBuilder::<u64>::new(topology, seed)
        .with_trace()
        .build(|ctx| async move {
            if ctx.id() == NodeId(0) {
                ctx.send(NodeId(2), 1).await; // t=0: delivered
                ctx.sleep(Duration::from_millis(20)).await;
                ctx.send(NodeId(2), 2).await; // t=20: cut off -> NoRoute drop
                ctx.sleep(Duration::from_millis(30)).await;
                ctx.send(NodeId(2), 3).await; // t=50: healed -> delivered
            }
        });

    for (a, b) in [(0u32, 1u32), (0, 2)] {
        sim.schedule_failure(
            Time::from_millis(10),
            FailureAction::FailLink(NodeId(a), NodeId(b)),
        );
        sim.schedule_failure(
            Time::from_millis(40),
            FailureAction::HealLink(NodeId(a), NodeId(b)),
        );
    }

    let (_stats, trace) = sim.run_traced();
    trace
        .to_json()
        .expect("task scheduled-failure trace should serialize to JSON cleanly")
}

#[test]
fn task_scheduled_failure_is_byte_equal_across_runs() {
    let a = run_task_scheduled_failure_scenario(0x5151);
    let b = run_task_scheduled_failure_scenario(0x5151);
    assert_eq!(
        a, b,
        "two scheduled-failure runs with the same seed produced different JSON \
         traces; the ApplyFailure event path is non-deterministic"
    );
}

#[test]
fn task_scheduled_failure_includes_drop_and_recovery() {
    let trace = run_task_scheduled_failure_scenario(0x5151);
    assert!(
        trace.contains("\"NoRoute\""),
        "expected a NoRoute drop during the scheduled outage"
    );
    assert_eq!(
        trace.matches("\"Delivered\"").count(),
        2,
        "expected deliveries before the failure and after the heal"
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
        "expected 40 Delivered events; got {delivered}"
    );
}
