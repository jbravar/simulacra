//! Gossip protocol on a 5-node ring.
//!
//! Each node waits for a rumor, then forwards it to its two clockwise
//! neighbors after a small delay. Illustrates the async task facade,
//! ring topology, and message injection.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example gossip
//! ```

// Illustrative example, not load-bearing library code.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "example code: scenario-bounded arithmetic, not load-bearing"
)]

use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, TopologyBuilder};

async fn gossip_node(ctx: NodeContext<String>) {
    let my_id = ctx.id().as_u32();

    // Wait for the rumor.
    let msg = ctx.recv().await;
    println!(
        "[{:>10}] Node {} received rumor from {}: {:?}",
        ctx.now(),
        my_id,
        msg.src,
        msg.payload
    );

    // Spread to the next two nodes in the ring.
    let n1 = NodeId((my_id + 1) % 5);
    let n2 = NodeId((my_id + 2) % 5);

    ctx.sleep(Duration::from_millis(5)).await;

    let rumor = format!("Rumor via N{my_id}");
    ctx.send(n1, rumor.clone()).await;
    ctx.send(n2, rumor).await;

    println!(
        "[{:>10}] Node {} spread rumor to {} and {}",
        ctx.now(),
        my_id,
        n1,
        n2
    );
}

fn main() {
    println!("Simulacra - gossip example\n");
    println!("Simulating gossip on a 5-node ring.\n");

    let topology = TopologyBuilder::new(5)
        .ring(Duration::from_millis(10))
        .build();

    let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(gossip_node);

    // Inject the initial rumor to node 0.
    sim.inject(NodeId(0), NodeId(0), "The secret is out!".to_string());

    let stats = sim.run();

    println!("\n--- Simulation Complete ---");
    println!("Final time:         {}", stats.final_time);
    println!("Events processed:   {}", stats.events_processed);
    println!("Messages delivered: {}", stats.messages_delivered);
    println!("Task polls:         {}", stats.task_polls);
    println!("Tasks completed:    {}", stats.tasks_completed);
}
