//! Simplified leader election on a fully-connected mesh.
//!
//! Each node broadcasts its own ID, collects IDs from peers for a bounded
//! window of simulated time, and then selects the highest ID it has seen as
//! the leader. This is a deliberately simplified bully-style protocol used
//! to demonstrate `recv_timeout`, broadcasting, and convergence.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example leader_election
//! ```
//!
//! Expected: every node elects the same leader (the one with the highest ID).

use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, TopologyBuilder};

const NODE_COUNT: u32 = 5;
const ELECTION_WINDOW: Duration = Duration::from_millis(50);

async fn elect(ctx: NodeContext<u32>) {
    let my_id = ctx.id().as_u32();

    // Broadcast our ID to everyone else.
    for peer in 0..NODE_COUNT {
        if peer != my_id {
            ctx.send(NodeId(peer), my_id).await;
        }
    }

    // Collect incoming IDs until the election window expires. We expect
    // NODE_COUNT - 1 messages, but we use recv_timeout so the protocol
    // tolerates missing messages gracefully.
    let mut best = my_id;
    let deadline = ctx.now() + ELECTION_WINDOW;
    loop {
        let remaining = deadline.saturating_duration_since(ctx.now());
        if remaining.is_zero() {
            break;
        }
        match ctx.recv_timeout(remaining).await {
            Some(env) => {
                if env.payload > best {
                    best = env.payload;
                }
            }
            None => break,
        }
    }

    println!(
        "[{:>10}] Node {} elected leader: {}",
        ctx.now(),
        my_id,
        best
    );
}

fn main() {
    let topology = TopologyBuilder::new(NODE_COUNT as usize)
        .full_mesh(Duration::from_millis(2))
        .build();

    let sim = TaskSimBuilder::<u32>::new(topology, 0xA1B2C3).build(elect);
    let stats = sim.run();

    println!("\n--- Simulation Complete ---");
    println!("Final time:         {}", stats.final_time);
    println!("Events processed:   {}", stats.events_processed);
    println!("Messages delivered: {}", stats.messages_delivered);
    println!("Tasks completed:    {}", stats.tasks_completed);
}
