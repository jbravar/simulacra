//! Client that retries a request with exponential backoff until it gets a reply.
//!
//! Node 0 is the client. Node 1 is a flaky server that ignores the first two
//! requests it receives, then replies to the third. The client retries with
//! backoff using `recv_timeout`, and succeeds on the third attempt.
//!
//! Demonstrates `recv_timeout`, per-request unique IDs, and cooperative
//! retries without leaking pending futures.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example request_reply_retry
//! ```

// Illustrative example, not load-bearing library code.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "example code: scenario-bounded arithmetic, not load-bearing"
)]

use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, TopologyBuilder};

/// Message format: request carries a per-attempt sequence number; reply echoes it.
#[derive(Debug, Clone)]
enum Msg {
    Request { seq: u32 },
    Reply { seq: u32 },
}

const CLIENT: NodeId = NodeId(0);
const SERVER: NodeId = NodeId(1);
const MAX_ATTEMPTS: u32 = 5;
const BASE_TIMEOUT: Duration = Duration::from_millis(25);

async fn client(ctx: NodeContext<Msg>) {
    if ctx.id() != CLIENT {
        return; // handled by the server branch below
    }

    for attempt in 1..=MAX_ATTEMPTS {
        let seq = attempt;
        println!(
            "[{:>10}] client: attempt {}/{} (seq={})",
            ctx.now(),
            attempt,
            MAX_ATTEMPTS,
            seq
        );
        ctx.send(SERVER, Msg::Request { seq }).await;

        // Exponential backoff: 25ms, 50ms, 100ms, 200ms, 400ms.
        let timeout = BASE_TIMEOUT.saturating_mul(1u64 << (attempt - 1));
        match ctx.recv_timeout(timeout).await {
            Some(env) => {
                if let Msg::Reply { seq: got } = env.payload {
                    println!(
                        "[{:>10}] client: reply received (seq={}) after {} attempts",
                        ctx.now(),
                        got,
                        attempt
                    );
                    return;
                }
            }
            None => {
                println!(
                    "[{:>10}] client: attempt {} timed out after {}",
                    ctx.now(),
                    attempt,
                    timeout
                );
            }
        }
    }

    println!(
        "[{:>10}] client: giving up after {} attempts",
        ctx.now(),
        MAX_ATTEMPTS
    );
}

async fn server(ctx: NodeContext<Msg>) {
    if ctx.id() != SERVER {
        return;
    }

    let mut seen = 0u32;
    loop {
        let env = ctx.recv().await;
        if let Msg::Request { seq } = env.payload {
            seen += 1;
            if seen < 3 {
                println!(
                    "[{:>10}] server: dropping request seq={} (#{} of 3)",
                    ctx.now(),
                    seq,
                    seen
                );
                // Intentionally ignore.
                continue;
            }
            println!("[{:>10}] server: replying to seq={}", ctx.now(), seq);
            ctx.send(env.src, Msg::Reply { seq }).await;
            return;
        }
    }
}

fn main() {
    let topology = TopologyBuilder::new(2)
        .link(0u32, 1u32, Duration::from_millis(5))
        .build();

    let sim = TaskSimBuilder::<Msg>::new(topology, 0xF00D).build(|ctx| async move {
        // Dispatch by node role.
        if ctx.id() == CLIENT {
            client(ctx).await;
        } else if ctx.id() == SERVER {
            server(ctx).await;
        }
    });

    let stats = sim.run();
    println!("\n--- Simulation Complete ---");
    println!("Final time:         {}", stats.final_time);
    println!("Messages delivered: {}", stats.messages_delivered);
    println!("Tasks completed:    {}", stats.tasks_completed);
}
