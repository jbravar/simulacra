//! Demonstrates link / node failure with reroute on a redundant network.
//!
//! A diamond topology offers two paths between client (node 0) and
//! server (node 3):
//!
//! - Fast: `0 -- 1 -- 3` (10ms total).
//! - Slow: `0 -- 2 -- 3` (100ms total).
//!
//! The client sends a ping every 50ms and prints the resulting RTT.
//! Three scheduled failures occur during the run:
//!
//! 1. `t=200ms`: fail link 0-1 — pings reroute via the slow path.
//! 2. `t=600ms`: fail node 2 — both paths are out, pings time out.
//! 3. `t=1000ms`: heal link 0-1 — fast path returns, RTT snaps back.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example failure_injection
//! ```
use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, Time, TopologyBuilder};

const CLIENT: NodeId = NodeId(0);
const FAST_HOP: NodeId = NodeId(1);
const SLOW_HOP: NodeId = NodeId(2);
const SERVER: NodeId = NodeId(3);

const PING_INTERVAL: Duration = Duration::from_millis(50);
const PING_TIMEOUT: Duration = Duration::from_millis(500);
const RUN_UNTIL: Time = Time::from_millis(1300);

#[derive(Debug, Clone)]
enum Msg {
    Ping { seq: u32, sent_at: Time },
    Pong { seq: u32, sent_at: Time },
}

async fn server(ctx: NodeContext<Msg>) {
    let token = ctx.shutdown_token();
    while !token.is_cancelled() {
        let env = ctx.recv().await;
        if let Msg::Ping { seq, sent_at } = env.payload {
            ctx.send(env.src, Msg::Pong { seq, sent_at }).await;
        }
    }
}

/// One task drives both the ping cadence and the failure schedule by
/// checking the simulated clock on each tick. A separate scheduler task
/// would need its own node since `NodeContext` is owned by exactly one
/// task per node.
async fn client(ctx: NodeContext<Msg>) {
    let mut seq: u32 = 0;
    let mut fired_fail_link = false;
    let mut fired_fail_node = false;
    let mut fired_heal_link = false;

    while ctx.now() < RUN_UNTIL {
        // Trigger scheduled failures based on the current clock. Each
        // `if` runs at most once.
        if !fired_fail_link && ctx.now() >= Time::from_millis(200) {
            fired_fail_link = true;
            println!(
                "[{:>10}] FAIL  link {}-{} (fast path) — pings should reroute via slow",
                ctx.now(),
                CLIENT.as_u32(),
                FAST_HOP.as_u32()
            );
            ctx.fail_link(CLIENT, FAST_HOP);
        }
        if !fired_fail_node && ctx.now() >= Time::from_millis(600) {
            fired_fail_node = true;
            println!(
                "[{:>10}] FAIL  node {} (slow hop) — full isolation, pings should time out",
                ctx.now(),
                SLOW_HOP.as_u32()
            );
            ctx.fail_node(SLOW_HOP);
        }
        if !fired_heal_link && ctx.now() >= Time::from_millis(1000) {
            fired_heal_link = true;
            println!(
                "[{:>10}] HEAL  link {}-{} — fast path returns",
                ctx.now(),
                CLIENT.as_u32(),
                FAST_HOP.as_u32()
            );
            ctx.heal_link(CLIENT, FAST_HOP);
        }

        seq += 1;
        let sent_at = ctx.now();
        ctx.send(SERVER, Msg::Ping { seq, sent_at }).await;
        match ctx.recv_timeout(PING_TIMEOUT).await {
            Some(env) => {
                if let Msg::Pong { seq, sent_at } = env.payload {
                    let rtt = env.received_at.saturating_duration_since(sent_at);
                    println!(
                        "[{:>10}] ping #{:<3} -> pong   rtt={}",
                        env.received_at, seq, rtt
                    );
                }
            }
            None => {
                println!("[{:>10}] ping #{:<3} -> TIMEOUT (no route)", ctx.now(), seq);
            }
        }
        ctx.sleep(PING_INTERVAL).await;
    }

    println!("[{:>10}] client: shutting down", ctx.now());
    ctx.shutdown_token().cancel();
}

fn main() {
    let topology = TopologyBuilder::new(4)
        .link(0u32, 1u32, Duration::from_millis(5))
        .link(1u32, 3u32, Duration::from_millis(5))
        .link(0u32, 2u32, Duration::from_millis(50))
        .link(2u32, 3u32, Duration::from_millis(50))
        .build();

    let sim = TaskSimBuilder::<Msg>::new(topology, 0xF00D).build(|ctx| async move {
        match ctx.id() {
            CLIENT => client(ctx).await,
            SERVER => server(ctx).await,
            // Intermediate nodes (1 and 2) don't run logic; routing handles them.
            _ => {}
        }
    });

    let stats = sim.run();
    println!("\n--- Simulation Complete ---");
    println!("Final time:         {}", stats.final_time);
    println!("Messages delivered: {}", stats.messages_delivered);
    println!("Messages dropped:   {}", stats.messages_dropped);
    println!("Tasks completed:    {}", stats.tasks_completed);
}
