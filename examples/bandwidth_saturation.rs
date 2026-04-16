//! Bandwidth saturation: stream many messages through a bottleneck link.
//!
//! Models a 10 Mbps link between two nodes. We fire 20 messages of 100 KB
//! each from A to B at `t=0`. Each message is 800,000 bits at 10 Mbps,
//! which is 80 ms of transmission time per message. Subsequent sends queue
//! behind the one currently on the wire, so the last message arrives
//! ~1.6 s after t=0 (plus the 5 ms link latency).
//!
//! Run with:
//!
//! ```sh
//! cargo run --example bandwidth_saturation
//! ```

use simulacra::{Duration, NetEvent, Network, NodeId, TopologyBuilder};

const LINK_LATENCY: Duration = Duration::from_millis(5);
const LINK_BANDWIDTH_BPS: u64 = 10_000_000 / 8; // 10 Mbps expressed in bytes/sec.
const MESSAGE_SIZE_BYTES: u64 = 100_000; // 100 KB payloads.
const MESSAGE_COUNT: usize = 20;

fn main() {
    let topology = TopologyBuilder::new(2)
        .link(0u32, 1u32, LINK_LATENCY)
        .build();

    let mut net: Network<u32> = Network::new(topology, 42);
    net.set_bandwidth(NodeId(0), NodeId(1), LINK_BANDWIDTH_BPS);

    println!(
        "Config: {} msgs × {} KB through {} Mbps link + {} latency\n",
        MESSAGE_COUNT,
        MESSAGE_SIZE_BYTES / 1000,
        LINK_BANDWIDTH_BPS * 8 / 1_000_000,
        LINK_LATENCY,
    );

    for i in 0..MESSAGE_COUNT as u32 {
        net.send_sized(NodeId(0), NodeId(1), i, MESSAGE_SIZE_BYTES);
    }

    let stats = net.run(|ctx, event| {
        if let NetEvent::Deliver(msg) = event {
            println!("[{:>12}] delivered message #{}", ctx.now(), msg.payload);
        }
    });

    println!(
        "\nDelivered {} messages; final simulated time = {}",
        stats.messages_delivered, stats.final_time
    );
    println!(
        "Without the cap, all {} would have arrived at {} (just the link latency).",
        MESSAGE_COUNT, LINK_LATENCY
    );
}
