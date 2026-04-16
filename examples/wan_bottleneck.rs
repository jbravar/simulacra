//! Multi-hop bottleneck: two clients sharing a single WAN link.
//!
//! Topology:
//!
//! ```text
//!   client_a ──LAN──┐                ┌──LAN── server
//!                   ├── WAN (slow)───┤
//!   client_b ──LAN──┘                └──
//! ```
//!
//! - LAN links: 100 Mbps, 1 ms latency (uncapped in this example).
//! - WAN link (1→2): 10 Mbps, 20 ms latency — the bottleneck.
//!
//! Each client sends a 1 MB message to the server at `t=0`. Because both
//! flows cross the same WAN link, they serialize on it: the second message
//! departs the WAN only after the first finishes transmitting, even though
//! the clients are independent.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example wan_bottleneck
//! ```

use simulacra::{Duration, NetEvent, Network, NodeId, TopologyBuilder};

const LAN_LATENCY: Duration = Duration::from_millis(1);
const WAN_LATENCY: Duration = Duration::from_millis(20);
const LAN_BW_BPS: u64 = 100_000_000 / 8; // 100 Mbps
const WAN_BW_BPS: u64 = 10_000_000 / 8; //  10 Mbps
const MESSAGE_SIZE: u64 = 1_000_000; //   1 MB

fn main() {
    // Node layout:
    //   0 = client_a, 1 = edge_router_left, 2 = edge_router_right, 3 = server
    //   4 = client_b, also connected to edge_router_left
    let topology = TopologyBuilder::new(5)
        // LAN side
        .link_with_capacity(0u32, 1u32, LAN_LATENCY, LAN_BW_BPS) // client_a -> edge
        .link_with_capacity(4u32, 1u32, LAN_LATENCY, LAN_BW_BPS) // client_b -> edge
        // WAN bottleneck
        .link_with_capacity(1u32, 2u32, WAN_LATENCY, WAN_BW_BPS) // edge -> edge
        // Other LAN side
        .link_with_capacity(2u32, 3u32, LAN_LATENCY, LAN_BW_BPS) // edge -> server
        .build();

    let mut net: Network<&'static str> = Network::new(topology, 42);

    println!("Two clients each send a 1 MB message to the server over a shared 10 Mbps WAN.\n");
    net.send_sized(NodeId(0), NodeId(3), "client_a payload", MESSAGE_SIZE);
    net.send_sized(NodeId(4), NodeId(3), "client_b payload", MESSAGE_SIZE);

    let stats = net.run(|ctx, event| {
        if let NetEvent::Deliver(msg) = event {
            println!(
                "[{:>12}] {:?} arrived at server (from {:?})",
                ctx.now(),
                msg.payload,
                msg.src
            );
        }
    });

    println!(
        "\nFinal simulated time = {} (both messages delivered)",
        stats.final_time
    );
    println!(
        "If the WAN were uncapped, both would arrive around {} (LAN + WAN latency only).",
        LAN_LATENCY + WAN_LATENCY + LAN_LATENCY
    );
}
