//! Criterion bench: star-topology broadcast throughput.
//!
//! One hub (node 0) broadcasts a message to every other node in a star
//! topology of size N. Exercises `Network::send` path, routing cache,
//! and event-queue drain.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use simulacra::{Duration, NetEvent, Network, NodeId, TopologyBuilder};

fn bench_star_broadcast(c: &mut Criterion) {
    let mut group = c.benchmark_group("network/star_broadcast");
    for &n in &[32usize, 256, 1024] {
        group.throughput(Throughput::Elements((n - 1) as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || {
                    let topology = TopologyBuilder::new(n)
                        .star(Duration::from_millis(5))
                        .build();
                    let mut net: Network<u64> = Network::new(topology, 0xC0FFEE);
                    for dst in 1..n {
                        net.send(NodeId(0), NodeId(dst as u32), dst as u64);
                    }
                    net
                },
                |net| {
                    let stats = net.run(|_ctx, event| {
                        if let NetEvent::Deliver(msg) = event {
                            black_box(msg);
                        }
                    });
                    black_box(stats);
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_star_broadcast);
criterion_main!(benches);
