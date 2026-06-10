//! Criterion bench: routing and delivery with a *populated* failure set.
//!
//! The existing benches all run with empty `failed_links` / `failed_nodes` /
//! `partitions`, so the Phase 7 failure-injection hot paths (the per-edge
//! `HashSet::contains` in Dijkstra, the partition check at send time) are
//! invisible. These benches deliberately fail ~10% of the surface so those
//! lookups carry real cost and future regressions on them become visible.

// Benchmark harness, not load-bearing library code: loop indices and the
// arithmetic that derives node ids / failure stride are bounded by the bench
// parameters.
#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    reason = "bench harness: indices/arithmetic bounded by bench parameters"
)]

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use simulacra::{Duration, NetEvent, Network, NodeId, TopologyBuilder};

/// Full mesh with ~10% of physical links failed, then a hub broadcast from
/// node 0. Routes that lose their direct edge reroute over Dijkstra, which
/// now probes a non-empty `failed_links` set on every edge expansion.
fn bench_link_failure_broadcast(c: &mut Criterion) {
    let mut group = c.benchmark_group("failure/link_failure_broadcast");
    for &n in &[32usize, 128] {
        group.throughput(Throughput::Elements((n - 1) as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || {
                    let topology = TopologyBuilder::new(n)
                        .full_mesh(Duration::from_millis(5))
                        .build();
                    let mut net: Network<u64> = Network::new(topology, 0x00C0_FFEE);
                    // Fail every 10th edge in a canonical enumeration (~10%).
                    let mut edge = 0usize;
                    for i in 0..n {
                        for j in (i + 1)..n {
                            if edge.is_multiple_of(10) {
                                net.fail_link(NodeId(i as u32), NodeId(j as u32));
                            }
                            edge += 1;
                        }
                    }
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
            );
        });
    }
    group.finish();
}

/// Star topology with ~10% of the hub's pairs partitioned, then a broadcast
/// from the hub. Each send consults the `partitions` set; the partitioned
/// fraction is dropped instead of delivered.
fn bench_partition_send(c: &mut Criterion) {
    let mut group = c.benchmark_group("failure/partition_send");
    for &n in &[32usize, 256] {
        group.throughput(Throughput::Elements((n - 1) as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || {
                    let topology = TopologyBuilder::new(n)
                        .star(Duration::from_millis(5))
                        .build();
                    let mut net: Network<u64> = Network::new(topology, 0x00C0_FFEE);
                    // Partition every 10th hub->spoke pair (~10%).
                    for dst in 1..n {
                        if dst.is_multiple_of(10) {
                            net.partition(NodeId(0), NodeId(dst as u32));
                        }
                    }
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
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_link_failure_broadcast, bench_partition_send);
criterion_main!(benches);
