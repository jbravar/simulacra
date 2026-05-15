//! Criterion bench: async-task gossip around a ring.
//!
//! N nodes in a ring. Each node receives one message and forwards it to the
//! next neighbor up to `HOPS` times. Exercises the `TaskSim` poll/wake path
//! plus message routing.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, TopologyBuilder};

fn bench_ring_gossip(c: &mut Criterion) {
    let mut group = c.benchmark_group("task/ring_gossip");
    for &n in &[16usize, 64, 256] {
        // Each node forwards once, so roughly N messages total delivered.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || {
                    let topology = TopologyBuilder::new(n)
                        .ring(Duration::from_millis(1))
                        .build();

                    let node_count = n;
                    let mut sim = TaskSimBuilder::<u32>::new(topology, 0xBEEF).build(
                        move |ctx: NodeContext<u32>| {
                            let nc = node_count as u32;
                            async move {
                                // Each node receives one message and forwards it onward
                                // unless the hop counter has reached zero.
                                let msg = ctx.recv().await;
                                let hops = msg.payload;
                                if hops > 0 {
                                    let next = NodeId((ctx.id().as_u32() + 1) % nc);
                                    ctx.send(next, hops - 1).await;
                                }
                            }
                        },
                    );

                    // Inject a single message at node 0 with N hops of budget.
                    sim.inject(NodeId(0), NodeId(0), n as u32);
                    sim
                },
                |sim| {
                    let stats = sim.run();
                    black_box(stats);
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ring_gossip);
criterion_main!(benches);
