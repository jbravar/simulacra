//! Microbenchmark for `EventQueue` push/pop throughput.
//!
//! The event queue is the inner loop of every simulation. This bench measures
//! the raw cost of scheduling and draining events, both under interleaved
//! times and under identical times (worst case for tie-breaking).

// Benchmark harness, not load-bearing library code: loop indices and the
// arithmetic that derives schedule times are bounded by the bench parameters.
#![expect(
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    reason = "bench harness: indices/arithmetic bounded by bench parameters"
)]

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use simulacra::{EventQueue, Time};

fn bench_push_pop_interleaved(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_queue/push_pop_interleaved");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || (),
                |()| {
                    let mut q: EventQueue<u32> = EventQueue::with_capacity(n);
                    // Schedule at spread-out times so the heap actually reorders.
                    for i in 0..n {
                        let t = Time::from_nanos(((i as u64).wrapping_mul(31)) % n as u64);
                        q.schedule(t, i as u32);
                    }
                    while let Some(s) = q.pop() {
                        black_box(s);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_push_pop_same_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_queue/push_pop_same_time");
    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={n}"), |b| {
            b.iter_batched(
                || (),
                |()| {
                    // All at Time::ZERO — pure tie-breaking stress.
                    let mut q: EventQueue<u32> = EventQueue::with_capacity(n);
                    for i in 0..n {
                        q.schedule(Time::ZERO, i as u32);
                    }
                    while let Some(s) = q.pop() {
                        black_box(s);
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_push_pop_interleaved,
    bench_push_pop_same_time
);
criterion_main!(benches);
