# Performance baseline

Numbers captured on Apple Silicon (aarch64-apple-darwin), `cargo bench --quick`.
They are **indicative**, not rigorous — variance on a laptop is high. The goal
is to have a coarse reference point so future optimization work (or
regressions) has something concrete to compare against.

## Status

- **v0.1.0 init** — captured when the benchmark harness first landed, before
  any optimization work.
- **v0.1.0 + phase-6** — after the Phase 6 allocation-reduction pass:
  - `TaskSim` reuses a single `ready_tasks` scratch buffer across the run loop.
  - `Topology` reuses Dijkstra scratch buffers (`dist`, `prev`, `heap`).
  - Route cache switched from `HashMap<(NodeId, NodeId), Route>` to a dense
    `Vec<Option<Route>>` indexed by `src * node_count + dst`.

## Machine

- Platform: aarch64-apple-darwin
- Rust: stable, edition 2024
- Profile: `bench` (release + debuginfo off)
- Mode: Criterion `--quick`

## Benchmarks

### `event_queue/push_pop_interleaved`

Push N events at spread-out times, then drain. No code changes in
`EventQueue`; delta is within noise.

| N       | v0.1.0 init | + phase-6 |
| ------- | ----------- | --------- |
| 1,000   | 30.1 µs     | ~30.2 µs  |
| 10,000  | 504 µs      | ~504 µs   |
| 100,000 | 7.11 ms     | ~7.57 ms  |

### `event_queue/push_pop_same_time`

Worst-case tie-breaking: all events at `Time::ZERO`.

| N       | v0.1.0 init | + phase-6 |
| ------- | ----------- | --------- |
| 1,000   | 28.9 µs     | ~29.1 µs  |
| 10,000  | 444 µs      | ~452 µs   |
| 100,000 | 6.07 ms     | ~6.06 ms  |

### `network/star_broadcast`

Star topology with 1 hub broadcasting to N−1 leaves.

| N     | v0.1.0 init | + phase-6 | delta |
| ----- | ----------- | --------- | ----- |
| 32    | 878 ns      | ~935 ns   | +6%   |
| 256   | 9.90 µs     | ~11.4 µs  | +15%  |
| 1,024 | 52.6 µs     | ~48.6 µs  | -8%   |

The small regressions at N=32 and N=256 come from up-front allocation of the
dense O(N²) `Vec<Option<Route>>` cache (pre-zeroed) on every fresh
`Network`. The 1,024-node case benefits from O(1) cache lookup vs. hash
probing. If this becomes a problem for very small-N workloads, the cache
could be made lazy (e.g., `Box<[Option<Route>]>` initialized on first
insert).

### `task/ring_gossip`

N async nodes in a ring, one message circulates once (N forwards total).
This is where Phase 6 pays off — every iteration used to allocate a fresh
`Vec<TaskId>` and re-run Dijkstra from scratch.

| N   | v0.1.0 init | + phase-6 | delta |
| --- | ----------- | --------- | ----- |
| 16  | 3.90 µs     | ~2.03 µs  | -48%  |
| 64  | 17.7 µs     | ~16.2 µs  | -9%   |
| 256 | 82.8 µs     | ~49.2 µs  | -41%  |

## Takeaways

- Raw `EventQueue` throughput scales sub-linearly (n log n), as expected
  from `BinaryHeap`. At 100k events the heap is ~7 ms to drain; a 1M-event
  sim is on the order of 100 ms in the queue alone.
- `TaskSim` saw the biggest Phase 6 wins (up to -48%) thanks to the
  ready-buffer reuse and topology scratch amortization. The gap between
  `Network` and `TaskSim` at equal scale has closed considerably.
- Next likely hotspot: `NetEvent<P>` clones inside the event queue for
  non-`Copy` payload types. Worth revisiting if Phase 7 workloads become
  payload-heavy.
- For very small-N workloads, the `Vec<Option<Route>>` cache's eager
  allocation is measurable. Lazy init is a cheap follow-up if we care.

## Re-running

```sh
cargo bench                                # all benches, full sampling (~few min)
cargo bench --bench event_queue -- --quick # fast smoke
cargo bench --bench task_gossip -- --quick
cargo bench --bench network_broadcast -- --quick
```

Criterion writes HTML reports under `target/criterion/`. Use
`target/criterion/<group>/<bench>/report/index.html` for per-bench detail.
