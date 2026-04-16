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
- **v0.1.0 + lazy-cache** — after making the route cache lazy
  (`Option<Vec<Option<Route>>>`). `clear_route_cache` is O(1) and
  construction + `add_link` no longer have to zero N² slots. Fixes what was
  effectively O(N³) star-topology construction.

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

| N     | v0.1.0 init | + phase-6 | + lazy-cache | vs. init |
| ----- | ----------- | --------- | ------------ | -------- |
| 32    | 878 ns      | ~935 ns   | ~920 ns      | +5%      |
| 256   | 9.90 µs     | ~11.4 µs  | ~9.77 µs     | -1%      |
| 1,024 | 52.6 µs     | ~48.6 µs  | ~53.2 µs     | +1%      |

The lazy-cache pass recovered most of the phase-6 small-N regressions
(N=256 is back to parity), at the cost of a small regression on the
larger N=1,024 case that was benefitting from eager allocation of a
warm, contiguous cache. Overall within ~5% of the initial baseline for
all sizes, and construction of a star topology is now O(N) again
instead of O(N³).

### `task/ring_gossip`

N async nodes in a ring, one message circulates once (N forwards total).
This is where Phase 6 pays off — every iteration used to allocate a fresh
`Vec<TaskId>` and re-run Dijkstra from scratch.

| N   | v0.1.0 init | + phase-6 | + lazy-cache | vs. init |
| --- | ----------- | --------- | ------------ | -------- |
| 16  | 3.90 µs     | ~2.03 µs  | ~2.05 µs     | -47%     |
| 64  | 17.7 µs     | ~16.2 µs  | ~9.83 µs     | -44%     |
| 256 | 82.8 µs     | ~49.2 µs  | ~66.3 µs     | -20%     |

Criterion flags the N=64 and N=256 deltas as within noise at
`--quick` sampling (p > 0.05), so run `cargo bench` proper for a
firmer picture. Even on the high end of the noise band, all sizes
remain comfortably ahead of the original baseline.

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
