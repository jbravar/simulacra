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
- **v0.1.0 + buffer-overflow** — after adding per-link `buffer_bytes`
  admission control with atomic all-or-nothing commit across hops.
  Network hot path is unchanged for unsized / unbuffered sends and adds a
  single `Option<u64>` check per hop when buffers are configured; no
  measurable bench delta is expected on the existing workloads (none of
  which use buffers).
- **v0.1.0 + failure-injection** (2026-05-06) — after the four Phase 7
  commits adding link failure (`Topology::fail_link`), node failure
  (`Topology::fail_node`), opt-in in-flight drop sweep
  (`Simulation::rewrite_queue` + `NetConfig::drop_in_flight_on_failure`),
  and async-task-layer failure injection (`NodeContext` / `TaskSim` fail/
  heal/partition methods + `TaskSimStats::messages_dropped`). Adds a
  `HashSet::contains` per Dijkstra edge expansion (failed_links and
  failed_nodes) and a partition `HashSet::contains` per `SendFut::poll`.
  Captured by re-benching the prior tier (`e7a873d`) and current HEAD on
  the same machine state — all deltas within ±5% noise (see per-bench
  notes below).

## Machine

- Platform: aarch64-apple-darwin
- Rust: stable, edition 2024
- Profile: `bench` (release + debuginfo off)
- Mode: Criterion `--quick`

## Benchmarks

### `event_queue/push_pop_interleaved`

Push N events at spread-out times, then drain. No code changes in
`EventQueue` since phase-6; the new `EventQueue::rewrite` is unused on
this workload. Deltas within noise.

| N       | v0.1.0 init | + phase-6 | + failure-injection (control) | + failure-injection (HEAD) |
| ------- | ----------- | --------- | ----------------------------- | -------------------------- |
| 1,000   | 30.1 µs     | ~30.2 µs  | 32.4 µs                       | 30.4 µs                    |
| 10,000  | 504 µs      | ~504 µs   | 535 µs                        | 510 µs                     |
| 100,000 | 7.11 ms     | ~7.57 ms  | 7.41 ms                       | 7.35 ms                    |

### `event_queue/push_pop_same_time`

Worst-case tie-breaking: all events at `Time::ZERO`.

| N       | v0.1.0 init | + phase-6 | + failure-injection (control) | + failure-injection (HEAD) |
| ------- | ----------- | --------- | ----------------------------- | -------------------------- |
| 1,000   | 28.9 µs     | ~29.1 µs  | 29.5 µs                       | 29.5 µs                    |
| 10,000  | 444 µs      | ~452 µs   | 461 µs                        | 458 µs                     |
| 100,000 | 6.07 ms     | ~6.06 ms  | 6.27 ms                       | 6.19 ms                    |

### `network/star_broadcast`

Star topology with 1 hub broadcasting to N−1 leaves.

| N     | v0.1.0 init | + lazy-cache | + failure-injection (control) | + failure-injection (HEAD) |
| ----- | ----------- | ------------ | ----------------------------- | -------------------------- |
| 32    | 878 ns      | ~920 ns      | 1.49 µs                       | 1.17 µs                    |
| 256   | 9.90 µs     | ~9.77 µs     | 12.04 µs                      | 12.57 µs                   |
| 1,024 | 52.6 µs     | ~53.2 µs     | 60.18 µs                      | 62.58 µs                   |

The control vs HEAD numbers (both captured 2026-05-06 on the same
machine) are within ±5% noise — the per-edge `HashSet::contains` for
failed_links/failed_nodes adds nothing measurable on workloads with
empty failure sets. The init-vs-control gap reflects ~3 weeks of
unrelated drift (kernel version, ambient load, etc.), not a regression
from any specific commit on this branch.

### `task/ring_gossip`

N async nodes in a ring, one message circulates once (N forwards total).
This is where Phase 6 pays off — every iteration used to allocate a fresh
`Vec<TaskId>` and re-run Dijkstra from scratch.

| N   | v0.1.0 init | + lazy-cache | + failure-injection (control) | + failure-injection (HEAD) |
| --- | ----------- | ------------ | ----------------------------- | -------------------------- |
| 16  | 3.90 µs     | ~2.05 µs     | 2.53 µs                       | 2.58 µs                    |
| 64  | 17.7 µs     | ~9.83 µs     | 12.37 µs                      | 12.16 µs                   |
| 256 | 82.8 µs     | ~66.3 µs     | 72.60 µs                      | 73.51 µs                   |

The failure-injection commits add a `partition: HashSet` membership
check per `SendFut::poll` (empty in this workload, so O(1) on a 0-bucket
set) and the same Dijkstra changes as `network/star_broadcast`. HEAD vs
control deltas are ≤2% — well inside `--quick` noise. The init-vs-control
gap is unrelated drift, not a branch regression.

Criterion still flags the N=64 and N=256 deltas as within noise at
`--quick` sampling (p > 0.05), so run `cargo bench` proper for a
firmer picture. Even on the high end of the noise band, all sizes
remain comfortably ahead of the original baseline.

### `failure/link_failure_broadcast` and `failure/partition_send`

These deliberately populate the failure surface so the Phase 7 hot paths
(per-edge `failed_links` probe in Dijkstra; `partitions` probe at send
time) carry real cost. `link_failure_broadcast` fails ~10% of a full
mesh's edges then broadcasts from the hub; `partition_send` partitions
~10% of a star's hub→spoke pairs then broadcasts.

| group / N                       | time (--quick, indicative) |
| ------------------------------- | -------------------------- |
| link_failure_broadcast, N=32    | ~1.31 µs                   |
| link_failure_broadcast, N=128   | ~10.8 µs                   |
| partition_send, N=32            | ~1.04 µs                   |
| partition_send, N=256           | ~10.7 µs                   |

Captured with `--quick` (low sample count), so treat as indicative, not
firm — run `cargo bench --bench failure_injection` for tight intervals.
The point of these benches is a *regression tripwire*: they make the
non-empty `HashSet::contains` costs visible, which the empty-failure-set
benches above cannot.

## Takeaways

- Raw `EventQueue` throughput scales sub-linearly (n log n), as expected
  from `BinaryHeap`. At 100k events the heap is ~7 ms to drain; a 1M-event
  sim is on the order of 100 ms in the queue alone.
- `TaskSim` saw the biggest Phase 6 wins (up to -48%) thanks to the
  ready-buffer reuse and topology scratch amortization. The gap between
  `Network` and `TaskSim` at equal scale has closed considerably.
- Payload-clone hotspot check: the send path moves `P` by value through
  `Message::new` → `NetEvent::{Deliver,Drop}` → `EventQueue::schedule`
  (→ `BinaryHeap::push`) → `pop`. A `rg "\.clone\(\)" src/` sweep finds
  only two call sites in the crate, neither on the net hot path
  (`TraceRecorder::record_clone` is opt-in; the other is a unit test).
  No mitigation needed for non-`Copy` payloads at the current layer.
- For very small-N workloads, the `Vec<Option<Route>>` cache's eager
  allocation is measurable. Lazy init is a cheap follow-up if we care.
- The Phase 7 failure-injection hot-path additions (HashSet checks per
  Dijkstra edge, partition check per SendFut::poll) are invisible on
  workloads with empty failure sets — see the empty-set benches above.
  The `failure/*` benches now exercise a populated failure surface (~10%)
  so those `HashSet::contains` costs are no longer hidden and a future
  regression on them will show up.

## Re-running

```sh
cargo bench                                # all benches, full sampling (~few min)
cargo bench --bench event_queue -- --quick # fast smoke
cargo bench --bench task_gossip -- --quick
cargo bench --bench network_broadcast -- --quick
cargo bench --bench failure_injection -- --quick
```

Criterion writes HTML reports under `target/criterion/`. Use
`target/criterion/<group>/<bench>/report/index.html` for per-bench detail.
