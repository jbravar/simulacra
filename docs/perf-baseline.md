# Performance baseline

Numbers captured on Apple Silicon (aarch64-apple-darwin), `cargo bench --quick`.
They are **indicative**, not rigorous — variance on a laptop is high. The goal
is to establish a coarse reference point so Phase 6 (scale exploration) has
something to beat.

## Machine

- Platform: aarch64-apple-darwin
- Rust: stable, edition 2024
- Profile: `bench` (release + debuginfo off)
- Mode: Criterion `--quick`

## Benchmarks

### `event_queue/push_pop_interleaved`

Push N events at spread-out times, then drain.

| N        | median time | throughput        |
| -------- | ----------- | ----------------- |
| 1,000    | 30.1 µs     | ~33 Melem/s       |
| 10,000   | 504 µs      | ~19.8 Melem/s     |
| 100,000  | 7.11 ms     | ~14.1 Melem/s     |

### `event_queue/push_pop_same_time`

Worst-case tie-breaking: all events at `Time::ZERO`.

| N        | median time | throughput        |
| -------- | ----------- | ----------------- |
| 1,000    | 28.9 µs     | ~34.5 Melem/s     |
| 10,000   | 444 µs      | ~22.5 Melem/s     |
| 100,000  | 6.07 ms     | ~16.5 Melem/s     |

### `network/star_broadcast`

Star topology with 1 hub broadcasting to N−1 leaves.

| N        | median time | throughput        |
| -------- | ----------- | ----------------- |
| 32       | 878 ns      | ~35.3 Melem/s     |
| 256      | 9.90 µs     | ~25.8 Melem/s     |
| 1,024    | 52.6 µs     | ~19.5 Melem/s     |

### `task/ring_gossip`

N async nodes in a ring, one message circulates once (N forwards total).

| N        | median time | throughput        |
| -------- | ----------- | ----------------- |
| 16       | 3.90 µs     | ~4.10 Melem/s     |
| 64       | 17.7 µs    | ~3.62 Melem/s     |
| 256      | 82.8 µs    | ~3.09 Melem/s     |

## Takeaways

- Raw `EventQueue` throughput scales sub-linearly (n log n), as expected from
  `BinaryHeap`. At 100k events the heap is ~7 ms to drain; a 1 M-event sim is
  on the order of 100 ms in the queue alone.
- The `Network` layer adds ~2–4× overhead on top of the queue (routing lookup,
  message allocation, latency model).
- The **`TaskSim` executor is the clear hotspot**: ~10× slower per event than
  `Network` at equal scale, driven by the `Vec<(Time, TaskEvent)>` with
  `sort_by_key` + `remove(0)` on every step. This is exactly what Phase C's
  migration to `EventQueue` should unlock.

## Re-running

```sh
cargo bench                             # all benches, full sampling (~few min)
cargo bench --bench event_queue -- --quick   # fast smoke
```

Criterion writes HTML reports under `target/criterion/`.
