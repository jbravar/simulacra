# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **End-to-end bandwidth model.** `Network::set_bandwidth` and `send_sized`
  apply per-`(src, dst)` serialization queueing on top of the latency
  model, and the underlying admission pass walks the path hop-by-hop.
- **Per-link bandwidth caps** via `TopologyBuilder::link_with_capacity` and
  `directed_link_with_capacity`, with hop-by-hop serialization across
  multi-hop paths. Two flows that share a bottleneck link are correctly
  serialized at the bottleneck, not at the endpoints.
- **Per-link buffer overflow drops** via `link_with_capacity_and_buffer`.
  Atomic all-or-nothing admission across a multi-hop path, surfaced as
  the new `DropReason::BufferOverflow`.
- **Pluggable per-link drop policies** via `DropPolicy::TailDrop` (default)
  and `DropPolicy::RandomEarlyDetection` (AQM-style early shedding).
- **Link failure with reroute.** `Topology::fail_link` / `heal_link` and
  their `_directed` variants. Failed edges are excluded from Dijkstra,
  so existing routes either reroute via an alternate or drop with
  `DropReason::NoRoute`. Wired through `Network`, `RunContext`, and
  `TracedNetwork`.
- **Node failure.** `Topology::fail_node` / `heal_node` excludes the node
  from routing entirely (as source, destination, or intermediate hop).
- **Opt-in in-flight drop on failure.** `NetConfig::drop_in_flight_on_failure`
  makes failure mutators sweep the event queue and rewrite unroutable
  pending `Deliver` events into `Drop` events firing at the time of the
  failure call. Backed by the new `Simulation::rewrite_queue` /
  `EventQueue::rewrite` primitives.
- **Failure injection in the async task facade.** `NodeContext` and
  `TaskSim` expose `partition` / `heal` / `fail_link` / `heal_link` /
  `fail_node` / `heal_node` (+ `_directed` variants). Drops are tracked
  via the new `TaskSimStats::messages_dropped` counter.
- **`SpikyLatency`** wraps any `LatencyModel` and adds a probabilistic
  fixed-size spike to a configurable fraction of messages.
- **`parallel::run_seeds`** runs many independent simulation seeds across
  threads, returning results in seed order regardless of completion.
- **`Simulation::rewrite_queue`** and **`EventQueue::rewrite`** drain and
  rebuild the event heap with a transform closure, with fresh sequence
  numbers for reinserted events.
- **`TracedNetwork::with_config` / `send_at` / `send_at_sized`** delegates.
- New examples: `failure_injection`, `bandwidth_saturation`,
  `wan_bottleneck`.
- `rust-version = "1.88"` MSRV declared in `Cargo.toml`. The crate uses
  `if let` chains, which were stabilized in Rust 1.88.
- crates.io metadata: `repository`, `homepage`, `documentation`, `keywords`,
  `categories`.

### Changed

- **Topology route cache is now lazy.** Construction and `add_link` are
  O(1) regardless of node count; the cache is allocated on the first
  successful `route()` call. Previously, building a star topology was
  effectively O(N³) due to per-mutation cache zeroing. `clear_route_cache`
  is now O(1).
- **Hot-path allocation reduction.** `TaskSim` reuses a single
  `ready_tasks` scratch buffer across the run loop. `Topology` reuses
  Dijkstra scratch buffers (`dist`, `prev`, `heap`). Route cache backing
  switched from `HashMap<(NodeId, NodeId), Route>` to a dense
  `Vec<Option<Route>>` indexed by `src * node_count + dst`.

### Fixed

- **`TaskSim::SendFut::poll` and `TaskSim::inject` now drop on no-route.**
  Previously they silently scheduled an immediate delivery when the
  topology had no route between source and destination. They now drop
  and increment `TaskSimStats::messages_dropped`. Existing tests that
  asserted the old broken behavior have been updated.

## [0.1.0] - 2026-04-16

Initial release.

- Deterministic discrete-event simulation kernel: `Time`, `Duration`,
  `Scheduled<E>`, `EventQueue`, `Simulation::run`.
- Seeded ChaCha8 RNG (`SimRng`) with `fork()` for independent sub-streams.
- Network domain: `NodeId`, `Topology` with adjacency-list routing and
  Dijkstra shortest paths, `TopologyBuilder` with `ring` / `star` /
  `full_mesh` / `line` / `link` / `link_asymmetric` patterns.
- Latency models: `FixedLatency`, `UniformJitter`, `PercentageJitter`,
  `OverheadPlusJitter`.
- Pair-level `Network::partition` / `heal` for symmetric link failures
  at the `(src, dst)` granularity.
- Configurable `NetConfig::packet_loss` for random Bernoulli drops.
- Trace recording via `TraceRecorder`, `TraceEvent`, `Trace::compare`,
  and `Trace::validate_replay`.
- `TracedNetwork<P, L>` wrapper for automatic trace capture, with
  optional JSON export/import behind the `serde` feature flag.
- Async task façade: `TaskSimBuilder`, `TaskSim`, `NodeContext` with
  `sleep` / `recv` / `recv_timeout` / `send` futures, `Envelope`,
  `select2`, `CancellationToken`. Custom no-op waker driven by the
  event queue (no real Tokio runtime).
- `Scenario<M>` convenience builder over `TaskSimBuilder`.

[Unreleased]: https://github.com/jbravar/simulacra/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jbravar/simulacra/releases/tag/v0.1.0
