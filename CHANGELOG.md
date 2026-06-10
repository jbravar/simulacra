# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Task-layer trace export.** The async `TaskSim` facade can now record a
  replayable trace, reaching parity with `TracedNetwork`. Build with
  `TaskSimBuilder::with_trace`, then run via `TaskSim::run_traced` /
  `run_until_traced` to get `(TaskSimStats, Trace<TaskTraceEvent>)`.
  `TaskTraceEvent` captures `Delivered` and `Dropped { reason }`
  (`TaskTraceDropReason` is `NoRoute` / `Partitioned`) and reuses the existing
  trace envelope — no `TRACE_SCHEMA_VERSION` bump. Recording is a pure
  observer with no effect on scheduling, RNG, or event ordering. The
  `tests/determinism.rs` guardrail gained a task-layer scenario that is
  byte-identical across runs.
- **In-flight drop for the async task layer.**
  `TaskSimBuilder::drop_in_flight_on_failure()` makes a mid-run `partition` /
  `fail_link` / `fail_node` (on either `NodeContext` or `TaskSim`) sweep the
  pending event queue and drop any in-flight message whose route no longer
  exists, recording it at the failure time. Mirrors
  `NetConfig::drop_in_flight_on_failure`; off by default (in-flight messages
  survive).
- **Failure-exercising benchmark** (`benches/failure_injection.rs`). Populates
  ~10% of the failure surface so the Phase 7 hot paths (per-edge `failed_links`
  probe in Dijkstra, `partitions` probe at send time) carry real cost and a
  regression on them becomes visible — the existing empty-failure-set benches
  could not show this. Baseline numbers in `docs/perf-baseline.md`.
- **Scheduled failures.** `Scenario::fail_at(time, FailureAction)` and the
  lower-level `TaskSim::schedule_failure` apply a topology/partition mutation at
  a fixed simulated time, replacing the hand-rolled "poll `ctx.now()` each tick"
  pattern. `FailureAction` covers partition / link / node fail and heal
  variants; failures are first-class scheduled events and compose with
  `drop_in_flight_on_failure`.

### Changed

- Hardened the clippy lint policy: enabled the `pedantic` and `nursery` groups
  plus a cherry-picked set of `restriction` lints (no-panic / no-unwrap,
  no-silent-failure, numeric-cast, and unsafe-hygiene). Suppressions must now be
  a scoped `#[expect(..., reason = "…")]` rather than a bare `#[allow]`
  (enforced by `allow_attributes_without_reason`). The kernel's integer-time
  **panic-on-overflow determinism contract is preserved**: arithmetic that must
  panic still does (via `checked_*().expect(...)` or a documented module-level
  `#[expect(clippy::arithmetic_side_effects)]`), and no operator, cast, or RNG
  draw was semantically altered — traces remain byte-identical.
- Marked ~30 public accessors and constructors `const fn` and added
  `#[must_use]` to constructors, accessors, builder-by-value methods, and the
  task futures. These are forward-compatible additions to the public API with
  no behavioral change.

### Fixed

- `TaskSimStats::messages_delivered` now counts only deliveries that actually
  reach an inbox, so it always equals the number of `Delivered` task-trace
  events. Previously a `Deliver` event with an out-of-range destination — only
  reachable via a degenerate out-of-bounds self-`inject` — was over-counted; it
  is now a no-op.

### Documentation

- Added `# Panics` / `# Errors` rustdoc sections documenting pre-existing
  panic and error behavior on `SimRng::choice`, the RED drop policy, and the
  `serde` trace (de)serialization methods.

## [0.1.0] - 2026-05-15

First release.

### Added

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
- **Cross-version RNG-stream guardrail.** A test pins `SimRng`'s derived
  stream for a fixed seed (the `rand`/`rand_chacha` distribution layer a
  dependency bump could silently change). Any change to what a seed
  produces now fails CI and must be deliberately re-blessed and noted
  here — the integration determinism test only checks self-consistency
  within one binary, so this closes that gap.
- **Versioned, format-tagged trace schema.** Serialized traces now carry
  a `format: "simulacra-trace"` tag and `schema_version` (exposed as
  `TRACE_SCHEMA_VERSION`); bump the constant on any serialized-shape
  change. New `TraceLoadError` (serde feature).

### Changed

- **`Trace::from_json` / `read_json` are now strict.** They return
  `Result<_, TraceLoadError>` instead of `serde_json::Error` /
  `io::Result`, rejecting foreign JSON (`NotASimulacraTrace`) and
  mismatched schema versions (`SchemaVersion`) rather than silently
  mis-parsing. The serialized trace gained a `format` + `schema_version`
  envelope (the in-memory `Trace` type is unchanged; `compare()` stays
  purely semantic).

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
- **Dependency upgrades:** `rand` 0.9 → 0.10, `rand_chacha` 0.9 → 0.10,
  `criterion` 0.5 → 0.8 (dev-only). The `rand` 0.10 API moved the
  sampling methods from `Rng` to a new `RngExt` trait; the only internal
  change was the import. **This is stream-neutral:** the new
  `SimRng`-stream guardrail test confirms every seed produces the exact
  same `u64`/`f64`/`bool`/`jitter`/`shuffle`/`fork` sequence as on
  `rand` 0.9, so no recorded trace or replay changes.

### Fixed

- **`TaskSim::SendFut::poll` and `TaskSim::inject` now drop on no-route.**
  Previously they silently scheduled an immediate delivery when the
  topology had no route between source and destination. They now drop
  and increment `TaskSimStats::messages_dropped`. Existing tests that
  asserted the old broken behavior have been updated.
