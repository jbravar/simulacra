# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Authoritative references

- `AGENTS.md` — contributor guide (commands, structure, conventions, gotchas). Read first.
- `DESIGN.md` — architecture, design principles, roadmap. The roadmap section is regularly amended in commits; check it before assuming a phase isn't done.
- `README.md` — user-facing intro and the **determinism contract**.

This file only adds what's not in those.

## Commands

```sh
cargo build
cargo test                              # default features
cargo test --features serde             # adds JSON trace tests
cargo test <name>                       # single test by substring
cargo test --doc                        # doc tests only
cargo fmt -- --check                    # CI uses this exact form
cargo clippy --all-targets --all-features -- -D warnings   # CI form; matches RUSTFLAGS=-D warnings
cargo bench                             # Criterion; pass --quick to iterate
cargo doc --no-deps --all-features      # CI builds docs with RUSTDOCFLAGS=-D warnings
```

CI matrix: `stable` and `beta` toolchains × `default` and `serde` features. Anything that compiles with warnings will fail CI.

## Architecture in one screen

The crate is a single `simulacra` library on **Rust 2024 edition** with a deliberately layered design — keep new code in the layer it belongs to, don't reach across.

```
┌─ scenario.rs ─── Scenario builder (ergonomics over TaskSimBuilder)
├─ task.rs ─────── async facade: TaskSim, NodeContext, sleep/recv/send futures
├─ net/ ────────── Network<P,L>, Topology, routing, latency models, TracedNetwork
├─ trace.rs ────── Trace, TraceEvent, TraceRecorder, replay validation
├─ parallel.rs ─── multi-seed parallel runner (independent runs, NOT parallel single-run)
└─ kernel:        sim.rs (run loop) · queue.rs (EventQueue/Scheduled) · rng.rs (SimRng) · time.rs (Time/Duration)
```

Key invariants that cut across layers:

- **Determinism**: same seed + topology + node code + injections ⇒ byte-identical trace. Enforced by `tests/determinism.rs` (round-trips JSON traces and asserts equality). Anything that breaks this — iteration order over `HashMap`, wall-clock reads, unseeded RNG, non-FIFO tie-breaks — is a bug.
- **Tie-breaking**: events at identical `Time` are processed in insertion (FIFO) order via `Scheduled::order`.
- **Time is integer-only** (`Time`/`Duration` in nanoseconds). No floats in the clock. Operations panic on overflow — use `saturating_*` if you need it.
- **No real async runtime**: `task.rs` uses a custom no-op waker driven by the event queue. Never pull in tokio/async-std types.
- **`SimRng::fork()`** creates independent child streams — use this when adding new sources of randomness so existing streams stay stable across changes.
- **Topology route cache is lazy** (`Option<Vec<Option<Route>>>`); `clear_route_cache()` is O(1). If you mutate links after routing, clear the cache.
- **Network sends from handlers** are scheduled *after* the handler returns, not immediately.
- **Failure injection has two layers with different scopes**: `Network::partition(a, b)` blocks the `(src, dst)` *pair* — it doesn't touch the topology. `Topology::fail_link(a, b)` removes a *physical edge* from Dijkstra so routes flow around it. `Topology::fail_node(n)` excludes a node entirely from routing. All three are checked at *send time* by default, so in-flight (already-scheduled) messages survive a mid-run fail/heal. Pre-scheduled `Network::send_at(...)` evaluates routing at call time, not at delivery time — to test mid-run failure semantics, use `RunContext::send` from inside a handler.
- **Opt-in in-flight drop**: setting `NetConfig::drop_in_flight_on_failure = true` makes failure mutators (partition / fail_link / fail_node) sweep the event queue and rewrite any pending `Deliver(msg)` whose `(msg.src, msg.dst)` no longer routes into a `Drop` event firing at the time of the failure call. Pre-run mutators sweep synchronously; mid-run mutators set a flag that the `Network::run` loop drains after the current handler (before pending sends are routed). The underlying primitive is `Simulation::rewrite_queue` / `EventQueue::rewrite`.
- **Two parallel failure surfaces**: the `Network<P, L>` API and the `TaskSim<M>` async facade have separate internal state (`partitions` lives on each independently; `failed_links` / `failed_nodes` live on each one's `Topology`). Same method names on both — `fail_link`, `fail_node`, `partition`, etc. The async facade drops sends across failed/partitioned routes silently and tracks them via `TaskSimStats::messages_dropped` (the destination's inbox is not modified, so callers learn about drops via stats, not via the `recv` future). The async facade now mirrors `drop_in_flight_on_failure` via `TaskSimBuilder::drop_in_flight_on_failure()`: when set, the failure mutators (on both `NodeContext` and `TaskSim`) sweep the `EventQueue<TaskEvent<M>>` and drop any pending `Deliver` whose `(src, dst)` no longer routes, recording it at the failure time. Unlike `Network` (which rewrites `Deliver` → `Drop` in-queue and uses a deferred flag mid-run), the task layer has no in-queue drop event, so the sweep removes the event and records the drop synchronously — and because task mutations are always synchronous, the sweep is eager (no deferred-flag step).

## Test layout

Unit tests live in `#[cfg(test)]` modules within each source file (not in a separate `tests/` tree per file). The only file under `tests/` is `determinism.rs` — the integration-level replay guarantee. When adding features that touch ordering, RNG, or trace events, add a case there too.

## Features and dependencies

- `default` features: core sim + network + task facade. No serde.
- `serde` feature: gates `Trace::{to_json, from_json, write_json, read_json}` and `Serialize`/`Deserialize` on trace event types. Anything serde-dependent must be behind `#[cfg(feature = "serde")]`.
- Dependency budget is tight by design (`rand`, `rand_chacha`, optional `serde`/`serde_json`, dev-only `criterion`). Don't add deps without a clear reason.

## Benches

Three Criterion benches under `benches/` (`event_queue`, `network_broadcast`, `task_gossip`) with baseline numbers in `docs/perf-baseline.md`. Update that doc when intentionally changing perf characteristics.

## Examples

`examples/` holds runnable demos (`gossip`, `leader_election`, `request_reply_retry`, `bandwidth_saturation`, `wan_bottleneck`). `cargo run --example <name>`. They double as smoke tests for the public API — if you change a public type's signature, these will fail to compile.
