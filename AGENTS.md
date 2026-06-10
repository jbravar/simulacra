# AGENTS.md

Guidelines for AI agents working in this Rust codebase.

## Project Overview

**Simulacra** - A deterministic discrete-event simulation engine for modeling message flow across large computer networks, with pluggable latency, jitter, and failure models.

See `DESIGN.md` for architecture and roadmap.

## Commands

```bash
# Build
cargo build

# Build release
cargo build --release

# Run example
cargo run

# Test
cargo test

# Test with serde/JSON support
cargo test --features serde

# Check (fast compile check without producing binary)
cargo check

# Format code
cargo fmt

# Lint
cargo clippy

# Format check (CI-friendly, fails if unformatted)
cargo fmt -- --check
```

## Project Structure

```
src/
├── lib.rs           # Library root, public API exports
├── main.rs          # Example: async gossip protocol simulation
├── time.rs          # Time and Duration types
├── queue.rs         # EventQueue with deterministic ordering
├── sim.rs           # Core simulation engine and run loop
├── rng.rs           # Seeded deterministic RNG (SimRng)
├── trace.rs         # Event tracing and replay validation
├── task.rs          # Async task facade (sleep/recv/send.await)
└── net/
    ├── mod.rs       # Network simulation (Network, NetEvent, Message)
    ├── node.rs      # NodeId, MessageId types
    ├── topology.rs  # Topology, TopologyBuilder, routing
    ├── latency.rs   # Latency models (FixedLatency, UniformJitter, etc.)
    └── traced.rs    # TracedNetwork for recording simulation traces

Cargo.toml           # Project manifest
DESIGN.md            # Architecture and roadmap
```

## Features

- **default**: Core simulation without JSON support
- **serde**: Enables JSON import/export for traces (`Trace::to_json()`, `Trace::from_json()`)

## Core Concepts

### Time Model
- `Time` - A point in simulated time (nanoseconds from start)
- `Duration` - A span of simulated time
- Both are integer-based (no floats) for determinism

### Event System
- `Scheduled<E>` - Event with time and sequence number for tie-breaking
- `EventQueue<E>` - Priority queue with deterministic ordering
- Events at same time are ordered by insertion sequence

### Simulation
- `Simulation<E>` - Core engine with `run()`, `run_until()`, `step()` methods
- Handlers can schedule new events via `schedule()` or `schedule_in()`
- `SimulationStats` returned after completion

### Randomness
- `SimRng` - ChaCha8-based deterministic RNG
- `fork()` creates independent child streams
- Helpers: `jitter()`, `duration_with_jitter()`, `choice()`, `shuffle()`

### Network Layer
- `NodeId` - Node identifiers (u32-based)
- `Topology` - Graph of nodes with weighted links (latency)
- `TopologyBuilder` - Fluent API for common patterns (ring, star, mesh, line)
- `Network<P, L>` - High-level network simulation with message delivery
- `NetEvent<P>` - Deliver or Drop events with user payload
- `RunContext` - Handler context with `now()`, `send()`, `topology()`, `rng()`
- Latency models: `FixedLatency`, `UniformJitter`, `PercentageJitter`, `OverheadPlusJitter`

### Async Task Model
- `TaskSimBuilder<M>` - Builder for task-based simulations
- `TaskSim<M>` - Executor that runs async node functions
- `NodeContext<M>` - Context for async node code with:
  - `sleep(duration).await` - Suspend for simulated time
  - `recv().await` - Wait for incoming message
  - `send(dst, payload).await` - Send message to another node
  - `now()` - Current simulation time
  - `id()` - This node's ID
- `Envelope<M>` - Received message with src, dst, payload, timestamps
- `TaskSimStats` - Statistics from task simulation run

### Tracing & Observability
- `Trace<E>` - Complete trace of a simulation run with seed, events, final time
- `TraceEvent<E>` - Single event with timestamp, sequence, and data
- `TraceRecorder<E>` - Builder for recording traces during simulation
- `TracedNetwork<P, L>` - Network wrapper that automatically records traces
- `NetTraceEvent` - Simplified trace event for network simulations (serializable)
- `Trace::compare()` - Compare two traces for equality
- `Trace::validate_replay()` - Verify deterministic replay
- JSON export/import with `serde` feature: `to_json()`, `from_json()`, `write_json()`, `read_json()`

## Code Conventions

### Rust Edition
Uses Rust **2024 edition** - ensure your toolchain is up to date (`rustup update`).

### Style
- Follow standard Rust conventions (rustfmt defaults)
- Run `cargo fmt` before committing
- Run `cargo clippy --all-targets --all-features -- -D warnings` (the CI form).
  The crate enables a strict lint set (`pedantic` + `nursery` + cherry-picked
  `restriction` lints; see `[lints.clippy]` in `Cargo.toml` and `clippy.toml`).
  Suppress a genuine false positive with a scoped `#[expect(lint, reason = "…")]`
  — never a bare `#[allow]` (the `allow_attributes_without_reason` lint enforces
  this). Kernel modules carry a documented module-level
  `#[expect(clippy::arithmetic_side_effects)]` because integer-time arithmetic
  panics on overflow *by design* (the determinism contract); do not "fix" that
  by switching to `saturating_*`
- Extensive doc comments on public APIs
- Unit tests in `#[cfg(test)]` modules within each file

### Design Principles (from DESIGN.md)
- **Determinism first**: Same seed + inputs = same results
- **Simple kernel, rich layers**: Keep core small
- **Data-oriented**: Avoid needlessly object-heavy models
- **Observable by default**: Instrumentation is first-class
- **Ergonomic without hiding the model**: Pleasant API, explicit causality

### Dependencies
- `rand` and `rand_chacha` for deterministic RNG
- `serde` and `serde_json` (optional, with `serde` feature)
- Keep dependencies minimal per design philosophy

## Testing

Run the full test suite with `cargo test`. Current test coverage:
- Time arithmetic and ordering (5 tests)
- Event queue deterministic ordering (5 tests)
- Simulation run loop behavior (6 tests)
- RNG determinism and distribution (10 tests)
- Network topology and routing (10 tests)
- Latency models (6 tests)
- Network message delivery (6 tests)
- Trace recording and comparison (7 tests)
- TracedNetwork integration (3 tests)
- Async task model (12 tests)
- Doc tests (3 tests)

**Total: 74 unit tests + 3 doc tests** (with `--features serde`: 76 tests)

## Roadmap Status

**Phase 1 (Complete)**: Minimal kernel
- ✅ Time, Duration types
- ✅ Scheduled<E>, EventQueue
- ✅ Simulation loop
- ✅ Deterministic ordering
- ✅ Seeded RNG

**Phase 2 (Complete)**: Network domain
- ✅ NodeId, MessageId types
- ✅ Topology with adjacency list
- ✅ Dijkstra shortest-path routing with caching
- ✅ TopologyBuilder (ring, star, mesh, line patterns)
- ✅ Message delivery with NetEvent
- ✅ Latency models (fixed, uniform jitter, percentage jitter, overhead+jitter)
- ✅ Packet loss simulation

**Phase 3 (Complete)**: Reproducibility and traces
- ✅ Trace recording with TraceRecorder
- ✅ TracedNetwork for automatic tracing
- ✅ Trace comparison and replay validation
- ✅ JSON export/import (serde feature)
- ✅ RunContext.now() for time access in handlers

**Phase 4**: Ergonomics (skipped for now)
- Better scenario construction APIs
- Helper builders
- Docs and examples

**Phase 5 (Complete)**: Async/task facade
- ✅ `TaskSimBuilder` and `TaskSim` executor
- ✅ `NodeContext` with `sleep()`, `recv()`, `send()` futures
- ✅ Custom waker integration with event queue
- ✅ Deterministic task scheduling
- ✅ Message delivery via topology routing

**Phase 6 (Next)**: Scale exploration
- Profiling
- Allocation reduction
- Multi-run parallel execution

See `DESIGN.md` for full roadmap through Phase 7.

## Gotchas

- **Edition 2024**: Very recent edition. Requires nightly or latest stable.
- **No CI/CD yet**: Tests must be run manually.
- **Time overflow**: Operations panic on overflow; use `saturating_*` variants for safety.
- **Tie-breaking**: Events at identical times are processed in insertion order (FIFO).
- **Route caching**: Topology caches computed routes; call `clear_route_cache()` if links change after initial routing.
- **RunContext::send()**: Messages sent from handlers are scheduled after the handler returns, not immediately.
- **Trace serialization**: Requires `serde` feature; event types must implement `Serialize`/`Deserialize`.
- **Task futures**: Use custom no-op waker; real async runtimes (tokio, etc.) are not involved.
- **NodeContext::recv()**: Blocks the task until a message arrives; no timeout support yet.
