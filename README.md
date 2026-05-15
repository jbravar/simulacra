# Simulacra

[![CI](https://github.com/jbravar/simulacra/actions/workflows/ci.yml/badge.svg)](https://github.com/jbravar/simulacra/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-edition%202024-blue)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A deterministic discrete-event simulation engine for modeling message flow across
large computer networks, with pluggable latency, jitter, failure, and
bandwidth/congestion models.

Simulacra gives you a Tokio-flavored async API on top of a small, inspectable
simulation kernel: you write node logic with `sleep().await` / `recv().await` /
`send().await`, and the engine drives it through simulated time so the same seed
always produces the same trace.

## Why Simulacra?

Most network simulation tools in the Rust ecosystem sit at one of two extremes:
either they ride on top of a real async runtime (Tokio, `turmoil`, `madsim`) and
inherit its quirks, or they're low-level event-queue libraries that force you to
reinvent the ergonomics yourself. Simulacra tries to hit a different point:

- **Pure simulated time.** No wall clock, no real threads, no real I/O.
- **Explicit determinism contract.** Same seed + same inputs ⇒ byte-identical trace.
- **Small kernel, rich layers.** `time`, `queue`, `sim`, `rng` are deliberately tiny;
  the network and async-task layers build on top.
- **Observable by default.** Traces are first-class and can be exported to JSON
  and diffed for replay validation.

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
simulacra = "0.1"
```

A minimal async ping/pong across a 2-node link:

```rust
use simulacra::{Duration, NodeContext, NodeId, TaskSimBuilder, TopologyBuilder};

async fn node(ctx: NodeContext<String>) {
    match ctx.id().as_u32() {
        0 => {
            ctx.send(NodeId(1), "ping".into()).await;
            let reply = ctx.recv().await;
            assert_eq!(reply.payload, "pong");
        }
        1 => {
            let msg = ctx.recv().await;
            ctx.sleep(Duration::from_millis(5)).await;
            ctx.send(msg.src, "pong".into()).await;
        }
        _ => {}
    }
}

fn main() {
    let topology = TopologyBuilder::new(2)
        .link(0u32, 1u32, Duration::from_millis(10))
        .build();

    let sim = TaskSimBuilder::<String>::new(topology, /* seed */ 42).build(node);
    let stats = sim.run();

    println!("final_time = {}", stats.final_time);
    println!("delivered  = {}", stats.messages_delivered);
}
```

See `examples/` for longer worked examples (gossip, leader election, retries
over a lossy link, failure injection, bandwidth saturation, WAN bottleneck).

## The determinism contract

Given:

- the same **seed**,
- the same **topology**,
- the same **node code**, and
- the same **injected inputs**,

Simulacra guarantees the event trace is identical across runs, machines, and
OS versions. The guardrails that keep this honest:

- Integer `Time`/`Duration` (no floats in the clock).
- Priority queue with explicit insertion-order tie-breaking (`queue::Scheduled`).
- Seeded ChaCha8 RNG with `fork()` for independent sub-streams.
- A `tests/determinism.rs` integration test runs a non-trivial scenario twice
  and asserts byte-equal JSON traces; CI fails if that ever regresses.

If you find a case where two runs with the same inputs produce different
traces, that's a bug — please open an issue.

## Feature flags

| Flag     | Default | Effect |
| -------- | ------- | ------ |
| (none)   | on      | Core simulation + network + async task facade. |
| `serde`  | off     | Enables `Trace::{to_json, from_json, write_json, read_json}` and serializable trace events. |

## Comparison with related projects

| Project     | Simulated time | Real async runtime | Network topology | Determinism focus | Primary use |
| ----------- | -------------- | ------------------ | ---------------- | ----------------- | ----------- |
| Simulacra   | yes (integer)  | no (custom executor) | yes (graph + routing) | primary goal | DES for network protocols |
| `madsim`    | yes            | patches Tokio      | yes              | primary goal      | Distributed-systems testing |
| `turmoil`   | yes            | runs on Tokio      | limited (pairwise) | primary goal    | Tokio-based service testing |
| `shuttle`   | n/a            | n/a                | no               | thread-interleaving | Concurrency bug finding |
| `desim`     | yes            | no                 | no               | secondary         | Generic DES library |

Pick Simulacra if you want an async-style API, explicit network topology,
deterministic replay, and you're comfortable not being on a real Tokio
runtime. Pick `madsim` or `turmoil` if you need to run real Tokio-based code
under simulated conditions.

## Current status

Version 0.1.0 — implemented and tested, but **not yet published to crates.io**.
Alongside the deterministic kernel, async facade, and topology-aware delivery,
0.1.0 ships full failure injection (link / node / partition with reroute), an
end-to-end **bandwidth and congestion model** (per-link capacity, hop-by-hop
serialization, buffer-overflow drops, and RED active queue management), the
`SpikyLatency` model, and the `Scenario` builder. 140+ unit tests; deterministic
JSON-trace replay is validated end-to-end in CI.

Parallel *single-run* execution remains out of scope; independent multi-seed
runs are parallelized via `parallel::run_seeds`.

See `DESIGN.md` for architecture and the roadmap, `CHANGELOG.md` for the full
0.1.0 contents, and `AGENTS.md` for contributor guidelines.

## Development

```sh
cargo build              # build
cargo test               # run the full test suite
cargo test --features serde   # include JSON trace tests
cargo fmt -- --check     # formatting check
cargo clippy --all-targets --all-features -- -D warnings
cargo bench              # Criterion benchmarks (see docs/perf-baseline.md)
```

## License

MIT. See `LICENSE`.
