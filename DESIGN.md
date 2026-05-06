# Simulacra

A deterministic discrete-event simulation engine for message flow across large computer networks, with pluggable latency, jitter, and failure models.

## Status

Early design / prototype.

## Vision

Simulacra is a Rust-first simulation platform for modeling large networks of computers and the movement of messages through those networks over simulated time.

The project starts from a deliberately simple premise:

- time advances by events, not wall clock time
- nodes are passive state, not OS threads
- messages move through a topology according to routing and delay models
- randomness is explicit and reproducible
- repeated runs with the same seed should produce the same result

The long-term goal is not just “a simulator,” but a modern, ergonomic, inspectable engine for systems simulation.

## Non-goals for v1

To keep the project honest, the first version should not try to be:

- a packet-level Internet simulator
- a full cloud/datacenter simulator
- a general-purpose async runtime replacement
- a parallel discrete-event simulator
- a GUI-heavy academic framework

Those may become future directions, but they should not define the initial architecture.

## Core idea

At its heart, Simulacra is a deterministic scheduler over timestamped events.

A minimal mental model:

```rust
while let Some(event) = queue.pop() {
    sim.now = event.time();
    sim.handle(event);
}
```

The first concrete domain is network-style message delivery:

1. a node sends a message
2. a route is selected
3. latency and jitter are applied
4. a delivery event is scheduled at a future simulated time
5. the target node receives the message when that event is processed

## Design principles

### Determinism first

Given the same seed, same topology, and same inputs, a simulation should produce the same result.

This implies:

- deterministic event ordering
- explicit tie-breaking rules
- seeded randomness
- no dependence on wall clock time

### Simple kernel, rich layers

The core engine should stay small:

- time
- event queue
- scheduler
- task or node registry
- deterministic RNG

Higher-level conveniences should layer on top of that core.

### Data-oriented where it matters

The system should avoid a needlessly object-heavy model. Nodes, links, messages, and events should be represented compactly where practical.

### Observable by default

A simulator is much more useful when its behavior can be inspected. Instrumentation should be treated as a first-class concern, not an afterthought.

### Ergonomic without hiding the model

The API should be pleasant, but it should not obscure the fact that this is a discrete-event simulator with explicit causality and simulated time.

## Architecture overview

The initial architecture is expected to have at least these conceptual pieces.

### 1. Simulation kernel

Responsible for:

- current simulated time
- event queue management
- deterministic event ordering
- running the main loop

Possible core shape:

```rust
pub struct Simulation {
    now: Time,
    queue: EventQueue,
    rng: SimRng,
    // domain-specific registries layered on top
}
```

### 2. Time model

A dedicated `Time` type should represent simulated time explicitly.

Open questions:

- integer ticks vs nanoseconds vs generic duration units
- whether `Time` and `Duration` should be distinct types
- overflow behavior

Initial recommendation:

- use integer-based simulated time
- keep `Time` and `Duration` distinct
- avoid floats in the core clock model

### 3. Event model

Events are the atomic units of causality.

Initial requirements:

- each event has a scheduled time
- event ordering must be deterministic
- tie-breaking should be explicit

A likely shape:

```rust
pub struct Scheduled<E> {
    pub at: Time,
    pub order: u64,
    pub event: E,
}
```

Where `order` is a monotonic sequence number used to break ties at the same timestamp.

### 4. Topology model

The initial domain centers on message flow through a graph of nodes and links.

Topology responsibilities:

- node identifiers
- edges / links
- route lookup or route computation
- latency base values
- optional capacity/failure metadata later

Initial recommendation:

- start with static topology
- start with precomputed routes or simple routing logic
- keep the topology layer separate from the scheduler

### 5. Network/message model

The first domain-specific event set can remain extremely small.

Example:

```rust
pub enum NetEvent {
    DeliverMessage {
        src: NodeId,
        dst: NodeId,
        message: MessageId,
    },
}
```

This is enough for an initial simulator that models delayed delivery over a graph.

### 6. Randomness model

Randomness should be deterministic and scoped.

Requirements:

- seeded runs
- repeatable jitter/failure behavior
- ability to replay exactly

Possible future refinement:

- separate RNG streams for different concerns such as routing, jitter, failures, workload generation

### 7. Observability

The engine should make it easy to answer questions like:

- what event fired at this time?
- why was this message delayed?
- what was the queue depth over time?
- what state transitions happened for this node?

Potential outputs:

- event trace logs
- counters / metrics
- queue depth histories
- timeline exports

## Execution model

### Baseline execution model

The first execution model should be single-process and single-threaded.

Rationale:

- simplest correct implementation
- deterministic by default
- easy to debug and reason about
- avoids premature complexity around causality and partition coordination

### Parallelism stance

Parallelism is not rejected; it is deferred.

Near-term parallelism should focus on:

- many independent simulation runs in parallel

Not on:

- parallelizing a single run

Longer-term, partitioned simulation may be explored if the architecture justifies it.

## Async/task model

A major design opportunity is to provide an async-like API on top of the discrete-event engine.

Example user-facing shape:

```rust
async fn node_main(ctx: NodeContext) {
    loop {
        let msg = ctx.recv().await;
        ctx.sleep(Duration::from_millis(10)).await;
        ctx.send(msg.reply_to(), reply(msg)).await;
    }
}
```

Important distinction:

- this would be inspired by Tokio-like ergonomics
- but it would not be driven by wall clock time or OS I/O
- the simulator would poll suspended tasks according to simulated events

Recommendation:

- do not make this the first implementation milestone
- first build the explicit event kernel
- then layer an async/task façade on top if the core remains clean

## Initial crate shape

A likely long-term workspace structure:

- `simulacra-core` — time, event queue, scheduler
- `simulacra-net` — topology, routing, message delivery, latency/jitter models
- `simulacra-task` — async/task façade over the simulation kernel
- `simulacra-vis` — visualization/export helpers
- `simulacra` — top-level convenience crate or prelude

For now, starting as a single crate is the right move.

## Proposed v0 scope

The first meaningful version should be intentionally narrow.

### v0 goals

- deterministic simulated clock
- priority queue of scheduled events
- static topology of nodes and links
- message send from one node to another
- route latency plus optional jitter
- seeded reproducibility
- basic event trace output

### v0 non-goals

- packet fragmentation
- bandwidth/congestion modeling
- dynamic routing protocols
- node CPU/memory execution modeling
- partitioned simulation
- GUI
- real async runtime integration

## Example first scenario

A very small end-to-end milestone:

- create 10 nodes in a graph
- define link latencies
- send a message from node A to node B
- compute route delay plus jitter
- schedule delivery
- run simulation to completion
- emit trace of all delivery events

If that works deterministically, the nucleus of the project is sound.

## Open design questions

### Time

- What should the canonical unit of simulated time be?
- Should the core be unitless ticks and let higher layers interpret them?

### Event queue

- Is `BinaryHeap` enough initially?
- Do we want a more specialized calendar queue or timing wheel later?

### Topology/routing

- Precompute shortest paths, or compute dynamically?
- Should route selection be part of the topology layer or a pluggable strategy?

### Payload storage

- Should events contain payloads directly, or refer to message storage by ID?
- What data layout minimizes allocations without making the API miserable?

### Deterministic ordering

- What exact tie-break rules should govern events at identical timestamps?

### Instrumentation

- What should be built into the core versus layered externally?

### Async façade

- Should the task model be a first-party layer or a separate experimental crate?

## Roadmap

### Phase 1: minimal kernel

- `Time`
- `Scheduled<E>`
- event queue
- simulation loop
- deterministic ordering

### Phase 2: network domain

- node IDs
- topology
- routing
- message delivery
- jitter model

### Phase 3: reproducibility and traces

- seeded RNG
- trace recording
- replay validation

### Phase 4: ergonomics

- better scenario construction APIs
- helper builders
- docs and examples

### Phase 5: async/task experiment

- simulated `sleep().await`
- task wakeups scheduled by the event queue
- node task contexts

### Phase 6: scale exploration

- profiling
- allocation reduction
- compact storage
- multi-run parallel execution

### Phase 7: advanced models

- loss/failure injection
  - `SpikyLatency` landed in 2026-04
  - pair-level partition/heal (`Network::partition` / `heal`) — initial commit
  - link failure with reroute (`Topology::fail_link` / `heal_link`,
    Dijkstra-aware) landed in 2026-05; in-flight messages survive
- minimal end-to-end bandwidth cap with per-`(src, dst)` serialization
  queueing landed in 2026-04 via `Network::set_bandwidth` + `send_sized`
- full queueing and contention (per-link capacity along multi-hop paths,
  not just per-pair; still TODO)
- node failure (composable from N-pair link failure; not yet a first-class API)
- in-flight drop on partition/link-failure (currently survives; would need
  event-queue surgery)
- partitioning experiments

## README draft

## Simulacra

Simulacra is a deterministic discrete-event simulation engine for modeling message flow across large computer networks.

It is designed around a few simple ideas:

- simulated time instead of wall clock time
- explicit event-driven causality
- deterministic replay from a seed
- ergonomic APIs layered over a small core

### Current focus

The first milestone is a minimal simulator that can:

- represent a network topology
- send messages across routes
- apply latency and jitter
- process delivery events in deterministic time order

### Why?

Most existing simulation tools in this space are either highly academic, domain-heavy, or not very ergonomic. Simulacra aims to explore a different point in the design space: modern Rust APIs, deterministic behavior, and a strong foundation for observability and tooling.

### Status

Very early. The architecture is still being defined.

## Immediate next steps

1. Define `Time`, `Duration`, and `Scheduled<E>`.
2. Implement the first event queue.
3. Implement `Simulation::run()`.
4. Model a minimal topology and `DeliverMessage` event.
5. Write one deterministic end-to-end scenario test.

## Notes for future contributors

Keep the kernel small. Prefer deterministic behavior over cleverness. Resist adding realism faster than the core can absorb it.
