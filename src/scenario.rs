//! Fluent scenario builder for the async task facade.
//!
//! `Scenario` is a thin convenience layer on top of [`TaskSimBuilder`] that
//! bundles up the three steps most simulations go through: build a topology,
//! stage some initial injections, and run. It does not add any new simulation
//! semantics — it is purely ergonomics.
//!
//! # Example
//!
//! ```ignore
//! use simulacra::{Duration, NodeId, Scenario, TopologyBuilder};
//!
//! let topology = TopologyBuilder::new(2)
//!     .link(0u32, 1u32, Duration::from_millis(10))
//!     .build();
//!
//! let stats = Scenario::<String>::new(topology, /* seed */ 42)
//!     .inject(NodeId(0), NodeId(1), "hello".to_string())
//!     .run(|ctx| async move {
//!         if ctx.id() == NodeId(1) {
//!             let msg = ctx.recv().await;
//!             assert_eq!(msg.payload, "hello");
//!         }
//!     });
//!
//! assert_eq!(stats.messages_delivered, 1);
//! ```

use std::future::Future;

use crate::net::{NodeId, Topology};
use crate::task::{NodeContext, TaskSimBuilder, TaskSimStats};
use crate::time::{Duration, Time};

/// Convenience builder for task-based scenarios.
pub struct Scenario<M: Clone + 'static> {
    topology: Topology,
    seed: u64,
    /// Initial message injections applied at `Time::ZERO` before the run.
    injections: Vec<(NodeId, NodeId, M)>,
    /// Optional wall-clock budget in simulated time.
    run_until: Option<Time>,
}

impl<M: Clone + 'static> Scenario<M> {
    /// Creates a new scenario with the given topology and seed.
    pub fn new(topology: Topology, seed: u64) -> Self {
        Scenario {
            topology,
            seed,
            injections: Vec::new(),
            run_until: None,
        }
    }

    /// Queues a message to be injected at `Time::ZERO`, before any task runs.
    pub fn inject(mut self, src: NodeId, dst: NodeId, payload: M) -> Self {
        self.injections.push((src, dst, payload));
        self
    }

    /// Limits the run to `duration` of simulated time. The simulation still
    /// stops earlier if it runs out of events and pending tasks.
    pub fn run_for(mut self, duration: Duration) -> Self {
        self.run_until = Some(Time::ZERO + duration);
        self
    }

    /// Runs the scenario to completion (or the optional time bound).
    ///
    /// The closure is invoked once per node to produce its async task.
    pub fn run<F, Fut>(self, node_fn: F) -> TaskSimStats
    where
        F: Fn(NodeContext<M>) -> Fut,
        Fut: Future<Output = ()> + 'static,
    {
        let Scenario {
            topology,
            seed,
            injections,
            run_until,
        } = self;

        let mut sim = TaskSimBuilder::<M>::new(topology, seed).build(node_fn);
        for (src, dst, payload) in injections {
            sim.inject(src, dst, payload);
        }

        match run_until {
            Some(deadline) => sim.run_until(move |now| now <= deadline),
            None => sim.run(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::TopologyBuilder;

    #[test]
    fn scenario_runs_with_injection() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let stats = Scenario::<u32>::new(topology, 42)
            .inject(NodeId(0), NodeId(1), 99)
            .run(|ctx| async move {
                if ctx.id() == NodeId(1) {
                    let msg = ctx.recv().await;
                    assert_eq!(msg.payload, 99);
                }
            });

        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn scenario_respects_run_for_bound() {
        let topology = TopologyBuilder::new(1).build();

        let stats = Scenario::<()>::new(topology, 42)
            .run_for(Duration::from_millis(10))
            .run(|ctx| async move {
                // Sleep longer than the bound; the scenario should stop first.
                ctx.sleep(Duration::from_secs(60)).await;
            });

        assert!(stats.final_time <= Time::from_millis(10));
        // Task did not complete because the run was cut short.
        assert_eq!(stats.tasks_completed, 0);
    }
}
