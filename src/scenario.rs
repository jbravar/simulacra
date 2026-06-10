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
use crate::task::{FailureAction, NodeContext, TaskSimBuilder, TaskSimStats};
use crate::time::{Duration, Time};

/// Convenience builder for task-based scenarios.
pub struct Scenario<M: Clone + 'static> {
    topology: Topology,
    seed: u64,
    /// Initial message injections applied at `Time::ZERO` before the run.
    injections: Vec<(NodeId, NodeId, M)>,
    /// Failure actions scheduled to fire at fixed simulated times.
    failures: Vec<(Time, FailureAction)>,
    /// Optional wall-clock budget in simulated time.
    run_until: Option<Time>,
}

impl<M: Clone + 'static> Scenario<M> {
    /// Creates a new scenario with the given topology and seed.
    #[must_use]
    pub const fn new(topology: Topology, seed: u64) -> Self {
        Self {
            topology,
            seed,
            injections: Vec::new(),
            failures: Vec::new(),
            run_until: None,
        }
    }

    /// Schedules a [`FailureAction`] to fire at simulated time `time`.
    ///
    /// Replaces the hand-rolled "check `ctx.now()` on every tick and flip a
    /// flag" pattern with declarative data. Failures fire deterministically in
    /// time order (ties broken by `fail_at` call order), and a failure-kind
    /// action drops in-flight messages when the scenario's sim was built with
    /// in-flight drop. Heals (`FailureAction::Heal*`) restore the surface.
    ///
    /// ```ignore
    /// use simulacra::{Duration, FailureAction, NodeId, Scenario, Time, TopologyBuilder};
    ///
    /// let stats = Scenario::<u32>::new(topology, 42)
    ///     .fail_at(Time::from_millis(30), FailureAction::FailLink(NodeId(0), NodeId(1)))
    ///     .fail_at(Time::from_millis(90), FailureAction::HealLink(NodeId(0), NodeId(1)))
    ///     .run(|ctx| async move { /* ... */ });
    /// ```
    #[must_use]
    pub fn fail_at(mut self, time: Time, action: FailureAction) -> Self {
        self.failures.push((time, action));
        self
    }

    /// Queues a message to be injected at `Time::ZERO`, before any task runs.
    #[must_use]
    pub fn inject(mut self, src: NodeId, dst: NodeId, payload: M) -> Self {
        self.injections.push((src, dst, payload));
        self
    }

    /// Limits the run to `duration` of simulated time. The simulation still
    /// stops earlier if it runs out of events and pending tasks.
    #[must_use]
    pub fn run_for(mut self, duration: Duration) -> Self {
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "`Time + Duration` is internally `checked_add().expect(\"time \
                      overflow\")`; panic on overflow is the documented time contract"
        )]
        let bound = Time::ZERO + duration;
        self.run_until = Some(bound);
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
        let Self {
            topology,
            seed,
            injections,
            failures,
            run_until,
        } = self;

        let mut sim = TaskSimBuilder::<M>::new(topology, seed).build(node_fn);
        for (src, dst, payload) in injections {
            sim.inject(src, dst, payload);
        }
        for (time, action) in failures {
            sim.schedule_failure(time, action);
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
    fn scenario_fail_at_drops_a_later_send() {
        // Schedule both of node 0's links to fail at t=10ms; a send from node 0
        // at t=20ms must then find no route and drop.
        let topology = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(5))
            .link(1u32, 2u32, Duration::from_millis(5))
            .link(0u32, 2u32, Duration::from_millis(50))
            .build();

        let stats = Scenario::<u64>::new(topology, 1)
            .fail_at(
                Time::from_millis(10),
                FailureAction::FailLink(NodeId(0), NodeId(1)),
            )
            .fail_at(
                Time::from_millis(10),
                FailureAction::FailLink(NodeId(0), NodeId(2)),
            )
            .run(|ctx| async move {
                if ctx.id() == NodeId(0) {
                    ctx.sleep(Duration::from_millis(20)).await;
                    ctx.send(NodeId(2), 1).await;
                }
            });

        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
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
