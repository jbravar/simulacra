//! The core simulation engine.
//!
//! This module provides the main simulation loop and coordination.

use crate::queue::{EventQueue, Scheduled};
use crate::time::Time;

/// Statistics about a completed simulation run.
#[derive(Debug, Clone, Default)]
pub struct SimulationStats {
    /// Total number of events processed.
    pub events_processed: u64,
    /// Final simulated time when the simulation ended.
    pub final_time: Time,
    /// Total number of events ever scheduled (including those not yet processed).
    pub events_scheduled: u64,
}

/// The core simulation engine.
///
/// This is a generic discrete-event simulator that processes events in deterministic
/// time order. The actual event handling is delegated to a user-provided handler.
///
/// # Type Parameters
///
/// * `E` - The event type
///
/// # Example
///
/// ```
/// use simulacra::{Simulation, Time, Duration};
///
/// #[derive(Debug)]
/// enum MyEvent {
///     Tick(u32),
/// }
///
/// let mut sim = Simulation::new();
/// sim.schedule(Time::from_millis(100), MyEvent::Tick(1));
/// sim.schedule(Time::from_millis(200), MyEvent::Tick(2));
///
/// let stats = sim.run(|sim, event| {
///     println!("At {:?}: {:?}", sim.now(), event);
/// });
///
/// assert_eq!(stats.events_processed, 2);
/// ```
#[derive(Debug)]
pub struct Simulation<E> {
    /// Current simulated time.
    now: Time,
    /// The event queue.
    queue: EventQueue<E>,
    /// Number of events processed so far.
    events_processed: u64,
}

impl<E> Simulation<E> {
    /// Creates a new simulation starting at time zero.
    pub fn new() -> Self {
        Simulation {
            now: Time::ZERO,
            queue: EventQueue::new(),
            events_processed: 0,
        }
    }

    /// Creates a new simulation with a pre-allocated event queue capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Simulation {
            now: Time::ZERO,
            queue: EventQueue::with_capacity(capacity),
            events_processed: 0,
        }
    }

    /// Returns the current simulated time.
    #[inline]
    pub fn now(&self) -> Time {
        self.now
    }

    /// Returns the number of events processed so far.
    #[inline]
    pub fn events_processed(&self) -> u64 {
        self.events_processed
    }

    /// Returns the number of events currently in the queue.
    #[inline]
    pub fn pending_events(&self) -> usize {
        self.queue.len()
    }

    /// Returns `true` if there are no pending events.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Schedules an event at the given time.
    ///
    /// Events scheduled at times before the current simulation time will be
    /// processed immediately when they come off the queue (this is generally
    /// a modeling error but is allowed for flexibility).
    ///
    /// Returns the sequence number assigned to this event.
    #[inline]
    pub fn schedule(&mut self, time: Time, event: E) -> u64 {
        self.queue.schedule(time, event)
    }

    /// Schedules an event after a delay from the current time.
    ///
    /// Returns the sequence number assigned to this event.
    #[inline]
    pub fn schedule_in(&mut self, delay: crate::time::Duration, event: E) -> u64 {
        self.queue.schedule(self.now + delay, event)
    }

    /// Returns a reference to the next scheduled event without processing it.
    #[inline]
    pub fn peek(&self) -> Option<&Scheduled<E>> {
        self.queue.peek()
    }

    /// Runs the simulation to completion, processing all events.
    ///
    /// The handler is called for each event in time order. The handler receives
    /// a mutable reference to the simulation (for scheduling new events) and
    /// the event to process.
    ///
    /// Returns statistics about the simulation run.
    pub fn run<F>(mut self, mut handler: F) -> SimulationStats
    where
        F: FnMut(&mut Self, E),
    {
        while let Some(scheduled) = self.queue.pop() {
            self.now = scheduled.time;
            self.events_processed += 1;
            handler(&mut self, scheduled.event);
        }

        SimulationStats {
            events_processed: self.events_processed,
            final_time: self.now,
            events_scheduled: self.queue.total_scheduled(),
        }
    }

    /// Runs the simulation until a condition is met or all events are processed.
    ///
    /// The handler returns `true` to continue, `false` to stop.
    ///
    /// Returns statistics and whether the simulation completed naturally.
    pub fn run_until<F>(mut self, mut handler: F) -> (SimulationStats, bool)
    where
        F: FnMut(&mut Self, E) -> bool,
    {
        let completed = loop {
            match self.queue.pop() {
                Some(scheduled) => {
                    self.now = scheduled.time;
                    self.events_processed += 1;
                    if !handler(&mut self, scheduled.event) {
                        break false;
                    }
                }
                None => break true,
            }
        };

        let stats = SimulationStats {
            events_processed: self.events_processed,
            final_time: self.now,
            events_scheduled: self.queue.total_scheduled(),
        };
        (stats, completed)
    }

    /// Runs the simulation for a single step, processing one event.
    ///
    /// Returns `Some(event)` if an event was processed, `None` if the queue is empty.
    pub fn step(&mut self) -> Option<Scheduled<E>> {
        if let Some(scheduled) = self.queue.pop() {
            self.now = scheduled.time;
            self.events_processed += 1;
            Some(scheduled)
        } else {
            None
        }
    }

    /// Clears all pending events.
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

impl<E> Default for Simulation<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::Duration;

    #[derive(Debug, Clone, PartialEq)]
    enum TestEvent {
        Ping(u32),
        Pong(u32),
    }

    #[test]
    fn basic_simulation() {
        let mut sim = Simulation::new();
        sim.schedule(Time::from_millis(100), TestEvent::Ping(1));
        sim.schedule(Time::from_millis(200), TestEvent::Ping(2));

        let mut events = Vec::new();
        let stats = sim.run(|sim, event| {
            events.push((sim.now(), event));
        });

        assert_eq!(stats.events_processed, 2);
        assert_eq!(stats.final_time, Time::from_millis(200));
        assert_eq!(
            events,
            vec![
                (Time::from_millis(100), TestEvent::Ping(1)),
                (Time::from_millis(200), TestEvent::Ping(2)),
            ]
        );
    }

    #[test]
    fn handler_can_schedule_events() {
        let mut sim = Simulation::new();
        sim.schedule(Time::from_millis(100), TestEvent::Ping(1));

        let mut events = Vec::new();
        let stats = sim.run(|sim, event| {
            events.push((sim.now(), event.clone()));
            if let TestEvent::Ping(n) = event
                && n < 3
            {
                sim.schedule_in(Duration::from_millis(50), TestEvent::Ping(n + 1));
            }
        });

        assert_eq!(stats.events_processed, 3);
        assert_eq!(
            events,
            vec![
                (Time::from_millis(100), TestEvent::Ping(1)),
                (Time::from_millis(150), TestEvent::Ping(2)),
                (Time::from_millis(200), TestEvent::Ping(3)),
            ]
        );
    }

    #[test]
    fn run_until_can_stop_early() {
        let mut sim = Simulation::new();
        for i in 1..=10 {
            sim.schedule(Time::from_millis(i * 100), TestEvent::Ping(i as u32));
        }

        let (stats, completed) = sim.run_until(|_sim, event| {
            if let TestEvent::Ping(n) = event {
                n < 5
            } else {
                true
            }
        });

        assert!(!completed);
        assert_eq!(stats.events_processed, 5);
        assert_eq!(stats.final_time, Time::from_millis(500));
    }

    #[test]
    fn step_processes_one_event() {
        let mut sim: Simulation<TestEvent> = Simulation::new();
        sim.schedule(Time::from_millis(100), TestEvent::Ping(1));
        sim.schedule(Time::from_millis(200), TestEvent::Ping(2));

        let first = sim.step().unwrap();
        assert_eq!(first.event, TestEvent::Ping(1));
        assert_eq!(sim.now(), Time::from_millis(100));
        assert_eq!(sim.pending_events(), 1);

        let second = sim.step().unwrap();
        assert_eq!(second.event, TestEvent::Ping(2));
        assert_eq!(sim.now(), Time::from_millis(200));
        assert!(sim.is_empty());
    }

    #[test]
    fn empty_simulation() {
        let sim: Simulation<TestEvent> = Simulation::new();
        let stats = sim.run(|_, _| {});

        assert_eq!(stats.events_processed, 0);
        assert_eq!(stats.final_time, Time::ZERO);
    }

    #[test]
    fn deterministic_ordering() {
        // Run the same simulation twice and verify identical results
        fn run_sim() -> Vec<(Time, TestEvent)> {
            let mut sim = Simulation::new();

            // Schedule events in a specific order
            sim.schedule(Time::from_millis(100), TestEvent::Ping(1));
            sim.schedule(Time::from_millis(100), TestEvent::Pong(1));
            sim.schedule(Time::from_millis(50), TestEvent::Ping(0));
            sim.schedule(Time::from_millis(100), TestEvent::Ping(2));

            let mut events = Vec::new();
            sim.run(|sim, event| {
                events.push((sim.now(), event));
            });
            events
        }

        let run1 = run_sim();
        let run2 = run_sim();
        assert_eq!(run1, run2);

        // Verify the expected order
        assert_eq!(
            run1,
            vec![
                (Time::from_millis(50), TestEvent::Ping(0)),
                (Time::from_millis(100), TestEvent::Ping(1)),
                (Time::from_millis(100), TestEvent::Pong(1)),
                (Time::from_millis(100), TestEvent::Ping(2)),
            ]
        );
    }
}
