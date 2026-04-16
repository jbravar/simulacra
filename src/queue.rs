//! Event queue with deterministic ordering.
//!
//! Events are ordered by time, with ties broken by insertion order (sequence number).
//! This ensures completely deterministic simulation behavior.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::time::Time;

/// A scheduled event with deterministic ordering.
///
/// Events are ordered first by time (earliest first), then by sequence number
/// (lowest first) to break ties deterministically.
#[derive(Debug, Clone)]
pub struct Scheduled<E> {
    /// When this event should be processed.
    pub time: Time,
    /// Monotonic sequence number for deterministic tie-breaking.
    pub sequence: u64,
    /// The event payload.
    pub event: E,
}

impl<E> Scheduled<E> {
    /// Creates a new scheduled event.
    pub fn new(time: Time, sequence: u64, event: E) -> Self {
        Scheduled {
            time,
            sequence,
            event,
        }
    }
}

impl<E> PartialEq for Scheduled<E> {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.sequence == other.sequence
    }
}

impl<E> Eq for Scheduled<E> {}

impl<E> PartialOrd for Scheduled<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Ord for Scheduled<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so we reverse the ordering to get a min-heap.
        // Compare by time first, then by sequence number.
        match other.time.cmp(&self.time) {
            Ordering::Equal => other.sequence.cmp(&self.sequence),
            ordering => ordering,
        }
    }
}

/// A priority queue of scheduled events.
///
/// Guarantees deterministic ordering: events are processed in time order,
/// with ties broken by insertion order.
#[derive(Debug)]
pub struct EventQueue<E> {
    heap: BinaryHeap<Scheduled<E>>,
    next_sequence: u64,
}

impl<E> EventQueue<E> {
    /// Creates a new empty event queue.
    pub fn new() -> Self {
        EventQueue {
            heap: BinaryHeap::new(),
            next_sequence: 0,
        }
    }

    /// Creates a new event queue with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        EventQueue {
            heap: BinaryHeap::with_capacity(capacity),
            next_sequence: 0,
        }
    }

    /// Schedules an event at the given time.
    ///
    /// Returns the sequence number assigned to this event.
    pub fn schedule(&mut self, time: Time, event: E) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.heap.push(Scheduled::new(time, sequence, event));
        sequence
    }

    /// Removes and returns the next event to process, if any.
    pub fn pop(&mut self) -> Option<Scheduled<E>> {
        self.heap.pop()
    }

    /// Returns a reference to the next event without removing it.
    pub fn peek(&self) -> Option<&Scheduled<E>> {
        self.heap.peek()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Returns the number of scheduled events.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Clears all events from the queue.
    pub fn clear(&mut self) {
        self.heap.clear();
    }

    /// Returns the total number of events that have been scheduled.
    ///
    /// This is useful for statistics and debugging.
    pub fn total_scheduled(&self) -> u64 {
        self.next_sequence
    }
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestEvent(u32);

    #[test]
    fn events_ordered_by_time() {
        let mut queue = EventQueue::new();

        queue.schedule(Time::from_millis(300), TestEvent(3));
        queue.schedule(Time::from_millis(100), TestEvent(1));
        queue.schedule(Time::from_millis(200), TestEvent(2));

        assert_eq!(queue.pop().unwrap().event, TestEvent(1));
        assert_eq!(queue.pop().unwrap().event, TestEvent(2));
        assert_eq!(queue.pop().unwrap().event, TestEvent(3));
        assert!(queue.pop().is_none());
    }

    #[test]
    fn ties_broken_by_sequence() {
        let mut queue = EventQueue::new();

        let t = Time::from_millis(100);
        queue.schedule(t, TestEvent(1));
        queue.schedule(t, TestEvent(2));
        queue.schedule(t, TestEvent(3));

        // Should come out in insertion order
        assert_eq!(queue.pop().unwrap().event, TestEvent(1));
        assert_eq!(queue.pop().unwrap().event, TestEvent(2));
        assert_eq!(queue.pop().unwrap().event, TestEvent(3));
    }

    #[test]
    fn sequence_numbers_are_monotonic() {
        let mut queue = EventQueue::new();

        let s1 = queue.schedule(Time::ZERO, TestEvent(1));
        let s2 = queue.schedule(Time::ZERO, TestEvent(2));
        let s3 = queue.schedule(Time::ZERO, TestEvent(3));

        assert_eq!(s1, 0);
        assert_eq!(s2, 1);
        assert_eq!(s3, 2);
        assert_eq!(queue.total_scheduled(), 3);
    }

    #[test]
    fn peek_does_not_remove() {
        let mut queue = EventQueue::new();
        queue.schedule(Time::from_millis(100), TestEvent(1));

        assert_eq!(queue.peek().unwrap().event, TestEvent(1));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop().unwrap().event, TestEvent(1));
        assert!(queue.is_empty());
    }

    #[test]
    fn complex_ordering() {
        let mut queue = EventQueue::new();

        // Schedule in arbitrary order
        queue.schedule(Time::from_millis(50), TestEvent(2));
        queue.schedule(Time::from_millis(100), TestEvent(4));
        queue.schedule(Time::from_millis(50), TestEvent(3));
        queue.schedule(Time::from_millis(25), TestEvent(1));
        queue.schedule(Time::from_millis(100), TestEvent(5));

        // Should come out: 25ms(1), 50ms(2), 50ms(3), 100ms(4), 100ms(5)
        let events: Vec<_> = std::iter::from_fn(|| queue.pop())
            .map(|s| s.event.0)
            .collect();
        assert_eq!(events, vec![1, 2, 3, 4, 5]);
    }
}
