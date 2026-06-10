//! Async task facade for node-based simulations.
//!
//! This module provides an async/await-style API for writing node logic,
//! while still being driven by the discrete-event simulation engine.
//!
//! # Example
//!
//! ```ignore
//! use simulacra::task::{TaskSim, NodeContext};
//!
//! async fn ping_pong(mut ctx: NodeContext<String>) {
//!     loop {
//!         let msg = ctx.recv().await;
//!         ctx.sleep(Duration::from_millis(10)).await;
//!         ctx.send(msg.src, format!("pong from {}", ctx.id())).await;
//!     }
//! }
//! ```
//!
//! # How It Works
//!
//! Unlike a real async runtime, this module does not use OS threads or real I/O.
//! Instead:
//!
//! - `sleep()` schedules a wake event in the simulation's event queue.
//! - `recv()` waits for a message to arrive (scheduled by send).
//! - `send()` schedules message delivery through the network layer.
//!
//! The simulation advances by processing events, which wake tasks as needed.

use std::cell::{Cell, RefCell};
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::net::{NodeId, Topology};
use crate::queue::EventQueue;
use crate::time::{Duration, Time};

/// Canonical ordering for an unordered node pair, used as the key for
/// symmetric partition tracking. Mirrors the helper used in `net::mod`.
const fn pair_key(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a.as_u32() <= b.as_u32() {
        (a, b)
    } else {
        (b, a)
    }
}

/// Unique identifier for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u32);

/// A message received by a node.
#[derive(Debug, Clone)]
pub struct Envelope<M> {
    /// Source node.
    pub src: NodeId,
    /// Destination node (this node).
    pub dst: NodeId,
    /// The message payload.
    pub payload: M,
    /// Time the message was sent.
    pub sent_at: Time,
    /// Time the message was received.
    pub received_at: Time,
}

/// Internal events for the task simulation.
#[derive(Debug)]
enum TaskEvent<M> {
    /// Wake a task to continue execution.
    Wake(TaskId),
    /// Deliver a message to a node.
    Deliver {
        src: NodeId,
        dst: NodeId,
        payload: M,
        sent_at: Time,
    },
}

/// Shared state for a node, accessible by its task.
struct NodeState<M> {
    /// Incoming message queue.
    inbox: VecDeque<Envelope<M>>,
    /// Whether a task is waiting for a message on this node.
    waiting_for_message: bool,
    /// Task ID for this node.
    task_id: TaskId,
}

/// Internal simulation state shared between the executor and task contexts.
struct SimState<M> {
    /// Current simulation time.
    now: Time,
    /// Network topology.
    topology: Topology,
    /// Per-node state.
    nodes: Vec<NodeState<M>>,
    /// Pending events, ordered by time with deterministic tie-breaking.
    events: EventQueue<TaskEvent<M>>,
    /// Tasks ready to be polled.
    ready_tasks: Vec<TaskId>,
    /// Shutdown flag, shared with all `NodeContext`s via `CancellationToken`.
    cancelled: Rc<Cell<bool>>,
    /// Active symmetric partitions between node pairs. Sends across a
    /// partitioned pair drop silently (no inbox push), but increment the
    /// run's `messages_dropped` counter.
    partitions: HashSet<(NodeId, NodeId)>,
    /// Running count of messages that were dropped (no route or
    /// partition) instead of delivered.
    messages_dropped: u64,
}

impl<M> SimState<M> {
    fn schedule_wake(&mut self, time: Time, task_id: TaskId) {
        self.events.schedule(time, TaskEvent::Wake(task_id));
    }
}

/// A cooperative cancellation signal shared by all tasks in a simulation.
///
/// The flag is set by <code>[TaskSim::shutdown_token].cancel()</code> (or by any task
/// via <code>[NodeContext::shutdown_token].cancel()</code>). Tasks can poll
/// `is_cancelled()` to decide whether to exit early.
#[derive(Clone)]
pub struct CancellationToken {
    flag: Rc<Cell<bool>>,
}

impl CancellationToken {
    /// Returns `true` if cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.get()
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        self.flag.set(true);
    }
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.flag.get())
            .finish()
    }
}

/// Context passed to async node functions.
///
/// Provides methods for sleeping, sending, and receiving messages.
pub struct NodeContext<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    node_id: NodeId,
    task_id: TaskId,
}

impl<M: 'static> NodeContext<M> {
    /// Returns this node's ID.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the current simulation time.
    #[must_use]
    pub fn now(&self) -> Time {
        self.state.borrow().now
    }

    /// Returns a cancellation token tied to this simulation.
    ///
    /// Tasks may poll `token.is_cancelled()` to decide whether to return
    /// early. Call `.cancel()` on any clone of the token to signal shutdown.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        CancellationToken {
            flag: Rc::clone(&self.state.borrow().cancelled),
        }
    }

    /// Installs a symmetric partition between `a` and `b`. Subsequent
    /// `send` calls across this pair (in either direction) will be dropped
    /// silently, with the run's `messages_dropped` counter incremented.
    /// In-flight messages already scheduled for delivery are not affected.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().partitions.insert(pair_key(a, b));
    }

    /// Removes a previously-installed partition between `a` and `b`.
    pub fn heal(&self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().partitions.remove(&pair_key(a, b));
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    #[must_use]
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.state.borrow().partitions.contains(&pair_key(a, b))
    }

    /// Fails the bidirectional link between `a` and `b` at the topology
    /// level. Sends whose route required this link will reroute (if an
    /// alternative exists) or drop. In-flight messages already scheduled
    /// for delivery are not affected.
    pub fn fail_link(&self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().topology.fail_link(a, b);
    }

    /// Restores a previously-failed bidirectional link between `a` and `b`.
    pub fn heal_link(&self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().topology.heal_link(a, b);
    }

    /// Returns `true` if both directions of the link between `a` and `b`
    /// are currently failed.
    #[must_use]
    pub fn is_link_failed(&self, a: NodeId, b: NodeId) -> bool {
        self.state.borrow().topology.is_link_failed(a, b)
    }

    /// Fails a single direction of the link between `src` and `dst`.
    pub fn fail_link_directed(&self, src: NodeId, dst: NodeId) {
        self.state
            .borrow_mut()
            .topology
            .fail_link_directed(src, dst);
    }

    /// Restores a single direction of the link between `src` and `dst`.
    pub fn heal_link_directed(&self, src: NodeId, dst: NodeId) {
        self.state
            .borrow_mut()
            .topology
            .heal_link_directed(src, dst);
    }

    /// Returns `true` if the directed edge `src -> dst` is failed.
    #[must_use]
    pub fn is_link_failed_directed(&self, src: NodeId, dst: NodeId) -> bool {
        self.state
            .borrow()
            .topology
            .is_link_failed_directed(src, dst)
    }

    /// Marks `node` as failed; routing excludes it as src, dst, or hop.
    pub fn fail_node(&self, node: NodeId) {
        self.state.borrow_mut().topology.fail_node(node);
    }

    /// Restores a previously-failed node.
    pub fn heal_node(&self, node: NodeId) {
        self.state.borrow_mut().topology.heal_node(node);
    }

    /// Returns `true` if `node` is currently marked as failed.
    #[must_use]
    pub fn is_node_failed(&self, node: NodeId) -> bool {
        self.state.borrow().topology.is_node_failed(node)
    }

    /// Sleeps for the specified duration of simulated time.
    #[must_use]
    pub fn sleep(&self, duration: Duration) -> Sleep<M> {
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "Time `+` Duration is internally checked and panic-on-overflow \
                      per the documented time contract"
        )]
        let wake_time = {
            let state = self.state.borrow();
            state.now + duration
        };
        Sleep {
            state: Rc::clone(&self.state),
            task_id: self.task_id,
            wake_time,
            scheduled: false,
        }
    }

    /// Receives the next message sent to this node.
    ///
    /// If no message is available, the task will suspend until one arrives.
    #[must_use]
    pub fn recv(&self) -> Recv<M> {
        Recv {
            state: Rc::clone(&self.state),
            node_id: self.node_id,
        }
    }

    /// Receives the next message or returns `None` after `timeout` elapses.
    ///
    /// Deterministic: the timeout is measured in simulated time, not wall-clock.
    #[must_use]
    pub fn recv_timeout(&self, timeout: Duration) -> RecvTimeout<M> {
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "Time `+` Duration is internally checked and panic-on-overflow \
                      per the documented time contract"
        )]
        let deadline = self.state.borrow().now + timeout;
        RecvTimeout {
            state: Rc::clone(&self.state),
            node_id: self.node_id,
            task_id: self.task_id,
            deadline,
            wake_scheduled: false,
        }
    }
}

impl<M: Clone + 'static> NodeContext<M> {
    /// Sends a message to another node.
    ///
    /// The message will be delivered after network latency.
    pub fn send(&self, dst: NodeId, payload: M) -> SendFut<M> {
        SendFut {
            state: Rc::clone(&self.state),
            src: self.node_id,
            dst,
            payload: Some(payload),
        }
    }
}

/// Future returned by `NodeContext::sleep()`.
pub struct Sleep<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    task_id: TaskId,
    wake_time: Time,
    scheduled: bool,
}

impl<M: 'static> Future for Sleep<M> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = self.state.borrow().now;

        if now >= self.wake_time {
            Poll::Ready(())
        } else {
            if !self.scheduled {
                self.state
                    .borrow_mut()
                    .schedule_wake(self.wake_time, self.task_id);
                self.scheduled = true;
            }
            Poll::Pending
        }
    }
}

/// Future returned by `NodeContext::recv()`.
pub struct Recv<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    node_id: NodeId,
}

impl<M: 'static> Future for Recv<M> {
    type Output = Envelope<M>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        #[expect(
            clippy::indexing_slicing,
            reason = "node_id is a valid topology node (< nodes.len()); the recv \
                      future is only constructed for the node's own context"
        )]
        let node = &mut state.nodes[self.node_id.as_usize()];

        if let Some(envelope) = node.inbox.pop_front() {
            node.waiting_for_message = false;
            Poll::Ready(envelope)
        } else {
            node.waiting_for_message = true;
            Poll::Pending
        }
    }
}

/// Future returned by `NodeContext::recv_timeout()`.
pub struct RecvTimeout<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    node_id: NodeId,
    task_id: TaskId,
    deadline: Time,
    wake_scheduled: bool,
}

impl<M: 'static> Future for RecvTimeout<M> {
    type Output = Option<Envelope<M>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Read snapshot values first so we can mutate `self` later without
        // overlapping the borrow on `self.state`.
        let node_idx = self.node_id.as_usize();
        let deadline = self.deadline;
        let task_id = self.task_id;
        let need_schedule = !self.wake_scheduled;

        #[expect(
            clippy::indexing_slicing,
            reason = "node_idx is a valid topology node (< nodes.len()); the \
                      recv-timeout future is only constructed for the node's own \
                      context"
        )]
        let outcome = {
            let mut state = self.state.borrow_mut();

            // A message that arrived at the same instant as the timeout should
            // still be delivered, so check the inbox first.
            if let Some(envelope) = state.nodes[node_idx].inbox.pop_front() {
                state.nodes[node_idx].waiting_for_message = false;
                return Poll::Ready(Some(envelope));
            }

            if state.now >= deadline {
                state.nodes[node_idx].waiting_for_message = false;
                return Poll::Ready(None);
            }

            state.nodes[node_idx].waiting_for_message = true;

            if need_schedule {
                state.schedule_wake(deadline, task_id);
            }
            Poll::Pending
        };

        if need_schedule {
            self.wake_scheduled = true;
        }
        outcome
    }
}

/// Future returned by `NodeContext::send()`.
pub struct SendFut<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    src: NodeId,
    dst: NodeId,
    payload: Option<M>,
}

impl<M: Clone + 'static> Future for SendFut<M> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `SendFut` holds only `Rc`, `NodeId`, and `Option<M>` fields,
        // none of which are address-sensitive or self-referential. We never
        // move any field out of the pinned reference (we only call `.take()`
        // on `payload` in place), so obtaining a `&mut Self` is sound.
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(payload) = this.payload.take() {
            let mut state = this.state.borrow_mut();
            let now = state.now;

            // Drop sends that cross an active partition or have no route.
            // The send-future still completes cleanly — the message just
            // never lands in the destination's inbox. Stats track the drop.
            if state.partitions.contains(&pair_key(this.src, this.dst)) {
                #[expect(
                    clippy::arithmetic_side_effects,
                    reason = "monotonic u64 dropped-message counter; 2^64 sends is \
                              physically unreachable in a simulation run"
                )]
                {
                    state.messages_dropped += 1;
                }
                return Poll::Ready(());
            }

            let Some(route) = state.topology.route(this.src, this.dst) else {
                #[expect(
                    clippy::arithmetic_side_effects,
                    reason = "monotonic u64 dropped-message counter; 2^64 sends is \
                              physically unreachable in a simulation run"
                )]
                {
                    state.messages_dropped += 1;
                }
                return Poll::Ready(());
            };

            #[expect(
                clippy::arithmetic_side_effects,
                reason = "Time `+` Duration is internally checked and \
                          panic-on-overflow per the documented time contract"
            )]
            let delivery_time = now + route.total_latency;
            state.events.schedule(
                delivery_time,
                TaskEvent::Deliver {
                    src: this.src,
                    dst: this.dst,
                    payload,
                    sent_at: now,
                },
            );
        }
        Poll::Ready(())
    }
}

/// Either of two possible values.
///
/// Returned by [`select2`] to indicate which arm completed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<A, B> {
    /// The first arm completed.
    Left(A),
    /// The second arm completed.
    Right(B),
}

/// Waits on two futures concurrently and returns whichever completes first.
///
/// Polling is biased: on each poll, `a` is polled before `b`. If both are
/// already ready on the same poll, the result from `a` wins. This is
/// deterministic and well-defined under simulated time.
///
/// Both futures must be safe to re-poll on `Pending`; `Sleep`, `Recv`, and
/// `RecvTimeout` all satisfy this.
pub const fn select2<A, B>(a: A, b: B) -> Select2<A, B>
where
    A: Future,
    B: Future,
{
    Select2 { a, b }
}

/// Future returned by [`select2`].
pub struct Select2<A, B> {
    a: A,
    b: B,
}

impl<A, B> Future for Select2<A, B>
where
    A: Future,
    B: Future,
{
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: we never move the inner futures `a`/`b` out of `self`; we
        // only re-pin them in place below, so projecting `&mut Self` from the
        // pinned reference upholds the structural pinning contract.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: `this.a` is a field of the pinned `Self` and is never moved
        // for the lifetime of this borrow, so re-pinning it is sound.
        let a = unsafe { Pin::new_unchecked(&mut this.a) };
        if let Poll::Ready(v) = a.poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        // SAFETY: `this.b` is a field of the pinned `Self` and is never moved
        // for the lifetime of this borrow, so re-pinning it is sound.
        let b = unsafe { Pin::new_unchecked(&mut this.b) };
        if let Poll::Ready(v) = b.poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    }
}

/// A task wrapping an async function.
struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
    completed: bool,
}

/// Create a no-op waker for our custom executor.
fn create_waker() -> Waker {
    fn clone_fn(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }
    const fn wake_fn(_data: *const ()) {}
    const fn wake_by_ref_fn(_data: *const ()) {}
    const fn drop_fn(_data: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

    let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
    // SAFETY: `VTABLE`'s functions form a valid `RawWakerVTable`: `clone`
    // returns a `RawWaker` with the same vtable and the null data pointer,
    // and `wake`/`wake_by_ref`/`drop` are no-ops that never dereference the
    // (null) data pointer. These uphold the `RawWaker` contract, so building
    // a `Waker` from it is sound.
    unsafe { Waker::from_raw(raw_waker) }
}

/// Builder for creating a task-based simulation.
pub struct TaskSimBuilder<M: 'static> {
    topology: Topology,
    seed: u64,
    _phantom: std::marker::PhantomData<M>,
}

impl<M: 'static> TaskSimBuilder<M> {
    /// Creates a new task simulation builder.
    #[must_use]
    pub const fn new(topology: Topology, seed: u64) -> Self {
        Self {
            topology,
            seed,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Builds the simulation with the given node function.
    ///
    /// The function will be called once for each node to create its task.
    pub fn build<F, Fut>(self, node_fn: F) -> TaskSim<M>
    where
        F: Fn(NodeContext<M>) -> Fut,
        Fut: Future<Output = ()> + 'static,
        M: Clone,
    {
        let node_count = self.topology.node_count();

        // Create shared state
        let state = Rc::new(RefCell::new(SimState {
            now: Time::ZERO,
            topology: self.topology,
            nodes: (0..node_count)
                .map(|i| {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "TaskId is a 32-bit identifier; node_count never \
                                  approaches 2^32"
                    )]
                    let task_id = TaskId(i as u32);
                    NodeState {
                        inbox: VecDeque::new(),
                        waiting_for_message: false,
                        task_id,
                    }
                })
                .collect(),
            events: EventQueue::new(),
            ready_tasks: Vec::new(),
            cancelled: Rc::new(Cell::new(false)),
            partitions: HashSet::new(),
            messages_dropped: 0,
        }));

        // Create tasks for each node
        let mut tasks = Vec::with_capacity(node_count);
        for i in 0..node_count {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "TaskId/NodeId are 32-bit identifiers; node_count never \
                          approaches 2^32"
            )]
            let (task_id, node_id) = (TaskId(i as u32), NodeId::new(i as u32));

            let ctx = NodeContext {
                state: Rc::clone(&state),
                node_id,
                task_id,
            };

            let future = node_fn(ctx);
            tasks.push(Task {
                future: Box::pin(future),
                completed: false,
            });
        }

        // Mark all tasks as ready initially
        {
            let mut s = state.borrow_mut();
            for i in 0..node_count {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "TaskId is a 32-bit identifier; node_count never \
                              approaches 2^32"
                )]
                let task_id = TaskId(i as u32);
                s.ready_tasks.push(task_id);
            }
        }

        TaskSim {
            state,
            tasks,
            seed: self.seed,
            ready_scratch: Vec::new(),
        }
    }
}

/// Statistics from a task simulation run.
#[derive(Debug, Clone, Default)]
pub struct TaskSimStats {
    /// Final simulation time.
    pub final_time: Time,
    /// Total events processed.
    pub events_processed: u64,
    /// Total messages delivered.
    pub messages_delivered: u64,
    /// Total task polls.
    pub task_polls: u64,
    /// Number of tasks that returned `Poll::Ready` during the run.
    pub tasks_completed: u64,
    /// Messages that were dropped instead of delivered, counted from
    /// `inject` and `NodeContext::send` calls hitting an active partition,
    /// failed link, or failed node. Drops are silent — the destination's
    /// inbox is not modified — so callers learn about them via this stat.
    pub messages_dropped: u64,
}

/// A task-based simulation.
pub struct TaskSim<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    tasks: Vec<Task>,
    seed: u64,
    /// Reusable scratch buffer for the ready-task drain in `run_until`.
    ///
    /// Holding it here lets us swap with `state.ready_tasks` each iteration
    /// and reuse its allocation across the whole run.
    ready_scratch: Vec<TaskId>,
}

impl<M: Clone + 'static> TaskSim<M> {
    /// Injects a message into the simulation.
    ///
    /// The message will be delivered after network latency. If the
    /// destination is unreachable from the source (no route, partitioned,
    /// failed link/node), the injection is dropped and the run's
    /// `messages_dropped` counter increments.
    pub fn inject(&mut self, src: NodeId, dst: NodeId, payload: M) {
        let mut state = self.state.borrow_mut();
        let now = state.now;

        if state.partitions.contains(&pair_key(src, dst)) {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "monotonic u64 dropped-message counter; 2^64 sends is \
                          physically unreachable in a simulation run"
            )]
            {
                state.messages_dropped += 1;
            }
            return;
        }

        let Some(route) = state.topology.route(src, dst) else {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "monotonic u64 dropped-message counter; 2^64 sends is \
                          physically unreachable in a simulation run"
            )]
            {
                state.messages_dropped += 1;
            }
            return;
        };

        #[expect(
            clippy::arithmetic_side_effects,
            reason = "Time `+` Duration is internally checked and panic-on-overflow \
                      per the documented time contract"
        )]
        let delivery_time = now + route.total_latency;
        state.events.schedule(
            delivery_time,
            TaskEvent::Deliver {
                src,
                dst,
                payload,
                sent_at: now,
            },
        );
    }

    /// Returns the seed used for this simulation.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Installs a symmetric partition between `a` and `b`. See
    /// [`NodeContext::partition`].
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().partitions.insert(pair_key(a, b));
    }

    /// Removes a previously-installed partition.
    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().partitions.remove(&pair_key(a, b));
    }

    /// Returns `true` if `a` and `b` are currently partitioned.
    #[must_use]
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.state.borrow().partitions.contains(&pair_key(a, b))
    }

    /// Fails the bidirectional link between `a` and `b`.
    pub fn fail_link(&mut self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().topology.fail_link(a, b);
    }

    /// Restores a previously-failed bidirectional link.
    pub fn heal_link(&mut self, a: NodeId, b: NodeId) {
        self.state.borrow_mut().topology.heal_link(a, b);
    }

    /// Returns `true` if both directions of the link between `a` and `b`
    /// are currently failed.
    #[must_use]
    pub fn is_link_failed(&self, a: NodeId, b: NodeId) -> bool {
        self.state.borrow().topology.is_link_failed(a, b)
    }

    /// Fails a single direction of the link between `src` and `dst`.
    pub fn fail_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.state
            .borrow_mut()
            .topology
            .fail_link_directed(src, dst);
    }

    /// Restores a single direction of the link between `src` and `dst`.
    pub fn heal_link_directed(&mut self, src: NodeId, dst: NodeId) {
        self.state
            .borrow_mut()
            .topology
            .heal_link_directed(src, dst);
    }

    /// Returns `true` if the directed edge `src -> dst` is failed.
    #[must_use]
    pub fn is_link_failed_directed(&self, src: NodeId, dst: NodeId) -> bool {
        self.state
            .borrow()
            .topology
            .is_link_failed_directed(src, dst)
    }

    /// Marks `node` as failed.
    pub fn fail_node(&mut self, node: NodeId) {
        self.state.borrow_mut().topology.fail_node(node);
    }

    /// Restores a previously-failed node.
    pub fn heal_node(&mut self, node: NodeId) {
        self.state.borrow_mut().topology.heal_node(node);
    }

    /// Returns `true` if `node` is currently marked as failed.
    #[must_use]
    pub fn is_node_failed(&self, node: NodeId) -> bool {
        self.state.borrow().topology.is_node_failed(node)
    }

    /// Returns a cancellation token for this simulation.
    ///
    /// Call `.cancel()` on the returned token (or any clone) to signal all
    /// tasks to shut down cooperatively.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        CancellationToken {
            flag: Rc::clone(&self.state.borrow().cancelled),
        }
    }

    /// Runs the simulation until no more events or tasks are pending.
    #[must_use]
    pub fn run(self) -> TaskSimStats {
        self.run_until(|_| true)
    }

    /// Runs the simulation until the predicate returns false or completion.
    #[expect(
        clippy::too_many_lines,
        reason = "the task-sim run loop is the determinism-critical core; splitting \
                  it risks reordering events or wakes, so it is kept whole"
    )]
    pub fn run_until<F>(mut self, mut continue_fn: F) -> TaskSimStats
    where
        F: FnMut(Time) -> bool,
    {
        let waker = create_waker();
        let mut cx = Context::from_waker(&waker);
        // Named `run_stats` (not `stats`) so it does not read as a near-typo
        // of the many short-lived `state` borrows in this loop.
        let mut run_stats = TaskSimStats::default();

        loop {
            // Swap the ready queue out into a scratch buffer we own, so we
            // can iterate it without holding a borrow on `state` during
            // poll (polls reborrow `state` to schedule further events).
            // Reusing the buffer across iterations avoids a per-loop alloc.
            self.ready_scratch.clear();
            {
                let mut state = self.state.borrow_mut();
                std::mem::swap(&mut self.ready_scratch, &mut state.ready_tasks);
            }

            for &task_id in &self.ready_scratch {
                if let Some(task) = self.tasks.get_mut(task_id.0 as usize) {
                    if task.completed {
                        continue;
                    }
                    #[expect(
                        clippy::arithmetic_side_effects,
                        reason = "monotonic u64 poll counter; 2^64 polls is \
                                  physically unreachable in a simulation run"
                    )]
                    {
                        run_stats.task_polls += 1;
                    }
                    if task.future.as_mut().poll(&mut cx) == Poll::Ready(()) {
                        task.completed = true;
                        #[expect(
                            clippy::arithmetic_side_effects,
                            reason = "monotonic u64 completion counter; 2^64 \
                                      completions is physically unreachable"
                        )]
                        {
                            run_stats.tasks_completed += 1;
                        }
                    }
                }
            }

            // Find the next event
            let next_event = self.state.borrow_mut().events.pop();

            if let Some(scheduled) = next_event {
                // Check if we should continue
                if !continue_fn(scheduled.time) {
                    break;
                }

                // Advance time
                {
                    let mut state = self.state.borrow_mut();
                    state.now = scheduled.time;
                }
                #[expect(
                    clippy::arithmetic_side_effects,
                    reason = "monotonic u64 event counter; 2^64 events is physically \
                              unreachable in a simulation run"
                )]
                {
                    run_stats.events_processed += 1;
                }

                match scheduled.event {
                    TaskEvent::Wake(task_id) => {
                        let mut state = self.state.borrow_mut();
                        state.ready_tasks.push(task_id);
                    }
                    TaskEvent::Deliver {
                        src,
                        dst,
                        payload,
                        sent_at,
                    } => {
                        let mut state = self.state.borrow_mut();
                        let now = state.now;

                        if dst.as_usize() < state.nodes.len() {
                            #[expect(
                                clippy::indexing_slicing,
                                reason = "guarded by `dst.as_usize() < state.nodes\
                                          .len()` on the enclosing `if`"
                            )]
                            {
                                let task_id = state.nodes[dst.as_usize()].task_id;
                                let waiting = state.nodes[dst.as_usize()].waiting_for_message;

                                state.nodes[dst.as_usize()].inbox.push_back(Envelope {
                                    src,
                                    dst,
                                    payload,
                                    sent_at,
                                    received_at: now,
                                });

                                // Wake the task if it's waiting for a message
                                if waiting {
                                    state.ready_tasks.push(task_id);
                                }
                            }
                        }
                        #[expect(
                            clippy::arithmetic_side_effects,
                            reason = "monotonic u64 delivered-message counter; 2^64 \
                                      deliveries is physically unreachable"
                        )]
                        {
                            run_stats.messages_delivered += 1;
                        }
                    }
                }
            } else {
                // No more events - check if any tasks are ready
                let has_ready = !self.state.borrow().ready_tasks.is_empty();
                if !has_ready {
                    break;
                }
            }
        }

        let state = self.state.borrow();
        run_stats.final_time = state.now;
        run_stats.messages_dropped = state.messages_dropped;
        run_stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::TopologyBuilder;

    #[test]
    fn basic_sleep() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<()>::new(topology, 42).build(|ctx| async move {
            ctx.sleep(Duration::from_millis(100)).await;
        });

        let stats = sim.run();
        assert_eq!(stats.final_time, Time::from_millis(100));
    }

    #[test]
    fn multiple_sleeps() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<()>::new(topology, 42).build(|ctx| async move {
            ctx.sleep(Duration::from_millis(50)).await;
            ctx.sleep(Duration::from_millis(50)).await;
            ctx.sleep(Duration::from_millis(50)).await;
        });

        let stats = sim.run();
        assert_eq!(stats.final_time, Time::from_millis(150));
    }

    #[test]
    fn send_recv() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            match ctx.id().as_u32() {
                0 => {
                    ctx.send(NodeId(1), "hello".to_string()).await;
                }
                1 => {
                    let msg = ctx.recv().await;
                    assert_eq!(msg.payload, "hello");
                    assert_eq!(msg.src, NodeId(0));
                }
                _ => {}
            }
        });

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn ping_pong() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            let id = ctx.id().as_u32();
            let msg = ctx.recv().await;
            if id == 0 {
                ctx.send(NodeId(1), format!("ping from 0, got: {}", msg.payload))
                    .await;
                let reply = ctx.recv().await;
                assert!(reply.payload.starts_with("pong"));
            } else {
                assert!(msg.payload.starts_with("ping"));
                ctx.send(msg.src, "pong from 1".to_string()).await;
            }
        });

        sim.inject(NodeId(1), NodeId(0), "start".to_string());

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 3);
    }

    #[test]
    fn concurrent_nodes() {
        let topology = TopologyBuilder::new(3)
            .full_mesh(Duration::from_millis(5))
            .build();

        let sim = TaskSimBuilder::<u32>::new(topology, 42).build(|ctx| async move {
            let id = ctx.id().as_u32();
            ctx.sleep(Duration::from_millis((u64::from(id) + 1) * 10))
                .await;
            let next = NodeId((id + 1) % 3);
            ctx.send(next, id).await;
            let _msg = ctx.recv().await;
        });

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 3);
    }

    #[test]
    fn deterministic_replay() {
        fn run_sim() -> (Time, u64) {
            let topology = TopologyBuilder::new(2)
                .link(0u32, 1u32, Duration::from_millis(10))
                .build();

            let sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
                if ctx.id().as_u32() == 0 {
                    ctx.sleep(Duration::from_millis(5)).await;
                    ctx.send(NodeId(1), "test".to_string()).await;
                } else {
                    let _ = ctx.recv().await;
                }
            });

            let stats = sim.run();
            (stats.final_time, stats.messages_delivered)
        }

        let (t1, m1) = run_sim();
        let (t2, m2) = run_sim();

        assert_eq!(t1, t2);
        assert_eq!(m1, m2);
    }

    #[test]
    fn now_updates() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<()>::new(topology, 42).build(|ctx| async move {
            assert_eq!(ctx.now(), Time::ZERO);
            ctx.sleep(Duration::from_millis(100)).await;
            assert_eq!(ctx.now(), Time::from_millis(100));
            ctx.sleep(Duration::from_millis(50)).await;
            assert_eq!(ctx.now(), Time::from_millis(150));
        });

        let _stats = sim.run();
    }

    #[test]
    fn self_send() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            ctx.send(ctx.id(), "hello self".to_string()).await;
            let msg = ctx.recv().await;
            assert_eq!(msg.src, ctx.id());
            assert_eq!(msg.dst, ctx.id());
            assert_eq!(msg.payload, "hello self");
        });

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn multiple_queued_messages() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<u32>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 1 {
                let m1 = ctx.recv().await;
                let m2 = ctx.recv().await;
                let m3 = ctx.recv().await;
                assert_eq!(m1.payload, 1);
                assert_eq!(m2.payload, 2);
                assert_eq!(m3.payload, 3);
            }
        });

        sim.inject(NodeId(0), NodeId(1), 1);
        sim.inject(NodeId(0), NodeId(1), 2);
        sim.inject(NodeId(0), NodeId(1), 3);

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 3);
    }

    #[test]
    fn interleaved_sleep_recv() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 0 {
                ctx.sleep(Duration::from_millis(5)).await;
                ctx.send(NodeId(1), "first".to_string()).await;
                ctx.sleep(Duration::from_millis(20)).await;
                ctx.send(NodeId(1), "second".to_string()).await;
            } else {
                let m1 = ctx.recv().await;
                assert_eq!(m1.payload, "first");
                ctx.sleep(Duration::from_millis(5)).await;
                let m2 = ctx.recv().await;
                assert_eq!(m2.payload, "second");
            }
        });

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 2);
    }

    #[test]
    fn no_route_drops_instead_of_delivering() {
        // Topology with no links: inject drops the message rather than
        // silently delivering it. Node 1's task exits via the shutdown
        // token so the run terminates cleanly.
        let topology = TopologyBuilder::new(2).build();

        let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 1 {
                // Use a recv-with-timeout to exit when no message arrives.
                let _ = ctx.recv_timeout(Duration::from_millis(1)).await;
            }
        });

        sim.inject(NodeId(0), NodeId(1), "unreachable?".to_string());

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn fail_link_pre_run_drops_send() {
        // 0 -- 1, link failed before any task runs. The send from inside
        // node 0's task must drop, not deliver.
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<&'static str>::new(topology, 42).build(|ctx| async move {
            if ctx.id() == NodeId(0) {
                ctx.send(NodeId(1), "blocked").await;
            } else {
                let _ = ctx.recv_timeout(Duration::from_millis(1)).await;
            }
        });

        sim.fail_link(NodeId(0), NodeId(1));
        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn fail_link_pre_run_reroutes_send() {
        // Triangle: failing the cheap 0-1 path forces the slow direct edge.
        let topology = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(5))
            .link(1u32, 2u32, Duration::from_millis(5))
            .link(0u32, 2u32, Duration::from_millis(50))
            .build();

        let mut sim = TaskSimBuilder::<&'static str>::new(topology, 42).build(|ctx| async move {
            if ctx.id() == NodeId(0) {
                ctx.send(NodeId(2), "long way").await;
            } else if ctx.id() == NodeId(2) {
                let msg = ctx.recv().await;
                assert_eq!(msg.payload, "long way");
                assert_eq!(msg.received_at, Time::from_millis(50));
            } else {
                let _ = ctx.recv_timeout(Duration::from_millis(1)).await;
            }
        });

        sim.fail_link(NodeId(0), NodeId(1));
        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.messages_dropped, 0);
    }

    #[test]
    fn fail_node_pre_run_drops_sends_to_failed_node() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<&'static str>::new(topology, 42).build(|ctx| async move {
            if ctx.id() == NodeId(0) {
                ctx.send(NodeId(1), "to_dead").await;
            }
            // Node 1's task still runs but its send won't deliver either way.
        });

        sim.fail_node(NodeId(1));
        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn partition_drops_sends_in_both_directions() {
        let topology = TopologyBuilder::new(3)
            .full_mesh(Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<&'static str>::new(topology, 42).build(|ctx| async move {
            match ctx.id().as_u32() {
                0 => {
                    ctx.send(NodeId(1), "blocked_a").await;
                    ctx.send(NodeId(2), "still_works").await;
                }
                1 => {
                    ctx.send(NodeId(0), "blocked_b").await;
                    let _ = ctx.recv_timeout(Duration::from_millis(1)).await;
                }
                _ => {
                    let msg = ctx.recv().await;
                    assert_eq!(msg.payload, "still_works");
                }
            }
        });

        sim.partition(NodeId(0), NodeId(1));
        assert!(sim.is_partitioned(NodeId(0), NodeId(1)));
        let stats = sim.run();
        // 2 dropped (0->1 and 1->0), 1 delivered (0->2).
        assert_eq!(stats.messages_dropped, 2);
        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn node_context_can_fail_and_heal_link_mid_run() {
        // Triangle topology. Node 0's task: send via fast path, then fail
        // it, send again (must reroute via slow), then heal, send again
        // (must deliver via fast). Node 2 records arrival times.
        let topology = TopologyBuilder::new(3)
            .link(0u32, 1u32, Duration::from_millis(5))
            .link(1u32, 2u32, Duration::from_millis(5))
            .link(0u32, 2u32, Duration::from_millis(50))
            .build();

        let arrivals: Rc<RefCell<Vec<(u32, Time)>>> = Rc::new(RefCell::new(Vec::new()));
        let arrivals_for_recv = Rc::clone(&arrivals);

        let sim = TaskSimBuilder::<u32>::new(topology, 42).build(move |ctx| {
            let arrivals = Rc::clone(&arrivals_for_recv);
            async move {
                if ctx.id() == NodeId(0) {
                    ctx.send(NodeId(2), 1).await;
                    ctx.sleep(Duration::from_millis(100)).await;
                    ctx.fail_link(NodeId(0), NodeId(1));
                    ctx.send(NodeId(2), 2).await;
                    ctx.sleep(Duration::from_millis(100)).await;
                    ctx.heal_link(NodeId(0), NodeId(1));
                    ctx.send(NodeId(2), 3).await;
                } else if ctx.id() == NodeId(2) {
                    for _ in 0..3 {
                        let msg = ctx.recv().await;
                        arrivals.borrow_mut().push((msg.payload, msg.received_at));
                    }
                }
            }
        });
        let _stats = sim.run();

        let arrivals = arrivals.borrow();
        assert_eq!(arrivals.len(), 3);
        // First message via 0-1-2 (10ms), arrives at t=10ms.
        // Second message at t=100ms via direct 0-2 (50ms), arrives at t=150ms.
        // Third message at t=200ms via 0-1-2 again (10ms), arrives at t=210ms.
        assert_eq!(arrivals[0], (1, Time::from_millis(10)));
        assert_eq!(arrivals[1], (2, Time::from_millis(150)));
        assert_eq!(arrivals[2], (3, Time::from_millis(210)));
    }

    #[test]
    fn task_completes_early() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let sim = TaskSimBuilder::<()>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 0 {
                ctx.sleep(Duration::from_millis(10)).await;
            } else {
                ctx.sleep(Duration::from_millis(100)).await;
            }
        });

        let stats = sim.run();
        assert_eq!(stats.final_time, Time::from_millis(100));
        assert_eq!(stats.tasks_completed, 2);
    }

    #[test]
    fn recv_timeout_fires() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<u32>::new(topology, 42).build(|ctx| async move {
            // Nobody will ever send us a message, so the timeout should fire.
            let result = ctx.recv_timeout(Duration::from_millis(50)).await;
            assert!(result.is_none());
            assert_eq!(ctx.now(), Time::from_millis(50));
        });

        let stats = sim.run();
        assert_eq!(stats.final_time, Time::from_millis(50));
        assert_eq!(stats.tasks_completed, 1);
    }

    #[test]
    fn recv_timeout_receives_before_deadline() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(10))
            .build();

        let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 1 {
                let result = ctx.recv_timeout(Duration::from_millis(100)).await;
                assert!(result.is_some());
                assert_eq!(result.unwrap().payload, "in-time");
            }
        });

        sim.inject(NodeId(0), NodeId(1), "in-time".to_string());

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn select2_prefers_recv_when_available() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(5))
            .build();

        let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 1 {
                let either = select2(ctx.recv(), ctx.sleep(Duration::from_millis(100))).await;
                match either {
                    Either::Left(env) => assert_eq!(env.payload, "hi"),
                    Either::Right(()) => panic!("expected recv to win"),
                }
            }
        });

        sim.inject(NodeId(0), NodeId(1), "hi".to_string());
        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
    }

    #[test]
    fn select2_falls_back_to_sleep() {
        let topology = TopologyBuilder::new(1).build();

        let sim = TaskSimBuilder::<u32>::new(topology, 42).build(|ctx| async move {
            let either = select2(ctx.recv(), ctx.sleep(Duration::from_millis(25))).await;
            assert!(matches!(either, Either::Right(())));
            assert_eq!(ctx.now(), Time::from_millis(25));
        });

        let stats = sim.run();
        assert_eq!(stats.final_time, Time::from_millis(25));
    }

    #[test]
    fn shutdown_token_observed_by_tasks() {
        let topology = TopologyBuilder::new(2)
            .link(0u32, 1u32, Duration::from_millis(1))
            .build();

        let sim = TaskSimBuilder::<u32>::new(topology, 42).build(|ctx| async move {
            let token = ctx.shutdown_token();
            for i in 0..1000u32 {
                if token.is_cancelled() {
                    break;
                }
                ctx.sleep(Duration::from_millis(1)).await;
                if ctx.id().as_u32() == 0 {
                    ctx.send(NodeId(1), i).await;
                }
            }
        });

        // Cancel before the run even starts.
        sim.shutdown_token().cancel();

        let stats = sim.run();
        assert_eq!(stats.tasks_completed, 2);
        assert_eq!(stats.messages_delivered, 0);
    }
}
