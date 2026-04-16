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
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use crate::net::{NodeId, Topology};
use crate::queue::EventQueue;
use crate::time::{Duration, Time};

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
}

impl<M> SimState<M> {
    fn schedule_wake(&mut self, time: Time, task_id: TaskId) {
        self.events.schedule(time, TaskEvent::Wake(task_id));
    }
}

/// A cooperative cancellation signal shared by all tasks in a simulation.
///
/// The flag is set by [`TaskSim::shutdown_token`]`.cancel()` (or by any task
/// via [`NodeContext::shutdown_token`]`.cancel()`). Tasks can poll
/// `is_cancelled()` to decide whether to exit early.
#[derive(Clone)]
pub struct CancellationToken {
    flag: Rc<Cell<bool>>,
}

impl CancellationToken {
    /// Returns `true` if cancellation has been requested.
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
    pub fn id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the current simulation time.
    pub fn now(&self) -> Time {
        self.state.borrow().now
    }

    /// Returns a cancellation token tied to this simulation.
    ///
    /// Tasks may poll `token.is_cancelled()` to decide whether to return
    /// early. Call `.cancel()` on any clone of the token to signal shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        CancellationToken {
            flag: Rc::clone(&self.state.borrow().cancelled),
        }
    }

    /// Sleeps for the specified duration of simulated time.
    pub fn sleep(&self, duration: Duration) -> Sleep<M> {
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
    pub fn recv(&self) -> Recv<M> {
        Recv {
            state: Rc::clone(&self.state),
            node_id: self.node_id,
        }
    }

    /// Receives the next message or returns `None` after `timeout` elapses.
    ///
    /// Deterministic: the timeout is measured in simulated time, not wall-clock.
    pub fn recv_timeout(&self, timeout: Duration) -> RecvTimeout<M> {
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
        // Use get_mut since we need to modify payload
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(payload) = this.payload.take() {
            let mut state = this.state.borrow_mut();
            let now = state.now;

            // Compute delivery time using topology
            let delivery_time = if let Some(route) = state.topology.route(this.src, this.dst) {
                now + route.total_latency
            } else {
                // No route - deliver immediately (or could drop)
                now
            };

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
pub fn select2<A, B>(a: A, b: B) -> Select2<A, B>
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
        // Safety: we do not move the inner futures out of `self`.
        let this = unsafe { self.get_unchecked_mut() };
        let a = unsafe { Pin::new_unchecked(&mut this.a) };
        if let Poll::Ready(v) = a.poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
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
    fn wake_fn(_data: *const ()) {}
    fn wake_by_ref_fn(_data: *const ()) {}
    fn drop_fn(_data: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

    let raw_waker = RawWaker::new(std::ptr::null(), &VTABLE);
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
    pub fn new(topology: Topology, seed: u64) -> Self {
        TaskSimBuilder {
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
                .map(|i| NodeState {
                    inbox: VecDeque::new(),
                    waiting_for_message: false,
                    task_id: TaskId(i as u32),
                })
                .collect(),
            events: EventQueue::new(),
            ready_tasks: Vec::new(),
            cancelled: Rc::new(Cell::new(false)),
        }));

        // Create tasks for each node
        let mut tasks = Vec::with_capacity(node_count);
        for i in 0..node_count {
            let task_id = TaskId(i as u32);
            let node_id = NodeId::new(i as u32);

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
                s.ready_tasks.push(TaskId(i as u32));
            }
        }

        TaskSim {
            state,
            tasks,
            seed: self.seed,
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
}

/// A task-based simulation.
pub struct TaskSim<M: 'static> {
    state: Rc<RefCell<SimState<M>>>,
    tasks: Vec<Task>,
    seed: u64,
}

impl<M: Clone + 'static> TaskSim<M> {
    /// Injects a message into the simulation.
    ///
    /// The message will be delivered after network latency.
    pub fn inject(&mut self, src: NodeId, dst: NodeId, payload: M) {
        let mut state = self.state.borrow_mut();
        let now = state.now;

        let delivery_time = if let Some(route) = state.topology.route(src, dst) {
            now + route.total_latency
        } else {
            now
        };

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
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns a cancellation token for this simulation.
    ///
    /// Call `.cancel()` on the returned token (or any clone) to signal all
    /// tasks to shut down cooperatively.
    pub fn shutdown_token(&self) -> CancellationToken {
        CancellationToken {
            flag: Rc::clone(&self.state.borrow().cancelled),
        }
    }

    /// Runs the simulation until no more events or tasks are pending.
    pub fn run(self) -> TaskSimStats {
        self.run_until(|_| true)
    }

    /// Runs the simulation until the predicate returns false or completion.
    pub fn run_until<F>(mut self, mut continue_fn: F) -> TaskSimStats
    where
        F: FnMut(Time) -> bool,
    {
        let waker = create_waker();
        let mut cx = Context::from_waker(&waker);
        let mut stats = TaskSimStats::default();

        loop {
            // Poll all ready tasks
            let ready_tasks: Vec<TaskId> = {
                let mut state = self.state.borrow_mut();
                std::mem::take(&mut state.ready_tasks)
            };

            for task_id in ready_tasks {
                if let Some(task) = self.tasks.get_mut(task_id.0 as usize) {
                    if task.completed {
                        continue;
                    }
                    stats.task_polls += 1;
                    if let Poll::Ready(()) = task.future.as_mut().poll(&mut cx) {
                        task.completed = true;
                        stats.tasks_completed += 1;
                    }
                }
            }

            // Find the next event
            let next_event = self.state.borrow_mut().events.pop();

            match next_event {
                Some(scheduled) => {
                    // Check if we should continue
                    if !continue_fn(scheduled.time) {
                        break;
                    }

                    // Advance time
                    {
                        let mut state = self.state.borrow_mut();
                        state.now = scheduled.time;
                    }
                    stats.events_processed += 1;

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
                            stats.messages_delivered += 1;
                        }
                    }
                }
                None => {
                    // No more events - check if any tasks are ready
                    let has_ready = !self.state.borrow().ready_tasks.is_empty();
                    if !has_ready {
                        break;
                    }
                }
            }
        }

        stats.final_time = self.state.borrow().now;
        stats
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
            if id == 0 {
                let msg = ctx.recv().await;
                ctx.send(NodeId(1), format!("ping from 0, got: {}", msg.payload))
                    .await;
                let reply = ctx.recv().await;
                assert!(reply.payload.starts_with("pong"));
            } else {
                let msg = ctx.recv().await;
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
            ctx.sleep(Duration::from_millis((id as u64 + 1) * 10)).await;
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

        sim.run();
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
    fn no_route_still_delivers() {
        let topology = TopologyBuilder::new(2).build(); // No links!

        let mut sim = TaskSimBuilder::<String>::new(topology, 42).build(|ctx| async move {
            if ctx.id().as_u32() == 1 {
                let msg = ctx.recv().await;
                assert_eq!(msg.payload, "unreachable?");
            }
        });

        sim.inject(NodeId(0), NodeId(1), "unreachable?".to_string());

        let stats = sim.run();
        assert_eq!(stats.messages_delivered, 1);
        assert_eq!(stats.final_time, Time::ZERO);
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
                    Either::Right(_) => panic!("expected recv to win"),
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
            assert!(matches!(either, Either::Right(_)));
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
