//! Parallel execution of independent simulation runs.
//!
//! Simulacra's stance is that a single simulation run is deterministic and
//! single-threaded. But many use cases (seed sweeps, property-based testing,
//! Monte Carlo exploration) benefit from running *many independent runs in
//! parallel across seeds*. That's what this module provides.
//!
//! # Example
//!
//! ```
//! use simulacra::parallel::run_seeds;
//!
//! let results = run_seeds(100, /* base_seed */ 0, /* threads */ None, |seed| {
//!     // Pretend this is a real simulation. Return anything `Send`.
//!     seed.wrapping_mul(31)
//! });
//!
//! assert_eq!(results.len(), 100);
//! // Results come back in seed order (0, 1, 2, ...), NOT completion order,
//! // so callers can rely on index == iteration.
//! ```
//!
//! # Determinism
//!
//! Each closure invocation receives the seed `base_seed.wrapping_add(i)` for
//! `i in 0..count`. The returned `Vec` is ordered by seed index, not
//! completion order, so the output is fully deterministic regardless of
//! thread count or machine load. Individual runs are still required to be
//! deterministic per Simulacra's core contract — this module does not add
//! any new non-determinism.
//!
//! # Thread count
//!
//! Passing `None` uses `std::thread::available_parallelism()`, falling back
//! to 1 if that fails. You can pass a specific number for benchmarking or
//! CI stability.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

/// Runs `factory(seed)` for `count` seeds derived from `base_seed`, using up
/// to `threads` threads (or the available parallelism if `None`).
///
/// The returned `Vec` has length `count` and is ordered by seed index,
/// regardless of completion order. Seeds used are
/// `base_seed.wrapping_add(i)` for `i in 0..count`.
///
/// # Panics
///
/// If any worker panics, this call will panic (via the join handle) once all
/// workers have been joined. Results from runs that completed before the
/// panic on other threads are discarded in that case.
#[expect(
    clippy::expect_used,
    reason = "join() re-raises worker panics (documented under # Panics); the \
              slot-fill expect is a construction invariant — every index in \
              0..count is produced exactly once before the merge"
)]
pub fn run_seeds<F, T>(
    count: usize,
    base_seed: u64,
    threads: Option<NonZeroUsize>,
    factory: F,
) -> Vec<T>
where
    F: Fn(u64) -> T + Sync,
    T: Send,
{
    if count == 0 {
        return Vec::new();
    }

    let thread_count = threads
        .or_else(|| thread::available_parallelism().ok())
        .map_or(1, NonZeroUsize::get)
        .min(count);

    // Shared work cursor: each worker grabs the next seed index atomically.
    let cursor = AtomicUsize::new(0);
    let factory = &factory;
    let cursor = &cursor;

    // Each worker returns its own Vec<(index, T)>. We merge into an
    // index-ordered Vec<T> after joining. This avoids any shared mutable
    // state and therefore any unsafe code.
    let chunks: Vec<Vec<(usize, T)>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(thread_count);
        for _ in 0..thread_count {
            handles.push(s.spawn(move || {
                let mut local: Vec<(usize, T)> = Vec::new();
                loop {
                    let i = cursor.fetch_add(1, Ordering::Relaxed);
                    if i >= count {
                        break;
                    }
                    let seed = base_seed.wrapping_add(i as u64);
                    local.push((i, factory(seed)));
                }
                local
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().expect("worker panicked"))
            .collect()
    });

    // Merge: place each (index, value) in the right slot.
    let mut slots: Vec<Option<T>> = Vec::with_capacity(count);
    slots.resize_with(count, || None);
    for chunk in chunks {
        for (i, v) in chunk {
            #[expect(
                clippy::indexing_slicing,
                reason = "`i` comes from the work cursor guarded by `if i >= count \
                          { break }`, so 0 <= i < count == slots.len()"
            )]
            {
                debug_assert!(slots[i].is_none(), "index {i} filled twice");
                slots[i] = Some(v);
            }
        }
    }

    slots
        .into_iter()
        .map(|o| o.expect("all slots must be filled after workers join"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_count_returns_empty() {
        let results: Vec<u64> = run_seeds(0, 0, None, |s| s);
        assert!(results.is_empty());
    }

    #[test]
    fn results_come_back_in_seed_order() {
        let results = run_seeds(100, 1000, None, |seed| seed);
        assert_eq!(results.len(), 100);
        for (i, r) in results.iter().enumerate() {
            assert_eq!(*r, 1000 + i as u64);
        }
    }

    #[test]
    fn single_thread_matches_multi_thread_output() {
        let factory = |seed: u64| seed.wrapping_mul(31).rotate_left(7);
        let single = run_seeds(50, 42, NonZeroUsize::new(1), factory);
        let multi = run_seeds(50, 42, NonZeroUsize::new(8), factory);
        assert_eq!(single, multi);
    }

    #[test]
    fn thread_count_higher_than_count_is_fine() {
        let results = run_seeds(3, 0, NonZeroUsize::new(16), |seed| seed);
        assert_eq!(results, vec![0, 1, 2]);
    }
}
