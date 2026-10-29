// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed, read-only observations of the original production poller workers.

use novarocks_control_runtime_core::identity::Tick;

// Sampled by the existing timer owner while it already owns its scheduler
// transaction. Pending/deferred ticks are distinct from the indexed head.
#[derive(Clone, Copy)]
#[cfg_attr(loom, allow(dead_code))]
pub(super) struct TimerScan {
    pub observed_at: Tick,
    pub earliest_indexed_deadline: Option<Tick>,
    pub pending_ticks: usize,
    pub registered_deadlines: usize,
}

#[cfg(not(loom))]
mod production {
    use super::{Tick, TimerScan};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    /// An actual loop phase, never a verdict about health or physical exit.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    #[repr(u64)]
    pub enum PollerPhase {
        Initializing,
        Timer,
        Core,
        Host,
        Idle,
        Exited,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct PollerTimerSnapshot {
        pub observed_at: Tick,
        pub earliest_indexed_deadline: Option<Tick>,
        /// Lateness of the indexed head at this scan, not pending Tick delivery.
        pub indexed_due_lateness_ns: Option<u64>,
        pub pending_ticks: u64,
        pub registered_deadlines: u64,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct PollerWorkerSnapshot {
        pub worker: usize,
        pub phase: PollerPhase,
        pub entered_at: Option<Tick>,
        pub completed_at: Option<Tick>,
        pub completed_iterations: u64,
        pub core_turns: u64,
        pub host_turns: u64,
        pub timer: Option<PollerTimerSnapshot>,
    }
    /// Independent atomic samples over a bounded interval. Exited only says
    /// this loop returned; it is not an OS join, Work exit or settlement receipt.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct PollerObservationSnapshot {
        pub workers: Vec<PollerWorkerSnapshot>,
    }

    struct WorkerSlot {
        phase: AtomicU64,
        entered_at: AtomicU64,
        entered: AtomicBool,
        completed_at: AtomicU64,
        completed: AtomicBool,
        iterations: AtomicU64,
        core_turns: AtomicU64,
        host_turns: AtomicU64,
        overflow: AtomicBool,
        timer_sampled: AtomicBool,
        timer_at: AtomicU64,
        timer_deadline: AtomicU64,
        timer_has_deadline: AtomicBool,
        timer_lateness: AtomicU64,
        timer_pending: AtomicU64,
        timer_registered: AtomicU64,
    }
    impl WorkerSlot {
        fn new() -> Self {
            Self {
                phase: AtomicU64::new(PollerPhase::Initializing as u64),
                entered_at: AtomicU64::new(0),
                entered: AtomicBool::new(false),
                completed_at: AtomicU64::new(0),
                completed: AtomicBool::new(false),
                iterations: AtomicU64::new(0),
                core_turns: AtomicU64::new(0),
                host_turns: AtomicU64::new(0),
                overflow: AtomicBool::new(false),
                timer_sampled: AtomicBool::new(false),
                timer_at: AtomicU64::new(0),
                timer_deadline: AtomicU64::new(0),
                timer_has_deadline: AtomicBool::new(false),
                timer_lateness: AtomicU64::new(0),
                timer_pending: AtomicU64::new(0),
                timer_registered: AtomicU64::new(0),
            }
        }
        fn increment(&self, counter: &AtomicU64) {
            if counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .is_err()
            {
                // Diagnostic overflow makes observation unavailable; it must
                // not fail a healthy runtime or change its resource ownership.
                self.overflow.store(true, Ordering::Release);
            }
        }
        fn snapshot(&self, worker: usize) -> Option<PollerWorkerSnapshot> {
            if self.overflow.load(Ordering::Acquire) {
                return None;
            }
            let phase = match self.phase.load(Ordering::Acquire) {
                0 => PollerPhase::Initializing,
                1 => PollerPhase::Timer,
                2 => PollerPhase::Core,
                3 => PollerPhase::Host,
                4 => PollerPhase::Idle,
                5 => PollerPhase::Exited,
                _ => return None,
            };
            let entered_at = self
                .entered
                .load(Ordering::Acquire)
                .then(|| Tick(self.entered_at.load(Ordering::Relaxed)));
            let completed_at = self
                .completed
                .load(Ordering::Acquire)
                .then(|| Tick(self.completed_at.load(Ordering::Relaxed)));
            let timer = self.timer_sampled.load(Ordering::Acquire).then(|| {
                let deadline = self
                    .timer_has_deadline
                    .load(Ordering::Acquire)
                    .then(|| Tick(self.timer_deadline.load(Ordering::Relaxed)));
                PollerTimerSnapshot {
                    observed_at: Tick(self.timer_at.load(Ordering::Relaxed)),
                    earliest_indexed_deadline: deadline,
                    indexed_due_lateness_ns: deadline
                        .map(|_| self.timer_lateness.load(Ordering::Relaxed)),
                    pending_ticks: self.timer_pending.load(Ordering::Relaxed),
                    registered_deadlines: self.timer_registered.load(Ordering::Relaxed),
                }
            });
            let result = PollerWorkerSnapshot {
                worker,
                phase,
                entered_at,
                completed_at,
                completed_iterations: self.iterations.load(Ordering::Relaxed),
                core_turns: self.core_turns.load(Ordering::Relaxed),
                host_turns: self.host_turns.load(Ordering::Relaxed),
                timer,
            };
            (!self.overflow.load(Ordering::Acquire)).then_some(result)
        }
    }
    struct Backing {
        workers: Vec<WorkerSlot>,
    }
    /// Retains only fixed atomic metadata, without runtime/actor/Work owners,
    /// callbacks, stop authority or thread joins.
    #[derive(Clone)]
    pub struct PollerObservation(Arc<Backing>);
    pub(in crate::driver) struct PollerWriter {
        original: PollerObservation,
        worker: usize,
    }

    impl PollerObservation {
        pub(in crate::driver) fn new(workers: usize) -> Result<Self, std::io::Error> {
            // Actual configured N, never a Task/R-derived multiplier. These
            // are the original pool's process-lived metadata records.
            workers
                .checked_mul(std::mem::size_of::<WorkerSlot>())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Backing>()))
                .ok_or_else(|| std::io::Error::other("poller observation layout overflow"))?;
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(workers)
                .map_err(|_| std::io::Error::other("poller observation allocation failed"))?;
            slots.extend((0..workers).map(|_| WorkerSlot::new()));
            Ok(Self(Arc::new(Backing { workers: slots })))
        }
        pub(in crate::driver) fn writer(&self, worker: usize) -> PollerWriter {
            PollerWriter {
                original: self.clone(),
                worker,
            }
        }
        /// Reads only these original atomic slots. There is no Core, Context,
        /// scheduler or ledger lock, callback, wait, threshold or recovery action.
        pub fn snapshot(&self) -> Option<PollerObservationSnapshot> {
            let mut workers = Vec::new();
            workers.try_reserve_exact(self.0.workers.len()).ok()?;
            for (worker, slot) in self.0.workers.iter().enumerate() {
                workers.push(slot.snapshot(worker)?);
            }
            Some(PollerObservationSnapshot { workers })
        }
    }
    impl PollerWriter {
        fn slot(&self) -> &WorkerSlot {
            &self.original.0.workers[self.worker]
        }
        pub(in crate::driver) fn enter(&self, phase: PollerPhase, now: Tick) {
            let slot = self.slot();
            slot.entered_at.store(now.0, Ordering::Relaxed);
            slot.entered.store(true, Ordering::Release);
            slot.phase.store(phase as u64, Ordering::Release);
        }
        pub(in crate::driver) fn complete(&self, now: Tick) {
            let slot = self.slot();
            slot.completed_at.store(now.0, Ordering::Relaxed);
            slot.completed.store(true, Ordering::Release);
        }
        pub(in crate::driver) fn iteration(&self, core: bool, host: bool) {
            let slot = self.slot();
            slot.increment(&slot.iterations);
            if core {
                slot.increment(&slot.core_turns);
            }
            if host {
                slot.increment(&slot.host_turns);
            }
        }
        pub(in crate::driver) fn timer(&self, scan: TimerScan) {
            let slot = self.slot();
            let (Ok(pending), Ok(indexed)) = (
                u64::try_from(scan.pending_ticks),
                u64::try_from(scan.registered_deadlines),
            ) else {
                slot.overflow.store(true, Ordering::Release);
                return;
            };
            slot.timer_at.store(scan.observed_at.0, Ordering::Relaxed);
            if let Some(deadline) = scan.earliest_indexed_deadline {
                slot.timer_deadline.store(deadline.0, Ordering::Relaxed);
            }
            slot.timer_has_deadline
                .store(scan.earliest_indexed_deadline.is_some(), Ordering::Release);
            slot.timer_lateness.store(
                scan.earliest_indexed_deadline
                    .map_or(0, |deadline| scan.observed_at.0.saturating_sub(deadline.0)),
                Ordering::Relaxed,
            );
            slot.timer_pending.store(pending, Ordering::Relaxed);
            slot.timer_registered.store(indexed, Ordering::Relaxed);
            slot.timer_sampled.store(true, Ordering::Release);
        }
    }
}
#[cfg(not(loom))]
pub use production::{
    PollerObservation, PollerObservationSnapshot, PollerPhase, PollerTimerSnapshot,
    PollerWorkerSnapshot,
};
