// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Process-wide admission for Frontend Native task-operation transport.
//!
//! Attempts keep their own fair dispatch queues. This owner protects their
//! shared process resource from the moment an immutable operation is queued,
//! through request encoding, until accepted Native I/O settles. Admission is
//! non-blocking and retains a control reserve, so an ordinary create/update
//! burst cannot consume the capacity needed to cancel, abort, or release work.
//! A refused caller keeps its exact carrier and registers one deduplicated
//! readiness wake; no waiter task or future is spawned.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use novarocks_task_codec::TransportBudget;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeTransportSupervisorBudget {
    max_items: usize,
    max_encoded_bytes: usize,
    reserved_control_items: usize,
    reserved_control_bytes: usize,
    max_batch_encoded_bytes: usize,
    max_waiters: usize,
}

impl NativeTransportSupervisorBudget {
    pub(crate) fn from_transport(transport: TransportBudget) -> Result<Self, String> {
        let reserved_control_bytes = transport.max_retained_batch_bytes();
        let budget = Self {
            max_items: transport.max_backend_queued_operations(),
            max_encoded_bytes: transport.max_backend_queued_bytes(),
            reserved_control_items: transport.max_batch_items(),
            // During admission both the immutable operation payload and its
            // encoded request are live. Reserve one maximum batch of each.
            reserved_control_bytes,
            max_batch_encoded_bytes: transport.max_batch_encoded_bytes(),
            max_waiters: transport.max_backend_queued_operations(),
        };
        budget.validate()?;
        Ok(budget)
    }

    fn validate(self) -> Result<(), String> {
        let minimum_items = self
            .reserved_control_items
            .checked_mul(2)
            .ok_or_else(|| "native task transport item windows overflowed".to_owned())?;
        let minimum_bytes = self
            .reserved_control_bytes
            .checked_mul(2)
            .ok_or_else(|| "native task transport retained-byte windows overflowed".to_owned())?;
        if minimum_items > self.max_items || minimum_bytes > self.max_encoded_bytes {
            return Err(
                "native task transport process bounds must fit one maximum ordinary batch and one maximum control batch"
                    .to_owned(),
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new(
        max_items: usize,
        max_encoded_bytes: usize,
        reserved_control_items: usize,
        reserved_control_bytes: usize,
        max_waiters: usize,
    ) -> Option<Self> {
        let budget = Self {
            max_items,
            max_encoded_bytes,
            reserved_control_items,
            reserved_control_bytes,
            max_batch_encoded_bytes: reserved_control_bytes / 2,
            max_waiters,
        };
        if max_items == 0
            || max_encoded_bytes == 0
            || reserved_control_items == 0
            || reserved_control_bytes == 0
            || max_waiters == 0
            || budget.validate().is_err()
        {
            return None;
        }
        Some(budget)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativeTransportLane {
    Ordinary,
    Control,
}

/// A process-level capacity change wake, implemented by an attempt's intake.
pub(crate) trait NativeTransportReadyWake: fmt::Debug + Send + Sync {
    fn wake(&self);
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct NativeTransportSnapshot {
    pub(crate) retained_items: usize,
    pub(crate) retained_queued_and_encoded_bytes: usize,
    pub(crate) ordinary_items: usize,
    pub(crate) ordinary_queued_and_encoded_bytes: usize,
    pub(crate) waiting_attempts: usize,
}

#[derive(Debug, Default)]
struct SupervisorState {
    retained_items: usize,
    retained_queued_and_encoded_bytes: usize,
    ordinary_items: usize,
    ordinary_queued_and_encoded_bytes: usize,
    ordinary_queue_bytes: usize,
    control_queue_bytes: usize,
    next_waiter_id: u64,
    waiters: BTreeMap<u64, Weak<dyn NativeTransportReadyWake>>,
    waiting: BTreeSet<u64>,
}

#[derive(Debug)]
struct SupervisorInner {
    budget: NativeTransportSupervisorBudget,
    source_transport: Option<TransportBudget>,
    state: Mutex<SupervisorState>,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeTransportSupervisor {
    inner: Arc<SupervisorInner>,
}

impl NativeTransportSupervisor {
    pub(crate) fn from_transport(transport: TransportBudget) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(SupervisorInner {
                budget: NativeTransportSupervisorBudget::from_transport(transport)?,
                source_transport: Some(transport),
                state: Mutex::new(SupervisorState::default()),
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn new(budget: NativeTransportSupervisorBudget) -> Self {
        Self {
            inner: Arc::new(SupervisorInner {
                budget,
                source_transport: None,
                state: Mutex::new(SupervisorState::default()),
            }),
        }
    }

    pub(crate) fn accepts_transport_budget(&self, transport: TransportBudget) -> bool {
        self.inner
            .source_transport
            .is_none_or(|source| source == transport)
    }

    pub(crate) fn register_waiter(
        &self,
        wake: Arc<dyn NativeTransportReadyWake>,
    ) -> Result<NativeTransportWaiter, String> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("native transport supervisor");
        state.waiters.retain(|_, wake| wake.strong_count() > 0);
        if state.waiters.len() >= self.inner.budget.max_waiters {
            return Err(format!(
                "native task transport waiter capacity {} is exhausted",
                self.inner.budget.max_waiters
            ));
        }
        let id = state
            .next_waiter_id
            .checked_add(1)
            .ok_or_else(|| "native task transport waiter identity exhausted".to_owned())?;
        state.next_waiter_id = id;
        state.waiters.insert(id, Arc::downgrade(&wake));
        Ok(NativeTransportWaiter {
            id,
            inner: Arc::downgrade(&self.inner),
            _wake: wake,
        })
    }

    pub(crate) fn try_reserve_queue(
        &self,
        waiter: &NativeTransportWaiter,
        lane: NativeTransportLane,
        items: usize,
        queued_bytes: usize,
    ) -> Result<NativeTransportQueuePermit, NativeTransportBackpressure> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("native transport supervisor");
        let total_fits = items > 0
            && queued_bytes > 0
            && state.retained_items.saturating_add(items) <= self.inner.budget.max_items
            && state
                .retained_queued_and_encoded_bytes
                .saturating_add(queued_bytes)
                <= self.inner.budget.max_encoded_bytes;
        let ordinary_window = self
            .inner
            .budget
            .max_encoded_bytes
            .saturating_sub(self.inner.budget.reserved_control_bytes);
        let queue_window = match lane {
            NativeTransportLane::Ordinary => ordinary_window,
            NativeTransportLane::Control => self.inner.budget.reserved_control_bytes,
        }
        .saturating_sub(self.inner.budget.max_batch_encoded_bytes);
        let lane_queue_bytes = match lane {
            NativeTransportLane::Ordinary => state.ordinary_queue_bytes,
            NativeTransportLane::Control => state.control_queue_bytes,
        };
        let queue_fits = lane_queue_bytes.saturating_add(queued_bytes) <= queue_window;
        let ordinary_fits = lane == NativeTransportLane::Control
            || (state.ordinary_items.saturating_add(items)
                <= self
                    .inner
                    .budget
                    .max_items
                    .saturating_sub(self.inner.budget.reserved_control_items)
                && state
                    .ordinary_queued_and_encoded_bytes
                    .saturating_add(queued_bytes)
                    <= self
                        .inner
                        .budget
                        .max_encoded_bytes
                        .saturating_sub(self.inner.budget.reserved_control_bytes));
        if !total_fits || !ordinary_fits || !queue_fits {
            if state.waiters.contains_key(&waiter.id) {
                state.waiting.insert(waiter.id);
            }
            return Err(NativeTransportBackpressure);
        }
        state.waiting.remove(&waiter.id);
        state.retained_items += items;
        state.retained_queued_and_encoded_bytes += queued_bytes;
        if lane == NativeTransportLane::Ordinary {
            state.ordinary_items += items;
            state.ordinary_queued_and_encoded_bytes += queued_bytes;
            state.ordinary_queue_bytes += queued_bytes;
        } else {
            state.control_queue_bytes += queued_bytes;
        }
        Ok(NativeTransportQueuePermit {
            inner: Arc::clone(&self.inner),
            lane,
            items,
            queued_bytes,
            in_flight: false,
        })
    }

    /// Reserves the encoded request before the codec allocates it.
    ///
    /// Queue admission keeps one maximum encoded batch free in each lane's
    /// process window, so a retained head batch can always make this transition.
    pub(crate) fn try_reserve_encoding(
        &self,
        waiter: &NativeTransportWaiter,
        lane: NativeTransportLane,
    ) -> Result<NativeTransportEncodingPermit, NativeTransportBackpressure> {
        let encoded_bytes = self.inner.budget.max_batch_encoded_bytes;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("native transport supervisor");
        let total_fits = state
            .retained_queued_and_encoded_bytes
            .saturating_add(encoded_bytes)
            <= self.inner.budget.max_encoded_bytes;
        let ordinary_fits = lane == NativeTransportLane::Control
            || state
                .ordinary_queued_and_encoded_bytes
                .saturating_add(encoded_bytes)
                <= self
                    .inner
                    .budget
                    .max_encoded_bytes
                    .saturating_sub(self.inner.budget.reserved_control_bytes);
        if !total_fits || !ordinary_fits {
            if state.waiters.contains_key(&waiter.id) {
                state.waiting.insert(waiter.id);
            }
            return Err(NativeTransportBackpressure);
        }
        state.waiting.remove(&waiter.id);
        state.retained_queued_and_encoded_bytes += encoded_bytes;
        if lane == NativeTransportLane::Ordinary {
            state.ordinary_queued_and_encoded_bytes += encoded_bytes;
        }
        Ok(NativeTransportEncodingPermit {
            inner: Arc::clone(&self.inner),
            lane,
            retained_bytes: encoded_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> NativeTransportSnapshot {
        let state = self
            .inner
            .state
            .lock()
            .expect("native transport supervisor");
        NativeTransportSnapshot {
            retained_items: state.retained_items,
            retained_queued_and_encoded_bytes: state.retained_queued_and_encoded_bytes,
            ordinary_items: state.ordinary_items,
            ordinary_queued_and_encoded_bytes: state.ordinary_queued_and_encoded_bytes,
            waiting_attempts: state.waiting.len(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeTransportBackpressure;

#[derive(Debug)]
pub(crate) struct NativeTransportWaiter {
    id: u64,
    inner: Weak<SupervisorInner>,
    /// Keeps the weak registry entry live for exactly this sink lifetime.
    _wake: Arc<dyn NativeTransportReadyWake>,
}

impl Drop for NativeTransportWaiter {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner.state.lock().expect("native transport supervisor");
        state.waiters.remove(&self.id);
        state.waiting.remove(&self.id);
    }
}

#[derive(Debug)]
pub(crate) struct NativeTransportQueuePermit {
    inner: Arc<SupervisorInner>,
    lane: NativeTransportLane,
    items: usize,
    queued_bytes: usize,
    in_flight: bool,
}

impl NativeTransportQueuePermit {
    pub(crate) fn mark_in_flight(&mut self) {
        if self.in_flight {
            return;
        }
        let wakes = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("native transport supervisor");
            let queue_bytes = match self.lane {
                NativeTransportLane::Ordinary => &mut state.ordinary_queue_bytes,
                NativeTransportLane::Control => &mut state.control_queue_bytes,
            };
            *queue_bytes = queue_bytes
                .checked_sub(self.queued_bytes)
                .expect("native transport queue accounting is exact");
            self.in_flight = true;
            take_waiter_wakes(&mut state)
        };
        wake_all(wakes);
    }
}

impl crate::task_execution::intent::TaskOperationQueuePermit for NativeTransportQueuePermit {
    fn mark_in_flight(&mut self) {
        NativeTransportQueuePermit::mark_in_flight(self);
    }
}

impl Drop for NativeTransportQueuePermit {
    fn drop(&mut self) {
        let wakes = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("native transport supervisor");
            state.retained_items = state
                .retained_items
                .checked_sub(self.items)
                .expect("native transport permit item accounting is exact");
            state.retained_queued_and_encoded_bytes = state
                .retained_queued_and_encoded_bytes
                .checked_sub(self.queued_bytes)
                .expect("native transport permit byte accounting is exact");
            if !self.in_flight {
                let queue_bytes = match self.lane {
                    NativeTransportLane::Ordinary => &mut state.ordinary_queue_bytes,
                    NativeTransportLane::Control => &mut state.control_queue_bytes,
                };
                *queue_bytes = queue_bytes
                    .checked_sub(self.queued_bytes)
                    .expect("native transport queue accounting is exact");
            }
            if self.lane == NativeTransportLane::Ordinary {
                state.ordinary_items = state
                    .ordinary_items
                    .checked_sub(self.items)
                    .expect("native ordinary transport item accounting is exact");
                state.ordinary_queued_and_encoded_bytes = state
                    .ordinary_queued_and_encoded_bytes
                    .checked_sub(self.queued_bytes)
                    .expect("native ordinary transport byte accounting is exact");
            }
            take_waiter_wakes(&mut state)
        };
        wake_all(wakes);
    }
}

#[derive(Debug)]
pub(crate) struct NativeTransportEncodingPermit {
    inner: Arc<SupervisorInner>,
    lane: NativeTransportLane,
    retained_bytes: usize,
}

impl NativeTransportEncodingPermit {
    pub(crate) fn shrink_to(&mut self, actual: usize) {
        assert!(
            actual <= self.retained_bytes,
            "encoded Native request exceeded its reserved maximum"
        );
        let released = self.retained_bytes - actual;
        if released == 0 {
            return;
        }
        let wakes = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("native transport supervisor");
            state.retained_queued_and_encoded_bytes = state
                .retained_queued_and_encoded_bytes
                .checked_sub(released)
                .expect("native encoding reservation is exact");
            if self.lane == NativeTransportLane::Ordinary {
                state.ordinary_queued_and_encoded_bytes = state
                    .ordinary_queued_and_encoded_bytes
                    .checked_sub(released)
                    .expect("native ordinary encoding reservation is exact");
            }
            self.retained_bytes = actual;
            take_waiter_wakes(&mut state)
        };
        wake_all(wakes);
    }
}

impl Drop for NativeTransportEncodingPermit {
    fn drop(&mut self) {
        let wakes = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("native transport supervisor");
            state.retained_queued_and_encoded_bytes = state
                .retained_queued_and_encoded_bytes
                .checked_sub(self.retained_bytes)
                .expect("native encoding reservation is exact");
            if self.lane == NativeTransportLane::Ordinary {
                state.ordinary_queued_and_encoded_bytes = state
                    .ordinary_queued_and_encoded_bytes
                    .checked_sub(self.retained_bytes)
                    .expect("native ordinary encoding reservation is exact");
            }
            take_waiter_wakes(&mut state)
        };
        wake_all(wakes);
    }
}

fn take_waiter_wakes(state: &mut SupervisorState) -> Vec<Arc<dyn NativeTransportReadyWake>> {
    std::mem::take(&mut state.waiting)
        .into_iter()
        .filter_map(|id| state.waiters.get(&id).and_then(Weak::upgrade))
        .collect()
}

fn wake_all(wakes: Vec<Arc<dyn NativeTransportReadyWake>>) {
    for wake in wakes {
        wake.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Debug, Default)]
    struct CountingWake(AtomicUsize);

    impl NativeTransportReadyWake for CountingWake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn supervisor() -> NativeTransportSupervisor {
        NativeTransportSupervisor::new(
            NativeTransportSupervisorBudget::new(4, 100, 1, 20, 4).expect("valid test budget"),
        )
    }

    fn reserve_for_send(
        supervisor: &NativeTransportSupervisor,
        waiter: &NativeTransportWaiter,
        lane: NativeTransportLane,
        items: usize,
        queued_bytes: usize,
        encoded_bytes: usize,
    ) -> (NativeTransportQueuePermit, NativeTransportEncodingPermit) {
        let mut queued = supervisor
            .try_reserve_queue(waiter, lane, items, queued_bytes)
            .expect("queue capacity");
        let mut encoded = supervisor
            .try_reserve_encoding(waiter, lane)
            .expect("encoding capacity reserved before allocation");
        encoded.shrink_to(encoded_bytes);
        queued.mark_in_flight();
        (queued, encoded)
    }

    #[test]
    fn ordinary_work_cannot_consume_the_control_reserve() {
        let supervisor = supervisor();
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor
            .register_waiter(wake.clone())
            .expect("register waiter");
        let ordinary = reserve_for_send(
            &supervisor,
            &waiter,
            NativeTransportLane::Ordinary,
            3,
            70,
            10,
        );
        assert!(
            supervisor
                .try_reserve_queue(&waiter, NativeTransportLane::Ordinary, 1, 1)
                .is_err(),
            "ordinary work must leave both control reserves intact"
        );
        let control = reserve_for_send(
            &supervisor,
            &waiter,
            NativeTransportLane::Control,
            1,
            10,
            10,
        );
        assert_eq!(
            supervisor.snapshot(),
            NativeTransportSnapshot {
                retained_items: 4,
                retained_queued_and_encoded_bytes: 100,
                ordinary_items: 3,
                ordinary_queued_and_encoded_bytes: 80,
                waiting_attempts: 0,
            }
        );
        drop(control);
        drop(ordinary);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    #[test]
    fn backpressure_registers_one_wake_and_drop_releases_exact_capacity() {
        let supervisor = supervisor();
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor
            .register_waiter(wake.clone())
            .expect("register waiter");
        let permit = supervisor
            .try_reserve_queue(&waiter, NativeTransportLane::Ordinary, 3, 70)
            .expect("ordinary capacity");
        assert!(
            supervisor
                .try_reserve_queue(&waiter, NativeTransportLane::Ordinary, 1, 1)
                .is_err()
        );
        assert!(
            supervisor
                .try_reserve_queue(&waiter, NativeTransportLane::Ordinary, 1, 1)
                .is_err()
        );
        assert_eq!(supervisor.snapshot().waiting_attempts, 1);
        drop(permit);
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    #[test]
    fn waiter_registry_has_a_hard_bound() {
        let supervisor = NativeTransportSupervisor::new(
            NativeTransportSupervisorBudget::new(4, 100, 1, 20, 1).expect("valid test budget"),
        );
        let first_wake = Arc::new(CountingWake::default());
        let first = supervisor
            .register_waiter(first_wake.clone())
            .expect("first waiter");
        assert!(
            supervisor
                .register_waiter(Arc::new(CountingWake::default()))
                .is_err()
        );
        drop(first);
        assert!(
            supervisor
                .register_waiter(Arc::new(CountingWake::default()))
                .is_ok()
        );
    }

    #[test]
    fn process_budget_must_leave_one_control_and_one_ordinary_window() {
        assert!(NativeTransportSupervisorBudget::new(2, 39, 1, 20, 2).is_none());
        assert!(NativeTransportSupervisorBudget::new(1, 40, 1, 20, 2).is_none());
    }

    #[test]
    fn one_maximum_ordinary_batch_is_always_admissible() {
        let transport = TransportBudget::new(2, 10, 5, 2, 20, 4, 40, 1, 2, Duration::from_secs(1))
            .expect("one ordinary and one control batch fit exactly");
        let supervisor = NativeTransportSupervisor::from_transport(transport)
            .expect("the transport budget proves both windows");
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor
            .register_waiter(wake)
            .expect("register one attempt");
        let permit = reserve_for_send(
            &supervisor,
            &waiter,
            NativeTransportLane::Ordinary,
            transport.max_batch_items(),
            transport.max_operation_queued_bytes(),
            transport.max_batch_encoded_bytes(),
        );
        assert_eq!(
            supervisor.snapshot(),
            NativeTransportSnapshot {
                retained_items: 2,
                retained_queued_and_encoded_bytes: 20,
                ordinary_items: 2,
                ordinary_queued_and_encoded_bytes: 20,
                waiting_attempts: 0,
            }
        );
        drop(permit);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }
}
