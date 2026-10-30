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
//!
//! The process item/byte ceiling is a transitional cumulative FE budget. The
//! same owner's per-target window spans attempts, so one slow backend cannot
//! retain the whole ordinary process window. Queue residence and the
//! attempt-local dispatcher bounds remain distinct; neither is a message-format
//! limit. A later resource owner may replace the cumulative ceiling without
//! removing the control reserve or slow-target isolation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use novarocks_task_codec::TransportBudget;
use novarocks_types::BackendProcessId;

/// Refusal to install one attempt's process transport readiness registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeTransportRegistrationError {
    Capacity { limit: usize },
    IdentityExhausted,
}

impl novarocks_execution_contract::failure::FailureProjection for NativeTransportRegistrationError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts,
            ResourceScope, domain,
        };
        let code = match self {
            Self::Capacity { .. } => 104,
            Self::IdentityExhausted => 105,
        };
        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::Capacity { limit } => fact.with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Process),
                dimension: ResourceDimension::Positions,
                requested: Some(1),
                required_growth: Some(1),
                limit: u64::try_from(*limit).ok(),
                live: None,
                committed: None,
            }),
            Self::IdentityExhausted => fact,
        }
    }
}

impl NativeTransportRegistrationError {
    pub(crate) fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        novarocks_execution_contract::SafeDetail::truncating(match self {
            Self::Capacity { .. } => "native task transport waiter capacity is exhausted",
            Self::IdentityExhausted => "native task transport waiter identity space is exhausted",
        })
    }
}
impl fmt::Display for NativeTransportRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_detail().as_str())
    }
}
impl std::error::Error for NativeTransportRegistrationError {}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeTransportSupervisorBudget {
    // Cumulative process windows, including queued and accepted sends.
    max_items: usize,
    max_encoded_bytes: usize,
    // One target across all attempts, including accepted sends. Ordinary work
    // uses this window; control may use one additional reserved batch.
    max_target_items: usize,
    max_target_retained_bytes: usize,
    // Capacity kept available for the control lane under ordinary saturation.
    reserved_control_items: usize,
    reserved_control_bytes: usize,
    max_batch_encoded_bytes: usize,
    max_waiters: usize,
}

/// Limits read from the actual original supervisor, not a second wallet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeTaskOperationCapacityFacts {
    pub(crate) process_items: usize,
    pub(crate) process_retained_bytes: usize,
    pub(crate) target_items: usize,
    pub(crate) target_retained_bytes: usize,
    pub(crate) control_reserved_items: usize,
    pub(crate) control_reserved_bytes: usize,
    pub(crate) maximum_batch_encoded_bytes: usize,
    pub(crate) waiter_records: usize,
}

impl NativeTransportSupervisorBudget {
    pub(crate) fn from_transport(transport: TransportBudget) -> Result<Self, String> {
        let reserved_control_bytes = transport.max_retained_batch_bytes();
        let budget = Self {
            max_items: transport.max_backend_queued_operations(),
            max_encoded_bytes: transport.max_backend_queued_bytes(),
            max_target_items: transport.max_query_backend_queued_operations(),
            // Queue and encoded forms overlap until the RPC settles. A
            // configured one-batch target window must still fit that batch.
            max_target_retained_bytes: transport
                .max_query_backend_queued_bytes()
                .max(transport.max_retained_batch_bytes()),
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
            max_target_items: max_items,
            max_target_retained_bytes: max_encoded_bytes,
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
    by_target: BTreeMap<BackendProcessId, TargetRetained>,
    next_waiter_id: u64,
    waiters: BTreeMap<u64, Weak<dyn NativeTransportReadyWake>>,
    waiting: BTreeSet<u64>,
}

#[derive(Debug, Default)]
struct TargetRetained {
    items: usize,
    bytes: usize,
    ordinary_items: usize,
    ordinary_bytes: usize,
    ordinary_queue_bytes: usize,
}

impl TargetRetained {
    fn empty(&self) -> bool {
        self.items == 0 && self.bytes == 0
    }
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
    /// The same preexisting registration lease bounds live operation sinks.
    /// No queue/action permit or extra waiter inventory is introduced here.
    pub(crate) fn source_waiter_records_for_transport(
        transport: TransportBudget,
    ) -> Result<usize, String> {
        Ok(NativeTransportSupervisorBudget::from_transport(transport)?.max_waiters)
    }

    pub(crate) fn capacity_sources(&self) -> NativeTaskOperationCapacityFacts {
        let budget = self.inner.budget;
        NativeTaskOperationCapacityFacts {
            process_items: budget.max_items,
            process_retained_bytes: budget.max_encoded_bytes,
            target_items: budget.max_target_items,
            target_retained_bytes: budget.max_target_retained_bytes,
            control_reserved_items: budget.reserved_control_items,
            control_reserved_bytes: budget.reserved_control_bytes,
            maximum_batch_encoded_bytes: budget.max_batch_encoded_bytes,
            waiter_records: budget.max_waiters,
        }
    }

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
    ) -> Result<NativeTransportWaiter, NativeTransportRegistrationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("native transport supervisor");
        state.waiters.retain(|_, wake| wake.strong_count() > 0);
        if state.waiters.len() >= self.inner.budget.max_waiters {
            return Err(NativeTransportRegistrationError::Capacity {
                limit: self.inner.budget.max_waiters,
            });
        }
        let id = state
            .next_waiter_id
            .checked_add(1)
            .ok_or(NativeTransportRegistrationError::IdentityExhausted)?;
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
        target: BackendProcessId,
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
        let target_state = state.by_target.get(&target);
        let target_items = target_state.map_or(0, |entry| entry.items);
        let target_bytes = target_state.map_or(0, |entry| entry.bytes);
        let target_ordinary_items = target_state.map_or(0, |entry| entry.ordinary_items);
        let target_ordinary_bytes = target_state.map_or(0, |entry| entry.ordinary_bytes);
        let target_ordinary_queue_bytes =
            target_state.map_or(0, |entry| entry.ordinary_queue_bytes);
        let target_fits = target_items.saturating_add(items)
            <= self
                .inner
                .budget
                .max_target_items
                .saturating_add(self.inner.budget.reserved_control_items)
            && target_bytes.saturating_add(queued_bytes)
                <= self
                    .inner
                    .budget
                    .max_target_retained_bytes
                    .saturating_add(self.inner.budget.reserved_control_bytes)
            && (lane == NativeTransportLane::Control
                || (target_ordinary_items.saturating_add(items)
                    <= self.inner.budget.max_target_items
                    && target_ordinary_bytes.saturating_add(queued_bytes)
                        <= self.inner.budget.max_target_retained_bytes
                    && target_ordinary_queue_bytes.saturating_add(queued_bytes)
                        <= self
                            .inner
                            .budget
                            .max_target_retained_bytes
                            .saturating_sub(self.inner.budget.max_batch_encoded_bytes)));
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
        // A process-wide shortfall wins over a target one: when both bounds
        // refuse, no other target could be admitted either.
        let refused = if !total_fits || !ordinary_fits || !queue_fits {
            Some(NativeTransportBackpressure::Process)
        } else if !target_fits {
            Some(NativeTransportBackpressure::Target)
        } else {
            None
        };
        if let Some(refused) = refused {
            if state.waiters.contains_key(&waiter.id) {
                state.waiting.insert(waiter.id);
            }
            return Err(refused);
        }
        // A success deliberately leaves this waiter's subscription in place.
        // The same attempt may still hold a refused candidate for another
        // target, and only a capacity release -- which takes the subscription
        // -- may wake it; clearing it here would let that release pass with
        // nobody to wake.
        state.retained_items += items;
        state.retained_queued_and_encoded_bytes += queued_bytes;
        if lane == NativeTransportLane::Ordinary {
            state.ordinary_items += items;
            state.ordinary_queued_and_encoded_bytes += queued_bytes;
            state.ordinary_queue_bytes += queued_bytes;
        } else {
            state.control_queue_bytes += queued_bytes;
        }
        let target_state = state.by_target.entry(target).or_default();
        target_state.items += items;
        target_state.bytes += queued_bytes;
        if lane == NativeTransportLane::Ordinary {
            target_state.ordinary_items += items;
            target_state.ordinary_bytes += queued_bytes;
            target_state.ordinary_queue_bytes += queued_bytes;
        }
        Ok(NativeTransportQueuePermit {
            inner: Arc::clone(&self.inner),
            target,
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
    /// Reserve the original run's exact protobuf bytes before its first DTO
    /// clone/encoding. Invalid input is permanent; physical saturation alone
    /// registers the same waiter and returns typed backpressure.
    pub(crate) fn try_reserve_original_encoding(
        &self,
        waiter: &NativeTransportWaiter,
        target: BackendProcessId,
        lane: NativeTransportLane,
        encoded_bytes: usize,
    ) -> Result<NativeTransportEncodingPermit, NativeEncodingReservationFailure> {
        if encoded_bytes == 0 || encoded_bytes > self.inner.budget.max_batch_encoded_bytes {
            return Err(NativeEncodingReservationFailure::InvalidBytes);
        }
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
        let target_state = state
            .by_target
            .get(&target)
            .expect("encoding must follow the same target's queue permit");
        let target_bytes = target_state.bytes;
        let target_ordinary_bytes = target_state.ordinary_bytes;
        let target_fits = target_bytes.saturating_add(encoded_bytes)
            <= self
                .inner
                .budget
                .max_target_retained_bytes
                .saturating_add(self.inner.budget.reserved_control_bytes)
            && (lane == NativeTransportLane::Control
                || target_ordinary_bytes.saturating_add(encoded_bytes)
                    <= self.inner.budget.max_target_retained_bytes);
        let refused = if !total_fits || !ordinary_fits {
            Some(NativeTransportBackpressure::Process)
        } else if !target_fits {
            Some(NativeTransportBackpressure::Target)
        } else {
            None
        };
        if let Some(refused) = refused {
            if state.waiters.contains_key(&waiter.id) {
                state.waiting.insert(waiter.id);
            }
            return Err(NativeEncodingReservationFailure::Backpressured(refused));
        }
        // As for queue admission, a success never clears a subscription an
        // earlier refusal registered.
        state.retained_queued_and_encoded_bytes += encoded_bytes;
        if lane == NativeTransportLane::Ordinary {
            state.ordinary_queued_and_encoded_bytes += encoded_bytes;
        }
        let target_state = state.by_target.entry(target).or_default();
        target_state.bytes += encoded_bytes;
        if lane == NativeTransportLane::Ordinary {
            target_state.ordinary_bytes += encoded_bytes;
        }
        Ok(NativeTransportEncodingPermit {
            inner: Arc::clone(&self.inner),
            target,
            lane,
            retained_bytes: encoded_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_reserve_encoding(
        &self,
        waiter: &NativeTransportWaiter,
        target: BackendProcessId,
        lane: NativeTransportLane,
    ) -> Result<NativeTransportEncodingPermit, NativeTransportBackpressure> {
        match self.try_reserve_original_encoding(
            waiter,
            target,
            lane,
            self.inner.budget.max_batch_encoded_bytes,
        ) {
            Ok(permit) => Ok(permit),
            Err(NativeEncodingReservationFailure::Backpressured(reason)) => Err(reason),
            Err(NativeEncodingReservationFailure::InvalidBytes) => {
                unreachable!("original configured maximum is valid")
            }
        }
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

/// The original encoder never turns a malformed invoice into a capacity wait.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativeEncodingReservationFailure {
    InvalidBytes,
    Backpressured(NativeTransportBackpressure),
}

/// Why process transport refused a reservation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativeTransportBackpressure {
    /// This target's own window is full. Another target may still be
    /// admitted.
    Target,
    /// A process-wide window -- the total, or the lane's global window -- is
    /// full. No target can be admitted until capacity is released.
    Process,
}

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
    target: BackendProcessId,
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
            if self.lane == NativeTransportLane::Ordinary {
                let target_state = state
                    .by_target
                    .get_mut(&self.target)
                    .expect("native target queue permit exists");
                target_state.ordinary_queue_bytes = target_state
                    .ordinary_queue_bytes
                    .checked_sub(self.queued_bytes)
                    .expect("native target queue accounting is exact");
            }
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
            let target_state = state
                .by_target
                .get_mut(&self.target)
                .expect("native target queue permit exists");
            target_state.items = target_state
                .items
                .checked_sub(self.items)
                .expect("native target item accounting is exact");
            target_state.bytes = target_state
                .bytes
                .checked_sub(self.queued_bytes)
                .expect("native target queue byte accounting is exact");
            if self.lane == NativeTransportLane::Ordinary {
                target_state.ordinary_items = target_state
                    .ordinary_items
                    .checked_sub(self.items)
                    .expect("native target ordinary item accounting is exact");
                target_state.ordinary_bytes = target_state
                    .ordinary_bytes
                    .checked_sub(self.queued_bytes)
                    .expect("native target ordinary queue byte accounting is exact");
                if !self.in_flight {
                    target_state.ordinary_queue_bytes = target_state
                        .ordinary_queue_bytes
                        .checked_sub(self.queued_bytes)
                        .expect("native target queue accounting is exact");
                }
            }
            if target_state.empty() {
                state.by_target.remove(&self.target);
            }
            take_waiter_wakes(&mut state)
        };
        wake_all(wakes);
    }
}

#[derive(Debug)]
pub(crate) struct NativeTransportEncodingPermit {
    inner: Arc<SupervisorInner>,
    target: BackendProcessId,
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
            let target_state = state
                .by_target
                .get_mut(&self.target)
                .expect("native target encoding permit exists");
            target_state.bytes = target_state
                .bytes
                .checked_sub(released)
                .expect("native target encoding reservation is exact");
            if self.lane == NativeTransportLane::Ordinary {
                target_state.ordinary_bytes = target_state
                    .ordinary_bytes
                    .checked_sub(released)
                    .expect("native target ordinary encoding reservation is exact");
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
            let target_state = state
                .by_target
                .get_mut(&self.target)
                .expect("native target encoding permit exists");
            target_state.bytes = target_state
                .bytes
                .checked_sub(self.retained_bytes)
                .expect("native target encoding reservation is exact");
            if self.lane == NativeTransportLane::Ordinary {
                target_state.ordinary_bytes = target_state
                    .ordinary_bytes
                    .checked_sub(self.retained_bytes)
                    .expect("native target ordinary encoding reservation is exact");
            }
            if target_state.empty() {
                state.by_target.remove(&self.target);
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
        target: BackendProcessId,
        lane: NativeTransportLane,
        items: usize,
        queued_bytes: usize,
        encoded_bytes: usize,
    ) -> (NativeTransportQueuePermit, NativeTransportEncodingPermit) {
        let mut queued = supervisor
            .try_reserve_queue(waiter, target, lane, items, queued_bytes)
            .expect("queue capacity");
        let mut encoded = supervisor
            .try_reserve_encoding(waiter, target, lane)
            .expect("encoding capacity reserved before allocation");
        encoded.shrink_to(encoded_bytes);
        queued.mark_in_flight();
        (queued, encoded)
    }

    #[test]
    fn actual_small_encoding_invoices_are_reserved_before_allocation_and_live_until_actual_drop() {
        use novarocks_proto_models::novarocks as wire;
        use prost::Message;
        let supervisor = NativeTransportSupervisor::new(
            NativeTransportSupervisorBudget::new(16, 1000, 1, 200, 4).unwrap(),
        );
        let target = BackendProcessId::new_v7();
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor.register_waiter(wake.clone()).unwrap();
        let actual = wire::ApplyTaskOperationsRequest {
            operations: vec![wire::TaskOperation {
                envelope: Some(wire::TaskOperationEnvelope {
                    operation_id: Some(wire::TaskOperationId {
                        value: vec![0x71; 16],
                    }),
                    max_wait_millis: 1000,
                }),
                operation: Some(wire::task_operation::Operation::CreateTask(
                    wire::CreateTaskRequest {
                        frozen_fragment: bytes::Bytes::from_static(b"f"),
                        creation_metadata: bytes::Bytes::from_static(b"m"),
                    },
                )),
            }],
        };
        let plan = novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(
            actual.encoded_len(),
        )
        .unwrap();
        let bytes = plan.protobuf_bytes();
        assert_eq!(plan.retained_bytes(), bytes + 5);
        assert!(bytes < supervisor.inner.budget.max_batch_encoded_bytes);
        let unchanged = supervisor.snapshot();
        for invalid in [0, supervisor.inner.budget.max_batch_encoded_bytes + 1] {
            assert_eq!(
                supervisor
                    .try_reserve_original_encoding(
                        &waiter,
                        target,
                        NativeTransportLane::Ordinary,
                        invalid
                    )
                    .unwrap_err(),
                NativeEncodingReservationFailure::InvalidBytes
            );
            assert_eq!(supervisor.snapshot(), unchanged);
        }
        assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        let mut queues = Vec::new();
        let mut encodings = Vec::new();
        for _ in 0..8 {
            let mut queued = supervisor
                .try_reserve_queue(&waiter, target, NativeTransportLane::Ordinary, 1, bytes)
                .unwrap();
            let encoding = supervisor
                .try_reserve_original_encoding(
                    &waiter,
                    target,
                    NativeTransportLane::Ordinary,
                    bytes,
                )
                .unwrap();
            assert_eq!(encoding.retained_bytes, bytes);
            queued.mark_in_flight();
            queues.push(queued);
            encodings.push(encoding);
        }
        assert_eq!(supervisor.snapshot().retained_items, 8);
        assert_eq!(
            supervisor.snapshot().retained_queued_and_encoded_bytes,
            16 * bytes
        );
        // The old maximum reservation would not admit these same held inputs.
        assert!(
            8 * (bytes + supervisor.inner.budget.max_batch_encoded_bytes)
                > supervisor.inner.budget.max_encoded_bytes
                    - supervisor.inner.budget.reserved_control_bytes
        );
        drop(queues);
        assert_eq!(supervisor.snapshot().retained_items, 0);
        assert_eq!(
            supervisor.snapshot().retained_queued_and_encoded_bytes,
            8 * bytes,
            "queue exit does not fabricate encoding exit"
        );
        drop(encodings);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    #[test]
    fn ordinary_work_cannot_consume_the_control_reserve() {
        let supervisor = supervisor();
        let target = BackendProcessId::new_v7();
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor
            .register_waiter(wake.clone())
            .expect("register waiter");
        let ordinary = reserve_for_send(
            &supervisor,
            &waiter,
            target,
            NativeTransportLane::Ordinary,
            3,
            70,
            10,
        );
        assert!(
            supervisor
                .try_reserve_queue(&waiter, target, NativeTransportLane::Ordinary, 1, 1)
                .is_err(),
            "ordinary work must leave both control reserves intact"
        );
        let control = reserve_for_send(
            &supervisor,
            &waiter,
            target,
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
        let target = BackendProcessId::new_v7();
        let wake = Arc::new(CountingWake::default());
        let waiter = supervisor
            .register_waiter(wake.clone())
            .expect("register waiter");
        let permit = supervisor
            .try_reserve_queue(&waiter, target, NativeTransportLane::Ordinary, 3, 70)
            .expect("ordinary capacity");
        assert!(
            supervisor
                .try_reserve_queue(&waiter, target, NativeTransportLane::Ordinary, 1, 1)
                .is_err()
        );
        assert!(
            supervisor
                .try_reserve_queue(&waiter, target, NativeTransportLane::Ordinary, 1, 1)
                .is_err()
        );
        assert_eq!(supervisor.snapshot().waiting_attempts, 1);
        drop(permit);
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    /// A refusal names which window was full, and a process-wide shortfall
    /// wins when both refuse: no other target could have been admitted then.
    #[test]
    fn a_refusal_reports_whether_the_target_or_the_process_was_full() {
        let mut budget = NativeTransportSupervisorBudget::new(4, 100, 1, 20, 4).expect("budget");
        budget.max_target_items = 1;
        let supervisor = NativeTransportSupervisor::new(budget);
        let slow = BackendProcessId::new_v7();
        let healthy = BackendProcessId::new_v7();
        let waiter = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("waiter");
        let held = supervisor
            .try_reserve_queue(&waiter, slow, NativeTransportLane::Ordinary, 1, 1)
            .expect("the first slow operation fits");
        assert_eq!(
            supervisor
                .try_reserve_queue(&waiter, slow, NativeTransportLane::Ordinary, 1, 1)
                .expect_err("the slow target's window is full"),
            NativeTransportBackpressure::Target
        );
        let ordinary = supervisor
            // Exactly fills the ordinary queue window: 1 + 69 bytes of the 70
            // an ordinary lane may queue once one encoded batch is kept free.
            .try_reserve_queue(&waiter, healthy, NativeTransportLane::Ordinary, 1, 69)
            .expect("the healthy target is admitted beside it");
        assert_eq!(
            supervisor
                .try_reserve_queue(&waiter, slow, NativeTransportLane::Ordinary, 1, 1)
                .expect_err("both the target and the process window are full"),
            NativeTransportBackpressure::Process,
            "a process-wide shortfall is reported even when the target is also full"
        );
        drop(ordinary);
        drop(held);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    /// An attempt refused for one target and then admitted for another still
    /// holds that first refusal. A later success must not clear the
    /// subscription, or the release that frees the first target would find
    /// nobody to wake.
    #[test]
    fn a_later_success_keeps_an_earlier_refusal_subscribed() {
        let mut budget = NativeTransportSupervisorBudget::new(4, 100, 1, 20, 4).expect("budget");
        budget.max_target_items = 1;
        let supervisor = NativeTransportSupervisor::new(budget);
        let slow = BackendProcessId::new_v7();
        let healthy = BackendProcessId::new_v7();
        let other_attempt = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("another attempt");
        let wake = Arc::new(CountingWake::default());
        let attempt = supervisor
            .register_waiter(wake.clone())
            .expect("this attempt");
        let held = supervisor
            .try_reserve_queue(&other_attempt, slow, NativeTransportLane::Ordinary, 1, 1)
            .expect("another attempt fills the slow target");
        assert_eq!(
            supervisor
                .try_reserve_queue(&attempt, slow, NativeTransportLane::Ordinary, 1, 1)
                .expect_err("the slow target is full"),
            NativeTransportBackpressure::Target
        );
        let admitted = supervisor
            .try_reserve_queue(&attempt, healthy, NativeTransportLane::Ordinary, 1, 1)
            .expect("the healthy target admits this attempt");
        assert_eq!(
            supervisor.snapshot().waiting_attempts,
            1,
            "admitting another target keeps the refusal subscribed"
        );
        let mut encoding = supervisor
            .try_reserve_encoding(&attempt, healthy, NativeTransportLane::Ordinary)
            .expect("its send is encoded");
        assert_eq!(
            supervisor.snapshot().waiting_attempts,
            1,
            "encoding another target's send keeps the refusal subscribed"
        );
        assert_eq!(wake.0.load(Ordering::SeqCst), 0, "nothing was released yet");
        drop(held);
        assert_eq!(
            wake.0.load(Ordering::SeqCst),
            1,
            "freeing the slow target wakes the attempt it refused"
        );
        assert_eq!(supervisor.snapshot().waiting_attempts, 0);
        encoding.shrink_to(1);
        drop(encoding);
        drop(admitted);
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
        let error = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .unwrap_err();
        assert_eq!(
            error,
            NativeTransportRegistrationError::Capacity { limit: 1 }
        );
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, ResourceDimension, ResourceScope,
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.constraint_scope, Some(ResourceScope::Process));
        assert_eq!(resource.dimension, ResourceDimension::Positions);
        assert_eq!(resource.requested, Some(1));
        assert_eq!(resource.limit, Some(1));
        assert_eq!(resource.live, None);
        drop(first);
        assert!(
            supervisor
                .register_waiter(Arc::new(CountingWake::default()))
                .is_ok()
        );
    }

    #[test]
    fn exhausted_waiter_identity_does_not_install_a_registration_or_claim_closed() {
        let supervisor = supervisor();
        supervisor.inner.state.lock().unwrap().next_waiter_id = u64::MAX;
        let error = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .unwrap_err();
        assert_eq!(error, NativeTransportRegistrationError::IdentityExhausted);
        use novarocks_execution_contract::failure::{
            FailureClass, FailureEvidence, FailureProjection,
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert_eq!(
            error.failure_fact().evidence(),
            FailureEvidence::Originating
        );
        assert_eq!(error.failure_fact().reason().code(), 105);
        assert!(supervisor.inner.state.lock().unwrap().waiters.is_empty());
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }

    #[test]
    fn process_budget_must_leave_one_control_and_one_ordinary_window() {
        assert!(NativeTransportSupervisorBudget::new(2, 39, 1, 20, 2).is_none());
        assert!(NativeTransportSupervisorBudget::new(1, 40, 1, 20, 2).is_none());
    }

    #[test]
    fn one_maximum_ordinary_batch_is_always_admissible() {
        let target = BackendProcessId::new_v7();
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
            target,
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

    #[test]
    fn one_slow_target_across_attempts_cannot_exhaust_a_healthy_targets_window() {
        let transport = TransportBudget::new(2, 10, 5, 4, 40, 8, 100, 1, 2, Duration::from_secs(1))
            .expect("one target and the process have independent windows");
        let supervisor = NativeTransportSupervisor::from_transport(transport).expect("budget");
        let slow = BackendProcessId::new_v7();
        let healthy = BackendProcessId::new_v7();
        let slow_first = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("first attempt");
        let slow_second = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("second attempt");
        let healthy_attempt = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("healthy attempt");

        let first = reserve_for_send(
            &supervisor,
            &slow_first,
            slow,
            NativeTransportLane::Ordinary,
            1,
            20,
            10,
        );
        let second = supervisor
            .try_reserve_queue(&slow_second, slow, NativeTransportLane::Ordinary, 1, 10)
            .expect("another attempt can use the remaining target window");
        assert!(
            supervisor
                .try_reserve_queue(&slow_second, slow, NativeTransportLane::Ordinary, 1, 1)
                .is_err(),
            "the same slow target cannot retain more work across attempts"
        );
        let normal = reserve_for_send(
            &supervisor,
            &healthy_attempt,
            healthy,
            NativeTransportLane::Ordinary,
            1,
            20,
            10,
        );
        let control = reserve_for_send(
            &supervisor,
            &slow_first,
            slow,
            NativeTransportLane::Control,
            1,
            5,
            5,
        );
        assert_eq!(supervisor.snapshot().retained_queued_and_encoded_bytes, 80);
        drop(control);
        drop(normal);
        drop(second);
        drop(first);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
        assert!(
            supervisor
                .inner
                .state
                .lock()
                .expect("state")
                .by_target
                .is_empty()
        );
    }

    #[test]
    fn target_item_window_spans_attempts_without_consuming_healthy_capacity() {
        let mut budget = NativeTransportSupervisorBudget::new(4, 100, 1, 20, 4).expect("budget");
        budget.max_target_items = 2;
        let supervisor = NativeTransportSupervisor::new(budget);
        let slow = BackendProcessId::new_v7();
        let healthy = BackendProcessId::new_v7();
        let first_attempt = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("first attempt");
        let second_attempt = supervisor
            .register_waiter(Arc::new(CountingWake::default()))
            .expect("second attempt");
        let first = supervisor
            .try_reserve_queue(&first_attempt, slow, NativeTransportLane::Ordinary, 1, 1)
            .expect("first slow operation");
        let second = supervisor
            .try_reserve_queue(&second_attempt, slow, NativeTransportLane::Ordinary, 1, 1)
            .expect("second slow operation");
        assert!(
            supervisor
                .try_reserve_queue(&second_attempt, slow, NativeTransportLane::Ordinary, 1, 1)
                .is_err(),
            "a third slow operation must respect the shared target count"
        );
        let normal = supervisor
            .try_reserve_queue(
                &second_attempt,
                healthy,
                NativeTransportLane::Ordinary,
                1,
                1,
            )
            .expect("the other target retains a normal send slot");
        drop(normal);
        drop(second);
        drop(first);
        assert_eq!(supervisor.snapshot(), NativeTransportSnapshot::default());
    }
}
