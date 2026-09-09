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

//! Bounded, fair per-backend dispatch.
//!
//! Every backend keeps four queues, one per [`DispatchLane`]. A weighted round
//! robin releases ordinary work; cancellation has its own control lane and is
//! selected first. A lane's weight is its
//! permit count from [`DispatchBudget`], and a lane may never have more
//! operations in flight than it has permits, so a large create burst can
//! neither exhaust the lifecycle permits nor take more than its share of a
//! rotation. That is the whole content of "every task is created
//! concurrently": no plan-depth serial chain, and no unbounded fan-out either.
//!
//! An abort may be pushed to the front of its lane. It never revokes an
//! operation that has already been released: an immutable request that is
//! already on its way keeps its position and converges on its own receipt.

use std::collections::{BTreeMap, VecDeque};

use novarocks_execution::task_execution::TaskOperationId;
use novarocks_query_application::coordination::{DispatchBudget, DispatchLane, MonotonicInstant};
use novarocks_task_codec::TransportBudget;
use novarocks_types::identity::BackendProcessId;

use super::error::{CapacityBound, TaskExecutionError};
use super::intent::{
    DispatchAcceptance, DispatchBatch, OperationIntent, TaskOperationQueueRequest,
};

/// Every lane, in rotation order.
const LANES: [DispatchLane; 4] = [
    DispatchLane::Create,
    DispatchLane::Update,
    DispatchLane::Lifecycle,
    DispatchLane::Control,
];

const fn lane_index(lane: DispatchLane) -> usize {
    match lane {
        DispatchLane::Create => 0,
        DispatchLane::Update => 1,
        DispatchLane::Lifecycle => 2,
        DispatchLane::Control => 3,
    }
}

/// One intent waiting in a bounded queue.
#[derive(Debug)]
struct QueuedOperation {
    intent: OperationIntent,
    queue_permit: Box<dyn super::intent::TaskOperationQueuePermit>,
    queued_at: MonotonicInstant,
    queued_bytes: usize,
}

/// One operation that waited past the frontend queue-residence bound.
///
/// It is reported rather than sent late: the backend would time it out on its
/// own deadline anyway, and failing closed locally keeps the frontend's own
/// error budget honest.
#[derive(Debug)]
pub struct ExpiredOperation {
    intent: OperationIntent,
    waited: std::time::Duration,
    /// The process reservation remains live until the serial owner consumes
    /// this local failure receipt and rolls back the unsent owner marker.
    _queue_permit: Box<dyn super::intent::TaskOperationQueuePermit>,
}

impl ExpiredOperation {
    pub fn operation_id(&self) -> TaskOperationId {
        self.intent.operation_id()
    }

    pub fn kind(&self) -> novarocks_execution::task_execution::OperationKind {
        self.intent.kind()
    }

    pub const fn waited(&self) -> std::time::Duration {
        self.waited
    }
}

#[derive(Debug)]
struct BackendQueues {
    lanes: [VecDeque<QueuedOperation>; 4],
    in_flight: [usize; 4],
    credits: [usize; 4],
    cursor: usize,
    queued_items: usize,
    queued_bytes: usize,
    active_tasks: usize,
}

impl BackendQueues {
    fn new(budget: DispatchBudget) -> Self {
        Self {
            lanes: [
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
                VecDeque::new(),
            ],
            in_flight: [0; 4],
            credits: [
                budget.permits_for(DispatchLane::Create),
                budget.permits_for(DispatchLane::Update),
                budget.permits_for(DispatchLane::Lifecycle),
                budget.permits_for(DispatchLane::Control),
            ],
            cursor: 0,
            queued_items: 0,
            queued_bytes: 0,
            active_tasks: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.queued_items == 0
    }
}

/// Where one released operation went, so its acknowledgement can free its
/// permit again.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct InFlightSlot {
    backend: BackendProcessId,
    lane: DispatchLane,
}

/// The per-attempt dispatcher.
///
/// It is scoped to one query execution, so its per-backend queues are exactly
/// the protocol's per-query-per-backend queues. Its local bounds protect one
/// attempt. Every production enqueue also carries a reservation from the
/// process transport supervisor, which is the authority across attempts.
#[derive(Debug)]
pub struct OperationDispatcher {
    budget: DispatchBudget,
    transport: TransportBudget,
    backends: BTreeMap<BackendProcessId, BackendQueues>,
    rotation: Vec<BackendProcessId>,
    rotation_cursor: usize,
    queued_items: usize,
    queued_bytes: usize,
    in_flight: BTreeMap<TaskOperationId, InFlightSlot>,
}

impl OperationDispatcher {
    pub fn new(budget: DispatchBudget, transport: TransportBudget) -> Self {
        Self {
            budget,
            transport,
            backends: BTreeMap::new(),
            rotation: Vec::new(),
            rotation_cursor: 0,
            queued_items: 0,
            queued_bytes: 0,
            in_flight: BTreeMap::new(),
        }
    }

    pub const fn budget(&self) -> DispatchBudget {
        self.budget
    }

    pub const fn transport_budget(&self) -> TransportBudget {
        self.transport
    }

    /// Counts one task this attempt places on a backend.
    pub fn register_task(&mut self, backend: BackendProcessId) -> Result<(), TaskExecutionError> {
        let limit = self.transport.max_active_tasks_per_backend();
        let queues = self.backend_entry(backend);
        if queues.active_tasks + 1 > limit {
            return Err(CapacityBound::ActiveTasksPerBackend { backend, limit }.into());
        }
        queues.active_tasks += 1;
        Ok(())
    }

    /// Queues one intent behind everything already waiting on its lane.
    #[cfg(test)]
    pub fn enqueue(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, false, Box::new(UntrackedTestQueuePermit))
    }

    pub(super) fn enqueue_reserved(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        queue_permit: Box<dyn super::intent::TaskOperationQueuePermit>,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, false, queue_permit)
    }

    /// Queues one intent ahead of everything waiting on its lane.
    ///
    /// Only a forced stand-down uses this. It reorders the queue; it never
    /// preempts an operation that was already released.
    pub(super) fn enqueue_priority_reserved(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        queue_permit: Box<dyn super::intent::TaskOperationQueuePermit>,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, true, queue_permit)
    }

    fn admit(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        front: bool,
        queue_permit: Box<dyn super::intent::TaskOperationQueuePermit>,
    ) -> Result<(), TaskExecutionError> {
        self.validate_operation_carrier(&intent)?;
        let queued_bytes = intent.queued_bytes();
        let backend = intent.backend_process_id();
        let lane = lane_index(intent.lane());

        let per_backend_items = self.transport.max_query_backend_queued_operations();
        let per_backend_bytes = self.transport.max_query_backend_queued_bytes();
        let total_items = self.transport.max_backend_queued_operations();
        let total_bytes = self.transport.max_backend_queued_bytes();
        if self.queued_items + 1 > total_items {
            return Err(CapacityBound::BackendOperations { limit: total_items }.into());
        }
        if self.queued_bytes + queued_bytes > total_bytes {
            return Err(CapacityBound::BackendBytes { limit: total_bytes }.into());
        }

        let queues = self.backend_entry(backend);
        if queues.queued_items + 1 > per_backend_items {
            return Err(CapacityBound::QueryBackendOperations {
                limit: per_backend_items,
            }
            .into());
        }
        if queues.queued_bytes + queued_bytes > per_backend_bytes {
            return Err(CapacityBound::QueryBackendBytes {
                limit: per_backend_bytes,
            }
            .into());
        }
        let entry = QueuedOperation {
            intent,
            queue_permit,
            queued_at: now,
            queued_bytes,
        };
        if front {
            queues.lanes[lane].push_front(entry);
        } else {
            queues.lanes[lane].push_back(entry);
        }
        queues.queued_items += 1;
        queues.queued_bytes += queued_bytes;
        self.queued_items += 1;
        self.queued_bytes += queued_bytes;
        Ok(())
    }

    /// Validates the one-operation carrier before process admission.
    ///
    /// Production callers use this before asking the shared supervisor for a
    /// reservation; `admit` repeats the same authority check so test and
    /// alternate in-crate callers cannot bypass it.
    pub(super) fn validate_operation_carrier(
        &self,
        intent: &OperationIntent,
    ) -> Result<(), TaskExecutionError> {
        self.validate_queue_request(intent.queue_request())?;
        if let OperationIntent::CreateTask(request) = &intent {
            let limit = self.transport.max_descriptor_encoded_bytes();
            let actual = request.descriptor().plan().encoded_len();
            if actual > limit {
                return Err(CapacityBound::DescriptorBytes { limit, actual }.into());
            }
        }
        Ok(())
    }

    /// Validates a bounded reservation request before an owner retains the
    /// corresponding operation payload.
    pub(super) fn validate_queue_request(
        &self,
        request: TaskOperationQueueRequest,
    ) -> Result<(), TaskExecutionError> {
        let queued_bytes = request.queued_bytes();
        let operation_limit = self.transport.max_operation_queued_bytes();
        if queued_bytes > operation_limit {
            return Err(CapacityBound::OperationBytes {
                limit: operation_limit,
                actual: queued_bytes,
            }
            .into());
        }
        Ok(())
    }

    fn backend_entry(&mut self, backend: BackendProcessId) -> &mut BackendQueues {
        if !self.backends.contains_key(&backend) {
            self.backends
                .insert(backend, BackendQueues::new(self.budget));
            self.rotation.push(backend);
        }
        self.backends
            .get_mut(&backend)
            .expect("the backend entry was just inserted")
    }

    /// Removes and reports every operation that waited past the queue
    /// residence bound.
    pub fn drain_expired(&mut self, now: MonotonicInstant) -> Vec<ExpiredOperation> {
        let residence = self.transport.frontend_queue_residence();
        let mut expired = Vec::new();
        let mut dropped_items = 0_usize;
        let mut dropped_bytes = 0_usize;
        for queues in self.backends.values_mut() {
            for lane in &mut queues.lanes {
                let mut kept = VecDeque::with_capacity(lane.len());
                while let Some(entry) = lane.pop_front() {
                    let waited = now.saturating_duration_since(entry.queued_at);
                    if waited >= residence {
                        queues.queued_items -= 1;
                        queues.queued_bytes -= entry.queued_bytes;
                        dropped_items += 1;
                        dropped_bytes += entry.queued_bytes;
                        expired.push(ExpiredOperation {
                            intent: entry.intent,
                            waited,
                            _queue_permit: entry.queue_permit,
                        });
                    } else {
                        kept.push_back(entry);
                    }
                }
                *lane = kept;
            }
        }
        self.queued_items -= dropped_items;
        self.queued_bytes -= dropped_bytes;
        expired
    }

    /// Releases one bounded batch, rotating fairly across backends and lanes.
    pub fn take_batch(&mut self) -> Option<DispatchBatch> {
        if self.rotation.is_empty() {
            return None;
        }
        // Scan control first across the whole attempt. Backend rotation still
        // decides among cancellations, but ordinary work on another backend
        // cannot hide a cancellation from the process control reserve.
        for offset in 0..self.rotation.len() {
            let index = (self.rotation_cursor + offset) % self.rotation.len();
            let backend = self.rotation[index];
            if let Some(batch) = self.take_backend_control_batch(backend) {
                self.rotation_cursor = (index + 1) % self.rotation.len();
                return Some(batch);
            }
        }
        for offset in 0..self.rotation.len() {
            let index = (self.rotation_cursor + offset) % self.rotation.len();
            let backend = self.rotation[index];
            if let Some(batch) = self.take_backend_batch(backend) {
                self.rotation_cursor = (index + 1) % self.rotation.len();
                return Some(batch);
            }
        }
        None
    }

    fn take_backend_batch(&mut self, backend: BackendProcessId) -> Option<DispatchBatch> {
        let lane = {
            let queues = self.backends.get_mut(&backend)?;
            if queues.is_empty() {
                return None;
            }
            Self::pick_lane(queues, self.budget)?
        };
        self.take_backend_lane_batch(backend, lane)
    }

    fn take_backend_control_batch(&mut self, backend: BackendProcessId) -> Option<DispatchBatch> {
        self.take_backend_lane_batch(backend, lane_index(DispatchLane::Control))
    }

    /// Takes from one exact lane without letting unrelated ordinary work hide
    /// an urgent lifecycle operation queued for the same backend.
    pub(super) fn take_priority_lane_batch(
        &mut self,
        backend: BackendProcessId,
        lane: DispatchLane,
    ) -> Option<DispatchBatch> {
        let lane_index = lane_index(lane);
        let queues = self.backends.get_mut(&backend)?;
        if queues.credits[lane_index] == 0 {
            queues.credits[lane_index] = self.budget.permits_for(lane);
        }
        self.take_backend_lane_batch(backend, lane_index)
    }

    fn take_backend_lane_batch(
        &mut self,
        backend: BackendProcessId,
        lane: usize,
    ) -> Option<DispatchBatch> {
        let max_items = self.transport.max_batch_items();
        let max_bytes = self.transport.max_batch_encoded_bytes();
        let (operations, queue_permits, queued_bytes, queued_at) = {
            let queues = self.backends.get_mut(&backend)?;
            if queues.lanes[lane].is_empty() {
                return None;
            }
            let permits = self.budget.permits_for(LANES[lane]);
            let headroom = permits.saturating_sub(queues.in_flight[lane]);
            let allowance = headroom.min(queues.credits[lane]).min(max_items);
            if allowance == 0 {
                return None;
            }
            let mut operations = Vec::new();
            let mut queue_permits = Vec::new();
            let mut queued_bytes = 0_usize;
            let mut queued_at = None;
            let requires_control_progress = queues.lanes[lane]
                .front()
                .expect("the lane was observed nonempty")
                .intent
                .requires_control_progress();
            while operations.len() < allowance {
                let Some(entry) = queues.lanes[lane].front() else {
                    break;
                };
                // A Native request is admitted as one process-level class.
                // Do not let ordinary updates hitch a ride on the capacity
                // reserved for a cancellation that shares their historical
                // attempt-local lane.
                if !operations.is_empty()
                    && entry.intent.requires_control_progress() != requires_control_progress
                {
                    break;
                }
                // Enqueueing already proved one operation fits. The aggregate
                // check keeps the queue-side carrier of the whole batch under
                // the same hard bound as its encoded request.
                if queued_bytes.saturating_add(entry.queued_bytes) > max_bytes {
                    break;
                }
                let entry = queues.lanes[lane]
                    .pop_front()
                    .expect("the front entry was just observed");
                queued_bytes += entry.queued_bytes;
                queued_at = Some(queued_at.map_or(entry.queued_at, |oldest| {
                    std::cmp::min(oldest, entry.queued_at)
                }));
                queues.queued_items -= 1;
                queues.queued_bytes -= entry.queued_bytes;
                operations.push(entry.intent);
                queue_permits.push(entry.queue_permit);
            }
            if operations.is_empty() {
                return None;
            }
            queues.cursor = (lane + 1) % LANES.len();
            (
                operations,
                queue_permits,
                queued_bytes,
                queued_at.expect("a nonempty batch has a queue timestamp"),
            )
        };
        self.queued_items -= operations.len();
        self.queued_bytes -= queued_bytes;
        Some(DispatchBatch::with_queued_at(
            backend,
            LANES[lane],
            operations,
            queue_permits,
            queued_bytes,
            queued_at,
        ))
    }

    /// Commits transport acceptance and only then consumes dispatch permits.
    pub(crate) fn accept(
        &mut self,
        accepted: DispatchAcceptance,
    ) -> Result<(), TaskExecutionError> {
        let queues = self
            .backends
            .get_mut(&accepted.backend)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let lane = lane_index(accepted.lane);
        let count = accepted.operation_ids.len();
        if count == 0
            || count > queues.credits[lane]
            || queues.in_flight[lane].saturating_add(count) > self.budget.permits_for(accepted.lane)
            || accepted
                .operation_ids
                .iter()
                .any(|operation_id| self.in_flight.contains_key(operation_id))
        {
            return Err(TaskExecutionError::Schedule(
                "transport accepted a batch outside its dispatcher reservation".to_owned(),
            ));
        }
        queues.credits[lane] -= count;
        queues.in_flight[lane] += count;
        for operation_id in accepted.operation_ids {
            self.in_flight.insert(
                operation_id,
                InFlightSlot {
                    backend: accepted.backend,
                    lane: accepted.lane,
                },
            );
        }
        Ok(())
    }

    /// Restores an unaccepted batch at the head of its original lane.
    ///
    /// No dispatch permit was consumed, so this is a pure ownership return.
    /// The oldest residence timestamp is retained for every restored item to
    /// prevent repeated process-level backpressure from extending deadlines.
    pub(super) fn restore_backpressured(&mut self, batch: DispatchBatch) {
        let backend = batch.backend();
        let lane = lane_index(batch.lane());
        let queued_at = batch.queued_at();
        let batch_queued_bytes = batch.queued_bytes();
        let (operations, queue_permits) = batch.into_queue_parts();
        let item_count = operations.len();
        let recomputed_bytes = operations
            .iter()
            .map(OperationIntent::queued_bytes)
            .sum::<usize>();
        assert_eq!(
            batch_queued_bytes, recomputed_bytes,
            "a refused batch must retain its exact queued-byte ownership"
        );
        {
            let queues = self.backend_entry(backend);
            for (intent, queue_permit) in operations.into_iter().zip(queue_permits).rev() {
                let queued_bytes = intent.queued_bytes();
                let entry = QueuedOperation {
                    intent,
                    queue_permit,
                    queued_at,
                    queued_bytes,
                };
                queues.lanes[lane].push_front(entry);
                queues.queued_items += 1;
                queues.queued_bytes += queued_bytes;
            }
        }
        self.queued_items += item_count;
        self.queued_bytes += batch_queued_bytes;
    }

    /// Picks the next lane in weighted rotation.
    ///
    /// A lane is eligible when it has work, has an unused permit, and has a
    /// credit left in this cycle. When every eligible lane has spent its
    /// credits the whole cycle refills, so each lane gets at most its weight
    /// per cycle and every non-empty lane gets at least one.
    fn pick_lane(queues: &mut BackendQueues, budget: DispatchBudget) -> Option<usize> {
        for refill in 0..2 {
            for offset in 0..LANES.len() {
                let lane = (queues.cursor + offset) % LANES.len();
                if queues.lanes[lane].is_empty() {
                    continue;
                }
                if queues.in_flight[lane] >= budget.permits_for(LANES[lane]) {
                    continue;
                }
                if queues.credits[lane] == 0 {
                    continue;
                }
                return Some(lane);
            }
            if refill == 0 {
                for (index, lane) in LANES.iter().enumerate() {
                    queues.credits[index] = budget.permits_for(*lane);
                }
            }
        }
        None
    }

    /// Frees the permit one released operation held.
    pub fn settle(&mut self, operation_id: TaskOperationId) -> Result<(), TaskExecutionError> {
        let slot = self
            .in_flight
            .remove(&operation_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let queues = self
            .backends
            .get_mut(&slot.backend)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let lane = lane_index(slot.lane);
        queues.in_flight[lane] = queues.in_flight[lane].saturating_sub(1);
        Ok(())
    }

    pub const fn queued_items(&self) -> usize {
        self.queued_items
    }

    pub const fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn in_flight_operations(&self) -> usize {
        self.in_flight.len()
    }

    pub fn lane_in_flight(&self, backend: BackendProcessId, lane: DispatchLane) -> usize {
        self.backends
            .get(&backend)
            .map_or(0, |queues| queues.in_flight[lane_index(lane)])
    }

    pub fn lane_queued(&self, backend: BackendProcessId, lane: DispatchLane) -> usize {
        self.backends
            .get(&backend)
            .map_or(0, |queues| queues.lanes[lane_index(lane)].len())
    }

    pub fn backends(&self) -> impl ExactSizeIterator<Item = BackendProcessId> + '_ {
        self.rotation.iter().copied()
    }
}

#[cfg(test)]
#[derive(Debug)]
struct UntrackedTestQueuePermit;

#[cfg(test)]
impl super::intent::TaskOperationQueuePermit for UntrackedTestQueuePermit {
    fn mark_in_flight(&mut self) {}
}
