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

mod expiry;
pub(crate) use expiry::{ExpiryProgress, QueueExpiryCursor};

use std::collections::{BTreeMap, BTreeSet};

use crate::config::FrozenTransportLimits as TransportBudget;
use crate::config::{DispatchBudget, DispatchLane};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{OperationKind, TaskOperationId};
use novarocks_types::identity::BackendProcessId;

use super::error::{CapacityBound, TaskExecutionError};
use crate::effect::{DispatchBatch, OperationIntent};
use crate::input::QueuedInputGrantRef;
use crate::intent::{DispatchAcceptance, TaskOperationQueueRequest};

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
    queue_permit: QueuedInputGrantRef,
    queued_at: MonotonicInstant,
    queued_bytes: usize,
}

pub(crate) fn queued_operation_layout() -> std::alloc::Layout {
    std::alloc::Layout::new::<QueuedOperation>()
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
    _queue_permit: QueuedInputGrantRef,
}

impl ExpiredOperation {
    pub fn operation_id(&self) -> TaskOperationId {
        self.intent.operation_id()
    }

    pub fn kind(&self) -> novarocks_execution_contract::OperationKind {
        self.intent.kind()
    }

    pub const fn waited(&self) -> std::time::Duration {
        self.waited
    }

    /// The core must explicitly return the original host grant after its local
    /// expiry verdict. Dropping this passive reference cannot release capacity.
    pub fn into_parts(self) -> (OperationIntent, std::time::Duration, QueuedInputGrantRef) {
        (self.intent, self.waited, self._queue_permit)
    }
}

/// An indexed FIFO with original Abort priority. Exact cancellation never
/// scans unrelated retained operations.
#[derive(Debug, Default)]
struct OrderedQueue {
    entries: BTreeMap<i128, QueuedOperation>,
    positions: BTreeMap<TaskOperationId, i128>,
    first: i128,
    last: i128,
}
impl OrderedQueue {
    fn new() -> Self {
        Self::default()
    }
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
    fn front(&self) -> Option<&QueuedOperation> {
        self.entries.first_key_value().map(|(_, value)| value)
    }
    fn get(&self, id: TaskOperationId) -> Option<&QueuedOperation> {
        self.entries.get(self.positions.get(&id)?)
    }
    fn push_front(&mut self, entry: QueuedOperation) {
        self.first = self
            .first
            .checked_sub(1)
            .expect("queue front ordinal exhausted");
        self.insert(self.first, entry);
    }
    fn push_back(&mut self, entry: QueuedOperation) {
        self.last = self
            .last
            .checked_add(1)
            .expect("queue back ordinal exhausted");
        self.insert(self.last, entry);
    }
    fn insert(&mut self, ordinal: i128, entry: QueuedOperation) {
        assert!(
            self.positions
                .insert(entry.intent.operation_id(), ordinal)
                .is_none(),
            "one original queued operation"
        );
        assert!(
            self.entries.insert(ordinal, entry).is_none(),
            "unique queue ordinal"
        );
    }
    fn remove(&mut self, id: TaskOperationId) -> Option<QueuedOperation> {
        self.entries.remove(&self.positions.remove(&id)?)
    }
    fn pop_front(&mut self) -> Option<QueuedOperation> {
        let (_, entry) = self.entries.pop_first()?;
        self.positions
            .remove(&entry.intent.operation_id())
            .expect("original queue position");
        Some(entry)
    }
}

#[derive(Debug)]
struct BackendQueues {
    lanes: [OrderedQueue; 4],
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
                OrderedQueue::new(),
                OrderedQueue::new(),
                OrderedQueue::new(),
                OrderedQueue::new(),
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

/// Exact carrier ownership for one operation inside the attempt dispatcher.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum DispatchOperationState {
    Queued,
    InFlight,
    Absent,
}

/// Whether one request fits an attempt's own queue bounds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LocalQueueCapacity {
    Fits,
    /// This attempt's queue for the request's backend is full.
    TargetFull,
    /// This attempt's total across every backend is full.
    AttemptFull,
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
    released_grants: Vec<QueuedInputGrantRef>,
    queued_routes: BTreeMap<TaskOperationId, (BackendProcessId, usize, MonotonicInstant)>,
    queued_times: BTreeSet<(MonotonicInstant, TaskOperationId)>,
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
            released_grants: Vec::new(),
            queued_routes: BTreeMap::new(),
            queued_times: BTreeSet::new(),
        }
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

    pub(super) fn enqueue_reserved(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        queue_permit: QueuedInputGrantRef,
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
        queue_permit: QueuedInputGrantRef,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, true, queue_permit)
    }

    fn admit(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        front: bool,
        queue_permit: QueuedInputGrantRef,
    ) -> Result<(), TaskExecutionError> {
        self.validate_operation_carrier(&intent)?;
        let operation_id = intent.operation_id();
        if self.operation_state(operation_id)? != DispatchOperationState::Absent {
            return Err(TaskExecutionError::Schedule(
                "one operation identity has multiple dispatcher owners".into(),
            ));
        }
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
        self.queued_routes
            .insert(operation_id, (backend, lane, now));
        self.queued_times.insert((now, operation_id));
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
        // Descriptor bytes are checked at the same original creation projection.
        // The core keeps the signed quote, never an executable descriptor.
        Ok(())
    }

    /// Refuses a create whose static plan and task assignment together exceed
    /// the bound the backend enforces on them.
    ///
    /// This is a property of the request itself, not of any queue: waiting
    /// cannot make it smaller, so it fails closed rather than backpressuring.
    pub(super) fn validate_plan_carrier_bytes(
        &self,
        actual: usize,
    ) -> Result<(), TaskExecutionError> {
        let limit = self.transport.max_descriptor_encoded_bytes();
        if actual > limit {
            return Err(CapacityBound::DescriptorBytes { limit, actual }.into());
        }
        Ok(())
    }

    /// Whether one request fits this attempt's own queue bounds right now.
    ///
    /// The same bounds `admit` enforces, asked before anything is reserved
    /// or taken. A full per-backend queue refuses only that target; a full
    /// attempt total refuses every target of this attempt alike.
    pub(super) fn local_capacity(&self, request: TaskOperationQueueRequest) -> LocalQueueCapacity {
        let queued_bytes = request.queued_bytes();
        if self.queued_items + 1 > self.transport.max_backend_queued_operations()
            || self.queued_bytes + queued_bytes > self.transport.max_backend_queued_bytes()
        {
            return LocalQueueCapacity::AttemptFull;
        }
        let (items, bytes) = self
            .backends
            .get(&request.backend_process_id())
            .map_or((0, 0), |queues| (queues.queued_items, queues.queued_bytes));
        if items + 1 > self.transport.max_query_backend_queued_operations()
            || bytes + queued_bytes > self.transport.max_query_backend_queued_bytes()
        {
            return LocalQueueCapacity::TargetFull;
        }
        LocalQueueCapacity::Fits
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

    /// Whether one exact urgent operation can enter and immediately use its
    /// lane on this serial turn.
    ///
    /// This is checked before an external owner hands its payload to the task
    /// substrate. The caller separately obtains the process transport's queue
    /// permit; this predicate is only the attempt-local dispatcher authority.
    /// Because one TaskRound is the dispatcher's sole mutator, the capacity
    /// cannot change between this check and the matching enqueue.
    pub(super) fn priority_capacity_available(
        &self,
        intent: &OperationIntent,
    ) -> Result<bool, TaskExecutionError> {
        self.validate_operation_carrier(intent)?;
        let queued_bytes = intent.queued_bytes();
        if self.queued_items + 1 > self.transport.max_backend_queued_operations()
            || self.queued_bytes + queued_bytes > self.transport.max_backend_queued_bytes()
        {
            return Ok(false);
        }
        let backend = intent.backend_process_id();
        let lane = lane_index(intent.lane());
        let Some(queues) = self.backends.get(&backend) else {
            return Ok(false);
        };
        if queues.queued_items + 1 > self.transport.max_query_backend_queued_operations()
            || queues.queued_bytes + queued_bytes > self.transport.max_query_backend_queued_bytes()
        {
            return Ok(false);
        }
        if queues.in_flight[lane] >= self.budget.permits_for(intent.lane()) {
            return Ok(false);
        }
        Ok(queued_bytes <= self.transport.max_batch_encoded_bytes())
    }

    /// Reports whether an exact operation is still locally queued or has
    /// crossed the process-transport acceptance boundary.
    pub(super) fn operation_state(
        &self,
        operation_id: TaskOperationId,
    ) -> Result<DispatchOperationState, TaskExecutionError> {
        let queued = usize::from(self.queued_routes.contains_key(&operation_id));
        let in_flight = usize::from(self.in_flight.contains_key(&operation_id));
        match queued + in_flight {
            0 => Ok(DispatchOperationState::Absent),
            1 if queued == 1 => Ok(DispatchOperationState::Queued),
            1 => Ok(DispatchOperationState::InFlight),
            _ => Err(TaskExecutionError::Schedule(
                "one operation identity has multiple dispatcher owners".to_owned(),
            )),
        }
    }

    pub(super) fn queued_operation_kind(
        &self,
        operation_id: TaskOperationId,
    ) -> Option<OperationKind> {
        self.queued_entry(operation_id)
            .map(|entry| entry.intent.kind())
    }
    fn queued_entry(&self, operation_id: TaskOperationId) -> Option<&QueuedOperation> {
        let &(backend, lane, _) = self.queued_routes.get(&operation_id)?;
        self.backends.get(&backend)?.lanes[lane].get(operation_id)
    }

    /// Normal drain preserves only the exact close control carried by an
    /// UpdateTask; all ordinary input updates are withdrawn.
    pub(super) fn queued_destination_close(&self, operation_id: TaskOperationId) -> bool {
        self.queued_entry(operation_id)
            .is_some_and(|entry| match &entry.intent {
                OperationIntent::UpdateTask { domains, .. } => domains.iter().all(|domain| {
                    matches!(
                        domain,
                        crate::effect::TaskDomainIntent::CloseExchangeDestination { .. }
                    )
                }),
                _ => false,
            })
    }

    /// Removes one exact operation that has not crossed process transport.
    /// Dropping its queue permit here returns process capacity before the
    /// application effect is resolved by an older generation's receipt.
    pub(super) fn cancel_queued(
        &mut self,
        operation_id: TaskOperationId,
    ) -> Result<OperationIntent, TaskExecutionError> {
        if self.operation_state(operation_id)? != DispatchOperationState::Queued {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let QueuedOperation {
            intent,
            queue_permit,
            ..
        } = self.remove_queued(operation_id)?;
        self.released_grants.push(queue_permit);
        Ok(intent)
    }

    pub(crate) fn take_released_grant(&mut self) -> Option<QueuedInputGrantRef> {
        self.released_grants.pop()
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
    fn remove_queued(
        &mut self,
        operation_id: TaskOperationId,
    ) -> Result<QueuedOperation, TaskExecutionError> {
        let (backend, lane, at) = self
            .queued_routes
            .remove(&operation_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let queues = self
            .backends
            .get_mut(&backend)
            .expect("indexed original backend");
        let entry = queues.lanes[lane]
            .remove(operation_id)
            .expect("indexed original queue entry");
        self.queued_times.remove(&(at, operation_id));
        queues.queued_items -= 1;
        queues.queued_bytes -= entry.queued_bytes;
        self.queued_items -= 1;
        self.queued_bytes -= entry.queued_bytes;
        Ok(entry)
    }
    pub(crate) fn queue_deadline(&self) -> Option<MonotonicInstant> {
        self.queued_times
            .first()
            .map(|&(at, _)| at.saturating_add(self.transport.frontend_queue_residence()))
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

    /// Takes only one already-queued exact head operation. Pending process
    /// observation uses this for actor-owned Abort, so ordinary lifecycle
    /// work cannot hitch a ride even though it shares the same lane.
    pub(super) fn take_priority_operation_batch(
        &mut self,
        operation_id: TaskOperationId,
    ) -> Option<DispatchBatch> {
        let &(backend, lane, _) = self.queued_routes.get(&operation_id)?;
        if self.backends.get(&backend)?.lanes[lane]
            .front()?
            .intent
            .operation_id()
            != operation_id
        {
            return None;
        }
        let queues = self.backends.get_mut(&backend)?;
        if queues.credits[lane] == 0 {
            queues.credits[lane] = self.budget.permits_for(LANES[lane]);
        }
        self.take_backend_lane_batch_limited(backend, lane, 1)
    }

    fn take_backend_lane_batch(
        &mut self,
        backend: BackendProcessId,
        lane: usize,
    ) -> Option<DispatchBatch> {
        self.take_backend_lane_batch_limited(backend, lane, self.transport.max_batch_items())
    }

    fn take_backend_lane_batch_limited(
        &mut self,
        backend: BackendProcessId,
        lane: usize,
        max_items: usize,
    ) -> Option<DispatchBatch> {
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
            let original_run = queues.lanes[lane]
                .front()
                .expect("the lane was observed nonempty")
                .intent
                .native_run_key();
            while operations.len() < allowance {
                let Some(entry) = queues.lanes[lane].front() else {
                    break;
                };
                // One original batch becomes one actual RPC/local offer.
                // Keep a different method or max-wait at the SAME queue head
                // for the next fair rotation, before moving any reservation.
                // No operation is reticketed or reordered by this boundary.
                if !operations.is_empty() && entry.intent.native_run_key() != original_run {
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
        for operation in &operations {
            let (_, _, at) = self
                .queued_routes
                .remove(&operation.operation_id())
                .expect("released original queue route");
            self.queued_times.remove(&(at, operation.operation_id()));
        }
        self.queued_items -= operations.len();
        self.queued_bytes -= queued_bytes;
        let batch = DispatchBatch::with_queued_at(
            backend,
            LANES[lane],
            operations,
            queue_permits,
            queued_bytes,
            queued_at,
        );
        Some(batch)
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
            let queues = self
                .backends
                .get_mut(&backend)
                .expect("restored original backend");
            for (intent, queue_permit) in operations.into_iter().zip(queue_permits).rev() {
                let queued_bytes = intent.queued_bytes();
                let operation_id = intent.operation_id();
                self.queued_routes
                    .insert(operation_id, (backend, lane, queued_at));
                self.queued_times.insert((queued_at, operation_id));
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

    #[cfg(test)]
    pub const fn queued_items(&self) -> usize {
        self.queued_items
    }

    #[cfg(test)]
    pub const fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    #[cfg(test)]
    pub fn in_flight_operations(&self) -> usize {
        self.in_flight.len()
    }

    #[cfg(test)]
    pub fn lane_in_flight(&self, backend: BackendProcessId, lane: DispatchLane) -> usize {
        self.backends
            .get(&backend)
            .map_or(0, |queues| queues.in_flight[lane_index(lane)])
    }

    #[cfg(test)]
    pub fn lane_queued(&self, backend: BackendProcessId, lane: DispatchLane) -> usize {
        self.backends
            .get(&backend)
            .map_or(0, |queues| queues.lanes[lane_index(lane)].len())
    }

    #[cfg(test)]
    pub fn backends(&self) -> impl ExactSizeIterator<Item = BackendProcessId> + '_ {
        self.rotation.iter().copied()
    }
}

#[cfg(test)]
mod indexed_queue_tests {
    use super::*;
    use crate::{
        ids::{IdSeed, OperationIds},
        start::PayloadRef,
    };
    use novarocks_execution_contract::OperationEnvelope;
    use std::{num::NonZeroU64, time::Duration};
    fn limits() -> TransportBudget {
        TransportBudget {
            max_batch_items: 4,
            max_batch_encoded_bytes: 4096,
            max_descriptor_encoded_bytes: 4096,
            max_query_backend_queued_operations: 1024,
            max_query_backend_queued_bytes: 1_048_576,
            max_backend_queued_operations: 4096,
            max_backend_queued_bytes: 4_194_304,
            max_tasks_per_participation: 1024,
            max_active_tasks_per_backend: 1024,
            frontend_queue_residence: Duration::from_secs(5),
        }
    }
    fn at(seconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(seconds))
    }
    fn grant(value: u64) -> QueuedInputGrantRef {
        QueuedInputGrantRef(NonZeroU64::new(value).unwrap())
    }
    fn operation(value: u64) -> OperationIntent {
        let task = crate::test_support::remote(1);
        let mut ids = OperationIds::new(IdSeed::new(u128::from(value), 1));
        OperationIntent::CreateTask {
            envelope: OperationEnvelope::with_default_wait(ids.issue(), OperationKind::CreateTask),
            identity: task.identity(),
            creation: PayloadRef(NonZeroU64::new(value).unwrap()),
            queued_bytes: 256,
        }
    }
    struct ExpiryBudget(usize);
    impl crate::capacity::StepContext for ExpiryBudget {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.0 {
                false
            } else {
                self.0 -= units;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> crate::capacity::QueueStage {
            panic!("expiry cannot grant transport capacity")
        }
    }
    #[test]
    fn exact_cancellation_keeps_surrounding_fifo_and_returns_only_original_grant() {
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        for value in 1..=100 {
            dispatcher
                .enqueue_reserved(operation(value), at(0), grant(value))
                .unwrap();
        }
        assert_eq!(
            dispatcher
                .cancel_queued(operation(51).operation_id())
                .unwrap()
                .operation_id(),
            operation(51).operation_id()
        );
        assert_eq!(dispatcher.take_released_grant(), Some(grant(51)));
        assert_eq!(dispatcher.take_released_grant(), None);
        let mut expected = (1..=100).filter(|value| *value != 51);
        while let Some(batch) = dispatcher.take_batch() {
            dispatcher.accept(batch.acceptance()).unwrap();
            for (intent, original_grant) in batch.operations.iter().zip(batch.queued_inputs.iter())
            {
                let next = expected.next().unwrap();
                assert_eq!(intent.operation_id(), operation(next).operation_id());
                assert_eq!(*original_grant, grant(next));
                dispatcher.settle(intent.operation_id()).unwrap();
            }
        }
        assert_eq!(expected.next(), None);
        assert_eq!(dispatcher.queued_items(), 0);
        assert_eq!(dispatcher.queue_deadline(), None);
    }
    #[test]
    fn oldest_expiry_is_not_hidden_by_new_priority_head() {
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        dispatcher
            .enqueue_reserved(operation(1), at(0), grant(1))
            .unwrap();
        dispatcher
            .enqueue_priority_reserved(operation(2), at(4), grant(2))
            .unwrap();
        assert_eq!(dispatcher.queue_deadline(), Some(at(5)));
        let mut cursor = QueueExpiryCursor::new(at(5));
        let ExpiryProgress::Expired(expired) =
            cursor.advance(&mut dispatcher, &mut ExpiryBudget(32))
        else {
            panic!("original expired tail behind a younger priority head")
        };
        let (intent, waited, original_grant) = expired.into_parts();
        assert_eq!(intent.operation_id(), operation(1).operation_id());
        assert_eq!(waited, Duration::from_secs(5));
        assert_eq!(original_grant, grant(1));
        assert!(matches!(
            cursor.advance(&mut dispatcher, &mut ExpiryBudget(32)),
            ExpiryProgress::Complete
        ));
        assert_eq!(dispatcher.queue_deadline(), Some(at(9)));
        let batch = dispatcher.take_batch().unwrap();
        assert_eq!(
            batch.operations[0].operation_id(),
            operation(2).operation_id()
        );
    }
    #[test]
    fn refused_batch_keeps_identity_and_original_residence_before_atomic_acceptance() {
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        for value in 1..=4 {
            dispatcher
                .enqueue_reserved(operation(value), at(value), grant(value))
                .unwrap();
        }
        let batch = dispatcher.take_batch().unwrap();
        assert_eq!(dispatcher.queue_deadline(), None);
        assert_eq!(dispatcher.in_flight_operations(), 0);
        dispatcher.restore_backpressured(batch);
        assert_eq!(dispatcher.queue_deadline(), Some(at(6)));
        let batch = dispatcher.take_batch().unwrap();
        let acceptance = batch.acceptance();
        dispatcher.accept(acceptance).unwrap();
        assert_eq!(dispatcher.in_flight_operations(), 4);
        for (index, intent) in batch.operations.iter().enumerate() {
            assert_eq!(
                intent.operation_id(),
                operation(index as u64 + 1).operation_id()
            );
            assert_eq!(
                dispatcher.operation_state(intent.operation_id()).unwrap(),
                DispatchOperationState::InFlight
            );
            dispatcher.settle(intent.operation_id()).unwrap();
        }
        assert_eq!(dispatcher.in_flight_operations(), 0);
    }
}
