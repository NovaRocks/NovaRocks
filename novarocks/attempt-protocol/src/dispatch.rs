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

mod batch_pass;
mod emitted;
pub(crate) use batch_pass::{BatchDispatch, BatchDispatchPass};
pub(crate) use emitted::{EmittedMember, PriorReplyRoute};

mod expiry;
pub(crate) use expiry::{ExpiryProgress, QueueExpiryCursor};

use std::collections::{BTreeMap, BTreeSet};

use crate::config::FrozenTransportLimits as TransportBudget;
use crate::config::{DispatchBudget, DispatchLane};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{OperationKind, OperationShape, TaskOperationId};
use novarocks_types::identity::BackendProcessId;

use super::error::{CapacityBound, TaskExecutionError};
use crate::effect::{AttemptEffectId, DispatchBatch, OperationIntent};
use crate::input::{QueuedInputGrantRef, SendGeneration};
use crate::intent::TaskOperationQueueRequest;

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

/// The exact original same-method FIFO prefix selected for one physical send.
struct BatchPrefix {
    items: usize,
    bytes: usize,
    queued_at: MonotonicInstant,
    control_progress: bool,
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
    fn batch_prefix(&self, allowance: usize, max_bytes: usize) -> Option<BatchPrefix> {
        let head = self.front()?;
        let run = head.intent.native_run_key();
        let mut prefix = BatchPrefix {
            items: 0,
            bytes: 0,
            queued_at: head.queued_at,
            control_progress: run.0,
        };
        for entry in self.entries.values().take(allowance) {
            if entry.intent.native_run_key() != run {
                break;
            }
            let Some(bytes) = prefix.bytes.checked_add(entry.queued_bytes) else {
                break;
            };
            if bytes > max_bytes {
                break;
            }
            prefix.items += 1;
            prefix.bytes = bytes;
            prefix.queued_at = prefix.queued_at.min(entry.queued_at);
        }
        (prefix.items != 0).then_some(prefix)
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
    blocked_dispatch_pass: u64,
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
            blocked_dispatch_pass: 0,
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
    input: QueuedInputGrantRef,
    shape: OperationShape,
    batch: Option<AttemptEffectId>,
    logical_pending: bool,
}

fn batch_allowance(
    queues: &BackendQueues,
    lane: usize,
    budget: DispatchBudget,
    max_items: usize,
) -> usize {
    budget
        .permits_for(LANES[lane])
        .saturating_sub(queues.in_flight[lane])
        .min(queues.credits[lane])
        .min(max_items)
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
    dispatch_pass_serial: u64,
    queued_items: usize,
    queued_bytes: usize,
    in_flight: BTreeMap<TaskOperationId, InFlightSlot>,
    emitted: BTreeMap<AttemptEffectId, emitted::EmittedBatch>,
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
            dispatch_pass_serial: 0,
            queued_items: 0,
            queued_bytes: 0,
            in_flight: BTreeMap::new(),
            emitted: BTreeMap::new(),
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
        let queues = self.backends.get(&backend)?;
        let allowance = batch_allowance(queues, lane, self.budget, max_items);
        let prefix =
            queues.lanes[lane].batch_prefix(allowance, self.transport.max_batch_encoded_bytes())?;
        Some(self.take_batch_prefix(backend, lane, prefix))
    }

    fn take_batch_prefix(
        &mut self,
        backend: BackendProcessId,
        lane: usize,
        prefix: BatchPrefix,
    ) -> DispatchBatch {
        let queues = self
            .backends
            .get_mut(&backend)
            .expect("same quoted backend");
        let mut operations = Vec::with_capacity(prefix.items);
        let mut queue_permits = Vec::with_capacity(prefix.items);
        for _ in 0..prefix.items {
            let entry = queues.lanes[lane]
                .pop_front()
                .expect("same quoted lane prefix");
            let operation = entry.intent.operation_id();
            self.queued_routes
                .remove(&operation)
                .expect("original queue route");
            self.queued_times.remove(&(entry.queued_at, operation));
            operations.push(entry.intent);
            queue_permits.push(entry.queue_permit);
        }
        queues.queued_items -= prefix.items;
        queues.queued_bytes -= prefix.bytes;
        queues.cursor = (lane + 1) % LANES.len();
        self.queued_items -= prefix.items;
        self.queued_bytes -= prefix.bytes;
        DispatchBatch::with_queued_at(
            backend,
            LANES[lane],
            operations,
            queue_permits,
            prefix.bytes,
            prefix.queued_at,
        )
    }

    /// Records the exact grant generation beside each occupied lane credit.
    /// Only the receipt or withdrawal of that generation can free it.
    #[cfg(test)]
    pub(crate) fn accept(&mut self, batch: &DispatchBatch) -> Result<(), TaskExecutionError> {
        self.accept_batch(batch, None)
    }

    fn accept_batch(
        &mut self,
        batch: &DispatchBatch,
        effect: Option<AttemptEffectId>,
    ) -> Result<(), TaskExecutionError> {
        let backend = batch.backend();
        let lane = lane_index(batch.lane());
        let queues = self
            .backends
            .get_mut(&backend)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let count = batch.operations.len();
        if count == 0
            || count != batch.queued_inputs.len()
            || count > queues.credits[lane]
            || queues.in_flight[lane].saturating_add(count) > self.budget.permits_for(batch.lane())
            || batch.operations.iter().any(|intent| {
                let operation = intent.operation_id();
                self.in_flight.contains_key(&operation)
                    || self.queued_routes.contains_key(&operation)
            })
        {
            return Err(TaskExecutionError::Schedule(
                "transport accepted a batch outside its dispatcher reservation".to_owned(),
            ));
        }
        queues.credits[lane] -= count;
        queues.in_flight[lane] += count;
        for (intent, grant) in batch.operations.iter().zip(&batch.queued_inputs) {
            self.in_flight.insert(
                intent.operation_id(),
                InFlightSlot {
                    backend,
                    lane: batch.lane(),
                    input: *grant,
                    shape: intent.shape(),
                    batch: effect,
                    logical_pending: true,
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

    /// Retires the dispatcher credit of this physical send. The independent
    /// WC grant and physical reservations remain with the original Source.
    #[cfg(test)]
    pub(crate) fn settle_generation(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
    ) -> Result<bool, TaskExecutionError> {
        Ok(self
            .retire_generation(operation, generation, None)?
            .is_some())
    }

    /// Returns the original row's logical ownership flag only after releasing
    /// its own credit. Physical exits are independently retained by Source.
    pub(super) fn retire_generation(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
        kind: Option<OperationKind>,
    ) -> Result<Option<bool>, TaskExecutionError> {
        let Some(slot) = self.in_flight.get(&operation).copied() else {
            return Ok(None);
        };
        if slot.input.generation() != generation {
            return Ok(None);
        }
        if kind.is_some_and(|kind| kind != slot.shape.kind()) {
            return Err(TaskExecutionError::Schedule(
                "reply kind differs from its original dispatch".into(),
            ));
        }
        let queues = self
            .backends
            .get_mut(&slot.backend)
            .expect("original in-flight backend");
        let lane = lane_index(slot.lane);
        queues.in_flight[lane] = queues.in_flight[lane]
            .checked_sub(1)
            .expect("the original in-flight row owns its lane credit");
        self.in_flight.remove(&operation);
        if let Some(effect) = slot.batch {
            self.retire_emitted_member(effect);
        }
        Ok(Some(slot.logical_pending))
    }

    #[cfg(test)]
    pub(super) fn in_flight_generation(
        &self,
        operation: TaskOperationId,
    ) -> Option<SendGeneration> {
        self.in_flight
            .get(&operation)
            .map(|slot| slot.input.generation())
    }

    pub(super) fn tracks_input(
        &self,
        operation: TaskOperationId,
        input: QueuedInputGrantRef,
    ) -> bool {
        self.queued_entry(operation)
            .is_some_and(|row| row.queue_permit == input)
            || self
                .in_flight
                .get(&operation)
                .is_some_and(|row| row.input == input)
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
        QueuedInputGrantRef::new(
            NonZeroU64::new(value).unwrap(),
            crate::input::SendGeneration::new(
                novarocks_coordination_contract::AttemptActionId::new(
                    NonZeroU64::new(value).unwrap(),
                ),
            ),
        )
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
            dispatcher.accept(&batch).unwrap();
            for (intent, original_grant) in batch.operations.iter().zip(batch.queued_inputs.iter())
            {
                let next = expected.next().unwrap();
                assert_eq!(intent.operation_id(), operation(next).operation_id());
                assert_eq!(*original_grant, grant(next));
                assert!(
                    dispatcher
                        .settle_generation(intent.operation_id(), original_grant.generation())
                        .unwrap()
                );
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
        dispatcher.accept(&batch).unwrap();
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
            assert!(
                dispatcher
                    .settle_generation(
                        intent.operation_id(),
                        batch.queued_inputs[index].generation()
                    )
                    .unwrap()
            );
        }
        assert_eq!(dispatcher.in_flight_operations(), 0);
    }

    #[test]
    fn late_result_cannot_spend_the_replays_lane_credit() {
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        let request = operation(1);
        let id = request.operation_id();
        let backend = request.backend_process_id();
        let lane = request.lane();
        let first = grant(1);
        let replay = grant(2);
        dispatcher
            .enqueue_reserved(request.clone(), at(1), first)
            .unwrap();
        let batch = dispatcher.take_batch().unwrap();
        dispatcher.accept(&batch).unwrap();
        assert!(
            dispatcher
                .settle_generation(id, first.generation())
                .unwrap()
        );
        dispatcher.enqueue_reserved(request, at(2), replay).unwrap();

        // An older result owns neither the queued replay nor its later
        // in-flight credit, even though the operation identity is unchanged.
        assert!(
            !dispatcher
                .settle_generation(id, first.generation())
                .unwrap()
        );
        assert_eq!(
            dispatcher.operation_state(id).unwrap(),
            DispatchOperationState::Queued
        );
        let batch = dispatcher.take_batch().unwrap();
        assert_eq!(batch.queued_inputs.as_ref(), &[replay]);
        dispatcher.accept(&batch).unwrap();
        assert!(
            !dispatcher
                .settle_generation(id, first.generation())
                .unwrap()
        );
        assert_eq!(dispatcher.lane_in_flight(backend, lane), 1);
        assert!(
            dispatcher
                .settle_generation(id, replay.generation())
                .unwrap()
        );
        assert!(
            !dispatcher
                .settle_generation(id, replay.generation())
                .unwrap()
        );
        assert_eq!(dispatcher.lane_in_flight(backend, lane), 0);
    }

    #[test]
    fn dispatch_yield_and_capacity_refusal_keep_the_original_fifo_until_one_send() {
        use crate::capacity::{QueueStage, ReservationToken, StepContext};
        struct Turn {
            left: usize,
            reservation: QueueStage,
            blocked: Option<BackendProcessId>,
            requests: Vec<(DispatchLane, bool, BackendProcessId, usize, usize)>,
        }
        impl StepContext for Turn {
            fn charge(&mut self, units: usize) -> bool {
                let Some(left) = self.left.checked_sub(units) else {
                    return false;
                };
                self.left = left;
                true
            }
            fn reserve_queue_stage(
                &mut self,
                lane: DispatchLane,
                control: bool,
                backend: BackendProcessId,
                items: usize,
                bytes: usize,
            ) -> QueueStage {
                self.requests.push((lane, control, backend, items, bytes));
                if self.blocked == Some(backend) {
                    QueueStage::TargetFull(backend)
                } else {
                    self.reservation
                }
            }
        }

        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        let first = operation(1);
        let backend = first.backend_process_id();
        let first_id = first.operation_id();
        let second = operation(2);
        let second_id = second.operation_id();
        dispatcher.enqueue_reserved(first, at(1), grant(1)).unwrap();
        dispatcher
            .enqueue_reserved(second, at(2), grant(2))
            .unwrap();

        let mut pass = BatchDispatchPass::new();
        let mut turn = Turn {
            left: 2,
            reservation: QueueStage::TargetFull(backend),
            blocked: None,
            requests: Vec::new(),
        };
        assert!(matches!(
            pass.advance(
                &mut dispatcher,
                grant(17).generation().admission_effect(),
                &mut turn
            )
            .unwrap(),
            BatchDispatch::Yield
        ));
        assert!(turn.requests.is_empty());
        assert_eq!(dispatcher.queued_items(), 2);

        turn.left = 10;
        assert!(matches!(
            pass.advance(
                &mut dispatcher,
                grant(17).generation().admission_effect(),
                &mut turn
            )
            .unwrap(),
            BatchDispatch::Blocked
        ));
        assert_eq!(
            turn.requests,
            [(DispatchLane::Create, false, backend, 2, 512)]
        );
        assert_eq!(dispatcher.in_flight_operations(), 0);
        assert_eq!(dispatcher.queued_entry(first_id).unwrap().queued_at, at(1));
        assert_eq!(dispatcher.queued_entry(second_id).unwrap().queued_at, at(2));

        let token = ReservationToken(NonZeroU64::new(17).unwrap());
        turn.left = 12;
        turn.reservation = QueueStage::Granted(token);
        let BatchDispatch::Ready(effect) = pass
            .advance(
                &mut dispatcher,
                grant(17).generation().admission_effect(),
                &mut turn,
            )
            .unwrap()
        else {
            panic!("the original grant and exact FIFO should be released");
        };
        assert_eq!(effect.id, grant(17).generation().admission_effect());
        let crate::effect::EffectBody::SendBatch { batch, reservation } = effect.body else {
            panic!("the reserved batch must keep its original effect");
        };
        assert_eq!(reservation, token);
        assert_eq!(batch.operations[0].operation_id(), first_id);
        assert_eq!(batch.operations[1].operation_id(), second_id);
        assert_eq!(batch.queued_inputs.as_ref(), &[grant(1), grant(2)]);
        assert_eq!(dispatcher.queued_items(), 0);
        assert_eq!(dispatcher.in_flight_operations(), 2);
        assert!(matches!(
            pass.advance(
                &mut dispatcher,
                grant(17).generation().admission_effect(),
                &mut turn
            )
            .unwrap(),
            BatchDispatch::Empty
        ));
        assert_eq!(turn.requests.len(), 2);

        // A refused target stays parked through a successful batch for a
        // different target. It does not consume another reservation call.
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, limits());
        dispatcher
            .enqueue_reserved(operation(1), at(1), grant(1))
            .unwrap();
        let mut healthy = operation(2);
        let OperationIntent::CreateTask { identity, .. } = &mut healthy else {
            unreachable!()
        };
        *identity = crate::test_support::remote(2).identity();
        let healthy_backend = healthy.backend_process_id();
        dispatcher
            .enqueue_reserved(healthy, at(2), grant(2))
            .unwrap();
        turn.left = 32;
        turn.requests.clear();
        turn.blocked = Some(backend);
        let BatchDispatch::Ready(effect) = pass
            .advance(
                &mut dispatcher,
                grant(17).generation().admission_effect(),
                &mut turn,
            )
            .unwrap()
        else {
            panic!("the healthy target advances in the same pass");
        };
        let crate::effect::EffectBody::SendBatch { batch, .. } = effect.body else {
            unreachable!()
        };
        assert_eq!(batch.backend(), healthy_backend);
        assert!(matches!(
            pass.advance(
                &mut dispatcher,
                grant(18).generation().admission_effect(),
                &mut turn
            )
            .unwrap(),
            BatchDispatch::Blocked
        ));
        assert_eq!(
            turn.requests.iter().map(|r| r.2).collect::<Vec<_>>(),
            [backend, healthy_backend]
        );
        assert!(dispatcher.tracks_input(first_id, grant(1)));
    }
}
