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
//! Every backend keeps three bounded queues, one per [`DispatchLane`], and a
//! weighted round robin releases work from them. A lane's weight is its
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

use novarocks_execution::task_execution::{
    DispatchBudget, DispatchLane, MonotonicInstant, TaskOperationId, TransportBudget,
};
use novarocks_types::identity::BackendProcessId;

use super::error::{CapacityBound, TaskExecutionError};
use super::intent::{DispatchBatch, OperationIntent};

/// Every lane, in rotation order.
const LANES: [DispatchLane; 3] = [
    DispatchLane::Create,
    DispatchLane::Update,
    DispatchLane::Lifecycle,
];

const fn lane_index(lane: DispatchLane) -> usize {
    match lane {
        DispatchLane::Create => 0,
        DispatchLane::Update => 1,
        DispatchLane::Lifecycle => 2,
    }
}

/// One intent waiting in a bounded queue.
#[derive(Clone, Debug)]
struct QueuedOperation {
    intent: OperationIntent,
    queued_at: MonotonicInstant,
    queued_bytes: usize,
}

/// One operation that waited past the frontend queue-residence bound.
///
/// It is reported rather than sent late: the backend would time it out on its
/// own deadline anyway, and failing closed locally keeps the frontend's own
/// error budget honest.
#[derive(Clone, Debug)]
pub struct ExpiredOperation {
    pub intent: OperationIntent,
    pub waited: std::time::Duration,
}

#[derive(Debug)]
struct BackendQueues {
    lanes: [VecDeque<QueuedOperation>; 3],
    in_flight: [usize; 3],
    credits: [usize; 3],
    cursor: usize,
    queued_items: usize,
    queued_bytes: usize,
    active_tasks: usize,
}

impl BackendQueues {
    fn new(budget: DispatchBudget) -> Self {
        Self {
            lanes: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
            in_flight: [0; 3],
            credits: [
                budget.permits_for(DispatchLane::Create),
                budget.permits_for(DispatchLane::Update),
                budget.permits_for(DispatchLane::Lifecycle),
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
/// the protocol's per-query-per-backend queues. The process-wide bounds are
/// applied to its own totals, which is a strictly tighter bound for a single
/// attempt; aggregating them across concurrent attempts belongs to the
/// transport owner that will host these dispatchers.
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
    pub fn enqueue(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, false)
    }

    /// Queues one intent ahead of everything waiting on its lane.
    ///
    /// Only a forced stand-down uses this. It reorders the queue; it never
    /// preempts an operation that was already released.
    pub fn enqueue_priority(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        self.admit(intent, now, true)
    }

    fn admit(
        &mut self,
        intent: OperationIntent,
        now: MonotonicInstant,
        front: bool,
    ) -> Result<(), TaskExecutionError> {
        let queued_bytes = intent.queued_bytes();
        if let OperationIntent::CreateTask(request) = &intent {
            let limit = self.transport.max_descriptor_encoded_bytes();
            let actual = request.descriptor().plan().encoded_len();
            if actual > limit {
                return Err(CapacityBound::DescriptorBytes { limit, actual }.into());
            }
        }
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
        let budget = self.budget;
        let max_items = self.transport.max_batch_items();
        let max_bytes = self.transport.max_batch_encoded_bytes();
        let (lane, operations, queued_bytes) = {
            let queues = self.backends.get_mut(&backend)?;
            if queues.is_empty() {
                return None;
            }
            let lane = Self::pick_lane(queues, budget)?;
            let permits = budget.permits_for(LANES[lane]);
            let headroom = permits.saturating_sub(queues.in_flight[lane]);
            let allowance = headroom.min(queues.credits[lane]).min(max_items);
            if allowance == 0 {
                return None;
            }
            let mut operations = Vec::new();
            let mut queued_bytes = 0_usize;
            while operations.len() < allowance {
                let Some(entry) = queues.lanes[lane].front() else {
                    break;
                };
                // The first operation always goes, so a payload at the
                // descriptor bound can still leave the queue; every later one
                // has to fit.
                if !operations.is_empty() && queued_bytes + entry.queued_bytes > max_bytes {
                    break;
                }
                let entry = queues.lanes[lane]
                    .pop_front()
                    .expect("the front entry was just observed");
                queued_bytes += entry.queued_bytes;
                queues.queued_items -= 1;
                queues.queued_bytes -= entry.queued_bytes;
                operations.push(entry.intent);
            }
            if operations.is_empty() {
                return None;
            }
            queues.credits[lane] -= operations.len();
            queues.in_flight[lane] += operations.len();
            queues.cursor = (lane + 1) % LANES.len();
            (lane, operations, queued_bytes)
        };
        self.queued_items -= operations.len();
        self.queued_bytes -= queued_bytes;
        for intent in &operations {
            self.in_flight.insert(
                intent.operation_id(),
                InFlightSlot {
                    backend,
                    lane: LANES[lane],
                },
            );
        }
        Some(DispatchBatch::new(
            backend,
            LANES[lane],
            operations,
            queued_bytes,
        ))
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
