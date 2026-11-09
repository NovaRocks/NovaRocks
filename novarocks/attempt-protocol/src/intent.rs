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

//! Passive request metadata retained by the pure protocol owners.
use crate::config::{DispatchBudget, DispatchLane};
use crate::effect::{OPERATION_FIXED_BYTES, OperationIntent, TaskDomainIntent};
use novarocks_execution_contract::{
    OperationEnvelope, OperationKind, RequestError, TaskIdentity, TaskOperationId,
};
use novarocks_types::BackendProcessId;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskOperationQueueRequest {
    pub backend: BackendProcessId,
    pub lane: DispatchLane,
    pub control_progress: bool,
    pub queued_bytes: usize,
}
impl TaskOperationQueueRequest {
    pub const fn create_task(backend: BackendProcessId, queued_bytes: usize) -> Self {
        Self {
            backend,
            lane: DispatchLane::Create,
            control_progress: false,
            queued_bytes,
        }
    }
    pub fn task_update(backend: BackendProcessId, domain: &TaskDomainIntent) -> Self {
        Self {
            backend,
            lane: DispatchLane::Update,
            control_progress: matches!(domain, TaskDomainIntent::CloseExchangeDestination { .. }),
            queued_bytes: domain.queued_bytes().saturating_add(OPERATION_FIXED_BYTES),
        }
    }
    pub const fn lane(self) -> DispatchLane {
        self.lane
    }
    pub const fn backend_process_id(self) -> BackendProcessId {
        self.backend
    }
    pub const fn requires_control_progress(self) -> bool {
        self.control_progress
    }
    pub const fn queued_bytes(self) -> usize {
        self.queued_bytes
    }
}

#[derive(Clone, Debug)]
pub struct TaskUpdateRequest {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
    domains: Arc<[TaskDomainIntent]>,
}
impl TaskUpdateRequest {
    /// Same shape checks as the original EC UpdateTask request author.
    pub fn try_new(
        operation: TaskOperationId,
        identity: TaskIdentity,
        domains: Vec<TaskDomainIntent>,
    ) -> Result<Self, RequestError> {
        if domains.is_empty() {
            return Err(RequestError::EmptyUpdate);
        }
        for domain in &domains {
            if let TaskDomainIntent::OpenExchangeEdges { edges, .. } = domain
                && edges.is_empty()
            {
                return Err(RequestError::EmptyEdgeSet);
            }
        }
        Ok(Self {
            envelope: OperationEnvelope::with_default_wait(operation, OperationKind::UpdateTask),
            identity,
            domains: domains.into(),
        })
    }
    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn domains(&self) -> &[TaskDomainIntent] {
        &self.domains
    }
    pub fn intent(&self) -> OperationIntent {
        OperationIntent::UpdateTask {
            envelope: self.envelope,
            identity: self.identity,
            domains: Arc::clone(&self.domains),
        }
    }
    pub fn queue_request(&self) -> TaskOperationQueueRequest {
        let intent = self.intent();
        TaskOperationQueueRequest {
            backend: self.identity.backend_process_id(),
            lane: DispatchBudget::lane_of(intent.shape()),
            control_progress: intent.requires_control_progress(),
            queued_bytes: intent.queued_bytes(),
        }
    }
}

impl crate::effect::DispatchBatch {
    pub fn with_queued_at(
        backend: BackendProcessId,
        lane: DispatchLane,
        operations: Vec<OperationIntent>,
        queue_permits: Vec<crate::input::QueuedInputGrantRef>,
        queued_bytes: usize,
        queued_at: novarocks_coordination_contract::MonotonicInstant,
    ) -> Self {
        assert_eq!(
            operations.len(),
            queue_permits.len(),
            "every queued operation retains its original input grant"
        );
        let control_progress = operations
            .first()
            .is_some_and(OperationIntent::requires_control_progress);
        assert!(
            operations
                .iter()
                .all(|intent| intent.requires_control_progress() == control_progress),
            "a batch contains one immutable traffic class"
        );
        Self {
            backend,
            lane,
            control_progress,
            operations: operations.into_boxed_slice(),
            queued_inputs: queue_permits.into_boxed_slice(),
            queued_bytes,
            queued_at,
        }
    }
    pub const fn backend(&self) -> BackendProcessId {
        self.backend
    }
    pub const fn lane(&self) -> DispatchLane {
        self.lane
    }
    pub fn operations(&self) -> &[OperationIntent] {
        &self.operations
    }
    pub const fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }
    pub const fn queued_at(&self) -> novarocks_coordination_contract::MonotonicInstant {
        self.queued_at
    }
    pub fn into_queue_parts(
        self,
    ) -> (Vec<OperationIntent>, Vec<crate::input::QueuedInputGrantRef>) {
        (self.operations.into_vec(), self.queued_inputs.into_vec())
    }
}
impl OperationIntent {
    pub fn queue_request(&self) -> TaskOperationQueueRequest {
        TaskOperationQueueRequest {
            backend: self.backend_process_id(),
            lane: self.lane(),
            control_progress: self.requires_control_progress(),
            queued_bytes: self.queued_bytes(),
        }
    }
    /// Same grouping fact as the original Native method and max-wait pair.
    /// Generated method DTOs and actual encoding remain native-owned.
    pub fn native_run_key(&self) -> (bool, std::time::Duration) {
        (
            self.requires_control_progress(),
            self.envelope().max_wait().get(),
        )
    }
}
