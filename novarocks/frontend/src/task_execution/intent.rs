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

//! Immutable operation intents, their acknowledgements, and the transport
//! seam.
//!
//! An intent is one frozen request an owner has decided to make. It is minted
//! once, kept behind a shared handle, and replayed verbatim after an unknown
//! transport outcome, so a retry can never be a different request wearing the
//! same operation id. Nothing here touches a wire.

use std::sync::Arc;

use novarocks_execution::task_execution::{
    AbortQueryContext, CancelTask, CreateTask, CreateTaskReceipt, DispatchBudget, DispatchLane,
    FetchTaskDynamicFilters, GetFinalTaskInfo, OperationKind, OperationOutcome,
    QueryContextDomainUpdate, QueryContextReceipt, ReleaseOutcome, ReleaseQueryContext,
    TaskDomainUpdate, TaskOperationId, UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
};
use novarocks_types::identity::BackendProcessId;

/// What one queued operation costs in queue bytes on top of its payload.
///
/// The exact wire size belongs to the codec, which this layer deliberately
/// cannot reach. This is the frontend's own accounting of a request's fixed
/// fields, and it is what keeps a queue of small operations bounded in bytes
/// rather than only in items.
pub const OPERATION_FIXED_BYTES: usize = 256;

/// One immutable request an owner has decided to make.
#[derive(Clone, Debug)]
pub enum OperationIntent {
    CreateTask(Arc<CreateTask>),
    UpdateTask(Arc<UpdateTask>),
    UpdateQueryContext(Arc<UpdateQueryContext>),
    CancelTask(CancelTask),
    AbortQueryContext(AbortQueryContext),
    ReleaseQueryContext(ReleaseQueryContext),
    FetchTaskDynamicFilters(FetchTaskDynamicFilters),
    GetFinalTaskInfo(GetFinalTaskInfo),
}

impl OperationIntent {
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::CreateTask(request) => request.envelope().kind(),
            Self::UpdateTask(request) => request.envelope().kind(),
            Self::UpdateQueryContext(request) => request.envelope().kind(),
            Self::CancelTask(request) => request.envelope().kind(),
            Self::AbortQueryContext(request) => request.envelope().kind(),
            Self::ReleaseQueryContext(request) => request.envelope().kind(),
            Self::FetchTaskDynamicFilters(request) => request.envelope().kind(),
            Self::GetFinalTaskInfo(request) => request.envelope().kind(),
        }
    }

    pub fn operation_id(&self) -> TaskOperationId {
        match self {
            Self::CreateTask(request) => request.envelope().operation_id(),
            Self::UpdateTask(request) => request.envelope().operation_id(),
            Self::UpdateQueryContext(request) => request.envelope().operation_id(),
            Self::CancelTask(request) => request.envelope().operation_id(),
            Self::AbortQueryContext(request) => request.envelope().operation_id(),
            Self::ReleaseQueryContext(request) => request.envelope().operation_id(),
            Self::FetchTaskDynamicFilters(request) => request.envelope().operation_id(),
            Self::GetFinalTaskInfo(request) => request.envelope().operation_id(),
        }
    }

    pub fn lane(&self) -> DispatchLane {
        DispatchBudget::lane_of(self.kind())
    }

    /// The backend process this request is addressed to.
    pub fn backend_process_id(&self) -> BackendProcessId {
        match self {
            Self::CreateTask(request) => request.identity().backend_process_id(),
            Self::UpdateTask(request) => request.identity().backend_process_id(),
            Self::UpdateQueryContext(request) => request.context().backend_process_id(),
            Self::CancelTask(request) => request.identity().backend_process_id(),
            Self::AbortQueryContext(request) => request.context().backend_process_id(),
            Self::ReleaseQueryContext(request) => request.context().backend_process_id(),
            Self::FetchTaskDynamicFilters(request) => request.identity().backend_process_id(),
            Self::GetFinalTaskInfo(request) => request.identity().backend_process_id(),
        }
    }

    /// What this request occupies in a bounded frontend queue.
    pub fn queued_bytes(&self) -> usize {
        let payload = match self {
            Self::CreateTask(request) => {
                request.descriptor().plan().encoded_len()
                    + request
                        .initial_domains()
                        .iter()
                        .map(task_domain_bytes)
                        .sum::<usize>()
            }
            Self::UpdateTask(request) => request
                .domains()
                .iter()
                .map(task_domain_bytes)
                .sum::<usize>(),
            Self::UpdateQueryContext(request) => match request.as_ref() {
                UpdateQueryContext::Establish(request) => {
                    request.catalog_binding().encoded_len()
                        + request.initial_runtime_filter().encoded_len()
                        + request.initial_credential().material().encoded_len()
                }
                UpdateQueryContext::AdvanceDomain(request) => {
                    context_domain_bytes(request.domain())
                }
                UpdateQueryContext::RenewLease(_) => 0,
            },
            Self::CancelTask(_)
            | Self::AbortQueryContext(_)
            | Self::ReleaseQueryContext(_)
            | Self::FetchTaskDynamicFilters(_)
            | Self::GetFinalTaskInfo(_) => 0,
        };
        payload.saturating_add(OPERATION_FIXED_BYTES)
    }
}

fn task_domain_bytes(domain: &TaskDomainUpdate) -> usize {
    match domain {
        TaskDomainUpdate::SplitAssignment(intent) => intent.payload().encoded_len(),
        TaskDomainUpdate::TaskDynamicFilter { payload, .. } => payload.encoded_len(),
        TaskDomainUpdate::OpenExchangeEdges { edges, .. } => edges.len() * size_of::<u32>(),
    }
}

fn context_domain_bytes(domain: &QueryContextDomainUpdate) -> usize {
    match domain {
        QueryContextDomainUpdate::CatalogBinding { payload, .. }
        | QueryContextDomainUpdate::SharedDynamicFilter { payload, .. } => payload.encoded_len(),
        QueryContextDomainUpdate::Credential(update) => update.material().encoded_len(),
    }
}

/// The receipt an acknowledgement carries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AckPayload {
    /// The outcome carries no receipt, either because the operation kind has
    /// none or because it failed closed.
    None,
    Create(CreateTaskReceipt),
    Update(UpdateTaskReceipt),
    Context(QueryContextReceipt),
    Release {
        receipt: QueryContextReceipt,
        outcome: ReleaseOutcome,
    },
}

/// One settled operation, as the transport reports it back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationAcknowledgement {
    operation_id: TaskOperationId,
    kind: OperationKind,
    outcome: OperationOutcome,
    payload: AckPayload,
}

impl OperationAcknowledgement {
    pub const fn new(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: OperationOutcome,
        payload: AckPayload,
    ) -> Self {
        Self {
            operation_id,
            kind,
            outcome,
            payload,
        }
    }

    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn kind(&self) -> OperationKind {
        self.kind
    }

    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub const fn payload(&self) -> &AckPayload {
        &self.payload
    }

    /// Whether the backend applied this operation, or replayed an already
    /// applied one.
    pub const fn is_applied(&self) -> bool {
        matches!(
            self.outcome,
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        )
    }
}

/// One batch of intents the dispatcher released for one backend and lane.
#[derive(Clone, Debug)]
pub struct DispatchBatch {
    backend: BackendProcessId,
    lane: DispatchLane,
    operations: Vec<OperationIntent>,
    queued_bytes: usize,
}

impl DispatchBatch {
    pub(super) const fn new(
        backend: BackendProcessId,
        lane: DispatchLane,
        operations: Vec<OperationIntent>,
        queued_bytes: usize,
    ) -> Self {
        Self {
            backend,
            lane,
            operations,
            queued_bytes,
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
}

/// The seam a later transport owner implements.
///
/// A submission must not block on a wire round trip: acknowledgements come
/// back separately, which is what lets the frontend keep one serial runner.
pub trait TaskOperationSink: std::fmt::Debug + Send + Sync {
    fn submit(&self, batch: &DispatchBatch);
}
