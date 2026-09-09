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
    AbortQueryContext, AcquireQueryContextAdmissionTicket, CancelTask, CreateTask,
    CreateTaskReceipt, FetchTaskDynamicFilters, GetFinalTaskInfo, OperationKind, OperationOutcome,
    QueryContextAdmissionTicketReceipt, QueryContextDomainUpdate, QueryContextReceipt,
    ReleaseOutcome, ReleaseQueryContext, TaskDomainUpdate, TaskOperationId, UpdateQueryContext,
    UpdateTask, UpdateTaskReceipt, status::SafeDetail,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::coordination::{
    DispatchBudget, DispatchLane, MonotonicInstant, OperationDispatchResult, WorkerReceiptOutcome,
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
    AcquireQueryContextAdmissionTicket(AcquireQueryContextAdmissionTicket),
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
            Self::AcquireQueryContextAdmissionTicket(request) => request.envelope().kind(),
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
            Self::AcquireQueryContextAdmissionTicket(request) => request.envelope().operation_id(),
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

    /// Whether this operation must retain progress when ordinary Native I/O
    /// is saturated.
    ///
    /// Lifecycle operations and `CancelTask` consume the process transport's
    /// control reserve even though they retain independent dispatcher lanes.
    pub(crate) const fn requires_control_progress(&self) -> bool {
        matches!(
            self,
            Self::AcquireQueryContextAdmissionTicket(_)
                | Self::UpdateQueryContext(_)
                | Self::CancelTask(_)
                | Self::AbortQueryContext(_)
                | Self::ReleaseQueryContext(_)
        )
    }

    /// The backend process this request is addressed to.
    pub fn backend_process_id(&self) -> BackendProcessId {
        match self {
            Self::AcquireQueryContextAdmissionTicket(request) => {
                request.context().backend_process_id()
            }
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
            Self::AcquireQueryContextAdmissionTicket(_) => 0,
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
                        + request.query_options().encoded_len()
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

    /// The process reservation this exact operation needs before any owner
    /// retains it as queued or in flight.
    pub fn queue_request(&self) -> TaskOperationQueueRequest {
        TaskOperationQueueRequest {
            lane: self.lane(),
            control_progress: self.requires_control_progress(),
            queued_bytes: self.queued_bytes(),
        }
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

/// One process-wide queue reservation request.
///
/// This deliberately carries no attempt-owned request. Producers can reserve
/// capacity from a bounded description before they mutate a `RemoteTask` or a
/// context owner, which makes process admission the first retention point.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskOperationQueueRequest {
    lane: DispatchLane,
    control_progress: bool,
    queued_bytes: usize,
}

impl TaskOperationQueueRequest {
    /// The reservation for one task-domain update before it enters a
    /// `RemoteTask`'s pending queue.
    pub(crate) fn task_update(update: &TaskDomainUpdate) -> Self {
        Self {
            lane: DispatchLane::Update,
            control_progress: false,
            queued_bytes: task_domain_bytes(update).saturating_add(OPERATION_FIXED_BYTES),
        }
    }

    pub const fn lane(self) -> DispatchLane {
        self.lane
    }

    pub const fn requires_control_progress(self) -> bool {
        self.control_progress
    }

    pub const fn queued_bytes(self) -> usize {
        self.queued_bytes
    }
}

/// The receipt an acknowledgement carries.
///
/// Not `Eq`: a release carries the backend's sealed runtime-filter
/// contribution, and the generated wire model of those counters is only
/// `PartialEq`.
#[derive(Clone, Debug, PartialEq)]
pub enum AckPayload {
    /// The outcome carries no receipt, either because the operation kind has
    /// none or because it failed closed.
    None,
    AdmissionTicket(QueryContextAdmissionTicketReceipt),
    Create(CreateTaskReceipt),
    Update(UpdateTaskReceipt),
    Context(QueryContextReceipt),
    Release {
        receipt: QueryContextReceipt,
        outcome: ReleaseOutcome,
        /// The releasing backend's sealed runtime-filter observation.
        ///
        /// `None` means this backend installed no participant for the query.
        /// An unavailable variant means it held one and could not publish its
        /// contribution; neither is an empty contribution.
        runtime_filter: Option<QueryTerminalProfileContributionTelemetry>,
    },
}

/// One settled operation, as the transport reports it back.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationAcknowledgement {
    operation_id: TaskOperationId,
    kind: OperationKind,
    dispatch_result: OperationDispatchResult,
    payload: AckPayload,
    /// The backend's own reason for a refusal, when it gave one.
    ///
    /// A refusal that reaches a client as an outcome name alone tells them
    /// nothing they can act on: the engine knew it was a struct field-count
    /// mismatch, and the client was shown `InvalidStateOrRequest`.
    detail: Option<SafeDetail>,
}

impl OperationAcknowledgement {
    pub fn new(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: OperationOutcome,
        payload: AckPayload,
    ) -> Self {
        let dispatch_result =
            OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(outcome));
        Self::from_dispatch_result(operation_id, kind, dispatch_result, payload)
    }

    pub fn worker_receipt(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: OperationOutcome,
        payload: AckPayload,
    ) -> Self {
        let receipt = WorkerReceiptOutcome::from_contract(outcome);
        Self::from_dispatch_result(
            operation_id,
            kind,
            OperationDispatchResult::WorkerReceipt(receipt),
            payload,
        )
    }

    pub const fn transport_unknown(operation_id: TaskOperationId, kind: OperationKind) -> Self {
        Self::from_dispatch_result(
            operation_id,
            kind,
            OperationDispatchResult::TransportUnknown,
            AckPayload::None,
        )
    }

    pub const fn from_dispatch_result(
        operation_id: TaskOperationId,
        kind: OperationKind,
        dispatch_result: OperationDispatchResult,
        payload: AckPayload,
    ) -> Self {
        Self {
            operation_id,
            kind,
            dispatch_result,
            payload,
            detail: None,
        }
    }

    /// The same acknowledgement, carrying the backend's stated reason.
    pub fn with_detail(mut self, detail: Option<SafeDetail>) -> Self {
        self.detail = detail;
        self
    }

    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn kind(&self) -> OperationKind {
        self.kind
    }

    pub const fn worker_outcome(&self) -> Option<OperationOutcome> {
        match self.dispatch_result {
            OperationDispatchResult::WorkerReceipt(receipt) => Some(receipt.outcome()),
            OperationDispatchResult::TransportUnknown => None,
        }
    }

    pub const fn dispatch_result(&self) -> OperationDispatchResult {
        self.dispatch_result
    }

    pub const fn payload(&self) -> &AckPayload {
        &self.payload
    }

    pub const fn detail(&self) -> Option<&SafeDetail> {
        self.detail.as_ref()
    }

    /// Whether the backend applied this operation, or replayed an already
    /// applied one.
    pub const fn is_applied(&self) -> bool {
        matches!(
            self.worker_outcome(),
            Some(OperationOutcome::Accepted | OperationOutcome::Idempotent)
        )
    }
}

/// One batch of intents the dispatcher released for one backend and lane.
#[derive(Debug)]
pub struct DispatchBatch {
    backend: BackendProcessId,
    lane: DispatchLane,
    operations: Vec<OperationIntent>,
    queue_permits: Vec<Box<dyn TaskOperationQueuePermit>>,
    queued_bytes: usize,
    /// The oldest residence timestamp of any operation in this batch.
    ///
    /// A transport refusal restores every operation with this timestamp. That
    /// is deliberately conservative: backpressure must not reset queue age
    /// and let an operation evade the residence deadline indefinitely.
    queued_at: MonotonicInstant,
}

impl DispatchBatch {
    #[cfg(test)]
    pub(super) fn test_fixture(
        backend: BackendProcessId,
        lane: DispatchLane,
        operations: Vec<OperationIntent>,
        queued_bytes: usize,
    ) -> Self {
        let queue_permits = operations
            .iter()
            .map(|_| Box::new(TestQueuePermit) as Box<dyn TaskOperationQueuePermit>)
            .collect();
        Self::with_queued_at(
            backend,
            lane,
            operations,
            queue_permits,
            queued_bytes,
            MonotonicInstant::ORIGIN,
        )
    }

    pub(super) fn with_queued_at(
        backend: BackendProcessId,
        lane: DispatchLane,
        operations: Vec<OperationIntent>,
        queue_permits: Vec<Box<dyn TaskOperationQueuePermit>>,
        queued_bytes: usize,
        queued_at: MonotonicInstant,
    ) -> Self {
        assert_eq!(
            operations.len(),
            queue_permits.len(),
            "every queued operation must retain one process reservation"
        );
        Self {
            backend,
            lane,
            operations,
            queue_permits,
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

    pub(super) const fn queued_at(&self) -> MonotonicInstant {
        self.queued_at
    }

    pub fn into_operations(self) -> Vec<OperationIntent> {
        self.operations
    }

    pub(super) fn into_queue_parts(
        self,
    ) -> (Vec<OperationIntent>, Vec<Box<dyn TaskOperationQueuePermit>>) {
        (self.operations, self.queue_permits)
    }

    /// Transfers every queue reservation into accepted Native I/O ownership.
    pub(crate) fn commit_queue_permits(mut self) -> Vec<Box<dyn TaskOperationQueuePermit>> {
        for permit in &mut self.queue_permits {
            permit.mark_in_flight();
        }
        std::mem::take(&mut self.queue_permits)
    }

    pub(crate) fn acceptance(&self) -> DispatchAcceptance {
        // This allocation is bounded by `TransportBudget::max_batch_items`.
        // Every element already owns a process queue permit; the later
        // encoding permit covers the potentially large protobuf request.
        DispatchAcceptance {
            backend: self.backend,
            lane: self.lane,
            operation_ids: self
                .operations
                .iter()
                .map(OperationIntent::operation_id)
                .collect(),
        }
    }
}

/// The immutable identity of a batch the transport accepted.
///
/// Payload ownership remains with the transport. The dispatcher needs only
/// these ids to move its local permits from queued to in-flight after, and
/// only after, acceptance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DispatchAcceptance {
    pub(super) backend: BackendProcessId,
    pub(super) lane: DispatchLane,
    pub(super) operation_ids: Vec<TaskOperationId>,
}

/// The result of one non-blocking transport admission attempt.
#[derive(Debug)]
pub enum TaskOperationSubmit {
    /// The transport owns the batch and will settle every operation.
    Accepted,
    /// No transport capacity was consumed; the caller still owns the exact
    /// batch and may restore it without recreating any intent.
    Backpressured(DispatchBatch),
}

/// One process-level reservation attached to an operation while it is queued.
///
/// The reservation moves with the exact operation through dequeue, transport
/// backpressure and replay. Once the transport accepts the batch it becomes an
/// in-flight reservation and remains held until that send settles or drops.
pub trait TaskOperationQueuePermit: std::fmt::Debug + Send {
    fn mark_in_flight(&mut self);
}

/// Result of reserving process capacity before an operation enters an attempt
/// queue.
#[derive(Debug)]
pub enum TaskOperationQueueAdmission {
    Admitted(Box<dyn TaskOperationQueuePermit>),
    Backpressured,
}

#[cfg(test)]
#[derive(Debug)]
struct TestQueuePermit;

#[cfg(test)]
impl TaskOperationQueuePermit for TestQueuePermit {
    fn mark_in_flight(&mut self) {}
}

#[cfg(test)]
pub(super) fn test_queue_permit() -> Box<dyn TaskOperationQueuePermit> {
    Box::new(TestQueuePermit)
}

/// The seam a later transport owner implements.
///
/// A submission must not block on a wire round trip: acknowledgements come
/// back separately, which is what lets the frontend keep one serial runner.
pub trait TaskOperationSink: std::fmt::Debug + Send + Sync {
    fn try_reserve_queue(&self, request: TaskOperationQueueRequest) -> TaskOperationQueueAdmission;

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit;
}
