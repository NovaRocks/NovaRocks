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

use crate::task_execution::operation::{OperationDispatchResult, WorkerReceiptOutcome};
use crate::task_protocol_config::{DispatchBudget, DispatchLane};
use novarocks_coordination_contract::MonotonicInstant;
#[cfg(test)]
use novarocks_execution::task_execution::FetchTaskDynamicFilters;
use novarocks_execution::task_execution::{
    AbortQueryContext, AcquireQueryContextAdmissionTicket, CancelTask, CreateTaskReceipt,
    EstablishQueryContext, OperationKind, OperationOutcome, OperationShape,
    QueryContextAdmissionTicketReceipt, QueryContextDomainUpdate, QueryContextReceipt,
    QuiesceQueryContext, QuiesceQueryContextReceipt, ReleaseOutcome, ReleaseQueryContext,
    TaskDomainUpdate, TaskOperationId, UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
    status::SafeDetail,
};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
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
    /// The exact allocation authorized by the logical-execution actor and
    /// transferred to Native transport.
    EstablishQueryContext(Arc<EstablishQueryContext>),
    /// One task's create, holding its frozen parts. Every send of the create
    /// holds the same parts.
    CreateTask(super::creation::CreateTaskIntent),
    UpdateTask(Arc<UpdateTask>),
    UpdateQueryContext(Arc<UpdateQueryContext>),
    CancelTask(CancelTask),
    QuiesceQueryContext(QuiesceQueryContext),
    AbortQueryContext(AbortQueryContext),
    ReleaseQueryContext(ReleaseQueryContext),
    #[cfg(test)]
    FetchTaskDynamicFilters(FetchTaskDynamicFilters),
}

impl OperationIntent {
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::AcquireQueryContextAdmissionTicket(request) => request.envelope().kind(),
            Self::EstablishQueryContext(request) => request.envelope().kind(),
            Self::CreateTask(request) => request.envelope().kind(),
            Self::UpdateTask(request) => request.envelope().kind(),
            Self::UpdateQueryContext(request) => request.envelope().kind(),
            Self::CancelTask(request) => request.envelope().kind(),
            Self::QuiesceQueryContext(request) => request.envelope().kind(),
            Self::AbortQueryContext(request) => request.envelope().kind(),
            Self::ReleaseQueryContext(request) => request.envelope().kind(),
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(request) => request.envelope().kind(),
        }
    }

    pub fn operation_id(&self) -> TaskOperationId {
        match self {
            Self::AcquireQueryContextAdmissionTicket(request) => request.envelope().operation_id(),
            Self::EstablishQueryContext(request) => request.envelope().operation_id(),
            Self::CreateTask(request) => request.envelope().operation_id(),
            Self::UpdateTask(request) => request.envelope().operation_id(),
            Self::UpdateQueryContext(request) => request.envelope().operation_id(),
            Self::CancelTask(request) => request.envelope().operation_id(),
            Self::QuiesceQueryContext(request) => request.envelope().operation_id(),
            Self::AbortQueryContext(request) => request.envelope().operation_id(),
            Self::ReleaseQueryContext(request) => request.envelope().operation_id(),
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(request) => request.envelope().operation_id(),
        }
    }

    pub fn lane(&self) -> DispatchLane {
        DispatchBudget::lane_of(self.shape())
    }

    pub fn shape(&self) -> OperationShape {
        match self {
            Self::AcquireQueryContextAdmissionTicket(_) => {
                OperationShape::AcquireQueryContextAdmissionTicket
            }
            Self::EstablishQueryContext(_) => OperationShape::EstablishQueryContext,
            Self::CreateTask(_) => OperationShape::CreateTask,
            Self::UpdateTask(_) => OperationShape::UpdateTask,
            Self::UpdateQueryContext(request) => request.shape(),
            Self::CancelTask(_) => OperationShape::CancelTask,
            Self::QuiesceQueryContext(_) => OperationShape::QuiesceQueryContext,
            Self::AbortQueryContext(_) => OperationShape::AbortQueryContext,
            Self::ReleaseQueryContext(_) => OperationShape::ReleaseQueryContext,
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(_) => OperationShape::FetchTaskDynamicFilters,
        }
    }

    /// Whether this operation must retain progress when ordinary Native I/O
    /// is saturated.
    ///
    /// Lifecycle operations and `CancelTask` consume the process transport's
    /// control reserve even though they retain independent dispatcher lanes.
    /// Exact destination closes keep the Task's one-at-a-time Update lane.
    pub(crate) fn native_method(&self) -> novarocks_proto_codec::native_rpc::NativeRpcMethod {
        let only_close = match self {
            Self::UpdateTask(request) => {
                !request.domains().is_empty()
                    && request.domains().iter().all(task_domain_requires_control)
            }
            _ => false,
        };
        novarocks_proto_codec::native_rpc::operation_method(self.shape(), only_close)
    }
    /// Original immutable grouping fact before dispatcher dequeue. This
    /// neither encodes a DTO nor reserves any local or Joint capacity.
    pub(crate) fn native_run_key(
        &self,
    ) -> (
        novarocks_proto_codec::native_rpc::NativeRpcMethod,
        std::time::Duration,
    ) {
        let wait = match self {
            Self::AcquireQueryContextAdmissionTicket(request) => {
                request.envelope().max_wait().get()
            }
            Self::EstablishQueryContext(request) => request.envelope().max_wait().get(),
            Self::CreateTask(request) => request.envelope().max_wait().get(),
            Self::UpdateTask(request) => request.envelope().max_wait().get(),
            Self::UpdateQueryContext(request) => request.envelope().max_wait().get(),
            Self::CancelTask(request) => request.envelope().max_wait().get(),
            Self::QuiesceQueryContext(request) => request.envelope().max_wait().get(),
            Self::AbortQueryContext(request) => request.envelope().max_wait().get(),
            Self::ReleaseQueryContext(request) => request.envelope().max_wait().get(),
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(_) => std::time::Duration::ZERO,
        };
        (self.native_method(), wait)
    }
    pub(crate) fn requires_control_progress(&self) -> bool {
        use novarocks_proto_codec::native_rpc::{FrontendNativeLane, NativeTrafficClass};
        self.native_method().contract().traffic
            == NativeTrafficClass::Frontend(FrontendNativeLane::LifecycleControl)
    }

    /// The backend process this request is addressed to.
    pub fn backend_process_id(&self) -> BackendProcessId {
        match self {
            Self::AcquireQueryContextAdmissionTicket(request) => {
                request.context().backend_process_id()
            }
            Self::EstablishQueryContext(request) => request.context().backend_process_id(),
            Self::CreateTask(request) => request.identity().backend_process_id(),
            Self::UpdateTask(request) => request.identity().backend_process_id(),
            Self::UpdateQueryContext(request) => request.context().backend_process_id(),
            Self::CancelTask(request) => request.identity().backend_process_id(),
            Self::QuiesceQueryContext(request) => request.context().backend_process_id(),
            Self::AbortQueryContext(request) => request.context().backend_process_id(),
            Self::ReleaseQueryContext(request) => request.context().backend_process_id(),
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(request) => request.identity().backend_process_id(),
        }
    }

    /// What this request occupies in a bounded frontend queue.
    pub fn queued_bytes(&self) -> usize {
        let payload = match self {
            Self::AcquireQueryContextAdmissionTicket(_) => 0,
            Self::EstablishQueryContext(request) => {
                request.catalog_binding().encoded_len()
                    + request.initial_runtime_filter().encoded_len()
                    + request.query_options().encoded_len()
                    + request.initial_credential().material().encoded_len()
            }
            // Exact: the create's own encoded length, computed from its two
            // frozen carriers, already includes its fixed allowance.
            Self::CreateTask(request) => return request.queued_bytes(),
            Self::UpdateTask(request) => request
                .domains()
                .iter()
                .map(task_domain_bytes)
                .sum::<usize>(),
            Self::UpdateQueryContext(request) => match request.as_ref() {
                UpdateQueryContext::Establish(_) => {
                    unreachable!("Establish must use its exact actor-owned intent variant")
                }
                UpdateQueryContext::AdvanceDomain(request) => {
                    context_domain_bytes(request.domain())
                }
                UpdateQueryContext::RenewLease(_) => 0,
            },
            Self::CancelTask(_)
            | Self::QuiesceQueryContext(_)
            | Self::AbortQueryContext(_)
            | Self::ReleaseQueryContext(_) => 0,
            #[cfg(test)]
            Self::FetchTaskDynamicFilters(_) => 0,
        };
        payload.saturating_add(OPERATION_FIXED_BYTES)
    }

    /// The process reservation this exact operation needs before any owner
    /// retains it as queued or in flight.
    pub fn queue_request(&self) -> TaskOperationQueueRequest {
        TaskOperationQueueRequest {
            backend: self.backend_process_id(),
            lane: self.lane(),
            control_progress: self.requires_control_progress(),
            queued_bytes: self.queued_bytes(),
            native_input: crate::native::task_transport::queued_intent_quote(self).ok(),
        }
    }
}

fn task_domain_bytes(domain: &TaskDomainUpdate) -> usize {
    match domain {
        TaskDomainUpdate::SplitAssignment(intent) => intent.payload().encoded_len(),
        TaskDomainUpdate::TaskDynamicFilter { payload, .. } => payload.encoded_len(),
        TaskDomainUpdate::OpenExchangeEdges { edges, .. } => edges.len() * size_of::<u32>(),
        TaskDomainUpdate::CloseExchangeDestination { .. } => {
            size_of::<novarocks_execution::task_execution::TaskIdentity>() + size_of::<u32>()
        }
    }
}

fn task_domain_requires_control(domain: &TaskDomainUpdate) -> bool {
    matches!(domain, TaskDomainUpdate::CloseExchangeDestination { .. })
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
/// This carries only the frozen target identity and bounded size/class facts,
/// not an attempt-owned request. Producers reserve before mutating a
/// `RemoteTask` or context owner, so one slow target cannot bypass process
/// admission by entering a different attempt's queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskOperationQueueRequest {
    backend: BackendProcessId,
    lane: DispatchLane,
    control_progress: bool,
    queued_bytes: usize,
    pub(crate) native_input:
        Option<novarocks_workload_control::joint_admission::NativeQueuedInputQuote>,
}

impl TaskOperationQueueRequest {
    /// The reservation for one task create, priced before the create is
    /// frozen. `queued_bytes` is the create's exact queued size.
    pub(crate) const fn create_task(backend: BackendProcessId, queued_bytes: usize) -> Self {
        Self {
            backend,
            lane: DispatchLane::Create,
            control_progress: false,
            queued_bytes,
            native_input: None,
        }
    }

    /// The reservation for one task-domain update before it enters a
    /// `RemoteTask`'s pending queue.
    pub(crate) fn task_update(backend: BackendProcessId, update: &TaskDomainUpdate) -> Self {
        Self {
            backend,
            lane: DispatchLane::Update,
            control_progress: task_domain_requires_control(update),
            queued_bytes: task_domain_bytes(update).saturating_add(OPERATION_FIXED_BYTES),
            native_input: crate::native::task_transport::queued_update_quote(update).ok(),
        }
    }

    pub(crate) fn with_native_input(
        mut self,
        quote: novarocks_workload_control::joint_admission::NativeQueuedInputQuote,
    ) -> Self {
        self.native_input = Some(quote);
        self
    }

    #[cfg(test)]
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

/// Inline preparation state in the original request producer. It owns only
/// the original paid ticket/receipt; it is not a second queue or capacity.
#[derive(Default)]
pub struct TaskOperationQueuePreparation {
    pub(crate) request: Option<TaskOperationQueueRequest>,
    pub(crate) admission:
        Option<novarocks_workload_control::joint_admission::NativeQueuedInputAdmission>,
    pub(crate) ready: Option<novarocks_workload_control::joint_admission::NativeQueuedInputReady>,
    pub(crate) grant: Option<novarocks_workload_control::joint_admission::NativeQueuedInputGrant>,
    pub(crate) failure: Option<novarocks_workload_control::WorkError>,
}
impl std::fmt::Debug for TaskOperationQueuePreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalNativeQueuePreparation")
            .field("request", &self.request)
            .field("waiting", &self.admission.is_some())
            .field("granted", &self.grant.is_some())
            .field("failure", &self.failure)
            .finish()
    }
}
impl TaskOperationQueuePreparation {
    pub const fn new() -> Self {
        Self {
            request: None,
            admission: None,
            ready: None,
            grant: None,
            failure: None,
        }
    }

    /// Withdraws this producer's never-published queue responsibility. A
    /// successful process reservation takes `grant` out of this cell, so this
    /// cannot retire a dispatched row or an independent payload alias.
    /// The caller must first finish dropping its original unpublished input.
    pub(crate) fn withdraw_unpublished(&mut self) {
        let grant = self.grant.take();
        let admission = self.admission.take();
        let ready = self.ready.take();
        self.request = None;
        self.failure = None;
        let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(ready);
            drop(admission);
        }));
        if let Err(cause) = dropped {
            std::panic::resume_unwind(Box::new(UnpublishedQueueCleanupFailure {
                _cause: cause,
                _grant: grant,
            }));
        }
        let Some(grant) = grant else {
            return;
        };
        match grant.queue_exited_in() {
            Ok(notification) => {
                drop(grant);
                notification.notify();
            }
            Err(error) => {
                std::panic::resume_unwind(Box::new(UnpublishedQueueCleanupFailure {
                    _cause: Box::new(error),
                    _grant: Some(grant),
                }));
            }
        }
    }
}

struct UnpublishedQueueCleanupFailure {
    // Same original grant remains queued if cleanup did not prove exit.
    _cause: Box<dyn std::any::Any + Send>,
    _grant: Option<novarocks_workload_control::joint_admission::NativeQueuedInputGrant>,
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
    Create(Box<CreateTaskReceipt>),
    Update(UpdateTaskReceipt),
    Context(QueryContextReceipt),
    Quiesce(QuiesceQueryContextReceipt),
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

/// One settled operation. A domain failure never fabricates a positive ACK.
#[derive(Debug)]
pub struct OperationAcknowledgement {
    operation_id: TaskOperationId,
    kind: OperationKind,
    result: OperationAcknowledgementResult,
    canonical: Option<crate::native::apply_send_owner::CanonicalAcknowledgement>,
}
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "Canonical replies use the original boxed cell array and intake messages box the whole acknowledgement; keeping the complete result inline preserves by-value construction and clone paths without an additional result allocation."
)]
enum OperationAcknowledgementResult {
    Worker(novarocks_execution_contract::operation::OperationResult<AckPayload>),
    NonWorker {
        dispatch: OperationDispatchResult,
        payload: AckPayload,
        detail: Option<SafeDetail>,
        local_failure: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    },
}
static NO_ACK_PAYLOAD: AckPayload = AckPayload::None;
impl Clone for OperationAcknowledgement {
    fn clone(&self) -> Self {
        if let Some(canonical) = &self.canonical {
            return Self {
                operation_id: self.operation_id,
                kind: self.kind,
                result: OperationAcknowledgementResult::NonWorker {
                    dispatch: OperationDispatchResult::TransportUnknown,
                    payload: AckPayload::None,
                    detail: None,
                    local_failure: None,
                },
                canonical: Some(canonical.clone()),
            };
        }
        Self {
            operation_id: self.operation_id,
            kind: self.kind,
            result: self.result.clone(),
            canonical: None,
        }
    }
}
impl PartialEq for OperationAcknowledgement {
    fn eq(&self, other: &Self) -> bool {
        self.operation_id == other.operation_id
            && self.kind == other.kind
            && self.original_result() == other.original_result()
    }
}
impl OperationAcknowledgement {
    /// Borrow the original batch's local dispatch refusal. A canonical cell
    /// retains this same cause through its actual backing's final exit.
    pub fn governance_failure(&self) -> Option<&novarocks_workload_control::WorkError> {
        self.canonical.as_ref()?.governance_failure()
    }
    #[cfg(test)]
    pub(crate) fn shares_original_canonical_with(&self, other: &Self) -> bool {
        self.canonical
            .as_ref()
            .zip(other.canonical.as_ref())
            .is_some_and(|(a, b)| std::ptr::eq(a.original(), b.original()))
    }
    fn original_result(&self) -> &OperationAcknowledgementResult {
        self.canonical
            .as_ref()
            .map_or(&self.result, |canonical| &canonical.original().result)
    }
    pub(crate) fn bind_canonical_telemetry(
        &mut self,
        owner: std::sync::Weak<
            dyn novarocks_proto_codec::lifecycle::terminal::CanonicalTerminalTelemetry,
        >,
        ordinal: usize,
    ) {
        assert!(
            self.canonical.is_none(),
            "the canonical cell receives an original acknowledgement"
        );
        let payload = match &mut self.result {
            OperationAcknowledgementResult::Worker(
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    acknowledgement: Some(payload),
                    ..
                },
            )
            | OperationAcknowledgementResult::NonWorker { payload, .. } => Some(payload),
            _ => None,
        };
        if let Some(AckPayload::Release {
            runtime_filter: Some(telemetry),
            ..
        }) = payload
        {
            telemetry.bind_canonical_owner(owner, ordinal);
        }
    }
    pub(crate) fn from_canonical(
        original: &Self,
        canonical: crate::native::apply_send_owner::CanonicalAcknowledgement,
    ) -> Self {
        Self {
            operation_id: original.operation_id,
            kind: original.kind,
            result: OperationAcknowledgementResult::NonWorker {
                dispatch: OperationDispatchResult::TransportUnknown,
                payload: AckPayload::None,
                detail: None,
                local_failure: None,
            },
            canonical: Some(canonical),
        }
    }
    #[cfg(test)]
    pub fn new(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: OperationOutcome,
        payload: AckPayload,
    ) -> Self {
        Self::worker_receipt(operation_id, kind, outcome, payload)
    }
    /// A local refusal is not a Worker receipt. Its original typed failure
    /// travels with the existing non-Worker dispatch result.
    pub(crate) fn local_refusal(
        operation_id: TaskOperationId,
        kind: OperationKind,
        failure: novarocks_query_application::execution_error::ExecutionFailureInput,
    ) -> Self {
        Self {
            operation_id,
            kind,
            canonical: None,
            result: OperationAcknowledgementResult::NonWorker {
                dispatch: OperationDispatchResult::NonWorkerRejected,
                payload: AckPayload::None,
                detail: None,
                local_failure: Some(failure),
            },
        }
    }

    pub(crate) fn local_failure(
        &self,
    ) -> Option<&novarocks_query_application::execution_error::ExecutionFailureInput> {
        match self.original_result() {
            OperationAcknowledgementResult::NonWorker { local_failure, .. } => {
                local_failure.as_ref()
            }
            _ => None,
        }
    }

    pub fn worker_receipt(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: OperationOutcome,
        payload: AckPayload,
    ) -> Self {
        Self::from_dispatch_result(
            operation_id,
            kind,
            OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(outcome)),
            payload,
        )
    }
    pub fn worker_domain_failure(
        operation_id: TaskOperationId,
        kind: OperationKind,
        outcome: novarocks_execution_contract::operation::DomainFailureOutcome,
        failure: novarocks_execution_contract::operation::OperationDomainFailure,
    ) -> Self {
        Self {
            operation_id,
            kind,
            canonical: None,
            result: OperationAcknowledgementResult::Worker(
                novarocks_execution_contract::operation::OperationResult::DomainFailure {
                    outcome,
                    failure,
                },
            ),
        }
    }
    #[cfg(test)]
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
        let result = match dispatch_result {
            OperationDispatchResult::WorkerReceipt(receipt) => {
                OperationAcknowledgementResult::Worker(
                    novarocks_execution_contract::operation::OperationResult::Verdict {
                        outcome: receipt.outcome(),
                        detail: None,
                        acknowledgement: Some(payload),
                    },
                )
            }
            dispatch => OperationAcknowledgementResult::NonWorker {
                dispatch,
                payload,
                detail: None,
                local_failure: None,
            },
        };
        Self {
            operation_id,
            kind,
            result,
            canonical: None,
        }
    }
    /// Protocol diagnostics cannot replace a producer's domain failure.
    pub fn with_detail(mut self, detail: Option<SafeDetail>) -> Self {
        assert!(
            self.canonical.is_none(),
            "diagnostics freeze before canonical publication"
        );
        match &mut self.result {
            OperationAcknowledgementResult::Worker(
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    detail: held,
                    ..
                },
            )
            | OperationAcknowledgementResult::NonWorker { detail: held, .. } => *held = detail,
            OperationAcknowledgementResult::Worker(
                novarocks_execution_contract::operation::OperationResult::DomainFailure { .. },
            ) => panic!("a domain failure cannot be rewritten as a protocol diagnostic"),
        }
        self
    }
    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }
    pub const fn kind(&self) -> OperationKind {
        self.kind
    }
    pub fn worker_outcome(&self) -> Option<OperationOutcome> {
        match self.original_result() {
            OperationAcknowledgementResult::Worker(result) => Some(result.outcome()),
            OperationAcknowledgementResult::NonWorker { .. } => None,
        }
    }
    pub fn dispatch_result(&self) -> OperationDispatchResult {
        match self.original_result() {
            OperationAcknowledgementResult::Worker(result) => {
                OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(
                    result.outcome(),
                ))
            }
            OperationAcknowledgementResult::NonWorker { dispatch, .. } => *dispatch,
        }
    }
    pub fn payload(&self) -> &AckPayload {
        match self.original_result() {
            OperationAcknowledgementResult::Worker(
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    acknowledgement: Some(payload),
                    ..
                },
            )
            | OperationAcknowledgementResult::NonWorker { payload, .. } => payload,
            _ => &NO_ACK_PAYLOAD,
        }
    }
    pub fn detail(&self) -> Option<&SafeDetail> {
        match self.original_result() {
            OperationAcknowledgementResult::Worker(result) => result.detail(),
            OperationAcknowledgementResult::NonWorker { detail, .. } => detail.as_ref(),
        }
    }
    /// Exact result branch with the acknowledgement body left with its original owner.
    pub fn worker_header(
        &self,
    ) -> Option<novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>>
    {
        use novarocks_execution_contract::operation::OperationResult;
        match self.original_result() {
            OperationAcknowledgementResult::Worker(OperationResult::Verdict {
                outcome,
                detail,
                ..
            }) => Some(OperationResult::Verdict {
                outcome: *outcome,
                detail: detail.clone(),
                acknowledgement: None,
            }),
            OperationAcknowledgementResult::Worker(OperationResult::DomainFailure {
                outcome,
                failure,
            }) => Some(OperationResult::DomainFailure {
                outcome: *outcome,
                failure: failure.clone(),
            }),
            OperationAcknowledgementResult::NonWorker { .. } => None,
        }
    }
    pub fn is_applied(&self) -> bool {
        matches!(
            self.worker_outcome(),
            Some(OperationOutcome::Accepted | OperationOutcome::Idempotent)
        )
    }
}

/// One batch of intents the dispatcher released for one backend and lane.
#[derive(Debug)]
pub struct DispatchBatch {
    pub(crate) native_pending: Option<crate::native::task_transport::NativeBatchAdmission>,
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

    pub(crate) fn with_queued_at(
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
            native_pending: None,
            operations,
            queue_permits,
            queued_bytes,
            queued_at,
        }
    }

    #[cfg(test)]
    pub(crate) fn replace_fixture_permits(
        &mut self,
        permits: Vec<Box<dyn TaskOperationQueuePermit>>,
    ) {
        assert_eq!(self.operations.len(), permits.len());
        self.queue_permits = permits;
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

    #[cfg(test)]
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
    AdmissionFailed {
        batch: DispatchBatch,
        error: novarocks_workload_control::WorkError,
    },
    /// The transport owns the batch and will settle every operation.
    Accepted,
    /// No transport capacity was consumed; the caller still owns the exact
    /// batch and may restore it without recreating any intent.
    Backpressured(DispatchBatch),
    /// The asynchronous actor gate permanently refused this exact batch
    /// before Native transport ownership. The caller still owns the carrier
    /// and must roll every operation back as definitely unsent.
    Rejected {
        batch: DispatchBatch,
        reason: String,
    },
}

/// One process-level reservation attached to an operation while it is queued.
///
/// The reservation moves with the exact operation through dequeue, transport
/// backpressure and replay. Once the transport accepts the batch it becomes an
/// in-flight reservation and remains held until that send settles or drops.
pub trait TaskOperationQueuePermit: std::fmt::Debug + Send {
    fn mark_in_flight(&mut self);
    /// Borrow the original Native queue capability for the same batch's
    /// actual dispatch cut. Absence is not permission to omit governance.
    /// Call outside WC State while the original queue holder stays stable.
    fn native_dispatch_input(
        &self,
    ) -> Option<&novarocks_workload_control::joint_admission::NativeQueuedInputGrant> {
        None
    }
    fn native_input_retention(
        &self,
    ) -> Result<
        Option<novarocks_resource_contract::ExistingOwnerRetentionLease>,
        novarocks_workload_control::WorkError,
    > {
        Ok(None)
    }
}

/// Result of reserving process capacity before an operation enters an attempt
/// queue.
///
/// The two refusals are different facts and their callers treat them
/// differently. A full target says nothing about any other target, so an
/// admission pass skips only that target's later candidates. A full process
/// window refuses every target alike, so the pass stops.
#[derive(Debug)]
pub enum TaskOperationQueueAdmission {
    Admitted(Box<dyn TaskOperationQueuePermit>),
    /// This target's own window is full; other targets may still admit.
    TargetFull,
    /// A process-wide window is full; no target can admit until capacity is
    /// released.
    ProcessFull,
    /// The original paid source failed before any physical queue publication.
    Rejected(novarocks_workload_control::WorkError),
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
    fn try_reserve_queue(
        &self,
        request: TaskOperationQueueRequest,
        preparation: &mut TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission;

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit;
}
