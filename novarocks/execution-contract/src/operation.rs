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

//! The closed typed operation surface, its receipts, and the machine-readable
//! outcome classification.
//!
//! Eight typed operations and one logical status subscription are the whole
//! control and observation surface of a task; the root result keeps its own
//! data plane. Every operation carries its own identity and deadline, and
//! every one produces its own receipt, so batching several onto one wire
//! request never gives them shared atomicity, a shared revision, or a shared
//! success.
//!
//! Endpoint retry, dispatch, admission, and resource policy belongs to the
//! query application, Worker, or Native adapter rather than this contract.

use std::fmt;
use std::time::Duration;

use std::sync::Arc;

use novarocks_types::NativeCompatibilityId;

use crate::task_execution::descriptor::TaskDescriptor;
use crate::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, CredentialEpoch, CredentialLeaseId, DomainProgression,
    DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitOffer, SplitWatermark,
    TaskDomainKind,
};
use crate::task_execution::identity::{
    AdmissionTicketId, IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
};
use crate::task_execution::lease::{LeaseReceipt, LeaseSequence, LeaseValidFor};
use crate::task_execution::status::{AbortCause, CancelReason, TaskStatus};
use crate::task_execution::transition::QueryContextState;

/// The operations that make up the task control and observation surface.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OperationKind {
    AcquireQueryContextAdmissionTicket,
    CreateTask,
    UpdateTask,
    UpdateQueryContext,
    CancelTask,
    AbortQueryContext,
    ReleaseQueryContext,
    FetchTaskDynamicFilters,
    GetFinalTaskInfo,
}

impl OperationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AcquireQueryContextAdmissionTicket => "AcquireQueryContextAdmissionTicket",
            Self::CreateTask => "CreateTask",
            Self::UpdateTask => "UpdateTask",
            Self::UpdateQueryContext => "UpdateQueryContext",
            Self::CancelTask => "CancelTask",
            Self::AbortQueryContext => "AbortQueryContext",
            Self::ReleaseQueryContext => "ReleaseQueryContext",
            Self::FetchTaskDynamicFilters => "FetchTaskDynamicFilters",
            Self::GetFinalTaskInfo => "GetFinalTaskInfo",
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a requested wait is not representable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MaxWaitError {
    Zero,
    Overflow,
}

impl fmt::Display for MaxWaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Zero => "max_wait must be greater than zero",
            Self::Overflow => "max_wait exceeds the representable range",
        })
    }
}

impl std::error::Error for MaxWaitError {}

/// How long a backend may spend on one operation.
///
/// This is a duration, never a cross-host absolute deadline: the backend
/// starts counting from its own monotonic clock when the operation arrives.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MaxWait(Duration);

impl MaxWait {
    /// What a frontend requests for an establish or a create, which may wait
    /// on the creation gate.
    pub const DEFAULT_CREATE: Duration = Duration::from_secs(15);

    /// What a frontend requests for an update, a read, a cancel, an abort, or
    /// a release.
    pub const DEFAULT_UPDATE: Duration = Duration::from_secs(5);

    /// The largest wait this contract represents before clamping.
    pub const MAX_REPRESENTABLE: Duration = Duration::from_secs(300);

    pub fn new(value: Duration) -> Result<Self, MaxWaitError> {
        if value.is_zero() {
            return Err(MaxWaitError::Zero);
        }
        if value > Self::MAX_REPRESENTABLE {
            return Err(MaxWaitError::Overflow);
        }
        Ok(Self(value))
    }

    /// The default wait for one operation kind.
    pub fn default_for(kind: OperationKind) -> Self {
        let value = match kind {
            OperationKind::AcquireQueryContextAdmissionTicket | OperationKind::CreateTask => {
                Self::DEFAULT_CREATE
            }
            OperationKind::UpdateQueryContext => Self::DEFAULT_CREATE,
            _ => Self::DEFAULT_UPDATE,
        };
        Self(value)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// Identity and deadline carried by every operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperationEnvelope {
    operation_id: TaskOperationId,
    kind: OperationKind,
    max_wait: MaxWait,
}

impl OperationEnvelope {
    pub const fn new(
        operation_id: TaskOperationId,
        kind: OperationKind,
        max_wait: MaxWait,
    ) -> Self {
        Self {
            operation_id,
            kind,
            max_wait,
        }
    }

    /// An envelope with the default wait for its kind.
    pub fn with_default_wait(operation_id: TaskOperationId, kind: OperationKind) -> Self {
        Self::new(operation_id, kind, MaxWait::default_for(kind))
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn kind(self) -> OperationKind {
        self.kind
    }

    pub const fn max_wait(self) -> MaxWait {
        self.max_wait
    }
}

/// The machine-readable outcome categories of one operation.
///
/// Every category is a Worker-produced receipt verdict with an exact wire
/// representation. Transport failure, observation loss, and destination
/// delivery are query-side port results and do not enter this enum.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OperationOutcome {
    /// The operation was applied.
    Accepted,
    /// An exact replay of an already applied operation.
    Idempotent,
    /// The operation exceeded its effective wait while queued or waiting on
    /// the creation gate. Nothing was created and nothing partial remains.
    OperationTimedOut,
    /// A different task, stage, query, or backend process.
    IdentityMismatch,
    /// The backend rejected the request before side effects because its
    /// immutable Native compatibility identity differs from the request.
    CompatibilityMismatch,
    /// The same task identity with a different descriptor or different
    /// initial domains.
    CreateConflict,
    /// Advance or renew reached a genuinely absent context with no
    /// retirement fence.
    ContextNotEstablished,
    /// An establish conflicted with the context that already exists.
    ContextConflict,
    /// A domain token was replayed with different content, skipped a value, or
    /// arrived after a seal.
    DomainConflict,
    /// The application lease was not renewed before its backend-local expiry.
    LeaseExpired,
    /// A release found tasks, output, reads, or operations still draining.
    ReleaseNotReady,
    /// A late operation reached a retained terminal context without
    /// conflicting.
    ContextTerminalReceipt,
    /// Malformed, out of bounds, or an illegal state transition.
    InvalidStateOrRequest,
    /// A terminal task received a new update it never applied.
    TerminalRejected,
    /// The retained record was reaped, or a retirement fence rejected it.
    Gone,
    /// A per-context or per-backend capacity bound was reached. This fails
    /// closed rather than degrading: there is no older path to fall back to.
    ResourceExhausted,
}

/// The split receipt of one plan node.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PlanNodeSplitReceipt {
    node: PlanNodeId,
    watermark: SplitWatermark,
    queued_splits: Option<u64>,
}

impl PlanNodeSplitReceipt {
    pub const fn new(node: PlanNodeId, watermark: SplitWatermark) -> Self {
        Self {
            node,
            watermark,
            queued_splits: None,
        }
    }

    pub const fn with_queued_splits(mut self, value: u64) -> Self {
        self.queued_splits = Some(value);
        self
    }

    pub const fn node(self) -> PlanNodeId {
        self.node
    }

    pub const fn watermark(self) -> SplitWatermark {
        self.watermark
    }

    pub const fn queued_splits(self) -> Option<u64> {
        self.queued_splits
    }
}

/// The receipt of one task-scoped domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskDomainReceipt {
    /// One receipt entry per plan node the request covered. There is no
    /// task-global split sequence.
    SplitAssignment {
        nodes: Vec<PlanNodeSplitReceipt>,
        progression: DomainProgression,
    },
    TaskDynamicFilter {
        accepted_version: Option<DomainVersion>,
        progression: DomainProgression,
    },
    OpenExchangeEdges {
        accepted_version: EdgeOpenVersion,
        opened: Vec<ExchangeEdgeId>,
        progression: DomainProgression,
    },
}

impl TaskDomainReceipt {
    pub const fn kind(&self) -> TaskDomainKind {
        match self {
            Self::SplitAssignment { .. } => TaskDomainKind::SplitAssignment,
            Self::TaskDynamicFilter { .. } => TaskDomainKind::TaskDynamicFilter,
            Self::OpenExchangeEdges { .. } => TaskDomainKind::OpenExchangeEdges,
        }
    }

    pub const fn progression(&self) -> DomainProgression {
        match self {
            Self::SplitAssignment { progression, .. }
            | Self::TaskDynamicFilter { progression, .. }
            | Self::OpenExchangeEdges { progression, .. } => *progression,
        }
    }
}

/// The receipt of one query-context domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryContextDomainReceipt {
    CatalogBinding {
        accepted_version: Option<DomainVersion>,
        progression: DomainProgression,
    },
    SharedDynamicFilter {
        accepted_version: Option<DomainVersion>,
        progression: DomainProgression,
    },
    /// Only the lease and the accepted epoch are reported. No credential
    /// material, and no digest of any, ever appears in a receipt.
    Credential {
        lease_id: CredentialLeaseId,
        accepted_epoch: CredentialEpoch,
        progression: DomainProgression,
    },
}

impl QueryContextDomainReceipt {
    pub const fn progression(&self) -> DomainProgression {
        match self {
            Self::CatalogBinding { progression, .. }
            | Self::SharedDynamicFilter { progression, .. }
            | Self::Credential { progression, .. } => *progression,
        }
    }
}

/// The acknowledgement of a task creation.
///
/// The acknowledgement itself is the linearization point: receiving it proves
/// the task, its receiver, and its inbound capability are installed and the
/// task was submitted as runnable. It carries the task's current status, which
/// closes the window between creating a task and observing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTaskReceipt {
    identity: TaskIdentity,
    domains: Vec<TaskDomainReceipt>,
    current_status: TaskStatus,
}

impl CreateTaskReceipt {
    pub const fn new(
        identity: TaskIdentity,
        domains: Vec<TaskDomainReceipt>,
        current_status: TaskStatus,
    ) -> Self {
        Self {
            identity,
            domains,
            current_status,
        }
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub fn domains(&self) -> &[TaskDomainReceipt] {
        &self.domains
    }

    pub const fn current_status(&self) -> &TaskStatus {
        &self.current_status
    }
}

/// The acknowledgement of a task update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateTaskReceipt {
    identity: TaskIdentity,
    domains: Vec<TaskDomainReceipt>,
}

impl UpdateTaskReceipt {
    pub const fn new(identity: TaskIdentity, domains: Vec<TaskDomainReceipt>) -> Self {
        Self { identity, domains }
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub fn domains(&self) -> &[TaskDomainReceipt] {
        &self.domains
    }
}

/// The acknowledgement of a query context operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryContextReceipt {
    context: QueryContextRef,
    state: QueryContextState,
    lease: Option<LeaseReceipt>,
    domains: Vec<QueryContextDomainReceipt>,
}

impl QueryContextReceipt {
    pub const fn new(context: QueryContextRef, state: QueryContextState) -> Self {
        Self {
            context,
            state,
            lease: None,
            domains: Vec::new(),
        }
    }

    pub const fn with_lease(mut self, receipt: LeaseReceipt) -> Self {
        self.lease = Some(receipt);
        self
    }

    pub fn with_domains(mut self, domains: Vec<QueryContextDomainReceipt>) -> Self {
        self.domains = domains;
        self
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn state(&self) -> QueryContextState {
        self.state
    }

    pub const fn lease(&self) -> Option<LeaseReceipt> {
        self.lease
    }

    pub fn domains(&self) -> &[QueryContextDomainReceipt] {
        &self.domains
    }
}

/// The immutable admission grant returned by the issuing worker.
///
/// The context and validity are repeated deliberately. A caller validates the
/// receipt against its exact request instead of treating possession of an
/// opaque ticket id as proof that it was issued for the right attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QueryContextAdmissionTicketReceipt {
    ticket_id: AdmissionTicketId,
    context: QueryContextRef,
    valid_for: LeaseValidFor,
}

impl QueryContextAdmissionTicketReceipt {
    pub const fn new(
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        valid_for: LeaseValidFor,
    ) -> Self {
        Self {
            ticket_id,
            context,
            valid_for,
        }
    }

    pub const fn ticket_id(self) -> AdmissionTicketId {
        self.ticket_id
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn valid_for(self) -> LeaseValidFor {
        self.valid_for
    }
}

/// How a release request was answered.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReleaseOutcome {
    /// Shared resources were released and the context is retained terminal.
    Released,
    /// Local tasks, output, reads, or operations are still draining. The
    /// context stays active, this operation was not applied, and it does not
    /// occupy the first-wins position, so the identical request may be
    /// retried once state advances.
    NotReady,
    /// The context was already terminal. The receipt reports the original
    /// cause.
    AlreadyTerminal,
}

/// One split-domain offer for one plan node: a contiguous batch, or the
/// standalone terminal marker that names no range.
///
/// The tokens are neutral and the connector payload is codec-owned, so this
/// layer can validate the watermark contract of ADR-0123 without ever
/// interpreting a connector split. [`SplitOffer`] carries the range invariant,
/// so an intent cannot be built around a descending or invented range.
#[derive(Clone, Debug)]
pub struct SplitAssignmentIntent {
    node: PlanNodeId,
    offer: SplitOffer,
    payload: Arc<dyn CodecOwnedContent>,
}

impl SplitAssignmentIntent {
    pub const fn new(
        node: PlanNodeId,
        offer: SplitOffer,
        payload: Arc<dyn CodecOwnedContent>,
    ) -> Self {
        Self {
            node,
            offer,
            payload,
        }
    }

    pub const fn node(&self) -> PlanNodeId {
        self.node
    }

    pub const fn offer(&self) -> SplitOffer {
        self.offer
    }

    pub const fn no_more_splits(&self) -> bool {
        self.offer.no_more_splits()
    }

    pub fn payload(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.payload
    }
}

/// A closed task-scoped domain change.
#[derive(Clone, Debug)]
pub enum TaskDomainUpdate {
    SplitAssignment(SplitAssignmentIntent),
    TaskDynamicFilter {
        version: DomainVersion,
        payload: Arc<dyn CodecOwnedContent>,
    },
    /// Grants send permission on edges the descriptor already froze. It can
    /// neither add a destination nor widen an inbound authorization.
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
}

impl TaskDomainUpdate {
    pub const fn kind(&self) -> TaskDomainKind {
        match self {
            Self::SplitAssignment(_) => TaskDomainKind::SplitAssignment,
            Self::TaskDynamicFilter { .. } => TaskDomainKind::TaskDynamicFilter,
            Self::OpenExchangeEdges { .. } => TaskDomainKind::OpenExchangeEdges,
        }
    }
}

/// A credential rotation of one query context.
///
/// The material is confidential, so this type has a hand-written `Debug` that
/// renders a placeholder. Nothing here can fingerprint or print the secret.
#[derive(Clone)]
pub struct CredentialUpdate {
    lease_id: CredentialLeaseId,
    epoch: CredentialEpoch,
    material: Arc<dyn ConfidentialContent>,
}

impl CredentialUpdate {
    pub const fn new(
        lease_id: CredentialLeaseId,
        epoch: CredentialEpoch,
        material: Arc<dyn ConfidentialContent>,
    ) -> Self {
        Self {
            lease_id,
            epoch,
            material,
        }
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> CredentialEpoch {
        self.epoch
    }

    /// Whether this rotation carries the same material as `installed`.
    pub fn matches_installed(&self, installed: &dyn ConfidentialContent) -> bool {
        self.material.matches(installed)
    }

    pub fn material(&self) -> &Arc<dyn ConfidentialContent> {
        &self.material
    }
}

impl fmt::Debug for CredentialUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialUpdate")
            .field("lease_id", &self.lease_id)
            .field("epoch", &self.epoch)
            .field("material", &"<redacted>")
            .finish()
    }
}

/// A closed query-context domain change.
#[derive(Clone, Debug)]
pub enum QueryContextDomainUpdate {
    CatalogBinding {
        version: DomainVersion,
        payload: Arc<dyn CodecOwnedContent>,
    },
    SharedDynamicFilter {
        version: DomainVersion,
        payload: Arc<dyn CodecOwnedContent>,
    },
    Credential(CredentialUpdate),
}

/// Why a request is not a legal value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestError {
    /// A create request's context reference does not address the same query
    /// and backend process as its descriptor.
    ContextMismatch(IdentityMismatch),
    /// An update carrying no domain change at all.
    EmptyUpdate,
    /// An edge-open naming no edge.
    EmptyEdgeSet,
}

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContextMismatch(mismatch) => write!(formatter, "create request {mismatch}"),
            Self::EmptyUpdate => formatter.write_str("update request carries no domain change"),
            Self::EmptyEdgeSet => formatter.write_str("edge-open request names no edge"),
        }
    }
}

impl std::error::Error for RequestError {}

/// Create one task.
///
/// The descriptor is a required field of this type and of no other. There is
/// no optional descriptor anywhere in the protocol, so no request can be
/// ambiguous about whether it creates or updates.
#[derive(Clone, Debug)]
pub struct CreateTask {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    descriptor: TaskDescriptor,
    initial_domains: Vec<TaskDomainUpdate>,
}

impl CreateTask {
    pub fn try_new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        descriptor: TaskDescriptor,
        initial_domains: Vec<TaskDomainUpdate>,
    ) -> Result<Self, RequestError> {
        descriptor
            .identity()
            .verify_query_context(context)
            .map_err(RequestError::ContextMismatch)?;
        Ok(Self {
            envelope: OperationEnvelope::with_default_wait(operation_id, OperationKind::CreateTask),
            context,
            descriptor,
            initial_domains,
        })
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub fn identity(&self) -> TaskIdentity {
        self.descriptor.identity()
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    pub fn initial_domains(&self) -> &[TaskDomainUpdate] {
        &self.initial_domains
    }
}

/// Advance one task's own domains.
///
/// This type has no descriptor field and never will: creating a task and
/// updating one are different types, not different modes of one request.
#[derive(Clone, Debug)]
pub struct UpdateTask {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
    domains: Vec<TaskDomainUpdate>,
}

impl UpdateTask {
    pub fn try_new(
        operation_id: TaskOperationId,
        identity: TaskIdentity,
        domains: Vec<TaskDomainUpdate>,
    ) -> Result<Self, RequestError> {
        if domains.is_empty() {
            return Err(RequestError::EmptyUpdate);
        }
        for domain in &domains {
            if let TaskDomainUpdate::OpenExchangeEdges { edges, .. } = domain
                && edges.is_empty()
            {
                return Err(RequestError::EmptyEdgeSet);
            }
        }
        Ok(Self {
            envelope: OperationEnvelope::with_default_wait(operation_id, OperationKind::UpdateTask),
            identity,
            domains,
        })
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub fn domains(&self) -> &[TaskDomainUpdate] {
        &self.domains
    }
}

/// Acquire worker-local capacity before creating a query context.
#[derive(Copy, Clone, Debug)]
pub struct AcquireQueryContextAdmissionTicket {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    valid_for: LeaseValidFor,
    native_compatibility_id: NativeCompatibilityId,
}

impl AcquireQueryContextAdmissionTicket {
    pub fn new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        valid_for: LeaseValidFor,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::AcquireQueryContextAdmissionTicket,
            ),
            context,
            valid_for,
            native_compatibility_id,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn valid_for(self) -> LeaseValidFor {
        self.valid_for
    }

    pub const fn native_compatibility_id(self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

/// Create one query context and install its shared facts atomically.
///
/// The initial lease is not a parameter of this type beyond its duration: its
/// sequence is always zero, so an establish cannot carry any other.
#[derive(Clone, Debug)]
pub struct EstablishQueryContext {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    admission_ticket_id: AdmissionTicketId,
    catalog_binding: Arc<dyn CodecOwnedContent>,
    initial_runtime_filter: Arc<dyn CodecOwnedContent>,
    query_options: Arc<dyn CodecOwnedContent>,
    initial_credential: CredentialUpdate,
    initial_lease_valid_for: LeaseValidFor,
}

impl EstablishQueryContext {
    #[allow(
        clippy::too_many_arguments,
        reason = "the establish contract names each independently validated immutable fact"
    )]
    pub fn new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        admission_ticket_id: AdmissionTicketId,
        catalog_binding: Arc<dyn CodecOwnedContent>,
        initial_runtime_filter: Arc<dyn CodecOwnedContent>,
        query_options: Arc<dyn CodecOwnedContent>,
        initial_credential: CredentialUpdate,
        initial_lease_valid_for: LeaseValidFor,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::UpdateQueryContext,
            ),
            context,
            admission_ticket_id,
            catalog_binding,
            initial_runtime_filter,
            query_options,
            initial_credential,
            initial_lease_valid_for,
        }
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn admission_ticket_id(&self) -> AdmissionTicketId {
        self.admission_ticket_id
    }

    pub fn catalog_binding(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.catalog_binding
    }

    pub fn initial_runtime_filter(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.initial_runtime_filter
    }

    /// The immutable execution options shared by every task in this context.
    pub fn query_options(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.query_options
    }

    pub const fn initial_credential(&self) -> &CredentialUpdate {
        &self.initial_credential
    }

    /// Always zero. The type makes any other initial sequence unconstructible.
    pub const fn initial_lease_sequence(&self) -> LeaseSequence {
        LeaseSequence::INITIAL
    }

    pub const fn initial_lease_valid_for(&self) -> LeaseValidFor {
        self.initial_lease_valid_for
    }
}

/// Advance one shared domain of an existing query context.
///
/// This type cannot create a context. A backend that receives it against a
/// genuinely absent context fails closed rather than establishing one.
#[derive(Clone, Debug)]
pub struct AdvanceQueryContextDomain {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    domain: QueryContextDomainUpdate,
}

impl AdvanceQueryContextDomain {
    pub fn new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        domain: QueryContextDomainUpdate,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::UpdateQueryContext,
            ),
            context,
            domain,
        }
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn domain(&self) -> &QueryContextDomainUpdate {
        &self.domain
    }
}

/// Renew the application lease of one query context.
#[derive(Clone, Debug)]
pub struct RenewQueryExecutionLease {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    sequence: LeaseSequence,
    valid_for: LeaseValidFor,
}

impl RenewQueryExecutionLease {
    pub fn new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        sequence: LeaseSequence,
        valid_for: LeaseValidFor,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::UpdateQueryContext,
            ),
            context,
            sequence,
            valid_for,
        }
    }

    pub const fn envelope(&self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn sequence(&self) -> LeaseSequence {
        self.sequence
    }

    pub const fn valid_for(&self) -> LeaseValidFor {
        self.valid_for
    }
}

/// The closed command set of `UpdateQueryContext`.
#[derive(Clone, Debug)]
pub enum UpdateQueryContext {
    Establish(EstablishQueryContext),
    AdvanceDomain(AdvanceQueryContextDomain),
    RenewLease(RenewQueryExecutionLease),
}

impl UpdateQueryContext {
    pub const fn envelope(&self) -> OperationEnvelope {
        match self {
            Self::Establish(request) => request.envelope(),
            Self::AdvanceDomain(request) => request.envelope(),
            Self::RenewLease(request) => request.envelope(),
        }
    }

    pub const fn context(&self) -> QueryContextRef {
        match self {
            Self::Establish(request) => request.context(),
            Self::AdvanceDomain(request) => request.context(),
            Self::RenewLease(request) => request.context(),
        }
    }

    /// Whether this command may create the context. Only an establish may.
    pub const fn may_create(&self) -> bool {
        matches!(self, Self::Establish(_))
    }
}

/// Stand one task down normally.
#[derive(Copy, Clone, Debug)]
pub struct CancelTask {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
    reason: CancelReason,
}

impl CancelTask {
    pub fn new(
        operation_id: TaskOperationId,
        identity: TaskIdentity,
        reason: CancelReason,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(operation_id, OperationKind::CancelTask),
            identity,
            reason,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    pub const fn reason(self) -> CancelReason {
        self.reason
    }
}

/// Force one query context and all its tasks down.
#[derive(Copy, Clone, Debug)]
pub struct AbortQueryContext {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    cause: AbortCause,
}

impl AbortQueryContext {
    pub fn new(operation_id: TaskOperationId, context: QueryContextRef, cause: AbortCause) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::AbortQueryContext,
            ),
            context,
            cause,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn cause(self) -> AbortCause {
        self.cause
    }
}

/// Declare that no legal create can follow and close the shared context.
///
/// It carries no task manifest and no digest. The backend verifies only what
/// it can observe locally, and answers `NotReady` while anything still drains.
#[derive(Copy, Clone, Debug)]
pub struct ReleaseQueryContext {
    envelope: OperationEnvelope,
    context: QueryContextRef,
}

impl ReleaseQueryContext {
    pub fn new(operation_id: TaskOperationId, context: QueryContextRef) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::ReleaseQueryContext,
            ),
            context,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }
}

/// Read one task's dynamic filter domains.
///
/// This is an observation read: it creates nothing, advances no status, and
/// renews no lease.
#[derive(Copy, Clone, Debug)]
pub struct FetchTaskDynamicFilters {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
    acknowledged_version: Option<DomainVersion>,
}

impl FetchTaskDynamicFilters {
    pub fn new(
        operation_id: TaskOperationId,
        identity: TaskIdentity,
        acknowledged_version: Option<DomainVersion>,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::FetchTaskDynamicFilters,
            ),
            identity,
            acknowledged_version,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    pub const fn acknowledged_version(self) -> Option<DomainVersion> {
        self.acknowledged_version
    }
}

/// Read one terminal task's final info.
#[derive(Copy, Clone, Debug)]
pub struct GetFinalTaskInfo {
    envelope: OperationEnvelope,
    identity: TaskIdentity,
}

impl GetFinalTaskInfo {
    pub fn new(operation_id: TaskOperationId, identity: TaskIdentity) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::GetFinalTaskInfo,
            ),
            identity,
        }
    }

    pub const fn envelope(self) -> OperationEnvelope {
        self.envelope
    }

    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MaxWait, MaxWaitError, OperationEnvelope, OperationKind, PlanNodeSplitReceipt,
        TaskDomainReceipt,
    };
    use crate::TaskOperationId;
    use crate::{DomainProgression, PlanNodeId, SplitSequence, SplitWatermark, TaskDomainKind};
    use std::time::Duration;

    #[test]
    fn operation_envelope_carries_exact_identity_kind_and_wait() {
        let operation_id = TaskOperationId::new_v7();
        let envelope =
            OperationEnvelope::with_default_wait(operation_id, OperationKind::CancelTask);
        assert_eq!(envelope.operation_id(), operation_id);
        assert_eq!(envelope.kind(), OperationKind::CancelTask);
        assert_eq!(envelope.max_wait().get(), Duration::from_secs(5));
        assert_eq!(MaxWait::new(Duration::ZERO), Err(MaxWaitError::Zero));
    }

    #[test]
    fn split_receipt_preserves_the_exact_plan_node_watermark() {
        let node = PlanNodeId::new(4).expect("plan node");
        let watermark =
            SplitWatermark::empty().apply_batch(SplitSequence::new(9).expect("sequence"), true);
        let receipt = PlanNodeSplitReceipt::new(node, watermark).with_queued_splits(3);
        let domain = TaskDomainReceipt::SplitAssignment {
            nodes: vec![receipt],
            progression: DomainProgression::Apply,
        };
        assert_eq!(domain.kind(), TaskDomainKind::SplitAssignment);
        assert_eq!(domain.progression(), DomainProgression::Apply);
    }
}

#[cfg(test)]
mod request_tests {
    use super::{
        AbortQueryContext, AcquireQueryContextAdmissionTicket, AdvanceQueryContextDomain,
        CancelTask, CreateTask, CredentialUpdate, EstablishQueryContext, FetchTaskDynamicFilters,
        GetFinalTaskInfo, OperationKind, QueryContextAdmissionTicketReceipt,
        QueryContextDomainUpdate, ReleaseQueryContext, RenewQueryExecutionLease, RequestError,
        SplitAssignmentIntent, TaskDomainUpdate, UpdateQueryContext, UpdateTask,
    };
    use crate::task_execution::descriptor::{
        ExchangeTopology, PhysicalFragmentPlan, TaskDescriptor,
    };
    use crate::task_execution::domain::{
        CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitOffer,
        SplitSequence, TaskDomainKind,
    };
    use crate::task_execution::identity::{
        AdmissionTicketId, IdentityField, IdentityMismatch, QueryContextRef, TaskIdentity,
        TaskOperationId,
    };
    use crate::task_execution::lease::{LeaseSequence, LeaseValidFor};
    use crate::task_execution::status::{AbortCause, CancelReason};
    use crate::{FragmentContractVersion, FragmentSinkKind};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{NativeCompatibilityId, UniqueId};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct FakeContent;

    impl CodecOwnedContent for FakeContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([5; 16])
        }

        fn encoded_len(&self) -> usize {
            64
        }
    }

    impl PhysicalFragmentPlan for FakeContent {
        fn contract_version(&self) -> FragmentContractVersion {
            FragmentContractVersion::CURRENT
        }

        fn sink_kind(&self) -> FragmentSinkKind {
            FragmentSinkKind::Result
        }
    }

    /// A stand-in secret. The sentinel must never appear in any rendering.
    struct FakeSecret(&'static str);

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    impl ConfidentialContent for FakeSecret {
        fn encoded_len(&self) -> usize {
            self.0.len()
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            self.encoded_len() == other.encoded_len()
        }
    }

    fn content() -> Arc<dyn CodecOwnedContent> {
        Arc::new(FakeContent)
    }

    fn plan() -> Arc<dyn PhysicalFragmentPlan> {
        Arc::new(FakeContent)
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("nonzero"))
            .expect("nonzero query")
    }

    fn task(id: u32, backend: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            execution(),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(id).expect("nonzero task"),
            backend,
        )
    }

    fn descriptor(identity: TaskIdentity) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 2),
            NonZeroUsize::new(1).expect("nonzero"),
            Vec::new(),
            ExchangeTopology::default(),
            plan(),
        )
        .expect("legal descriptor")
    }

    fn credential() -> CredentialUpdate {
        CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(FakeSecret(SECRET_SENTINEL)),
        )
    }

    fn valid_for() -> LeaseValidFor {
        LeaseValidFor::new(Duration::from_secs(30)).expect("representable")
    }

    #[test]
    fn a_create_request_takes_its_identity_from_its_required_descriptor() {
        let backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let identity = task(1, backend);
        let context = QueryContextRef::new(execution(), frontend, backend);

        let request = CreateTask::try_new(
            TaskOperationId::new_v7(),
            context,
            descriptor(identity),
            Vec::new(),
        )
        .expect("matching context");
        assert_eq!(request.identity(), identity);
        assert_eq!(request.context(), context);
        assert_eq!(request.envelope().kind(), OperationKind::CreateTask);
        assert_eq!(request.envelope().max_wait().get(), Duration::from_secs(15));
        assert!(request.initial_domains().is_empty());
    }

    #[test]
    fn a_create_request_rejects_a_context_for_another_backend() {
        let backend = BackendProcessId::new_v7();
        let identity = task(1, backend);
        let wrong = QueryContextRef::new(
            execution(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        assert_eq!(
            CreateTask::try_new(
                TaskOperationId::new_v7(),
                wrong,
                descriptor(identity),
                Vec::new()
            )
            .expect_err("mismatched backend must fail closed"),
            RequestError::ContextMismatch(IdentityMismatch::new(IdentityField::BackendProcess))
        );
    }

    #[test]
    fn an_update_must_carry_at_least_one_domain_change() {
        let identity = task(1, BackendProcessId::new_v7());
        assert_eq!(
            UpdateTask::try_new(TaskOperationId::new_v7(), identity, Vec::new())
                .expect_err("an empty update is meaningless"),
            RequestError::EmptyUpdate
        );
        assert_eq!(
            UpdateTask::try_new(
                TaskOperationId::new_v7(),
                identity,
                vec![TaskDomainUpdate::OpenExchangeEdges {
                    version: EdgeOpenVersion::FIRST,
                    edges: Vec::new(),
                }]
            )
            .expect_err("an edge-open must name an edge"),
            RequestError::EmptyEdgeSet
        );

        let request = UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![TaskDomainUpdate::OpenExchangeEdges {
                version: EdgeOpenVersion::FIRST,
                edges: vec![ExchangeEdgeId::new(1).expect("nonzero")],
            }],
        )
        .expect("legal update");
        assert_eq!(request.identity(), identity);
        assert_eq!(request.envelope().kind(), OperationKind::UpdateTask);
        assert_eq!(request.envelope().max_wait().get(), Duration::from_secs(5));
        assert_eq!(
            request.domains()[0].kind(),
            TaskDomainKind::OpenExchangeEdges
        );
    }

    #[test]
    fn a_split_intent_carries_neutral_tokens_and_a_codec_owned_payload() {
        let node = PlanNodeId::new(4).expect("nonnegative");
        let first = SplitSequence::new(1).expect("nonzero");
        let last = SplitSequence::new(3).expect("nonzero");
        assert!(
            SplitOffer::batch(last, first, false).is_none(),
            "a descending batch is not a batch"
        );
        let offer = SplitOffer::batch(first, last, true).expect("legal batch");
        let intent = SplitAssignmentIntent::new(node, offer, content());
        assert_eq!(intent.node(), node);
        assert_eq!(intent.offer().range(), Some((first, last)));
        assert!(intent.no_more_splits());

        // A standalone terminal marker names no range at all, so nothing about
        // it can be read as a claim to have delivered sequence 1.
        let seal = SplitAssignmentIntent::new(node, SplitOffer::Seal, content());
        assert_eq!(seal.offer().range(), None);
        assert!(seal.no_more_splits());
        assert_eq!(intent.payload().encoded_len(), 64);
        assert_eq!(
            TaskDomainUpdate::SplitAssignment(intent).kind(),
            TaskDomainKind::SplitAssignment
        );
    }

    #[test]
    fn only_establish_may_create_a_query_context() {
        let context = QueryContextRef::new(
            execution(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let establish = UpdateQueryContext::Establish(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AdmissionTicketId::try_from_bytes([0x51; 16]).expect("nonzero ticket"),
            content(),
            content(),
            content(),
            credential(),
            valid_for(),
        ));
        let advance = UpdateQueryContext::AdvanceDomain(AdvanceQueryContextDomain::new(
            TaskOperationId::new_v7(),
            context,
            QueryContextDomainUpdate::SharedDynamicFilter {
                version: DomainVersion::FIRST,
                payload: content(),
            },
        ));
        let renew = UpdateQueryContext::RenewLease(RenewQueryExecutionLease::new(
            TaskOperationId::new_v7(),
            context,
            LeaseSequence::new(1),
            valid_for(),
        ));

        assert!(establish.may_create());
        assert!(!advance.may_create());
        assert!(!renew.may_create());
        for request in [&establish, &advance, &renew] {
            assert_eq!(request.context(), context);
            assert_eq!(request.envelope().kind(), OperationKind::UpdateQueryContext);
        }
    }

    #[test]
    fn an_establish_can_only_ever_carry_lease_sequence_zero() {
        let context = QueryContextRef::new(
            execution(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let establish = EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AdmissionTicketId::try_from_bytes([0x52; 16]).expect("nonzero ticket"),
            content(),
            content(),
            content(),
            credential(),
            valid_for(),
        );
        assert_eq!(establish.initial_lease_sequence(), LeaseSequence::INITIAL);
        assert!(establish.initial_lease_sequence().is_initial());
        assert_eq!(establish.initial_lease_valid_for(), valid_for());
        assert_eq!(
            establish.initial_credential().epoch(),
            CredentialEpoch::FIRST
        );
    }

    #[test]
    fn admission_ticket_request_and_receipt_bind_exact_context_and_validity() {
        let context = QueryContextRef::new(
            execution(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let valid_for = valid_for();
        let native_compatibility_id = NativeCompatibilityId::new([0x47; 32]);
        let request = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            valid_for,
            native_compatibility_id,
        );
        assert_eq!(
            request.envelope().kind(),
            OperationKind::AcquireQueryContextAdmissionTicket
        );
        assert_eq!(request.context(), context);
        assert_eq!(request.valid_for(), valid_for);
        assert_eq!(request.native_compatibility_id(), native_compatibility_id);

        let ticket_id = AdmissionTicketId::try_from_bytes([0x53; 16]).expect("nonzero ticket");
        let receipt = QueryContextAdmissionTicketReceipt::new(ticket_id, context, valid_for);
        assert_eq!(receipt.ticket_id(), ticket_id);
        assert_eq!(receipt.context(), context);
        assert_eq!(receipt.valid_for(), valid_for);
    }

    #[test]
    fn credential_material_never_appears_in_any_rendering() {
        let update = credential();
        let rendered = format!("{update:?}");
        assert!(
            !rendered.contains(SECRET_SENTINEL),
            "credential material leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(rendered.contains("epoch"), "{rendered}");

        let domain = QueryContextDomainUpdate::Credential(credential());
        let rendered = format!("{domain:?}");
        assert!(!rendered.contains(SECRET_SENTINEL), "{rendered}");

        // Same-epoch equality is answered by the owner of the live material,
        // never by a digest of it.
        assert!(update.matches_installed(&FakeSecret(SECRET_SENTINEL)));
        assert!(!update.matches_installed(&FakeSecret("shorter")));
    }

    #[test]
    fn the_remaining_requests_carry_their_own_kind_and_deadline() {
        let backend = BackendProcessId::new_v7();
        let identity = task(1, backend);
        let context = QueryContextRef::new(execution(), FrontendProcessId::new_v7(), backend);

        let cancel = CancelTask::new(
            TaskOperationId::new_v7(),
            identity,
            CancelReason::UpstreamNoLongerNeeded,
        );
        assert_eq!(cancel.envelope().kind(), OperationKind::CancelTask);
        assert_eq!(cancel.reason(), CancelReason::UpstreamNoLongerNeeded);

        let abort =
            AbortQueryContext::new(TaskOperationId::new_v7(), context, AbortCause::LeaseExpired);
        assert_eq!(abort.envelope().kind(), OperationKind::AbortQueryContext);
        assert_eq!(abort.cause(), AbortCause::LeaseExpired);

        let release = ReleaseQueryContext::new(TaskOperationId::new_v7(), context);
        assert_eq!(
            release.envelope().kind(),
            OperationKind::ReleaseQueryContext
        );
        assert_eq!(release.context(), context);

        let fetch = FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            Some(DomainVersion::FIRST),
        );
        assert_eq!(
            fetch.envelope().kind(),
            OperationKind::FetchTaskDynamicFilters
        );
        assert_eq!(fetch.acknowledged_version(), Some(DomainVersion::FIRST));
        assert_eq!(
            FetchTaskDynamicFilters::new(TaskOperationId::new_v7(), identity, None)
                .acknowledged_version(),
            None
        );

        let info = GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity);
        assert_eq!(info.envelope().kind(), OperationKind::GetFinalTaskInfo);
        assert_eq!(info.identity(), identity);

        for wait in [
            cancel.envelope().max_wait(),
            abort.envelope().max_wait(),
            release.envelope().max_wait(),
            fetch.envelope().max_wait(),
            info.envelope().max_wait(),
        ] {
            assert_eq!(wait.get(), Duration::from_secs(5));
        }
    }

    #[test]
    fn every_request_carries_a_distinct_operation_id() {
        let backend = BackendProcessId::new_v7();
        let identity = task(1, backend);
        let first = CancelTask::new(
            TaskOperationId::new_v7(),
            identity,
            CancelReason::UpstreamNoLongerNeeded,
        );
        let second = CancelTask::new(
            TaskOperationId::new_v7(),
            identity,
            CancelReason::UpstreamNoLongerNeeded,
        );
        assert_ne!(
            first.envelope().operation_id(),
            second.envelope().operation_id()
        );
    }
}
