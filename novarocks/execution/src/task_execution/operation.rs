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
//! This module also freezes the protocol's dispatch and payload budgets.
//! "Every task is created concurrently" rules out a plan-depth serial chain,
//! not an unbounded fan-out of requests, queues, or memory.

use std::fmt;
use std::time::Duration;

use std::sync::Arc;

use crate::task_execution::descriptor::TaskDescriptor;
use crate::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, CredentialEpoch, CredentialLeaseId, DomainProgression,
    DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitSequence, SplitWatermark,
    TaskDomainKind,
};
use crate::task_execution::identity::{
    IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
};
use crate::task_execution::lease::{LeaseReceipt, LeaseSequence, LeaseValidFor};
use crate::task_execution::status::{AbortCause, CancelReason, TaskStatus};
use crate::task_execution::transition::QueryContextState;

/// The operations that make up the task control and observation surface.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OperationKind {
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
    /// Whether this operation keeps a query context alive or closes it.
    ///
    /// Lifecycle operations get their own dispatch lane so a burst of creates
    /// and updates can never starve a renewal, an abort, or a release.
    pub const fn is_lifecycle(self) -> bool {
        matches!(
            self,
            Self::UpdateQueryContext | Self::AbortQueryContext | Self::ReleaseQueryContext
        )
    }

    pub const fn as_str(self) -> &'static str {
        match self {
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
            OperationKind::CreateTask => Self::DEFAULT_CREATE,
            OperationKind::UpdateQueryContext => Self::DEFAULT_CREATE,
            _ => Self::DEFAULT_UPDATE,
        };
        Self(value)
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

/// The server-side caps a backend clamps every requested wait into.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperationWaitCaps {
    create: Duration,
    update: Duration,
}

impl OperationWaitCaps {
    pub const DEFAULT: Self = Self {
        create: MaxWait::DEFAULT_CREATE,
        update: MaxWait::DEFAULT_UPDATE,
    };

    pub fn new(create: Duration, update: Duration) -> Option<Self> {
        if create.is_zero() || update.is_zero() {
            return None;
        }
        Some(Self { create, update })
    }

    /// The effective wait for one operation, never longer than the cap.
    pub fn clamp(self, kind: OperationKind, requested: MaxWait) -> Duration {
        let cap = match kind {
            OperationKind::CreateTask | OperationKind::UpdateQueryContext => self.create,
            _ => self.update,
        };
        requested.get().min(cap)
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
/// Retry, failure, and observation recovery are decided from this category
/// alone. Nothing in this protocol classifies an outcome by inspecting an
/// error message.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OperationOutcome {
    /// The operation was applied.
    Accepted,
    /// An exact replay of an already applied operation.
    Idempotent,
    /// The transport could not prove whether the backend applied it.
    RetryableTransportUnknown,
    /// The operation exceeded its effective wait while queued or waiting on
    /// the creation gate. Nothing was created and nothing partial remains.
    OperationTimedOut,
    /// The status transport dropped, but the exact backend process is intact.
    RetryableObservationLoss,
    /// A different task, stage, query, or backend process.
    IdentityMismatch,
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
    /// A push destination withdrew its capability for a normal reason.
    NormalDestinationCanceled,
    /// A destination failed, aborted, or does not match the frozen topology.
    DestinationFailure,
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

/// What a frontend does with an outcome.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FrontendAction {
    /// The operation is settled; carry on.
    Settled,
    /// Resend the identical immutable request to the same task and process,
    /// inside the owner's error budget and the legal request horizon.
    RetryExactRequest,
    /// Resubscribe the observation with the per-task cursors. The backend's
    /// tasks are untouched.
    ResubscribeObservation,
    /// This operation failed closed. The attempt's fate depends on what the
    /// operation was for, not on this classification.
    FailOperationClosed,
    /// The current query attempt fails. Never retried on another backend.
    FailAttempt,
    /// Keep renewing and retry the identical release once local state has
    /// advanced.
    RetryAfterProgress,
    /// Stop sending for this context and finish reconciling. Whether this is
    /// fatal depends on whether the frontend already holds the terminal facts
    /// it needs.
    StopSendingAndReconcile,
}

impl OperationOutcome {
    /// The action this outcome prescribes.
    pub const fn frontend_action(self) -> FrontendAction {
        match self {
            Self::Accepted | Self::Idempotent => FrontendAction::Settled,
            Self::RetryableTransportUnknown => FrontendAction::RetryExactRequest,
            Self::RetryableObservationLoss => FrontendAction::ResubscribeObservation,
            Self::OperationTimedOut => FrontendAction::FailOperationClosed,
            Self::ReleaseNotReady => FrontendAction::RetryAfterProgress,
            Self::ContextTerminalReceipt | Self::Gone => FrontendAction::StopSendingAndReconcile,
            Self::NormalDestinationCanceled => FrontendAction::Settled,
            Self::IdentityMismatch
            | Self::CreateConflict
            | Self::ContextNotEstablished
            | Self::ContextConflict
            | Self::DomainConflict
            | Self::LeaseExpired
            | Self::DestinationFailure
            | Self::InvalidStateOrRequest
            | Self::TerminalRejected
            | Self::ResourceExhausted => FrontendAction::FailAttempt,
        }
    }

    /// Whether the identical immutable request may be resent.
    ///
    /// Only a genuinely unknown transport outcome may be. A typed rejection
    /// is never retried, however transient its wording looks.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::RetryableTransportUnknown)
    }

    /// Whether the operation was applied or provably never applied.
    pub const fn is_settled(self) -> bool {
        !matches!(
            self,
            Self::RetryableTransportUnknown | Self::RetryableObservationLoss
        )
    }
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

impl ReleaseOutcome {
    /// Whether the owner must keep renewing the lease.
    pub const fn requires_continued_renewal(self) -> bool {
        matches!(self, Self::NotReady)
    }
}

/// The dispatch budgets one frontend keeps per backend process.
///
/// Concurrency is bounded and fair by construction. Lifecycle permits are
/// reserved so a renewal, an abort, or a release can always get out even
/// while a large create batch is in flight.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DispatchBudget {
    create_permits: usize,
    update_permits: usize,
    lifecycle_permits: usize,
}

impl DispatchBudget {
    pub const DEFAULT: Self = Self {
        create_permits: 16,
        update_permits: 12,
        lifecycle_permits: 4,
    };

    pub const fn new(
        create_permits: usize,
        update_permits: usize,
        lifecycle_permits: usize,
    ) -> Option<Self> {
        if create_permits == 0 || update_permits == 0 || lifecycle_permits == 0 {
            return None;
        }
        Some(Self {
            create_permits,
            update_permits,
            lifecycle_permits,
        })
    }

    pub const fn create_permits(self) -> usize {
        self.create_permits
    }

    pub const fn update_permits(self) -> usize {
        self.update_permits
    }

    pub const fn lifecycle_permits(self) -> usize {
        self.lifecycle_permits
    }

    pub const fn total_permits(self) -> usize {
        self.create_permits + self.update_permits + self.lifecycle_permits
    }

    /// Which lane an operation is dispatched on.
    pub const fn lane_of(kind: OperationKind) -> DispatchLane {
        if kind.is_lifecycle() {
            DispatchLane::Lifecycle
        } else if matches!(kind, OperationKind::CreateTask) {
            DispatchLane::Create
        } else {
            DispatchLane::Update
        }
    }

    pub const fn permits_for(self, lane: DispatchLane) -> usize {
        match lane {
            DispatchLane::Create => self.create_permits,
            DispatchLane::Update => self.update_permits,
            DispatchLane::Lifecycle => self.lifecycle_permits,
        }
    }
}

/// The weighted-fair dispatch lanes of one backend.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DispatchLane {
    Create,
    Update,
    Lifecycle,
}

/// Payload and queue budgets of the operation transport.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransportBudget {
    max_batch_items: usize,
    max_batch_encoded_bytes: usize,
    max_descriptor_encoded_bytes: usize,
    max_query_backend_queued_operations: usize,
    max_query_backend_queued_bytes: usize,
    max_backend_queued_operations: usize,
    max_backend_queued_bytes: usize,
    max_tasks_per_context: usize,
    max_active_tasks_per_backend: usize,
    frontend_queue_residence: Duration,
}

impl TransportBudget {
    pub const DEFAULT: Self = Self {
        max_batch_items: 32,
        max_batch_encoded_bytes: 48 * 1024 * 1024,
        max_descriptor_encoded_bytes: 16 * 1024 * 1024,
        max_query_backend_queued_operations: 4096,
        max_query_backend_queued_bytes: 256 * 1024 * 1024,
        max_backend_queued_operations: 16384,
        max_backend_queued_bytes: 512 * 1024 * 1024,
        max_tasks_per_context: 4096,
        max_active_tasks_per_backend: 32768,
        frontend_queue_residence: Duration::from_secs(15),
    };

    /// Builds a budget, rejecting a zero or an inverted bound.
    ///
    /// The defaults are the frozen contract, but a deployment has to be able
    /// to tighten them and a test has to be able to prove the enforcement
    /// path without manufacturing a 48 MiB payload. The ordering rules are
    /// what make the bounds a hierarchy rather than ten unrelated numbers: a
    /// descriptor has to fit in a batch, a batch in one query's queue, and
    /// that queue in the process's, or the smaller bound makes the larger one
    /// unreachable. The task counts nest for the same reason — a
    /// `QueryContextRef` names one query on one backend, so one context's
    /// tasks are a subset of that backend's.
    #[expect(
        clippy::too_many_arguments,
        reason = "every bound is independent; grouping them would invent a hierarchy the contract does not have"
    )]
    pub fn new(
        max_batch_items: usize,
        max_batch_encoded_bytes: usize,
        max_descriptor_encoded_bytes: usize,
        max_query_backend_queued_operations: usize,
        max_query_backend_queued_bytes: usize,
        max_backend_queued_operations: usize,
        max_backend_queued_bytes: usize,
        max_tasks_per_context: usize,
        max_active_tasks_per_backend: usize,
        frontend_queue_residence: Duration,
    ) -> Option<Self> {
        if max_batch_items == 0
            || max_batch_encoded_bytes == 0
            || max_descriptor_encoded_bytes == 0
            || max_query_backend_queued_operations == 0
            || max_query_backend_queued_bytes == 0
            || max_backend_queued_operations == 0
            || max_backend_queued_bytes == 0
            || max_tasks_per_context == 0
            || max_active_tasks_per_backend == 0
            || frontend_queue_residence.is_zero()
        {
            return None;
        }
        if max_descriptor_encoded_bytes > max_batch_encoded_bytes
            || max_batch_encoded_bytes > max_query_backend_queued_bytes
            || max_query_backend_queued_bytes > max_backend_queued_bytes
            || max_batch_items > max_query_backend_queued_operations
            || max_query_backend_queued_operations > max_backend_queued_operations
            || max_tasks_per_context > max_active_tasks_per_backend
        {
            return None;
        }
        Some(Self {
            max_batch_items,
            max_batch_encoded_bytes,
            max_descriptor_encoded_bytes,
            max_query_backend_queued_operations,
            max_query_backend_queued_bytes,
            max_backend_queued_operations,
            max_backend_queued_bytes,
            max_tasks_per_context,
            max_active_tasks_per_backend,
            frontend_queue_residence,
        })
    }

    pub const fn max_batch_items(self) -> usize {
        self.max_batch_items
    }

    pub const fn max_batch_encoded_bytes(self) -> usize {
        self.max_batch_encoded_bytes
    }

    pub const fn max_descriptor_encoded_bytes(self) -> usize {
        self.max_descriptor_encoded_bytes
    }

    pub const fn max_query_backend_queued_operations(self) -> usize {
        self.max_query_backend_queued_operations
    }

    pub const fn max_query_backend_queued_bytes(self) -> usize {
        self.max_query_backend_queued_bytes
    }

    pub const fn max_backend_queued_operations(self) -> usize {
        self.max_backend_queued_operations
    }

    pub const fn max_backend_queued_bytes(self) -> usize {
        self.max_backend_queued_bytes
    }

    pub const fn max_tasks_per_context(self) -> usize {
        self.max_tasks_per_context
    }

    pub const fn max_active_tasks_per_backend(self) -> usize {
        self.max_active_tasks_per_backend
    }

    /// How long an operation may sit in the frontend queue before it fails
    /// closed locally rather than being sent late.
    pub const fn frontend_queue_residence(self) -> Duration {
        self.frontend_queue_residence
    }

    /// Whether a batch of `items` totalling `encoded_bytes` fits.
    pub const fn batch_fits(self, items: usize, encoded_bytes: usize) -> bool {
        items > 0 && items <= self.max_batch_items && encoded_bytes <= self.max_batch_encoded_bytes
    }
}

/// One contiguous split assignment batch for one plan node.
///
/// The tokens are neutral and the connector payload is codec-owned, so this
/// layer can validate the watermark contract of ADR-0123 without ever
/// interpreting a connector split.
#[derive(Clone, Debug)]
pub struct SplitAssignmentIntent {
    node: PlanNodeId,
    first: SplitSequence,
    last: SplitSequence,
    no_more: bool,
    payload: Arc<dyn CodecOwnedContent>,
}

impl SplitAssignmentIntent {
    pub fn new(
        node: PlanNodeId,
        first: SplitSequence,
        last: SplitSequence,
        no_more: bool,
        payload: Arc<dyn CodecOwnedContent>,
    ) -> Option<Self> {
        if last < first {
            return None;
        }
        Some(Self {
            node,
            first,
            last,
            no_more,
            payload,
        })
    }

    pub const fn node(&self) -> PlanNodeId {
        self.node
    }

    pub const fn first(&self) -> SplitSequence {
        self.first
    }

    pub const fn last(&self) -> SplitSequence {
        self.last
    }

    pub const fn no_more_splits(&self) -> bool {
        self.no_more
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

/// Create one query context and install its shared facts atomically.
///
/// The initial lease is not a parameter of this type beyond its duration: its
/// sequence is always zero, so an establish cannot carry any other.
#[derive(Clone, Debug)]
pub struct EstablishQueryContext {
    envelope: OperationEnvelope,
    context: QueryContextRef,
    catalog_binding: Arc<dyn CodecOwnedContent>,
    initial_runtime_filter: Arc<dyn CodecOwnedContent>,
    initial_credential: CredentialUpdate,
    initial_lease_valid_for: LeaseValidFor,
}

impl EstablishQueryContext {
    pub fn new(
        operation_id: TaskOperationId,
        context: QueryContextRef,
        catalog_binding: Arc<dyn CodecOwnedContent>,
        initial_runtime_filter: Arc<dyn CodecOwnedContent>,
        initial_credential: CredentialUpdate,
        initial_lease_valid_for: LeaseValidFor,
    ) -> Self {
        Self {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::UpdateQueryContext,
            ),
            context,
            catalog_binding,
            initial_runtime_filter,
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

    pub fn catalog_binding(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.catalog_binding
    }

    pub fn initial_runtime_filter(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.initial_runtime_filter
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
        DispatchBudget, DispatchLane, FrontendAction, MaxWait, MaxWaitError, OperationEnvelope,
        OperationKind, OperationOutcome, OperationWaitCaps, PlanNodeSplitReceipt, ReleaseOutcome,
        TaskDomainReceipt, TransportBudget,
    };
    use crate::task_execution::domain::{
        DomainProgression, PlanNodeId, SplitSequence, SplitWatermark, TaskDomainKind,
    };
    use crate::task_execution::identity::TaskOperationId;
    use std::time::Duration;

    const ALL_OUTCOMES: [OperationOutcome; 19] = [
        OperationOutcome::Accepted,
        OperationOutcome::Idempotent,
        OperationOutcome::RetryableTransportUnknown,
        OperationOutcome::OperationTimedOut,
        OperationOutcome::RetryableObservationLoss,
        OperationOutcome::IdentityMismatch,
        OperationOutcome::CreateConflict,
        OperationOutcome::ContextNotEstablished,
        OperationOutcome::ContextConflict,
        OperationOutcome::DomainConflict,
        OperationOutcome::LeaseExpired,
        OperationOutcome::ReleaseNotReady,
        OperationOutcome::ContextTerminalReceipt,
        OperationOutcome::NormalDestinationCanceled,
        OperationOutcome::DestinationFailure,
        OperationOutcome::InvalidStateOrRequest,
        OperationOutcome::TerminalRejected,
        OperationOutcome::Gone,
        OperationOutcome::ResourceExhausted,
    ];

    #[test]
    fn max_wait_rejects_zero_and_overflow_and_defaults_per_kind() {
        assert_eq!(MaxWait::new(Duration::ZERO), Err(MaxWaitError::Zero));
        assert_eq!(
            MaxWait::new(MaxWait::MAX_REPRESENTABLE + Duration::from_secs(1)),
            Err(MaxWaitError::Overflow)
        );
        assert_eq!(
            MaxWait::default_for(OperationKind::CreateTask).get(),
            Duration::from_secs(15)
        );
        assert_eq!(
            MaxWait::default_for(OperationKind::UpdateQueryContext).get(),
            Duration::from_secs(15)
        );
        for kind in [
            OperationKind::UpdateTask,
            OperationKind::CancelTask,
            OperationKind::AbortQueryContext,
            OperationKind::ReleaseQueryContext,
            OperationKind::FetchTaskDynamicFilters,
            OperationKind::GetFinalTaskInfo,
        ] {
            assert_eq!(
                MaxWait::default_for(kind).get(),
                Duration::from_secs(5),
                "{kind}"
            );
        }
    }

    #[test]
    fn the_backend_clamps_a_requested_wait_and_never_extends_it() {
        let caps = OperationWaitCaps::DEFAULT;
        let long = MaxWait::new(Duration::from_secs(120)).expect("representable");
        assert_eq!(
            caps.clamp(OperationKind::CreateTask, long),
            Duration::from_secs(15)
        );
        assert_eq!(
            caps.clamp(OperationKind::UpdateTask, long),
            Duration::from_secs(5)
        );
        let short = MaxWait::new(Duration::from_millis(250)).expect("representable");
        assert_eq!(
            caps.clamp(OperationKind::CreateTask, short),
            Duration::from_millis(250),
            "a shorter request is honoured, never padded up to the cap"
        );
        assert!(OperationWaitCaps::new(Duration::ZERO, Duration::from_secs(1)).is_none());
    }

    #[test]
    fn an_envelope_carries_its_own_identity_kind_and_deadline() {
        let id = TaskOperationId::new_v7();
        let envelope = OperationEnvelope::with_default_wait(id, OperationKind::CancelTask);
        assert_eq!(envelope.operation_id(), id);
        assert_eq!(envelope.kind(), OperationKind::CancelTask);
        assert_eq!(envelope.max_wait().get(), Duration::from_secs(5));
    }

    #[test]
    fn only_an_unknown_transport_outcome_is_retryable() {
        for outcome in ALL_OUTCOMES {
            let retryable = outcome == OperationOutcome::RetryableTransportUnknown;
            assert_eq!(outcome.is_retryable(), retryable, "{outcome:?}");
        }
        assert!(!OperationOutcome::RetryableObservationLoss.is_retryable());
        assert!(!OperationOutcome::OperationTimedOut.is_retryable());
        assert!(!OperationOutcome::DomainConflict.is_retryable());
    }

    #[test]
    fn every_outcome_has_exactly_one_prescribed_action() {
        assert_eq!(
            OperationOutcome::Accepted.frontend_action(),
            FrontendAction::Settled
        );
        assert_eq!(
            OperationOutcome::RetryableTransportUnknown.frontend_action(),
            FrontendAction::RetryExactRequest
        );
        assert_eq!(
            OperationOutcome::RetryableObservationLoss.frontend_action(),
            FrontendAction::ResubscribeObservation
        );
        assert_eq!(
            OperationOutcome::OperationTimedOut.frontend_action(),
            FrontendAction::FailOperationClosed
        );
        assert_eq!(
            OperationOutcome::ReleaseNotReady.frontend_action(),
            FrontendAction::RetryAfterProgress
        );
        assert_eq!(
            OperationOutcome::NormalDestinationCanceled.frontend_action(),
            FrontendAction::Settled,
            "a normal downstream departure is not a producer failure"
        );
        for fatal in [
            OperationOutcome::IdentityMismatch,
            OperationOutcome::CreateConflict,
            OperationOutcome::ContextNotEstablished,
            OperationOutcome::ContextConflict,
            OperationOutcome::DomainConflict,
            OperationOutcome::LeaseExpired,
            OperationOutcome::DestinationFailure,
            OperationOutcome::InvalidStateOrRequest,
            OperationOutcome::TerminalRejected,
            OperationOutcome::ResourceExhausted,
        ] {
            assert_eq!(
                fatal.frontend_action(),
                FrontendAction::FailAttempt,
                "{fatal:?}"
            );
        }
        for reconcile in [
            OperationOutcome::ContextTerminalReceipt,
            OperationOutcome::Gone,
        ] {
            assert_eq!(
                reconcile.frontend_action(),
                FrontendAction::StopSendingAndReconcile,
                "{reconcile:?}"
            );
        }
    }

    #[test]
    fn only_the_two_unknown_outcomes_leave_an_operation_unsettled() {
        for outcome in ALL_OUTCOMES {
            let settled = !matches!(
                outcome,
                OperationOutcome::RetryableTransportUnknown
                    | OperationOutcome::RetryableObservationLoss
            );
            assert_eq!(outcome.is_settled(), settled, "{outcome:?}");
        }
    }

    #[test]
    fn a_split_receipt_reports_one_watermark_per_plan_node() {
        let node = PlanNodeId::new(4).expect("nonnegative");
        let watermark =
            SplitWatermark::empty().apply_batch(SplitSequence::new(9).expect("nonzero"), true);
        let receipt = PlanNodeSplitReceipt::new(node, watermark).with_queued_splits(3);
        assert_eq!(receipt.node(), node);
        assert_eq!(
            receipt.watermark().accepted_through().map(|s| s.get()),
            Some(9)
        );
        assert!(receipt.watermark().no_more_splits());
        assert_eq!(receipt.queued_splits(), Some(3));
        assert_eq!(
            PlanNodeSplitReceipt::new(node, watermark).queued_splits(),
            None,
            "an unreported queue depth is absent, not zero"
        );

        let domain = TaskDomainReceipt::SplitAssignment {
            nodes: vec![receipt],
            progression: DomainProgression::Apply,
        };
        assert_eq!(domain.kind(), TaskDomainKind::SplitAssignment);
        assert_eq!(domain.progression(), DomainProgression::Apply);
    }

    #[test]
    fn a_not_ready_release_keeps_the_owner_renewing() {
        assert!(ReleaseOutcome::NotReady.requires_continued_renewal());
        assert!(!ReleaseOutcome::Released.requires_continued_renewal());
        assert!(!ReleaseOutcome::AlreadyTerminal.requires_continued_renewal());
    }

    #[test]
    fn lifecycle_operations_get_their_own_reserved_lane() {
        assert!(OperationKind::UpdateQueryContext.is_lifecycle());
        assert!(OperationKind::AbortQueryContext.is_lifecycle());
        assert!(OperationKind::ReleaseQueryContext.is_lifecycle());
        for not_lifecycle in [
            OperationKind::CreateTask,
            OperationKind::UpdateTask,
            OperationKind::CancelTask,
            OperationKind::FetchTaskDynamicFilters,
            OperationKind::GetFinalTaskInfo,
        ] {
            assert!(!not_lifecycle.is_lifecycle(), "{not_lifecycle}");
        }

        assert_eq!(
            DispatchBudget::lane_of(OperationKind::CreateTask),
            DispatchLane::Create
        );
        assert_eq!(
            DispatchBudget::lane_of(OperationKind::UpdateTask),
            DispatchLane::Update
        );
        assert_eq!(
            DispatchBudget::lane_of(OperationKind::CancelTask),
            DispatchLane::Update
        );
        assert_eq!(
            DispatchBudget::lane_of(OperationKind::ReleaseQueryContext),
            DispatchLane::Lifecycle
        );
    }

    #[test]
    fn dispatch_permits_are_bounded_and_reserve_a_lifecycle_share() {
        let budget = DispatchBudget::DEFAULT;
        assert_eq!(budget.create_permits(), 16);
        assert_eq!(budget.update_permits(), 12);
        assert_eq!(budget.lifecycle_permits(), 4);
        assert_eq!(budget.total_permits(), 32);
        assert_eq!(budget.permits_for(DispatchLane::Lifecycle), 4);
        assert!(
            budget.permits_for(DispatchLane::Lifecycle) > 0,
            "a create burst must never be able to starve a renewal"
        );
        assert!(DispatchBudget::new(1, 1, 0).is_none());
    }

    #[test]
    fn a_tightened_budget_is_accepted_and_an_inverted_hierarchy_is_not() {
        // A deployment must be able to tighten these, and a test must be able
        // to prove the enforcement path without manufacturing a 48 MiB
        // payload.
        let tight = TransportBudget::new(
            2,
            4096,
            1024,
            8,
            8192,
            16,
            16_384,
            4,
            8,
            Duration::from_secs(1),
        )
        .expect("a tightened budget is legal");
        assert!(tight.batch_fits(2, 4096));
        assert!(!tight.batch_fits(3, 1));
        assert!(!tight.batch_fits(1, 4097));

        // Every zero is refused.
        assert!(
            TransportBudget::new(
                0,
                4096,
                1024,
                8,
                8192,
                16,
                16_384,
                4,
                8,
                Duration::from_secs(1)
            )
            .is_none()
        );
        assert!(
            TransportBudget::new(2, 4096, 1024, 8, 8192, 16, 16_384, 4, 8, Duration::ZERO)
                .is_none()
        );

        // The bounds are a hierarchy: a descriptor fits in a batch, a batch in
        // one query's queue, that queue in the process's.
        assert!(
            TransportBudget::new(
                2,
                1024,
                4096,
                8,
                8192,
                16,
                16_384,
                4,
                8,
                Duration::from_secs(1)
            )
            .is_none(),
            "a descriptor larger than a batch could never be sent"
        );
        assert!(
            TransportBudget::new(
                2,
                8192,
                1024,
                8,
                4096,
                16,
                16_384,
                4,
                8,
                Duration::from_secs(1)
            )
            .is_none(),
            "a batch larger than one query's queue could never be enqueued"
        );
        assert!(
            TransportBudget::new(
                2,
                4096,
                1024,
                8,
                8192,
                16,
                4096,
                4,
                8,
                Duration::from_secs(1)
            )
            .is_none(),
            "one query may not be allowed more than the whole process"
        );
        assert!(
            TransportBudget::new(
                2,
                4096,
                1024,
                8,
                8192,
                16,
                16_384,
                8,
                4,
                Duration::from_secs(1)
            )
            .is_none(),
            "one context may not hold more tasks than the backend allows"
        );

        // The frozen default satisfies its own hierarchy.
        let default = TransportBudget::DEFAULT;
        assert!(
            TransportBudget::new(
                default.max_batch_items(),
                default.max_batch_encoded_bytes(),
                default.max_descriptor_encoded_bytes(),
                default.max_query_backend_queued_operations(),
                default.max_query_backend_queued_bytes(),
                default.max_backend_queued_operations(),
                default.max_backend_queued_bytes(),
                default.max_tasks_per_context(),
                default.max_active_tasks_per_backend(),
                default.frontend_queue_residence(),
            )
            .is_some()
        );
    }

    #[test]
    fn transport_budgets_bound_batches_queues_and_task_counts() {
        let budget = TransportBudget::DEFAULT;
        assert_eq!(budget.max_batch_items(), 32);
        assert_eq!(budget.max_batch_encoded_bytes(), 48 * 1024 * 1024);
        assert_eq!(budget.max_descriptor_encoded_bytes(), 16 * 1024 * 1024);
        assert_eq!(budget.max_query_backend_queued_operations(), 4096);
        assert_eq!(budget.max_query_backend_queued_bytes(), 256 * 1024 * 1024);
        assert_eq!(budget.max_backend_queued_operations(), 16384);
        assert_eq!(budget.max_backend_queued_bytes(), 512 * 1024 * 1024);
        assert_eq!(budget.max_tasks_per_context(), 4096);
        assert_eq!(budget.max_active_tasks_per_backend(), 32768);
        assert_eq!(budget.frontend_queue_residence(), Duration::from_secs(15));

        assert!(budget.batch_fits(32, 48 * 1024 * 1024));
        assert!(!budget.batch_fits(33, 1));
        assert!(!budget.batch_fits(1, 48 * 1024 * 1024 + 1));
        assert!(!budget.batch_fits(0, 0), "an empty batch is not a batch");
    }
}

#[cfg(test)]
mod request_tests {
    use super::{
        AbortQueryContext, AdvanceQueryContextDomain, CancelTask, CreateTask, CredentialUpdate,
        EstablishQueryContext, FetchTaskDynamicFilters, GetFinalTaskInfo, OperationKind,
        QueryContextDomainUpdate, ReleaseQueryContext, RenewQueryExecutionLease, RequestError,
        SplitAssignmentIntent, TaskDomainUpdate, UpdateQueryContext, UpdateTask,
    };
    use crate::exec::fragment::program::{FragmentContractVersion, FragmentSinkKind};
    use crate::task_execution::descriptor::{
        ExchangeTopology, PhysicalFragmentPlan, TaskDescriptor,
    };
    use crate::task_execution::domain::{
        CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId,
        SplitSequence, TaskDomainKind,
    };
    use crate::task_execution::identity::{
        IdentityField, IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use crate::task_execution::lease::{LeaseSequence, LeaseValidFor};
    use crate::task_execution::status::{AbortCause, CancelReason};
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
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
            SplitAssignmentIntent::new(node, last, first, false, content()).is_none(),
            "a descending batch is not a batch"
        );
        let intent =
            SplitAssignmentIntent::new(node, first, last, true, content()).expect("legal batch");
        assert_eq!(intent.node(), node);
        assert_eq!(intent.first(), first);
        assert_eq!(intent.last(), last);
        assert!(intent.no_more_splits());
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
