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

//! The frontend task-protocol owners' closed failure set.
//!
//! Every variant is a fail-closed protocol or capacity fact. None of them is
//! a hint to try something weaker: there is no fallback path, no guessed
//! default, and no message-text classification anywhere in this module.

use std::fmt;
use std::time::Duration;

use crate::task_execution::operation::OperationDispatchResult;
use crate::task_execution::status_validation::FinalInfoDisagreement;
use novarocks_coordination_contract::StatusObservation;
use novarocks_execution::task_execution::{
    DescriptorError, DomainConflict, ExchangeEdgeId, IdentityMismatch, OperationKind,
    OperationOutcome, QueryContextRef, RequestError, TaskOperationId, TaskState,
};
use novarocks_types::identity::BackendProcessId;

/// Why a task-status subscription can no longer represent its frozen
/// participant. Identity violations are not transport uncertainty and must
/// never select attempt recovery.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParticipantObservationFailure {
    Transport(&'static str),
    /// Original typed connection loss survived the bounded covered reconnect.
    ConnectionLost,
    IdentityViolation(&'static str),
}

impl ParticipantObservationFailure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transport(state) | Self::IdentityViolation(state) => state,
            Self::ConnectionLost => "connection_lost",
        }
    }
}

/// Why the frontend task-protocol owners refuse to continue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskExecutionError {
    Workload(novarocks_workload_control::WorkError),
    /// A local owner has already projected its own fact and safe detail.
    Execution(novarocks_query_application::execution_error::ExecutionFailureInput),
    SplitAssignment(crate::query_execution::split_assignment::SplitAssignmentDriverError),
    /// The original logical owner refused this operation; preserve its reason
    /// and normal stop observation instead of reconstructing it from text.
    Actor(novarocks_query_application::coordination::LogicalExecutionActorError),
    NativeTransport(crate::native::task_transport::NativeTaskTransportCreationError),
    Establish(novarocks_query_application::coordination::EstablishIssueError),
    /// The static schedule cannot be turned into a legal task graph.
    Schedule(String),
    /// A descriptor built from the static schedule is not a legal value.
    Descriptor(DescriptorError),
    /// A request built from the static schedule is not a legal value.
    Request(RequestError),
    /// A capacity bound of the operation transport was reached.
    Capacity(CapacityBound),
    /// A locally queued operation reached its residence deadline before any
    /// transport accepted it. No Worker receipt is fabricated for this
    /// frontend-owned failure.
    QueueResidenceExpired {
        operation_id: TaskOperationId,
        kind: OperationKind,
        waited: Duration,
    },
    /// Establish for this frozen backend process exhausted its bounded exact
    /// replay authorization while every Worker outcome remained unknown. The
    /// whole-attempt recovery owner must observe whether this exact process
    /// was replaced before deciding a successor schedule.
    PreReadyEstablishTransportUnknown {
        backend: BackendProcessId,
    },
    /// The Worker definitively rejected the initial Establish. The immutable
    /// attempt is unusable, but only the whole-attempt recovery owner may
    /// decide whether independently observed process replacement permits a
    /// successor schedule.
    PreReadyEstablishRejected {
        context: QueryContextRef,
        operation_id: TaskOperationId,
        rejection:
            novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>,
    },
    /// An acknowledgement named an operation this owner never sent, or named
    /// one that is already settled.
    UnknownOperation,
    /// An acknowledgement carried the wrong receipt shape for its operation.
    MissingReceipt(OperationKind),
    /// A Worker receipt did not settle the immutable domain intent that this
    /// query coordinator released.
    DomainReceipt(String),
    /// A receipt addressed a different task, stage, query, or process.
    Identity(IdentityMismatch),
    /// An update would move one of a task's domains backwards.
    /// A domain update this owner refused, naming which domain and the token
    /// it arrived with.
    ///
    /// The three task domains are advanced by unrelated producers, so a
    /// conflict that reports only its kind costs a cluster run just to learn
    /// which producer to look at.
    DomainRegression {
        domain: &'static str,
        token: String,
        conflict: DomainConflict,
    },
    /// An edge-open decision repeats an edge this owner already opened.
    EdgeAlreadyOpened(ExchangeEdgeId),
    /// A published status snapshot cannot be reconciled with what this owner
    /// already holds.
    Observation(StatusObservation),
    /// A task reached a state the protocol does not allow from its current
    /// one.
    IllegalTaskTransition {
        from: TaskState,
        to: TaskState,
    },
    /// A backend answered an operation with an outcome that fails the attempt.
    OperationFailed {
        context: QueryContextRef,
        operation_id: TaskOperationId,
        kind: OperationKind,
        rejection:
            novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>,
    },
    /// A settled local or native-ingress failure has no Worker receipt.
    DispatchRejected {
        kind: OperationKind,
        result: OperationDispatchResult,
    },
    /// A fetched final task info contradicts the terminal status this attempt
    /// already observed.
    FinalInfo(FinalInfoDisagreement),
    /// A backend's status subscription settled somewhere resubscribing cannot
    /// repair, so this attempt has no observation of that backend left.
    ///
    /// This is the transport evidence that decides an attempt whose backend
    /// process is gone. Nothing else in the attempt is obliged to notice one:
    /// a lease renewal that cannot reach the process classifies as a
    /// retryable transport unknown and is retried, an exchange peer fails only
    /// if it happens to have one, and a root task on a surviving backend
    /// simply blocks. Measured before this existed: killing one of three
    /// backends left a distributed SELECT hanging until its statement
    /// deadline whenever the killed process did not host the root task.
    ParticipantUnobservable {
        backend: BackendProcessId,
        state: ParticipantObservationFailure,
    },
}

/// Which transport capacity bound was reached.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CapacityBound {
    /// This query's queued operations for one backend.
    QueryBackendOperations { limit: usize },
    /// This query's queued bytes for one backend.
    QueryBackendBytes { limit: usize },
    /// Queued operations across every backend of this query.
    BackendOperations { limit: usize },
    /// Queued bytes across every backend of this query.
    BackendBytes { limit: usize },
    /// Tasks in one query context.
    TasksPerContext { limit: usize },
    /// Active tasks this query places on one backend.
    ActiveTasksPerBackend {
        backend: BackendProcessId,
        limit: usize,
    },
    /// One descriptor's encoded plan.
    DescriptorBytes { limit: usize, actual: usize },
    /// One operation's queue-side carrier.
    OperationBytes { limit: usize, actual: usize },
    /// Edge-open versions one producer task can mint.
    ///
    /// A task mints one per edge-open decision and never reuses one, so this
    /// bound is the version space itself. It fails closed rather than wrapping
    /// because a wrapped version would claim to be the request that opened a
    /// different edge set.
    EdgeOpenVersions { limit: u32 },
}

#[cfg(test)]
impl CapacityBound {
    pub const fn as_operation_outcome(self) -> OperationOutcome {
        OperationOutcome::ResourceExhausted
    }
}

/// Facts belong to the exact FE bound that refused this request. A numeric
/// limit alone does not reveal the request size or the aggregate live usage.
impl novarocks_execution_contract::failure::FailureProjection for CapacityBound {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, ResourceDimension as D, ResourceFacts,
            ResourceScope as S, domain,
        };
        let (code, request_scope, constraint_scope, dimension, requested, limit) = match *self {
            Self::QueryBackendOperations { limit } => (
                40,
                Some(S::Request),
                Some(S::Query),
                D::Items,
                None,
                u64::try_from(limit).ok(),
            ),
            Self::QueryBackendBytes { limit } => (
                41,
                Some(S::Request),
                Some(S::Query),
                D::Bytes,
                None,
                u64::try_from(limit).ok(),
            ),
            Self::BackendOperations { limit } => (
                42,
                Some(S::Request),
                Some(S::Query),
                D::Items,
                None,
                u64::try_from(limit).ok(),
            ),
            Self::BackendBytes { limit } => (
                43,
                Some(S::Request),
                Some(S::Query),
                D::Bytes,
                None,
                u64::try_from(limit).ok(),
            ),
            // Context and query-on-one-backend cardinalities have no exact
            // ResourceScope word. They are not a Task or Process measurement.
            Self::TasksPerContext { limit } => {
                (44, None, None, D::Items, None, u64::try_from(limit).ok())
            }
            Self::ActiveTasksPerBackend { limit, .. } => {
                (45, None, None, D::Items, None, u64::try_from(limit).ok())
            }
            Self::DescriptorBytes { limit, actual } => (
                46,
                Some(S::Request),
                Some(S::Request),
                D::Bytes,
                u64::try_from(actual).ok(),
                u64::try_from(limit).ok(),
            ),
            Self::OperationBytes { limit, actual } => (
                47,
                Some(S::Request),
                Some(S::Request),
                D::Bytes,
                u64::try_from(actual).ok(),
                u64::try_from(limit).ok(),
            ),
            Self::EdgeOpenVersions { limit } => (
                49,
                Some(S::Task),
                Some(S::Task),
                D::Items,
                None,
                Some(u64::from(limit)),
            ),
        };
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
        .with_resource(ResourceFacts {
            request_scope,
            constraint_scope,
            dimension,
            requested,
            limit,
            required_growth: None,
            live: None,
            committed: None,
        })
    }
}

impl fmt::Display for CapacityBound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryBackendOperations { limit } => write!(
                formatter,
                "queued operations for one backend of this query reached {limit}"
            ),
            Self::QueryBackendBytes { limit } => write!(
                formatter,
                "queued bytes for one backend of this query reached {limit}"
            ),
            Self::BackendOperations { limit } => {
                write!(formatter, "queued operations reached {limit}")
            }
            Self::BackendBytes { limit } => write!(formatter, "queued bytes reached {limit}"),
            Self::TasksPerContext { limit } => {
                write!(formatter, "tasks in one query context reached {limit}")
            }
            Self::ActiveTasksPerBackend { backend, limit } => write!(
                formatter,
                "active tasks on backend {backend} reached {limit}"
            ),
            Self::DescriptorBytes { limit, actual } => write!(
                formatter,
                "descriptor plan is {actual} bytes, limit is {limit}"
            ),
            Self::OperationBytes { limit, actual } => write!(
                formatter,
                "operation queue carrier is {actual} bytes, limit is {limit}"
            ),
            Self::EdgeOpenVersions { limit } => {
                write!(formatter, "edge-open versions for one task reached {limit}")
            }
        }
    }
}

impl fmt::Display for TaskExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution(input) => formatter.write_str(input.detail().as_str()),
            Self::SplitAssignment(error) => write!(formatter, "split assignment failed: {error}"),
            Self::Workload(error) => std::fmt::Display::fmt(error, formatter),
            Self::NativeTransport(error) => write!(
                formatter,
                "native Task transport construction failed: {error}"
            ),
            Self::Actor(error) => write!(
                formatter,
                "logical execution actor refused Task protocol work: {error}"
            ),
            Self::Establish(error) => write!(
                formatter,
                "logical execution actor Establish gate failed: {error}"
            ),
            Self::Schedule(detail) => write!(
                formatter,
                "static schedule is not schedulable as a task graph: {detail}"
            ),
            Self::Descriptor(error) => {
                write!(formatter, "task descriptor is not a legal value: {error}")
            }
            Self::Request(error) => write!(
                formatter,
                "task protocol request is not a legal value: {error}"
            ),
            Self::Capacity(bound) => write!(formatter, "task protocol capacity bound: {bound}"),
            Self::QueueResidenceExpired {
                operation_id,
                kind,
                waited,
            } => write!(
                formatter,
                "{kind} operation {operation_id} waited {waited:?} in the frontend queue and expired before transport acceptance"
            ),
            Self::PreReadyEstablishTransportUnknown { backend } => write!(
                formatter,
                "EstablishQueryContext for backend {backend} exhausted its exact replay budget with unknown Worker outcomes"
            ),
            Self::PreReadyEstablishRejected {
                context,
                operation_id,
                rejection,
            } => write!(
                formatter,
                "EstablishQueryContext {operation_id} for context {context:?} was rejected before ControlReady with {:?}",
                rejection.outcome()
            ),
            Self::UnknownOperation => {
                formatter.write_str("acknowledgement names an operation this owner did not send")
            }
            Self::MissingReceipt(kind) => {
                write!(formatter, "{kind} was accepted without its receipt")
            }
            Self::DomainReceipt(detail) => {
                write!(
                    formatter,
                    "task domain receipt does not settle its intent: {detail}"
                )
            }
            Self::Identity(mismatch) => write!(formatter, "task protocol receipt {mismatch}"),
            Self::DomainRegression {
                domain,
                token,
                conflict,
            } => {
                write!(
                    formatter,
                    "task domain update is not a progression: {conflict} (domain={domain} {token})"
                )
            }
            Self::EdgeAlreadyOpened(edge) => {
                write!(formatter, "exchange edge {edge} was already opened")
            }
            Self::Observation(observation) => {
                write!(formatter, "published status is not usable: {observation:?}")
            }
            Self::IllegalTaskTransition { from, to } => {
                write!(formatter, "task state {from} may not become {to}")
            }
            Self::OperationFailed {
                kind, rejection, ..
            } => match rejection.detail() {
                Some(detail) => write!(formatter, "{kind} failed closed: {detail}"),
                None => write!(
                    formatter,
                    "{kind} failed closed with {:?}",
                    rejection.outcome()
                ),
            },
            Self::DispatchRejected { kind, result } => {
                write!(
                    formatter,
                    "{kind} failed before a Worker receipt: {result:?}"
                )
            }
            Self::FinalInfo(disagreement) => write!(formatter, "{disagreement}"),
            Self::ParticipantUnobservable { backend, state } => write!(
                formatter,
                "backend {backend} is no longer observable: task status subscription {}",
                state.as_str()
            ),
        }
    }
}

impl std::error::Error for TaskExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Workload(error) => Some(error),
            Self::NativeTransport(error) => Some(error),
            Self::Actor(error) => Some(error),
            Self::Establish(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DescriptorError> for TaskExecutionError {
    fn from(error: DescriptorError) -> Self {
        Self::Descriptor(error)
    }
}

impl From<RequestError> for TaskExecutionError {
    fn from(error: RequestError) -> Self {
        Self::Request(error)
    }
}

impl From<IdentityMismatch> for TaskExecutionError {
    fn from(error: IdentityMismatch) -> Self {
        Self::Identity(error)
    }
}

impl From<CapacityBound> for TaskExecutionError {
    fn from(bound: CapacityBound) -> Self {
        Self::Capacity(bound)
    }
}

/// FE-authored facts for ordinary Worker protocol verdicts. An originating
/// DomainFailure bypasses this projection and keeps its exact producer fact.
/// QUERY reasons 1..=13 and 27 belong to QueryFailureReason. This closed FE
/// protocol projection owns 14..=26; 28..=32 remain unassigned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskProtocolFailureReason {
    OperationTimedOut,
    IdentityMismatch,
    CompatibilityMismatch,
    ContextNotEstablished,
    ContextConflict,
    DomainConflict,
    LeaseExpired,
    ContextTerminalReceipt,
    InvalidStateOrRequest,
    TerminalRejected,
    Gone,
    ResourceExhausted,
    NonterminalVerdictAtFailure,
}
impl TaskProtocolFailureReason {
    fn terminal_verdict(outcome: OperationOutcome) -> Self {
        match outcome {
            OperationOutcome::OperationTimedOut => Self::OperationTimedOut,
            OperationOutcome::IdentityMismatch => Self::IdentityMismatch,
            OperationOutcome::CompatibilityMismatch => Self::CompatibilityMismatch,
            OperationOutcome::ContextNotEstablished => Self::ContextNotEstablished,
            OperationOutcome::ContextConflict => Self::ContextConflict,
            OperationOutcome::DomainConflict => Self::DomainConflict,
            OperationOutcome::LeaseExpired => Self::LeaseExpired,
            OperationOutcome::ContextTerminalReceipt => Self::ContextTerminalReceipt,
            OperationOutcome::InvalidStateOrRequest => Self::InvalidStateOrRequest,
            OperationOutcome::TerminalRejected => Self::TerminalRejected,
            OperationOutcome::Gone => Self::Gone,
            OperationOutcome::ResourceExhausted => Self::ResourceExhausted,
            // These are success/wait facts, not failure authors. Reaching a
            // terminal-only error branch is a local protocol invariant;
            // it cannot manufacture Resource, Task acceptance or BackendLost.
            OperationOutcome::Accepted
            | OperationOutcome::Idempotent
            | OperationOutcome::ReleaseNotReady
            | OperationOutcome::NotReady
            | OperationOutcome::PreparationBusy
            | OperationOutcome::AdmissionTicketStillActive => Self::NonterminalVerdictAtFailure,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for TaskProtocolFailureReason {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code) = match self {
            Self::OperationTimedOut => (FailureClass::Dependency, 14),
            Self::IdentityMismatch => return FailureFact::new(
                FailureClass::Invariant,
                novarocks_query_application::execution_error::TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
            ),
            Self::CompatibilityMismatch => (FailureClass::Invariant, 16),
            Self::ContextNotEstablished => (FailureClass::Invariant, 17),
            Self::ContextConflict => (FailureClass::Invariant, 18),
            Self::DomainConflict => (FailureClass::Invariant, 19),
            Self::LeaseExpired => (FailureClass::Control, 20),
            Self::ContextTerminalReceipt => (FailureClass::Control, 21),
            Self::InvalidStateOrRequest => (FailureClass::Invariant, 22),
            Self::TerminalRejected => (FailureClass::Control, 23),
            Self::Gone => (FailureClass::Control, 24),
            Self::ResourceExhausted => (FailureClass::Resource, 25),
            Self::NonterminalVerdictAtFailure => (FailureClass::Invariant, 26),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// The same protocol reason author preserves a locally refused operation's
/// fact without claiming that a Worker sent a receipt.
pub(crate) fn local_protocol_refusal(
    kind: OperationKind,
    outcome: OperationOutcome,
) -> novarocks_query_application::execution_error::ExecutionFailureInput {
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    ExecutionFailureInput::new(
        TaskProtocolFailureReason::terminal_verdict(outcome).failure_fact(),
        SafeDetail::truncating(&format!("{kind} returned {outcome:?}")),
        ExecutionFailureSite::Coordinator,
    )
}

/// Project only a settled local dispatch refusal; identity, Worker and
/// cleanup failures keep their original owners and typed branches.
pub(crate) fn dispatch_rejection(
    acknowledgement: &crate::task_execution::intent::OperationAcknowledgement,
    kind: OperationKind,
) -> TaskExecutionError {
    if let Some(input) = acknowledgement.local_failure() {
        return TaskExecutionError::Execution(input.clone());
    }
    if acknowledgement.dispatch_result() == OperationDispatchResult::NonWorkerRejected
        && let Some(cause) = acknowledgement.governance_failure()
    {
        return TaskExecutionError::Workload(cause.clone());
    }
    TaskExecutionError::DispatchRejected {
        kind,
        result: acknowledgement.dispatch_result(),
    }
}

impl TaskExecutionError {
    /// Boundary projection, not a second Task conclusion or a recovery gate.
    pub(crate) fn preparation_input(
        &self,
    ) -> novarocks_query_application::execution_error::ExecutionFailureInput {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_execution_contract::{SafeDetail, operation::OperationResult};
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        match self {
            Self::Workload(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    // WorkError contains only producer-defined scalar facts,
                    // not Native payloads or arbitrary exception messages.
                    SafeDetail::truncating_display(format_args!(
                        "Native workload admission failed: {error}"
                    )),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::Execution(input) => return input.clone(),
            Self::SplitAssignment(error) => {
                return error.execution_failure().unwrap_or_else(|| {
                    ExecutionFailureInput::new(
                        QueryFailureReason::SplitAssignmentFailed.failure_fact(),
                        SafeDetail::truncating(
                            "split assignment could not deliver the frozen input",
                        ),
                        ExecutionFailureSite::Coordinator,
                    )
                });
            }
            Self::NativeTransport(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    error.safe_detail(),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::Actor(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    error.safe_detail(),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::Establish(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    error.safe_detail(),
                    ExecutionFailureSite::Coordinator,
                );
            }
            _ => {}
        }
        let operation = match self {
            Self::OperationFailed {
                context,
                operation_id,
                kind,
                rejection,
            } => Some((*context, *operation_id, *kind, rejection)),
            Self::PreReadyEstablishRejected {
                context,
                operation_id,
                rejection,
            } => Some((
                *context,
                *operation_id,
                OperationKind::UpdateQueryContext,
                rejection,
            )),
            _ => None,
        };
        if let Some((context, operation_id, kind, rejection)) = operation {
            let site = ExecutionFailureSite::Operation {
                context,
                operation_id,
                kind,
            };
            return match rejection {
                OperationResult::DomainFailure { failure, .. } => {
                    ExecutionFailureInput::new(failure.fact(), failure.detail().clone(), site)
                }
                OperationResult::Verdict {
                    outcome, detail, ..
                } => ExecutionFailureInput::new(
                    TaskProtocolFailureReason::terminal_verdict(*outcome).failure_fact(),
                    detail.clone().unwrap_or_else(|| {
                        SafeDetail::truncating(&format!("{kind} returned {outcome:?}"))
                    }),
                    site,
                ),
            };
        }
        let (reason, site) = match self {
            Self::Capacity(bound) => {
                return ExecutionFailureInput::new(
                    bound.failure_fact(),
                    SafeDetail::truncating("frontend task protocol capacity bound was exceeded"),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::Descriptor(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    SafeDetail::truncating("task descriptor violates its frozen contract"),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::Request(error) => {
                return ExecutionFailureInput::new(
                    error.failure_fact(),
                    SafeDetail::truncating("task operation request violates its frozen contract"),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::DispatchRejected {
                result: OperationDispatchResult::IngressRejected(_),
                ..
            } => (
                QueryFailureReason::TransportCapacity,
                ExecutionFailureSite::Coordinator,
            ),
            Self::QueueResidenceExpired { .. } => (
                QueryFailureReason::TransportDeadline,
                ExecutionFailureSite::Coordinator,
            ),
            Self::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::ConnectionLost,
            } => (
                QueryFailureReason::BackendLost,
                ExecutionFailureSite::Backend { process: *backend },
            ),
            Self::PreReadyEstablishTransportUnknown { backend }
            | Self::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::Transport(_),
            } => (
                QueryFailureReason::ObservationLost,
                ExecutionFailureSite::Backend { process: *backend },
            ),
            Self::DispatchRejected {
                result: OperationDispatchResult::TransportUnknown,
                ..
            } => (
                QueryFailureReason::ObservationLost,
                ExecutionFailureSite::Coordinator,
            ),
            Self::DispatchRejected {
                result: OperationDispatchResult::WorkerReceipt(receipt),
                ..
            } => {
                return ExecutionFailureInput::new(
                    TaskProtocolFailureReason::terminal_verdict(receipt.outcome()).failure_fact(),
                    SafeDetail::truncating("frontend task protocol could not continue"),
                    ExecutionFailureSite::Coordinator,
                );
            }
            Self::DomainRegression { conflict, .. } => {
                return ExecutionFailureInput::new(
                    conflict.failure_fact(),
                    SafeDetail::truncating("frontend task protocol could not continue"),
                    ExecutionFailureSite::Coordinator,
                );
            }
            _ => (
                QueryFailureReason::PreparationProtocol,
                ExecutionFailureSite::Coordinator,
            ),
        };
        ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating("frontend task protocol could not continue"),
            site,
        )
    }
}

#[cfg(test)]
mod canonical_input_tests {
    use super::*;
    use novarocks_execution_contract::failure::FailureProjection;
    use novarocks_execution_contract::{
        SafeDetail,
        failure::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts,
            ResourceScope, domain,
        },
        operation::{DomainFailureOutcome, OperationDomainFailure, OperationResult},
    };
    use novarocks_query_application::execution_error::{ExecutionFailureSite, QueryFailureReason};
    fn context() -> QueryContextRef {
        use novarocks_types::{AttemptId, FrontendProcessId, QueryExecutionId, QueryId};
        QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn split_source_failures_preserve_the_original_neutral_input_including_resource_facts() {
        use crate::query_execution::split_assignment::{ScanNodeKey, SplitAssignmentDriverError};
        use novarocks_query_application::api::NativeAttemptPreparationFailure;
        use novarocks_query_application::execution_error::ExecutionFailureInput;

        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(30).unwrap()),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Process),
            dimension: ResourceDimension::Positions,
            requested: Some(2),
            required_growth: None,
            limit: Some(1),
            live: Some(1),
            committed: None,
        });
        let detail = SafeDetail::truncating("original split source failure fact");
        let original =
            ExecutionFailureInput::new(fact, detail.clone(), ExecutionFailureSite::Coordinator);
        for split_error in [
            SplitAssignmentDriverError::SourceClose {
                scan: ScanNodeKey::new(1, 7),
                fact,
                detail: detail.clone(),
            },
            SplitAssignmentDriverError::SourceEnumeration {
                scan: ScanNodeKey::new(1, 7),
                fact,
                detail: detail.clone(),
            },
        ] {
            let projected = TaskExecutionError::SplitAssignment(split_error).preparation_input();
            assert_eq!(projected, original);
            assert_ne!(
                projected.fact(),
                QueryFailureReason::SplitAssignmentFailed.failure_fact()
            );
            let failure = NativeAttemptPreparationFailure::from_input(projected);
            assert_eq!(failure.input(), &original);
            assert_eq!(failure.error().execution_failure(), Some(&original));
            assert_eq!(
                failure.class(),
                novarocks_coordination_contract::AttemptFailureClass::ResourceGovernance
            );
            assert_eq!(
                failure.topology_requirement(),
                novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
            );
        }
    }

    #[test]
    fn plain_split_failures_are_distinct_nonrecoverable_facts_with_fixed_safe_detail() {
        use crate::query_execution::connector_domain::SplitAssignmentError;
        use crate::query_execution::split_assignment::{
            AssignmentTarget, ScanNodeKey, SplitAssignmentDriverError,
        };
        use novarocks_query_application::api::NativeAttemptPreparationFailure;

        let scan = ScanNodeKey::new(1, 7);
        let target = AssignmentTarget {
            backend_idx: 0,
            fragment_instance_id: novarocks_types::UniqueId::new(11, 12),
        };
        let raw = "BackendLost: private provider token must never be a recovery signal";
        for error in [
            SplitAssignmentDriverError::Closed,
            SplitAssignmentDriverError::DeliveryInProgress,
            SplitAssignmentDriverError::NoAdmittedTask { scan },
            SplitAssignmentDriverError::Assignment(SplitAssignmentError::AlreadyTerminal {
                plan_node_id: 7,
            }),
            SplitAssignmentDriverError::Transport {
                target: target.clone(),
                detail: raw.to_owned(),
            },
            SplitAssignmentDriverError::Rejected {
                target,
                reason: raw.to_owned(),
                detail: raw.to_owned(),
            },
            SplitAssignmentDriverError::SplitSource {
                scan: Some(scan),
                detail: raw.to_owned(),
            },
        ] {
            let input = TaskExecutionError::SplitAssignment(error).preparation_input();
            assert_eq!(
                input.fact(),
                QueryFailureReason::SplitAssignmentFailed.failure_fact()
            );
            assert_eq!(input.fact().reason().code(), 176);
            assert_ne!(
                input.fact().reason(),
                QueryFailureReason::BackendLost.failure_fact().reason()
            );
            assert_eq!(
                input.detail().as_str(),
                "split assignment could not deliver the frozen input"
            );
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            let failure = NativeAttemptPreparationFailure::from_input(input.clone());
            assert_eq!(
                failure.class(),
                novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure
            );
            assert_eq!(
                failure.topology_requirement(),
                novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
            );
            assert_eq!(failure.error().execution_failure(), Some(&input));
            assert!(!failure.error().message().contains(raw));
        }
    }

    #[test]
    fn domain_operation_keeps_original_fact_detail_and_exact_operation_identity() {
        let context = context();
        let operation_id = TaskOperationId::new_v7();
        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(30).unwrap()),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Task),
            constraint_scope: Some(ResourceScope::Process),
            dimension: ResourceDimension::Positions,
            requested: Some(4),
            required_growth: None,
            limit: Some(3),
            live: Some(3),
            committed: None,
        });
        let detail = SafeDetail::truncating("original Worker failure 申请位置");
        let input = TaskExecutionError::OperationFailed {
            context,
            operation_id,
            kind: OperationKind::CreateTask,
            rejection: OperationResult::DomainFailure {
                outcome: DomainFailureOutcome::InvalidStateOrRequest,
                failure: OperationDomainFailure::new(fact, detail.clone()),
            },
        }
        .preparation_input();
        assert_eq!(input.fact(), fact);
        assert_eq!(input.detail(), &detail);
        assert_eq!(
            input.site(),
            ExecutionFailureSite::Operation {
                context,
                operation_id,
                kind: OperationKind::CreateTask
            }
        );
    }
    #[test]
    fn establish_rejection_keeps_request_context_and_operation_without_fake_task_phase() {
        let context = context();
        let operation_id = TaskOperationId::new_v7();
        let input = TaskExecutionError::PreReadyEstablishRejected {
            context,
            operation_id,
            rejection: OperationResult::Verdict {
                outcome: OperationOutcome::IdentityMismatch,
                detail: None,
                acknowledgement: None,
            },
        }
        .preparation_input();
        assert_eq!(
            input.site(),
            ExecutionFailureSite::Operation {
                context,
                operation_id,
                kind: OperationKind::UpdateQueryContext
            }
        );
        assert_eq!(input.fact().reason().code(), 15);
        assert_eq!(
            input.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }
    #[test]
    fn ordinary_resource_verdict_is_a_protocol_fact_without_fabricated_resource_measurements() {
        let input = TaskExecutionError::OperationFailed {
            context: context(),
            operation_id: TaskOperationId::new_v7(),
            kind: OperationKind::CreateTask,
            rejection: OperationResult::Verdict {
                outcome: OperationOutcome::ResourceExhausted,
                detail: Some(SafeDetail::truncating("capacity")),
                acknowledgement: None,
            },
        }
        .preparation_input();
        assert_eq!(input.fact().class(), FailureClass::Resource);
        assert_eq!(input.fact().reason().code(), 25);
        assert!(input.fact().resource().is_none());
    }
    #[test]
    fn wait_verdict_in_a_terminal_error_branch_is_an_invariant_not_a_resource_failure() {
        for outcome in [
            OperationOutcome::NotReady,
            OperationOutcome::PreparationBusy,
            OperationOutcome::ReleaseNotReady,
            OperationOutcome::AdmissionTicketStillActive,
        ] {
            let input = TaskExecutionError::OperationFailed {
                context: context(),
                operation_id: TaskOperationId::new_v7(),
                kind: OperationKind::CreateTask,
                rejection: OperationResult::Verdict {
                    outcome,
                    detail: None,
                    acknowledgement: None,
                },
            }
            .preparation_input();
            assert_eq!(input.fact().class(), FailureClass::Invariant);
            assert_eq!(input.fact().reason().code(), 26);
            assert!(input.fact().resource().is_none());
        }
    }
    #[test]
    fn only_original_covered_connection_loss_requests_exact_process_recovery() {
        use novarocks_coordination_contract::AttemptFailureClass;
        use novarocks_query_application::api::NativeAttemptTopologyRequirement;
        let backend = BackendProcessId::new_v7();
        let actual = TaskExecutionError::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::ConnectionLost,
        }
        .preparation_input();
        assert_eq!(
            actual.fact(),
            QueryFailureReason::BackendLost.failure_fact()
        );
        assert_eq!(
            actual.site(),
            ExecutionFailureSite::Backend { process: backend }
        );
        assert_eq!(
            actual.attempt_class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            actual.topology_requirement(),
            NativeAttemptTopologyRequirement::ExcludeProcess(backend)
        );
        for state in ["budget_exhausted", "rejected", "connection_lost"] {
            let unknown = TaskExecutionError::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::Transport(state),
            }
            .preparation_input();
            assert_eq!(
                unknown.fact(),
                QueryFailureReason::ObservationLost.failure_fact()
            );
            assert_eq!(
                unknown.attempt_class(),
                AttemptFailureClass::ExecutionFailure
            );
            assert_eq!(
                unknown.topology_requirement(),
                NativeAttemptTopologyRequirement::LiveSnapshot
            );
        }
    }

    #[test]
    fn unknown_establish_and_expired_local_queue_do_not_invent_backend_loss() {
        let backend = BackendProcessId::new_v7();
        let unknown =
            TaskExecutionError::PreReadyEstablishTransportUnknown { backend }.preparation_input();
        assert_eq!(
            unknown.fact(),
            QueryFailureReason::ObservationLost.failure_fact()
        );
        assert_eq!(
            unknown.site(),
            ExecutionFailureSite::Backend { process: backend }
        );
        let expired = TaskExecutionError::QueueResidenceExpired {
            operation_id: TaskOperationId::new_v7(),
            kind: OperationKind::CreateTask,
            waited: Duration::from_millis(5),
        }
        .preparation_input();
        assert_eq!(
            expired.fact(),
            QueryFailureReason::TransportDeadline.failure_fact()
        );
        assert_ne!(
            unknown.fact().reason(),
            QueryFailureReason::BackendLost.failure_fact().reason()
        );
        assert_ne!(
            expired.fact().reason(),
            QueryFailureReason::BackendLost.failure_fact().reason()
        );
    }
    #[test]
    fn existing_typed_domain_conflict_keeps_its_own_projection() {
        let conflict = DomainConflict::AfterSeal;
        let input = TaskExecutionError::DomainRegression {
            domain: "splits",
            token: "7".to_owned(),
            conflict,
        }
        .preparation_input();
        assert_eq!(input.fact(), conflict.failure_fact());
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    }
}

#[cfg(test)]
mod capacity_projection_tests {
    use super::*;
    use novarocks_execution_contract::failure::{
        FailureClass, FailureProjection, ResourceDimension, ResourceScope,
    };
    #[test]
    fn measured_request_bytes_and_known_zero_survive_the_canonical_boundary() {
        for bound in [
            CapacityBound::DescriptorBytes {
                limit: 0,
                actual: 0,
            },
            CapacityBound::OperationBytes {
                limit: 7,
                actual: 11,
            },
        ] {
            let original = bound.failure_fact();
            let input = TaskExecutionError::Capacity(bound).preparation_input();
            assert_eq!(input.fact(), original);
            let facts = input.fact().resource().unwrap();
            assert_eq!(facts.dimension, ResourceDimension::Bytes);
            let (limit, actual) = match bound {
                CapacityBound::DescriptorBytes { limit, actual }
                | CapacityBound::OperationBytes { limit, actual } => (limit, actual),
                _ => unreachable!(),
            };
            assert_eq!(facts.limit, Some(limit as u64));
            assert_eq!(facts.requested, Some(actual as u64));
            assert_eq!(facts.required_growth, None);
            assert_eq!(facts.live, None);
            assert_eq!(facts.committed, None);
            assert_eq!(input.fact().class(), FailureClass::Resource);
        }
    }
    #[test]
    fn cardinality_bounds_do_not_invent_context_task_scope_or_request_usage() {
        for bound in [
            CapacityBound::TasksPerContext { limit: 0 },
            CapacityBound::ActiveTasksPerBackend {
                backend: BackendProcessId::new_v7(),
                limit: usize::MAX,
            },
        ] {
            let facts = bound.failure_fact().resource().unwrap();
            assert_eq!(facts.dimension, ResourceDimension::Items);
            assert_eq!(facts.request_scope, None);
            assert_eq!(facts.constraint_scope, None);
            assert_eq!(facts.requested, None);
            assert_eq!(facts.live, None);
            assert_eq!(facts.required_growth, None);
            assert_eq!(facts.committed, None);
            assert!(facts.limit.is_some());
        }
        let queue = CapacityBound::QueryBackendOperations { limit: 7 }
            .failure_fact()
            .resource()
            .unwrap();
        assert_eq!(queue.constraint_scope, Some(ResourceScope::Query));
        assert_eq!(queue.request_scope, Some(ResourceScope::Request));
        assert_eq!(queue.requested, None);
        assert_eq!(queue.limit, Some(7));
    }

    #[test]
    fn descriptor_and_request_boundaries_delegate_the_original_owner_fact() {
        use novarocks_execution_contract::failure::FailureProjection;
        let descriptor = DescriptorError::SenderOrdinalOutOfRange {
            ordinal: 1,
            count: 1,
        };
        assert_eq!(
            TaskExecutionError::Descriptor(descriptor.clone())
                .preparation_input()
                .fact(),
            descriptor.failure_fact()
        );
        let request = RequestError::EmptyEdgeSet;
        assert_eq!(
            TaskExecutionError::Request(request.clone())
                .preparation_input()
                .fact(),
            request.failure_fact()
        );
    }
}

#[cfg(test)]
mod actor_gate_failure_carry_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
    #[test]
    fn an_expired_actor_activation_keeps_its_original_stop_fact() {
        let original =
            novarocks_query_application::coordination::LogicalExecutionActorError::WrongPhase;
        let error = TaskExecutionError::Actor(original);
        let input = error.preparation_input();
        assert_eq!(input.fact(), original.failure_fact());
        assert_eq!(input.detail(), &original.safe_detail());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(
            input.attempt_class(),
            novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure
        );
    }
}
