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

#[cfg(test)]
use novarocks_execution_contract::OperationOutcome;
use std::fmt;
use std::time::Duration;

use crate::operation::OperationDispatchResult;
use crate::status_validation::FinalInfoDisagreement;
use novarocks_coordination_contract::StatusObservation;
use novarocks_execution_contract::{
    DescriptorError, DomainConflict, ExchangeEdgeId, IdentityMismatch, OperationKind,
    ParticipationRef, RequestError, TaskIdentity, TaskOperationId, TaskState,
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
    Workload(crate::failure::HostProjectedFailure),
    HostExecution(crate::failure::HostProjectedFailure),
    /// A local owner has already projected its own fact and safe detail.
    Execution(crate::failure::ProtocolFailure),
    SplitAssignment(crate::failure::HostProjectedFailure),
    /// The original logical owner refused this operation; preserve its reason
    /// and normal stop observation instead of reconstructing it from text.
    Actor(crate::failure::HostProjectedFailure),
    NativeTransport(crate::failure::HostProjectedFailure),
    Establish(crate::failure::HostProjectedFailure),
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
        participation: ParticipationRef,
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
        /// Exact Task owner, when this is a Task operation. Lifecycle failures
        /// have no Task; callers must not reconstruct one by scanning a graph.
        task: Option<TaskIdentity>,
        participation: ParticipationRef,
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
    /// Tasks in one participation.
    TasksPerParticipation { limit: usize },
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
            // Participation and query-on-one-backend cardinalities have no exact
            // ResourceScope word. They are not a Task or Process measurement.
            Self::TasksPerParticipation { limit } => {
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
            Self::TasksPerParticipation { limit } => {
                write!(formatter, "tasks in one participation reached {limit}")
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
            Self::Execution(input) => formatter.write_str(input.detail.as_str()),
            Self::SplitAssignment(error) => write!(formatter, "split assignment failed: {error}"),
            Self::Workload(error) | Self::HostExecution(error) => {
                std::fmt::Display::fmt(error, formatter)
            }
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
                "EstablishParticipation for backend {backend} exhausted its exact replay budget with unknown Worker outcomes"
            ),
            Self::PreReadyEstablishRejected {
                participation,
                operation_id,
                rejection,
            } => write!(
                formatter,
                "EstablishParticipation {operation_id} for participation {participation:?} was rejected before ControlReady with {:?}",
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
            Self::Workload(error) | Self::HostExecution(error) => Some(error),
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

/// Project the already typed local failure without reconstructing its owner.
pub(crate) fn dispatch_rejection(
    acknowledgement: &crate::input::OperationReply,
    kind: OperationKind,
) -> TaskExecutionError {
    if let Some(input) = acknowledgement.local_failure() {
        return TaskExecutionError::Execution(input.clone());
    }
    if let Some(input) = acknowledgement.host_failure() {
        use crate::failure::HostFailureKind as K;
        return match input.kind {
            K::Workload => TaskExecutionError::Workload(*input),
            K::Execution => TaskExecutionError::HostExecution(*input),
            K::SplitAssignment => TaskExecutionError::SplitAssignment(*input),
            K::LogicalActor => TaskExecutionError::Actor(*input),
            K::NativeTransport => TaskExecutionError::NativeTransport(*input),
            K::EstablishAuthorization => TaskExecutionError::Establish(*input),
        };
    }
    TaskExecutionError::DispatchRejected {
        kind,
        result: acknowledgement.dispatch_result(),
    }
}
