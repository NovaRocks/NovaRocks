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

use novarocks_execution::task_execution::{
    DescriptorError, DomainConflict, ExchangeEdgeId, FinalInfoDisagreement, IdentityMismatch,
    OperationKind, OperationOutcome, RequestError, ResultPacketVerdict, StatusObservation,
    TaskState,
};
use novarocks_types::identity::{BackendProcessId, TaskId};

use crate::query_execution::FragmentInstancePlacement;

/// Why the frontend task-protocol owners refuse to continue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskExecutionError {
    /// The static schedule cannot be turned into a legal task graph.
    Schedule(String),
    /// A descriptor built from the static schedule is not a legal value.
    Descriptor(DescriptorError),
    /// A request built from the static schedule is not a legal value.
    Request(RequestError),
    /// A frozen backend process identity is missing for a scheduled backend.
    UnknownBackend { backend_idx: usize },
    /// Two tasks of one query execution derive the same kernel key, so a
    /// destination address would be ambiguous.
    KernelKeyCollision { first: TaskId, second: TaskId },
    /// A capacity bound of the operation transport was reached.
    Capacity(CapacityBound),
    /// An acknowledgement named an operation this owner never sent, or named
    /// one that is already settled.
    UnknownOperation,
    /// An acknowledgement carried the wrong receipt shape for its operation.
    MissingReceipt(OperationKind),
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
    IllegalTaskTransition { from: TaskState, to: TaskState },
    /// A backend answered an operation with an outcome that fails the attempt.
    OperationFailed {
        kind: OperationKind,
        outcome: OperationOutcome,
        /// What the backend said, when it said anything.
        ///
        /// Without it a refusal reaches the client as an outcome name and the
        /// engine's own message -- a CAST field-count mismatch, a bitmap
        /// aggregate's argument rule -- is lost on the way out.
        detail: Option<String>,
    },
    /// The root result stream lost, repeated, or overran a packet, so the
    /// result this frontend holds is not provably the whole result.
    ResultStream(ResultPacketVerdict),
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
        state: &'static str,
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
    /// Edge-open versions one producer task can mint.
    ///
    /// A task mints one per edge-open decision and never reuses one, so this
    /// bound is the version space itself. It fails closed rather than wrapping
    /// because a wrapped version would claim to be the request that opened a
    /// different edge set.
    EdgeOpenVersions { limit: u32 },
}

impl CapacityBound {
    /// Every capacity bound fails closed: there is no older or cheaper path to
    /// degrade onto.
    pub const fn as_operation_outcome(self) -> OperationOutcome {
        OperationOutcome::ResourceExhausted
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
            Self::EdgeOpenVersions { limit } => {
                write!(formatter, "edge-open versions for one task reached {limit}")
            }
        }
    }
}

impl fmt::Display for TaskExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::UnknownBackend { backend_idx } => write!(
                formatter,
                "scheduled backend {backend_idx} has no frozen backend process identity"
            ),
            Self::KernelKeyCollision { first, second } => write!(
                formatter,
                "tasks {first} and {second} derive the same fragment instance id"
            ),
            Self::Capacity(bound) => write!(formatter, "task protocol capacity bound: {bound}"),
            Self::UnknownOperation => {
                formatter.write_str("acknowledgement names an operation this owner did not send")
            }
            Self::MissingReceipt(kind) => {
                write!(formatter, "{kind} was accepted without its receipt")
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
                kind,
                outcome,
                detail,
            } => match detail {
                Some(detail) => write!(formatter, "{kind} failed closed: {detail}"),
                None => write!(formatter, "{kind} failed closed with {outcome:?}"),
            },
            Self::ResultStream(verdict) => {
                write!(formatter, "root result stream is not intact: {verdict}")
            }
            Self::FinalInfo(disagreement) => write!(formatter, "{disagreement}"),
            Self::ParticipantUnobservable { backend, state } => write!(
                formatter,
                "backend {backend} is no longer observable: task status subscription {state}"
            ),
        }
    }
}

impl std::error::Error for TaskExecutionError {}

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

/// A schedule fact that names the placement it came from.
pub(crate) fn schedule_error(
    placement: &FragmentInstancePlacement,
    detail: impl fmt::Display,
) -> TaskExecutionError {
    TaskExecutionError::Schedule(format!(
        "fragment {} instance {}: {detail}",
        placement.fragment_id, placement.instance_index
    ))
}
