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

//! The port a driver uses to deliver one task update.
//!
//! Keeping this a port rather than a concrete client is what lets the driver
//! be tested for ack, backpressure, terminal, and cancellation behavior without
//! a live backend, and keeps native transport ownership with the coordinator.

use std::fmt;

use super::driver::AssignmentTarget;
use super::driver::SplitAssignmentStop;
use crate::query_execution::connector_domain::TaskUpdateRequest;
use novarocks_proto_codec::lifecycle::QueryExecutionId;

/// What one backend reported for one plan node after an update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AcceptedPlanNode {
    pub(crate) plan_node_id: i32,
    pub(crate) accepted_through_sequence: u64,
    pub(crate) no_more_splits: bool,
    /// Splits still queued on the task. A driver uses this for backpressure;
    /// it is an observation, never an instruction.
    pub(crate) queued_splits: u64,
}

/// The outcome of one delivered task update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TaskUpdateOutcome {
    Accepted(Vec<AcceptedPlanNode>),
    /// The task refused the update. The driver must not retry the same content
    /// blindly: a rejection is a decision about this attempt, not a hiccup.
    Rejected {
        reason: String,
        detail: String,
    },
    /// The destination finished before this update reached it, so there is
    /// nothing left to deliver there.
    ///
    /// Neither accepted nor refused: no split was enqueued, and the backend
    /// did not judge the request -- it had already stopped consuming. The
    /// sender must stop delivering to this destination and keep serving the
    /// others rather than failing the round, because the frontend held a
    /// status older than that terminal by construction and could not have
    /// known.
    DestinationFinished {
        detail: String,
    },
}

/// One transport-owned immutable delivery.
///
/// The caller keeps its request and asks the same transport to observe this
/// token. A retry of an unknown outcome must attach to this same delivery,
/// never allocate a second sequence or enumerate another batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaskUpdateTicket(pub(crate) u64);

/// Whether a TaskUpdate failure has an unknown remote outcome and may be
/// retried with the exact same immutable request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskUpdateTransportErrorKind {
    /// The unary RPC may have reached the backend, but its terminal outcome
    /// was not observed by the frontend.
    RetryableNetwork,
    /// The request, client construction, or received response was invalid for
    /// this attempt. Retrying it would not establish what the backend did.
    Fatal,
    /// The split-assignment round was stopped locally.
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TaskUpdateTransportError {
    kind: TaskUpdateTransportErrorKind,
    detail: String,
}

impl TaskUpdateTransportError {
    /// Compatibility constructor for local callers that have no retryable
    /// transport evidence. Such failures must fail closed.
    pub(crate) fn new(detail: impl Into<String>) -> Self {
        Self::fatal(detail)
    }

    pub(crate) fn retryable_network(detail: impl Into<String>) -> Self {
        Self {
            kind: TaskUpdateTransportErrorKind::RetryableNetwork,
            detail: detail.into(),
        }
    }

    pub(crate) fn fatal(detail: impl Into<String>) -> Self {
        Self {
            kind: TaskUpdateTransportErrorKind::Fatal,
            detail: detail.into(),
        }
    }

    /// Constructed by the driver when its round stop has won the race with an
    /// in-flight delivery. The transport itself never guesses local stop from
    /// a gRPC status.
    pub(crate) fn closed(detail: impl Into<String>) -> Self {
        Self {
            kind: TaskUpdateTransportErrorKind::Closed,
            detail: detail.into(),
        }
    }

    pub(crate) const fn kind(&self) -> TaskUpdateTransportErrorKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for TaskUpdateTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for TaskUpdateTransportError {}

pub(crate) trait TaskUpdateTransport: Send + Sync {
    /// Opens or reattaches to one exact immutable request without waiting for
    /// its acknowledgement.
    fn begin(
        &self,
        execution_id: QueryExecutionId,
        target: &AssignmentTarget,
        request: &TaskUpdateRequest,
    ) -> Result<TaskUpdateTicket, TaskUpdateTransportError>;

    /// Observes a delivery. `None` means it is still owned by the substrate.
    fn poll(
        &self,
        ticket: TaskUpdateTicket,
        stop: &SplitAssignmentStop,
    ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{AttemptId, QueryId, UniqueId};

    struct RecordingTransport {
        observed_request_addresses: std::sync::Mutex<Vec<usize>>,
    }

    impl TaskUpdateTransport for RecordingTransport {
        fn begin(
            &self,
            _execution_id: QueryExecutionId,
            _target: &AssignmentTarget,
            request: &TaskUpdateRequest,
        ) -> Result<TaskUpdateTicket, TaskUpdateTransportError> {
            self.observed_request_addresses
                .lock()
                .expect("recording transport lock")
                .push(std::ptr::from_ref(request).addr());
            Ok(TaskUpdateTicket(1))
        }

        fn poll(
            &self,
            _ticket: TaskUpdateTicket,
            _stop: &SplitAssignmentStop,
        ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
            Some(Ok(TaskUpdateOutcome::Accepted(Vec::new())))
        }
    }

    #[test]
    fn unclassified_transport_failure_is_fatal() {
        let error = TaskUpdateTransportError::new("local encoding failed");
        assert_eq!(error.kind(), TaskUpdateTransportErrorKind::Fatal);
        assert_eq!(error.detail(), "local encoding failed");
    }

    #[test]
    fn error_kind_preserves_retry_and_round_stop_meaning() {
        assert_eq!(
            TaskUpdateTransportError::retryable_network("terminal ack lost").kind(),
            TaskUpdateTransportErrorKind::RetryableNetwork
        );
        assert_eq!(
            TaskUpdateTransportError::closed("round cancelled").kind(),
            TaskUpdateTransportErrorKind::Closed
        );
    }

    #[test]
    fn transport_can_receive_the_same_immutable_request_more_than_once() {
        let transport = RecordingTransport {
            observed_request_addresses: std::sync::Mutex::new(Vec::new()),
        };
        let execution_id = QueryExecutionId::new(
            QueryId::new(3, 4),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        let target = AssignmentTarget {
            backend_idx: 0,
            fragment_instance_id: UniqueId::new(5, 6),
        };
        let request = TaskUpdateRequest::new(target.fragment_instance_id, Vec::new());
        transport
            .begin(execution_id, &target, &request)
            .expect("first borrowed send");
        transport
            .begin(execution_id, &target, &request)
            .expect("second borrowed send");

        assert_eq!(
            *transport
                .observed_request_addresses
                .lock()
                .expect("recording transport lock"),
            vec![std::ptr::from_ref(&request).addr(); 2]
        );
    }
}
