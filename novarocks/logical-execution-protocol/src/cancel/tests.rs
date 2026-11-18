// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_coordination_contract::{
    AttemptFailureClass, ExecutionFailureSite, QueryExecutionErrorKind,
};

#[test]
fn all_scope_reasons_preserve_the_four_existing_consequence_classes() {
    use LogicalCancellationReason as R;
    for (reason, conclusion, stop, kind, class) in [
        (
            R::DeadlineExceeded,
            LogicalConclusion::Failed,
            AttemptStopReason::Deadline,
            QueryExecutionErrorKind::DeadlineExceeded,
            AttemptFailureClass::DeadlineExceeded,
        ),
        (
            R::FrontendDrainDeadlineExceeded,
            LogicalConclusion::Failed,
            AttemptStopReason::DrainDeadline,
            QueryExecutionErrorKind::DeadlineExceeded,
            AttemptFailureClass::DeadlineExceeded,
        ),
        (
            R::ServerShutdown,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Shutdown,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::OwnerDropped,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Shutdown,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::Requested,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::ClientCancelRequested,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::ClientDisconnected,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::ExplicitKill,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
        (
            R::ExplicitKillConnection,
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::Cancelled,
        ),
    ] {
        let decision = cancellation_decision(reason);
        assert_eq!(
            (decision.conclusion, decision.stop, decision.failure.kind()),
            (conclusion, stop, kind)
        );
        let failure = decision
            .failure
            .execution_failure()
            .expect("cancellation retains its original typed fact");
        assert_eq!(failure.attempt_class(), class);
        assert_eq!(failure.site(), ExecutionFailureSite::Coordinator);
    }
}

#[test]
fn cancellation_detail_has_one_author_and_preserves_the_drain_identity() {
    assert_eq!(
        cancellation_decision(LogicalCancellationReason::DeadlineExceeded)
            .failure
            .message(),
        "logical execution deadline expired before success EOF"
    );
    assert_eq!(
        cancellation_decision(LogicalCancellationReason::FrontendDrainDeadlineExceeded)
            .failure
            .message(),
        "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded"
    );
    assert_eq!(
        cancellation_decision(LogicalCancellationReason::ClientCancelRequested)
            .failure
            .message(),
        "logical execution was cancelled before success EOF"
    );
}
