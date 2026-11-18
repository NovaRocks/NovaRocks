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

//! Consequences of the original scope latch. The core never reads the latch.

use crate::{failure::query_failure, start::LogicalConclusion};
use novarocks_coordination_contract::{AttemptStopReason, QueryExecutionError, QueryFailureReason};

/// Payload-free projection of all existing workload cancellation reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalCancellationReason {
    DeadlineExceeded,
    FrontendDrainDeadlineExceeded,
    ServerShutdown,
    OwnerDropped,
    Requested,
    ClientCancelRequested,
    ClientDisconnected,
    ExplicitKill,
    ExplicitKillConnection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancellationDecision {
    pub conclusion: LogicalConclusion,
    pub stop: AttemptStopReason,
    pub failure: QueryExecutionError,
}

/// Also used before installation, so cancellation classification has one author.
pub fn cancellation_decision(reason: LogicalCancellationReason) -> CancellationDecision {
    use LogicalCancellationReason as R;
    let (conclusion, stop, failure) = match reason {
        R::DeadlineExceeded => (
            LogicalConclusion::Failed,
            AttemptStopReason::Deadline,
            query_failure(
                QueryFailureReason::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
        ),
        R::FrontendDrainDeadlineExceeded => (
            LogicalConclusion::Failed,
            AttemptStopReason::DrainDeadline,
            query_failure(
                QueryFailureReason::DeadlineExceeded,
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded",
            ),
        ),
        R::ServerShutdown | R::OwnerDropped => (
            LogicalConclusion::Cancelled,
            AttemptStopReason::Shutdown,
            query_failure(
                QueryFailureReason::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
        ),
        R::Requested
        | R::ClientCancelRequested
        | R::ClientDisconnected
        | R::ExplicitKill
        | R::ExplicitKillConnection => (
            LogicalConclusion::Cancelled,
            AttemptStopReason::Client,
            query_failure(
                QueryFailureReason::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
        ),
    };
    CancellationDecision {
        conclusion,
        stop,
        failure,
    }
}

#[cfg(test)]
mod tests;
