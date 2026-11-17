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

//! Failure values authored by logical execution decisions, without host custody.

use novarocks_coordination_contract::{
    ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError, QueryFailureReason,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};

pub fn query_failure(reason: QueryFailureReason, detail: &str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

/// Original activated Rows return contract failure; no Task/operation author
/// or native physical-exit fact is inferred from the absent read runtime.
pub fn missing_rows_runtime() -> QueryExecutionError {
    query_failure(
        QueryFailureReason::ResultProtocol,
        "Native activated a row execution without its root result runtime",
    )
}

pub fn invariant_failure(detail: &str) -> QueryExecutionError {
    query_failure(QueryFailureReason::ActorProtocol, detail)
}

/// The original client transport reported a dropped/closed receipt without a
/// more specific producer error. This does not synthesize a successful delivery.
pub fn result_delivery_failed() -> QueryExecutionError {
    LogicalRequestFailure::ResultDeliveryFailed.error()
}

/// Surviving logical request refusals retain the original actor reason codes.
/// Removed mailbox/permit machinery does not become a new failure author.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogicalRequestFailure {
    StaleAuthority,
    WrongPhase,
    ReplacementNotReady,
    RootAttemptTerminal,
    ExecutionConcluded,
    ResultDeliveryFailed,
    ResultProtocolViolation,
}
impl LogicalRequestFailure {
    pub(crate) fn error(self) -> QueryExecutionError {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code, observed_stop, detail) = match self {
            Self::StaleAuthority => (
                FailureClass::Control,
                51,
                true,
                "attempt authority is no longer current",
            ),
            Self::WrongPhase => (
                FailureClass::Control,
                53,
                true,
                "logical execution operation is not allowed in its current phase",
            ),
            Self::ReplacementNotReady => (
                FailureClass::Control,
                56,
                false,
                "attempt replacement qualification is not ready",
            ),
            Self::RootAttemptTerminal => (
                FailureClass::Control,
                58,
                true,
                "root task already reached a non-success terminal state",
            ),
            Self::ExecutionConcluded => (
                FailureClass::Control,
                59,
                true,
                "logical execution already has a terminal conclusion",
            ),
            Self::ResultDeliveryFailed => (
                FailureClass::Unexpected,
                60,
                false,
                "logical result delivery failed without a more specific recorded cause",
            ),
            Self::ResultProtocolViolation => (
                FailureClass::Invariant,
                99,
                false,
                "logical result sequence or delivery authority violated its protocol",
            ),
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        let fact = if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        };
        QueryExecutionError::from_execution(ExecutionFailureInput::new(
            fact,
            SafeDetail::truncating(detail),
            ExecutionFailureSite::Coordinator,
        ))
    }
}

#[cfg(test)]
mod tests;
