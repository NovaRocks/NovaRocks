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

use novarocks_execution_contract::OperationOutcome;

/// A verdict that was actually produced by a Worker operation.
///
/// The execution contract now contains only the closed Worker verdict set.
/// This wrapper keeps receipt settlement distinct from frontend transport and
/// observation results at the query coordination boundary.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WorkerReceiptOutcome(OperationOutcome);

impl WorkerReceiptOutcome {
    pub const fn from_contract(outcome: OperationOutcome) -> Self {
        Self(outcome)
    }

    pub const fn outcome(self) -> OperationOutcome {
        self.0
    }
}

/// The two possible results of dispatching one immutable Worker operation.
///
/// A settled RPC carries a validated Worker verdict. Losing the transport
/// response leaves the remote effect unknown and authorizes replaying only
/// that exact request. Observation and destination delivery have separate
/// event types and cannot enter this decision.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OperationDispatchResult {
    WorkerReceipt(WorkerReceiptOutcome),
    TransportUnknown,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FrontendAction {
    Settled,
    RetryExactRequest,
    FailOperationClosed,
    FailAttempt,
    RetryAfterProgress,
    StopSendingAndReconcile,
}

pub fn frontend_action(result: OperationDispatchResult) -> FrontendAction {
    let OperationDispatchResult::WorkerReceipt(receipt) = result else {
        return FrontendAction::RetryExactRequest;
    };
    match receipt.outcome() {
        OperationOutcome::Accepted | OperationOutcome::Idempotent => FrontendAction::Settled,
        OperationOutcome::OperationTimedOut => FrontendAction::FailOperationClosed,
        OperationOutcome::ReleaseNotReady => FrontendAction::RetryAfterProgress,
        OperationOutcome::ContextTerminalReceipt
        | OperationOutcome::Gone
        | OperationOutcome::TerminalRejected => FrontendAction::StopSendingAndReconcile,
        OperationOutcome::IdentityMismatch
        | OperationOutcome::CompatibilityMismatch
        | OperationOutcome::CreateConflict
        | OperationOutcome::ContextNotEstablished
        | OperationOutcome::ContextConflict
        | OperationOutcome::DomainConflict
        | OperationOutcome::LeaseExpired
        | OperationOutcome::InvalidStateOrRequest
        | OperationOutcome::ResourceExhausted => FrontendAction::FailAttempt,
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(outcome: OperationOutcome) -> OperationDispatchResult {
        OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(outcome))
    }

    #[test]
    fn terminal_receipts_reconcile_instead_of_overriding_task_status() {
        assert_eq!(
            frontend_action(receipt(OperationOutcome::ContextTerminalReceipt)),
            FrontendAction::StopSendingAndReconcile
        );
        assert_eq!(
            frontend_action(receipt(OperationOutcome::TerminalRejected)),
            FrontendAction::StopSendingAndReconcile
        );
    }

    #[test]
    fn only_transport_unknown_replays_the_exact_operation() {
        assert_eq!(
            frontend_action(OperationDispatchResult::TransportUnknown),
            FrontendAction::RetryExactRequest
        );
        assert_eq!(
            frontend_action(receipt(OperationOutcome::ResourceExhausted)),
            FrontendAction::FailAttempt
        );
    }
}
