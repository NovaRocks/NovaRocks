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

//! Worker-owned results of linearized task operations.
//!
//! These receipts are transport-neutral facts. Native adapters encode them,
//! but cannot construct or reinterpret a Worker verdict.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution_contract::task_execution::identity::{ParticipationRef, TaskOperationId};
use novarocks_execution_contract::task_execution::operation::{
    CreateTaskReceipt, OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationReceipt,
    ReleaseOutcome, UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, FinalTaskInfo, SafeDetail, TaskStatus,
};
use novarocks_execution_contract::task_execution::transition::ParticipationState;

/// One operation's Worker result. Domain failure carries no acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationReceipt<T> {
    operation_id: TaskOperationId,
    result: novarocks_execution_contract::operation::OperationResult<T>,
}
impl<T> OperationReceipt<T> {
    pub const fn from_result(
        operation_id: TaskOperationId,
        result: novarocks_execution_contract::operation::OperationResult<T>,
    ) -> Self {
        Self {
            operation_id,
            result,
        }
    }
    pub const fn acknowledged(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        acknowledgement: T,
    ) -> Self {
        Self::from_result(
            operation_id,
            novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome,
                detail: None,
                acknowledgement: Some(acknowledgement),
            },
        )
    }
    pub fn rejected(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        detail: impl AsRef<str>,
    ) -> Self {
        Self::from_result(
            operation_id,
            novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome,
                detail: Some(SafeDetail::truncating(detail.as_ref())),
                acknowledgement: None,
            },
        )
    }
    pub fn settled(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        detail: impl AsRef<str>,
    ) -> Self {
        Self::rejected(operation_id, outcome, detail)
    }
    pub const fn domain_failure(
        operation_id: TaskOperationId,
        outcome: novarocks_execution_contract::operation::DomainFailureOutcome,
        failure: novarocks_execution_contract::operation::OperationDomainFailure,
    ) -> Self {
        Self::from_result(
            operation_id,
            novarocks_execution_contract::operation::OperationResult::DomainFailure {
                outcome,
                failure,
            },
        )
    }
    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }
    pub const fn result(&self) -> &novarocks_execution_contract::operation::OperationResult<T> {
        &self.result
    }
    pub const fn outcome(&self) -> OperationOutcome {
        self.result.outcome()
    }
    pub const fn detail(&self) -> Option<&SafeDetail> {
        self.result.detail()
    }
    pub const fn acknowledgement(&self) -> Option<&T> {
        self.result.acknowledgement()
    }
    pub fn into_acknowledgement(self) -> Option<T> {
        match self.result {
            novarocks_execution_contract::operation::OperationResult::Verdict {
                acknowledgement,
                ..
            } => acknowledgement,
            novarocks_execution_contract::operation::OperationResult::DomainFailure { .. } => None,
        }
    }
    pub fn map_ack<U>(self, map: impl FnOnce(T) -> U) -> OperationReceipt<U> {
        OperationReceipt::from_result(self.operation_id, self.result.map_ack(map))
    }
}

/// The acknowledgement body of a participation release.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ReleaseAcknowledgement {
    participation: ParticipationRef,
    release: ReleaseOutcome,
    state: ParticipationState,
    termination_cause: Option<AbortCause>,
}

impl ReleaseAcknowledgement {
    pub const fn new(
        participation: ParticipationRef,
        release: ReleaseOutcome,
        state: ParticipationState,
        termination_cause: Option<AbortCause>,
    ) -> Self {
        Self {
            participation,
            release,
            state,
            termination_cause,
        }
    }

    pub const fn participation(self) -> ParticipationRef {
        self.participation
    }

    pub const fn release(self) -> ReleaseOutcome {
        self.release
    }

    pub const fn state(self) -> ParticipationState {
        self.state
    }

    pub const fn termination_cause(self) -> Option<AbortCause> {
        self.termination_cause
    }
}

/// One task's retained dynamic-filter domain, if it has published one.
///
/// The payload remains opaque to the Worker lifecycle owner and any adapter.
#[derive(Clone, Debug)]
pub struct TaskDynamicFilterRead {
    version: DomainVersion,
    payload: Arc<dyn CodecOwnedContent>,
}

impl TaskDynamicFilterRead {
    pub const fn new(version: DomainVersion, payload: Arc<dyn CodecOwnedContent>) -> Self {
        Self { version, payload }
    }

    pub const fn version(&self) -> DomainVersion {
        self.version
    }

    pub fn payload(&self) -> &Arc<dyn CodecOwnedContent> {
        &self.payload
    }
}

pub type CreateTaskOutcome = OperationReceipt<CreateTaskReceipt>;
pub type AdmissionTicketOutcome = OperationReceipt<ParticipationAdmissionTicketReceipt>;
pub type UpdateTaskOutcome = OperationReceipt<UpdateTaskReceipt>;
pub type ParticipationOutcome = OperationReceipt<ParticipationReceipt>;
pub type ReleaseParticipationOutcome = OperationReceipt<ReleaseAcknowledgement>;
pub type QuiesceParticipationOutcome =
    OperationReceipt<novarocks_execution_contract::QuiesceParticipationReceipt>;
pub type CancelTaskOutcome = OperationReceipt<TaskStatus>;
pub type DynamicFilterReadOutcome = OperationReceipt<TaskDynamicFilterRead>;
pub type FinalTaskInfoOutcome = OperationReceipt<FinalTaskInfo>;

#[cfg(test)]
mod tests {
    use novarocks_execution_contract::task_execution::identity::TaskOperationId;
    use novarocks_execution_contract::task_execution::operation::OperationOutcome;

    use super::OperationReceipt;

    #[test]
    fn rejected_receipt_retains_only_a_bounded_safe_detail() {
        let receipt: OperationReceipt<()> = OperationReceipt::rejected(
            TaskOperationId::new_v7(),
            OperationOutcome::InvalidStateOrRequest,
            "invalid state",
        );

        assert_eq!(receipt.outcome(), OperationOutcome::InvalidStateOrRequest);
        assert_eq!(
            receipt.detail().expect("safe detail").as_str(),
            "invalid state"
        );
        assert!(receipt.acknowledgement().is_none());
    }
}
