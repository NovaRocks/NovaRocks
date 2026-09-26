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
use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskOperationId};
use novarocks_execution_contract::task_execution::operation::{
    CreateTaskReceipt, OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextReceipt,
    ReleaseOutcome, UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, FinalTaskInfo, SafeDetail, TaskStatus,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;

/// One operation's settled Worker verdict and optional acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationReceipt<T> {
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    detail: Option<SafeDetail>,
    acknowledgement: Option<T>,
}

impl<T> OperationReceipt<T> {
    pub const fn acknowledged(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        acknowledgement: T,
    ) -> Self {
        Self {
            operation_id,
            outcome,
            detail: None,
            acknowledgement: Some(acknowledgement),
        }
    }

    pub fn rejected(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        detail: impl AsRef<str>,
    ) -> Self {
        Self {
            operation_id,
            outcome,
            detail: Some(SafeDetail::truncating(detail.as_ref())),
            acknowledgement: None,
        }
    }

    pub fn settled(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        detail: impl AsRef<str>,
    ) -> Self {
        Self::rejected(operation_id, outcome, detail)
    }

    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub const fn detail(&self) -> Option<&SafeDetail> {
        self.detail.as_ref()
    }

    pub const fn acknowledgement(&self) -> Option<&T> {
        self.acknowledgement.as_ref()
    }

    pub fn into_acknowledgement(self) -> Option<T> {
        self.acknowledgement
    }
}

/// The acknowledgement body of a context release.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ReleaseAcknowledgement {
    context: QueryContextRef,
    release: ReleaseOutcome,
    state: QueryContextState,
    termination_cause: Option<AbortCause>,
}

impl ReleaseAcknowledgement {
    pub const fn new(
        context: QueryContextRef,
        release: ReleaseOutcome,
        state: QueryContextState,
        termination_cause: Option<AbortCause>,
    ) -> Self {
        Self {
            context,
            release,
            state,
            termination_cause,
        }
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn release(self) -> ReleaseOutcome {
        self.release
    }

    pub const fn state(self) -> QueryContextState {
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
pub type AdmissionTicketOutcome = OperationReceipt<QueryContextAdmissionTicketReceipt>;
pub type UpdateTaskOutcome = OperationReceipt<UpdateTaskReceipt>;
pub type QueryContextOutcome = OperationReceipt<QueryContextReceipt>;
pub type ReleaseQueryContextOutcome = OperationReceipt<ReleaseAcknowledgement>;
pub type QuiesceQueryContextOutcome =
    OperationReceipt<novarocks_execution_contract::QuiesceQueryContextReceipt>;
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
