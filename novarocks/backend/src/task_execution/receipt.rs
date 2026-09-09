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

//! What every handler returns: one operation id, one machine-readable
//! outcome, and at most one typed acknowledgement body.
//!
//! The shape mirrors the wire receipt exactly, minus the encoding. That is
//! deliberate: a transport adapter maps these fields one-to-one and therefore
//! cannot invent an outcome, drop an acknowledgement, or classify a result by
//! reading its diagnostic text.

use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskOperationId};
use novarocks_execution_contract::task_execution::operation::{
    CreateTaskReceipt, OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextReceipt,
    ReleaseOutcome, UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, FinalTaskInfo, SafeDetail, TaskStatus,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;

use super::host::TaskDynamicFilterRead;

/// One operation's receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationReceipt<T> {
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    detail: Option<SafeDetail>,
    acknowledgement: Option<T>,
}

impl<T> OperationReceipt<T> {
    /// A receipt carrying an acknowledgement body.
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

    /// A receipt that carries only an outcome and a redacted diagnostic.
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

    /// A settled receipt with nothing to acknowledge.
    ///
    /// It is the same shape as a rejection, and deliberately a different
    /// constructor: some outcomes — an observation that is already up to date,
    /// a read whose subject retains nothing — are settled rather than refused,
    /// and reading `rejected` at those call sites would misstate them.
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

    /// The acknowledgement, for a caller that already checked the outcome.
    pub fn into_acknowledgement(self) -> Option<T> {
        self.acknowledgement
    }
}

/// The acknowledgement body of a release.
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

    /// The shared-resource lifecycle cause, present once terminal. It is never
    /// a second authority over a task's own terminal outcome.
    pub const fn termination_cause(self) -> Option<AbortCause> {
        self.termination_cause
    }
}

pub type CreateTaskOutcome = OperationReceipt<CreateTaskReceipt>;
pub type AdmissionTicketOutcome = OperationReceipt<QueryContextAdmissionTicketReceipt>;
pub type UpdateTaskOutcome = OperationReceipt<UpdateTaskReceipt>;
pub type QueryContextOutcome = OperationReceipt<QueryContextReceipt>;
pub type ReleaseQueryContextOutcome = OperationReceipt<ReleaseAcknowledgement>;
pub type CancelTaskOutcome = OperationReceipt<TaskStatus>;
pub type DynamicFilterReadOutcome = OperationReceipt<TaskDynamicFilterRead>;
pub type FinalTaskInfoOutcome = OperationReceipt<FinalTaskInfo>;
