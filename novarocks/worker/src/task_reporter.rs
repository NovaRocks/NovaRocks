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

//! Detached proposal and neutral observation views, with no lifecycle issuer.

use crate::{
    TaskSecondaryDiagnosticsSnapshot,
    control_runtime::lifecycle_read::{TaskReadBinding, TaskReadError},
    task_port::TaskExecutionView,
};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::{
    AbortCause, CancelReason, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
    TaskIdentity, TaskStatus,
};
use novarocks_worker_protocol::convergence::TaskConvergenceSnapshot;
use std::sync::Arc;

/// Clones retain no live Task owner, Work, source or lifecycle capability.
/// The actual host keeps the original owner until it seals this same neutral
/// read binding after physical exit and retirement of all its inventories.
#[derive(Clone)]
pub struct TaskStatusReporter {
    view: Arc<TaskExecutionView>,
    read: Arc<TaskReadBinding>,
}
impl std::fmt::Debug for TaskStatusReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskStatusReporter")
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}
impl TaskStatusReporter {
    pub(crate) fn from_original(view: Arc<TaskExecutionView>, read: Arc<TaskReadBinding>) -> Self {
        assert_eq!(
            view.identity(),
            read.identity(),
            "original Task observation identity"
        );
        Self { view, read }
    }
    pub fn identity(&self) -> TaskIdentity {
        self.read.identity()
    }
    pub fn current(&self) -> Result<TaskStatus, TaskReadError> {
        Ok(self.read.snapshot()?.status.clone())
    }
    pub fn convergence(&self) -> Result<TaskConvergenceSnapshot, TaskReadError> {
        Ok(self.read.snapshot()?.convergence)
    }
    pub fn secondary_diagnostics(&self) -> Result<TaskSecondaryDiagnosticsSnapshot, TaskReadError> {
        Ok(self.read.snapshot()?.diagnostics.clone())
    }
    pub fn failing(&self, cause: ExecutionFailure, phase: TaskFailurePhase) {
        self.view
            .submit(TaskConclusionProposal::Failed { cause, phase });
    }
    pub fn canceling(&self, reason: CancelReason) {
        self.view.submit(TaskConclusionProposal::Canceled(reason));
    }
    pub fn aborting(&self, cause: AbortCause) {
        self.view.submit(TaskConclusionProposal::Aborted(cause));
    }
}
