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

//! Bounded current-process maintenance job ownership.

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MaintenanceJobState {
    Pending,
    Running,
    Finished,
    KnownUncommitted,
    CommitUnknown,
    KnownCommittedFinalizationFailed,
    PreDispatchFailed,
    Failed,
    TargetReplaced,
    CancelledBeforeDispatch,
}

impl MaintenanceJobState {
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::KnownUncommitted => "KNOWN_UNCOMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::KnownCommittedFinalizationFailed => "KNOWN_COMMITTED_FINALIZATION_FAILED",
            Self::PreDispatchFailed => "PRE_DISPATCH_FAILED",
            Self::Failed => "FAILED",
            Self::TargetReplaced => "TARGET_REPLACED",
            Self::CancelledBeforeDispatch => "CANCELLED_BEFORE_DISPATCH",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalError {
    pub failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    pub effect: Option<novarocks_coordination_contract::ExecutionEffect>,
    pub state: MaintenanceJobState,
    pub message: String,
    pub compile_control: Option<novarocks_type_contract::CompileControlError>,
    pub execution_failure:
        Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
}

impl TerminalError {
    pub fn with_failure_cause(
        mut self,
        cause: Option<novarocks_failure_contract::SharedFailureCause>,
    ) -> Self {
        self.failure_cause = cause;
        self
    }
    /// Selection only delegates to an actual carrier; no reason is inferred.
    pub fn selected_failure_fact(&self) -> Option<novarocks_failure_contract::FailureFact> {
        use novarocks_failure_contract::FailureProjection;
        self.failure_cause
            .as_ref()
            .map(FailureProjection::failure_fact)
            .or_else(|| self.execution_failure.as_ref().map(|input| input.fact()))
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(FailureProjection::failure_fact)
            })
    }

    pub fn with_effect(mut self, effect: novarocks_coordination_contract::ExecutionEffect) -> Self {
        self.effect = Some(effect);
        self
    }

    pub fn with_execution_failure(
        mut self,
        input: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    ) -> Self {
        self.execution_failure = input;
        self
    }

    /// Preserve an ordinary producer failure without turning it into a compiler interruption.
    pub fn from_original_cause(
        error: impl novarocks_failure_contract::FailureProjection
        + std::error::Error
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::pre_dispatch_failed("table-maintenance original operation failed").with_failure_cause(
            Some(
                novarocks_failure_contract::SharedFailureCause::from_static_detail(
                    error,
                    "table-maintenance original operation failed",
                ),
            ),
        )
    }
    pub fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        let mut failure = Self::pre_dispatch_failed(error.to_string());
        failure.compile_control = Some(error);
        failure
    }
    pub fn with_compile_control(
        mut self,
        error: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = error;
        self
    }
    pub fn pre_dispatch_failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::PreDispatchFailed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::Failed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: None,
        }
    }
    pub fn target_replaced(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::TargetReplaced,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn cancelled_before_dispatch(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CancelledBeforeDispatch,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn known_uncommitted(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownUncommitted,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::None),
        }
    }
    pub fn commit_unknown(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::CommitUnknown,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: None,
        }
    }
    pub fn known_committed_finalization_failed(message: impl Into<String>) -> Self {
        Self {
            state: MaintenanceJobState::KnownCommittedFinalizationFailed,
            message: message.into(),
            compile_control: None,
            execution_failure: None,
            failure_cause: None,
            effect: Some(novarocks_coordination_contract::ExecutionEffect::External),
        }
    }
}

impl From<String> for TerminalError {
    fn from(error: String) -> Self {
        Self::pre_dispatch_failed(error)
    }
}
impl fmt::Display for TerminalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for TerminalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failure_cause
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
            .or_else(|| {
                self.compile_control
                    .as_ref()
                    .map(|cause| cause as &dyn std::error::Error)
            })
    }
}
