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

//! Executor bodies over the original canonical records. Protocol choices stay
//! in AttemptCore; the common Source supplies Work, result roles and join custody.

mod diagnostics;
mod dynamic_filters;
pub(crate) mod split;

pub(crate) use diagnostics::{DiagnosticRecords, DiagnosticRegistration};
pub(crate) use dynamic_filters::{
    DynamicFilterFailureSource, DynamicFilterRecords, DynamicReadRegistration,
};

use crate::task_execution::error::TaskExecutionError;
use novarocks_attempt_protocol::failure::{
    AttemptFailure, AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
};
use novarocks_control_runtime::exit_custody::{ExitCustody, ExitCustodyPhase};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_query_application::execution_error::ExecutionFailureSite;
use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) enum PumpRecordError {
    DuplicateRecord,
    MissingRecord,
    WrongIdentity,
    AlreadyStarted,
    MissingFeedback,
    ResultAlreadyBound,
}
impl std::fmt::Display for PumpRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "attempt pump record invariant failed: {self:?}")
    }
}
impl std::error::Error for PumpRecordError {}

/// Observes only the custodies issued by the original Source. This is not a
/// second join ledger: only ExitCustody performs report/header retirement.
struct OriginalJobRecord {
    custody: ExitCustody,
    reply_input: Option<InputId>,
    consumed: BTreeSet<InputId>,
}
impl OriginalJobRecord {
    fn new(custody: ExitCustody) -> Self {
        Self {
            custody,
            reply_input: None,
            consumed: BTreeSet::new(),
        }
    }
    fn bind_reply(&mut self, input: InputId) -> Result<(), PumpRecordError> {
        if self.reply_input.is_some() {
            return Err(PumpRecordError::ResultAlreadyBound);
        }
        self.reply_input = Some(input);
        Ok(())
    }
    fn core_consumed(&mut self, inputs: &[InputId]) {
        if let Some(input) = self.reply_input
            && inputs.contains(&input)
        {
            self.consumed.insert(input);
        }
    }
    fn retired(&self) -> bool {
        self.custody.phase() == ExitCustodyPhase::Retired
            && self
                .reply_input
                .is_some_and(|input| self.consumed.contains(&input))
    }
}

/// The original TaskExecutionError remains in its canonical host record.
/// Projection uses its existing typed author, preserving the original site.
fn project_task_error(original: HostFailureRef, error: &TaskExecutionError) -> AttemptFailure {
    let input = error.preparation_input();
    let site = match input.site() {
        ExecutionFailureSite::Task { identity, phase } => {
            AttemptFailureSite::Task { identity, phase }
        }
        ExecutionFailureSite::TaskControl { identity } => {
            AttemptFailureSite::TaskControl { identity }
        }
        ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind,
        } => AttemptFailureSite::Operation {
            participation,
            operation_id,
            kind,
        },
        ExecutionFailureSite::RootRead { root } => AttemptFailureSite::RootRead { root },
        ExecutionFailureSite::Backend { process } => AttemptFailureSite::Backend { process },
        ExecutionFailureSite::Coordinator => AttemptFailureSite::Attempt,
    };
    AttemptFailure::Host(HostProjectedFailure {
        original,
        kind: HostFailureKind::Execution,
        class: input.attempt_class(),
        site,
    })
}
