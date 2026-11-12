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
mod job;
mod source;
pub(crate) mod split;
pub(crate) mod split_source;

pub(crate) use diagnostics::{DiagnosticRecords, DiagnosticRegistration};
pub(crate) use dynamic_filters::{DynamicFilterRecords, DynamicReadRegistration};
pub(crate) use job::{BoundPumpWork, PumpOutcome};
pub(crate) use source::AttemptPumpSource;

use crate::task_execution::{error::TaskExecutionError, intent::attempt_receipt::failure_site};
use novarocks_attempt_protocol::failure::{
    AttemptFailure, HostFailureKind, HostFailureRef, HostProjectedFailure,
};
use novarocks_control_runtime::exit_custody::{ExitCustody, ExitCustodyPhase};
#[cfg(test)]
use novarocks_control_runtime_core::identity::InputId;

#[derive(Debug)]
pub(crate) enum PumpRecordError {
    DuplicateRecord,
    MissingRecord,
    WrongIdentity,
    AlreadyStarted,
    MissingFeedback,
    ResultAlreadyBound,
    Split(split::SplitRecordError),
    SplitCloseUnresolved(crate::task_execution::pump_health::PumpCloseUnresolved),
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
    #[cfg(test)]
    reply_input: Option<InputId>,
    result_resolved: bool,
}
impl OriginalJobRecord {
    fn new(custody: ExitCustody) -> Self {
        Self {
            custody,
            #[cfg(test)]
            reply_input: None,
            result_resolved: false,
        }
    }
    #[cfg(test)]
    fn bind_reply(&mut self, input: InputId) -> Result<(), PumpRecordError> {
        if self.reply_input.is_some() {
            return Err(PumpRecordError::ResultAlreadyBound);
        }
        self.reply_input = Some(input);
        Ok(())
    }
    #[cfg(test)]
    fn core_consumed(&mut self, inputs: &[InputId]) {
        if let Some(input) = self.reply_input
            && inputs.contains(&input)
        {
            self.result_resolved = true;
        }
    }
    fn resolve_result(&mut self) {
        self.result_resolved = true;
    }
    fn retired(&self) -> bool {
        self.custody.phase() == ExitCustodyPhase::Retired && self.result_resolved
    }
}

/// The original TaskExecutionError remains in its canonical host record.
/// Projection uses its existing typed author, preserving the original site.
fn project_task_error(original: HostFailureRef, error: &TaskExecutionError) -> AttemptFailure {
    let input = error.preparation_input();
    AttemptFailure::Host(HostProjectedFailure {
        original,
        kind: HostFailureKind::Execution,
        class: input.attempt_class(),
        site: failure_site(input.site()),
    })
}
