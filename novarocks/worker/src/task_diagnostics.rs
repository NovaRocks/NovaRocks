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

//! Bounded Worker-local secondary facts, separate from Task acceptance and exit.
//! Retired Task export and wire/Frontend projection are not connected here.

use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::failure::FailureFact;
use novarocks_execution_contract::{SafeDetail, TaskFailurePhase};
use std::{fmt, sync::Arc};

const MAX_DIAGNOSTICS: usize = 4;
const MAX_DETAIL_BYTES: usize = 512;

/// Why a secondary observation was recorded; neither role can accept a Task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskDiagnosticRole {
    LateFailure,
    Cleanup,
}

#[derive(Clone, Eq, PartialEq)]
struct InlineSafeDetail {
    bytes: [u8; MAX_DETAIL_BYTES],
    len: u16,
}
impl InlineSafeDetail {
    fn copy_safe(detail: &SafeDetail) -> Self {
        Self::copy_utf8(detail.as_str())
    }
    fn copy_utf8(detail: &str) -> Self {
        let mut end = detail.len().min(MAX_DETAIL_BYTES);
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        let mut bytes = [0; MAX_DETAIL_BYTES];
        bytes[..end].copy_from_slice(&detail.as_bytes()[..end]);
        Self {
            bytes,
            len: end as u16,
        }
    }
    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.len)])
            .expect("secondary detail copies a valid UTF-8 prefix")
    }
}
impl fmt::Debug for InlineSafeDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_str(), f)
    }
}

/// Fixed-layout neutral facts. No original source or runtime owner is retained.
#[derive(Debug, Eq, PartialEq)]
pub struct TaskSecondaryDiagnostic {
    role: TaskDiagnosticRole,
    phase: TaskFailurePhase,
    fact: FailureFact,
    detail: InlineSafeDetail,
}
impl TaskSecondaryDiagnostic {
    fn prepare(
        cause: &ExecutionFailure,
        phase: TaskFailurePhase,
        role: TaskDiagnosticRole,
        fact: FailureFact,
    ) -> Arc<Self> {
        // Both rendering and the cold fixed record allocation precede every
        // acceptance lock/enclosing lifecycle fence. Length is not redaction:
        // only ExecutionFailure's neutral safe summary enters this record.
        let detail = InlineSafeDetail::copy_safe(&cause.safe_detail_for_fact(fact));
        Arc::new(Self {
            role,
            phase,
            fact,
            detail,
        })
    }
    pub const fn role(&self) -> TaskDiagnosticRole {
        self.role
    }
    pub const fn phase(&self) -> TaskFailurePhase {
        self.phase
    }
    pub const fn fact(&self) -> FailureFact {
        self.fact
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
    pub(crate) fn late_failure(
        cause: &ExecutionFailure,
        phase: TaskFailurePhase,
        fact: FailureFact,
    ) -> Arc<Self> {
        Self::prepare(cause, phase, TaskDiagnosticRole::LateFailure, fact)
    }
}

/// A prepared diagnostic effect, never a Task conclusion proposal. Prepare it
/// before taking an enclosing lifecycle fence; the rich source stays borrowed.
#[derive(Debug)]
pub struct PreparedCleanupDiagnostic {
    record: Option<Arc<TaskSecondaryDiagnostic>>,
}
impl PreparedCleanupDiagnostic {
    pub fn from_failure(cause: &ExecutionFailure, phase: TaskFailurePhase) -> Self {
        use novarocks_execution_contract::failure::FailureProjection;
        Self {
            record: Some(TaskSecondaryDiagnostic::prepare(
                cause,
                phase,
                TaskDiagnosticRole::Cleanup,
                cause.failure_fact(),
            )),
        }
    }
    pub(crate) fn record_mut(&mut self) -> &mut Option<Arc<TaskSecondaryDiagnostic>> {
        &mut self.record
    }
}

/// A diagnostic storage outcome supplies no acceptance, exit or capacity proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticRecordOutcome {
    Stored,
    Dropped,
}

/// One local snapshot clones at most four cold records without allocating or
/// pinning Task/runtime/source owners. Retirement/wire export remains separate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSecondaryDiagnosticsSnapshot {
    entries: [Option<Arc<TaskSecondaryDiagnostic>>; MAX_DIAGNOSTICS],
    len: u8,
    dropped: u64,
}
impl TaskSecondaryDiagnosticsSnapshot {
    /// Copy only the Core's already safe neutral records. An invalid geometry
    /// is rejected rather than truncated into a different observed history.
    pub(crate) fn from_protocol(
        diagnostics: &novarocks_worker_protocol::proposal_summary::Diagnostics,
    ) -> Option<Self> {
        if diagnostics.entries.len() > MAX_DIAGNOSTICS {
            return None;
        }
        let mut entries = std::array::from_fn(|_| None);
        for (slot, diagnostic) in entries.iter_mut().zip(&diagnostics.entries) {
            *slot = Some(Arc::new(TaskSecondaryDiagnostic {
                role: if diagnostic.cleanup {
                    TaskDiagnosticRole::Cleanup
                } else {
                    TaskDiagnosticRole::LateFailure
                },
                phase: diagnostic.phase,
                fact: diagnostic.fact,
                detail: InlineSafeDetail::copy_safe(&diagnostic.detail),
            }));
        }
        Some(Self {
            entries,
            len: diagnostics.entries.len() as u8,
            dropped: diagnostics.dropped,
        })
    }
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &TaskSecondaryDiagnostic> {
        self.entries[..usize::from(self.len)].iter().map(|entry| {
            entry
                .as_deref()
                .expect("occupied secondary diagnostic prefix")
        })
    }
    /// Counts omitted reports, saturating at u64::MAX rather than wrapping.
    pub const fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// Fixed slot metadata only. Normal Tasks do not reserve four inline payloads.
#[derive(Debug)]
pub(crate) struct TaskSecondaryDiagnostics {
    entries: [Option<Arc<TaskSecondaryDiagnostic>>; MAX_DIAGNOSTICS],
    len: u8,
    dropped: u64,
}
impl Default for TaskSecondaryDiagnostics {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
            dropped: 0,
        }
    }
}
impl TaskSecondaryDiagnostics {
    pub(crate) fn record(
        &mut self,
        prepared: &mut Option<Arc<TaskSecondaryDiagnostic>>,
    ) -> DiagnosticRecordOutcome {
        assert!(prepared.is_some(), "a diagnostic record is consumed once");
        if usize::from(self.len) == MAX_DIAGNOSTICS {
            self.dropped = self.dropped.saturating_add(1);
            // Leave overflow owned by the caller's prepared effect. In the
            // failure path it leaves every enclosing fence with the original
            // losing source, through Pending/OwnedTaskConclusion.
            return DiagnosticRecordOutcome::Dropped;
        }
        self.entries[usize::from(self.len)] = prepared.take();
        self.len += 1;
        DiagnosticRecordOutcome::Stored
    }
    pub(crate) fn snapshot(&self) -> TaskSecondaryDiagnosticsSnapshot {
        TaskSecondaryDiagnosticsSnapshot {
            entries: self.entries.clone(),
            len: self.len,
            dropped: self.dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_utf8_record_keeps_the_valid_prefix_without_backing_capacity() {
        let text = "ab€".repeat(200);
        let detail = InlineSafeDetail::copy_utf8(&text);
        assert_eq!(detail.as_str(), &text[..512]);
        assert_eq!(detail.as_str().len(), MAX_DETAIL_BYTES);
        assert!(std::str::from_utf8(detail.as_str().as_bytes()).is_ok());
        let split = "€".repeat(200);
        assert_eq!(InlineSafeDetail::copy_utf8(&split).as_str(), &split[..510]);
    }

    #[test]
    fn omitted_report_count_saturates_without_replacing_the_first_records() {
        let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let mut journal = TaskSecondaryDiagnostics::default();
        for _ in 0..MAX_DIAGNOSTICS {
            let mut prepared =
                PreparedCleanupDiagnostic::from_failure(&cause, TaskFailurePhase::Execution);
            assert_eq!(
                journal.record(prepared.record_mut()),
                DiagnosticRecordOutcome::Stored
            );
        }
        journal.dropped = u64::MAX;
        let mut overflow =
            PreparedCleanupDiagnostic::from_failure(&cause, TaskFailurePhase::Execution);
        assert_eq!(
            journal.record(overflow.record_mut()),
            DiagnosticRecordOutcome::Dropped
        );
        assert_eq!(journal.snapshot().dropped(), u64::MAX);
        assert_eq!(journal.snapshot().entries().len(), MAX_DIAGNOSTICS);
        assert!(overflow.record.is_some());
    }
}
