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

//! The gate an external write commit has to pass.
//!
//! Two facts must hold, and neither implies the other:
//!
//! * **The prepared write set is complete.** Proved by reading the root
//!   result to `Eof`. It says the write data plane closed -- every writer
//!   finished, every sender reached EOS, the finish node emitted, and the
//!   frontend received all of it. It says nothing about whether some other
//!   participant failed.
//! * **Execution succeeded.** Proved by the lifecycle terminal set. It says
//!   every participant of this exact attempt terminated successfully. Since
//!   the lifecycle no longer carries staged artifacts, it says nothing about
//!   whether the frontend actually received the write data.
//!
//! Before this split, one signal stood for both, and a query could reach a
//! commit on the strength of half the evidence. Keeping them separate is the
//! point of this type: the commit call site cannot compile without checking
//! both, and neither can be quietly substituted for the other.
//!
//! Cancellation and deadline are a third, independent veto. They do not prove
//! anything happened; they only forbid starting an external effect.
// Design: ADR-0133 (docs/adr/ADR-0133-dataflow-connector-writer-and-frontend-commit.md)

use crate::query_execution::write_result::DecodedPreparedWriteSet;
use crate::task_execution::completion::WriteVerdict;

/// Why a write may not commit yet, or at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteCommitBlocked {
    /// The root result never reached end of stream, so no complete set exists.
    PreparedWriteSetIncomplete,
    /// At least one participant of this attempt did not succeed.
    ExecutionDidNotSucceed,
    /// The statement was cancelled or its deadline expired before commit.
    Cancelled,
    /// The task substrate answered with something other than a completion.
    TaskExecutionIncomplete,
    /// Both authorities were consulted for one attempt.
    ///
    /// One attempt is judged by exactly one authority. Accepting whichever
    /// arrived last, or either one that said yes, would let the weaker rule
    /// decide a commit whenever both were wired up.
    ConflictingExecutionEvidence,
}

impl WriteCommitBlocked {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedWriteSetIncomplete => {
                "connector write did not receive a complete prepared write set"
            }
            Self::ExecutionDidNotSucceed => {
                "connector write execution did not succeed on every participant"
            }
            Self::Cancelled => "connector write was cancelled before its external commit",
            Self::TaskExecutionIncomplete => {
                "connector write execution did not complete on the task substrate"
            }
            Self::ConflictingExecutionEvidence => {
                "connector write execution was judged by two authorities at once"
            }
        }
    }
}

/// Which authority proved that execution succeeded.
///
/// The task substrate's verdict is strictly stronger than the lifecycle
/// terminal set: beyond "every participant terminated successfully" it also
/// refuses a writer that stood down normally and a task that reported writer
/// facts without being declared a writer. Both are ways a partial write looks
/// like a whole one to a rule that only counts terminals.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ExecutionEvidence {
    #[default]
    None,
    /// The old lifecycle terminal set, removed with the old stack.
    LifecycleTerminals(bool),
    /// The task substrate's verdict over the frozen writer set.
    TaskVerdict(WriteVerdict),
}

/// Accumulates the independent facts and answers one question.
#[derive(Debug, Default)]
pub(crate) struct WriteCommitBarrier {
    prepared: Option<DecodedPreparedWriteSet>,
    execution: ExecutionEvidence,
    /// Set once both authorities have spoken for this attempt.
    conflicting: bool,
    cancelled: bool,
}

impl WriteCommitBarrier {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the complete set.
    ///
    /// The caller must have observed `Eof`; that is why this takes an already
    /// finished set rather than accumulating rows itself. A prefix cannot
    /// reach this method.
    pub(crate) fn observe_prepared_write_set(&mut self, prepared: DecodedPreparedWriteSet) {
        self.prepared = Some(prepared);
    }

    /// Record whether every participant of the exact attempt succeeded.
    pub(crate) const fn observe_execution_terminals(&mut self, succeeded: bool) {
        self.execution = match self.execution {
            ExecutionEvidence::None | ExecutionEvidence::LifecycleTerminals(_) => {
                ExecutionEvidence::LifecycleTerminals(succeeded)
            }
            ExecutionEvidence::TaskVerdict(_) => ExecutionEvidence::None,
        };
        self.conflicting |= matches!(self.execution, ExecutionEvidence::None);
    }

    /// Record the task substrate's verdict over this write's frozen writer
    /// set.
    ///
    /// This takes the verdict rather than a boolean so a caller cannot hand
    /// over a `true` derived from a weaker rule: the reason a write may not
    /// commit has to survive the trip to this gate, or the gate can only say
    /// "no" without saying what to look at.
    // The tests exercise it; production gains its caller when the coordinator
    // cuts over. `expect` rather than `allow` so this fails once that lands.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the coordinator calls this when it cuts over to the task substrate"
        )
    )]
    pub(crate) const fn observe_task_execution(&mut self, verdict: WriteVerdict) {
        self.execution = match self.execution {
            ExecutionEvidence::None | ExecutionEvidence::TaskVerdict(_) => {
                ExecutionEvidence::TaskVerdict(verdict)
            }
            ExecutionEvidence::LifecycleTerminals(_) => ExecutionEvidence::None,
        };
        self.conflicting |= matches!(self.execution, ExecutionEvidence::None);
    }

    pub(crate) const fn observe_cancelled(&mut self) {
        self.cancelled = true;
    }

    /// Consume the barrier, yielding the set only when every fact holds.
    ///
    /// The order the facts arrived in does not matter; what matters is that
    /// all of them did.
    pub(crate) fn into_committable(self) -> Result<DecodedPreparedWriteSet, WriteCommitBlocked> {
        if self.cancelled {
            return Err(WriteCommitBlocked::Cancelled);
        }
        if self.conflicting {
            return Err(WriteCommitBlocked::ConflictingExecutionEvidence);
        }
        match self.execution {
            ExecutionEvidence::None | ExecutionEvidence::LifecycleTerminals(false) => {
                return Err(WriteCommitBlocked::ExecutionDidNotSucceed);
            }
            ExecutionEvidence::TaskVerdict(verdict) if !verdict.is_complete() => {
                return Err(WriteCommitBlocked::TaskExecutionIncomplete);
            }
            ExecutionEvidence::LifecycleTerminals(true) | ExecutionEvidence::TaskVerdict(_) => {}
        }
        self.prepared
            .ok_or(WriteCommitBlocked::PreparedWriteSetIncomplete)
    }
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

    use super::*;

    fn complete_set() -> DecodedPreparedWriteSet {
        DecodedPreparedWriteSet::for_test(
            7,
            vec![(
                WriteTargetOrdinal::try_new(0).expect("bounded ordinal"),
                vec![1, 2, 3],
            )],
        )
    }

    #[test]
    fn both_facts_together_open_the_gate_in_either_arrival_order() {
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect("committable").row_count(),
            7
        );

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        barrier.observe_prepared_write_set(complete_set());
        assert_eq!(
            barrier.into_committable().expect("committable").row_count(),
            7
        );
    }

    #[test]
    fn a_complete_set_does_not_stand_in_for_a_successful_execution() {
        // The data plane closed, but some other participant failed. Committing
        // here would publish a snapshot for a query that did not succeed.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(false);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::ExecutionDidNotSucceed
        );
    }

    #[test]
    fn a_successful_execution_does_not_stand_in_for_a_complete_set() {
        // Every participant terminated successfully, but the frontend never
        // read the root result to end of stream. The lifecycle no longer
        // carries the artifacts, so success alone proves nothing about them.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::PreparedWriteSetIncomplete
        );
    }

    #[test]
    fn cancellation_vetoes_a_write_that_otherwise_had_both_facts() {
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(true);
        barrier.observe_cancelled();
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::Cancelled
        );
    }

    #[test]
    fn a_task_verdict_opens_the_gate_and_a_partial_one_does_not() {
        use novarocks_execution::task_execution::identity::TaskIdentity;
        use novarocks_execution::task_execution::status::TaskState;
        use novarocks_types::identity::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };

        let identity = TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(3, 4),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query"),
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(1).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::Complete);
        assert_eq!(
            barrier.into_committable().expect("committable").row_count(),
            7
        );

        // A writer that stood down normally is compatible with a read's early
        // completion and never with a write: the rows it had not written yet
        // are simply missing. The terminal-counting rule cannot see this.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::WriterCanceled(identity));
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::TaskExecutionIncomplete
        );

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::TaskNotFinished {
            task: identity,
            state: TaskState::Running,
        });
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::TaskExecutionIncomplete
        );
    }

    #[test]
    fn two_authorities_for_one_attempt_close_the_gate_rather_than_race() {
        // While both are wired, accepting whichever arrived last -- or either
        // one that said yes -- would let the weaker rule decide a commit. The
        // order they arrive in must not change the answer either.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_execution_terminals(true);
        barrier.observe_task_execution(WriteVerdict::Complete);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::ConflictingExecutionEvidence
        );

        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::Complete);
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::ConflictingExecutionEvidence
        );

        // Repeating one authority is not a conflict: a barrier may learn the
        // same fact twice.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::Complete);
        barrier.observe_task_execution(WriteVerdict::Complete);
        assert!(barrier.into_committable().is_ok());
    }

    #[test]
    fn a_barrier_that_learned_nothing_refuses() {
        assert_eq!(
            WriteCommitBarrier::new()
                .into_committable()
                .expect_err("must not commit"),
            WriteCommitBlocked::ExecutionDidNotSucceed
        );
    }
}
