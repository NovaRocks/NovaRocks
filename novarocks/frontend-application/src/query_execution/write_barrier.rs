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
//! * **Execution reached a success-compatible terminal set.** Proved by the
//!   task substrate's verdict over the frozen writer set and root finish task.
//!   A writer may finish normally or be canceled by the one normal downstream
//!   release after `TableFinish` consumed every sender. The separate prepared
//!   set proof is what establishes that no writer output was lost.
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
    /// At least one participant of this attempt did not reach a
    /// success-compatible terminal.
    ExecutionDidNotSucceed,
    /// The statement was cancelled before commit.
    Cancelled,
    /// The task substrate answered with something other than a completion.
    TaskExecutionIncomplete,
    /// The statement deadline expired before the external effect began.
    DeadlineExpired,
}

impl WriteCommitBlocked {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PreparedWriteSetIncomplete => {
                "connector write did not receive a complete prepared write set"
            }
            Self::ExecutionDidNotSucceed => {
                "connector write execution did not reach a success-compatible terminal on every participant"
            }
            Self::Cancelled => "connector write was cancelled before its external commit",
            Self::TaskExecutionIncomplete => {
                "connector write execution did not complete on the task substrate"
            }
            Self::DeadlineExpired => "connector write deadline expired before its external commit",
        }
    }
}

/// Accumulates the independent facts and answers one question.
#[derive(Debug, Default)]
pub(crate) struct WriteCommitBarrier {
    prepared: Option<DecodedPreparedWriteSet>,
    execution: Option<WriteVerdict>,
    cancelled: bool,
    deadline_expired: bool,
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

    /// Record the task substrate's verdict over this write's frozen writer
    /// set.
    ///
    /// This takes the verdict rather than a boolean so a caller cannot hand
    /// over a `true` derived from a weaker rule: the reason a write may not
    /// commit has to survive the trip to this gate, or the gate can only say
    /// "no" without saying what to look at.
    pub(crate) const fn observe_task_execution(&mut self, verdict: WriteVerdict) {
        self.execution = Some(verdict);
    }

    /// Unit-level compatibility for statement-flow tests that exercise the
    /// barrier without constructing a task graph. Production code has one
    /// completion authority and cannot call this helper.
    #[cfg(test)]
    pub(crate) const fn observe_execution_terminals(&mut self, succeeded: bool) {
        self.execution = Some(if succeeded {
            WriteVerdict::Complete
        } else {
            WriteVerdict::AttemptFailed
        });
    }

    pub(crate) const fn observe_cancelled(&mut self) {
        self.cancelled = true;
    }

    pub(crate) const fn observe_deadline_expired(&mut self) {
        self.deadline_expired = true;
    }

    /// Consume the barrier, yielding the set only when every fact holds.
    ///
    /// The order the facts arrived in does not matter; what matters is that
    /// all of them did.
    pub(crate) fn into_committable(self) -> Result<DecodedPreparedWriteSet, WriteCommitBlocked> {
        if self.cancelled {
            return Err(WriteCommitBlocked::Cancelled);
        }
        if self.deadline_expired {
            return Err(WriteCommitBlocked::DeadlineExpired);
        }
        match self.execution {
            None | Some(WriteVerdict::AttemptFailed) => {
                return Err(WriteCommitBlocked::ExecutionDidNotSucceed);
            }
            Some(WriteVerdict::TaskNotFinished { .. } | WriteVerdict::UndeclaredWriter(_)) => {
                return Err(WriteCommitBlocked::TaskExecutionIncomplete);
            }
            Some(WriteVerdict::Complete) => {}
        }
        self.prepared
            .ok_or(WriteCommitBlocked::PreparedWriteSetIncomplete)
    }
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum CompletionFact {
        PreparedWriteSet,
        ExecutionAllSuccess,
        TaskFailure,
        Cancelled,
        DeadlineExpired,
    }

    fn complete_set() -> DecodedPreparedWriteSet {
        DecodedPreparedWriteSet::for_test(
            7,
            vec![(
                WriteTargetOrdinal::try_new(0).expect("bounded ordinal"),
                vec![1, 2, 3],
            )],
        )
    }

    fn permutations(facts: &[CompletionFact]) -> Vec<Vec<CompletionFact>> {
        if facts.is_empty() {
            return vec![Vec::new()];
        }
        let mut result = Vec::new();
        for index in 0..facts.len() {
            let mut remaining = facts.to_vec();
            let fact = remaining.remove(index);
            for mut suffix in permutations(&remaining) {
                let mut permutation = Vec::with_capacity(facts.len());
                permutation.push(fact);
                permutation.append(&mut suffix);
                result.push(permutation);
            }
        }
        result
    }

    fn observe(barrier: &mut WriteCommitBarrier, fact: CompletionFact) {
        match fact {
            CompletionFact::PreparedWriteSet => {
                // Production can construct this value only after the Root
                // decoder proved exact row membership and observed EOF.
                barrier.observe_prepared_write_set(complete_set());
            }
            CompletionFact::ExecutionAllSuccess => {
                barrier.observe_execution_terminals(true);
            }
            CompletionFact::TaskFailure => barrier.observe_execution_terminals(false),
            CompletionFact::Cancelled => barrier.observe_cancelled(),
            CompletionFact::DeadlineExpired => barrier.observe_deadline_expired(),
        }
    }

    fn run(facts: &[CompletionFact]) -> Result<DecodedPreparedWriteSet, WriteCommitBlocked> {
        let mut barrier = WriteCommitBarrier::new();
        for fact in facts {
            observe(&mut barrier, *fact);
        }
        barrier.into_committable()
    }

    #[test]
    fn both_facts_together_open_the_gate_in_either_arrival_order() {
        let facts = [
            CompletionFact::PreparedWriteSet,
            CompletionFact::ExecutionAllSuccess,
        ];
        for order in permutations(&facts) {
            assert_eq!(
                run(&order).expect("committable").row_count(),
                7,
                "{order:?}"
            );
        }
    }

    #[test]
    fn a_complete_set_does_not_stand_in_for_a_successful_execution() {
        // The data plane closed, but some other participant failed. Committing
        // here would publish a snapshot for a query that did not succeed.
        let facts = [
            CompletionFact::PreparedWriteSet,
            CompletionFact::TaskFailure,
        ];
        for order in permutations(&facts) {
            assert_eq!(
                run(&order).expect_err("must not commit"),
                WriteCommitBlocked::ExecutionDidNotSucceed,
                "{order:?}"
            );
        }
    }

    #[test]
    fn a_successful_execution_does_not_stand_in_for_a_complete_set() {
        // The task set succeeded, but the frontend never read the root result
        // to end of stream. Task status carries no artifacts, so success alone
        // proves nothing about the prepared write set.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_execution_terminals(true);
        assert_eq!(
            barrier.into_committable().expect_err("must not commit"),
            WriteCommitBlocked::PreparedWriteSetIncomplete
        );
    }

    #[test]
    fn cancellation_vetoes_a_write_that_otherwise_had_both_facts() {
        let facts = [
            CompletionFact::PreparedWriteSet,
            CompletionFact::ExecutionAllSuccess,
            CompletionFact::Cancelled,
        ];
        for order in permutations(&facts) {
            assert_eq!(
                run(&order).expect_err("must not commit"),
                WriteCommitBlocked::Cancelled,
                "{order:?}"
            );
        }
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

        // A terminal that the tracker did not classify as success-compatible
        // remains incomplete even when a prepared set exists.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(complete_set());
        barrier.observe_task_execution(WriteVerdict::TaskNotFinished {
            task: identity,
            state: TaskState::Aborted,
        });
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
    fn deadline_vetoes_a_write_that_otherwise_had_both_facts() {
        let facts = [
            CompletionFact::PreparedWriteSet,
            CompletionFact::ExecutionAllSuccess,
            CompletionFact::DeadlineExpired,
        ];
        for order in permutations(&facts) {
            assert_eq!(
                run(&order).expect_err("must not commit"),
                WriteCommitBlocked::DeadlineExpired,
                "{order:?}"
            );
        }
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
