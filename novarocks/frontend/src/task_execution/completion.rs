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

//! What the frontend may tell a client, and when.
//!
//! A client-visible success is the one thing in this system that cannot be
//! taken back, so both trackers here are built to be provably conservative:
//! every fact they need is one the frontend itself observed, and a fact that
//! is merely absent never counts in favour of success.
//!
//! Reads and writes deliberately do not share a predicate:
//!
//! * A **read** completes as soon as the root task is `FINISHED`, this
//!   frontend has consumed the end of the root result stream, and no failure
//!   or abort was latched. It does not wait for upstream tasks to finish
//!   standing down, which is what makes a `LIMIT` return without waiting on
//!   whatever was still scanning.
//! * A **write** gets none of that. Every writer and the root finish task must
//!   be `FINISHED`, the prepared write set must be complete, and the external
//!   commit must have succeeded. A cancelled writer can never be counted as
//!   success, because a write that stood down early published less than the
//!   statement asked for.
//!
//! Draining the attempt is a third, separate question owned by
//! [`AttemptDrainFacts`](novarocks_execution::task_execution::AttemptDrainFacts):
//! it closes internal resources and never gates a completion that has already
//! been linearized.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_execution::task_execution::{
    FinalTaskInfo, ResultPacketVerdict, RootResultStream, TaskIdentity, TaskState, TaskStatus,
    verify_final_info,
};

use super::error::TaskExecutionError;

/// What the frontend may tell a client about a read, and why not when it may
/// not.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReadVerdict {
    /// Every fact holds: end of stream may be delivered to the client.
    Complete,
    /// The root task has not published `FINISHED`. A root that is still
    /// flushing owes output, and a cancelled or aborted root is never success.
    RootNotFinished(TaskState),
    /// The root published `FINISHED`, but this frontend has not consumed the
    /// end of its result stream. The backend's own claim that its output
    /// responsibility is complete is not a substitute: only this frontend
    /// knows what it received.
    EndOfStreamNotObserved,
    /// The attempt latched a failure or an abort. A read can never succeed
    /// past one.
    AttemptFailed,
}

impl ReadVerdict {
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// The frontend's own view of one read attempt's completion.
#[derive(Clone, Debug)]
pub struct ReadCompletionTracker {
    root: TaskIdentity,
    stream: RootResultStream,
}

impl ReadCompletionTracker {
    pub const fn new(root: TaskIdentity) -> Self {
        Self {
            root,
            stream: RootResultStream::new(),
        }
    }

    pub const fn root(&self) -> TaskIdentity {
        self.root
    }

    pub const fn stream(&self) -> RootResultStream {
        self.stream
    }

    pub const fn end_of_stream_observed(&self) -> bool {
        self.stream.end_of_stream_observed()
    }

    /// Records one packet the root result plane delivered.
    ///
    /// The identity is checked here rather than assumed: a packet from any
    /// other task, or from a replaced backend process, must not advance the
    /// root's stream, or a foreign response could satisfy this query's
    /// completion.
    pub fn consume_packet(
        &mut self,
        root: TaskIdentity,
        packet_sequence: u64,
        end_of_stream: bool,
    ) -> Result<(), TaskExecutionError> {
        self.root.verify_matches(root)?;
        let verdict = self.stream.consume(packet_sequence, end_of_stream);
        if matches!(verdict, ResultPacketVerdict::Accept) {
            return Ok(());
        }
        Err(TaskExecutionError::ResultStream(verdict))
    }

    /// The verdict over the facts this tracker holds plus the two it cannot
    /// observe on its own.
    ///
    /// `attempt_failed` is the query's first-wins failure latch. It is a
    /// parameter rather than internal state so that a caller cannot forget to
    /// consult it: a completion computed without it would not compile.
    pub fn verdict(&self, root_state: TaskState, attempt_failed: bool) -> ReadVerdict {
        if attempt_failed {
            return ReadVerdict::AttemptFailed;
        }
        if root_state != TaskState::Finished {
            return ReadVerdict::RootNotFinished(root_state);
        }
        if !self.stream.end_of_stream_observed() {
            return ReadVerdict::EndOfStreamNotObserved;
        }
        ReadVerdict::Complete
    }
}

/// Whether this attempt's declared writers all reached their terminal, and
/// why not when they did not.
///
/// This is the task-terminal half of a write's completion and nothing more.
/// Whether a complete prepared write set arrived is the write commit
/// barrier's fact, and whether the external commit succeeded is
/// `finish_write_session`'s. Each of those has exactly one owner, and this
/// type deliberately cannot answer either: a second answer to a question that
/// admits one is how a partial write comes to report success.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WriteVerdict {
    /// Every fact holds.
    Complete,
    /// The attempt latched a failure or an abort.
    AttemptFailed,
    /// A writer or the root finish task is not `FINISHED`.
    TaskNotFinished {
        task: TaskIdentity,
        state: TaskState,
    },
    /// A writer stood down normally. That is compatible with a read's early
    /// completion and never with a write: rows it had not written yet are
    /// simply missing.
    WriterCanceled(TaskIdentity),
    /// A task outside the declared writer set reported writer facts, so the
    /// declared set is not the real one and "every writer finished" cannot be
    /// decided from it.
    UndeclaredWriter(TaskIdentity),
}

impl WriteVerdict {
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// The frontend's own view of one distributed write attempt's completion.
///
/// The writer set is declared at construction from the frozen write plan and
/// never inferred from what the statuses happen to report: writer facts are
/// optional metrics, and absence means "not reported", so inferring the set
/// from them would silently shrink it — and a shrunken writer set is exactly
/// how a partial write reports success.
#[derive(Clone, Debug)]
pub struct WriteCompletionTracker {
    writers: BTreeSet<TaskIdentity>,
    root_finish: TaskIdentity,
    states: BTreeMap<TaskIdentity, TaskState>,
    canceled_writers: BTreeSet<TaskIdentity>,
    undeclared_writers: BTreeSet<TaskIdentity>,
}

impl WriteCompletionTracker {
    /// Freezes the writer set of one write attempt.
    ///
    /// A write with no declared writer is refused rather than treated as a
    /// write that trivially finished: if the caller cannot name the writers,
    /// nothing here can decide that all of them finished.
    pub fn try_new(
        root_finish: TaskIdentity,
        writers: impl IntoIterator<Item = TaskIdentity>,
    ) -> Result<Self, TaskExecutionError> {
        let writers: BTreeSet<TaskIdentity> = writers.into_iter().collect();
        if writers.is_empty() {
            return Err(TaskExecutionError::Schedule(
                "a distributed write declares no writer task".to_owned(),
            ));
        }
        Ok(Self {
            writers,
            root_finish,
            states: BTreeMap::new(),
            canceled_writers: BTreeSet::new(),
            undeclared_writers: BTreeSet::new(),
        })
    }

    pub const fn root_finish_task(&self) -> TaskIdentity {
        self.root_finish
    }

    pub fn writers(&self) -> impl ExactSizeIterator<Item = &TaskIdentity> + '_ {
        self.writers.iter()
    }

    /// Records one observed status snapshot.
    ///
    /// Statuses of tasks this write does not care about are still inspected
    /// for writer facts, because a task that wrote rows without being declared
    /// a writer invalidates the declared set.
    pub fn observe_status(&mut self, status: &TaskStatus) {
        let identity = status.identity();
        let is_declared = self.writers.contains(&identity) || identity == self.root_finish;
        if is_declared {
            self.states.insert(identity, status.state());
            if self.writers.contains(&identity) && status.state() == TaskState::Canceled {
                self.canceled_writers.insert(identity);
            }
            return;
        }
        if status.writer().is_some() {
            self.undeclared_writers.insert(identity);
        }
    }

    /// The task-terminal half of the commit gate.
    ///
    /// The write commit barrier keeps "the prepared write set is complete" and
    /// "execution succeeded" as two independent facts on purpose, so this
    /// reports only the second one.
    pub fn execution_verdict(&self, attempt_failed: bool) -> WriteVerdict {
        if attempt_failed {
            return WriteVerdict::AttemptFailed;
        }
        if let Some(&undeclared) = self.undeclared_writers.iter().next() {
            return WriteVerdict::UndeclaredWriter(undeclared);
        }
        if let Some(&canceled) = self.canceled_writers.iter().next() {
            return WriteVerdict::WriterCanceled(canceled);
        }
        for task in self.writers.iter().copied().chain([self.root_finish]) {
            // An unobserved task is `PLANNED` here rather than absent: not
            // having heard from a writer is not evidence that it finished.
            let state = self
                .states
                .get(&task)
                .copied()
                .unwrap_or(TaskState::Planned);
            if state != TaskState::Finished {
                return WriteVerdict::TaskNotFinished { task, state };
            }
        }
        WriteVerdict::Complete
    }
}

/// Accepts one fetched final task info against the terminal already observed.
///
/// Final info is observation only: a missing one costs diagnostics and nothing
/// else, which is why there is no verdict for absence here. One that
/// contradicts the terminal is a protocol conflict, because it would mean two
/// answers exist to a question with one answer.
pub fn accept_final_info(
    observed_terminal: &TaskStatus,
    info: &FinalTaskInfo,
) -> Result<(), TaskExecutionError> {
    verify_final_info(observed_terminal, info).map_err(TaskExecutionError::FinalInfo)
}

#[cfg(test)]
mod tests {
    use super::{ReadCompletionTracker, ReadVerdict, WriteCompletionTracker, WriteVerdict};
    use crate::task_execution::error::TaskExecutionError;
    use novarocks_execution::task_execution::{
        AbortCause, CancelReason, FinalTaskInfo, IdentityField, IdentityMismatch,
        ResultPacketVerdict, TaskIdentity, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusVersion, TaskWriterFacts, TerminationDetail,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(5, 6), AttemptId::new(1).expect("nonzero"))
            .expect("nonzero query")
    }

    fn identity(stage: u32, task: u32, backend: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            execution(),
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            backend,
        )
    }

    fn status(id: TaskIdentity, version: u64, state: TaskState) -> TaskStatus {
        let termination = match state {
            TaskState::Canceled | TaskState::Canceling => Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskState::Aborted | TaskState::Aborting => {
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed))
            }
            _ => None,
        };
        let output = if state == TaskState::Finished {
            TaskOutputFacts::new(true)
        } else {
            TaskOutputFacts::default()
        };
        TaskStatus::try_new(
            id,
            TaskStatusVersion::new(version).expect("nonzero version"),
            state,
            termination,
            output,
        )
        .expect("a legal snapshot")
    }

    #[test]
    fn a_read_needs_the_root_finished_and_this_frontends_own_end_of_stream() {
        let backend = BackendProcessId::new_v7();
        let root = identity(9, 1, backend);
        let mut tracker = ReadCompletionTracker::new(root);

        assert_eq!(
            tracker.verdict(TaskState::Running, false),
            ReadVerdict::RootNotFinished(TaskState::Running)
        );
        // The backend can report FINISHED before this frontend has read the
        // stream out. Trusting that alone would tell a client a read succeeded
        // that this process never received.
        assert_eq!(
            tracker.verdict(TaskState::Finished, false),
            ReadVerdict::EndOfStreamNotObserved
        );

        tracker
            .consume_packet(root, 0, false)
            .expect("first packet");
        assert_eq!(
            tracker.verdict(TaskState::Finished, false),
            ReadVerdict::EndOfStreamNotObserved
        );
        tracker
            .consume_packet(root, 1, true)
            .expect("end of stream");
        assert!(tracker.end_of_stream_observed());
        assert!(tracker.verdict(TaskState::Finished, false).is_complete());
    }

    #[test]
    fn a_cancelled_root_and_a_latched_failure_both_refuse_a_read() {
        let backend = BackendProcessId::new_v7();
        let root = identity(9, 1, backend);
        let mut tracker = ReadCompletionTracker::new(root);
        tracker
            .consume_packet(root, 0, true)
            .expect("end of stream");

        // Every non-finished root state is refused, including the terminal
        // ones that a careless "is terminal" check would accept.
        for state in [
            TaskState::Flushing,
            TaskState::Canceling,
            TaskState::Canceled,
            TaskState::Aborted,
            TaskState::Failed,
        ] {
            assert_eq!(
                tracker.verdict(state, false),
                ReadVerdict::RootNotFinished(state),
                "{state}"
            );
        }
        assert_eq!(
            tracker.verdict(TaskState::Finished, true),
            ReadVerdict::AttemptFailed,
            "a latched failure outranks an otherwise complete root"
        );
    }

    #[test]
    fn a_result_packet_from_another_task_cannot_complete_this_read() {
        let root = identity(9, 1, BackendProcessId::new_v7());
        let foreign = identity(9, 1, BackendProcessId::new_v7());
        let mut tracker = ReadCompletionTracker::new(root);

        assert_eq!(
            tracker.consume_packet(foreign, 0, true),
            Err(TaskExecutionError::Identity(IdentityMismatch::new(
                IdentityField::BackendProcess
            )))
        );
        assert!(!tracker.end_of_stream_observed());
        assert_eq!(
            tracker.verdict(TaskState::Finished, false),
            ReadVerdict::EndOfStreamNotObserved
        );
    }

    #[test]
    fn a_lost_result_packet_refuses_the_read_instead_of_truncating_it() {
        let root = identity(9, 1, BackendProcessId::new_v7());
        let mut tracker = ReadCompletionTracker::new(root);
        tracker
            .consume_packet(root, 0, false)
            .expect("first packet");

        assert_eq!(
            tracker.consume_packet(root, 2, true),
            Err(TaskExecutionError::ResultStream(ResultPacketVerdict::Gap {
                expected: 1,
                observed: 2,
            })),
            "packet one was dropped by the backend after delivery, so its rows are gone"
        );
        assert!(
            !tracker.end_of_stream_observed(),
            "a refused packet must not complete the stream"
        );
    }

    #[test]
    fn a_write_needs_every_declared_writer_and_the_root_finish_task() {
        let backend = BackendProcessId::new_v7();
        let writer_one = identity(2, 1, backend);
        let writer_two = identity(2, 2, backend);
        let root = identity(1, 3, backend);
        let mut tracker = WriteCompletionTracker::try_new(root, [writer_one, writer_two])
            .expect("a declared writer set");

        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: writer_one,
                state: TaskState::Planned,
            },
            "silence from a writer is not evidence that it finished"
        );

        tracker.observe_status(&status(writer_one, 2, TaskState::Finished));
        tracker.observe_status(&status(writer_two, 2, TaskState::Flushing));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: writer_two,
                state: TaskState::Flushing,
            }
        );

        tracker.observe_status(&status(writer_two, 3, TaskState::Finished));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: root,
                state: TaskState::Planned,
            },
            "the root finish task is not optional"
        );
        tracker.observe_status(&status(root, 4, TaskState::Finished));
        assert!(tracker.execution_verdict(false).is_complete());

        // What this verdict does NOT say is deliberately not askable here.
        // "A complete prepared write set arrived" belongs to the write commit
        // barrier and "the external commit succeeded" to finish_write_session;
        // both directions of the first are asserted over there, in
        // write_barrier's a_successful_execution_does_not_stand_in_for_a_
        // complete_set and its converse. A second answer here is what this
        // type was trimmed to make unrepresentable.
    }

    #[test]
    fn a_cancelled_writer_can_never_be_counted_as_a_successful_write() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer]).expect("a declared writer set");

        // The exact shape a read is allowed to succeed through: a normal
        // upstream stand-down. For a write it means rows that were never
        // written.
        tracker.observe_status(&status(writer, 2, TaskState::Canceled));
        tracker.observe_status(&status(root, 3, TaskState::Finished));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::WriterCanceled(writer)
        );
    }

    #[test]
    fn a_task_that_wrote_rows_without_being_declared_invalidates_the_writer_set() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let undeclared = identity(2, 7, backend);
        let root = identity(1, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer]).expect("a declared writer set");

        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        assert!(tracker.execution_verdict(false).is_complete());

        // A status carrying writer facts from a task the caller did not
        // declare: "every writer finished" cannot be decided from a set that
        // is missing one.
        let interloper =
            status(undeclared, 2, TaskState::Running).with_writer(TaskWriterFacts::empty());
        tracker.observe_status(&interloper);
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::UndeclaredWriter(undeclared)
        );
    }

    #[test]
    fn a_write_with_no_declared_writer_is_refused_rather_than_trivially_complete() {
        let root = identity(1, 3, BackendProcessId::new_v7());
        assert!(matches!(
            WriteCompletionTracker::try_new(root, []),
            Err(TaskExecutionError::Schedule(_))
        ));
    }

    #[test]
    fn a_latched_failure_refuses_a_write_whose_tasks_all_finished() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer]).expect("a declared writer set");
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        // Every task reached FINISHED, so only the latch stands between this
        // write and a success report. A verdict that read the terminals alone
        // would call it complete.
        assert!(tracker.execution_verdict(false).is_complete());
        assert_eq!(tracker.execution_verdict(true), WriteVerdict::AttemptFailed);
    }

    #[test]
    fn a_final_info_that_contradicts_the_observed_terminal_is_refused() {
        let backend = BackendProcessId::new_v7();
        let root = identity(9, 1, backend);
        let terminal = status(root, 8, TaskState::Finished);
        let info = FinalTaskInfo::try_new(root, terminal.clone(), Vec::new(), false)
            .expect("a matching info");
        assert_eq!(super::accept_final_info(&terminal, &info), Ok(()));

        let later = status(root, 9, TaskState::Finished);
        assert!(matches!(
            super::accept_final_info(&later, &info),
            Err(TaskExecutionError::FinalInfo(_))
        ));
    }
}
