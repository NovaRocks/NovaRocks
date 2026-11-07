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

//! Writer and per-kind summary-source terminals and accepted final-info validation.
//!
//! The registered result consumer proves complete local End consumption.
//! This module owns only the independent Task-terminal half for writers and
//! summary sources; prepared facts and external publication stay with
//! their original domain authors. Protocol terminals never prove physical exit.

use novarocks_result_contract::{PerKind, SummaryKind};
use std::collections::{BTreeMap, BTreeSet};

use crate::task_execution::status_validation::verify_final_info;
use novarocks_execution::task_execution::{FinalTaskInfo, TaskIdentity, TaskState, TaskStatus};

use super::error::TaskExecutionError;

/// Whether this attempt's declared writers and summary sources completed, and
/// why not when they did not.
///
/// This is the task-terminal half of a write's completion and nothing more.
/// Complete prepared facts belong to the registered write consumer, and
/// external publication belongs to the original Session and statement owner.
/// Each of those has exactly one owner, and this
/// type deliberately cannot answer either: a second answer to a question that
/// admits one is how a partial write comes to report success.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WriteVerdict {
    /// Every fact holds.
    Complete,
    /// The attempt latched a failure or an abort.
    AttemptFailed,
    /// A writer, declared summary source, or root finish task has not completed.
    TaskNotFinished {
        task: TaskIdentity,
        state: TaskState,
        summary_kind: Option<SummaryKind>,
    },
    /// A task outside the declared writer set reported writer facts, so the
    /// declared set is not the real one and "every writer finished" cannot be
    /// decided from it.
    UndeclaredWriter(TaskIdentity),
}

impl WriteVerdict {
    /// Whether this verdict can still become complete from a later status.
    ///
    /// Silence and live task states are pending. Every other non-complete
    /// verdict is already conclusive and must fail the statement immediately;
    /// waiting for a deadline cannot produce a different terminal fact.
    pub const fn is_pending(self) -> bool {
        matches!(
            self,
            Self::TaskNotFinished { state, .. } if !state.is_terminal()
        )
    }
}

impl std::fmt::Display for WriteVerdict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Complete => formatter.write_str("complete"),
            Self::AttemptFailed => formatter.write_str("attempt failed"),
            Self::TaskNotFinished {
                task,
                state,
                summary_kind,
            } => {
                if let Some(kind) = summary_kind {
                    write!(formatter, "{kind} summary task {task} is {state}")
                } else {
                    write!(formatter, "task {task} is {state}")
                }
            }
            Self::UndeclaredWriter(task) => {
                write!(
                    formatter,
                    "undeclared writer task {task} reported write facts"
                )
            }
        }
    }
}

/// The frontend's own view of one distributed write attempt's completion.
///
/// The writer set is declared at construction from the frozen write plan and
/// never inferred from what the statuses happen to report: writer facts are
/// optional metrics, and absence means "not reported", so inferring the set
/// from them would silently shrink it — and a shrunken writer set is exactly
/// how a partial write reports success.
// Design: ADR-0219 (docs/adr/ADR-0219-typed-task-summaries-and-per-kind-completion.md)
#[derive(Clone, Debug)]
pub struct WriteCompletionTracker {
    writers: BTreeSet<TaskIdentity>,
    summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    root_finish: TaskIdentity,
    // Each original writer remains here until its current status satisfies
    // this owner's success-compatible rule. Verdicts never rescan completed writers.
    pending_writers: BTreeMap<TaskIdentity, TaskState>,
    pending_summary_sources: PerKind<BTreeMap<TaskIdentity, TaskState>>,
    root_state: TaskState,
    undeclared_writers: BTreeSet<TaskIdentity>,
}

impl WriteCompletionTracker {
    /// Freezes independent writer and per-kind summary-source Task sets.
    /// Ordinary writes declare an empty summary-source set.
    ///
    /// A write with no declared writer is refused rather than treated as a
    /// write that trivially finished: if the caller cannot name the writers,
    /// nothing here can decide that all of them finished.
    pub fn try_new(
        root_finish: TaskIdentity,
        writers: impl IntoIterator<Item = TaskIdentity>,
        summary_sources: PerKind<BTreeSet<TaskIdentity>>,
    ) -> Result<Self, TaskExecutionError> {
        let writers: BTreeSet<TaskIdentity> = writers.into_iter().collect();
        if writers.is_empty() {
            return Err(TaskExecutionError::Schedule(
                "a distributed write declares no writer task".to_owned(),
            ));
        }
        if summary_sources
            .iter()
            .any(|(_, sources)| sources.is_empty())
        {
            return Err(TaskExecutionError::Schedule(
                "a declared summary kind has no source task".to_owned(),
            ));
        }
        let pending_writers = writers
            .iter()
            .map(|&writer| (writer, TaskState::Planned))
            .collect();
        let mut pending_summary_sources = PerKind::empty();
        for (kind, sources) in summary_sources.iter() {
            pending_summary_sources.insert(
                kind,
                sources
                    .iter()
                    .map(|&source| (source, TaskState::Planned))
                    .collect(),
            );
        }
        Ok(Self {
            writers,
            summary_sources,
            root_finish,
            pending_writers,
            pending_summary_sources,
            root_state: TaskState::Planned,
            undeclared_writers: BTreeSet::new(),
        })
    }

    /// Records one observed status snapshot.
    ///
    /// Statuses of tasks this write does not care about are still inspected
    /// for writer facts, because a task that wrote rows without being declared
    /// a writer invalidates the declared set.
    pub fn observe_status(&mut self, status: &TaskStatus) {
        let identity = status.identity();
        let is_writer = self.writers.contains(&identity);
        if identity == self.root_finish {
            self.root_state = status.state();
        }
        if is_writer {
            if status.state() == TaskState::Finished
                || (status.state() == TaskState::Canceled
                    && status.is_success_compatible_terminal())
            {
                self.pending_writers.remove(&identity);
            } else {
                self.pending_writers.insert(identity, status.state());
            }
        }
        for (kind, sources) in self.summary_sources.iter() {
            if sources.contains(&identity) {
                // Original BE retirement proves strict delivery; membership and
                // FE status observation never manufacture a physical-exit fact.
                let pending = self
                    .pending_summary_sources
                    .get_mut(kind)
                    .expect("pending kinds retain the frozen source declaration");
                if status.state() == TaskState::Finished
                    || (status.state() == TaskState::Canceled
                        && status.is_success_compatible_terminal())
                {
                    pending.remove(&identity);
                } else {
                    pending.insert(identity, status.state());
                }
            }
        }
        // Summary membership grants no writer authority.
        if is_writer || identity == self.root_finish {
            return;
        }
        if status.writer().is_some() {
            self.undeclared_writers.insert(identity);
        }
    }

    /// The task-terminal half of the commit gate.
    ///
    /// The registered write consumer checks the complete Root payload and
    /// this verdict independently before accepting the original Session union.
    /// This tracker reports only the Task-terminal fact.
    pub fn execution_verdict(&self, attempt_failed: bool) -> WriteVerdict {
        if attempt_failed {
            return WriteVerdict::AttemptFailed;
        }
        if let Some(&undeclared) = self.undeclared_writers.iter().next() {
            return WriteVerdict::UndeclaredWriter(undeclared);
        }
        if let Some((&task, &state)) = self.pending_writers.first_key_value() {
            return WriteVerdict::TaskNotFinished {
                task,
                state,
                summary_kind: None,
            };
        }
        for (kind, pending) in self.pending_summary_sources.iter() {
            if let Some((&task, &state)) = pending.first_key_value() {
                return WriteVerdict::TaskNotFinished {
                    task,
                    state,
                    summary_kind: Some(kind),
                };
            }
        }
        if self.root_state != TaskState::Finished {
            return WriteVerdict::TaskNotFinished {
                task: self.root_finish,
                state: self.root_state,
                summary_kind: None,
            };
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
    use super::{PerKind, SummaryKind, WriteCompletionTracker, WriteVerdict};
    use crate::task_execution::error::TaskExecutionError;
    use novarocks_execution::task_execution::{
        AbortCause, CancelReason, FinalTaskInfo, TaskIdentity, TaskOutputFacts, TaskState,
        TaskStatus, TaskStatusVersion, TaskWriterFacts, TerminationDetail,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::collections::BTreeSet;

    fn summaries(
        sources: impl IntoIterator<Item = TaskIdentity>,
    ) -> PerKind<BTreeSet<TaskIdentity>> {
        PerKind {
            write_counts: Some(sources.into_iter().collect()),
            delete_verification: None,
        }
    }

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
    fn root_and_writers_cannot_replace_a_missing_summary_source_terminal() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], summaries([source])).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: source,
                state: TaskState::Planned,
                summary_kind: Some(SummaryKind::WriteCounts),
            }
        );
        tracker.observe_status(&status(source, 2, TaskState::Running));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: source,
                state: TaskState::Running,
                summary_kind: Some(SummaryKind::WriteCounts),
            }
        );
        tracker.observe_status(&status(source, 3, TaskState::Finished));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
        assert_eq!(tracker.execution_verdict(true), WriteVerdict::AttemptFailed);
    }

    #[test]
    fn a_foreign_process_status_cannot_satisfy_an_exact_summary_source() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let foreign = identity(3, 3, BackendProcessId::new_v7());
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], summaries([source])).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(foreign, 2, TaskState::Finished));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: source,
                state: TaskState::Planned,
                summary_kind: Some(SummaryKind::WriteCounts),
            }
        );
        tracker.observe_status(&status(source, 2, TaskState::Aborted));
        let verdict = tracker.execution_verdict(false);
        assert_eq!(
            verdict,
            WriteVerdict::TaskNotFinished {
                task: source,
                state: TaskState::Aborted,
                summary_kind: Some(SummaryKind::WriteCounts),
            }
        );
        assert!(
            !verdict.is_pending(),
            "a non-success terminal cannot become a successful wait"
        );
    }

    #[test]
    fn a_summary_source_is_not_an_undeclared_writer_exemption() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], summaries([source])).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(
            &status(source, 2, TaskState::Finished).with_writer(TaskWriterFacts::empty()),
        );
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::UndeclaredWriter(source)
        );
    }

    #[test]
    fn one_exact_task_status_can_satisfy_multiple_declared_roles() {
        let root = identity(1, 1, BackendProcessId::new_v7());
        let mut tracker = WriteCompletionTracker::try_new(root, [root], summaries([root])).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
    }

    #[test]
    fn summary_sources_use_the_original_success_compatible_terminal_rule() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], summaries([source])).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(source, 2, TaskState::Canceled));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
    }

    #[test]
    fn a_write_needs_every_declared_writer_and_the_root_finish_task() {
        let backend = BackendProcessId::new_v7();
        let writer_one = identity(2, 1, backend);
        let writer_two = identity(2, 2, backend);
        let root = identity(1, 3, backend);
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer_one, writer_two], PerKind::empty())
                .expect("a declared writer set");

        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: writer_one,
                state: TaskState::Planned,
                summary_kind: None,
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
                summary_kind: None,
            }
        );

        tracker.observe_status(&status(writer_two, 3, TaskState::Finished));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: root,
                state: TaskState::Planned,
                summary_kind: None,
            },
            "the root finish task is not optional"
        );
        tracker.observe_status(&status(root, 4, TaskState::Finished));
        assert!(tracker.execution_verdict(false) == WriteVerdict::Complete);

        // Complete Root payload belongs to the registered write consumer;
        // external publication belongs to the original Session/Call. This
        // tracker provides neither of those independent facts.
    }

    #[test]
    fn a_success_compatible_writer_cancel_is_accepted_by_the_task_half() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 3, backend);
        let mut tracker = WriteCompletionTracker::try_new(root, [writer], PerKind::empty())
            .expect("a declared writer set");

        // The task verdict is only one half of the write gate. This normal
        // stand-down is accepted here because the other half -- a complete
        // Root prepared set -- can exist only after this writer's sender
        // reached EOS.
        tracker.observe_status(&status(writer, 2, TaskState::Canceled));
        assert_eq!(
            tracker.execution_verdict(false),
            WriteVerdict::TaskNotFinished {
                task: root,
                state: TaskState::Planned,
                summary_kind: None,
            },
            "a normal writer cancel is not sufficient without the Root terminal"
        );
        tracker.observe_status(&status(root, 3, TaskState::Finished));
        assert!(tracker.execution_verdict(false) == WriteVerdict::Complete);
    }

    #[test]
    fn a_writer_cancel_with_redundant_output_evidence_is_also_accepted() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 3, backend);
        let mut tracker = WriteCompletionTracker::try_new(root, [writer], PerKind::empty())
            .expect("a declared writer set");

        let completed_then_canceled = TaskStatus::try_new(
            writer,
            TaskStatusVersion::new(2).expect("nonzero version"),
            TaskState::Canceled,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::new(true),
        )
        .expect("a legal terminal");
        tracker.observe_status(&completed_then_canceled);
        tracker.observe_status(&status(root, 3, TaskState::Finished));

        assert!(tracker.execution_verdict(false) == WriteVerdict::Complete);
    }

    #[test]
    fn a_task_that_wrote_rows_without_being_declared_invalidates_the_writer_set() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let undeclared = identity(2, 7, backend);
        let root = identity(1, 3, backend);
        let mut tracker = WriteCompletionTracker::try_new(root, [writer], PerKind::empty())
            .expect("a declared writer set");

        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        assert!(tracker.execution_verdict(false) == WriteVerdict::Complete);

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
            WriteCompletionTracker::try_new(root, [], PerKind::empty()),
            Err(TaskExecutionError::Schedule(_))
        ));
    }

    #[test]
    fn a_latched_failure_refuses_a_write_whose_tasks_all_finished() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 3, backend);
        let mut tracker = WriteCompletionTracker::try_new(root, [writer], PerKind::empty())
            .expect("a declared writer set");
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        // Every task reached FINISHED, so only the latch stands between this
        // write and a success report. A verdict that read the terminals alone
        // would call it complete.
        assert!(tracker.execution_verdict(false) == WriteVerdict::Complete);
        assert_eq!(tracker.execution_verdict(true), WriteVerdict::AttemptFailed);
    }

    #[test]
    fn original_writer_status_distinguishes_complete_pending_and_refused() {
        let backend = BackendProcessId::new_v7();
        let writer = identity(2, 1, backend);
        let root = identity(1, 1, backend);
        for state in [TaskState::Finished, TaskState::Running, TaskState::Aborted] {
            let mut tracker =
                WriteCompletionTracker::try_new(root, [writer], PerKind::empty()).unwrap();
            tracker.observe_status(&status(root, 2, TaskState::Finished));
            tracker.observe_status(&status(writer, 2, state));
            let verdict = tracker.execution_verdict(false);
            if state == TaskState::Finished {
                assert_eq!(verdict, WriteVerdict::Complete);
            } else {
                assert_eq!(
                    verdict,
                    WriteVerdict::TaskNotFinished {
                        task: writer,
                        state,
                        summary_kind: None,
                    }
                );
                assert_eq!(verdict.is_pending(), state == TaskState::Running);
            }
        }
    }

    #[test]
    fn only_live_or_absent_task_states_are_pending_write_verdicts() {
        let task = identity(2, 1, BackendProcessId::new_v7());
        for state in [
            TaskState::Planned,
            TaskState::Running,
            TaskState::Flushing,
            TaskState::Canceling,
            TaskState::Aborting,
            TaskState::Failing,
        ] {
            assert!(
                WriteVerdict::TaskNotFinished {
                    task,
                    state,
                    summary_kind: None,
                }
                .is_pending(),
                "{state} may still publish a later terminal"
            );
        }
        for state in [
            TaskState::Finished,
            TaskState::Canceled,
            TaskState::Aborted,
            TaskState::Failed,
        ] {
            assert!(
                !WriteVerdict::TaskNotFinished {
                    task,
                    state,
                    summary_kind: None,
                }
                .is_pending(),
                "{state} is conclusive"
            );
        }
        assert!(!WriteVerdict::AttemptFailed.is_pending());
        assert!(!WriteVerdict::UndeclaredWriter(task).is_pending());
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

    #[test]
    fn each_summary_kind_requires_its_own_exact_sources_and_keeps_repeated_status_idempotent() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let declared = PerKind {
            write_counts: Some([writer].into()),
            delete_verification: Some([source].into()),
        };
        let mut tracker = WriteCompletionTracker::try_new(root, [writer], declared).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        let pending = WriteVerdict::TaskNotFinished {
            task: source,
            state: TaskState::Planned,
            summary_kind: Some(SummaryKind::DeleteVerification),
        };
        assert_eq!(tracker.execution_verdict(false), pending);
        assert!(pending.to_string().contains("delete-verification"));
        let foreign = identity(3, 3, BackendProcessId::new_v7());
        tracker.observe_status(&status(foreign, 2, TaskState::Finished));
        assert_eq!(tracker.execution_verdict(false), pending);
        tracker.observe_status(&status(source, 2, TaskState::Canceled));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
    }

    #[test]
    fn one_task_can_finish_both_summary_kinds_without_granting_writer_authority() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        let source = identity(3, 3, backend);
        let declared = PerKind {
            write_counts: Some([source].into()),
            delete_verification: Some([source].into()),
        };
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], declared.clone()).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        tracker.observe_status(&status(source, 2, TaskState::Finished));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
        let mut refused = WriteCompletionTracker::try_new(root, [writer], declared).unwrap();
        refused.observe_status(
            &status(source, 2, TaskState::Finished).with_writer(TaskWriterFacts::empty()),
        );
        assert_eq!(
            refused.execution_verdict(false),
            WriteVerdict::UndeclaredWriter(source)
        );
    }

    #[test]
    fn a_present_summary_kind_requires_a_real_source_but_absent_kinds_are_legal() {
        let backend = BackendProcessId::new_v7();
        let root = identity(1, 1, backend);
        let writer = identity(2, 2, backend);
        for kind in SummaryKind::ALL {
            let mut sources = PerKind::empty();
            sources.insert(kind, BTreeSet::new());
            assert!(matches!(
                WriteCompletionTracker::try_new(root, [writer], sources),
                Err(TaskExecutionError::Schedule(_)),
            ));
        }
        let mut tracker =
            WriteCompletionTracker::try_new(root, [writer], PerKind::empty()).unwrap();
        tracker.observe_status(&status(root, 2, TaskState::Finished));
        tracker.observe_status(&status(writer, 2, TaskState::Finished));
        assert_eq!(tracker.execution_verdict(false), WriteVerdict::Complete);
    }
}
