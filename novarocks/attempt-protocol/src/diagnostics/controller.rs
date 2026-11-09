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

//! One original serial diagnostic collector. Business readiness, closing new
//! reads, selecting the immutable full cut, publication, and actual read exit
//! remain independent facts. No transport, timer or clock is owned here.

use super::sources::{DiagnosticSourceAssembly, DiagnosticSourcesState};
use crate::{
    capacity::StepContext,
    error::TaskExecutionError,
    input::DiagnosticRead,
    publication::{
        DiagnosticClosure, DiagnosticMissing, DiagnosticPublication, DiagnosticTaskObservation,
    },
    status_validation::verify_final_status,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, AttemptRequestToken, MonotonicInstant,
};
use novarocks_execution_contract::{TaskIdentity, TaskStatus};
use novarocks_types::TaskId;
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug)]
struct TaskRow {
    identity: TaskIdentity,
    terminal: Option<TaskStatus>,
    observation: DiagnosticTaskObservation,
    started: bool,
}
#[derive(Debug)]
struct PendingRead {
    request: AttemptRequestToken,
    observed: TaskStatus,
    result_received: bool,
    exited: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticReadRequest {
    pub(crate) request: AttemptRequestToken,
    pub(crate) task: TaskIdentity,
    pub(crate) deadline: MonotonicInstant,
}
/// Selection of the original Source allocation for credentialed disposal.
/// The reference transfers no allocation, Work, capacity, disposal authority,
/// or actual-exit proof. The same Source's Cleanup leg resolves it.
#[must_use = "original diagnostic data must be handed to credentialed disposal"]
#[derive(Debug)]
pub(crate) struct DiagnosticDisposal {
    request: AttemptRequestToken,
    task: TaskIdentity,
    information: crate::input::FinalTaskInfoRef,
}
impl DiagnosticDisposal {
    pub(crate) fn into_effect(self) -> crate::effect::EffectBody {
        crate::effect::EffectBody::DisposeDiagnosticInfo {
            request: self.request,
            task: self.task,
            information: self.information,
        }
    }
}
/// Correlation rejection preserves the separate disposal selection. The actual
/// allocation stays with its original Source even if the pure Core unwinds.
/// Deliberately no From conversion: callers must handle error and disposition.
#[must_use = "preserve both the protocol error and original diagnostic disposition"]
#[derive(Debug)]
pub(crate) struct DiagnosticReadRejection {
    error: TaskExecutionError,
    disposal: Option<DiagnosticDisposal>,
}
impl DiagnosticReadRejection {
    pub(crate) fn into_parts(self) -> (TaskExecutionError, Option<DiagnosticDisposal>) {
        (self.error, self.disposal)
    }
}
#[derive(Debug)]
struct FrozenDiagnostic {
    ready: AttemptComputationCut,
    closure: DiagnosticClosure,
    after: Option<TaskId>,
    tasks: Vec<(TaskIdentity, DiagnosticTaskObservation)>,
    sources: DiagnosticSourceAssembly,
}
#[derive(Debug)]
pub(crate) struct DiagnosticCollector {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    enabled: bool,
    ready: Option<AttemptComputationCut>,
    rows: BTreeMap<TaskId, TaskRow>,
    candidates: VecDeque<TaskIdentity>,
    unread: usize,
    pending: Option<PendingRead>,
    reads_closed: bool,
    selected: bool,
    frozen: Option<FrozenDiagnostic>,
}
impl DiagnosticCollector {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        enabled: bool,
        tasks: impl IntoIterator<Item = TaskIdentity>,
    ) -> Self {
        let rows = tasks
            .into_iter()
            .filter(|_| enabled)
            .map(|task| {
                (
                    task.task_id(),
                    TaskRow {
                        identity: task,
                        terminal: None,
                        observation: DiagnosticTaskObservation::Missing(
                            DiagnosticMissing::TaskNotTerminal,
                        ),
                        started: false,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let task_count = rows.len();
        Self {
            activation,
            root,
            enabled,
            ready: None,
            unread: task_count,
            rows,
            candidates: VecDeque::with_capacity(task_count),
            pending: None,
            reads_closed: false,
            selected: false,
            frozen: None,
        }
    }
    pub(crate) fn begin(&mut self, ready: AttemptComputationCut) -> Result<(), TaskExecutionError> {
        if !self.enabled
            || ready.activation != self.activation
            || ready.root != self.root
            || self.ready.is_some_and(|original| original != ready)
        {
            return Err(TaskExecutionError::Schedule(
                "diagnostic readiness differs from its original activation, root or immutable cut"
                    .into(),
            ));
        }
        // Bind only. The first original convergence turn must supply its
        // actual cancellation fact before testing the diagnostic deadline.
        self.ready = Some(ready);
        Ok(())
    }
    /// Invoke before adopting this input's diagnostic result. A Ready RPC at
    /// the deadline cannot enter the immutable cut selected by that same turn.
    pub(crate) fn before_input(
        &mut self,
        now: MonotonicInstant,
        cancelled: bool,
        sources: &DiagnosticSourcesState,
    ) {
        let Some(ready) = self.ready else {
            return;
        };
        if self.selected {
            return;
        }
        let closure = if cancelled {
            Some(DiagnosticClosure::Cancelled)
        } else if now.has_reached(ready.diagnostic_deadline) {
            Some(DiagnosticClosure::ObservationDeadline)
        } else {
            None
        };
        if let Some(closure) = closure {
            self.freeze(now, closure, sources);
        }
    }
    /// Task status has already been accepted by the original protocol owner.
    /// Only the original first terminal enters the graph-bounded candidate set.
    pub(crate) fn observe_terminal(
        &mut self,
        status: &TaskStatus,
    ) -> Result<(), TaskExecutionError> {
        if !self.enabled || self.selected {
            return Ok(());
        }
        let row = self
            .rows
            .get_mut(&status.identity().task_id())
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if row.identity != status.identity() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if !status.is_terminal() {
            return Err(TaskExecutionError::Schedule(
                "diagnostic candidate is not an accepted Task terminal".into(),
            ));
        }
        if row.started {
            return Ok(());
        }
        if row.terminal.is_none() {
            self.candidates.push_back(status.identity());
            row.observation = DiagnosticTaskObservation::Missing(DiagnosticMissing::NotRead);
        }
        row.terminal = Some(status.clone());
        Ok(())
    }
    pub(crate) fn read_ready(&self) -> bool {
        self.enabled
            && self.ready.is_some()
            && !self.reads_closed
            && self.pending.is_none()
            && !self.candidates.is_empty()
    }

    pub(crate) fn deadline(&self) -> Option<MonotonicInstant> {
        if self.selected {
            return None;
        }
        self.ready.map(|ready| ready.diagnostic_deadline)
    }

    pub(crate) fn has_snapshot_pending(&self) -> bool {
        self.frozen.is_some()
    }

    pub(crate) fn requires_snapshot(&self) -> bool {
        self.ready.is_some()
    }

    pub(crate) fn next_read(
        &mut self,
        request: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Option<DiagnosticReadRequest> {
        if !self.read_ready() {
            return None;
        }
        let ready = self.ready.expect("original diagnostic readiness");
        if !context.charge(1) {
            return None;
        }
        let task = self
            .candidates
            .pop_front()
            .expect("original accepted terminal");
        let row = self.rows.get_mut(&task.task_id()).expect("frozen Task row");
        assert!(!row.started, "each original Task is read once");
        row.started = true;
        row.observation = DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadPending);
        self.unread -= 1;
        self.pending = Some(PendingRead {
            request,
            observed: row.terminal.take().expect("original observed terminal"),
            result_received: false,
            exited: false,
        });
        Some(DiagnosticReadRequest {
            request,
            task,
            deadline: ready.diagnostic_deadline,
        })
    }
    fn disposal(
        &self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: DiagnosticRead,
    ) -> Option<DiagnosticDisposal> {
        let DiagnosticRead::Available(info) = result else {
            return None;
        };
        let adopted = self
            .rows
            .get(&info.reference.task().task_id())
            .is_some_and(|row| {
                row.identity == info.reference.task()
                    && row.observation == DiagnosticTaskObservation::Available(info.reference)
            });
        (!adopted).then_some(DiagnosticDisposal {
            request,
            task,
            information: info.reference,
        })
    }
    pub(crate) fn result(
        &mut self,
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: DiagnosticRead,
    ) -> Result<Option<DiagnosticDisposal>, DiagnosticReadRejection> {
        let reference_matches = match &result {
            DiagnosticRead::Available(info) => {
                info.reference.request() == request && info.reference.task() == task
            }
            _ => true,
        };
        let matches = reference_matches
            && self.pending.as_ref().is_some_and(|pending| {
                pending.request == request
                    && pending.observed.identity() == task
                    && !pending.result_received
            });
        if !matches {
            return Err(DiagnosticReadRejection {
                error: TaskExecutionError::UnknownOperation,
                disposal: self.disposal(request, task, result),
            });
        }
        let discarded = if self.selected {
            self.disposal(request, task, result)
        } else {
            let (observation, discarded) = match result {
                DiagnosticRead::Available(info) => {
                    if verify_final_status(
                        &self
                            .pending
                            .as_ref()
                            .expect("same original read correlation")
                            .observed,
                        &info.final_status,
                    )
                    .is_ok()
                    {
                        (DiagnosticTaskObservation::Available(info.reference), None)
                    } else {
                        (
                            DiagnosticTaskObservation::Missing(DiagnosticMissing::TerminalMismatch),
                            Some(DiagnosticDisposal {
                                request,
                                task,
                                information: info.reference,
                            }),
                        )
                    }
                }
                DiagnosticRead::Unavailable(outcome) => (
                    DiagnosticTaskObservation::Missing(DiagnosticMissing::Unavailable(outcome)),
                    None,
                ),
                DiagnosticRead::Failed(_) => (
                    DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadFailed),
                    None,
                ),
            };
            self.rows
                .get_mut(&task.task_id())
                .expect("original Task remains until full cut")
                .observation = observation;
            discarded
        };
        // A selected full cut ignores late data, while retaining the original
        // request correlation until its real capture exit is independently seen.
        let pending = self
            .pending
            .as_mut()
            .expect("same original read correlation");
        pending.result_received = true;
        if pending.exited {
            self.pending = None;
        }
        self.settle_reads();
        Ok(discarded)
    }
    pub(crate) fn actual_exit(
        &mut self,
        request: AttemptRequestToken,
    ) -> Result<(), TaskExecutionError> {
        let pending = self
            .pending
            .as_mut()
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if pending.request != request || pending.exited {
            return Err(TaskExecutionError::UnknownOperation);
        }
        pending.exited = true;
        if pending.result_received {
            self.pending = None;
        }
        self.settle_reads();
        Ok(())
    }
    fn settle_reads(&mut self) {
        if self.enabled
            && self.unread == 0
            && self
                .pending
                .as_ref()
                .is_none_or(|read| read.result_received)
        {
            self.reads_closed = true;
        }
    }
    /// FrontierSettled is the full original snapshot condition, not merely
    /// the point at which the final-info reader stops accepting new reads.
    pub(crate) fn after_input(&mut self, now: MonotonicInstant, sources: &DiagnosticSourcesState) {
        self.settle_reads();
        if !self.selected
            && self.ready.is_some()
            && self.reads_closed
            && sources.all_released()
            && !sources.split_pending()
        {
            self.freeze(now, DiagnosticClosure::FrontierSettled, sources);
        }
    }
    fn freeze(
        &mut self,
        now: MonotonicInstant,
        closure: DiagnosticClosure,
        sources: &DiagnosticSourcesState,
    ) {
        assert!(!self.selected);
        self.selected = true;
        self.reads_closed = true;
        // Retain the original metadata rows after selection/publication. A
        // duplicate reference cannot dispose an already adopted allocation.
        self.frozen = Some(FrozenDiagnostic {
            ready: self.ready.expect("original accepted readiness"),
            closure,
            tasks: Vec::with_capacity(self.rows.len()),
            after: None,
            sources: DiagnosticSourceAssembly::new(sources.freeze(now)),
        });
    }
    /// Failed/unstarted convergence has no readiness window to invent.
    /// Existing reads, if any, keep their original result and actual-exit legs.
    pub(crate) fn stop_starting(&mut self) {
        self.reads_closed = true;
    }
    pub(crate) fn reads_closed(&self) -> bool {
        self.reads_closed
    }
    pub(crate) fn actual_reads_exited(&self) -> bool {
        self.pending.is_none()
    }
    pub(crate) fn selected(&self) -> bool {
        self.selected
    }
    pub(crate) fn has_assembly_work(&self) -> bool {
        self.frozen
            .as_ref()
            .is_some_and(|cut| cut.tasks.len() < self.rows.len() || !cut.sources.complete())
    }
    pub(crate) fn advance(
        &mut self,
        sources: &DiagnosticSourcesState,
        context: &mut dyn StepContext,
    ) -> bool {
        let Some(cut) = self.frozen.as_mut() else {
            return false;
        };
        if cut.tasks.len() < self.rows.len() {
            if !context.charge(1) {
                return false;
            }
            let (&task, row) = match cut.after {
                None => self.rows.first_key_value(),
                Some(after) => self
                    .rows
                    .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .next(),
            }
            .expect("selected original Task row");
            cut.after = Some(task);
            cut.tasks.push((row.identity, row.observation));
            return true;
        }
        cut.sources.advance(sources, context)
    }
    pub(crate) fn take_publication(&mut self) -> Option<DiagnosticPublication> {
        if self.has_assembly_work() {
            return None;
        }
        let cut = self.frozen.take()?;
        Some(DiagnosticPublication {
            ready: cut.ready,
            closure: cut.closure,
            tasks: cut.tasks,
            sources: cut
                .sources
                .finish()
                .expect("charged Source assembly is complete"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        capacity::QueueStage, config::DispatchLane, failure::HostFailureRef,
        publication::SplitDiagnosticObservation, start::PayloadRef,
    };
    use novarocks_execution_contract::{
        OperationOutcome, ParticipationRef, TaskOutputFacts, TaskState, TaskStatusVersion,
        root_result::RootResultEnd,
    };
    use novarocks_types::BackendProcessId;
    use std::{num::NonZeroU64, time::Duration};
    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.0 {
                false
            } else {
                self.0 -= units;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("diagnostics never grant transport capacity")
        }
    }
    fn token(n: u64) -> AttemptRequestToken {
        AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
    }
    fn reference(n: u64) -> PayloadRef {
        PayloadRef(NonZeroU64::new(n).unwrap())
    }
    fn now(n: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(n))
    }
    fn status(task: TaskIdentity, version: u64) -> TaskStatus {
        TaskStatus::try_new(
            task,
            TaskStatusVersion::new(version).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    }
    fn available(request: AttemptRequestToken, task: TaskIdentity, version: u64) -> DiagnosticRead {
        DiagnosticRead::Available(crate::input::FinalTaskInfoProjection {
            reference: crate::input::FinalTaskInfoRef::new(request, task),
            final_status: status(task, version),
        })
    }
    fn setup(
        tasks: usize,
        split: bool,
    ) -> (
        DiagnosticCollector,
        DiagnosticSourcesState,
        Vec<TaskIdentity>,
        Vec<ParticipationRef>,
        AttemptComputationCut,
    ) {
        let remotes = (1..=tasks as u32)
            .map(crate::test_support::remote)
            .collect::<Vec<_>>();
        let ids = remotes
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        let parts = remotes
            .iter()
            .map(|task| task.participation())
            .collect::<Vec<_>>();
        let root = ids[0];
        let activation = AttemptActivationIdentity::from_parts(1, root.query_execution_id(), 1);
        let cut = AttemptComputationCut {
            activation,
            root,
            cut: now(0),
            diagnostic_deadline: now(10),
            convergence_deadline: now(20),
            end: RootResultEnd {
                sequence: NonZeroU64::new(1).unwrap(),
                output_rows: 1,
            },
        };
        (
            DiagnosticCollector::new(activation, root, true, ids.iter().copied()),
            DiagnosticSourcesState::new(parts.clone(), split.then_some(reference(99))),
            ids,
            parts,
            cut,
        )
    }
    fn materialize(
        collector: &mut DiagnosticCollector,
        sources: &DiagnosticSourcesState,
    ) -> DiagnosticPublication {
        assert!(collector.take_publication().is_none());
        assert!(!collector.advance(sources, &mut Turn(0)));
        while collector.has_assembly_work() {
            assert!(collector.advance(sources, &mut Turn(1)));
        }
        let cut = collector.take_publication().unwrap();
        assert!(collector.take_publication().is_none());
        assert!(!collector.advance(sources, &mut Turn(1)));
        cut
    }
    #[test]
    fn read_frontier_full_cut_publication_and_actual_exit_are_separate_boundaries() {
        let (mut collector, mut sources, tasks, parts, ready) = setup(1, true);
        collector.begin(ready).unwrap();
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        collector.next_read(token(1), &mut Turn(1)).unwrap();
        collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap();
        collector.after_input(now(1), &sources);
        assert!(collector.reads_closed());
        assert!(!collector.selected());
        assert!(!collector.actual_reads_exited());
        sources
            .observe_release(parts[0], true, Some(reference(5)))
            .unwrap();
        collector.after_input(now(2), &sources);
        assert!(!collector.selected());
        sources
            .observe_split(reference(99), SplitDiagnosticObservation::Failed)
            .unwrap();
        collector.after_input(now(3), &sources);
        assert!(collector.selected());
        let cut = materialize(&mut collector, &sources);
        assert_eq!(cut.ready.cut, now(0));
        assert_eq!(cut.sources.selected_at, now(3));
        assert_eq!(cut.closure, DiagnosticClosure::FrontierSettled);
        assert_eq!(cut.sources.split, SplitDiagnosticObservation::Failed);
        assert!(cut.sources.runtime_filters.complete);
        assert_eq!(
            cut.sources.runtime_filters.contributions,
            vec![(parts[0].backend_process_id(), reference(5))]
        );
        assert!(matches!(
            cut.tasks[0].1,
            DiagnosticTaskObservation::Available(_)
        ));
        assert!(!collector.actual_reads_exited());
        collector.actual_exit(token(1)).unwrap();
        assert!(collector.actual_reads_exited());
    }
    #[test]
    fn deadline_freezes_pending_read_and_all_sources_before_later_results_or_assembly() {
        let (mut collector, mut sources, tasks, parts, ready) = setup(3, true);
        collector.begin(ready).unwrap();
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        collector.observe_terminal(&status(tasks[1], 1)).unwrap();
        collector.next_read(token(1), &mut Turn(1)).unwrap();
        collector.before_input(now(10), false, &sources);
        collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap();
        // Final-info, Task status, RF and split completion after the original
        // cut must not improve any row while bounded materialization runs.
        collector.observe_terminal(&status(tasks[2], 1)).unwrap();
        assert!(collector.advance(&sources, &mut Turn(1)));
        for part in parts {
            sources
                .observe_release(part, true, Some(reference(8)))
                .unwrap();
        }
        sources
            .observe_split(
                reference(99),
                SplitDiagnosticObservation::Available(reference(100)),
            )
            .unwrap();
        collector.after_input(now(11), &sources);
        let cut = materialize(&mut collector, &sources);
        assert_eq!(cut.sources.selected_at, now(10));
        assert_eq!(cut.closure, DiagnosticClosure::ObservationDeadline);
        assert!(!cut.sources.runtime_filters.complete);
        assert!(cut.sources.runtime_filters.contributions.is_empty());
        assert_eq!(cut.sources.split, SplitDiagnosticObservation::Pending);
        for (index, missing) in [
            DiagnosticMissing::ReadPending,
            DiagnosticMissing::NotRead,
            DiagnosticMissing::TaskNotTerminal,
        ]
        .into_iter()
        .enumerate()
        {
            assert!(
                matches!(cut.tasks[index].1,DiagnosticTaskObservation::Missing(actual) if actual==missing)
            );
        }
        assert!(!collector.actual_reads_exited());
        collector.actual_exit(token(1)).unwrap();
        assert!(collector.actual_reads_exited());
    }
    #[test]
    fn original_actual_exit_gates_next_read_but_cannot_fabricate_result_completion() {
        let (mut collector, _sources, tasks, _, ready) = setup(2, false);
        collector.begin(ready).unwrap();
        for task in &tasks {
            collector.observe_terminal(&status(*task, 1)).unwrap();
        }
        assert!(collector.next_read(token(1), &mut Turn(0)).is_none());
        let first = collector.next_read(token(1), &mut Turn(1)).unwrap();
        collector.actual_exit(first.request).unwrap();
        assert!(!collector.actual_reads_exited());
        assert!(collector.next_read(token(2), &mut Turn(1)).is_none());
        collector
            .result(
                first.request,
                first.task,
                DiagnosticRead::Unavailable(OperationOutcome::Gone),
            )
            .unwrap();
        let second = collector.next_read(token(2), &mut Turn(1)).unwrap();
        collector
            .result(
                second.request,
                second.task,
                available(second.request, second.task, 1),
            )
            .unwrap();
        assert!(collector.reads_closed());
        assert!(!collector.actual_reads_exited());
        assert!(collector.next_read(token(3), &mut Turn(1)).is_none());
        collector.actual_exit(second.request).unwrap();
        assert!(collector.actual_reads_exited());
    }
    #[test]
    fn selected_cancellation_has_priority_and_never_gets_replaced_by_deadline() {
        let (mut collector, mut sources, tasks, parts, ready) = setup(1, false);
        collector.begin(ready).unwrap();
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        sources.observe_release(parts[0], true, None).unwrap();
        collector.before_input(now(10), true, &sources);
        collector.before_input(now(11), false, &sources);
        collector.after_input(now(12), &sources);
        let cut = materialize(&mut collector, &sources);
        assert_eq!(cut.closure, DiagnosticClosure::Cancelled);
        assert_eq!(cut.sources.selected_at, now(10));
        assert_eq!(cut.sources.split, SplitDiagnosticObservation::Absent);
        assert!(collector.reads_closed());
        assert!(collector.actual_reads_exited());
    }
    #[test]
    fn read_failure_and_terminal_mismatch_are_diagnostic_missing_not_attempt_failure() {
        let (mut collector, mut sources, tasks, parts, ready) = setup(3, false);
        collector.begin(ready).unwrap();
        for task in &tasks {
            collector.observe_terminal(&status(*task, 2)).unwrap();
        }
        let results = [
            available(token(1), tasks[0], 1),
            DiagnosticRead::Failed(HostFailureRef(NonZeroU64::new(55).unwrap())),
            DiagnosticRead::Unavailable(OperationOutcome::Gone),
        ];
        for (index, result) in results.into_iter().enumerate() {
            let request = collector
                .next_read(token(index as u64 + 1), &mut Turn(1))
                .unwrap();
            collector
                .result(request.request, request.task, result)
                .unwrap();
            collector.actual_exit(request.request).unwrap();
        }
        for part in parts {
            sources.observe_release(part, true, None).unwrap();
        }
        collector.after_input(now(5), &sources);
        let cut = materialize(&mut collector, &sources);
        for (index, missing) in [
            DiagnosticMissing::TerminalMismatch,
            DiagnosticMissing::ReadFailed,
            DiagnosticMissing::Unavailable(OperationOutcome::Gone),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(
                matches!(cut.tasks[index].1,DiagnosticTaskObservation::Missing(actual) if actual==missing)
            );
        }
        assert_eq!(cut.closure, DiagnosticClosure::FrontierSettled);
    }
    #[test]
    fn foreign_ready_and_request_cannot_replace_original_selection_or_pending_job() {
        let (mut collector, sources, tasks, _, ready) = setup(2, false);
        let mut foreign = ready;
        foreign.root = tasks[1];
        assert!(collector.begin(foreign).is_err());
        collector.begin(ready).unwrap();
        foreign = ready;
        foreign.diagnostic_deadline = now(11);
        assert!(collector.begin(foreign).is_err());
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        collector.next_read(token(1), &mut Turn(1)).unwrap();
        assert!(
            collector
                .result(token(2), tasks[0], available(token(1), tasks[0], 1))
                .is_err()
        );
        assert!(
            collector
                .result(token(1), tasks[1], available(token(1), tasks[1], 1))
                .is_err()
        );
        assert!(collector.actual_exit(token(2)).is_err());
        collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap();
        assert!(
            collector
                .result(token(1), tasks[0], available(token(1), tasks[0], 1))
                .is_err()
        );
        collector.before_input(now(10), false, &sources);
        collector.actual_exit(token(1)).unwrap();
        assert!(collector.actual_exit(token(1)).is_err());
        let cut = materialize(&mut collector, &sources);
        assert!(matches!(
            cut.tasks[0].1,
            DiagnosticTaskObservation::Available(_)
        ));
    }
    #[test]
    fn failed_unstarted_cleanup_does_not_invent_a_diagnostic_window_or_rpc() {
        let (mut collector, sources, tasks, _, _) = setup(1, false);
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        collector.stop_starting();
        collector.before_input(now(100), true, &sources);
        collector.after_input(now(100), &sources);
        assert!(collector.next_read(token(1), &mut Turn(1)).is_none());
        assert!(collector.take_publication().is_none());
        assert!(!collector.selected());
        assert!(collector.actual_reads_exited());
    }
    #[test]
    fn canonical_task_order_is_independent_of_stage_order_and_read_arrival_order() {
        let (_, mut sources, original, parts, mut ready) = setup(2, false);
        let tasks = original
            .iter()
            .enumerate()
            .map(|(index, task)| {
                TaskIdentity::new(
                    task.query_execution_id(),
                    novarocks_types::StageId::new(2 - index as u32).unwrap(),
                    task.task_id(),
                    task.backend_process_id(),
                )
            })
            .collect::<Vec<_>>();
        ready.root = tasks[0];
        let mut collector = DiagnosticCollector::new(
            ready.activation,
            ready.root,
            true,
            tasks.iter().rev().copied(),
        );
        collector.begin(ready).unwrap();
        for task in tasks.iter().rev() {
            collector.observe_terminal(&status(*task, 1)).unwrap();
        }
        for index in 0..2 {
            let read = collector.next_read(token(index + 1), &mut Turn(1)).unwrap();
            assert_eq!(read.task, tasks[1 - index as usize]);
            collector
                .result(
                    read.request,
                    read.task,
                    available(read.request, read.task, 1),
                )
                .unwrap();
            collector.actual_exit(read.request).unwrap();
        }
        for part in parts {
            sources.observe_release(part, true, None).unwrap();
        }
        collector.after_input(now(3), &sources);
        let cut = materialize(&mut collector, &sources);
        assert_eq!(
            cut.tasks.iter().map(|(task, _)| *task).collect::<Vec<_>>(),
            tasks
        );
    }
    #[test]
    fn disabled_diagnostics_cannot_bind_or_publish_a_readiness_window() {
        let (_, mut sources, tasks, parts, ready) = setup(1, false);
        let mut collector =
            DiagnosticCollector::new(ready.activation, ready.root, false, tasks.iter().copied());
        assert!(collector.begin(ready).is_err());
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        sources.observe_release(parts[0], true, None).unwrap();
        collector.after_input(now(1), &sources);
        collector.before_input(now(10), false, &sources);
        assert!(!collector.selected());
        assert!(collector.next_read(token(1), &mut Turn(1)).is_none());
        assert!(collector.take_publication().is_none());
    }
    #[test]
    fn overdue_ready_binding_keeps_actual_cancel_priority_for_the_first_convergence_turn() {
        let (mut collector, sources, _, _, ready) = setup(1, false);
        collector.begin(ready).unwrap();
        assert!(!collector.selected());
        collector.before_input(now(11), true, &sources);
        let cut = materialize(&mut collector, &sources);
        assert_eq!(cut.closure, DiagnosticClosure::Cancelled);
        assert_eq!(cut.sources.selected_at, now(11));
    }
    #[test]
    fn late_mismatched_and_foreign_results_return_original_ref_for_source_disposal() {
        for mode in 0..3 {
            let (mut collector, sources, tasks, _, ready) = setup(1, false);
            collector.begin(ready).unwrap();
            collector.observe_terminal(&status(tasks[0], 2)).unwrap();
            collector.next_read(token(1), &mut Turn(1)).unwrap();
            if mode == 0 {
                collector.before_input(now(10), false, &sources);
            }
            let reference = crate::input::FinalTaskInfoRef::new(token(1), tasks[0]);
            let incoming = DiagnosticRead::Available(crate::input::FinalTaskInfoProjection {
                reference,
                final_status: status(tasks[0], if mode == 1 { 1 } else { 2 }),
            });
            let request = if mode == 2 { token(2) } else { token(1) };
            let returned = match collector.result(request, tasks[0], incoming) {
                Ok(disposal) => disposal.unwrap(),
                Err(rejected) => {
                    let (error, disposal) = rejected.into_parts();
                    assert!(matches!(error, TaskExecutionError::UnknownOperation));
                    disposal.unwrap()
                }
            };
            let crate::effect::EffectBody::DisposeDiagnosticInfo {
                request: original_request,
                task,
                information,
            } = returned.into_effect()
            else {
                unreachable!()
            };
            assert_eq!(original_request, request);
            assert_eq!(task, tasks[0]);
            assert_eq!(information, reference);
            assert!(!collector.actual_reads_exited());
        }
    }
    #[test]
    fn repeated_reference_cannot_dispose_adopted_information_before_or_after_publication() {
        let (mut collector, mut sources, tasks, parts, ready) = setup(1, false);
        collector.begin(ready).unwrap();
        collector.observe_terminal(&status(tasks[0], 1)).unwrap();
        collector.next_read(token(1), &mut Turn(1)).unwrap();
        collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap();
        let (_, disposal) = collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap_err()
            .into_parts();
        assert!(disposal.is_none());
        for part in parts {
            sources.observe_release(part, true, None).unwrap();
        }
        collector.after_input(now(1), &sources);
        let cut = materialize(&mut collector, &sources);
        assert_eq!(
            cut.tasks[0].1,
            DiagnosticTaskObservation::Available(crate::input::FinalTaskInfoRef::new(
                token(1),
                tasks[0]
            ))
        );
        let (_, disposal) = collector
            .result(token(1), tasks[0], available(token(1), tasks[0], 1))
            .unwrap_err()
            .into_parts();
        assert!(disposal.is_none());
        assert!(!collector.actual_reads_exited());
        collector.actual_exit(token(1)).unwrap();
    }
}
