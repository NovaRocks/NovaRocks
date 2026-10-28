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

//! The owner's private state: one entry per query context, one entry per task.
//!
//! The four task states are what makes the creation transaction observable as
//! atomic. `Creating` is opaque to every operation except a converging create,
//! `Live` is the only findable running task, `Retired` is the secret-free
//! terminal record, and `Gone` is the retirement fence that keeps a legal late
//! request from being mistaken for a brand new task.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use crate::{InstalledLease, MonotonicInstant, QueryContextDomains, TerminationLatch};
use novarocks_execution_contract::task_execution::creation::PreparedTaskFacts;
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::{
    CreateTaskReceipt, EstablishQueryContext, EstablishSemanticIdentity, OperationOutcome,
    QueryContextReceipt, QuiesceQueryContextReceipt,
};
#[cfg(test)]
use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, FinalTaskInfo, TaskStatus, TerminationDetail,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_types::identity::{StageId, TaskId};

use crate::ReleasedContextEvidence;
use crate::{RunnableTask, TaskDomains, TaskStatusOwner, TaskStatusSource};

/// The comparable, secret-free identity of one establish request.
///
/// The credential material is deliberately absent: it cannot be
/// fingerprinted, so an exact replay is recognised by comparing the live
/// material through [`ConfidentialContent::matches`] instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EstablishRecord {
    pub(super) operation: TaskOperationId,
    pub(super) admission_ticket_id: AdmissionTicketId,
    pub(super) semantic_identity: EstablishSemanticIdentity,
    pub(super) original_receipt: Option<QueryContextReceipt>,
}

impl EstablishRecord {
    pub(super) fn of(request: &EstablishQueryContext) -> Self {
        Self {
            operation: request.envelope().operation_id(),
            admission_ticket_id: request.admission_ticket_id(),
            semantic_identity: request.semantic_identity(),
            original_receipt: None,
        }
    }

    /// Whether two establishes are the same immutable request.
    ///
    /// Only the original operation and every immutable fact may replay.
    pub(super) fn same_request(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.admission_ticket_id == other.admission_ticket_id
            && self.semantic_identity == other.semantic_identity
    }
}

/// Why a creation transaction rolled back.
///
/// It is shared with every create that converged on the same creation, so
/// they all observe the same failure rather than each retrying separately.
#[derive(Clone, Debug)]
pub(super) struct CreationFailure {
    pub(super) result:
        novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>,
}
impl CreationFailure {
    pub(super) fn verdict(outcome: OperationOutcome, detail: impl AsRef<str>) -> Self {
        Self {
            result: novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome,
                detail: Some(novarocks_execution_contract::SafeDetail::truncating(
                    detail.as_ref(),
                )),
                acknowledgement: None,
            },
        }
    }
    pub(super) fn domain(
        outcome: novarocks_execution_contract::operation::DomainFailureOutcome,
        failure: novarocks_execution_contract::operation::OperationDomainFailure,
    ) -> Self {
        Self {
            result: novarocks_execution_contract::operation::OperationResult::DomainFailure {
                outcome,
                failure,
            },
        }
    }
    pub(super) fn receipt<T>(&self, operation: TaskOperationId) -> crate::OperationReceipt<T> {
        crate::OperationReceipt::from_result(
            operation,
            self.result.clone().map_ack(|never| match never {}),
        )
    }
}

/// The reservation one creation owner holds on a task identity.
///
/// It is keyed by the exact task identity alone and keeps no copy of what the
/// winning request carried. A create that converges on it is answered by the
/// round this cell belongs to, never by comparing its own body with the
/// winner's: the frontend freezes exactly one body per identity, so a request
/// naming an identity asks for the entity that identity already names.
pub(crate) struct CreationCell {
    preparation: Arc<crate::task_preparation::TaskPreparationScope>,
    progress: Mutex<CreationProgress>,
    accepted_status: Mutex<Option<Arc<TaskStatusOwner>>>,
}

struct CreationProgress {
    failure: Option<CreationFailure>,
    rollback_complete: bool,
}

/// Immutable metadata observes one exact accepted Task without retaining its
/// owner, reporter, runtime, input, or publication capability. A short loan
/// upgrades the owner only while an operation is actually using it.
#[derive(Clone)]
pub struct TaskPreparationView {
    owner: Weak<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
    identity: TaskIdentity,
    cadence: std::time::Duration,
}

impl TaskPreparationView {
    pub(crate) fn from_execution_port(
        owner: &Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>,
        cadence: std::time::Duration,
    ) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            identity: owner.identity(),
            cadence,
        }
    }
    // Existing direct-host fixtures keep their actual acceptance owner alive;
    // this view carries only a weak execution port and no role issuer.
    pub(crate) fn from_owner(owner: &Arc<TaskStatusOwner>, cadence: std::time::Duration) -> Self {
        let port: Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort> =
            owner.clone();
        Self::from_execution_port(&port, cadence)
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub fn borrow_control(&self) -> crate::PreparationControlLoan<'_> {
        crate::PreparationControlLoan::from_view(self)
    }

    pub(crate) fn control(
        &self,
    ) -> (
        Option<Arc<dyn novarocks_execution::runtime::task_control::TaskExecutionPort>>,
        std::time::Duration,
    ) {
        (self.owner.upgrade(), self.cadence)
    }
}

#[cfg(any(test, novarocks_dev_controls))]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TestPreparationIntent {
    Cancel(CancelReason),
    Abort(AbortCause),
}

impl CreationCell {
    pub(super) fn new(
        identity: TaskIdentity,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
    ) -> Self {
        Self::with_scope(Arc::new(
            crate::task_preparation::TaskPreparationScope::new(identity, health),
        ))
    }
    fn with_scope(preparation: Arc<crate::task_preparation::TaskPreparationScope>) -> Self {
        Self {
            preparation,
            progress: Mutex::new(CreationProgress {
                failure: None,
                rollback_complete: false,
            }),
            accepted_status: Mutex::new(None),
        }
    }

    pub(crate) fn preparation_scope(&self) -> &crate::task_preparation::TaskPreparationScope {
        &self.preparation
    }
    pub(crate) fn preparation_scope_arc(
        &self,
    ) -> Arc<crate::task_preparation::TaskPreparationScope> {
        Arc::clone(&self.preparation)
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub(crate) fn for_accepted_fixture(owner: &TaskStatusOwner) -> Self {
        Self::with_scope(owner.preparation_scope_arc())
    }

    pub(super) fn preparation_stop_view(
        &self,
        cadence: std::time::Duration,
    ) -> TaskPreparationView {
        let owner = self
            .accepted_status()
            .expect("preparation borrows an accepted Task");
        TaskPreparationView::from_owner(&owner, cadence)
    }

    pub(super) fn fail(&self, failure: CreationFailure) {
        self.progress.lock().expect("creation cell lock").failure = Some(failure);
    }

    /// Only the transaction that actually removed its installations supplies
    /// this fact. It is not a Task conclusion and does not prove job exit.
    pub(super) fn finish_rollback(&self, failure: CreationFailure) {
        let mut state = self.progress.lock().expect("creation cell lock");
        assert!(
            !state.rollback_complete,
            "one preparation rollback completion"
        );
        state.failure = Some(failure);
        state.rollback_complete = true;
    }

    pub(super) fn rollback_complete(&self) -> bool {
        self.progress
            .lock()
            .expect("creation cell lock")
            .rollback_complete
    }

    pub(super) fn failure(&self) -> Option<CreationFailure> {
        self.progress
            .lock()
            .expect("creation cell lock")
            .failure
            .clone()
    }

    pub(super) fn accept(&self, status: Arc<TaskStatusOwner>) {
        // Validate pure immutable provenance before taking the association lock.
        assert!(
            Arc::ptr_eq(&self.preparation, &status.preparation_scope_arc()),
            "Accepted borrows the exact original preparation scope"
        );
        let mut accepted = self.accepted_status.lock().expect("creation cell lock");
        if let Some(current) = accepted.as_ref() {
            assert!(
                Arc::ptr_eq(current, &status),
                "one creation has exactly one actual Task owner"
            );
        } else {
            *accepted = Some(status);
        }
    }

    pub(super) fn accepted_status(&self) -> Option<Arc<TaskStatusOwner>> {
        self.accepted_status
            .lock()
            .expect("creation cell lock")
            .clone()
    }
}

#[cfg(test)]
mod preparation_stop_view_tests {
    use super::*;

    fn fixture_identity() -> TaskIdentity {
        use novarocks_types::identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};
        let execution =
            QueryExecutionId::new(QueryId::new(310, 311), AttemptId::new(1).unwrap()).unwrap();
        TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn accepted_owner() -> Arc<TaskStatusOwner> {
        Arc::new(TaskStatusOwner::new(
            fixture_identity(),
            Arc::new(TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            std::time::Duration::ZERO,
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        ))
    }

    #[test]
    fn a_real_accepted_task_failure_interrupts_the_preparation_view() {
        use novarocks_execution_contract::TaskFailurePhase;
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::ZERO);
        let original = novarocks_execution::runtime::fragment::ExecutionFailure::from(
            novarocks_functions::KernelFailure::ResourceExhausted,
        );
        let control = owner.execution_control(TaskFailurePhase::Preparation);
        assert!(control.accepted().is_none());
        control.report_failure(original.clone());
        let receipt = control.accepted().unwrap();
        assert_eq!(receipt.neutral().generation().get(), 1);
        assert!(std::ptr::eq(
            receipt.originating_failure().unwrap().cause(),
            original.cause()
        ));
        let loan = view.borrow_control();
        for observed in [loan.checkpoint().unwrap_err(), loan.wait().unwrap_err()] {
            assert!(std::ptr::eq(observed.cause(), original.cause()));
            assert_eq!(
                novarocks_execution_contract::failure::FailureProjection::failure_fact(&observed),
                novarocks_execution_contract::failure::FailureProjection::failure_fact(&original)
            );
        }
    }

    #[test]
    fn an_accepted_task_cancel_interrupts_preparation_without_a_second_cell_decision() {
        use novarocks_execution_contract::{TaskConclusionPort, TaskConclusionProposal};
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::ZERO);
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        assert!(
            view.borrow_control().checkpoint().is_err(),
            "the Task owner, rather than a duplicate Cell flag, decides post-Accept stop"
        );
    }

    #[test]
    fn a_preparation_view_does_not_keep_an_exited_task_owner_alive_or_authorize_new_work() {
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        let weak = Arc::downgrade(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::ZERO);
        drop(owner);
        drop(cell);
        assert!(
            weak.upgrade().is_none(),
            "immutable metadata may retain a weak observer only"
        );
        assert!(
            view.borrow_control().checkpoint().is_err(),
            "an expired Task cannot authorize new preparation work"
        );
    }

    #[test]
    fn an_active_preparation_loan_pins_only_its_actual_work_and_reads_later_refinement() {
        use novarocks_execution_contract::{
            TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        let weak = Arc::downgrade(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::ZERO);
        let loan = view.borrow_control();
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let observed = loan.checkpoint().unwrap_err();
        assert_eq!(
            novarocks_execution_contract::failure::FailureProjection::failure_fact(&observed)
                .evidence(),
            novarocks_execution_contract::failure::FailureEvidence::StopObservation
        );
        let original: novarocks_execution::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        owner
            .execution_control(TaskFailurePhase::Preparation)
            .report_failure(original.clone());
        assert!(
            std::ptr::eq(loan.checkpoint().unwrap_err().cause(), original.cause()),
            "a cached control observation cannot hide legal originating refinement"
        );
        drop(owner);
        drop(cell);
        assert!(
            weak.upgrade().is_some(),
            "the real in-progress operation keeps its short loan"
        );
        drop(loan);
        assert!(
            weak.upgrade().is_none(),
            "immutable views retain no Task owner"
        );
        assert!(view.borrow_control().checkpoint().is_err());
    }

    #[test]
    fn borrowed_preparation_view_keeps_the_original_accepted_cancel() {
        use novarocks_execution_contract::{
            TaskConclusion, TaskConclusionPort, TaskConclusionProposal,
        };
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::from_millis(1));
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let first = view.borrow_control();
        let observed = first.checkpoint().unwrap_err();
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::LeaseExpired));
        drop(cell);
        assert!(owner.accepted().unwrap().originating_failure().is_none());
        assert_eq!(
            owner.accepted().unwrap().neutral().conclusion(),
            &TaskConclusion::Terminated(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        );
        assert_eq!(
            novarocks_execution_contract::failure::FailureProjection::failure_fact(&observed)
                .evidence(),
            novarocks_execution_contract::failure::FailureEvidence::StopObservation
        );
    }

    #[test]
    fn explicit_zero_cadence_preserves_immediate_poll_and_original_stop() {
        use novarocks_execution_contract::{TaskConclusionPort, TaskConclusionProposal};
        let owner = accepted_owner();
        let cell = CreationCell::for_accepted_fixture(&owner);
        cell.accept(owner.clone());
        let view = cell.preparation_stop_view(std::time::Duration::ZERO);
        assert!(view.borrow_control().wait().is_ok());
        assert!(view.borrow_control().checkpoint().is_ok());
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::LeaseExpired));
        let loan = view.borrow_control();
        let original = loan.checkpoint().unwrap_err();
        assert!(std::ptr::eq(
            loan.wait().unwrap_err().cause(),
            original.cause()
        ));
    }

    #[test]
    fn cached_domain_refusal_replays_the_exact_cause_without_task_acceptance() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_execution_contract::operation::{DomainFailureOutcome, OperationResult};
        let source: novarocks_execution::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let original = source.clone();
        let rejection =
            crate::HostRejection::from_failure(TaskFailureCategory::ResourceExhausted, source);
        let frozen = rejection.operation_failure();
        assert!(std::ptr::eq(
            rejection.into_failure().cause(),
            original.cause()
        ));
        let cell = CreationCell::new(
            fixture_identity(),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        );
        cell.fail(CreationFailure::domain(
            DomainFailureOutcome::ResourceExhausted,
            frozen.clone(),
        ));
        let failure = cell.failure().unwrap();
        let first: crate::OperationReceipt<()> = failure.receipt(TaskOperationId::new_v7());
        let replay: crate::OperationReceipt<()> =
            cell.failure().unwrap().receipt(TaskOperationId::new_v7());
        for receipt in [&first, &replay] {
            let OperationResult::DomainFailure { outcome, failure } = receipt.result() else {
                panic!("cached domain cause")
            };
            assert_eq!(*outcome, DomainFailureOutcome::ResourceExhausted);
            assert_eq!(failure, &frozen);
            assert_eq!(failure.fact(), original.failure_fact());
            assert!(receipt.acknowledgement().is_none());
        }
        assert!(cell.accepted_status().is_none());
    }

    #[test]
    fn preaccept_creation_failure_is_an_operation_receipt_without_task_authority() {
        let cell = CreationCell::new(
            fixture_identity(),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        );
        cell.fail(CreationFailure::verdict(
            OperationOutcome::InvalidStateOrRequest,
            "original preparation failure",
        ));
        assert!(cell.accepted_status().is_none());
        assert_eq!(
            cell.failure().unwrap().result.detail().unwrap().as_str(),
            "original preparation failure"
        );
    }
}

/// One installed, running task.
pub(super) struct LiveTask {
    pub(super) descriptor: Arc<TaskDescriptor>,
    /// What the creation winner's execution host proved about this task while
    /// it prepared it. The descriptor carries no plan, so these are the only
    /// plan facts this owner acts on, and nothing a later request carries can
    /// replace them.
    pub(super) prepared: PreparedTaskFacts,
    pub(super) receipt: CreateTaskReceipt,
    /// The immutable create verdict when this worker was submitted after its
    /// context had already closed. The worker remains owned until physical
    /// convergence, but neither the winner nor a replay may turn that losing
    /// create into an acknowledgement.
    pub(super) creation_failure: Option<CreationFailure>,
    pub(super) status: Arc<TaskStatusOwner>,
    pub(super) runnable: Arc<dyn RunnableTask>,
    pub(super) domains: TaskDomains,
    pub(super) receiver_installed: bool,
    pub(super) capability_installed: bool,
}

/// The retained terminal record of one task.
///
/// It is secret-free by construction: the descriptor, the prepared facts, the
/// split payloads, and every credential are dropped at retirement and only the
/// receipt or fixed creation failure, the immutable terminal status, the final
/// info, the result-owner bit, and the retirement instant survive. A create
/// replay is answered from the receipt or the failure alone, so nothing about
/// the body that created the task has to outlive the task.
pub(super) struct RetiredTask {
    pub(super) receipt: CreateTaskReceipt,
    pub(super) creation_failure: Option<CreationFailure>,
    pub(super) status: TaskStatus,
    pub(super) final_info: Option<FinalTaskInfo>,
    pub(super) result_owner: bool,
    pub(super) retired_at: MonotonicInstant,
    pub(super) bytes: usize,
}

pub(super) enum TaskEntry {
    /// A creation transaction owns this identity. Nothing but a converging
    /// create can see it, so a rollback leaves no half task behind.
    Creating(Arc<CreationCell>),
    Live(Box<LiveTask>),
    Retired(Box<RetiredTask>),
    /// The retained record was reclaimed. The fence stays behind so a legal
    /// late request is answered `Gone` rather than being mistaken for a task
    /// that never existed. It deliberately carries nothing else: its content
    /// is exactly the fact that this identity was used and is finished.
    Gone,
}

/// The part of a task identity not already fixed by its owning context.
///
/// Query execution and backend process are identical for every member of one
/// context, so retaining them 4096 times would add no fencing strength.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SpentTaskIdentity {
    stage: StageId,
    task: TaskId,
}

impl SpentTaskIdentity {
    const fn of(identity: TaskIdentity) -> Self {
        Self {
            stage: identity.stage_id(),
            task: identity.task_id(),
        }
    }
}

impl TaskEntry {
    pub(super) const fn is_terminal_record(&self) -> bool {
        matches!(self, Self::Retired(_) | Self::Gone)
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Retired(retired) => retired.bytes,
            _ => 0,
        }
    }
}

/// The estimated retained footprint of one terminal record.
///
/// It is an accounting estimate, not an encoded size: the point of the bound
/// is that retention cannot grow without limit, and a structural estimate
/// achieves that without encoding a record just to measure it.
pub(super) fn estimate_retained_bytes(
    receipt: &CreateTaskReceipt,
    status: &TaskStatus,
    final_info: Option<&FinalTaskInfo>,
) -> usize {
    use std::mem::size_of;

    let mut bytes = size_of::<CreateTaskReceipt>()
        + size_of::<TaskStatus>()
        + std::mem::size_of_val(receipt.domains());
    bytes += termination_bytes(status.termination());
    if let Some(info) = final_info {
        bytes += size_of::<FinalTaskInfo>();
        for statistics in info.operator_statistics() {
            bytes += size_of::<
                novarocks_execution_contract::task_execution::status::OperatorStatistics,
            >() + statistics.operator().as_str().len();
        }
        bytes += termination_bytes(info.final_status().termination());
    }
    bytes
}

fn termination_bytes(termination: Option<&TerminationDetail>) -> usize {
    match termination {
        Some(TerminationDetail::Failed(failure)) => failure.detail().as_str().len(),
        _ => 0,
    }
}

/// One query context on this backend.
pub(super) struct ContextEntry {
    pub(super) state: QueryContextState,
    pub(super) latch: TerminationLatch,
    pub(super) lease: Option<InstalledLease>,
    pub(super) establish: Option<EstablishRecord>,
    /// Shared facts become observable only when the context reaches `Active`.
    pub(super) facts_visible: bool,
    pub(super) domains: QueryContextDomains,
    pub(super) source: Arc<TaskStatusSource>,
    pub(super) tasks: BTreeMap<TaskIdentity, TaskEntry>,
    /// Result visibility belongs to the context, independent of task records
    /// and their retained tombstone horizon.
    pub(super) roots: BTreeMap<TaskIdentity, Arc<crate::root_result_channel::RootResultChannel>>,
    pub(super) quiesce: Option<QuiesceQueryContextReceipt>,
    /// Compact, context-lifetime anti-replay fence for every task identity
    /// that reached an installed worker. Detailed terminal records may be
    /// reclaimed independently; an identity in this set cannot be created a
    /// second time while the context remains addressable.
    spent_tasks: BTreeSet<SpentTaskIdentity>,
    pub(super) retired_at: Option<MonotonicInstant>,
    /// When this context entered `ABORTING`. It bounds how long an
    /// uncooperative task may delay cleanup.
    pub(super) terminating_since: Option<MonotonicInstant>,
    /// The observation revision the last retirement pass saw. A context whose
    /// revision has not moved cannot have a newly retirable task, which keeps
    /// one operation from costing a scan of every task on the backend.
    pub(super) last_retire_revision: Option<u64>,
    /// Set once the shared facts have been handed back to the host, so a
    /// rollback and a normal release cannot both release them.
    pub(super) facts_released: bool,
    /// What the host's tear-down sealed when it took the shared facts back.
    ///
    /// Retained on the entry rather than reported from the tear-down call
    /// itself because the two are not on the same stack: the host is released
    /// by the completion pass, and the release acknowledgement that reports
    /// this is encoded afterwards from the retired entry. It lives exactly as
    /// long as the retained context record.
    pub(super) released_evidence: ReleasedContextEvidence,
}

impl ContextEntry {
    pub(super) fn absent(source: Arc<TaskStatusSource>) -> Self {
        Self {
            state: QueryContextState::Absent,
            latch: TerminationLatch::open(),
            lease: None,
            establish: None,
            facts_visible: false,
            domains: QueryContextDomains::empty(),
            source,
            tasks: BTreeMap::new(),
            roots: BTreeMap::new(),
            quiesce: None,
            spent_tasks: BTreeSet::new(),
            retired_at: None,
            terminating_since: None,
            last_retire_revision: None,
            facts_released: false,
            released_evidence: ReleasedContextEvidence::none(),
        }
    }

    /// How many task identities this context has cumulatively occupied.
    ///
    /// A creation in progress counts before it becomes spent, and every
    /// installed worker remains counted after its detailed record is
    /// reclaimed. Two concurrent creates therefore cannot slip past the
    /// bound, and retirement cannot replenish the context's identity budget.
    pub(super) fn occupied_slots(&self) -> usize {
        self.spent_tasks.len()
            + self
                .tasks
                .iter()
                .filter(|(identity, entry)| {
                    !self.has_spent(**identity) && matches!(entry, TaskEntry::Creating(_))
                })
                .count()
    }

    pub(super) fn has_spent(&self, identity: TaskIdentity) -> bool {
        self.spent_tasks.contains(&SpentTaskIdentity::of(identity))
    }

    pub(super) fn mark_spent(&mut self, identity: TaskIdentity) {
        self.spent_tasks.insert(SpentTaskIdentity::of(identity));
    }

    pub(super) fn clear_spent(&mut self) {
        self.spent_tasks.clear();
    }

    pub(super) fn accepted_identities(
        &self,
        context: novarocks_execution_contract::QueryContextRef,
    ) -> Vec<TaskIdentity> {
        self.spent_tasks
            .iter()
            .map(|identity| {
                TaskIdentity::new(
                    context.query_execution_id(),
                    identity.stage,
                    identity.task,
                    context.backend_process_id(),
                )
            })
            .collect()
    }

    /// The abort cause a terminal receipt reports.
    ///
    /// A task failure that latched the context is reported as
    /// `PEER_TASK_FAILED`: the context ack carries the shared-resource cause
    /// only and is never a second authority over a task's own terminal.
    pub(super) fn termination_cause(&self) -> Option<AbortCause> {
        match self.latch.cause() {
            Some(TerminationDetail::Aborted(cause)) => Some(*cause),
            Some(TerminationDetail::Failed(_)) => Some(AbortCause::PeerTaskFailed),
            Some(TerminationDetail::Canceled(_)) => Some(AbortCause::QueryFailed),
            None => None,
        }
    }
}
