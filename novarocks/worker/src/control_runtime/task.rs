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

//! The host's actual elected Task owner. The Core alone decides creation and
//! lifecycle; this record holds its original reservations, published port and
//! one finite set of original fragment capabilities outside the actor.

mod domain_return;
mod failed_retirement;
pub(crate) use failed_retirement::{FailedTaskAction, FailedTaskNeutral, FailedTaskStep};

use super::{
    actors::{WorkerAction, WorkerActor},
    admitted_role::{adopt_reported_work, dispose_unused_completion, reserve_reported_role},
    causal::{
        CausalAdmissionRefusal, CausalError, RpcAuthorAdmission, RpcAuthorAdmissionError,
        RpcAuthorAdmissionRefusal, TaskCausalInbox,
    },
    causal_input::{CausalInputBinding, CausalInputCustody, CausalInputError, InputAuthor},
    completion_action::CompletionActionCustody,
    host_executor::{HostExecutorHandle, HostSubmitError},
    lifecycle_read::{TaskLifetime, TaskReadBinding, TaskReadError, TaskReadSnapshot},
    loading::{LoaderCustody, LoaderJob, LoadingSources, TaskCreatePayload},
    loading_events::{
        LoadingAuthorReturn, LoadingEventError, LoadingEventJob, LoadingEventOutcome,
        LoadingEvents, LoadingJobReturned,
    },
    loading_host_job::LoadingHostJob,
    open_summary::TaskOpenSummaryInbox,
    operator_statistics::{StatisticsBinding, StatisticsError, TaskStatisticsInbox},
    preparation::{
        PreparationAdoptionError, PreparationBinding, PreparationCustody, PreparationJob,
        PreparationOutcome, PreparationParent, PreparationStorageSplitError, QueuedPreparation,
        RunningPreparation,
    },
    preparation_refusal::{HostPreparationRefusal, RefusalPosition},
    task_host_call::{
        HOST_CALL_KINDS, TaskHostCall, TaskHostCallError, TaskHostCallJob, TaskHostCallKind,
        TaskHostCallOwner, TaskHostCallPayment,
    },
};
use super::{
    source_positions::{SOURCE_SLOT_STORAGE, SourceSlotAdmission},
    source_slot_issuer::{
        SourceSlotIssueError, SourceSlotIssuePartial, SourceSlotPayer, reserve_source_slots,
    },
};
use crate::control_runtime_policy::TaskLifecycleRole;
use crate::{
    settlement::RegistrationNotice,
    task_completion_roles::{AdmittedCompletionRole, TaskCompletionRoles},
    task_port::{TaskPendingNotice, TaskRuntimePort},
    task_proposal_ingress::{CutSourceAdmission, RequestMetadata, TaskProposalIngress},
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket},
    lease::{CapacityBundle, WorkLease},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, ReservationId},
    mailbox::InputClass,
    scope::WorkKind,
};
use novarocks_execution::runtime::{execution_health::ExecutionHealth, fragment::ExecutionFailure};
use novarocks_execution_contract::{AdmittedFragmentRoles, QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextEffect, creation::PayloadId};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct TaskHostBinding {
    pub context: QueryContextRef,
    /// One host-issued source identity shared by this Task's causal authors.
    /// It is neither a WorkId cast nor one independent source per role.
    pub task_source: ActorId,
    pub protocol_generation: Generation,
    pub max_cuts: usize,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
    /// Explicit finite scope/header-only component fixture; absent from production.
    #[cfg(test)]
    pub source_storage_model: bool,
}

#[derive(Debug)]
pub(crate) enum TaskHostError {
    SourceSlots(SourceSlotIssueError),
    SourceStorage(PreparationStorageSplitError),
    SourceInventoryPending,
    Disposal(super::task_disposal::TaskDisposalError),
    Loading(LoadingEventError),
    FailureSettlement(crate::settlement::SettlementDispatchError),
    FailureProjection(crate::task_port::ProjectionError),
    DynamicFilters(super::dynamic_filters::DynamicFilterError),
    HostCall(TaskHostCallError),
    OwnerFailed,
    OpenSummary(super::open_summary::OpenSummaryError),
    WrongEffect,
    WrongTaskOrPayload,
    InvalidGeometry,
    DuplicatePreparation,
    LoaderAlreadyIssued,
    InputNotReady,
    CompletionBeforeAccepted,
    CompletionAlreadyIssued,
    OpenSummaryAlreadyIssued,
    LifecycleAlreadyIssued(TaskLifecycleRole),
    Statistics(StatisticsError),
    Driver(DriverError),
    Runtime(RuntimeError),
    AdmissionBindInvariant(RuntimeError),
    Causal(CausalError),
    CompletionAction(CausalInputError),
    Preparation(PreparationAdoptionError),
}
impl From<CausalError> for TaskHostError {
    fn from(error: CausalError) -> Self {
        Self::Causal(error)
    }
}
impl From<RuntimeError> for TaskHostError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl std::fmt::Display for TaskHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loading(error) => {
                write!(f, "original loading failure takeover failed: {error:?}")
            }
            Self::FailureSettlement(error) => {
                write!(f, "original failed settlement dispatch failed: {error:?}")
            }
            Self::FailureProjection(error) => {
                write!(f, "original failed source detachment failed: {error:?}")
            }
            Self::DynamicFilters(error) => {
                write!(f, "original RF failure takeover failed: {error:?}")
            }
            Self::Disposal(error) => write!(f, "original Task disposal failed: {error:?}"),
            Self::SourceSlots(error) => write!(f, "source slot admission failed: {error:?}"),
            Self::SourceStorage(error) => {
                write!(f, "original source storage transfer failed: {error:?}")
            }
            Self::HostCall(error) => {
                write!(f, "original Task host-call admission failed: {error:?}")
            }
            Self::OpenSummary(error) => {
                write!(f, "original open summary retirement failed: {error:?}")
            }
            Self::OwnerFailed => f.write_str("the Task owner failed before new business capture"),
            Self::SourceInventoryPending => {
                f.write_str("an original source admission still requires unused retirement")
            }
            Self::WrongEffect => {
                f.write_str("Task host adoption requires its original LoadInput effect")
            }
            Self::WrongTaskOrPayload => {
                f.write_str("Task host effect differs from the elected Task or payload")
            }
            Self::InvalidGeometry => f.write_str("Task host storage geometry overflowed"),
            Self::DuplicatePreparation => f.write_str("Task already owns its original preparation"),
            Self::LoaderAlreadyIssued => f.write_str("Task already issued its original loader"),
            Self::InputNotReady => {
                f.write_str("Task preparation requires its actual measured loading input")
            }
            Self::CompletionBeforeAccepted => {
                f.write_str("fragment completion roles require actual Accepted custody")
            }
            Self::CompletionAlreadyIssued => {
                f.write_str("original fragment completion roles were already issued")
            }
            Self::OpenSummaryAlreadyIssued => {
                f.write_str("the original Task open-summary input was already issued")
            }
            Self::LifecycleAlreadyIssued(role) => {
                write!(
                    f,
                    "the Task's original {role:?} lifecycle input was already issued"
                )
            }
            Self::Statistics(error) => {
                write!(f, "original Task statistics handoff failed: {error:?}")
            }
            Self::Driver(error) => write!(f, "Task host responsibility transfer failed: {error}"),
            Self::Runtime(error) => write!(f, "Task host work admission failed: {error}"),
            Self::AdmissionBindInvariant(error) => {
                write!(
                    f,
                    "committed Task admission binding invariant failed: {error}"
                )
            }
            Self::Causal(error) => write!(f, "Task host causal admission failed: {error:?}"),
            Self::CompletionAction(error) => {
                write!(
                    f,
                    "Task host completion report observation failed: {error:?}"
                )
            }
            Self::Preparation(error) => write!(f, "Task host preparation transfer failed: {error}"),
        }
    }
}
impl std::error::Error for TaskHostError {}

/// A real elected Task whose source/port metadata could not finish bootstrap.
/// No loader, input or host callback was attached. The same admission remains
/// owned until its unused children really retire and its original holder exits.
#[must_use = "The failure owner must retain and dispose the original unpublished Task"]
pub(crate) struct UnpublishedTask {
    pub task: TaskIdentity,
    pub lifetime: WorkLease,
    pub active: CapacityBundle,
    pub source_partials: Vec<SourceSlotIssuePartial>,
    pub host_calls: Vec<TaskHostCall>,
    /// A split fee whose empty reported holder could not be created. It never
    /// acquired a callback or port and stays with this original unpublished Task.
    pub host_call_payments: Vec<CapacityBundle>,
    pub disposal: Option<super::task_disposal::TaskDisposalRole>,
    pub disposal_payments: Vec<CapacityBundle>,
}
impl UnpublishedTask {
    /// No host job, port, getter or observer was ever exposed. Retire only the
    /// same empty roles, then the never-bound pure fee, before the Task exits.
    pub(crate) fn retire_host_calls(
        &mut self,
        ledger: &novarocks_control_runtime::transaction::RuntimeLedger,
    ) -> Result<bool, TaskHostError> {
        let mut retired = true;
        if let Some(disposal) = &self.disposal {
            retired &= disposal
                .retire_unused(ledger)
                .map_err(TaskHostError::Disposal)?;
        }
        for call in &self.host_calls {
            retired &= call
                .retire_unused(ledger)
                .map_err(TaskHostError::HostCall)?;
        }
        if !retired {
            return Ok(false);
        }
        for fee in &mut self.host_call_payments {
            if !fee.is_released() {
                fee.release(&fee.resources()?)?;
            }
        }
        self.host_call_payments.clear();
        for fee in &mut self.disposal_payments {
            if !fee.is_released() {
                fee.release(&fee.resources()?)?;
            }
        }
        self.disposal_payments.clear();
        Ok(true)
    }
}
pub(crate) struct TaskAdoptionRefusal {
    pub error: TaskHostError,
    pub unpublished: Option<Box<UnpublishedTask>>,
}
impl From<TaskHostError> for TaskAdoptionRefusal {
    fn from(error: TaskHostError) -> Self {
        Self {
            error,
            unpublished: None,
        }
    }
}
impl std::fmt::Debug for TaskAdoptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskAdoptionRefusal")
            .field("error", &self.error)
            .field("retains_original_admission", &self.unpublished.is_some())
            .finish()
    }
}
impl std::fmt::Display for TaskAdoptionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

/// These are alternative actual ownership stages, not copied obligation rows.
pub(crate) enum TaskHostOwner {
    Ready(Box<TaskHostRecord>),
    Initializing(Box<TaskHostRecord>),
    Unpublished(Box<UnpublishedTask>),
}

/// Submission is physical queue ownership, never a loader or Task exit fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoadingSubmission {
    NoPendingJob,
    Submitted,
    Pending(HostSubmitError),
}

/// This is the original resource inventory, not a copied obligation ledger.
/// Taking it moves custody to a host teardown job; it proves no physical exit.
pub(crate) struct TaskHostResources {
    pub lifetime: TaskLifetime,
    pub active: CapacityBundle,
    pub port: Arc<TaskRuntimePort>,
    pub host_calls: [TaskHostCall; HOST_CALL_KINDS.len()],
    pub disposal: super::task_disposal::TaskDisposalRole,
    pub loader: Option<LoaderCustody>,
    pub loading_events: Option<LoadingEvents>,
    pub loading_return: Option<OwnedReturnReader<LoadingJobReturned>>,
    /// The same already-admitted job stays here if the physical queue refuses
    /// it. There is one position per elected Task, not another scheduling queue.
    pub pending_loading: Option<LoadingHostJob>,
    pub preparation: Option<PreparationCustody>,
    pub preparation_remainder: Option<CapacityBundle>,
    pub accepted_source_storage: Option<CapacityBundle>,
    pub pending_preparation_adoption: Option<TaskPreparationJob>,
    pub source_partials: Vec<SourceSlotIssuePartial>,
    pub completion: Option<Arc<TaskCompletionRoles>>,
    pub completion_action: Option<CompletionActionCustody>,
    pub lifecycle_inputs: BTreeMap<TaskLifecycleRole, CausalInputCustody>,
    pub statistics: Option<TaskStatisticsInbox>,
    pub dynamic_filters: Option<super::dynamic_filters::TaskDynamicFilterInbox>,
    pub open_summary: Option<TaskOpenSummaryInbox>,
    pub causal: TaskCausalInbox,
    pub production: Option<Arc<ProductionPreparation>>,
}
impl TaskHostResources {
    fn resources_dynamic_filter_pending(&self) -> bool {
        self.dynamic_filters
            .as_ref()
            .is_some_and(|source| !source.is_retired())
    }
    /// The custodian has disposed actual payloads and retired each independent
    /// report/input position. Transfer read authority only from that inventory;
    /// a Task terminal or a stopped execution borrow gate is insufficient.
    pub(crate) fn seal_read_observation(&self) -> Result<Arc<TaskReadSnapshot>, TaskReadError> {
        use novarocks_control_runtime::exit_role::ExitRolePhase;
        if self
            .production
            .as_ref()
            .is_some_and(|owner| !owner.is_retired())
            || self.host_calls.iter().any(|call| !call.is_retired())
            || self
                .source_partials
                .iter()
                .any(|partial| !partial.is_empty())
            || self.pending_preparation_adoption.is_some()
            || !self.causal.is_empty()
            || self
                .statistics
                .as_ref()
                .is_some_and(|statistics| !statistics.is_retired())
            || self.resources_dynamic_filter_pending()
            || self.pending_loading.is_some()
            || self
                .open_summary
                .as_ref()
                .is_some_and(|open| !open.is_closed())
            || self
                .loader
                .as_ref()
                .is_some_and(|loader| !loader.is_retired())
            || self
                .loading_events
                .as_ref()
                .is_some_and(|events| !events.is_retired())
            || self.loading_return.as_ref().is_some_and(|reader| {
                !matches!(reader.state(), Ok(
                    novarocks_control_runtime_core::completion::ReturnState::Taken
                    | novarocks_control_runtime_core::completion::ReturnState::RevokedUnused
                ))
            })
            || (self.preparation.is_some() && self.preparation_remainder.is_none())
            || self
                .completion
                .as_ref()
                .is_some_and(|roles| !roles.is_retired())
            || self
                .completion_action
                .as_ref()
                .is_some_and(|role| role.phase() != ExitRolePhase::Retired)
            || self
                .lifecycle_inputs
                .values()
                .any(|input| input.phase() != ExitRolePhase::Retired)
        {
            return Err(TaskReadError::HostInventoryStillLive);
        }
        self.lifetime.seal_after_exit()
    }
}

/// The original request is a real preparation capture, not retained Task
/// metadata. Its last reference exits inside the same supervised host call
/// as the input, before P/bytes can be returned.
#[must_use = "The original preparation request and input require actual exit"]
pub(crate) struct TaskPreparationJob {
    job: Option<PreparationJob>,
    request: Option<Arc<novarocks_execution_contract::CreateTask>>,
    host: Option<HostPreparationBinding>,
    inbound: Option<crate::TaskInboundPermit>,
    inbound_bound: bool,
}
struct HostPreparationBinding {
    port: Arc<TaskRuntimePort>,
    children: Arc<crate::task_preparation::TaskPreparationScope>,
    cadence: std::time::Duration,
    roles: crate::TaskPreparationRoles,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
}
pub(crate) enum HostInstallationOutcome {
    Installed(crate::PreparedTaskInstallation),
    Refused(HostPreparationRefusal),
    WorkRefused(RuntimeError),
}
/// Dropped inside PreparationJob's original supervised body even when its
/// stop/start gate rejects invocation of the unused callback.
struct FailedPreparationCaptures {
    binding: Option<HostPreparationBinding>,
    inbound: Option<crate::TaskInboundPermit>,
}
impl Drop for FailedPreparationCaptures {
    fn drop(&mut self) {
        if let Some(inbound) = self.inbound.take() {
            if let Err((error, original)) = inbound.release() {
                std::mem::forget(original);
                panic!("Original unexposed inbound payment release failed: {error:?}");
            }
        }
        drop(self.binding.take());
    }
}
struct PreparationPendingWake(Arc<dyn TaskPendingNotice>);
impl std::task::Wake for PreparationPendingWake {
    fn wake(self: Arc<Self>) {
        self.0.pending();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.pending();
    }
}
impl TaskPreparationJob {
    /// The real Native preparation entry consumes the same queued input and
    /// original role set. It cannot construct a Task authority or reissue a
    /// finisher from a cloned control handle.
    pub(crate) fn install_receiver(
        mut self,
        host: &dyn crate::TaskExecutionHost,
    ) -> PreparationOutcome<HostInstallationOutcome> {
        let binding = self
            .host
            .take()
            .expect("actual host preparation was bound before enqueue");
        let children = binding.children.clone();
        let outcome = self.run(move |request, input, parent| {
            // A host error may itself retain the input it consumed. Admit its
            // original capture child before the foreign call, so moving that
            // error out of this body cannot make P/bytes look released.
            let refusal = match RefusalPosition::reserve(&parent) {
                Ok(position) => position,
                Err(error) => {
                    drop(input);
                    return HostInstallationOutcome::WorkRefused(error);
                }
            };
            let port = binding.port.execution_port();
            let preparation = crate::TaskPreparationContext::from_admitted(
                port,
                &binding.children,
                binding.cadence,
                binding.roles,
                std::task::Waker::from(Arc::new(PreparationPendingWake(binding.pending.clone()))),
            )
            .expect("original admitted role and child identities match the Task");
            let installed = host
                .install_receiver(request.descriptor(), input, preparation)
                .and_then(|installation| {
                    installation.validate_task(request.descriptor())?;
                    Ok(installation)
                });
            match installed {
                Ok(installation) => HostInstallationOutcome::Installed(installation),
                Err(rejection) => HostInstallationOutcome::Refused(refusal.retain(
                    rejection,
                    request.identity(),
                    binding.pending,
                    binding.health,
                )),
            }
        });
        // This is only the actual preparation call/capture boundary. Its
        // registered provider child still owes separate rollback/adoption.
        children.body_exited();
        outcome
    }
    /// The actual failed owner took this still-unexposed job from its own
    /// adoption slot. Dispose all original captures inside the same prepaid
    /// preparation body, before that custodian observes capture exit.
    fn dispose_failed_on_host(mut self) {
        let binding = self.host.take();
        let children = binding.as_ref().map(|binding| binding.children.clone());
        let captures = FailedPreparationCaptures {
            binding,
            inbound: self.inbound.take(),
        };
        let _outcome = self.run(move |_request, input, _parent| {
            drop(input);
            drop(captures);
        });
        if let Some(children) = children {
            children.body_exited();
        }
    }
    pub(crate) fn run<T>(
        mut self,
        host: impl FnOnce(
            &novarocks_execution_contract::CreateTask,
            novarocks_execution_contract::TaskCreationInput,
            PreparationParent,
        ) -> T,
    ) -> PreparationOutcome<T> {
        let request = self.request.take().expect("one original Create carrier");
        self.job
            .take()
            .expect("one original preparation body")
            .run(move |input, parent| host(&request, input, parent))
    }
}

impl QueuedPreparation for TaskPreparationJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        self.job
            .as_ref()
            .expect("original preparation queue custody")
            .key()
    }
}
impl RunningPreparation<TaskPreparationJob> {
    pub(crate) fn install_receiver(
        self,
        host: &dyn crate::TaskExecutionHost,
    ) -> PreparationOutcome<HostInstallationOutcome> {
        self.run_body(|job| job.install_receiver(host))
    }
    pub(crate) fn run<T>(
        self,
        host: impl FnOnce(
            &novarocks_execution_contract::CreateTask,
            novarocks_execution_contract::TaskCreationInput,
            PreparationParent,
        ) -> T,
    ) -> PreparationOutcome<T> {
        self.run_body(|job| job.run(host))
    }
}

enum LoadingReturnMode {
    Host,
    #[cfg(test)]
    Component,
}
type LoadingAdmission = (
    LoadingEventJob,
    Option<OwnedReturnSender<LoadingJobReturned>>,
);
pub(crate) struct TaskHostRecord {
    task: TaskIdentity,
    actor: ActorId,
    task_source: ActorId,
    context: QueryContextRef,
    payload: PayloadId,
    scope: ScopeHandle,
    // A cleanup producer may use a later failed scope epoch. Its consumer is
    // still the original Core, whose exact leg the supervisor abandons.
    core_generation: Generation,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    resources: TaskHostResources,
    // One original fragment factory, including a failed partial admission.
    // Replaying a request or cloning the port never reissues these positions.
    completion_issued: bool,
    completion_action_issued: bool,
    settlement_notice_issued: bool,
    loader_issued: bool,
    request_admission_open: bool,
    #[cfg(test)]
    source_storage_model: bool,
    lifecycle_issued: [bool; TaskLifecycleRole::COUNT],
    normal_source_detached: [bool; 2],
    normal_lifetime_ended: bool,
    completion_core_observed: bool,
    production_settlement_cut: Option<(
        novarocks_worker_protocol::proposal_summary::CutId,
        novarocks_control_runtime_core::identity::WorkId,
    )>,
    failed_retirement: failed_retirement::FailedRetirementCursor,
    host_cut_cursor: Option<novarocks_worker_protocol::proposal_summary::CutId>,
    host_retired_pairs: BTreeMap<
        novarocks_worker_protocol::proposal_summary::CutId,
        crate::task_proposal_ingress::RetiredCut,
    >,
}
/// A borrowed proof from the original failed Task inventory. Its producer
/// facts are monotonic and its business issuers are already fenced by the
/// actual failed scope. It grants neither admission nor a replacement role.
pub(super) struct TaskExecutionBorrowersExited<'a> {
    owner: &'a TaskHostRecord,
}
impl TaskExecutionBorrowersExited<'_> {
    pub(super) fn matches(&self, task: TaskIdentity, scope: &ScopeHandle) -> bool {
        self.owner.task == task && self.owner.scope.same_scope(scope)
    }
}

impl TaskHostRecord {
    pub(super) fn source_address(&self) -> ActorId {
        self.task_source
    }
    /// Electing the Task and paying its active/loading storage happened in
    /// the Core step. This takes the same guard once; it reserves no new quota.
    pub(crate) fn adopt_loading(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        binding: TaskHostBinding,
        context_lifetime: &WorkLease,
    ) -> Result<Self, TaskAdoptionRefusal> {
        let (task, payload, reservation) = match &ticket.effect {
            WorkerAction::Context(ContextEffect::LoadInput {
                task,
                payload,
                reservation,
            }) => (*task, *payload, *reservation),
            _ => return Err(TaskHostError::WrongEffect.into()),
        };
        task.verify_query_context(binding.context)
            .map_err(|_| TaskHostError::WrongTaskOrPayload)?;
        binding
            .max_cuts
            .checked_add(2)
            .and_then(|segments| segments.checked_mul(2))
            .ok_or(TaskHostError::InvalidGeometry)?;
        if !runtime
            .owns_scope(ticket.actor, context_lifetime.scope())
            .map_err(TaskHostError::Driver)?
        {
            return Err(TaskHostError::WrongTaskOrPayload.into());
        }
        let guard = runtime
            .take_step_reservation(ticket, reservation)
            .map_err(TaskHostError::Driver)?;
        let scope = guard.scope().clone();
        // Admission already committed in the Core step. Bind performs no new
        // capacity/gate decision; a structural refusal is a shared runtime
        // invariant failure, never an ordinary Task loading refusal.
        let (lifetime, mut active) = guard
            .bind(WorkKind::Business)
            .map_err(TaskHostError::AdmissionBindInvariant)?;
        // These are the elected Task's four already paid base positions.
        // Their parent is the original Task lifetime, not a short preparation
        // or settlement role whose join could depend on the retained source.
        let parent = active.id();
        #[cfg(test)]
        let payer = if binding.source_storage_model {
            SourceSlotPayer::ProtocolModel
        } else {
            SourceSlotPayer::Retained(&mut active)
        };
        #[cfg(not(test))]
        let payer = SourceSlotPayer::Retained(&mut active);
        let sources = match reserve_source_slots::<4>(&scope, parent, ticket.generation, payer) {
            Ok(sources) => sources,
            Err(refusal) => {
                return Err(TaskAdoptionRefusal {
                    error: TaskHostError::SourceSlots(refusal.error),
                    unpublished: Some(Box::new(UnpublishedTask {
                        task,
                        lifetime,
                        active,
                        source_partials: vec![refusal.partial],
                        host_calls: Vec::new(),
                        host_call_payments: Vec::new(),
                        disposal: None,
                        disposal_payments: Vec::new(),
                    })),
                });
            }
        };
        // Source4 has its own complete original holders. Bootstrap the two
        // host-only roles before constructing or exposing any Task port/view.
        let unpublished = |error,
                           lifetime,
                           active,
                           sources: [SourceSlotAdmission; 4],
                           host_calls,
                           host_call_payments| {
            TaskAdoptionRefusal {
                error,
                unpublished: Some(Box::new(UnpublishedTask {
                    task,
                    lifetime,
                    active,
                    source_partials: vec![SourceSlotIssuePartial::from_admissions(&scope, sources)],
                    host_calls,
                    host_call_payments,
                    disposal: None,
                    disposal_payments: Vec::new(),
                })),
            }
        };
        let mut host_calls = Vec::with_capacity(HOST_CALL_KINDS.len());
        for kind in HOST_CALL_KINDS {
            let payment = match TaskHostCallPayment::split_from(
                &mut active,
                #[cfg(test)]
                binding.source_storage_model,
            ) {
                Ok(payment) => payment,
                Err(error) => {
                    return Err(unpublished(
                        TaskHostError::HostCall(error.into()),
                        lifetime,
                        active,
                        sources,
                        host_calls,
                        Vec::new(),
                    ));
                }
            };
            let role = match reserve_reported_role(&scope, parent, WorkKind::Cleanup, None) {
                Ok(role) => role,
                Err(error) => {
                    let fees = match payment {
                        TaskHostCallPayment::Paid(fee) => vec![fee],
                        #[cfg(test)]
                        TaskHostCallPayment::ProtocolModel => Vec::new(),
                    };
                    return Err(unpublished(
                        TaskHostError::HostCall(error.into()),
                        lifetime,
                        active,
                        sources,
                        host_calls,
                        fees,
                    ));
                }
            };
            let owner = TaskHostCallOwner {
                kind,
                runtime: runtime.clone(),
                actor: ticket.actor,
                task,
                pending: binding.pending.clone(),
                health: binding.health.clone(),
            };
            let original = adopt_reported_work(role.work, role.capacity, role.report);
            let call = match TaskHostCall::from_unbound_admitted(owner, original, payment) {
                Ok(call) => call,
                Err(refusal) => {
                    // These freshly reserved, unexposed originals have exact
                    // origin/fee geometry and a single unclaimed return. A
                    // violated binding is a shared invariant, not quota refusal.
                    std::mem::forget((refusal, lifetime, active, sources, host_calls));
                    std::process::abort();
                }
            };
            host_calls.push(call);
        }
        let disposal_payment = match super::task_disposal::TaskDisposalPayment::split_from(
            &mut active,
            #[cfg(test)]
            binding.source_storage_model,
        ) {
            Ok(payment) => payment,
            Err(error) => {
                return Err(unpublished(
                    TaskHostError::Runtime(error),
                    lifetime,
                    active,
                    sources,
                    host_calls,
                    Vec::new(),
                ));
            }
        };
        let disposal_reported = match super::task_disposal::admit_disposal_work(context_lifetime) {
            Ok(role) => role,
            Err(error) => {
                let mut refusal = unpublished(
                    TaskHostError::Runtime(error),
                    lifetime,
                    active,
                    sources,
                    host_calls,
                    Vec::new(),
                );
                match disposal_payment {
                    super::task_disposal::TaskDisposalPayment::Paid(fee) => refusal
                        .unpublished
                        .as_mut()
                        .expect("original unpublished Task")
                        .disposal_payments
                        .push(fee),
                    #[cfg(test)]
                    super::task_disposal::TaskDisposalPayment::ProtocolModel => {}
                }
                return Err(refusal);
            }
        };
        let disposal = match super::task_disposal::TaskDisposalRole::from_admitted(
            super::task_disposal::TaskDisposalBinding {
                runtime: runtime.clone(),
                actor: ticket.actor,
                task,
                pending: binding.pending.clone(),
                health: binding.health.clone(),
            },
            disposal_reported,
            disposal_payment,
        ) {
            Ok(role) => role,
            Err(originals) => {
                std::mem::forget((originals, lifetime, active, sources, host_calls));
                std::process::abort();
            }
        };
        let ingress = match TaskProposalIngress::from_admitted(
            task,
            binding.max_cuts,
            sources,
            binding.pending.clone(),
            binding.health.clone(),
        ) {
            Ok(ingress) => Arc::new(ingress),
            Err((_error, sources)) => {
                let mut refusal = unpublished(
                    TaskHostError::InvalidGeometry,
                    lifetime,
                    active,
                    sources,
                    host_calls,
                    Vec::new(),
                );
                refusal
                    .unpublished
                    .as_mut()
                    .expect("original unpublished Task")
                    .disposal = Some(disposal);
                return Err(refusal);
            }
        };
        let port = Arc::new(TaskRuntimePort::from_admitted(
            task,
            &scope,
            ingress.clone(),
            binding.pending.clone(),
            binding.health.clone(),
        ));
        for call in &host_calls {
            if let Err(error) = call.attach_port(&port) {
                std::mem::forget((error, port, lifetime, active, host_calls));
                std::process::abort();
            }
        }
        let host_calls = match host_calls.try_into() {
            Ok(calls) => calls,
            Err(originals) => {
                std::mem::forget((originals, port, lifetime, active));
                std::process::abort();
            }
        };
        let lifetime = TaskLifetime::from_admitted(lifetime, &port);
        Ok(Self {
            task,
            actor: ticket.actor,
            task_source: binding.task_source,
            context: binding.context,
            payload,
            scope,
            core_generation: ticket.generation,
            pending: binding.pending,
            health: binding.health.clone(),
            resources: TaskHostResources {
                lifetime,
                active,
                port,
                host_calls,
                disposal,
                loader: None,
                loading_events: None,
                loading_return: None,
                pending_loading: None,
                preparation: None,
                preparation_remainder: None,
                accepted_source_storage: None,
                pending_preparation_adoption: None,
                source_partials: Vec::new(),
                completion: None,
                completion_action: None,
                lifecycle_inputs: BTreeMap::new(),
                statistics: None,
                dynamic_filters: None,
                open_summary: None,
                production: None,
                causal: TaskCausalInbox::new(
                    runtime.clone(),
                    ticket.actor,
                    binding.task_source,
                    task,
                    binding.protocol_generation,
                    ingress,
                    binding.health,
                ),
            },
            completion_issued: false,
            completion_action_issued: false,
            settlement_notice_issued: false,
            loader_issued: false,
            request_admission_open: true,
            #[cfg(test)]
            source_storage_model: binding.source_storage_model,
            lifecycle_issued: [false; TaskLifecycleRole::COUNT],
            normal_source_detached: [false; 2],
            normal_lifetime_ended: false,
            completion_core_observed: false,
            production_settlement_cut: None,
            failed_retirement: failed_retirement::FailedRetirementCursor::default(),
            host_cut_cursor: None,
            host_retired_pairs: BTreeMap::new(),
        })
    }
    /// Address the original preadmitted role. Cloning its observer handle
    /// grants no replacement Work, report, fee or execution permission.
    pub(crate) fn host_call(&self, kind: TaskHostCallKind) -> &TaskHostCall {
        self.resources
            .host_calls
            .iter()
            .find(|call| call.kind() == kind)
            .expect("the elected Task retains each fixed original host call")
    }
    /// Read only original execution-call holders. No Core/report/Source/R
    /// consumer, delivery, retained data alias or this closing call is part of
    /// the predicate. Readiness alone gives no execution permission.
    pub(super) fn execution_borrowers_exited(&self) -> Result<bool, TaskHostCallError> {
        if self.resources.pending_preparation_adoption.is_some()
            || self
                .resources
                .preparation
                .as_ref()
                .is_some_and(|owner| !owner.host_borrowers_exited())
            || self
                .resources
                .completion
                .as_ref()
                .is_some_and(|roles| !roles.execution_calls_exited())
            || !self.host_call(TaskHostCallKind::Stop).actual_call_exited()
        {
            return Ok(false);
        }
        if let Some(action) = &self.resources.completion_action
            && !action.actual_exit_ready()?
        {
            return Ok(false);
        }
        Ok(true)
    }
    /// Failure publication order and actual producer exit are distinct gates.
    /// Native preparation records Some before exposing its unique job and
    /// never clears that original custody. Here a missing producer is therefore
    /// meaningful only after the same failed scope has closed fresh issuers:
    /// old EnqueuePreparation guards fail their generation check, while any
    /// previously handed-off job is covered by its existing preparation row.
    /// Loading data and RPC deliveries cannot create a Native execution trait
    /// projection and are not execution borrowers.
    pub(crate) fn take_failed_retire_borrowing_job(
        &self,
        drained: &super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<Option<TaskHostCallJob>, TaskHostCallError> {
        drained.validate(self.resources.causal.runtime(), self.actor)?;
        if !self.execution_borrowers_exited()? {
            return Ok(None);
        }
        let exited = TaskExecutionBorrowersExited { owner: self };
        self.host_call(TaskHostCallKind::RetireBorrowing)
            .take_retire_failure_job(drained, &exited)
            .map(Some)
    }
    pub(crate) fn authorize_failed_disposal(
        &self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<super::task_disposal::TaskDisposalAuthorization, TaskHostError> {
        stage
            .validate(self.resources.causal.runtime(), self.actor)
            .map_err(TaskHostError::Driver)?;
        self.resources
            .seal_read_observation()
            .map_err(|_| TaskHostError::SourceInventoryPending)?;
        self.resources
            .disposal
            .authorize_failure(stage)
            .map_err(TaskHostError::Disposal)
    }
    pub(crate) fn authorize_normal_disposal(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<super::task_disposal::TaskDisposalAuthorization, TaskHostError> {
        self.resources
            .disposal
            .authorize_normal(ticket)
            .map_err(TaskHostError::Disposal)
    }
    pub(crate) fn task_id(&self) -> TaskIdentity {
        self.task
    }
    /// The exact admitted incarnation, including rejected creations that
    /// never produced a normal RetireTask effect.
    pub(crate) fn original_payload_id(&self) -> PayloadId {
        self.payload
    }
    pub(crate) fn request_admission_open(&self) -> bool {
        self.request_admission_open
    }
    /// Fence only new RPC authors. Already admitted cuts and original holder
    /// exits continue independently; this supplies no cancellation or exit fact.
    pub(crate) fn close_request_admission(&mut self) {
        self.request_admission_open = false;
    }
    /// Bind one original Context R through this exact Task's inbox and pending
    /// notice. Callers provide the paid originals, never replacement callbacks.
    pub(crate) fn bind_request_author(
        &mut self,
        admitted: AdmittedCompletionRole,
        request: RequestMetadata,
        class: InputClass,
        consumers: u64,
        admission: CutSourceAdmission,
    ) -> Result<RpcAuthorAdmission, RpcAuthorAdmissionRefusal> {
        if !self.request_admission_open {
            return Err(RpcAuthorAdmissionRefusal::BeforeCut {
                error: RpcAuthorAdmissionError::Causal(CausalError::RequestAdmissionClosed),
                admitted,
                admission,
            });
        }
        self.resources.causal.admit_rpc_author(
            admitted,
            request,
            class,
            consumers,
            self.pending.clone(),
            admission,
        )
    }
    pub(crate) fn port(&self) -> Arc<TaskRuntimePort> {
        self.resources.port.clone()
    }
    /// Attach the Task's fixed loading-paid producer position before exposing
    /// the original port or loader job. Notifications never reissue this role.
    pub(crate) fn admit_open_summary(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
    ) -> Result<(), TaskHostError> {
        if self.resources.open_summary.is_some() {
            return Err(TaskHostError::OpenSummaryAlreadyIssued);
        }
        let permit = runtime
            .reserve_input(
                self.actor,
                self.task_source,
                novarocks_control_runtime_core::mailbox::InputClass::Lifecycle,
            )
            .map_err(TaskHostError::Driver)?;
        let role = match permit.into_role() {
            Ok(role) => role,
            Err((error, permit)) => {
                // No payload or capture was attached. Revoke the exact unused
                // position, including when scope failure won this race.
                permit.revoke_unused().map_err(TaskHostError::Driver)?;
                return Err(TaskHostError::Driver(error));
            }
        };
        let open = TaskOpenSummaryInbox::from_admitted(
            runtime.clone(),
            self.actor,
            self.task,
            self.resources.lifetime.id(),
            self.resources.port.ingress().clone(),
            role,
        )
        .unwrap_or_else(|_| panic!("the original freshly converted Task input role is idle"));
        self.resources.open_summary = Some(open);
        Ok(())
    }
    pub(crate) fn open_summary_mut(&mut self) -> Option<&mut TaskOpenSummaryInbox> {
        self.resources.open_summary.as_mut()
    }
    pub(crate) fn observation(&self) -> Arc<TaskReadBinding> {
        self.resources.lifetime.observation()
    }
    pub(crate) fn admit_settlement_notice(
        &mut self,
        consumers: u64,
    ) -> Result<
        (
            novarocks_worker_protocol::proposal_summary::CutId,
            Box<dyn RegistrationNotice>,
        ),
        TaskHostError,
    > {
        if self.resources.preparation.is_none() {
            return Err(TaskHostError::CompletionBeforeAccepted);
        }
        if self.settlement_notice_issued {
            return Err(TaskHostError::CompletionAlreadyIssued);
        }
        let sources = self.reserve_source_slots::<2>(false)?;
        let admitted = self
            .resources
            .causal
            .admit_settlement(
                consumers,
                self.pending.clone(),
                CutSourceAdmission::Fresh(sources),
            )
            .map_err(|refusal| self.retain_source_refusal(refusal))?;
        self.settlement_notice_issued = true;
        Ok(admitted)
    }
    fn reserve_source_slots<const N: usize>(
        &mut self,
        loading: bool,
    ) -> Result<[SourceSlotAdmission; N], TaskHostError> {
        if self
            .resources
            .source_partials
            .iter()
            .any(|partial| !partial.is_empty())
        {
            return Err(TaskHostError::SourceInventoryPending);
        }
        let parent = self.resources.active.id();
        #[cfg(test)]
        let payer = if self.source_storage_model {
            SourceSlotPayer::ProtocolModel
        } else if loading {
            SourceSlotPayer::Retained(&mut self.resources.active)
        } else {
            SourceSlotPayer::Pool(&mut self.resources.accepted_source_storage)
        };
        #[cfg(not(test))]
        let payer = if loading {
            SourceSlotPayer::Retained(&mut self.resources.active)
        } else {
            SourceSlotPayer::Pool(&mut self.resources.accepted_source_storage)
        };
        match reserve_source_slots::<N>(&self.scope, parent, self.core_generation, payer) {
            Ok(slots) => Ok(slots),
            Err(refusal) => {
                self.resources.source_partials.push(refusal.partial);
                Err(TaskHostError::SourceSlots(refusal.error))
            }
        }
    }
    fn retain_source_refusal(&mut self, refusal: CausalAdmissionRefusal) -> TaskHostError {
        let CutSourceAdmission::Fresh(slots) = refusal.admission else {
            unreachable!("this fixed-role issuer supplies original fresh source slots");
        };
        self.resources
            .source_partials
            .push(SourceSlotIssuePartial::from_admissions(&self.scope, slots));
        TaskHostError::Causal(refusal.error)
    }
    pub(crate) fn causal(&mut self) -> &mut TaskCausalInbox {
        &mut self.resources.causal
    }
    /// Component fixtures can test the loader itself without attaching the
    /// production causal wrapper. Host composition uses begin_causal_loader.
    #[cfg(test)]
    pub(crate) fn begin_loader(
        &mut self,
        payload: TaskCreatePayload,
    ) -> Result<LoaderJob, (TaskHostError, TaskCreatePayload)> {
        let (loader, input) = match self.reserve_loader_roles(&payload) {
            Ok(roles) => roles,
            Err(error) => return Err((error, payload)),
        };
        let (custody, job) = LoaderCustody::from_admitted(
            payload,
            loader,
            input,
            self.pending.clone(),
            self.health.clone(),
        );
        self.resources.loader = Some(custody);
        Ok(job)
    }
    fn reserve_loader_roles(
        &mut self,
        payload: &TaskCreatePayload,
    ) -> Result<(AdmittedCompletionRole, AdmittedCompletionRole), TaskHostError> {
        if payload.id != self.payload
            || payload.request.identity() != self.task
            || payload.request.context() != self.context
        {
            return Err(TaskHostError::WrongTaskOrPayload);
        }
        if self.loader_issued {
            return Err(TaskHostError::LoaderAlreadyIssued);
        }
        self.loader_issued = true;
        let loader = match reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            WorkKind::Business,
            Some(self.core_generation),
        ) {
            Ok(loader) => loader,
            Err(error) => return Err(TaskHostError::Runtime(error)),
        };
        // The input holder exists before the loader captures its graph. It is
        // a cleanup child, so a delayed destructor also keeps loader whole-exit
        // false; normal Accepted handoff positively exits this unused holder.
        let input =
            match reserve_reported_role(&self.scope, loader.capacity.id(), WorkKind::Cleanup, None)
            {
                Ok(input) => input,
                Err(error) => {
                    dispose_unused_completion(loader);
                    return Err(TaskHostError::Runtime(error));
                }
            };
        Ok((loader, input))
    }
    /// Issue both already-paid loading authors before the first getter can
    /// execute. The same input and job transfer only after every original
    /// position has attached; partial refusal retains its unused cut inventory
    /// in this Task and returns the exact unborrowed payload to its custodian.
    #[cfg(test)]
    pub(crate) fn begin_causal_loader(
        &mut self,
        payload: TaskCreatePayload,
    ) -> Result<LoadingEventJob, (TaskHostError, TaskCreatePayload)> {
        self.begin_loading_admission(payload, LoadingReturnMode::Component)
            .map(|(job, sender)| {
                assert!(
                    sender.is_none(),
                    "component fixture issued no return sender"
                );
                job
            })
    }
    pub(crate) fn begin_host_loader(
        &mut self,
        payload: TaskCreatePayload,
    ) -> Result<LoadingHostJob, (TaskHostError, TaskCreatePayload)> {
        let (job, sender) = self.begin_loading_admission(payload, LoadingReturnMode::Host)?;
        let Some(sender) = sender else {
            std::mem::forget(job);
            std::process::abort();
        };
        match LoadingHostJob::from_admitted(job, sender) {
            Ok(job) => Ok(job),
            Err(refusal) => {
                std::mem::forget(refusal);
                std::process::abort();
            }
        }
    }
    /// Install the original job in its Task inventory before attempting the
    /// physical handoff. A closed/full executor returns that exact job here;
    /// it does not refund admission, run a getter, or acknowledge a capture.
    pub(crate) fn submit_loading(
        &mut self,
        payload: TaskCreatePayload,
        executor: &HostExecutorHandle<LoadingHostJob>,
    ) -> Result<LoadingSubmission, (TaskHostError, TaskCreatePayload)> {
        let job = self.begin_host_loader(payload)?;
        assert!(self.resources.pending_loading.is_none());
        self.resources.pending_loading = Some(job);
        Ok(self.retry_loading_submission(executor))
    }
    /// Address one actual Task when its paid ready position is scheduled.
    /// Callers must not spin on Pending or infer progress from an empty slot:
    /// the original report reader observes the independent physical return.
    pub(crate) fn retry_loading_submission(
        &mut self,
        executor: &HostExecutorHandle<LoadingHostJob>,
    ) -> LoadingSubmission {
        let Some(job) = self.resources.pending_loading.take() else {
            return LoadingSubmission::NoPendingJob;
        };
        match executor.try_submit(job) {
            Ok(()) => LoadingSubmission::Submitted,
            Err((reason, original)) => {
                self.resources.pending_loading = Some(original);
                LoadingSubmission::Pending(reason)
            }
        }
    }
    fn begin_loading_admission(
        &mut self,
        payload: TaskCreatePayload,
        mode: LoadingReturnMode,
    ) -> Result<LoadingAdmission, (TaskHostError, TaskCreatePayload)> {
        let (loader, input) = match self.reserve_loader_roles(&payload) {
            Ok(roles) => roles,
            Err(error) => return Err((error, payload)),
        };
        // The original report position exists before payload capture. Claim
        // its one owned return now, rather than requesting any storage when
        // the real host call completes. Direct component jobs are test-only.
        let mut ports = match mode {
            LoadingReturnMode::Host => {
                match loader.report.claim_owned_return::<LoadingJobReturned>() {
                    Ok(ports) => Some(ports),
                    Err(error) => {
                        // This is a fresh bound/queued producer, reserved
                        // report and first claim, before payload capture.
                        // Close/failure cannot reject it. Shared corruption
                        // must not unwind this poller or pretend unused exit.
                        std::mem::forget((error, input, loader, payload));
                        std::process::abort();
                    }
                }
            }
            #[cfg(test)]
            LoadingReturnMode::Component => None,
        };
        let mut authors = Vec::with_capacity(2);
        for _ in 0..2 {
            let result = self.reserve_source_slots::<2>(true).and_then(|sources| {
                self.resources
                    .causal
                    .admit_input(
                        &loader.work,
                        1,
                        self.pending.clone(),
                        CutSourceAdmission::Fresh(sources),
                    )
                    .map_err(|refusal| self.retain_source_refusal(refusal))
            });
            match result {
                Ok(author) => authors.push(author),
                Err(error) => {
                    for (_, sender) in authors {
                        sender.unused();
                    }
                    if let Some((sender, reader)) = ports.take() {
                        // This cell never held a value or crossed a host
                        // capture boundary. A refusal here is shared metadata
                        // corruption; do not unwind opaque input on a poller.
                        if let Err(refusal) = sender.revoke_unused() {
                            std::mem::forget(refusal);
                            std::process::abort();
                        }
                        drop(reader);
                    }
                    dispose_unused_completion(input);
                    dispose_unused_completion(loader);
                    return Err((error, payload));
                }
            }
        }
        let exited = authors.pop().expect("the original LoaderExited author");
        let loaded = authors.pop().expect("the original InputLoaded author");
        let exit_cut = exited.0;
        let (custody, job) = LoaderCustody::from_admitted(
            payload,
            loader,
            input,
            self.pending.clone(),
            self.health.clone(),
        );
        let events = LoadingEvents::from_admitted(
            self.task,
            custody.clone(),
            loaded,
            exited,
            &self.resources.causal,
        );
        self.resources
            .causal
            .hold_loader_exit(exit_cut, custody.clone())
            .expect("the original loader retains its own exit event receipt");
        let job = events
            .wrap_job(job)
            .unwrap_or_else(|_| panic!("the original loader job attaches once to its own authors"));
        self.resources.loader = Some(custody);
        self.resources.loading_events = Some(events);
        let sender = ports.map(|(sender, reader)| {
            self.resources.loading_return = Some(reader);
            sender
        });
        Ok((job, sender))
    }
    pub(crate) fn take_loading_return(
        &mut self,
    ) -> Result<Option<LoadingJobReturned>, TaskHostError> {
        match self.resources.loading_return.as_mut() {
            Some(reader) => reader.try_take().map_err(TaskHostError::Runtime),
            None => Ok(None),
        }
    }
    /// Returning a physical job supplies the original author receipt; routing
    /// IDs alone cannot apply it to another Task or a replacement custodian.
    pub(crate) fn observe_loading_author_return(
        &mut self,
        receipt: LoadingAuthorReturn,
    ) -> Result<(), (TaskHostError, LoadingAuthorReturn)> {
        if !self
            .resources
            .loading_events
            .as_ref()
            .is_some_and(|events| receipt.belongs_to(events))
        {
            return Err((TaskHostError::WrongTaskOrPayload, receipt));
        }
        self.resources
            .causal
            .observe_author_return(receipt)
            .map_err(|(error, receipt)| (TaskHostError::Causal(error), receipt))
    }
    /// Fault outcomes carry their original owner even when no positive author
    /// return was issued. They cannot be routed by numeric Task IDs alone.
    pub(crate) fn observe_loading_job_return(
        &mut self,
        returned: LoadingJobReturned,
    ) -> Result<LoadingEventOutcome, (TaskHostError, LoadingJobReturned)> {
        if !self
            .resources
            .loading_events
            .as_ref()
            .is_some_and(|events| returned.belongs_to(events))
        {
            return Err((TaskHostError::WrongTaskOrPayload, returned));
        }
        returned.try_observe_with(|receipt| self.observe_loading_author_return(receipt))
    }
    /// The send/install/notification call returns before this receipt can be
    /// observed. Preserve it independently of subsequent report failures.
    pub(crate) fn publish_loading_exit(
        &mut self,
    ) -> Result<Option<LoadingAuthorReturn>, LoadingEventError> {
        match self.resources.loading_events.as_ref() {
            Some(events) => events.publish_ready_exit(),
            None => Ok(None),
        }
    }
    pub(crate) fn observe_loading_exit_report(&mut self) -> Result<bool, LoadingEventError> {
        match self.resources.loading_events.as_ref() {
            Some(events) => {
                let before = events.is_retired();
                let retired = events.observe_exit_report(&mut self.resources.causal)?;
                if !before && events.is_retired() {
                    self.pending.pending();
                }
                Ok(retired)
            }
            None => Ok(false),
        }
    }
    #[cfg(test)]
    pub(crate) fn observe_loading_exit(&mut self) -> Result<bool, LoadingEventError> {
        if let Some(receipt) = self.publish_loading_exit()? {
            self.observe_loading_author_return(receipt)
                .unwrap_or_else(|(error, _receipt)| {
                    panic!("the actual loading publisher belongs to its original Task: {error}")
                });
        }
        self.observe_loading_exit_report()
    }
    /// Only the original Core's measured rejection can release this loader
    /// input. A caller timeout, terminal snapshot or missing preparation is
    /// not permission to destroy an input still waiting for an Accepted handoff.
    pub(crate) fn take_rejected_loader_input(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<super::loading::InputDisposalJob, TaskHostError> {
        if ticket.actor != self.actor
            || !matches!(&ticket.effect,
                WorkerAction::Context(ContextEffect::DisposeLoadedInput { task, payload })
                if *task == self.task && *payload == self.payload)
            || self.resources.preparation.is_some()
            || self.resources.pending_preparation_adoption.is_some()
            || self.resources.production.is_some()
        {
            return Err(TaskHostError::WrongEffect);
        }
        self.resources
            .causal
            .runtime()
            .validate_effect_ticket(ticket)
            .map_err(TaskHostError::Driver)?;
        self.resources
            .loader
            .as_ref()
            .and_then(LoaderCustody::take_input_disposal)
            .ok_or(TaskHostError::InputNotReady)
    }
    pub(crate) fn loader(&self) -> Option<&LoaderCustody> {
        self.resources.loader.as_ref()
    }
    pub(crate) fn adopt_preparation(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<TaskPreparationJob, TaskHostError> {
        if !matches!(&ticket.effect, WorkerAction::Context(ContextEffect::EnqueuePreparation { task, payload, .. }) if *task == self.task && *payload == self.payload)
        {
            return Err(TaskHostError::WrongTaskOrPayload);
        }
        if self.resources.preparation.is_some() {
            return Err(TaskHostError::DuplicatePreparation);
        }
        let accepted_slots = TaskLifecycleRole::COUNT
            .checked_sub(TaskLifecycleRole::LOADING.len())
            .and_then(|roles| roles.checked_mul(2))
            .ok_or(TaskHostError::InvalidGeometry)?;
        let source_storage = SOURCE_SLOT_STORAGE
            .checked_mul(accepted_slots)
            .and_then(|need| need.resources())
            .map_err(|_| TaskHostError::InvalidGeometry)?;
        let loader = self
            .resources
            .loader
            .as_ref()
            .ok_or(TaskHostError::InputNotReady)?
            .clone();
        let (request, input) = loader
            .take_measured_sources()
            .ok_or(TaskHostError::InputNotReady)?
            .into_parts();
        let (owner, job) = match PreparationCustody::adopt(
            runtime,
            ticket,
            input,
            PreparationBinding {
                stop: self.resources.port.stop_signal(),
                pending: self.pending.clone(),
                health: self.health.clone(),
            },
        ) {
            Ok(admitted) => admitted,
            Err((error, input)) => {
                loader.restore_sources(LoadingSources::new(request, input));
                return Err(TaskHostError::Preparation(error));
            }
        };
        self.resources.preparation = Some(owner.clone());
        let job = TaskPreparationJob {
            job: Some(job),
            request: Some(request),
            host: None,
            inbound: None,
            inbound_bound: false,
        };
        loader.input_transferred();
        #[cfg(test)]
        let detach = !self.source_storage_model;
        #[cfg(not(test))]
        let detach = true;
        if detach {
            match owner.take_task_source_storage(&source_storage) {
                Ok(storage) => self.resources.accepted_source_storage = Some(storage),
                Err(error) => {
                    self.resources.pending_preparation_adoption = Some(job);
                    return Err(TaskHostError::SourceStorage(error));
                }
            }
        }
        Ok(job)
    }
    pub(crate) fn bind_inbound_payment(
        &self,
        job: &mut TaskPreparationJob,
        context_lifetime: &novarocks_control_runtime::lease::WorkLease,
    ) -> Result<(), TaskHostError> {
        if job.inbound_bound {
            return Err(TaskHostError::DuplicatePreparation);
        }
        let owner = self
            .resources
            .preparation
            .as_ref()
            .ok_or(TaskHostError::InputNotReady)?;
        job.inbound = owner
            .take_inbound_payment()
            .map_err(TaskHostError::SourceStorage)?;
        job.inbound_bound = true;
        if let Some(permit) = &mut job.inbound {
            permit
                .transfer_to(context_lifetime)
                .map_err(|error| match error {
                    crate::TaskInboundPermitError::Runtime(error) => TaskHostError::Runtime(error),
                })?;
        }
        Ok(())
    }
    pub(crate) fn preparation(&self) -> Option<&PreparationCustody> {
        self.resources.preparation.as_ref()
    }
    pub(crate) fn collect_preparation_remainder(&mut self) -> Result<bool, RuntimeError> {
        if self.resources.preparation_remainder.is_some() {
            return Ok(true);
        }
        let Some(preparation) = self.resources.preparation.as_ref() else {
            return Ok(false);
        };
        self.resources.preparation_remainder = preparation.take_after_exit()?;
        Ok(self.resources.preparation_remainder.is_some())
    }
    /// One real repeated producer and one unique finalizer, both attached
    /// before any statistics event sink captures them. Replays and Reporter
    /// clones cannot reserve another input, Work or report.
    pub(crate) fn issue_statistics_producer(
        &mut self,
    ) -> Result<
        (
            crate::TaskStatisticsProducer,
            crate::TaskStatisticsFinalizer,
        ),
        TaskHostError,
    > {
        if self.scope.gates()?.failed {
            return Err(TaskHostError::OwnerFailed);
        }
        let kind = TaskLifecycleRole::OperatorStatistics;
        if self.resources.preparation.is_none() {
            return Err(TaskHostError::CompletionBeforeAccepted);
        }
        if std::mem::replace(&mut self.lifecycle_issued[kind as usize], true) {
            return Err(TaskHostError::LifecycleAlreadyIssued(kind));
        }
        let role = reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            WorkKind::Cleanup,
            Some(self.core_generation),
        )?;
        let runtime = self.resources.causal.runtime().clone();
        let permit = match runtime.reserve_input(
            self.actor,
            self.task_source,
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
        ) {
            Ok(permit) => permit,
            Err(error) => {
                dispose_unused_completion(role);
                return Err(TaskHostError::Driver(error));
            }
        };
        let input = match permit.into_role() {
            Ok(input) => input,
            Err((error, permit)) => {
                permit
                    .revoke_unused()
                    .expect("no statistics payload was attached");
                dispose_unused_completion(role);
                return Err(TaskHostError::Driver(error));
            }
        };
        let (inbox, producer, finalizer) = TaskStatisticsInbox::from_admitted(
            role,
            input,
            StatisticsBinding {
                task: self.task,
                runtime,
                actor: self.actor,
                pending: self.pending.clone(),
                health: self.health.clone(),
            },
        );
        self.resources.statistics = Some(inbox);
        Ok((producer, finalizer))
    }
    pub(crate) fn statistics(&mut self) -> Option<&mut TaskStatisticsInbox> {
        self.resources.statistics.as_mut()
    }
    /// Detached views borrow this original owner and its one read authority;
    /// they issue no Work, lifecycle input or physical-exit notice.
    pub(crate) fn reporter(&self) -> crate::TaskStatusReporter {
        crate::TaskStatusReporter::from_original(
            self.resources.port.execution_view(),
            self.resources.lifetime.observation(),
        )
    }
    /// Issue before the dormant runtime start captures this capability. Only
    /// this owner can consume the original Running role's paid inventory.
    pub(crate) fn issue_start_permit(&mut self) -> Result<crate::TaskStartPermit, TaskHostError> {
        let generation = self.core_generation;
        self.issue_lifecycle_author(TaskLifecycleRole::Running)
            .map(|author| crate::TaskStartPermit::from_admitted_author(author, generation))
    }
    pub(crate) fn issue_terminal_permit(
        &mut self,
    ) -> Result<crate::TaskTerminalPermit, TaskHostError> {
        self.issue_lifecycle_author(TaskLifecycleRole::TerminalObservation)
            .map(crate::TaskTerminalPermit::from_admitted_author)
    }
    pub(crate) fn publish_context_close(
        &mut self,
        generation: Generation,
        intent: novarocks_worker_protocol::context::TaskCloseIntent,
    ) -> Result<(), TaskHostError> {
        let kind = match intent {
            novarocks_worker_protocol::context::TaskCloseIntent::Quiesce => {
                TaskLifecycleRole::QuiesceClose
            }
            novarocks_worker_protocol::context::TaskCloseIntent::Abort(_) => {
                TaskLifecycleRole::ContextClose
            }
        };
        self.issue_lifecycle_author_bound(kind, Some((generation, intent)))?
            .publish_close(generation, intent)
            .map_err(TaskHostError::CompletionAction)
    }
    fn issue_lifecycle_author(
        &mut self,
        kind: TaskLifecycleRole,
    ) -> Result<InputAuthor, TaskHostError> {
        self.issue_lifecycle_author_bound(kind, None)
    }
    fn issue_lifecycle_author_bound(
        &mut self,
        kind: TaskLifecycleRole,
        close: Option<(
            Generation,
            novarocks_worker_protocol::context::TaskCloseIntent,
        )>,
    ) -> Result<InputAuthor, TaskHostError> {
        if self.resources.preparation.is_none() && !kind.is_context_close() {
            return Err(TaskHostError::CompletionBeforeAccepted);
        }
        if std::mem::replace(&mut self.lifecycle_issued[kind as usize], true) {
            return Err(TaskHostError::LifecycleAlreadyIssued(kind));
        }
        let role = reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            if kind == TaskLifecycleRole::Running {
                WorkKind::Business
            } else {
                WorkKind::Cleanup
            },
            Some(self.core_generation),
        )?;
        let sources = match self.reserve_source_slots::<2>(kind.is_context_close()) {
            Ok(sources) => sources,
            Err(error) => {
                dispose_unused_completion(role);
                return Err(error);
            }
        };
        let admitted = if let Some((generation, intent)) = close {
            self.resources.causal.admit_close(
                generation,
                intent,
                &role.work,
                1,
                self.pending.clone(),
                CutSourceAdmission::Fresh(sources),
            )
        } else {
            self.resources.causal.admit_input(
                &role.work,
                1,
                self.pending.clone(),
                CutSourceAdmission::Fresh(sources),
            )
        };
        let (cut, sender) = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => {
                let error = self.retain_source_refusal(refusal);
                dispose_unused_completion(role);
                return Err(error);
            }
        };
        let (custody, author) = CausalInputCustody::from_admitted(
            role,
            &mut self.resources.causal,
            cut,
            sender,
            CausalInputBinding {
                task: self.task,
                pending: self.pending.clone(),
                health: self.health.clone(),
                unresolved: novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CausalInputUnresolved.reason(),
                ),
            },
        );
        assert!(
            self.resources
                .lifecycle_inputs
                .insert(kind, custody)
                .is_none()
        );
        Ok(author)
    }
    /// Observe the same finite original roles. Physical author return and
    /// consumption of its exact Core event remain independent report legs.
    pub(crate) fn observe_lifecycle_inputs(&mut self) -> Result<bool, TaskHostError> {
        let mut retired = true;
        for input in self.resources.lifecycle_inputs.values() {
            let before = input.phase();
            retired &= input
                .observe_consumers(&mut self.resources.causal)
                .map_err(TaskHostError::CompletionAction)?;
            if before != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
                && input.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Retired
            {
                self.pending.pending();
            }
        }
        Ok(retired)
    }
    pub(crate) fn lifecycle_input(&self, kind: TaskLifecycleRole) -> Option<&CausalInputCustody> {
        self.resources.lifecycle_inputs.get(&kind)
    }
    /// Called once before the dormant fragment captures any original callback.
    /// Each role consumes a header from the already paid Task storage; the
    /// zero-resource reservations add real child work, not duplicate quota.
    pub(crate) fn issue_completion_roles(
        &mut self,
        registration: Box<dyn RegistrationNotice>,
    ) -> Result<AdmittedFragmentRoles<ExecutionFailure>, (TaskHostError, Box<dyn RegistrationNotice>)>
    {
        match self.scope.gates() {
            Ok(gates) if gates.failed => return Err((TaskHostError::OwnerFailed, registration)),
            Err(error) => return Err((TaskHostError::Runtime(error), registration)),
            Ok(_) => {}
        }
        if self.resources.preparation.is_none() {
            return Err((TaskHostError::CompletionBeforeAccepted, registration));
        }
        if self.completion_issued {
            return Err((TaskHostError::CompletionAlreadyIssued, registration));
        }
        self.completion_issued = true;
        let mut roles = Vec::<AdmittedCompletionRole>::with_capacity(4);
        for kind in [
            WorkKind::Cleanup,
            WorkKind::Cleanup,
            WorkKind::Cleanup,
            WorkKind::Settlement,
        ] {
            let parent = if kind == WorkKind::Settlement {
                roles[0].capacity.id()
            } else {
                self.resources.active.id()
            };
            match reserve_reported_role(&self.scope, parent, kind, Some(self.core_generation)) {
                Ok(role) => roles.push(role),
                Err(error) => {
                    while let Some(role) = roles.pop() {
                        dispose_unused_completion(role);
                    }
                    return Err((TaskHostError::Runtime(error), registration));
                }
            }
        }
        let child = roles.pop().expect("one original settlement child");
        let publisher = roles.pop().expect("one original terminal publisher");
        let wall = roles.pop().expect("one original wall-time finisher");
        let finisher = roles.pop().expect("one original fragment finisher");
        let (owner, admitted) = TaskCompletionRoles::from_admitted(
            self.task,
            [finisher, wall, publisher],
            child,
            registration,
            self.pending.clone(),
            self.health.clone(),
        );
        self.resources.completion = Some(Arc::new(owner));
        Ok(admitted)
    }
    pub(crate) fn completion(&self) -> Option<&Arc<TaskCompletionRoles>> {
        self.resources.completion.as_ref()
    }
    /// The Worker terminal action is a separate real holder from Execution's
    /// terminal notifier. Reserve its original Work/report and causal input
    /// before constructing the reporter/action captures, exactly once per Task.
    pub(crate) fn issue_completion_exit_notice(
        &mut self,
    ) -> Result<crate::CompletionExitNotice, TaskHostError> {
        if self.scope.gates()?.failed {
            return Err(TaskHostError::OwnerFailed);
        }
        if self.resources.preparation.is_none() {
            return Err(TaskHostError::CompletionBeforeAccepted);
        }
        if self.completion_action_issued {
            return Err(TaskHostError::CompletionAlreadyIssued);
        }
        self.completion_action_issued = true;
        let role = reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            WorkKind::Cleanup,
            Some(self.core_generation),
        )?;
        let sources = match self.reserve_source_slots::<2>(false) {
            Ok(sources) => sources,
            Err(error) => {
                dispose_unused_completion(role);
                return Err(error);
            }
        };
        let (cut, sender) = match self.resources.causal.admit_input(
            &role.work,
            1,
            self.pending.clone(),
            CutSourceAdmission::Fresh(sources),
        ) {
            Ok(admitted) => admitted,
            Err(refusal) => {
                let error = self.retain_source_refusal(refusal);
                dispose_unused_completion(role);
                return Err(error);
            }
        };
        let (custody, notice) = CompletionActionCustody::from_admitted(
            self.task,
            role,
            &mut self.resources.causal,
            cut,
            sender,
            self.pending.clone(),
            self.health.clone(),
        );
        self.resources.completion_action = Some(custody);
        Ok(notice)
    }
    pub(crate) fn completion_action(&self) -> Option<&CompletionActionCustody> {
        self.resources.completion_action.as_ref()
    }
    /// Consume only this original action's proven report legs, before the
    /// corresponding runtime inputs retire. Source storage remains with its
    /// original aggregate payer until the Task's full inventory converges.
    pub(crate) fn observe_completion_action(&mut self) -> Result<bool, TaskHostError> {
        let Some(action) = self.resources.completion_action.as_ref() else {
            return Ok(true);
        };
        let before = action.phase();
        let retired = action
            .observe_consumers(&mut self.resources.causal)
            .map_err(TaskHostError::CompletionAction)?;
        if before != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
            && action.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Retired
        {
            self.pending.pending();
        }
        Ok(retired)
    }
    /// Bind before giving the original job to its actual fair host queue. The
    /// preparation custodian, rather than a detached runtime alias, retains
    /// provider-child cleanup until real adoption/rollback finishes.
    pub(crate) fn bind_host_preparation(
        &self,
        job: &mut TaskPreparationJob,
        roles: crate::TaskPreparationRoles,
        cadence: std::time::Duration,
    ) -> Result<(), crate::TaskPreparationRoles> {
        if !roles.matches(self.task)
            || job.host.is_some()
            || !job
                .request
                .as_ref()
                .is_some_and(|request| request.identity() == self.task)
        {
            return Err(roles);
        }
        let preparation = self
            .resources
            .preparation
            .as_ref()
            .expect("Accepted preparation exists");
        let children = Arc::new(crate::task_preparation::TaskPreparationScope::new(
            self.task,
            self.health.clone(),
        ));
        preparation.bind_host_children(children.clone());
        job.host = Some(HostPreparationBinding {
            port: self.resources.port.clone(),
            children,
            cadence,
            roles,
            pending: self.pending.clone(),
            health: self.health.clone(),
        });
        Ok(())
    }
    /// Read-only inventory used to assess actual retirement readiness. This
    /// borrow transfers no resource or exit authority.
    pub(super) fn resources_for_retirement(&self) -> &TaskHostResources {
        &self.resources
    }
    /// Host ownership transfer only. The receiving cleanup job must dispose
    /// opaque sources/captures and observe real body/children/report exits
    /// before releasing these bundles; Core terminal is no substitute.
    pub(crate) fn into_resources(self) -> TaskHostResources {
        self.resources
    }
}

/// Component teardown owns never-issued original callbacks. This calls the
/// same real unused/report/reaper protocol; it supplies no synthetic exit and
/// cannot withdraw a queued or started production host call.
#[cfg(test)]
pub(in crate::control_runtime) fn retire_unused_host_calls_for_test(
    calls: &[TaskHostCall],
    ledger: &novarocks_control_runtime::transaction::RuntimeLedger,
) {
    for call in calls {
        if call.is_retired() {
            continue;
        }
        let mut retired = false;
        for _ in 0..64 {
            if call.retire_unused(ledger).unwrap() {
                retired = true;
                break;
            }
        }
        assert!(retired && call.is_retired());
    }
}

#[cfg(test)]
fn test_model_source_roles<const N: usize>(
    scope: &ScopeHandle,
    parent: ReservationId,
    core_generation: Generation,
) -> [novarocks_control_runtime::reported_work::ReportedWork; N] {
    std::array::from_fn(|_| {
        let role =
            reserve_reported_role(scope, parent, WorkKind::Cleanup, Some(core_generation)).unwrap();
        super::admitted_role::adopt_reported_work(role.work, role.capacity, role.report)
    })
}
#[cfg(test)]
fn test_model_source_slots<const N: usize>(
    scope: &ScopeHandle,
    parent: ReservationId,
    core_generation: Generation,
) -> [SourceSlotAdmission; N] {
    test_model_source_roles::<N>(scope, parent, core_generation)
        .map(SourceSlotAdmission::protocol_model)
}

#[cfg(test)]
mod tests {
    mod task_rpc_binding;
    use super::*;
    use novarocks_control_runtime::{exit_role::ExitRole, transaction::RuntimeLedger};
    use novarocks_control_runtime_core::identity::ConsumerId;
    use novarocks_control_runtime_core::{
        identity::{DimensionId, Generation},
        scope::WorkAdmission,
    };
    #[test]
    fn cleanup_role_admitted_after_failure_keeps_the_original_core_consumer_generation() {
        let generation = Generation::new(1).unwrap();
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 4, 1);
        let scope = ledger.open_scope_next(None, generation, 4, 4).unwrap();
        let (mut parent, mut capacity) = scope
            .reserve_next(
                generation,
                resources.clone(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let failed = scope.fail().unwrap();
        assert!(failed > generation);
        let role =
            reserve_reported_role(&scope, capacity.id(), WorkKind::Cleanup, Some(generation))
                .unwrap();
        let report = role.report.clone();
        report
            .abandon_failed_owner(ConsumerId::new(1).unwrap(), &scope, generation)
            .expect(
                "old Core consumer is abandoned, including cleanup headers issued after failure",
            );
        let (role, mut authority) =
            ExitRole::from_admitted(crate::control_runtime::admitted_role::adopt_reported_work(
                role.work,
                role.capacity,
                role.report,
            ));
        authority.start().unwrap();
        authority.exited().unwrap();
        assert!(
            !role.try_retire().unwrap(),
            "actual observer consumption remains necessary"
        );
        report.consume(ConsumerId::new(2).unwrap()).unwrap();
        assert!(role.try_retire().unwrap());
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        ledger.reap_scope_ready(scope.id(), 4).unwrap();
        capacity.release(&resources).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}

/// Actual captures produced by the original preparation. Metadata readers
/// cannot destroy the dormant runtime or its exact Root channel.
#[derive(Default)]
struct ProductionPreparedState {
    host_install_attempted: bool,
    installation_observed: bool,
    installation_failed: bool,
    root_closed: bool,
    root_close_reason: Option<novarocks_execution_contract::root_lifetime::RootRetentionClose>,
    root_ownership_reported: bool,
    root_idle_author: Option<InputAuthor>,
    root: Option<std::sync::Weak<RootRetentionCustody>>,
    root_handoff: Option<RootContextRetention>,
    runnable: Option<std::mem::ManuallyDrop<Arc<dyn crate::RunnableTask>>>,
    descriptor: Option<novarocks_execution_contract::TaskDescriptor>,
    refusal: Option<HostPreparationRefusal>,
    activated: bool,
    activation_queued: bool,
    retired: bool,
    retirement_started: bool,
    inbound: Option<crate::TaskInboundPermit>,
    // One fixed cold-failure position: typed refusal OR panic, never both.
    retirement_failure: Option<ExecutionRetirementFailure>,
    terminal_return: Option<Arc<crate::task_lifecycle::PreparationTerminalReturn>>,
    rollback_job_active: bool,
    rollback_finished: bool,
}
enum ExecutionRetirementFailure {
    Rejected(std::mem::ManuallyDrop<Box<crate::HostRejection>>),
    Panicked(std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionRetirementOutcome {
    Completed,
    Rejected,
    Unresolved,
}
#[derive(Default)]
pub(crate) struct ProductionPreparation {
    root_wake: Option<Arc<super::context_host::task_driver::TaskRootWake>>,
    root_binding: Option<(TaskIdentity, QueryContextRef, ScopeHandle)>,
    health: Option<Arc<ExecutionHealth>>,
    state: std::sync::Mutex<ProductionPreparedState>,
}
impl ProductionPreparation {
    pub(crate) fn is_retired(&self) -> bool {
        self.state
            .lock()
            .expect("original production captures")
            .retired
    }
    pub(crate) fn installation_observed(&self) -> bool {
        self.state
            .lock()
            .expect("original production captures")
            .installation_observed
    }
}
impl TaskHostRecord {
    pub(crate) fn trace_draining_originals(&self) {
        let status = self.resources.port.current();
        let convergence = self.resources.port.publication().borrow().convergence();
        let production = self.resources.production.as_ref().map(|owner| {
            let state = owner.state.lock().expect("original Task drain observation");
            (
                state.host_install_attempted,
                state.installation_observed,
                state.installation_failed,
                state.activated,
                state.retired,
                state.rollback_job_active,
                state.rollback_finished,
                state.root.is_some(),
            )
        });
        let inputs = self
            .resources
            .lifecycle_inputs
            .iter()
            .map(|(kind, input)| (*kind, input.cut(), input.phase(), input.actual_exit_ready()))
            .collect::<Vec<_>>();
        let cuts = self
            .resources
            .causal
            .cut_inventory(None, 8)
            .into_iter()
            .map(|cut| (cut, self.resources.port.ingress().cut_consumption(cut)))
            .collect::<Vec<_>>();
        let calls = self
            .resources
            .host_calls
            .iter()
            .map(|call| (call.kind(), call.actual_call_exited(), call.is_retired()))
            .collect::<Vec<_>>();
        tracing::debug!(pid = std::process::id(), task = ?self.task,
            state = ?status.state(), installed = status.installed(), terminating = status.termination().is_some(),
            ?convergence, output = ?status.output(),
            loader = ?self.resources.loader.as_ref().map(|loader| loader.phase()),
            preparation_body = self.resources.preparation.as_ref().map(|p| p.body_and_captures_exited()),
            preparation_report = self.resources.preparation.as_ref().map(|p| p.exit_report_retired()),
            remainder = self.resources.preparation_remainder.is_some(), ?production, ?inputs, ?cuts, ?calls,
            normal_source_detached = ?self.normal_source_detached,
            normal_lifetime_ended = self.normal_lifetime_ended,
            lifetime = ?self.resources.lifetime.id(),
            lifetime_actual_exit = ?self.resources.lifetime.actual_exit_ready(),
            completion_core_observed = self.completion_core_observed,
            completion_retired = ?self.resources.completion.as_ref().map(|roles| roles.is_retired()),
            completion_action_retired = ?self.resources.completion_action.as_ref()
                .map(|action| action.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Retired),
            statistics_retired = ?self.resources.statistics.as_ref().map(|source| source.is_retired()),
            dynamic_filters_retired = ?self.resources.dynamic_filters.as_ref().map(|source| source.is_retired()),
            open_summary_closed = ?self.resources.open_summary.as_ref().map(|source| source.is_closed()),
            open_summary_drained = self.resources.port.ingress().open_summary_drained(),
            "Worker original Task inventory at process drain");
        self.resources.causal.trace_drain_inventory(8);
    }
    /// Retry only this original preparation's provider rollback on a physical
    /// worker. The already admitted preparation row retains its whole liability.
    pub(crate) fn take_preparation_rollback_job(
        &self,
        host: Arc<dyn crate::TaskExecutionHost>,
    ) -> Option<super::context_host::task_driver::TaskDriverHostJob> {
        let preparation = self.resources.preparation.as_ref()?;
        if !preparation.body_and_captures_exited() {
            return None;
        }
        let owner = self.resources.production.as_ref()?;
        let mut state = owner.state.lock().expect("original preparation rollback");
        if state.rollback_job_active
            || state.rollback_finished
            || state.terminal_return.is_none()
            // A submitted runnable owns the original dormant kernel and its
            // completion authorities. Its existing activation drains a stopped
            // kernel through real cleanup; never-adopted rollback must not
            // race that handoff and strand those same authorities.
            || state.runnable.is_some()
            || (!state.installation_failed
                && (state.activated || !self.resources.port.is_stopped()))
        {
            return None;
        }
        state.rollback_job_active = true;
        Some(
            super::context_host::task_driver::TaskDriverHostJob::RollbackPreparation {
                preparation: preparation.clone(),
                owner: owner.clone(),
                host,
                pending: self.pending.clone(),
            },
        )
    }
}
impl ProductionPreparation {
    pub(crate) fn rollback_on_host(
        &self,
        preparation: &PreparationCustody,
        host: &dyn crate::TaskExecutionHost,
        pending: &dyn TaskPendingNotice,
    ) {
        let (descriptor, returned) = {
            let state = self.state.lock().expect("original rollback captures");
            (state.descriptor.clone(), state.terminal_return.clone())
        };
        let mut finished = preparation.rollback_host_runtime(
            || {
                if let Some(descriptor) = &descriptor {
                    host.remove_inbound_capability(descriptor);
                }
            },
            || {
                if let Some(descriptor) = &descriptor {
                    host.remove_receiver(descriptor);
                }
            },
        );
        if finished {
            if let (Some(children), Some(returned)) = (preparation.host_children(), returned) {
                if let Some(proof) = children.never_adopted_rollback_exit() {
                    // Provider rollback and return of both original callback
                    // authorities are independent. A later real return wakes
                    // this same owner; do not seal it while one is still held.
                    finished = false;
                    if proof.output_exit() == crate::TaskPreparationOutputExit::Unknown {
                        // Actual child/capture exit does not prove output exit.
                        // Retain both original terminal authorities and the
                        // original P/payment under the same unresolved owner.
                        children.output_exit_unresolved();
                    } else {
                        match returned.take_after_rollback(&proof) {
                            Ok(Some((mut terminal, mut exit))) => {
                                terminal.bind();
                                exit.bind();
                                // Failure/control proposals precede this original
                                // role in the same causal FIFO. No replacement
                                // terminal or physical-exit authority is minted.
                                let _ = terminal.handoff(crate::TaskTerminalObservation {
                                    intent: crate::TaskTerminalIntent::FinishExistingTermination {
                                        output: None,
                                    },
                                    normal_close_validated: false,
                                    // Only the same original physical Poll's
                                    // positive output fact authorizes this field.
                                    output_released: matches!(
                                        proof.output_exit(),
                                        crate::TaskPreparationOutputExit::NoOutputCreated
                                            | crate::TaskPreparationOutputExit::ActualOutputExited
                                    ),
                                    final_statistics: None,
                                });
                                exit.completed();
                                finished = true;
                            }
                            Ok(None) => {}
                            Err(
                                crate::task_lifecycle::PreparationTerminalReturnError::AlreadyTaken,
                            ) => {
                                finished = true;
                            }
                            Err(error) => {
                                tracing::error!(
                                    ?error,
                                    "Original preparation terminal return failed"
                                );
                            }
                        }
                    }
                }
            }
        }
        let mut state = self
            .state
            .lock()
            .expect("actual preparation rollback return");
        state.rollback_job_active = false;
        state.rollback_finished = finished;
        drop(state);
        if finished {
            pending.pending();
        }
    }
}
impl TaskHostRecord {
    pub(crate) fn issue_installed_author(&mut self) -> Result<InputAuthor, TaskHostError> {
        self.issue_lifecycle_author(TaskLifecycleRole::Installed)
    }
    pub(crate) fn bind_production_preparation(
        &mut self,
        root_wake: Arc<super::context_host::task_driver::TaskRootWake>,
    ) -> Result<Arc<ProductionPreparation>, TaskHostError> {
        if self.resources.production.is_some() {
            return Err(TaskHostError::DuplicatePreparation);
        }
        let owner = Arc::new(ProductionPreparation {
            root_wake: Some(root_wake),
            root_binding: Some((self.task, self.context, self.scope.clone())),
            health: Some(self.health.clone()),
            ..ProductionPreparation::default()
        });
        self.resources.production = Some(owner.clone());
        let root_idle = self.issue_lifecycle_author(TaskLifecycleRole::RootIdle)?;
        owner
            .state
            .lock()
            .expect("original Root idle author")
            .root_idle_author = Some(root_idle);
        Ok(owner)
    }
    pub(crate) fn preparation_roles(
        &mut self,
    ) -> Result<crate::TaskPreparationRoles, TaskHostError> {
        let (settlement_cut, registration) = self.admit_settlement_notice(1)?;
        let fragments = match self.issue_completion_roles(registration) {
            Ok(fragments) => fragments,
            Err((error, registration)) => {
                std::mem::forget(registration);
                return Err(error);
            }
        };
        // Registration bound this exact cut to the same original settlement
        // Work before any callback escaped. Keep its address until that real
        // callback/capture exit can reach the causal owner.
        let settlement_work = self
            .resources
            .completion
            .as_ref()
            .expect("original completion roles")
            .settlement_work();
        self.production_settlement_cut = Some((settlement_cut, settlement_work));
        let start = self.issue_start_permit()?;
        let terminal = self.issue_terminal_permit()?;
        let (statistics, final_statistics) = self.issue_statistics_producer()?;
        let dynamic_filters = self.issue_dynamic_filter_producer()?;
        let completion_exit = self.issue_completion_exit_notice()?;
        Ok(crate::TaskPreparationRoles {
            fragments,
            start,
            terminal,
            statistics,
            dynamic_filters,
            final_statistics,
            completion_exit,
        })
    }
}
impl TaskPreparationJob {
    pub(crate) fn install_complete(
        mut self,
        host: Arc<dyn crate::TaskExecutionHost>,
        reporter: crate::TaskStatusReporter,
        progressions: Arc<Vec<novarocks_execution_contract::DomainProgression>>,
        initial_refusal: Option<novarocks_execution_contract::DomainConflict>,
        installed: InputAuthor,
        owner: Arc<ProductionPreparation>,
    ) -> PreparationOutcome<()> {
        assert!(
            self.inbound_bound,
            "physical preparation requires its original inbound payment decision"
        );
        {
            let mut state = owner
                .state
                .lock()
                .expect("original preparation inbound payment");
            assert!(state.inbound.is_none(), "one original inbound payment");
            state.inbound = self.inbound.take();
        }
        let diagnostic_task = self
            .request
            .as_ref()
            .expect("original preparation request")
            .identity();
        let diagnostic_key = self.job.as_ref().expect("original preparation body").key();
        let installed_cut = installed.custody().cut();
        let mut binding = self.host.take().expect("original host preparation binding");
        let children = binding.children.clone();
        let pending = binding.pending.clone();
        let terminal_return = crate::task_lifecycle::PreparationTerminalReturn::new(
            children.clone(),
            pending.clone(),
        );
        terminal_return
            .bind_original(
                &mut binding.roles.terminal,
                &mut binding.roles.completion_exit,
            )
            .expect("same original preparation terminal roles");
        owner
            .state
            .lock()
            .expect("original preparation terminal return")
            .terminal_return = Some(terminal_return);
        owner
            .state
            .lock()
            .expect("original preparation descriptor")
            .descriptor = self
            .request
            .as_ref()
            .map(|request| request.descriptor().clone());
        let author = Arc::new(std::sync::Mutex::new(Some(installed)));
        let sent = author.clone();
        let exited_owner = owner.clone();
        let outcome = self.run(move |request, input, parent| {
            let refusal = match RefusalPosition::reserve(&parent) {
                Ok(position) => position,
                Err(_) => {
                    drop(input);
                    return;
                }
            };
            if owner
                .state
                .lock()
                .expect("original Root issuance fence")
                .root_closed
            {
                drop(input);
                owner
                    .state
                    .lock()
                    .expect("original rejected installation")
                    .installation_failed = true;
                return;
            }
            let preparation = crate::TaskPreparationContext::from_admitted(
                binding.port.execution_port(),
                &binding.children,
                binding.cadence,
                binding.roles,
                std::task::Waker::from(
                    owner
                        .root_wake
                        .as_ref()
                        .expect("original preparation Root readiness source")
                        .clone(),
                ),
            )
            .expect("same original preparation roles");
            let prepared = (|| {
                // This pure journal refusal belongs to the original already
                // Accepted preparation. Its payload, unused roles and failure
                // publication exit through this real physical invocation.
                if let Some(conflict) = initial_refusal {
                    tracing::debug!(pid = std::process::id(), task = ?diagnostic_task,
                        work = ?diagnostic_key, installed_cut = ?installed_cut, ?conflict,
                        "Original initial preparation refusal entered physical body");
                    return Err(crate::HostRejection::from_domain(
                        novarocks_execution_contract::TaskFailureCategory::Protocol,
                        conflict,
                    ));
                }
                let permit = owner
                    .state
                    .lock()
                    .expect("original inbound reservation handoff")
                    .inbound
                    .take();
                if let Err(refusal) =
                    host.reserve_inbound_close_capacity_with_permit(request.descriptor(), permit)
                {
                    owner
                        .state
                        .lock()
                        .expect("original refused inbound payment")
                        .inbound = refusal.permit;
                    return Err(refusal.rejection);
                }
                owner
                    .state
                    .lock()
                    .expect("original host installation call")
                    .host_install_attempted = true;
                let installation =
                    host.install_receiver(request.descriptor(), input, preparation)?;
                installation.validate_task(request.descriptor())?;
                let (_, root) = installation.into_parts();
                {
                    let mut state = owner.state.lock().expect("original production captures");
                    state.installation_observed = true;
                    if let Some(root) = root.as_ref() {
                        let (task, context, scope) = owner
                            .root_binding
                            .as_ref()
                            .expect("original installed Root binding");
                        let token = RootContextRetention::from_installed(
                            *task,
                            *context,
                            scope.clone(),
                            owner
                                .root_wake
                                .as_ref()
                                .expect("original Root wake")
                                .clone(),
                            root.clone(),
                        );
                        state.root = Some(token.downgrade());
                        state.root_handoff = Some(token);
                    }
                    state.descriptor = Some(request.descriptor().clone());
                }
                if let Some(root) = root {
                    let waker = std::task::Waker::from(
                        owner
                            .root_wake
                            .as_ref()
                            .expect("original Root wake owner")
                            .clone(),
                    );
                    if let Err(original) = root.bind_exit_waker(waker) {
                        std::mem::forget(original);
                        std::process::abort();
                    }
                    // The Task temporarily holds only the unique handoff.
                    // The original Context metadata turn adopts the unique
                    // token before Installed can activate this producer.
                }
                if owner
                    .state
                    .lock()
                    .expect("original Root issuance fence")
                    .root_closed
                {
                    let sealing = {
                        let state = owner.state.lock().expect("same original Root");
                        state.root_handoff.as_ref().map(|token| {
                            token.prepare_seal_for_context(
                                state
                                    .root_close_reason
                                    .expect("original Context Root close reason"),
                            )
                        })
                    };
                    match sealing {
                        Some(Ok(RetainedRootSealPreparation::Job(job))) => {
                            if job.finish_on_host().is_err() {
                                std::panic::panic_any(
                                    crate::root_result_channel::RootChannelError::Producer,
                                );
                            }
                        }
                        Some(Err(_)) => std::panic::panic_any(
                            crate::root_result_channel::RootChannelError::Producer,
                        ),
                        Some(Ok(
                            RetainedRootSealPreparation::Ready
                            | RetainedRootSealPreparation::Pending,
                        ))
                        | None => {}
                    }
                    binding.children.request_rollback();
                    binding
                        .children
                        .cleanup(crate::task_preparation::PreparationCleanup::Inbound, || {
                            host.remove_inbound_capability(request.descriptor())
                        });
                    binding.children.cleanup(
                        crate::task_preparation::PreparationCleanup::Receiver,
                        || host.remove_receiver(request.descriptor()),
                    );
                    return Ok(());
                }
                host.install_inbound_capability(request.descriptor())?;
                let queued = crate::task_domain_execution::apply_planned_task_domain_updates(
                    host.as_ref(),
                    request.descriptor(),
                    request.initial_domains(),
                    &progressions,
                    request.envelope().operation_id(),
                )
                .map_err(|error| error.into_host())?;
                let runnable = host.submit_runnable(request.descriptor(), reporter.clone())?;
                owner
                    .state
                    .lock()
                    .expect("original production captures")
                    .runnable = Some(std::mem::ManuallyDrop::new(runnable));
                let input = novarocks_worker_protocol::context::ContextInput::InstalledTask {
                    task: request.identity(),
                    initial_queued: queued,
                };
                let author = sent
                    .lock()
                    .expect("original Installed author")
                    .take()
                    .expect("single Installed fact");
                // Rejection retains this same input in its original custody.
                let _ = author.publish(input);
                Ok::<(), crate::HostRejection>(())
            })();
            if let Err(rejection) = prepared {
                {
                    let mut state = owner.state.lock().expect("original preparation result");
                    if !state.installation_observed {
                        state.installation_failed = true;
                    }
                }
                tracing::debug!(pid = std::process::id(), task = ?diagnostic_task,
                    work = ?diagnostic_key, installed_cut = ?installed_cut,
                    fact = ?rejection.operation_failure().fact(),
                    "Original preparation submitted its actual failure proposal");
                let failure = refusal
                    .retain(
                        rejection,
                        request.identity(),
                        binding.pending,
                        binding.health,
                    )
                    .into_retained_source();
                reporter.failing(
                    failure,
                    novarocks_execution_contract::TaskFailurePhase::Preparation,
                );
                binding.children.request_rollback();
                binding
                    .children
                    .cleanup(crate::task_preparation::PreparationCleanup::Inbound, || {
                        host.remove_inbound_capability(request.descriptor())
                    });
                binding.children.cleanup(
                    crate::task_preparation::PreparationCleanup::Receiver,
                    || host.remove_receiver(request.descriptor()),
                );
            }
        });
        if matches!(
            outcome,
            PreparationOutcome::TaskStopped | PreparationOutcome::StartRefused(_)
        ) {
            // This actual physical invocation disposed its never-run closure
            // and input. No receiver/Root could have been installed by it.
            tracing::debug!(pid = std::process::id(), task = ?diagnostic_task,
                work = ?diagnostic_key, installed_cut = ?installed_cut,
                start_refused = matches!(outcome, PreparationOutcome::StartRefused(_)),
                "Original preparation physically disposed its unstarted invocation");
            exited_owner
                .state
                .lock()
                .expect("unstarted preparation exit")
                .installation_failed = true;
        }
        if initial_refusal.is_some() {
            let outcome_kind = match &outcome {
                PreparationOutcome::Returned(_) => "Returned",
                PreparationOutcome::TaskStopped => "TaskStopped",
                PreparationOutcome::StartRefused(_) => "StartRefused",
                PreparationOutcome::Unresolved => "Unresolved",
            };
            tracing::debug!(pid = std::process::id(), task = ?diagnostic_task,
                work = ?diagnostic_key, installed_cut = ?installed_cut, outcome_kind,
                "Original initial-refusal preparation invocation returned");
        }
        children.body_exited();
        pending.pending();
        if let Some(author) = author.lock().expect("original Installed author").take() {
            let _ = author.withdraw();
        }
        outcome
    }
}

impl ProductionPreparation {
    /// Called only by the physical host runner after the real Installed
    /// snapshot has committed. The queued capture itself grants no start.
    pub(crate) fn activate(&self, preparation: &PreparationCustody) -> bool {
        let runnable = {
            let state = self.state.lock().expect("original production captures");
            if state.activated {
                return true;
            }
            state.runnable.as_ref().map(|runnable| Arc::clone(runnable))
        };
        let Some(runnable) = runnable else {
            return false;
        };
        match preparation.adopt_host_runtime(move || runnable.commit_creation()) {
            Ok(true) => {
                tracing::debug!(pid = std::process::id(), key = ?preparation.key(),
                    "Original production activation and preparation adoption exited");
                self.state
                    .lock()
                    .expect("original production captures")
                    .activated = true;
                true
            }
            Ok(false) => {
                tracing::debug!(pid = std::process::id(), key = ?preparation.key(),
                    "Original production activation returned before complete adoption exit");
                false
            }
            Err(original) => {
                tracing::debug!(pid = std::process::id(), key = ?preparation.key(),
                    "Original preparation refused its never-invoked activation capture");
                drop(original);
                false
            }
        }
    }
}

impl TaskHostRecord {
    pub(crate) fn retain_preparation_job(&mut self, job: TaskPreparationJob) {
        assert!(self.resources.pending_preparation_adoption.is_none());
        self.resources.pending_preparation_adoption = Some(job);
    }
}

impl TaskHostRecord {
    /// Only an actually committed Installed projection can expose this
    /// original activation capture to the physical executor.
    pub(crate) fn take_production_activation(
        &self,
    ) -> Option<super::context_host::task_driver::TaskDriverHostJob> {
        if !self.resources.port.current().installed() {
            return None;
        }
        let owner = self.resources.production.as_ref()?;
        let preparation = self.resources.preparation.as_ref()?;
        let mut state = owner.state.lock().expect("original production captures");
        if state.activation_queued || state.activated || state.runnable.is_none() {
            return None;
        }
        state.activation_queued = true;
        Some(
            super::context_host::task_driver::TaskDriverHostJob::Activate {
                key: preparation.key(),
                preparation: preparation.clone(),
                owner: owner.clone(),
                pending: self.pending.clone(),
            },
        )
    }
}

impl TaskHostRecord {
    /// Each fact is issued by its original finite lifecycle role. Report
    /// consumers remain independent of those positive physical observations.
    pub(crate) fn advance_production_facts(&mut self) -> Result<bool, TaskHostError> {
        use novarocks_control_runtime_core::{completion::Consumption, identity::ConsumerId};
        use novarocks_worker_protocol::context::{ContextInput, PhysicalFact};
        let Some(preparation) = self.resources.preparation.clone() else {
            return Ok(false);
        };
        let mut progressed = false;
        if let Some(roles) = &self.resources.completion {
            let before = roles.settlement_phase();
            // An uninvoked finisher's real holder may already have returned
            // from failed preparation. Observe that original body/capture
            // evidence; revocation alone does not supply an exit fact.
            roles.observe_unused_settlement();
            if before != roles.settlement_phase() {
                progressed = true;
                self.pending.pending();
            }
        }
        if self.resources.preparation_remainder.is_some() {
            if progressed {
                self.trace_preparation_failure_progress();
            }
            return self
                .resources
                .production
                .as_ref()
                .map_or(Ok(false), |owner| owner.publish_root_ownership_if_ready())
                .map(|root_progress| progressed || root_progress);
        }
        let body = preparation.body_and_captures_exited();
        let whole = preparation.publish_whole_exit()?;
        for (kind, fact, ready, report, retired) in [
            (
                TaskLifecycleRole::PreparationBodyExited,
                PhysicalFact::PreparationBodyExited,
                body,
                preparation.body_report(),
                preparation.body_report_retired(),
            ),
            (
                TaskLifecycleRole::PreparationExited,
                PhysicalFact::PreparationExited,
                whole,
                preparation.exit_report(),
                preparation.exit_report_retired(),
            ),
        ] {
            if retired {
                continue;
            }
            if ready {
                let observer = ConsumerId::new(2).expect("fixed observer");
                if report.consumption(observer)? == Consumption::Pending {
                    report.consume(observer)?;
                    progressed = true;
                }
                if !self.lifecycle_issued[kind as usize] {
                    let author = self.issue_lifecycle_author(kind)?;
                    author
                        .publish(ContextInput::Physical {
                            task: self.task,
                            fact,
                        })
                        .map_err(TaskHostError::CompletionAction)?;
                    progressed = true;
                }
            }
            if self
                .resources
                .lifecycle_inputs
                .get(&kind)
                .is_some_and(|input| {
                    input.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Retired
                })
            {
                let core = ConsumerId::new(1).expect("fixed Core");
                if report.consumption(core)? == Consumption::Pending {
                    report.consume(core)?;
                    progressed = true;
                }
            }
        }
        if let Some(owner) = &self.resources.production {
            if owner.publish_root_ownership_if_ready()? {
                progressed = true;
            }
        }
        self.observe_lifecycle_inputs()?;
        if whole {
            progressed |= self.collect_preparation_remainder()?;
        }
        if progressed {
            self.trace_preparation_failure_progress();
        }
        Ok(progressed)
    }
    /// Failure-path diagnostics only: bounded neutral original inventory.
    /// A log line grants no start, consumption, publication, or exit authority.
    fn trace_preparation_failure_progress(&self) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let Some(owner) = &self.resources.production else {
            return;
        };
        let (failed, installed, root_idle) = {
            let state = owner.state.lock().expect("original preparation diagnostic");
            (
                state.installation_failed,
                state.installation_observed,
                state.root_ownership_reported,
            )
        };
        if !failed {
            return;
        }
        let preparation = self.resources.preparation.as_ref();
        let roles = self.resources.completion.as_ref();
        let cut = self
            .resources
            .causal
            .cut_inventory(None, 1)
            .into_iter()
            .next();
        let cut_consumption = cut.map(|cut| self.resources.port.ingress().cut_consumption(cut));
        let input_roles = self
            .resources
            .lifecycle_inputs
            .iter()
            .map(|(kind, input)| (*kind, input.cut(), input.phase(), input.actual_exit_ready()))
            .collect::<Vec<_>>();
        tracing::debug!(pid = std::process::id(), task = ?self.task, actor = ?self.actor,
            installation_failed = failed, installation_observed = installed, root_ownership_reported = root_idle,
            preparation_work = ?preparation.map(|p| p.key()),
            body_captures_exited = preparation.is_some_and(|p| p.body_and_captures_exited()),
            body_report_retired = preparation.is_some_and(|p| p.body_report_retired()),
            whole_report_retired = preparation.is_some_and(|p| p.exit_report_retired()),
            remainder = self.resources.preparation_remainder.is_some(),
            finisher = ?roles.map(|r| r.phase(TaskLifecycleRole::Finisher)),
            wall = ?roles.map(|r| r.phase(TaskLifecycleRole::WallTime)),
            publisher = ?roles.map(|r| r.phase(TaskLifecycleRole::TerminalPublisher)),
            settlement = ?roles.map(|r| r.settlement_phase()),
            execution_calls_exited = roles.is_some_and(|r| r.execution_calls_exited()),
            completion_action = ?self.resources.completion_action.as_ref().map(|a| a.phase()),
            oldest_cut = ?cut, ?cut_consumption, host_cut_cursor = ?self.host_cut_cursor,
            retired_pairs = self.host_retired_pairs.len(), ?input_roles,
            "Original preparation failure made actual metadata progress");
    }
    pub(crate) fn pending_notice(&self) -> Arc<dyn TaskPendingNotice> {
        self.pending.clone()
    }
}

impl TaskHostRecord {
    /// One original host-owned cut per metadata slice. Task RPC cuts remain
    /// exclusively with their original R custody. No absent input proves a
    /// receipt: the causal owner saves actual commits before header retirement.
    pub(crate) fn advance_production_cut(
        &mut self,
        ledger: &novarocks_control_runtime::transaction::RuntimeLedger,
    ) -> Result<bool, TaskHostError> {
        use super::causal::CausalSourceRetirement;
        if let Some((&cut, _)) = self.host_retired_pairs.first_key_value() {
            let pair = &self.host_retired_pairs[&cut].pair;
            let ingress = self.resources.port.ingress();
            for holder in ingress
                .take_unused_owned_pair(pair)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .into_iter()
                .flatten()
            {
                holder.exit();
            }
            for position in ingress
                .owned_pair_inventory(pair)
                .map_err(|error| TaskHostError::Causal(error.into()))?
            {
                if !position
                    .custody
                    .try_retire()
                    .map_err(|error| TaskHostError::Causal(CausalError::Role(error)))?
                {
                    return Ok(false);
                }
            }
            let reaped_count = ledger.reap_scope_ready(self.scope.id(), 1)?;
            let probes = ingress
                .prepare_owned_pair_reap(pair)
                .map_err(|error| TaskHostError::Causal(error.into()))?;
            let [a, b] = probes.map(|probe| probe.observe_reaped());
            let (Ok(a), Ok(b)) = (a, b) else {
                if reaped_count > 0 {
                    self.pending.pending();
                }
                return Ok(false);
            };
            let mut detached = ingress
                .release_owned_pair(pair, [a, b])
                .map_err(|(error, _)| TaskHostError::Causal(error.into()))?;
            for storage in &mut detached {
                storage.release()?;
            }
            self.host_retired_pairs.remove(&cut);
            return Ok(true);
        }
        let cut = self
            .resources
            .causal
            .cut_inventory(self.host_cut_cursor, 1)
            .into_iter()
            .next()
            .or_else(|| {
                self.resources
                    .causal
                    .cut_inventory(None, 1)
                    .into_iter()
                    .next()
            });
        let Some(cut) = cut else {
            return Ok(false);
        };
        self.host_cut_cursor = Some(cut);
        if self.resources.causal.rpc_cut(cut)? {
            return Ok(false);
        }
        self.observe_lifecycle_inputs()?;
        self.observe_completion_action()?;
        if let Some((original_cut, original_work)) = self.production_settlement_cut {
            if original_cut == cut
                && self.resources.completion.as_ref().is_some_and(|roles| {
                    roles.settlement_work() == original_work && roles.settlement_call_exited()
                })
            {
                self.resources
                    .port
                    .ingress()
                    .observe_cut_exit(cut, true, false)
                    .map_err(|error| TaskHostError::Causal(error.into()))?;
            }
        }
        if !self.resources.causal.retire_host_headers(cut)? {
            return Ok(false);
        }
        match self
            .resources
            .causal
            .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        {
            Ok(retired) => {
                self.host_retired_pairs.insert(cut, retired);
                Ok(true)
            }
            Err(refusal) => match refusal.error {
                CausalError::Cut(
                    novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved,
                )
                | CausalError::Source(
                    super::source_positions::SourcePositionError::NotAvailable
                    | super::source_positions::SourcePositionError::SourceStillHeld,
                ) => {
                    let progress = self.resources.causal.advance_source_pair_retirement(
                        cut,
                        ledger,
                        &self.scope,
                    )?;
                    let Some(reaped) = progress.reaped else {
                        return Ok(progress.progressed);
                    };
                    match self.resources.causal.retire_after_exit(
                        cut, CausalSourceRetirement::Reaped(reaped),
                    ) {
                        Ok(retired) => {
                            self.host_retired_pairs.insert(cut, retired);
                            Ok(true)
                        }
                        Err(refusal) => match refusal.error {
                            CausalError::Driver(DriverError::RoleBusy)
                            | CausalError::Cut(
                                novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved
                                | novarocks_worker_protocol::proposal_summary::CutError::NotReady
                                | novarocks_worker_protocol::proposal_summary::CutError::Consuming,
                            ) => Ok(progress.progressed),
                            error => Err(TaskHostError::Causal(error)),
                        },
                    }
                }
                CausalError::Driver(DriverError::RoleBusy)
                | CausalError::Cut(
                    novarocks_worker_protocol::proposal_summary::CutError::NotReady
                    | novarocks_worker_protocol::proposal_summary::CutError::Consuming,
                ) => Ok(false),
                error => Err(TaskHostError::Causal(error)),
            },
        }
    }
}

impl ProductionPreparation {
    fn publish_root_ownership_if_ready(&self) -> Result<bool, TaskHostError> {
        use novarocks_worker_protocol::context::{ContextInput, PhysicalFact};
        let (author, fact) = {
            let mut state = self
                .state
                .lock()
                .expect("original Root transfer observation");
            if state.root_ownership_reported
                || (!state.installation_observed && !state.installation_failed)
            {
                return Ok(false);
            }
            let fact = match &state.root {
                Some(root) => {
                    let root = root.upgrade().ok_or(TaskHostError::InputNotReady)?;
                    if !root.producer_relinquished() {
                        return Ok(false);
                    }
                    PhysicalFact::RootTransferredToContext
                }
                None => PhysicalFact::RootIdle,
            };
            let Some(author) = state.root_idle_author.take() else {
                return Ok(false);
            };
            state.root_ownership_reported = true;
            (author, fact)
        };
        let task = author.task();
        author
            .publish(ContextInput::Physical { task, fact })
            .map_err(TaskHostError::CompletionAction)?;
        Ok(true)
    }
}
impl ProductionPreparation {
    pub(crate) fn close_root_issuance(
        &self,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
    ) -> bool {
        let mut state = self.state.lock().expect("original Root issuance fence");
        state.root_closed = true;
        state.root_close_reason.get_or_insert(reason);
        state.installation_observed || state.installation_failed
    }
}
impl TaskHostRecord {
    pub(crate) fn close_root_issuance(
        &self,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
    ) -> bool {
        self.resources
            .production
            .as_ref()
            .is_none_or(|owner| owner.close_root_issuance(reason))
    }
}

#[derive(Debug)]
pub(crate) enum TaskRootReadError {
    NotInstalled,
    NoRoot,
    OwnerNotReady,
    Channel(crate::root_result_channel::RootChannelError),
}
impl ProductionPreparation {
    fn begin_original_root_read(
        &self,
        read: &novarocks_execution_contract::root_result::RootResultRead,
    ) -> Result<crate::root_result_channel::RootChannelRead, TaskRootReadError> {
        let state = self.state.lock().expect("original admitted Root read");
        if !state.installation_observed {
            return Err(TaskRootReadError::OwnerNotReady);
        }
        let root = state
            .root
            .as_ref()
            .ok_or(TaskRootReadError::NoRoot)?
            .upgrade()
            .ok_or(TaskRootReadError::OwnerNotReady)?;
        root.begin_original_root_read(read)
    }
}
impl TaskHostRecord {
    /// Only the private Context R issuer calls this after original funding.
    /// An admitted read pins the actual channel; a numeric TaskId grants no
    /// Root, input, capacity or foreign execution capability by itself.
    pub(crate) fn begin_original_root_read(
        &self,
        read: &novarocks_execution_contract::root_result::RootResultRead,
    ) -> Result<crate::root_result_channel::RootChannelRead, TaskRootReadError> {
        if !self.resources.port.current().installed() {
            return Err(TaskRootReadError::NotInstalled);
        }
        self.resources
            .production
            .as_ref()
            .ok_or(TaskRootReadError::OwnerNotReady)?
            .begin_original_root_read(read)
    }
}

impl TaskHostRecord {
    /// Move only the installed Root token; executable Task ownership remains here.
    pub(crate) fn take_root_context_retention(
        &mut self,
    ) -> Result<Option<RootContextRetention>, TaskHostError> {
        Ok(self.resources.production.as_ref().and_then(|owner| {
            owner
                .state
                .lock()
                .expect("original installed Root handoff")
                .root_handoff
                .take()
        }))
    }
}

#[cfg(test)]
impl TaskHostRecord {
    /// Failure diagnostics only: original identities, phases and counters.
    /// No opaque input, error source, callback or protocol payload is Debugged.
    pub(crate) fn print_neutral_production_inventory(&self) {
        self.print_neutral_retirement_inventory();
        let runtime = self.resources.causal.runtime();
        eprintln!(
            "Task metadata task={:?} health={:?} host_cut_cursor={:?} retired_pairs={} input_inventory={:?}",
            self.task,
            self.health.failure(),
            self.host_cut_cursor,
            self.host_retired_pairs.len(),
            runtime.input_inventory(self.actor, None, 64)
        );
        if let Ok(batches) = runtime.committed_batches(self.actor, None, 64) {
            for (batch, input) in batches {
                if let Ok(effects) = runtime.effect_inventory(
                    self.actor,
                    input,
                    None,
                    super::actors::ContextActor::MAX_STEP_ACTIONS,
                ) {
                    for (effect, state) in effects {
                        let kind = runtime
                            .pending_effect_metadata(self.actor, input, effect.id)
                            .ok()
                            .flatten()
                            .map(|effect| match effect {
                                WorkerAction::Context(effect) => {
                                    format!("Context {:?}", std::mem::discriminant(&effect))
                                }
                                WorkerAction::Reply(_) => "Reply".to_owned(),
                                _ => "Other".to_owned(),
                            });
                        eprintln!(
                            "Task original batch={batch:?} input={input:?} action={:?} work={:?} state={state:?} kind={kind:?}",
                            effect.id, effect.work
                        );
                    }
                }
            }
        }
        self.resources.causal.print_neutral_inventory();
        if let Some(preparation) = &self.resources.preparation {
            eprintln!(
                "Task preparation body_caps={} body_report_retired={} whole_report_retired={} remainder={}",
                preparation.body_and_captures_exited(),
                preparation.body_report_retired(),
                preparation.exit_report_retired(),
                self.resources.preparation_remainder.is_some()
            );
        }
        if let Some(roles) = &self.resources.completion {
            eprintln!(
                "Task execution roles finisher={:?} wall={:?} publisher={:?} settlement={:?} actual_calls_exited={}",
                roles.phase(TaskLifecycleRole::Finisher),
                roles.phase(TaskLifecycleRole::WallTime),
                roles.phase(TaskLifecycleRole::TerminalPublisher),
                roles.settlement_phase(),
                roles.execution_calls_exited()
            );
        }
    }
}

impl TaskHostRecord {
    pub(crate) fn restore_root_context_retention(
        &mut self,
        root: RootContextRetention,
    ) -> Result<(), RootContextRetention> {
        if root.identity() != self.task
            || root.context() != self.context
            || !root.matches_scope(&self.scope)
        {
            return Err(root);
        }
        let Some(owner) = &self.resources.production else {
            return Err(root);
        };
        let mut state = owner.state.lock().expect("original refused Root handoff");
        if state.root_handoff.is_some() {
            return Err(root);
        }
        state.root_handoff = Some(root);
        Ok(())
    }
}

mod retirement;
pub(crate) use retirement::TaskDisposal;
mod root_retention;
pub(crate) use root_retention::{
    RetainedRootSeal, RetainedRootSealError, RetainedRootSealPreparation, RootContextRetention,
    RootRetentionCustody,
};

impl TaskHostRecord {
    pub(crate) fn issue_dynamic_filter_producer(
        &mut self,
    ) -> Result<crate::TaskDynamicFilterProducer, TaskHostError> {
        if self.scope.gates()?.failed {
            return Err(TaskHostError::OwnerFailed);
        }
        if self.resources.preparation.is_none() {
            return Err(TaskHostError::CompletionBeforeAccepted);
        }
        let kind = TaskLifecycleRole::DynamicFilters;
        if std::mem::replace(&mut self.lifecycle_issued[kind as usize], true) {
            return Err(TaskHostError::LifecycleAlreadyIssued(kind));
        }
        let role = reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            WorkKind::Cleanup,
            Some(self.core_generation),
        )?;
        let runtime = self.resources.causal.runtime().clone();
        let permit = match runtime.reserve_input(
            self.actor,
            self.task_source,
            novarocks_control_runtime_core::mailbox::InputClass::Ordinary,
        ) {
            Ok(permit) => permit,
            Err(error) => {
                dispose_unused_completion(role);
                return Err(TaskHostError::Driver(error));
            }
        };
        let input = match permit.into_role() {
            Ok(input) => input,
            Err((error, permit)) => {
                permit.revoke_unused().expect("unexposed RF input");
                dispose_unused_completion(role);
                return Err(TaskHostError::Driver(error));
            }
        };
        let (inbox, producer) = super::dynamic_filters::TaskDynamicFilterInbox::from_admitted(
            role,
            input,
            super::dynamic_filters::DynamicFilterBinding {
                task: self.task,
                scope: self.scope.clone(),
                generation: self.core_generation,
                runtime,
                actor: self.actor,
                pending: self.pending.clone(),
            },
        );
        self.resources.dynamic_filters = Some(inbox);
        Ok(producer)
    }
    pub(crate) fn dynamic_filters(
        &mut self,
    ) -> Option<&mut super::dynamic_filters::TaskDynamicFilterInbox> {
        self.resources.dynamic_filters.as_mut()
    }
}
