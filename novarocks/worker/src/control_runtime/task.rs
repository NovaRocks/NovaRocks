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

use super::{
    actors::{WorkerAction, WorkerActor},
    admitted_role::{dispose_unused_completion, reserve_reported_role},
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
impl From<RuntimeError> for TaskHostError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl std::fmt::Display for TaskHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SourceSlots(error) => write!(f, "source slot admission failed: {error:?}"),
            Self::SourceStorage(error) => {
                write!(f, "original source storage transfer failed: {error:?}")
            }
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
    pub open_summary: Option<TaskOpenSummaryInbox>,
    pub causal: TaskCausalInbox,
}
impl TaskHostResources {
    /// The custodian has disposed actual payloads and retired each independent
    /// report/input position. Transfer read authority only from that inventory;
    /// a Task terminal or a stopped execution borrow gate is insufficient.
    pub(crate) fn seal_read_observation(&self) -> Result<Arc<TaskReadSnapshot>, TaskReadError> {
        use novarocks_control_runtime::exit_role::ExitRolePhase;
        if self
            .source_partials
            .iter()
            .any(|partial| !partial.is_empty())
            || self.pending_preparation_adoption.is_some()
            || !self.causal.is_empty()
            || self
                .statistics
                .as_ref()
                .is_some_and(|statistics| !statistics.is_retired())
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
}
impl TaskHostRecord {
    /// Electing the Task and paying its active/loading storage happened in
    /// the Core step. This takes the same guard once; it reserves no new quota.
    pub(crate) fn adopt_loading(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        binding: TaskHostBinding,
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
                    })),
                });
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
                let partial = SourceSlotIssuePartial::from_admissions(&scope, sources);
                return Err(TaskAdoptionRefusal {
                    error: TaskHostError::InvalidGeometry,
                    unpublished: Some(Box::new(UnpublishedTask {
                        task,
                        lifetime,
                        active,
                        source_partials: vec![partial],
                    })),
                });
            }
        };
        let port = Arc::new(TaskRuntimePort::from_admitted(
            task,
            &scope,
            ingress.clone(),
            binding.pending.clone(),
            binding.health.clone(),
        ));
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
                open_summary: None,
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
        })
    }
    pub(crate) fn task_id(&self) -> TaskIdentity {
        self.task
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
                        loader.work.id(),
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
            Some(events) => events.observe_exit_report(&mut self.resources.causal),
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
            .checked_sub(2)
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
    fn issue_lifecycle_author(
        &mut self,
        kind: TaskLifecycleRole,
    ) -> Result<InputAuthor, TaskHostError> {
        if self.resources.preparation.is_none() {
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
        let sources = match self.reserve_source_slots::<2>(false) {
            Ok(sources) => sources,
            Err(error) => {
                dispose_unused_completion(role);
                return Err(error);
            }
        };
        let (cut, sender) = match self.resources.causal.admit_input(
            role.work.id(),
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
            retired &= input
                .observe_consumers(&mut self.resources.causal)
                .map_err(TaskHostError::CompletionAction)?;
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
            role.work.id(),
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
        action
            .observe_consumers(&mut self.resources.causal)
            .map_err(TaskHostError::CompletionAction)
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
    /// Host ownership transfer only. The receiving cleanup job must dispose
    /// opaque sources/captures and observe real body/children/report exits
    /// before releasing these bundles; Core terminal is no substitute.
    pub(crate) fn into_resources(self) -> TaskHostResources {
        self.resources
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
