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

//! A Task's actual admitted causal positions. Registration seals its prefix
//! under the proposal handoff lock; this host custodian moves only neutral
//! inputs to the common runtime. It never runs callbacks or source disposal.

use super::actors::{ContextMessage, WorkerActor, WorkerMessage};
use super::causal_input::CausalInputCustody;
use super::failure::ScopePublicationsDrained;
use super::loading::LoaderCustody;
use super::loading_events::LoadingAuthorReturn;
use super::source_positions::{ReapedPosition, SourcePositionError};
use crate::task_completion_roles::AdmittedCompletionRole;
use crate::{
    settlement::RegistrationNotice,
    task_causal_input::CausalInputRole,
    task_port::TaskPendingNotice,
    task_proposal_ingress::{
        CausalEventInput, CausalEventKind, CutSourceAdmission, IngressError, RequestMetadata,
        RetiredCut, TaskProposalIngress,
    },
};
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, InputPhase};
use novarocks_control_runtime::lease::WorkLease;
use novarocks_control_runtime::{
    exit_role::{ExitAuthority, ExitRole, ExitRolePhase},
    reported_work::{ReportedWork, ReportedWorkBindingError},
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, Generation, InputId, WorkId},
    mailbox::InputClass,
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::TaskIdentity;
use novarocks_execution_contract::{
    TaskFailurePhase,
    failure::{FailureClass, FailureFact},
};
use novarocks_worker_protocol::{
    context::{ContextInput, TaskCloseIntent},
    proposal_summary::{CutError, CutId, ProposalSummary, SourceId},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CausalError {
    Cut(CutError),
    Driver(DriverError),
    UnknownRole,
    AlreadyAdmitted,
    BeforeAccepted,
    NotDelivered,
    InputObservationMissing,
    IngressGeometry,
    WrongTask,
    Source(SourcePositionError),
    UnknownSource,
    InvalidRequestClass,
    RequestAdmissionClosed,
    WrongAuthor,
    Role(novarocks_control_runtime::exit_role::ExitRoleError),
}
impl From<CutError> for CausalError {
    fn from(error: CutError) -> Self {
        Self::Cut(error)
    }
}
impl From<DriverError> for CausalError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<IngressError> for CausalError {
    fn from(error: IngressError) -> Self {
        match error {
            IngressError::Geometry => Self::IngressGeometry,
            IngressError::WrongTask => Self::WrongTask,
            IngressError::Cut(error) => Self::Cut(error),
            IngressError::Source(error) => Self::Source(error),
        }
    }
}
pub(crate) struct CausalAdmissionRefusal {
    pub error: CausalError,
    pub admission: CutSourceAdmission,
}
pub(crate) enum CausalSourceRetirement {
    Unclaimed,
    Reaped([ReapedPosition; 2]),
}
pub(crate) struct CausalFailedProgress {
    pub units: usize,
    pub progressed: bool,
    pub inputs_retired: bool,
    pub rejected: Option<(Option<WorkerMessage>, WorkerMessage)>,
}
pub(crate) struct CausalSourceProgress {
    pub progressed: bool,
    pub reaped: Option<[ReapedPosition; 2]>,
}
pub(crate) struct CausalRetirementRefusal {
    pub error: CausalError,
    pub retirement: CausalSourceRetirement,
}
/// Original author metadata alone grants no exit authority. Only the unique
/// host-return receipt can submit it as a positive capture-exit observation.
pub(crate) struct CausalAuthorBinding {
    ingress: Weak<TaskProposalIngress>,
    task: TaskIdentity,
    cut: CutId,
    work: WorkId,
}
impl CausalAuthorBinding {
    pub(crate) fn work_id(&self) -> WorkId {
        self.work
    }
    pub(crate) fn cut(&self) -> CutId {
        self.cut
    }
}
impl std::fmt::Debug for CausalAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CausalAdmissionRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for CausalRetirementRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CausalRetirementRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[must_use = "The original causal author must send or explicitly retire its unused role"]
pub(crate) struct TaskCausalSender {
    ingress: Weak<TaskProposalIngress>,
    cut: CutId,
    pending: Arc<dyn TaskPendingNotice>,
    task: TaskIdentity,
    health: Arc<ExecutionHealth>,
}
impl TaskCausalSender {
    fn ingress(&self) -> Option<Arc<TaskProposalIngress>> {
        let ingress = self.ingress.upgrade();
        if ingress.is_none() {
            self.health.report_unresolved(
                ExecutionExitOrigin::Task {
                    identity: self.task,
                    phase: TaskFailurePhase::Execution,
                },
                FailureFact::new(
                    FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                ),
            );
        }
        ingress
    }
    pub(crate) fn send(self) {
        let Some(ingress) = self.ingress() else {
            return;
        };
        ingress
            .register_causal_event(self.cut)
            .expect("the original admitted role seals its causal prefix once");
        self.pending.pending();
    }
    pub(super) fn send_close(
        self,
        generation: Generation,
        intent: TaskCloseIntent,
    ) -> Result<(), CausalError> {
        let ingress = self.ingress().ok_or(CausalError::UnknownRole)?;
        ingress.register_close_event(self.cut, generation, intent)?;
        self.pending.pending();
        Ok(())
    }
    pub(crate) fn unused(self) {
        let Some(ingress) = self.ingress() else {
            return;
        };
        ingress
            .revoke_event(self.cut)
            .expect("the original unused causal author is revoked once");
        self.pending.pending();
    }
    pub(crate) fn send_input(self, input: ContextInput) -> Result<(), ContextInput> {
        let Some(ingress) = self.ingress() else {
            return Err(input);
        };
        match ingress.register_input_event(self.cut, input) {
            Ok(()) => {
                self.pending.pending();
                Ok(())
            }
            Err((_error, input)) => Err(input),
        }
    }
}
/// One concrete observer of the same whole R author. It owns no second
/// Work, authority or report; the validated original ExitRole retains them.
struct RpcAuthorObserver {
    binding: CausalAuthorBinding,
    role: ExitRole,
}
#[must_use = "The original RPC sender must return or remain unresolved"]
pub(crate) struct RpcCausalSender {
    sender: TaskCausalSender,
    original: Arc<RpcAuthorObserver>,
}
#[must_use = "Only this original sender return can acknowledge its captures"]
pub(crate) struct RpcAuthorReturned {
    original: Arc<RpcAuthorObserver>,
}
impl std::fmt::Debug for RpcAuthorReturned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcAuthorReturned")
            .field("task", &self.original.binding.task)
            .field("cut", &self.original.binding.cut)
            .finish_non_exhaustive()
    }
}
pub(crate) struct RpcInputRefusal {
    pub input: ContextInput,
    pub returned: RpcAuthorReturned,
}
impl std::fmt::Debug for RpcInputRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcInputRefusal")
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}
impl RpcCausalSender {
    /// This is only the real synchronous sender/capture return. The R author
    /// authority, getter and external delivery remain with the original owner.
    pub(crate) fn send_input(
        self,
        input: ContextInput,
    ) -> Result<RpcAuthorReturned, RpcInputRefusal> {
        let Self { sender, original } = self;
        let returned = RpcAuthorReturned { original };
        match sender.send_input(input) {
            Ok(()) => Ok(returned),
            Err(input) => Err(RpcInputRefusal { input, returned }),
        }
    }
    pub(crate) fn withdraw(self) -> RpcAuthorReturned {
        let Self { sender, original } = self;
        sender.unused();
        RpcAuthorReturned { original }
    }
}
pub(crate) struct RpcAuthorAdmission {
    pub cut: CutId,
    pub role: ExitRole,
    pub authority: ExitAuthority,
    pub sender: RpcCausalSender,
}
#[derive(Debug)]
pub(crate) enum RpcAuthorAdmissionError {
    Binding(ReportedWorkBindingError),
    Causal(CausalError),
}
/// Every refusal retains the same complete original Work/capacity/report.
/// After cut admission, the Source2 already belongs to that actual cut.
#[must_use = "Retain the original R and Source2 or its actually admitted partial cut"]
pub(crate) enum RpcAuthorAdmissionRefusal {
    BeforeCut {
        error: RpcAuthorAdmissionError,
        admitted: AdmittedCompletionRole,
        admission: CutSourceAdmission,
    },
    BoundCut {
        error: CausalError,
        admitted: AdmittedCompletionRole,
        cut: CutId,
        sender: TaskCausalSender,
    },
}
impl std::fmt::Debug for RpcAuthorAdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeCut { error, .. } => f
                .debug_struct("RpcAuthorBeforeCut")
                .field("error", error)
                .finish_non_exhaustive(),
            Self::BoundCut { error, cut, .. } => f
                .debug_struct("RpcAuthorBoundCut")
                .field("error", error)
                .field("cut", cut)
                .finish_non_exhaustive(),
        }
    }
}

impl RegistrationNotice for TaskCausalSender {
    fn bind_work(&mut self, work: WorkId) {
        let Some(ingress) = self.ingress() else {
            return;
        };
        ingress
            .bind_event_work(self.cut, work)
            .expect("one original Work binding for the prepaid settlement cut");
    }
    fn registered(self: Box<Self>) {
        (*self).send();
    }
    fn unused(self: Box<Self>) {
        (*self).unused();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CausalDelivery {
    pub cut: CutId,
    pub prefix: Option<InputId>,
    pub event: InputId,
}
struct Delivered {
    // None means no input transferred, not an input guessed absent later.
    proof: Option<InputId>,
    // Only this original prefix can prove consumption of these exact sources.
    // The event receipt is never substituted for a source-bearing summary.
    prefix: Option<InputId>,
    sources: [Option<SourceId>; 2],
    source_consumption: Consumption,
    // The event is a separate Core step. A committed prefix alone cannot
    // consume the lifecycle author's own completion report.
    event: Option<InputId>,
    event_consumption: Consumption,
    // Partial fill failure returns the precise original neutral payloads.
    // They remain in this paid role until explicit failed-owner takeover.
    rejected: Option<(Option<WorkerMessage>, WorkerMessage)>,
}
impl Delivered {
    fn sources(summary: &ProposalSummary) -> [Option<SourceId>; 2] {
        let mut sources = summary.candidate_sources();
        let result = [sources.next(), sources.next()];
        assert!(
            sources.next().is_none(),
            "one prefix has at most two source candidates"
        );
        result
    }
    fn names_source(&self, source: SourceId) -> bool {
        self.sources.contains(&Some(source))
    }
}
struct Role {
    host_consumer_consumed: bool,
    inputs: CausalInputRole<WorkerActor>,
    delivered: Option<Delivered>,
    // Reference the original custodian, not a copied consumption flag. Its
    // report must finish before this role can discard the exact event receipt.
    input_author: Option<CausalInputCustody>,
    rpc_author: Option<Arc<RpcAuthorObserver>>,
    loader_exit: Option<LoaderCustody>,
}

pub(crate) struct TaskCausalInbox {
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    source: ActorId,
    task: TaskIdentity,
    // Protocol close generation is independent of the runtime failure epoch.
    generation: Generation,
    ingress: Arc<TaskProposalIngress>,
    roles: BTreeMap<CutId, Role>,
    // Exact transferred predecessor awaiting its actual Core receipts.
    // This is one address into the original role inventory, not another cut.
    observing_prefix: Option<CutId>,
    health: Arc<ExecutionHealth>,
}
/// The caller has already paid for this Context-local author and Source pair
/// from the original RPC position. Neither this metadata nor admission mints
/// a Work, a request token, or a replacement source reservation.
pub(crate) struct TaskCausalRequest<'a> {
    pub author: &'a WorkLease,
    pub request: RequestMetadata,
    pub class: InputClass,
}
impl TaskCausalInbox {
    pub(crate) fn new(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        source: ActorId,
        task: TaskIdentity,
        generation: Generation,
        ingress: Arc<TaskProposalIngress>,
        health: Arc<ExecutionHealth>,
    ) -> Self {
        Self {
            runtime,
            actor,
            source,
            task,
            generation,
            ingress,
            roles: BTreeMap::new(),
            observing_prefix: None,
            health,
        }
    }
    pub(super) fn runtime(&self) -> &ActorRuntime<WorkerActor> {
        &self.runtime
    }
    /// The trusted host issuer checks the same original sender and event Work
    /// before it attaches the admitted lifecycle author's unique authority.
    pub(crate) fn check_input_author(
        &self,
        task: TaskIdentity,
        cut: CutId,
        work: WorkId,
        sender: &TaskCausalSender,
    ) -> Result<(), CausalError> {
        if self.task != task
            || sender.task != task
            || sender.cut != cut
            || !Weak::ptr_eq(&sender.ingress, &Arc::downgrade(&self.ingress))
        {
            return Err(CausalError::WrongTask);
        }
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        if role.delivered.is_some()
            || role.input_author.is_some()
            || role.rpc_author.is_some()
            || role.loader_exit.is_some()
        {
            return Err(CausalError::AlreadyAdmitted);
        }
        self.ingress.validate_input_author(cut, task, work)?;
        Ok(())
    }
    pub(crate) fn author_binding(
        &self,
        task: TaskIdentity,
        cut: CutId,
        work: WorkId,
        sender: &TaskCausalSender,
    ) -> Result<CausalAuthorBinding, CausalError> {
        self.check_input_author(task, cut, work, sender)?;
        Ok(CausalAuthorBinding {
            ingress: Arc::downgrade(&self.ingress),
            task,
            cut,
            work,
        })
    }
    /// Consume the unique host-return receipt, including after this original
    /// event's runtime input has retired. Neither numeric IDs nor Core commit
    /// can substitute for the original owner and its actual return.
    pub(crate) fn observe_author_return(
        &self,
        receipt: LoadingAuthorReturn,
    ) -> Result<(), (CausalError, LoadingAuthorReturn)> {
        let checked = (|| {
            let binding = receipt.binding();
            self.validate_author_binding(binding)?;
            self.ingress.observe_cut_exit(binding.cut, true, false)?;
            Ok(())
        })();
        if let Err(error) = checked {
            return Err((error, receipt));
        }
        receipt.disarm();
        Ok(())
    }
    fn validate_author_binding(&self, binding: &CausalAuthorBinding) -> Result<(), CausalError> {
        if binding.task != self.task
            || !Weak::ptr_eq(&binding.ingress, &Arc::downgrade(&self.ingress))
        {
            return Err(CausalError::WrongTask);
        }
        if !self.roles.contains_key(&binding.cut) {
            return Err(CausalError::UnknownRole);
        }
        self.ingress
            .validate_bound_input_author(binding.cut, binding.task, binding.work)?;
        Ok(())
    }
    pub(crate) fn bound_author_report_consumed(
        &mut self,
        binding: &CausalAuthorBinding,
    ) -> Result<bool, CausalError> {
        self.validate_author_binding(binding)?;
        self.action_report_consumed(binding.task, binding.cut)
    }
    /// The caller owns this role's original prepaid storage. Admission happens
    /// before attaching its callback. A failed pure reservation rolls back only
    /// these actually unused input positions, outside the proposal lock.
    pub(crate) fn admit_settlement(
        &mut self,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, Box<dyn RegistrationNotice>), CausalAdmissionRefusal> {
        let (cut, notice) =
            self.admit(CausalEventKind::Settlement, consumers, pending, admission)?;
        Ok((cut, Box::new(notice)))
    }
    pub(crate) fn admit_close(
        &mut self,
        generation: Generation,
        intent: TaskCloseIntent,
        work: &WorkLease,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        self.admit_original_author(
            CausalEventKind::ApplyClose(generation, intent),
            work,
            consumers,
            pending,
            admission,
        )
    }
    /// One finite original lifecycle author. Its already admitted Work is the
    /// actual same-scope/ledger witness; closing external business admission
    /// cannot revoke this producer's original cleanup/report obligations.
    pub(crate) fn admit_input(
        &mut self,
        work: &WorkLease,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        self.admit_original_author(CausalEventKind::Input, work, consumers, pending, admission)
    }
    fn admit_original_author(
        &mut self,
        kind: CausalEventKind,
        work: &WorkLease,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let inputs = match CausalInputRole::reserve_admitted(
            &self.runtime,
            self.actor,
            self.source,
            InputClass::Lifecycle,
            work,
        ) {
            Ok(inputs) => inputs,
            Err(error) => {
                return Err(CausalAdmissionRefusal {
                    error: error.into(),
                    admission,
                });
            }
        };
        let (cut, mut sender) =
            self.admit_with_inputs(kind, consumers, pending, admission, inputs)?;
        sender.bind_work(work.id());
        Ok((cut, sender))
    }
    /// A Task RPC seals the same proposal prefix as a lifecycle fact, but its
    /// event retains its own request class, operation and deadline. Only that
    /// actual event input supplies the Core request token.
    pub(crate) fn admit_request(
        &mut self,
        request: TaskCausalRequest<'_>,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        if !matches!(request.class, InputClass::Ordinary | InputClass::Control) {
            return Err(CausalAdmissionRefusal {
                error: CausalError::InvalidRequestClass,
                admission,
            });
        }
        let inputs = match CausalInputRole::reserve_admitted(
            &self.runtime,
            self.actor,
            self.source,
            request.class,
            request.author,
        ) {
            Ok(inputs) => inputs,
            Err(error) => {
                return Err(CausalAdmissionRefusal {
                    error: error.into(),
                    admission,
                });
            }
        };
        let (cut, mut sender) = self.admit_with_inputs(
            CausalEventKind::Request(request.request),
            consumers,
            pending,
            admission,
            inputs,
        )?;
        sender.bind_work(request.author.id());
        Ok((cut, sender))
    }
    /// One trusted whole-R entry: validate the original triple, borrow that
    /// same Work for actual actor/scope admission, then consume the unchanged
    /// carrier into ExitRole. Callers cannot swap a role after author checking.
    pub(crate) fn admit_rpc_author(
        &mut self,
        admitted: AdmittedCompletionRole,
        request: RequestMetadata,
        class: InputClass,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<RpcAuthorAdmission, RpcAuthorAdmissionRefusal> {
        let reported =
            match ReportedWork::try_new(admitted.work, admitted.capacity, admitted.report) {
                Ok(reported) => reported,
                Err(refusal) => {
                    return Err(RpcAuthorAdmissionRefusal::BeforeCut {
                        error: RpcAuthorAdmissionError::Binding(refusal.error),
                        admitted: AdmittedCompletionRole {
                            work: refusal.work,
                            capacity: refusal.capacity,
                            report: refusal.report,
                        },
                        admission,
                    });
                }
            };
        let admitted = self.admit_request(
            TaskCausalRequest {
                author: reported.work(),
                request,
                class,
            },
            consumers,
            pending,
            admission,
        );
        let (cut, sender) = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => {
                let (work, capacity, report) = reported.into_parts();
                return Err(RpcAuthorAdmissionRefusal::BeforeCut {
                    error: RpcAuthorAdmissionError::Causal(refusal.error),
                    admitted: AdmittedCompletionRole {
                        work,
                        capacity,
                        report,
                    },
                    admission: refusal.admission,
                });
            }
        };
        let binding = match self.author_binding(self.task, cut, reported.work().id(), &sender) {
            Ok(binding) => binding,
            Err(error) => {
                let (work, capacity, report) = reported.into_parts();
                return Err(RpcAuthorAdmissionRefusal::BoundCut {
                    error,
                    admitted: AdmittedCompletionRole {
                        work,
                        capacity,
                        report,
                    },
                    cut,
                    sender,
                });
            }
        };
        let (role, authority) = ExitRole::from_admitted(reported);
        let original = Arc::new(RpcAuthorObserver {
            binding,
            role: role.clone(),
        });
        self.roles
            .get_mut(&cut)
            .expect("same admitted RPC cut")
            .rpc_author = Some(original.clone());
        Ok(RpcAuthorAdmission {
            cut,
            role,
            authority,
            sender: RpcCausalSender { sender, original },
        })
    }
    /// A unique return from this exact bound sender observes only the cut's
    /// capture leg. It neither fills/consumes the R report nor signs R exit.
    pub(crate) fn observe_rpc_author_return(
        &self,
        returned: RpcAuthorReturned,
    ) -> Result<(), (CausalError, RpcAuthorReturned)> {
        let checked = (|| {
            let original = &returned.original;
            self.validate_author_binding(&original.binding)?;
            if self
                .roles
                .get(&original.binding.cut)
                .and_then(|role| role.rpc_author.as_ref())
                .is_none_or(|bound| !Arc::ptr_eq(bound, original))
            {
                return Err(CausalError::WrongAuthor);
            }
            self.ingress
                .observe_cut_exit(original.binding.cut, true, false)?;
            Ok(())
        })();
        checked.map_err(|error| (error, returned))
    }
    /// Return only the exact fill receipt of this original RPC cut. Absence
    /// means not fully delivered; it grants no token or consumption proof.
    pub(crate) fn rpc_delivery(&self, cut: CutId) -> Result<Option<CausalDelivery>, CausalError> {
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        if role.rpc_author.is_none() {
            return Err(CausalError::WrongAuthor);
        }
        Ok(role.delivered.as_ref().and_then(|delivered| {
            delivered.event.map(|event| CausalDelivery {
                cut,
                prefix: delivered.prefix,
                event,
            })
        }))
    }
    fn admit(
        &mut self,
        kind: CausalEventKind,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let inputs = match CausalInputRole::reserve(
            &self.runtime,
            self.actor,
            self.source,
            InputClass::Lifecycle,
        ) {
            Ok(inputs) => inputs,
            Err(error) => {
                return Err(CausalAdmissionRefusal {
                    error: error.into(),
                    admission,
                });
            }
        };
        self.admit_with_inputs(kind, consumers, pending, admission, inputs)
    }
    fn admit_with_inputs(
        &mut self,
        kind: CausalEventKind,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
        mut inputs: CausalInputRole<WorkerActor>,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let cut = match self
            .ingress
            .reserve_event_cut(self.task, kind, consumers, admission)
        {
            Ok(cut) => cut,
            Err((error, admission)) => {
                inputs
                    .close_after_exit()
                    .expect("unbound causal input holders have really exited");
                return Err(CausalAdmissionRefusal {
                    error: error.into(),
                    admission,
                });
            }
        };
        assert!(
            self.roles
                .insert(
                    cut,
                    Role {
                        host_consumer_consumed: false,
                        inputs,
                        delivered: None,
                        input_author: None,
                        rpc_author: None,
                        loader_exit: None,
                    }
                )
                .is_none()
        );
        Ok((
            cut,
            TaskCausalSender {
                ingress: Arc::downgrade(&self.ingress),
                cut,
                pending,
                task: self.task,
                health: self.health.clone(),
            },
        ))
    }
    pub(crate) fn bind_input_author(
        &mut self,
        action: CausalInputCustody,
    ) -> Result<(), CausalError> {
        if action.task() != self.task {
            return Err(CausalError::WrongTask);
        }
        let role = self
            .roles
            .get_mut(&action.cut())
            .ok_or(CausalError::UnknownRole)?;
        if role.input_author.is_some()
            || role.rpc_author.is_some()
            || role.loader_exit.is_some()
            || role.delivered.is_some()
        {
            return Err(CausalError::AlreadyAdmitted);
        }
        role.input_author = Some(action);
        Ok(())
    }
    /// The exit event receipt remains with the same actual loader until its
    /// independent report consumers retire. The Loaded author has no such
    /// gate: waiting for input transfer there would cycle with Accepted.
    pub(crate) fn hold_loader_exit(
        &mut self,
        cut: CutId,
        loader: LoaderCustody,
    ) -> Result<(), CausalError> {
        if loader.identity() != self.task {
            return Err(CausalError::WrongTask);
        }
        self.ingress
            .validate_input_author(cut, self.task, loader.work_id())?;
        let role = self.roles.get_mut(&cut).ok_or(CausalError::UnknownRole)?;
        if role.input_author.is_some()
            || role.rpc_author.is_some()
            || role.loader_exit.is_some()
            || role.delivered.is_some()
        {
            return Err(CausalError::AlreadyAdmitted);
        }
        role.loader_exit = Some(loader);
        Ok(())
    }
    /// This custodian is exclusive; all roles for this Task use the same
    /// source FIFO. A different Task has its own source and can keep advancing.
    pub(crate) fn deliver_next(&mut self) -> Result<Option<CausalDelivery>, CausalError> {
        if let Some(previous) = self.observing_prefix {
            if self.observe_core(previous)? == Consumption::Pending
                || !self.event_core_consumed(previous)?
            {
                return Ok(None);
            }
            self.observing_prefix = None;
        }
        let input = match self.ingress.take_next_event(false) {
            Ok(input) => input,
            Err(IngressError::Cut(CutError::NotReady | CutError::Consuming)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let cut = input.prefix.cut.expect("causal role owns one real cut");
        let role = self
            .roles
            .get_mut(&cut)
            .expect("original admitted causal input positions");
        assert!(role.delivered.is_none(), "one delivery per admitted role");
        let sources = Delivered::sources(&input.prefix.summary);
        let prefix = if input.prefix.summary.is_empty() {
            None
        } else {
            Some(WorkerMessage::Context(ContextMessage::Command {
                work: input.work,
                request: None,
                command: ContextInput::Summary {
                    task: self.task,
                    generation: self.generation,
                    cut: Some(cut),
                    summary: input.prefix.summary,
                },
            }))
        };
        let next_generation = match &input.event {
            ContextInput::ApplyTaskClose {
                generation,
                intent: TaskCloseIntent::Abort(_),
                ..
            } => Some(*generation),
            _ => None,
        };
        let event = WorkerMessage::Context(ContextMessage::Command {
            work: input.work,
            request: input.request,
            command: input.event,
        });
        match role.inputs.fill_with_receipt(prefix, event) {
            Ok((prefix, event)) => {
                if let Some(generation) = next_generation {
                    self.generation = generation;
                }
                role.delivered = Some(Delivered {
                    proof: prefix.or(Some(event)),
                    prefix,
                    sources,
                    source_consumption: Consumption::Pending,
                    event: Some(event),
                    event_consumption: Consumption::Pending,
                    rejected: None,
                });
                self.observing_prefix = Some(cut);
                Ok(Some(CausalDelivery { cut, prefix, event }))
            }
            Err(failure) => {
                role.delivered = Some(Delivered {
                    proof: failure.predecessor_input,
                    prefix: failure.predecessor_input,
                    sources,
                    source_consumption: Consumption::Pending,
                    event: None,
                    event_consumption: Consumption::Pending,
                    rejected: Some((failure.predecessor, failure.event)),
                });
                Err(failure.error.into())
            }
        }
    }
    /// Observe the actual input before retiring its record. Neither numeric
    /// InputId order, a published hint, nor absence from a queue proves Core
    /// consumption. An empty prefix is covered by its actual event input.
    pub(crate) fn observe_core(&mut self, cut: CutId) -> Result<Consumption, CausalError> {
        // Save the original event's receipt before its runtime input can be
        // retired. Prefix consumption and Source pair reuse are independent.
        let delivered = self
            .roles
            .get_mut(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_mut()
            .ok_or(CausalError::NotDelivered)?;
        if delivered.event_consumption == Consumption::Pending
            && let Some(event) = delivered.event
            && self
                .runtime
                .input_custody(self.actor, event)?
                .ok_or(CausalError::InputObservationMissing)?
                .phase
                == InputPhase::Committed
        {
            delivered.event_consumption = Consumption::Consumed;
        }
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        if delivered.sources.iter().any(Option::is_some)
            && delivered.source_consumption == Consumption::Pending
            && let Some(prefix) = delivered.prefix
        {
            let phase = self
                .runtime
                .input_custody(self.actor, prefix)?
                .ok_or(CausalError::InputObservationMissing)?
                .phase;
            if phase == InputPhase::Committed {
                self.roles
                    .get_mut(&cut)
                    .unwrap()
                    .delivered
                    .as_mut()
                    .unwrap()
                    .source_consumption = Consumption::Consumed;
            }
        }
        let consumption = self.ingress.cut_consumption(cut).ok_or(CutError::Unknown)?;
        if consumption != Consumption::Pending {
            return Ok(consumption);
        }
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        let phase = match delivered.proof {
            Some(input) => Some(
                self.runtime
                    .input_custody(self.actor, input)?
                    .ok_or(CausalError::InputObservationMissing)?
                    .phase,
            ),
            None => None,
        };
        if phase == Some(InputPhase::Committed) {
            self.ingress.summary_consumed(Some(cut))?;
            return Ok(Consumption::Consumed);
        }
        Ok(Consumption::Pending)
    }
    /// Exact identities from the original neutral summary and fill receipt.
    /// An untransferred partial-fill prefix is None, never the event's ID.
    pub(crate) fn source_prefix_input(
        &self,
        cut: CutId,
        source: SourceId,
    ) -> Result<Option<InputId>, CausalError> {
        let delivered = self.source_delivery(cut, source)?;
        Ok(delivered.prefix)
    }
    /// This proof is recorded only by a real prefix commit observation or by
    /// the validated failure-publication drain. It grants no source exit.
    pub(crate) fn source_core_consumption(
        &self,
        cut: CutId,
        source: SourceId,
    ) -> Result<Consumption, CausalError> {
        let delivered = self.source_delivery(cut, source)?;
        if delivered.source_consumption == Consumption::Pending
            && let Some(prefix) = delivered.prefix
        {
            self.runtime
                .input_custody(self.actor, prefix)?
                .ok_or(CausalError::InputObservationMissing)?;
        }
        Ok(delivered.source_consumption)
    }
    fn source_delivery(&self, cut: CutId, source: SourceId) -> Result<&Delivered, CausalError> {
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        if !delivered.names_source(source) {
            return Err(CausalError::UnknownSource);
        }
        Ok(delivered)
    }
    /// This observes the original event, independently of the preceding
    /// summary's consumption. Failed-owner abandonment requires its separate
    /// actual publication-drain authority; absence is never a consumed input.
    pub(crate) fn event_core_consumed(&self, cut: CutId) -> Result<bool, CausalError> {
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        if delivered.event_consumption == Consumption::Consumed {
            return Ok(true);
        }
        let Some(input) = delivered.event else {
            return Ok(false);
        };
        Ok(self
            .runtime
            .input_custody(self.actor, input)?
            .ok_or(CausalError::InputObservationMissing)?
            .phase
            == InputPhase::Committed)
    }
    /// The original action can acknowledge its Core report from its own
    /// committed event, or from its own unused revocation after actual exit.
    /// The pair originally supplied by this cut may now be a live open pair;
    /// waiting for that pair here would create a capacity-one cycle.
    fn action_report_consumed(
        &mut self,
        task: TaskIdentity,
        cut: CutId,
    ) -> Result<bool, CausalError> {
        if self.task != task {
            return Err(CausalError::WrongTask);
        }
        if !self.roles.contains_key(&cut) {
            return Err(CausalError::UnknownRole);
        }
        if self.ingress.event_unused(cut)? {
            return Ok(true);
        }
        if self.roles[&cut].delivered.is_none() {
            return Ok(false);
        }
        self.observe_core(cut)?;
        self.event_core_consumed(cut)
    }
    pub(crate) fn abandon_core(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
        cut: CutId,
    ) -> Result<Consumption, CausalError> {
        drained.validate(&self.runtime, self.actor)?;
        self.observe_core(cut)?;
        let delivered = self
            .roles
            .get_mut(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_mut()
            .ok_or(CausalError::NotDelivered)?;
        // Save each exact input independently before its header can retire.
        // A committed predecessor never proves that the event was consumed.
        for (input, receipt) in [
            (delivered.prefix, &mut delivered.source_consumption),
            (delivered.event, &mut delivered.event_consumption),
        ] {
            if *receipt != Consumption::Pending {
                continue;
            }
            *receipt = match input {
                Some(input) => match self
                    .runtime
                    .input_custody(self.actor, input)?
                    .ok_or(CausalError::InputObservationMissing)?
                    .phase
                {
                    InputPhase::Committed => Consumption::Consumed,
                    InputPhase::AbandonedByOwnerFailure => Consumption::AbandonedByOwnerFailure,
                    _ => Consumption::Pending,
                },
                // The saved partial-fill/queued receipt proves no transfer.
                None => Consumption::AbandonedByOwnerFailure,
            };
        }
        let ready = delivered.source_consumption != Consumption::Pending
            && delivered.event_consumption != Consumption::Pending;
        let observed = self.ingress.cut_consumption(cut).ok_or(CutError::Unknown)?;
        if observed != Consumption::Pending {
            return Ok(observed);
        }
        if !ready {
            return Ok(Consumption::Pending);
        }
        self.ingress.fail();
        self.ingress.abandon_consuming_event(cut)?;
        Ok(Consumption::AbandonedByOwnerFailure)
    }
    /// The failure owner uses the actual fixed runtime state, never a damaged
    /// Core table. Returned summaries still name sources in original custody;
    /// abandonment grants no source/capture exit or storage release.
    pub(crate) fn take_failed_queued(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<Option<CausalEventInput>, CausalError> {
        drained.validate(&self.runtime, self.actor)?;
        self.ingress.fail();
        match self.ingress.take_next_event(true) {
            Ok(input) => {
                let cut = input.prefix.cut.expect("original queued role cut");
                let role = self.roles.get_mut(&cut).expect("queued input role custody");
                assert!(role.delivered.is_none());
                role.delivered = Some(Delivered {
                    proof: None,
                    prefix: None,
                    sources: Delivered::sources(&input.prefix.summary),
                    source_consumption: Consumption::AbandonedByOwnerFailure,
                    event: None,
                    event_consumption: Consumption::AbandonedByOwnerFailure,
                    rejected: None,
                });
                Ok(Some(input))
            }
            Err(IngressError::Cut(CutError::NotReady | CutError::Consuming)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) fn validate_failed_scope(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        scope: &novarocks_control_runtime::transaction::ScopeHandle,
    ) -> Result<(), CausalError> {
        stage.validate_scope(&self.runtime, self.actor, scope)?;
        Ok(())
    }
    /// The same loader has actually finished its complete physical tree. Its
    /// original publisher can now relinquish only a never-registered event;
    /// neither stage nor a missing input proves that the event was consumed.
    pub(crate) fn withdraw_failed_unpublished_author(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        binding: &CausalAuthorBinding,
        loader: &LoaderCustody,
    ) -> Result<bool, CausalError> {
        self.validate_failed_scope(stage, loader.scope())?;
        self.validate_author_binding(binding)?;
        if loader.identity() != self.task || loader.work_id() != binding.work {
            return Err(CausalError::WrongAuthor);
        }
        if !loader.ready_for_exit().map_err(CausalError::Role)? {
            return Ok(false);
        }
        if self.roles[&binding.cut].delivered.is_some() {
            return Err(CausalError::AlreadyAdmitted);
        }
        if !self.ingress.event_unused(binding.cut)? {
            self.ingress.revoke_event(binding.cut)?;
        }
        Ok(true)
    }
    pub(crate) fn failed_author_consumption(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        binding: &CausalAuthorBinding,
    ) -> Result<Consumption, CausalError> {
        stage.validate(&self.runtime, self.actor)?;
        self.validate_author_binding(binding)?;
        if self.roles[&binding.cut]
            .input_author
            .as_ref()
            .is_some_and(|author| author.actual_exit_ready() == Ok(true))
        {
            self.ingress.observe_cut_exit(binding.cut, true, false)?;
        }
        if self.ingress.event_unused(binding.cut)? {
            return Ok(Consumption::AbandonedByOwnerFailure);
        }
        if self.roles[&binding.cut].delivered.is_none() {
            return Ok(Consumption::Pending);
        }
        self.abandon_core(stage, binding.cut)?;
        Ok(self.roles[&binding.cut]
            .delivered
            .as_ref()
            .unwrap()
            .event_consumption)
    }
    /// One exact original cut, at most one real header retirement per budget
    /// unit. Returned neutral bodies remain the caller's original inventory;
    /// this method neither drops rich sources nor signs their physical exit.
    pub(crate) fn advance_failed_original_cut(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        cut: CutId,
        budget: usize,
    ) -> Result<CausalFailedProgress, CausalError> {
        stage.validate(&self.runtime, self.actor)?;
        let mut progress = CausalFailedProgress {
            units: 0,
            progressed: false,
            inputs_retired: false,
            rejected: None,
        };
        if budget == 0 {
            return Ok(progress);
        }
        if !self.roles.contains_key(&cut) {
            return Err(CausalError::UnknownRole);
        }
        if !self.ingress.event_unused(cut)? {
            if self.roles[&cut].delivered.is_none() {
                return Ok(progress);
            }
            self.abandon_core(stage, cut)?;
            let delivered = self.roles[&cut].delivered.as_ref().unwrap();
            if delivered.event_consumption == Consumption::Pending
                || delivered.source_consumption == Consumption::Pending
            {
                return Ok(progress);
            }
        }
        progress.rejected = self.take_rejected(cut);
        progress.progressed = progress.rejected.is_some();
        for input in self.roles[&cut]
            .inputs
            .in_flight()?
            .into_iter()
            .flatten()
            .take(budget)
        {
            progress.units += 1;
            match self.runtime.retire_input(self.actor, input) {
                Ok(()) => progress.progressed = true,
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if self.roles[&cut]
            .inputs
            .in_flight()?
            .iter()
            .all(Option::is_none)
            && self.observe_effects_exited(cut)?
        {
            progress.inputs_retired = true;
            if !self.ingress.event_unused(cut)? && !self.roles[&cut].host_consumer_consumed {
                self.ingress.consume_cut(cut, 0)?;
                self.roles.get_mut(&cut).unwrap().host_consumer_consumed = true;
                progress.progressed = true;
            }
        }
        Ok(progress)
    }
    /// A reserved future author has not captured its predecessor prefix yet.
    /// Only this original delivered receipt or the author's actual unused
    /// withdrawal permits retiring that cut's current Source pair. Runtime
    /// input/effect retirement is deliberately not this gate: an old cleanup
    /// can itself wait for the independent Source's actual report/header exit.
    pub(crate) fn failed_source_pair_ready(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        cut: CutId,
    ) -> Result<bool, CausalError> {
        stage.validate(&self.runtime, self.actor)?;
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        Ok(role.delivered.is_some() || self.ingress.event_unused(cut)?)
    }
    /// Move one original source to its already-paid physical disposal owner.
    /// Taking a carrier grants no exit, report consumption or refund.
    pub(crate) fn take_failed_source(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        route: crate::task_proposal_ingress::PairRoute,
        ordinal: usize,
    ) -> Result<
        Option<
            novarocks_execution_contract::RetainedTaskSource<
                novarocks_execution::runtime::fragment::ExecutionFailure,
            >,
        >,
        CausalError,
    > {
        stage.validate(&self.runtime, self.actor)?;
        self.ingress.fail();
        self.ingress
            .take_failed_source(route, ordinal)
            .map_err(Into::into)
    }
    pub(crate) fn advance_failed_source_pair(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        cut: CutId,
        ledger: &novarocks_control_runtime::transaction::RuntimeLedger,
        scope: &novarocks_control_runtime::transaction::ScopeHandle,
        original_generation: Generation,
        budget: usize,
    ) -> Result<CausalSourceProgress, CausalError> {
        self.validate_failed_scope(stage, scope)?;
        let mut progressed = false;
        if budget == 0 {
            return Ok(CausalSourceProgress {
                progressed,
                reaped: None,
            });
        }
        self.ingress.fail();
        let route = crate::task_proposal_ingress::PairRoute::Cut(cut);
        for holder in self.ingress.take_unused_pair(route)?.into_iter().flatten() {
            holder.exit();
            progressed = true;
        }
        for original in self.ingress.pair_inventory(route)? {
            if let Some(source) = original.source {
                let core = self.source_core_consumption(cut, source)?;
                progressed |= original
                    .custody
                    .observe_failed_consumers(
                        stage,
                        &self.runtime,
                        self.actor,
                        original_generation,
                        core,
                    )
                    .map_err(CausalError::Role)?;
            } else {
                let before = original.custody.phase();
                original.custody.try_retire().map_err(CausalError::Role)?;
                progressed |= before != original.custody.phase();
            }
        }
        let runtime_error = |error| {
            CausalError::Role(novarocks_control_runtime::exit_role::ExitRoleError::Runtime(error))
        };
        progressed |= ledger
            .reap_scope_ready(scope.id(), budget.min(2))
            .map_err(runtime_error)?
            != 0;
        let [first, second] = self.ingress.prepare_pair_reap(route)?;
        let reaped = match (first.observe_reaped(), second.observe_reaped()) {
            (Ok(first), Ok(second)) => Some([first, second]),
            _ => None,
        };
        Ok(CausalSourceProgress { progressed, reaped })
    }
    /// Actual failed-owner teardown takes any values that never transferred.
    /// The runtime still owns a partially handed-off prefix, if one exists.
    pub(crate) fn take_rejected(
        &mut self,
        cut: CutId,
    ) -> Option<(Option<WorkerMessage>, WorkerMessage)> {
        self.roles
            .get_mut(&cut)?
            .delivered
            .as_mut()?
            .rejected
            .take()
    }
    /// These exact one-shot paid input positions become idle only when their
    /// actual runtime inputs retire. Runtime retirement verifies every batch
    /// effect and reservation handoff before clearing the same role pointer.
    /// This observes effects exit only; it grants neither author capture exit
    /// nor Core consumption or failed-owner publication-drain authority.
    pub(crate) fn observe_effects_exited(&self, cut: CutId) -> Result<bool, CausalError> {
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        if role.inputs.in_flight()?.iter().any(Option::is_some) {
            return Ok(false);
        }
        match &role.delivered {
            Some(delivered) if delivered.rejected.is_some() => return Ok(false),
            None if !self.ingress.event_unused(cut)? => return Ok(false),
            _ => {}
        }
        self.ingress.observe_cut_exit(cut, false, true)?;
        Ok(true)
    }
    /// Advance only this sealed cut's two original sources. Core receipts,
    /// physical holder exit, role retirement and header reclamation remain
    /// separate observations; none can stand in for another.
    pub(crate) fn advance_source_pair_retirement(
        &self,
        cut: CutId,
        ledger: &novarocks_control_runtime::transaction::RuntimeLedger,
        scope: &novarocks_control_runtime::transaction::ScopeHandle,
    ) -> Result<CausalSourceProgress, CausalError> {
        use novarocks_control_runtime_core::identity::ConsumerId;
        if !self.runtime.owns_scope(self.actor, scope)? {
            return Err(CausalError::WrongAuthor);
        }
        let runtime_error = |error| {
            CausalError::Role(novarocks_control_runtime::exit_role::ExitRoleError::Runtime(error))
        };
        let mut progressed = false;
        let route = crate::task_proposal_ingress::PairRoute::Cut(cut);
        // The ingress verifies sealed/unused before moving any Available
        // holder. Its real unused Drop revokes, rather than consuming reports.
        for holder in self.ingress.take_unused_pair(route)?.into_iter().flatten() {
            holder.exit();
            progressed = true;
        }
        for original in self.ingress.pair_inventory(route)? {
            let before = original.custody.phase();
            if before == ExitRolePhase::Retired {
                continue;
            }
            if let Some(source) = original.source {
                // ExitAuthority fills this original report only after its real
                // body/captures return. A saved Core receipt alone cannot fill it.
                if original
                    .custody
                    .actual_exit_ready()
                    .map_err(CausalError::Role)?
                {
                    let core = ConsumerId::new(1).expect("original source Core consumer");
                    if self.source_core_consumption(cut, source)? == Consumption::Consumed
                        && original
                            .custody
                            .report()
                            .consumption(core)
                            .map_err(runtime_error)?
                            == Consumption::Pending
                    {
                        original
                            .custody
                            .report()
                            .consume(core)
                            .map_err(runtime_error)?;
                        progressed = true;
                    }
                    let host = ConsumerId::new(2).expect("original source host consumer");
                    if original
                        .custody
                        .report()
                        .consumption(host)
                        .map_err(runtime_error)?
                        == Consumption::Pending
                    {
                        original
                            .custody
                            .report()
                            .consume(host)
                            .map_err(runtime_error)?;
                        progressed = true;
                    }
                }
            }
            original.custody.try_retire().map_err(CausalError::Role)?;
            progressed |= original.custody.phase() != before;
        }
        progressed |= ledger
            .reap_scope_ready(scope.id(), 1)
            .map_err(runtime_error)?
            != 0;
        let [first, second] = self.ingress.prepare_pair_reap(route)?;
        let reaped = match (first.observe_reaped(), second.observe_reaped()) {
            (Ok(first), Ok(second)) => Some([first, second]),
            _ => None,
        };
        Ok(CausalSourceProgress { progressed, reaped })
    }
    /// Source/capture/effect owners must first supply their true exit and all
    /// cut consumers. This wrapper only verifies/returns the original input
    /// positions; input retirement alone cannot manufacture those other facts.
    pub(crate) fn retire_after_exit(
        &mut self,
        cut: CutId,
        retirement: CausalSourceRetirement,
    ) -> Result<RetiredCut, CausalRetirementRefusal> {
        let checked = (|| {
            let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
            if role.input_author.as_ref().is_some_and(|action| {
                action.phase() != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
            }) || role
                .rpc_author
                .as_ref()
                .is_some_and(|author| author.role.phase() != ExitRolePhase::Retired)
                || role.loader_exit.as_ref().is_some_and(|loader| {
                    loader.phase() != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
                })
                || role.inputs.in_flight()?.iter().any(Option::is_some)
                || role
                    .delivered
                    .as_ref()
                    .is_some_and(|d| d.rejected.is_some())
            {
                return Err(DriverError::RoleBusy.into());
            }
            self.ingress.event_unused(cut).map_err(Into::into)
        })();
        let unused = match checked {
            Ok(unused) => unused,
            Err(error) => return Err(CausalRetirementRefusal { error, retirement }),
        };
        let retired = match retirement {
            CausalSourceRetirement::Unclaimed => {
                let retired = if unused {
                    self.ingress.revoke_unused_cut_unclaimed(cut)
                } else {
                    self.ingress.retire_cut_unclaimed(cut)
                };
                retired.map_err(|error| CausalRetirementRefusal {
                    error: error.into(),
                    retirement: CausalSourceRetirement::Unclaimed,
                })?
            }
            CausalSourceRetirement::Reaped(reaped) => {
                let retired = if unused {
                    self.ingress.revoke_unused_cut(cut, reaped)
                } else {
                    self.ingress.retire_cut(cut, reaped)
                };
                retired.map_err(|(error, reaped)| CausalRetirementRefusal {
                    error: error.into(),
                    retirement: CausalSourceRetirement::Reaped(reaped),
                })?
            }
        };
        let role = self.roles.get_mut(&cut).expect("original causal role");
        role.inputs
            .close_after_exit()
            .expect("exclusive idle causal positions retain their actor");
        self.roles.remove(&cut);
        if self.observing_prefix == Some(cut) {
            self.observing_prefix = None;
        }
        Ok(retired)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }
    /// Walk only the original finite role inventory. A notification does not
    /// identify a cut and must never synthesize its ID from issuance order.
    pub(crate) fn cut_inventory(&self, after: Option<CutId>, limit: usize) -> Vec<CutId> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.roles
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
            .map(|(cut, _)| *cut)
            .collect()
    }
}

#[cfg(test)]
mod tests;

impl TaskCausalInbox {
    pub(crate) fn trace_drain_inventory(&self, budget: usize) {
        for (cut, role) in self.roles.iter().take(budget) {
            let delivered = role.delivered.as_ref().map(|d| {
                (
                    d.prefix,
                    d.event,
                    d.event_consumption,
                    d.source_consumption,
                    d.sources,
                    d.rejected.is_some(),
                )
            });
            let sources = self
                .ingress
                .pair_inventory(crate::task_proposal_ingress::PairRoute::Cut(*cut))
                .map(|positions| {
                    positions.map(|position| {
                        (
                            position.key.address(),
                            position.source,
                            position.phase,
                            position.custody.phase(),
                            position.custody.actual_exit_ready(),
                        )
                    })
                });
            let inputs = role.inputs.in_flight().map(|inputs| {
                inputs.map(|input| {
                    input.map(|input| {
                        (
                            input,
                            self.runtime.input_custody(self.actor, input),
                            self.runtime.effect_inventory(self.actor, input, None, 16),
                        )
                    })
                })
            });
            tracing::debug!(pid = std::process::id(), task = ?self.task, ?cut,
                ?delivered, ?sources, ?inputs,
                input_author = ?role.input_author.as_ref().map(|a| (a.phase(), a.actual_exit_ready())),
                rpc_author = ?role.rpc_author.as_ref().map(|a| a.role.phase()),
                host_consumed = role.host_consumer_consumed, unused = ?self.ingress.event_unused(*cut),
                "Worker original causal cut inventory at process drain");
        }
    }
    /// This owner records the original event receipt before retiring either
    /// of its actual input headers. RPC cuts remain with their original R.
    pub(crate) fn retire_host_headers(&mut self, cut: CutId) -> Result<bool, CausalError> {
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        if role.rpc_author.is_some() {
            return Ok(false);
        }
        if let Some(author) = &role.input_author {
            if !author.actual_exit_ready().map_err(CausalError::Role)? {
                return Ok(false);
            }
            self.ingress.observe_cut_exit(cut, true, false)?;
        }
        let unused = self.ingress.event_unused(cut)?;
        if !unused {
            if role.delivered.is_none() {
                return Ok(false);
            }
            if self.observe_core(cut)? == Consumption::Pending || !self.event_core_consumed(cut)? {
                return Ok(false);
            }
        }
        let in_flight = self.roles[&cut].inputs.in_flight()?;
        for input in in_flight.into_iter().flatten() {
            match self.runtime.retire_input(self.actor, input) {
                Ok(()) => {}
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        if !self.observe_effects_exited(cut)? {
            return Ok(false);
        }
        if !unused && !self.roles[&cut].host_consumer_consumed {
            self.ingress.consume_cut(cut, 0)?;
            self.roles
                .get_mut(&cut)
                .expect("original cut")
                .host_consumer_consumed = true;
        }
        Ok(true)
    }
    /// Positive receipt for this original RPC queued item taken by the real
    /// failed owner before either of its runtime inputs ever transferred.
    pub(crate) fn rpc_abandoned_before_fill(&self, cut: CutId) -> Result<bool, CausalError> {
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        if role.rpc_author.is_none() {
            return Ok(false);
        }
        let Some(delivered) = &role.delivered else {
            return Ok(false);
        };
        Ok(delivered.proof.is_none()
            && delivered.prefix.is_none()
            && delivered.event.is_none()
            && delivered.source_consumption == Consumption::AbandonedByOwnerFailure
            && delivered.event_consumption == Consumption::AbandonedByOwnerFailure
            && delivered.rejected.is_none()
            && self.ingress.cut_consumption(cut) == Some(Consumption::AbandonedByOwnerFailure))
    }
    pub(crate) fn rpc_cut(&self, cut: CutId) -> Result<bool, CausalError> {
        Ok(self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .rpc_author
            .is_some())
    }
}

#[cfg(test)]
impl TaskCausalInbox {
    pub(crate) fn print_neutral_inventory(&self) {
        eprintln!(
            "causal metadata observing_prefix={:?} roles={}",
            self.observing_prefix,
            self.roles.len()
        );
        for (cut, role) in &self.roles {
            eprintln!(
                "causal cut={cut:?} inputs={:?} input_author={:?} loader_exit={:?} rpc={:?} unused={:?} core={:?} host_consumer={}",
                role.inputs.in_flight(),
                role.input_author
                    .as_ref()
                    .map(|author| (author.phase(), author.actual_exit_ready())),
                role.loader_exit.as_ref().map(|loader| loader.phase()),
                role.rpc_author.as_ref().map(|author| author.role.phase()),
                self.ingress.event_unused(*cut),
                self.ingress.cut_consumption(*cut),
                role.host_consumer_consumed
            );
            if let Some(delivered) = &role.delivered {
                eprintln!(
                    "causal delivery cut={cut:?} prefix={:?} event={:?} event_consumption={:?} source_consumption={:?} sources={:?} rejected={}",
                    delivered.prefix,
                    delivered.event,
                    delivered.event_consumption,
                    delivered.source_consumption,
                    delivered.sources,
                    delivered.rejected.is_some()
                );
            }
        }
    }
}
