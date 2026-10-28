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
use crate::{
    settlement::RegistrationNotice,
    task_causal_input::CausalInputRole,
    task_port::TaskPendingNotice,
    task_proposal_ingress::{
        CausalEventInput, CausalEventKind, CutSourceAdmission, IngressError, RetiredCut,
        TaskProposalIngress,
    },
};
use novarocks_control_runtime::driver::{ActorRuntime, DriverError, InputPhase};
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
    context::ContextInput,
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
    inputs: CausalInputRole<WorkerActor>,
    delivered: Option<Delivered>,
    // Reference the original custodian, not a copied consumption flag. Its
    // report must finish before this role can discard the exact event receipt.
    input_author: Option<CausalInputCustody>,
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
    health: Arc<ExecutionHealth>,
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
            health,
        }
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
        if role.delivered.is_some() || role.input_author.is_some() || role.loader_exit.is_some() {
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
        work: WorkId,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let (cut, mut sender) = self.admit(
            CausalEventKind::ApplyClose(generation),
            consumers,
            pending,
            admission,
        )?;
        sender.bind_work(work);
        Ok((cut, sender))
    }
    /// One finite, already-funded lifecycle author. Its owned neutral input
    /// is sealed with the same proposal prefix as settlement and Task close.
    pub(crate) fn admit_input(
        &mut self,
        work: WorkId,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let (cut, mut sender) =
            self.admit(CausalEventKind::Input, consumers, pending, admission)?;
        sender.bind_work(work);
        Ok((cut, sender))
    }
    fn admit(
        &mut self,
        kind: CausalEventKind,
        consumers: u64,
        pending: Arc<dyn TaskPendingNotice>,
        admission: CutSourceAdmission,
    ) -> Result<(CutId, TaskCausalSender), CausalAdmissionRefusal> {
        let mut inputs = match CausalInputRole::reserve(
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
                        inputs,
                        delivered: None,
                        input_author: None,
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
        if role.input_author.is_some() || role.loader_exit.is_some() || role.delivered.is_some() {
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
        if role.input_author.is_some() || role.loader_exit.is_some() || role.delivered.is_some() {
            return Err(CausalError::AlreadyAdmitted);
        }
        role.loader_exit = Some(loader);
        Ok(())
    }
    /// This custodian is exclusive; all roles for this Task use the same
    /// source FIFO. A different Task has its own source and can keep advancing.
    pub(crate) fn deliver_next(&mut self) -> Result<Option<CausalDelivery>, CausalError> {
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
            ContextInput::ApplyTaskClose { generation, .. } => Some(*generation),
            _ => None,
        };
        let event = WorkerMessage::Context(ContextMessage::Command {
            work: input.work,
            request: None,
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
        let observed = self.observe_core(cut)?;
        if observed != Consumption::Pending {
            return Ok(observed);
        }
        let delivered = self
            .roles
            .get(&cut)
            .ok_or(CausalError::UnknownRole)?
            .delivered
            .as_ref()
            .ok_or(CausalError::NotDelivered)?;
        let abandoned = match delivered.proof {
            Some(input) => {
                self.runtime
                    .input_custody(self.actor, input)?
                    .ok_or(CausalError::InputObservationMissing)?
                    .phase
                    == InputPhase::AbandonedByOwnerFailure
            }
            None => true, // Exact partial-fill receipt proves no input transferred.
        };
        if !abandoned {
            return Ok(Consumption::Pending);
        }
        self.ingress.fail();
        self.ingress.abandon_consuming_event(cut)?;
        let delivered = self
            .roles
            .get_mut(&cut)
            .unwrap()
            .delivered
            .as_mut()
            .unwrap();
        if delivered.source_consumption == Consumption::Pending {
            delivered.source_consumption = Consumption::AbandonedByOwnerFailure;
        }
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
                    source_consumption: Consumption::Pending,
                    event: None,
                    event_consumption: Consumption::Pending,
                    rejected: None,
                });
                Ok(Some(input))
            }
            Err(IngressError::Cut(CutError::NotReady | CutError::Consuming)) => Ok(None),
            Err(error) => Err(error.into()),
        }
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
            }) || role.loader_exit.as_ref().is_some_and(|loader| {
                loader.phase() != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
            }) || role.inputs.in_flight()?.iter().any(Option::is_some)
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
