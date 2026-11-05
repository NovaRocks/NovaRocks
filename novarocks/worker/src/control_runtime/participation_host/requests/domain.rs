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

//! The original domain R retains its rich input across pure FIFO waiting.
//! Getters, planned application and every foreign destructor run on host
//! workers. The Core's exact completion and actual host exit are separate.

use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    causal_input::InputAuthor,
    failure::ScopePublicationsDrained,
    participation_host_jobs::ParticipationDomainInstance,
    preparation::QueuedPreparation,
    request_ready::RequestReadySlot,
};
use crate::{
    TaskExecutionHost, task_domain_execution::apply_planned_task_domain_updates,
    task_port::TaskRuntimePort, task_proposal_ingress::DiscardedProposal,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    exit_custody::{ExitCustody, ExitCustodyError, ExitObligation},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ScopeId, ScopedWorkRef};
use novarocks_execution_contract::{
    AdvanceParticipationDomain, DomainProgression, ParticipationDomainUpdate, ParticipationRef,
    TaskConclusionProposal, TaskDescriptor, TaskDomainUpdate, TaskFailurePhase, TaskIdentity,
    TaskOperationId, UpdateTask,
};
use novarocks_worker_protocol::{
    creation::PayloadId,
    domain::{CredentialInput, CredentialState, ParticipationDomainInput, TaskDomainInput},
    participation::{DomainRefusal, ParticipationCommand, ParticipationEffect, ParticipationInput},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub(crate) enum DomainHostError {
    ActorTurn(ActorTurnError),
    Runtime(RuntimeError),
    Role(ExitCustodyError),
    WrongOwner,
    WrongPhase,
    WrongPlan,
    Unresolved,
}

impl std::fmt::Display for DomainHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActorTurn(error) => write!(f, "Domain host driver failed: {error}"),
            Self::Runtime(error) => write!(f, "Domain host resource operation failed: {error}"),
            Self::Role(error) => write!(f, "Domain host original role operation failed: {error}"),
            Self::WrongOwner => f.write_str("Domain host belongs to another original owner"),
            Self::WrongPhase => f.write_str("Domain host is in the wrong lifecycle phase"),
            Self::WrongPlan => f.write_str("Domain host input differs from the original plan"),
            Self::Unresolved => f.write_str("Domain host original capture remains unresolved"),
        }
    }
}
impl std::error::Error for DomainHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ActorTurn(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Role(error) => Some(error),
            Self::WrongOwner | Self::WrongPhase | Self::WrongPlan | Self::Unresolved => None,
        }
    }
}
/// Constructed by the actual R factory, after its whole payment and return
/// roles exist. These metadata identify originals; they grant no capacity.
pub(crate) struct DomainHostBinding {
    pub runtime: ActorRuntime<WorkerActor>,
    pub registration: ActorRegistration,
    pub scope: ScopeHandle,
    pub generation: Generation,
    pub participation: ParticipationRef,
    pub work: ScopedWorkRef,
    // A participation-local canonical credential identity, never a financial author.
    pub credential_resource: Option<ScopedWorkRef>,
    pub operation: TaskOperationId,
    pub payload: PayloadId,
    pub ready: Arc<RequestReadySlot>,
}

impl DomainHostBinding {
    fn credential_resource(&self) -> Result<u64, DomainHostError> {
        let original = self
            .credential_resource
            .ok_or(DomainHostError::WrongOwner)?;
        if original.scope != self.scope.id() {
            return Err(DomainHostError::WrongOwner);
        }
        // All canonical credential resources belong to this actual Participation
        // scope. Work IDs there are monotonic and never reused after exit.
        Ok(original.work.get())
    }
}

/// The actual Task owner supplies its descriptor and original weak runtime
/// port, rather than resolving another directory entry on a host worker.
pub(crate) struct DomainTaskTarget {
    pub descriptor: Option<TaskDescriptor>,
    pub port: Arc<TaskRuntimePort>,
    pub host: Arc<dyn TaskExecutionHost>,
}

struct TaskCaptures {
    descriptor: Option<ManuallyDrop<TaskDescriptor>>,
    port: Option<ManuallyDrop<Arc<TaskRuntimePort>>>,
    host: Option<ManuallyDrop<Arc<dyn TaskExecutionHost>>>,
    refused_source: Option<ManuallyDrop<DiscardedProposal>>,
}
impl TaskCaptures {
    fn descriptor(&self) -> Option<&TaskDescriptor> {
        self.descriptor.as_deref()
    }
    fn port(&self) -> &TaskRuntimePort {
        self.port.as_ref().expect("original Task port")
    }
    fn host(&self) -> &dyn TaskExecutionHost {
        self.host.as_ref().expect("original Task host").as_ref()
    }
}
enum Rich {
    Task {
        updates: Option<ManuallyDrop<Vec<TaskDomainUpdate>>>,
        target: TaskCaptures,
    },
    Participation {
        update: Option<ManuallyDrop<ParticipationDomainUpdate>>,
        instance: Option<ManuallyDrop<ParticipationDomainInstance>>,
        credential: Option<CredentialInput>,
    },
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Captured,
    Projected,
    Running,
    AwaitingFailure,
    Applied,
    Disposed,
    Unknown,
}
struct State {
    request: Option<InputId>,
    task: Option<TaskIdentity>,
    phase: Phase,
    rich: Option<Rich>,
    completed: bool,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    // A refused publication is an original, not an invented consumed input.
    refused: Option<(ParticipationInput, DomainReturn)>,
    unknown_author: Option<DomainReturn>,
    start_refusal: Option<ExitCustodyError>,
}
struct Shared {
    binding: DomainHostBinding,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "The original domain R retains its rich input and exact Core completion"]
pub(crate) struct DomainCustody(Arc<Shared>);

impl DomainCustody {
    fn binding_valid(binding: &DomainHostBinding) -> Result<(), DomainHostError> {
        binding
            .runtime
            .validate_registration(&binding.registration)
            .map_err(DomainHostError::ActorTurn)?;
        if !binding.registration.matches_scope(&binding.scope) {
            return Err(DomainHostError::WrongOwner);
        }
        Ok(())
    }
    pub(crate) fn capture_task(
        binding: DomainHostBinding,
        original: UpdateTask,
        target: DomainTaskTarget,
    ) -> Result<
        Self,
        (
            DomainHostError,
            DomainHostBinding,
            UpdateTask,
            DomainTaskTarget,
        ),
    > {
        let error = Self::binding_valid(&binding).err().or_else(|| {
            (binding.credential_resource.is_some()
                || original.envelope().operation_id() != binding.operation
                || target
                    .descriptor
                    .as_ref()
                    .is_some_and(|descriptor| original.identity() != descriptor.identity())
                || original.identity() != target.port.identity()
                || !target.port.publication().owned_by(&binding.scope))
            .then_some(DomainHostError::WrongOwner)
        });
        if let Some(error) = error {
            return Err((error, binding, original, target));
        }
        let (_, task, updates) = original.into_parts();
        Ok(Self::capture(
            binding,
            Some(task),
            Rich::Task {
                updates: Some(ManuallyDrop::new(updates)),
                target: TaskCaptures {
                    descriptor: target.descriptor.map(ManuallyDrop::new),
                    port: Some(ManuallyDrop::new(target.port)),
                    host: Some(ManuallyDrop::new(target.host)),
                    refused_source: None,
                },
            },
        ))
    }
    pub(crate) fn capture_participation(
        mut binding: DomainHostBinding,
        original: AdvanceParticipationDomain,
        instance: ParticipationDomainInstance,
        credential_resource: ScopedWorkRef,
    ) -> Result<
        Self,
        (
            DomainHostError,
            DomainHostBinding,
            AdvanceParticipationDomain,
            ParticipationDomainInstance,
        ),
    > {
        let error = Self::binding_valid(&binding).err().or_else(|| {
            (binding.credential_resource.is_some()
                || credential_resource.scope != binding.scope.id()
                || original.envelope().operation_id() != binding.operation
                || original.participation() != binding.participation
                || !instance.matches_owner(&binding.runtime, &binding.scope, binding.participation))
            .then_some(DomainHostError::WrongOwner)
        });
        if let Some(error) = error {
            return Err((error, binding, original, instance));
        }
        binding.credential_resource = Some(credential_resource);
        let (_, _, update) = original.into_parts();
        Ok(Self::capture(
            binding,
            None,
            Rich::Participation {
                update: Some(ManuallyDrop::new(update)),
                instance: Some(ManuallyDrop::new(instance)),
                credential: None,
            },
        ))
    }
    fn capture(binding: DomainHostBinding, task: Option<TaskIdentity>, rich: Rich) -> Self {
        let shared = Arc::new(Shared {
            binding,
            state: Mutex::new(State {
                request: None,
                task,
                phase: Phase::Captured,
                rich: Some(rich),
                completed: false,
                panic: None,
                refused: None,
                unknown_author: None,
                start_refusal: None,
            }),
            retained: Mutex::new(None),
        });
        *shared.retained.lock().expect("original domain retention") = Some(shared.clone());
        Self(shared)
    }
    /// Called within the original incoming R's physical body. No additional
    /// Work or source is created to project this already admitted carrier.
    pub(crate) fn project_on_host(&self) -> Result<ParticipationCommand, DomainHostError> {
        let mut rich = {
            let mut state = self.0.state.lock().expect("original domain projection");
            if state.phase != Phase::Captured {
                return Err(DomainHostError::WrongPhase);
            }
            state.phase = Phase::Running;
            state.rich.take().expect("original domain input")
        };
        let result = catch_unwind(AssertUnwindSafe(|| match &mut rich {
            Rich::Task { updates, target } => Ok(ParticipationCommand::UpdateTask {
                task: target.port().identity(),
                payload: self.0.binding.payload,
                updates: updates
                    .as_ref()
                    .expect("original Task updates")
                    .iter()
                    .map(task_input)
                    .collect(),
            }),
            Rich::Participation {
                update,
                instance,
                credential,
            } => {
                let original = update.as_ref().expect("original Participation domain");
                let input = match &**original {
                    ParticipationDomainUpdate::CatalogBinding { version, payload } => {
                        ParticipationDomainInput::Catalog {
                            version: *version,
                            fingerprint: payload.fingerprint(),
                        }
                    }
                    ParticipationDomainUpdate::SharedDynamicFilter { version, payload } => {
                        ParticipationDomainInput::Filter {
                            version: *version,
                            fingerprint: payload.fingerprint(),
                        }
                    }
                    ParticipationDomainUpdate::Credential(update) => {
                        let input = instance
                            .as_mut()
                            .expect("original Participation instance")
                            .credential_on_host(update, self.0.binding.credential_resource()?)
                            .map_err(|_| DomainHostError::WrongPhase)?;
                        *credential = Some(input);
                        instance
                            .as_mut()
                            .expect("original Participation instance")
                            .clear_comparison_on_host();
                        ParticipationDomainInput::Credential(input)
                    }
                };
                Ok(ParticipationCommand::UpdateParticipation {
                    payload: self.0.binding.payload,
                    update: input,
                })
            }
        }));
        let mut state = self
            .0
            .state
            .lock()
            .expect("original domain projected input");
        state.rich = Some(rich);
        let outcome = match result {
            Ok(Ok(command)) => {
                state.phase = Phase::Projected;
                Ok(command)
            }
            Ok(Err(error)) => {
                state.phase = Phase::Unknown;
                Err(error)
            }
            Err(panic) => {
                state.phase = Phase::Unknown;
                state.panic = Some(ManuallyDrop::new(panic));
                Err(DomainHostError::Unresolved)
            }
        };
        drop(state);
        self.0.binding.ready.notify();
        outcome
    }

    /// Bind only the actual delivered incoming request token. Wire operation
    /// IDs never select a pending rich input or a feedback input.
    pub(crate) fn bind_request(&self, request: InputId) -> Result<(), DomainHostError> {
        let mut state = self.0.state.lock().expect("original domain request token");
        if state.phase != Phase::Projected || state.request.is_some() {
            return Err(DomainHostError::WrongPhase);
        }
        state.request = Some(request);
        Ok(())
    }
    fn author_matches(&self, state: &State, author: &DomainReturn) -> bool {
        match (state.task, author) {
            (Some(task), DomainReturn::Task(author)) => author.task() == task,
            (None, DomainReturn::Participation(author)) => {
                author.scope.same_scope(&self.0.binding.scope)
            }
            _ => false,
        }
    }
    fn validate(&self, ticket: &EffectTicket<WorkerAction>) -> Result<(), DomainHostError> {
        self.0
            .binding
            .runtime
            .validate_effect_ticket(ticket)
            .map_err(DomainHostError::ActorTurn)?;
        if ticket.actor != self.0.binding.registration.actor() {
            return Err(DomainHostError::WrongOwner);
        }
        Ok(())
    }
    pub(crate) fn take_job(
        &self,
        ticket: &EffectTicket<WorkerAction>,
        author: DomainReturn,
    ) -> Result<DomainHostJob, (DomainHostError, DomainReturn)> {
        if let Err(error) = self.validate(ticket) {
            return Err((error, author));
        }
        let mut state = self.0.state.lock().expect("exact domain Host effect");
        if state.phase != Phase::Projected {
            return Err((DomainHostError::WrongPhase, author));
        }
        if let DomainReturn::Participation(original) = &author
            && !original.scope.same_scope(&self.0.binding.scope)
        {
            return Err((DomainHostError::WrongOwner, author));
        }
        let operation = match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::ApplyTaskDomains {
                request,
                task,
                payload,
                progressions,
            }) if state.request == Some(*request)
                && state.task == Some(*task)
                && *payload == self.0.binding.payload =>
            {
                let Rich::Task { updates, .. } =
                    state.rich.as_ref().expect("original Task rich input")
                else {
                    return Err((DomainHostError::WrongOwner, author));
                };
                if updates.as_ref().expect("original Task updates").len() != progressions.len()
                    || !author.is_task(*task)
                {
                    return Err((DomainHostError::WrongPlan, author));
                }
                Operation::TaskApply {
                    request: *request,
                    task: *task,
                    plan: progressions.clone(),
                }
            }
            WorkerAction::Participation(ParticipationEffect::ApplyParticipationDomain {
                request,
                payload,
                progression,
            }) if state.request == Some(*request)
                && state.task.is_none()
                && *payload == self.0.binding.payload
                && author.is_participation() =>
            {
                Operation::ParticipationApply {
                    request: *request,
                    progression: *progression,
                }
            }
            WorkerAction::Participation(
                ParticipationEffect::RecompareParticipationCredential {
                    request,
                    payload,
                    expected,
                },
            ) if state.request == Some(*request)
                && state.task.is_none()
                && *payload == self.0.binding.payload
                && author.is_participation() =>
            {
                Operation::Compare {
                    request: *request,
                    expected: *expected,
                }
            }
            _ => return Err((DomainHostError::WrongOwner, author)),
        };
        let rich = state.rich.take().expect("one original domain Host body");
        state.phase = Phase::Running;
        Ok(DomainHostJob {
            owner: self.clone(),
            operation: Some(operation),
            rich: Some(rich),
            author: Some(author),
            generation: self.0.binding.generation,
        })
    }
    /// A queued/early-refused request still needs its actual opaque destructor
    /// on a host worker. The same prepaid application author supplies that body.
    pub(crate) fn take_disposal_job(
        &self,
        author: DomainReturn,
    ) -> Result<DomainHostJob, (DomainHostError, DomainReturn)> {
        let mut state = self.0.state.lock().expect("original domain disposal");
        if !state.completed || state.phase != Phase::Projected {
            return Err((DomainHostError::WrongPhase, author));
        }
        if !self.author_matches(&state, &author)
            && !matches!(&author, DomainReturn::Participation(author) if author.scope.same_scope(&self.0.binding.scope))
        {
            return Err((DomainHostError::WrongOwner, author));
        }
        let rich = state.rich.take().expect("original queued domain source");
        state.phase = Phase::Running;
        Ok(DomainHostJob {
            owner: self.clone(),
            operation: Some(Operation::Dispose),
            rich: Some(rich),
            author: Some(author),
            generation: self.0.binding.generation,
        })
    }
    /// Only the live original failure publication stage can abandon the old
    /// Core leg and run cleanup in its new cleanup epoch. This never retries
    /// an already executed or panicked foreign application.
    pub(crate) fn take_failed_disposal_job(
        &self,
        drained: &ScopePublicationsDrained<'_>,
        supplied: Option<DomainReturn>,
    ) -> Result<DomainHostJob, (DomainHostError, Option<DomainReturn>)> {
        if let Err(error) =
            drained.validate(&self.0.binding.runtime, self.0.binding.registration.actor())
        {
            return Err((DomainHostError::ActorTurn(error), supplied));
        }
        let generation = match self.0.binding.scope.gates() {
            Ok(gates) => gates.generation,
            Err(error) => return Err((DomainHostError::Runtime(error), supplied)),
        };
        let mut state = self
            .0
            .state
            .lock()
            .expect("original failed domain disposal");
        if !matches!(state.phase, Phase::Projected | Phase::AwaitingFailure) {
            return Err((DomainHostError::WrongPhase, supplied));
        }
        let author = if state.phase == Phase::AwaitingFailure {
            if supplied.is_some() {
                return Err((DomainHostError::WrongOwner, supplied));
            }
            state
                .unknown_author
                .take()
                .expect("same unstarted domain author")
        } else {
            let Some(author) = supplied else {
                return Err((DomainHostError::WrongOwner, None));
            };
            // The same never-exposed Task funding may already have been
            // converted into its original direct disposal return. As on the
            // healthy disposal path, this is a Participation-local prepaid author,
            // not a new Task callback or application authority.
            if !self.author_matches(&state, &author)
                && !matches!(&author, DomainReturn::Participation(author) if author.scope.same_scope(&self.0.binding.scope))
            {
                return Err((DomainHostError::WrongOwner, Some(author)));
            }
            author
        };
        let rich = state.rich.take().expect("original failed domain captures");
        state.completed = true;
        state.phase = Phase::Running;
        Ok(DomainHostJob {
            owner: self.clone(),
            operation: Some(Operation::Dispose),
            rich: Some(rich),
            author: Some(author),
            generation,
        })
    }
    pub(crate) fn complete_core(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), DomainHostError> {
        self.validate(ticket)?;
        let mut state = self.0.state.lock().expect("exact domain Core completion");
        match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::DomainRequestCompleted {
                request,
                payload,
                task,
            }) if state.request == Some(*request)
                && *payload == self.0.binding.payload
                && *task == state.task =>
            {
                state.completed = true
            }
            _ => return Err(DomainHostError::WrongOwner),
        }
        drop(state);
        self.0.binding.ready.notify();
        Ok(())
    }
    /// Inspect only the original custody's actual outcome. A background
    /// callback, destructor or refused return cannot appear as healthy waiting
    /// or as physical exit. The caller keeps this same R in its failure inventory.
    pub(crate) fn unresolved(&self) -> bool {
        let state = self
            .0
            .state
            .lock()
            .expect("original domain unresolved state");
        state.phase == Phase::Unknown || state.panic.is_some() || state.refused.is_some()
    }
    /// No application body ran. This is permission to inspect the actual
    /// Scope failure owner, never a substitute for its publication stage.
    /// A healthy Scope with a start refusal is a driver fault, not a reason to
    /// fabricate a failure session or park forever waiting for one.
    pub(crate) fn awaiting_failure(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original domain start-fence state")
            .phase
            == Phase::AwaitingFailure
    }
    pub(crate) fn start_refusal(&self) -> Option<ExitCustodyError> {
        self.0
            .state
            .lock()
            .expect("original domain start refusal")
            .start_refusal
    }
    /// A failure stage resolves only the old Core leg. Physical capture and
    /// author return remain separate positive observations of the same body.
    pub(super) fn abandon_failed_core(
        &self,
        stage: &ScopePublicationsDrained<'_>,
    ) -> Result<(), DomainHostError> {
        stage
            .validate(&self.0.binding.runtime, self.0.binding.registration.actor())
            .map_err(DomainHostError::ActorTurn)?;
        self.0
            .state
            .lock()
            .expect("original failed Domain Core leg")
            .completed = true;
        Ok(())
    }
    /// Some(true) retains the actual never-started author from its refused
    /// Job; Some(false) needs one of the original prepaid unused authors.
    pub(super) fn failed_disposal_author(&self) -> Option<bool> {
        match self
            .0
            .state
            .lock()
            .expect("original Domain disposal phase")
            .phase
        {
            Phase::AwaitingFailure => Some(true),
            Phase::Projected => Some(false),
            _ => None,
        }
    }
    pub(super) fn request_bound(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original Domain request binding")
            .request
            .is_some()
    }
    pub(super) fn matches_feedback(&self, input: &ParticipationInput) -> bool {
        let state = self
            .0
            .state
            .lock()
            .expect("original Domain feedback identity");
        match input {
            ParticipationInput::ParticipationUpdateApplied { request, .. }
            | ParticipationInput::ParticipationCredentialCompared { request, .. } => {
                state.task.is_none() && state.request == Some(*request)
            }
            ParticipationInput::TaskUpdateApplied { request, task, .. } => {
                state.task == Some(*task) && state.request == Some(*request)
            }
            _ => false,
        }
    }
    pub(super) fn matches_projection(&self, command: &ParticipationCommand) -> bool {
        let state = self
            .0
            .state
            .lock()
            .expect("original Domain projection identity");
        match command {
            ParticipationCommand::UpdateTask { task, payload, .. } => {
                state.task == Some(*task) && *payload == self.0.binding.payload
            }
            ParticipationCommand::UpdateParticipation { payload, .. } => {
                state.task.is_none() && *payload == self.0.binding.payload
            }
            _ => false,
        }
    }
    pub(super) fn notify(&self) {
        self.0.binding.ready.notify();
    }
    pub(super) fn work(&self) -> ScopedWorkRef {
        self.0.binding.work
    }
    pub(super) fn participation(&self) -> ParticipationRef {
        self.0.binding.participation
    }
    pub(super) fn task(&self) -> Option<TaskIdentity> {
        self.0.state.lock().expect("original Domain Task").task
    }
    pub(super) fn bind_task_descriptor(
        &self,
        descriptor: TaskDescriptor,
    ) -> Result<(), DomainHostError> {
        let mut state = self
            .0
            .state
            .lock()
            .expect("same original installed domain target");
        if state.phase != Phase::Projected || state.task != Some(descriptor.identity()) {
            return Err(DomainHostError::WrongOwner);
        }
        let Some(Rich::Task { target, .. }) = state.rich.as_mut() else {
            return Err(DomainHostError::WrongPhase);
        };
        if target.descriptor.is_none() {
            target.descriptor = Some(ManuallyDrop::new(descriptor));
        }
        Ok(())
    }
    pub(super) fn task_application_ready(&self) -> bool {
        let state = self.0.state.lock().expect("actual domain target readiness");
        match state.rich.as_ref() {
            Some(Rich::Task { target, .. }) => {
                target.descriptor.is_some() || target.port().current().is_terminal()
            }
            _ => false,
        }
    }
    pub(super) fn needs_disposal(&self) -> bool {
        let state = self
            .0
            .state
            .lock()
            .expect("original Domain disposal readiness");
        state.completed && state.phase == Phase::Projected
    }
    pub(super) fn withdraw_before_request(&self) -> Result<(), DomainHostError> {
        let mut state = self
            .0
            .state
            .lock()
            .expect("original unsubmitted domain withdrawal");
        if state.request.is_some() || !matches!(state.phase, Phase::Captured | Phase::Projected) {
            return Err(DomainHostError::WrongPhase);
        }
        state.completed = true;
        // Captured inputs still need their real destructor; no projection is
        // necessary when no command ever crossed the request boundary.
        state.phase = Phase::Projected;
        Ok(())
    }
    pub(super) fn projection_abandoned(&self) {
        self.0
            .state
            .lock()
            .expect("original lost domain projection")
            .phase = Phase::Unknown;
        self.0.binding.ready.notify();
    }
    pub(crate) fn actual_exit_ready(&self) -> bool {
        let state = self.0.state.lock().expect("actual domain source exit");
        state.completed
            && matches!(state.phase, Phase::Applied | Phase::Disposed)
            && state.rich.is_none()
            && state.refused.is_none()
            && state.panic.is_none()
            && state.unknown_author.is_none()
    }
    /// Only the original R owner calls this after both legs and its independent
    /// reports/inputs have retired. It does not refund or sign any exit itself.
    pub(crate) fn release_retention(&self) -> bool {
        if !self.actual_exit_ready() {
            return false;
        }
        let original = self
            .0
            .retained
            .lock()
            .expect("original domain retention")
            .take();
        drop(original);
        true
    }
}

pub(super) fn task_input(update: &TaskDomainUpdate) -> TaskDomainInput {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => TaskDomainInput::Split {
            node: intent.node(),
            offer: intent.offer(),
        },
        TaskDomainUpdate::TaskDynamicFilter { version, payload } => TaskDomainInput::Filter {
            version: *version,
            fingerprint: payload.fingerprint(),
        },
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => TaskDomainInput::Open {
            version: *version,
            edges: edges.clone(),
        },
        TaskDomainUpdate::CloseExchangeDestination {
            version,
            edge,
            destination,
        } => TaskDomainInput::Close {
            version: *version,
            edge: *edge,
            destination: *destination,
        },
    }
}

/// Every Participation comparison/application has a distinct already-admitted
/// ReportedWork and one-shot return cell. The caller retains the returned
/// role/reader through actual input consumption and report retirement.
pub(crate) struct ParticipationDomainReturnReceipt {
    pub exit_custody: ExitCustody,
    pub reader: OwnedReturnReader<ParticipationInput>,
}
pub(crate) struct ParticipationDomainReturnAuthor {
    scope: ScopeHandle,
    work: ScopedWorkRef,
    authority: Option<ExitObligation>,
    sender: Option<OwnedReturnSender<ParticipationInput>>,
}
impl ParticipationDomainReturnAuthor {
    pub(crate) fn from_reported(
        original: ReportedWork,
    ) -> Result<(ParticipationDomainReturnReceipt, Self), (RuntimeError, ReportedWork)> {
        let (sender, reader) = match original.report().claim_owned_return() {
            Ok(pair) => pair,
            Err(error) => return Err((error, original)),
        };
        let scope = original.work().scope().clone();
        let work = original.work().reference();
        let (exit_custody, authority) = ExitCustody::from_admitted(original);
        Ok((
            ParticipationDomainReturnReceipt {
                exit_custody,
                reader,
            },
            Self {
                scope,
                work,
                authority: Some(authority),
                sender: Some(sender),
            },
        ))
    }
}
pub(crate) enum DomainReturn {
    Task(InputAuthor),
    Participation(ParticipationDomainReturnAuthor),
}
impl DomainReturn {
    pub(super) fn withdraw_unused(self) -> Result<(), Option<Self>> {
        match self.unused() {
            Ok(()) => Ok(()),
            Err(UnusedRefusal::Participation(author)) => Err(Some(Self::Participation(author))),
            Err(UnusedRefusal::Retained) => Err(None),
        }
    }
    fn is_task(&self, task: TaskIdentity) -> bool {
        matches!(self, Self::Task(author) if author.task() == task)
    }
    fn is_participation(&self) -> bool {
        matches!(self, Self::Participation(_))
    }
    fn work(&self) -> ScopedWorkRef {
        match self {
            Self::Task(author) => author.custody().work_id(),
            Self::Participation(author) => author.work,
        }
    }
    fn start(&mut self, generation: Generation) -> Result<(), ExitCustodyError> {
        match self {
            Self::Task(author) => author.start_in_generation(generation),
            Self::Participation(author) => author
                .authority
                .as_mut()
                .expect("original Participation domain authority")
                .start_in_generation(generation),
        }
    }
    fn publish(self, input: ParticipationInput) -> Result<(), PublicationRefusal> {
        match self {
            Self::Task(author) => {
                // InputAuthor retains any rejected original input internally.
                author
                    .publish(input)
                    .map_err(|_| PublicationRefusal::RetainedTask)
            }
            Self::Participation(mut author) => {
                let sender = author
                    .sender
                    .take()
                    .expect("original Participation domain return");
                match sender.publish(input) {
                    Ok(()) => author
                        .authority
                        .take()
                        .expect("original Participation domain exit")
                        .exited()
                        .map_err(|_| PublicationRefusal::RetainedTask),
                    Err(refusal) => {
                        author.sender = Some(refusal.sender);
                        Err(PublicationRefusal::Participation(
                            Box::new(refusal.value),
                            author,
                        ))
                    }
                }
            }
        }
    }
    fn unused(self) -> Result<(), UnusedRefusal> {
        match self {
            Self::Task(author) => author.withdraw().map_err(|_| UnusedRefusal::Retained),
            Self::Participation(mut author) => {
                let sender = author
                    .sender
                    .take()
                    .expect("original unused Participation domain return");
                match sender.revoke_unused() {
                    Ok(()) => author
                        .authority
                        .take()
                        .expect("original unused domain exit")
                        .withdraw_unpublished_and_exit()
                        .map_err(|_| UnusedRefusal::Retained),
                    Err(refusal) => {
                        author.sender = Some(refusal.sender);
                        Err(UnusedRefusal::Participation(author))
                    }
                }
            }
        }
    }
}

enum PublicationRefusal {
    RetainedTask,
    Participation(Box<ParticipationInput>, ParticipationDomainReturnAuthor),
}
enum UnusedRefusal {
    Retained,
    Participation(ParticipationDomainReturnAuthor),
}

enum Operation {
    TaskApply {
        request: InputId,
        task: TaskIdentity,
        plan: Arc<Vec<DomainProgression>>,
    },
    ParticipationApply {
        request: InputId,
        progression: DomainProgression,
    },
    Compare {
        request: InputId,
        expected: CredentialState,
    },
    Dispose,
}
#[must_use = "Queue refusal returns this same original domain job"]
pub(crate) struct DomainHostJob {
    owner: DomainCustody,
    operation: Option<Operation>,
    rich: Option<Rich>,
    author: Option<DomainReturn>,
    generation: Generation,
}
impl QueuedPreparation for DomainHostJob {
    fn queue_key(&self) -> (ScopeId, novarocks_control_runtime_core::identity::WorkId) {
        let author = self.author.as_ref().expect("original domain author").work();
        (author.scope, author.work)
    }
    fn fairness_group(&self, _: ScopeId) -> crate::control_runtime::preparation::HostFairnessGroup {
        crate::control_runtime::preparation::HostFairnessGroup::Participation(
            self.owner.0.binding.participation,
        )
    }
}
impl DomainHostJob {
    pub(super) fn author_work(&self) -> ScopedWorkRef {
        self.author
            .as_ref()
            .expect("same original Domain job author")
            .work()
    }
    pub(crate) fn run_on_host(mut self) {
        let mut author = self.author.take().expect("original domain return author");
        if let Err(error) = author.start(self.generation) {
            // Preserve this exact unstarted author and captures. Only the
            // live failure stage may subsequently authorize their disposal.
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("original domain start refused");
            state.rich = self.rich.take();
            state.unknown_author = Some(author);
            state.phase = Phase::AwaitingFailure;
            state.start_refusal = Some(error);
            self.operation.take();
            drop(state);
            self.owner.0.binding.ready.notify();
            return;
        }
        let operation = self.operation.take().expect("original domain operation");
        let mut rich = self.rich.take().expect("original domain source");
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.apply_on_host(&operation, &mut rich)
        }));
        match result {
            Ok(Ok(input)) => {
                let comparison = matches!(operation, Operation::Compare { .. });
                let disposal = matches!(operation, Operation::Dispose);
                if !comparison
                    && let Err(panic) =
                        catch_unwind(AssertUnwindSafe(|| dispose_on_host(&mut rich)))
                {
                    self.retain_unknown(rich, author, Some(panic));
                    return;
                }
                // Transfer retained rich back before the once-only publisher
                // seals the current Task proposal prefix and reports its exit.
                {
                    let mut state = self
                        .owner
                        .0
                        .state
                        .lock()
                        .expect("actual domain Host return");
                    if comparison {
                        state.rich = Some(rich);
                    }
                    state.phase = if comparison {
                        Phase::Projected
                    } else if disposal {
                        Phase::Disposed
                    } else {
                        Phase::Applied
                    };
                }
                if let Some(input) = input {
                    if let Err(refused) = author.publish(input) {
                        let mut state = self
                            .owner
                            .0
                            .state
                            .lock()
                            .expect("original refused domain return");
                        if let PublicationRefusal::Participation(input, author) = refused {
                            state.refused = Some((*input, DomainReturn::Participation(author)));
                        }
                        state.phase = Phase::Unknown;
                    }
                } else if let Err(refused) = author.unused() {
                    let mut state = self
                        .owner
                        .0
                        .state
                        .lock()
                        .expect("original unused domain return refusal");
                    if let UnusedRefusal::Participation(author) = refused {
                        state.unknown_author = Some(DomainReturn::Participation(author));
                    }
                    state.phase = Phase::Unknown;
                }
                self.owner.0.binding.ready.notify();
            }
            Ok(Err(_)) => self.retain_unknown(rich, author, None),
            Err(panic) => self.retain_unknown(rich, author, Some(panic)),
        }
    }
    fn apply_on_host(
        &self,
        operation: &Operation,
        rich: &mut Rich,
    ) -> Result<Option<ParticipationInput>, DomainHostError> {
        match (operation, rich) {
            (
                Operation::TaskApply {
                    request,
                    task,
                    plan,
                },
                Rich::Task { updates, target },
            ) => {
                let Some(descriptor) = target.descriptor() else {
                    if !target.port().current().is_terminal() {
                        return Err(DomainHostError::WrongPhase);
                    }
                    // The actual original Task ended before installation. No
                    // Host application or queued depth is fabricated here.
                    return Ok(Some(ParticipationInput::TaskUpdateApplied { request: *request, task: *task,
                        result: Err(DomainRefusal {
                            outcome: novarocks_execution_contract::operation::DomainFailureOutcome::InvalidStateOrRequest,
                            failure: novarocks_execution_contract::operation::OperationDomainFailure::new(
                                novarocks_execution_contract::failure::FailureFact::new(
                                    novarocks_execution_contract::failure::FailureClass::Invariant,
                                    crate::failure_reason::WorkerFailureReason::DomainAfterSeal.reason()),
                                novarocks_execution_contract::SafeDetail::truncating("the original Task ended before domain installation")),
                        }) }));
                };
                let result = match apply_planned_task_domain_updates(
                    target.host(),
                    descriptor,
                    updates.as_ref().expect("original Task updates"),
                    plan,
                    self.owner.0.binding.operation,
                ) {
                    Ok(queued) => Ok(queued),
                    Err(rejection) => {
                        let refusal = DomainRefusal {
                            outcome: rejection.domain_failure_outcome(),
                            failure: rejection.operation_failure(),
                        };
                        // This exact original source reaches the Task before
                        // feedback seals its proposal prefix. Partial Host
                        // side effects can never be rolled back then continued.
                        let handoff =
                            target
                                .port()
                                .ingress()
                                .submit(TaskConclusionProposal::Failed {
                                    cause: rejection.into_host().into_failure(),
                                    phase: TaskFailurePhase::Execution,
                                });
                        if !handoff.transferred || handoff.error.is_some() {
                            target.refused_source = handoff.discarded.map(ManuallyDrop::new);
                            return Err(DomainHostError::Unresolved);
                        }
                        // A definite loser remains a real source destructor
                        // on this physical host, after the ingress lock left.
                        drop(handoff.discarded);
                        Err(refusal)
                    }
                };
                Ok(Some(ParticipationInput::TaskUpdateApplied {
                    request: *request,
                    task: *task,
                    result,
                }))
            }
            (
                Operation::ParticipationApply {
                    request,
                    progression,
                },
                Rich::Participation {
                    update,
                    instance,
                    credential,
                },
            ) => {
                let original = update
                    .as_ref()
                    .expect("original Participation domain update");
                let result = if *progression == DomainProgression::Apply {
                    match instance
                        .as_ref()
                        .expect("original Participation instance")
                        .advance_on_host(original)
                    {
                        Ok(()) => {
                            if let ParticipationDomainUpdate::Credential(update) = &**original {
                                let input = credential.ok_or(DomainHostError::WrongPhase)?;
                                let old = instance
                                    .as_ref()
                                    .expect("original Participation instance")
                                    .adopt_credential_on_host(
                                        update,
                                        input.current_resource,
                                        input.replacement_resource,
                                    )
                                    .map_err(|_| DomainHostError::WrongPhase)?;
                                drop(old.map(ManuallyDrop::into_inner));
                            }
                            Ok(())
                        }
                        Err(rejection) => {
                            let refusal = DomainRefusal { outcome: novarocks_execution_contract::operation::DomainFailureOutcome::InvalidStateOrRequest,
                                failure: rejection.operation_failure() };
                            // Foreign refusal/source destruction is this
                            // actual host body's responsibility.
                            drop(rejection);
                            Err(refusal)
                        }
                    }
                } else {
                    Ok(())
                };
                Ok(Some(ParticipationInput::ParticipationUpdateApplied {
                    request: *request,
                    result,
                }))
            }
            (
                Operation::Compare { request, expected },
                Rich::Participation {
                    update,
                    instance,
                    credential,
                },
            ) => {
                let Some(original) = update.as_ref() else {
                    return Err(DomainHostError::WrongPhase);
                };
                let ParticipationDomainUpdate::Credential(update) = &**original else {
                    return Err(DomainHostError::WrongOwner);
                };
                if instance
                    .as_ref()
                    .expect("original Participation instance")
                    .credential_state()
                    != Some(*expected)
                {
                    return Err(DomainHostError::WrongOwner);
                }
                let input = instance
                    .as_mut()
                    .expect("original Participation instance")
                    .credential_on_host(update, self.owner.0.binding.credential_resource()?)
                    .map_err(|_| DomainHostError::WrongPhase)?;
                if input.current_resource != expected.resource
                    || update.credential_domain_id() != expected.credential_domain_id
                {
                    return Err(DomainHostError::WrongOwner);
                }
                *credential = Some(input);
                instance
                    .as_mut()
                    .expect("original Participation instance")
                    .clear_comparison_on_host();
                Ok(Some(ParticipationInput::ParticipationCredentialCompared {
                    request: *request,
                    expected: *expected,
                    result: Ok(input.same_material),
                }))
            }
            (Operation::Dispose, _) => Ok(None),
            _ => Err(DomainHostError::WrongOwner),
        }
    }
    fn retain_unknown(&self, rich: Rich, author: DomainReturn, panic: Option<Box<dyn Any + Send>>) {
        let mut state = self
            .owner
            .0
            .state
            .lock()
            .expect("unresolved original domain source");
        state.rich = Some(rich);
        state.phase = Phase::Unknown;
        state.panic = panic.map(ManuallyDrop::new);
        // Keep the exact author, including a possibly started authority and
        // return sender, in the same finite original rather than retry it.
        state.unknown_author = Some(author);
        drop(state);
        self.owner.0.binding.ready.notify();
    }
}
impl Drop for DomainHostJob {
    fn drop(&mut self) {
        if self.operation.is_some() || self.rich.is_some() || self.author.is_some() {
            if let Some(rich) = self.rich.take() {
                let mut state = self
                    .owner
                    .0
                    .state
                    .lock()
                    .expect("uninvoked original domain job");
                state.rich = Some(rich);
                state.phase = Phase::Unknown;
            }
            if let Some(author) = self.author.take() {
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("uninvoked original domain author")
                    .unknown_author = Some(author);
            }
            self.owner.0.binding.ready.notify();
        }
    }
}

fn dispose_on_host(rich: &mut Rich) {
    match rich {
        Rich::Task { updates, target } => {
            // Stop at the first foreign destructor panic. Remaining captures
            // are independently wrapped, so unwind cannot cascade through them.
            if let Some(updates) = updates.as_mut() {
                while let Some(original) = updates.pop() {
                    drop(original);
                }
            }
            if let Some(empty) = updates.take() {
                drop(ManuallyDrop::into_inner(empty));
            }
            if let Some(source) = target.refused_source.take() {
                drop(ManuallyDrop::into_inner(source));
            }
            if let Some(host) = target.host.take() {
                drop(ManuallyDrop::into_inner(host));
            }
            if let Some(port) = target.port.take() {
                drop(ManuallyDrop::into_inner(port));
            }
            if let Some(descriptor) = target.descriptor.take() {
                drop(ManuallyDrop::into_inner(descriptor));
            }
        }
        Rich::Participation {
            update, instance, ..
        } => {
            if let Some(original) = update.take() {
                drop(ManuallyDrop::into_inner(original));
            }
            if let Some(loan) = instance.as_mut() {
                loan.finish_on_host();
            }
            if let Some(loan) = instance.take() {
                drop(ManuallyDrop::into_inner(loan));
            }
        }
    }
}
