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

//! Actual M/R calls share one prepaid resource custodian. Foreign calls and
//! destructors run only on host workers. An early release waits for the same
//! instance's publication without occupying a cleanup worker or blocking M.

use super::{
    actors::{ControlMessage, ParticipationMessage, WorkerAction, WorkerActor, WorkerMessage},
    control_request::ControlRequestNotice,
    establish_ingress::ValidatedMaterializationSource,
    failure::ScopePublicationsDrained,
    participation_io::ParticipationIoParts,
    participation_ready::ParticipationReadyNotice,
    preparation::{HostFairnessGroup, QueuedPreparation},
    reported_work_admission::adopt_reported_work,
};
use crate::{
    HostRejection, ParticipationHostFactory, ParticipationHostInstance,
    ParticipationMaterializationControl, ReleasedParticipationEvidence,
    task_completion_set::AdmittedCompletionTransfer,
};
use novarocks_control_runtime::{
    actor_turn::{
        ActorRuntime, ActorTurnError, EffectTicket, InputAuthor, InputPhase,
        ReservedInputObligation, StandingInputId, StandingInputObligation,
    },
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    notify::Notify,
    obligation::{WorkObligation, WorkRetirementObserver},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{
        ActionId, ActorId, ConsumerId, Generation, InputId, ReservationId, ScopeId, ScopedWorkRef,
        WorkId,
    },
    mailbox::InputClass,
    scope::ScopeError,
};
use novarocks_execution_contract::{
    CredentialUpdate, ParticipationRef, operation::DomainFailureOutcome,
};
use novarocks_worker_protocol::{
    control::{ControlEffect, ControlInput},
    creation::{AdmissionRefusal, RequestReplyTag},
    participation::{
        ParticipationEffect, ParticipationEstablishment, ParticipationInput,
        ParticipationMaterialized,
    },
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, Weak},
};

type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;
const HOLDER_PANIC_SLOTS: usize = 10;

#[derive(Clone, Debug)]
pub(crate) enum ParticipationHostReturned {
    Materialized(Result<ParticipationMaterialized, AdmissionRefusal>),
    MaterializationWithdrawn,
    Released { normal_close: bool },
    Disposed,
    StartRefused(ExitCustodyError),
    Panicked,
    WorkRefused(crate::ParticipationWorkError),
}
impl std::fmt::Display for ParticipationHostReturned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialized(Ok(facts)) => write!(
                f,
                "Participation host materialized credential resource {}",
                facts.initial_credential_resource
            ),
            Self::Materialized(Err(refusal)) => {
                write!(
                    f,
                    "Participation host materialization was refused: {refusal:?}"
                )
            }
            Self::MaterializationWithdrawn => {
                f.write_str("Participation host materialization was withdrawn")
            }
            Self::Released { normal_close } => write!(
                f,
                "Participation host released execution; normal close observed: {normal_close}"
            ),
            Self::Disposed => f.write_str("Participation host holder was physically disposed"),
            Self::StartRefused(error) => {
                write!(f, "Participation host call could not start: {error}")
            }
            Self::WorkRefused(error) => write!(f, "Participation work was refused: {error}"),
            Self::Panicked => {
                f.write_str("Participation host call panicked; original custody remains unresolved")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ParticipationHostJobError {
    Removal(super::participation_removal::ParticipationRemovalError),
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    Exit(ExitCustodyError),
    Work(crate::ParticipationWorkError),
    WrongOwner,
    WrongPhase,
}
impl std::fmt::Display for ParticipationHostJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Removal(error) => write!(f, "Participation host removal failed: {error:?}"),
            Self::Runtime(error) => write!(f, "Participation host role operation failed: {error}"),
            Self::ActorTurn(error) => {
                write!(
                    f,
                    "Participation host input or effect operation failed: {error}"
                )
            }
            Self::Exit(error) => {
                write!(
                    f,
                    "Participation host physical exit observation failed: {error}"
                )
            }
            Self::Work(error) => write!(f, "Participation work supervision failed: {error}"),
            Self::WrongOwner => {
                f.write_str("Participation host job does not belong to the original owner")
            }
            Self::WrongPhase => {
                f.write_str("Participation host job is in the wrong lifecycle phase")
            }
        }
    }
}
impl std::error::Error for ParticipationHostJobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::Exit(error) => Some(error),
            Self::Work(error) => Some(error),
            Self::Removal(_) | Self::WrongOwner | Self::WrongPhase => None,
        }
    }
}

impl From<RuntimeError> for ParticipationHostJobError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<ActorTurnError> for ParticipationHostJobError {
    fn from(e: ActorTurnError) -> Self {
        Self::ActorTurn(e)
    }
}
impl From<ExitCustodyError> for ParticipationHostJobError {
    fn from(e: ExitCustodyError) -> Self {
        Self::Exit(e)
    }
}

struct Call {
    work: ScopedWorkRef,
    retirement: WorkRetirementObserver,
    exit_custody: ExitCustody,
    report: ReportSlot,
    authority: Option<ExitObligation>,
    sender: Option<OwnedReturnSender<ParticipationHostReturned>>,
    reader: OwnedReturnReader<ParticipationHostReturned>,
    returned: Option<ParticipationHostReturned>,
    input: Option<StandingInputObligation<WorkerActor>>,
    delivered: bool,
    closed: bool,
    core_consumed: bool,
    observer_consumed: bool,
    retired: bool,
}
impl Call {
    fn new(
        original: AdmittedCompletionTransfer,
        input: Option<StandingInputObligation<WorkerActor>>,
    ) -> Self {
        let work = original.work.reference();
        let retirement = original.work.retirement_observer();
        let report = original.report.clone();
        let (sender, reader) = report
            .claim_owned_return()
            .expect("fresh original Participation return");
        let (exit_custody, authority) = ExitCustody::from_admitted(adopt_reported_work(
            original.work,
            original.capacity,
            original.report,
        ));
        Self {
            work,
            retirement,
            exit_custody,
            report,
            authority: Some(authority),
            sender: Some(sender),
            reader,
            returned: None,
            input,
            delivered: false,
            closed: false,
            core_consumed: false,
            observer_consumed: false,
            retired: false,
        }
    }
    fn take(
        &mut self,
    ) -> Result<
        (ExitObligation, OwnedReturnSender<ParticipationHostReturned>),
        ParticipationHostJobError,
    > {
        if self.authority.is_none() || self.sender.is_none() {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        Ok((self.authority.take().unwrap(), self.sender.take().unwrap()))
    }
}

#[derive(Default)]
struct Rich {
    factory: Option<ManuallyDrop<Arc<dyn ParticipationHostFactory>>>,
    instance: Option<ManuallyDrop<Arc<dyn ParticipationHostInstance>>>,
    credential: Option<ManuallyDrop<CredentialUpdate>>,
    rejection: Option<ManuallyDrop<HostRejection>>,
    panics: [Option<PanicPayload>; HOLDER_PANIC_SLOTS],
    roots: Option<super::participation_host::root_read::RootReadDisposal>,
    // One original Node execution owner is retained across physical R and H.
    execution: Option<ManuallyDrop<Arc<dyn crate::TaskExecutionHost>>>,
}
struct State {
    m: Call,
    r: Call,
    h: Call,
    rich: Rich,
    // Same original release fact, already frozen by the Native owner. It
    // survives H without retaining any foreign host or destructor.
    evidence: Option<ReleasedParticipationEvidence>,
    m_issued: bool,
    r_issued: bool,
    h_issued: bool,
    release_requested: bool,
    execution_bound: bool,
    execution_identity: Option<Weak<dyn crate::TaskExecutionHost>>,
    domain_borrowers: usize,
    credential_resource: Option<u64>,
    // Positive outcome of the actual M boundary, not absence in a directory.
    no_instance_created: bool,
    fault: Option<ParticipationHostReturned>,
    creator: Option<Creator>,
    failed_return_cursor: usize,
    failed_retirement_cursor: usize,
}

#[derive(Debug, Default)]
pub(crate) struct FailedHostReturnProgress {
    pub units: usize,
    pub observed: usize,
    pub settled: usize,
    pub waiting: bool,
}
struct Creator {
    request: RequestReplyTag,
    author: InputAuthor,
    notice: ControlRequestNotice,
    permit: Option<ReservedInputObligation<WorkerActor>>,
    input: Option<StandingInputObligation<WorkerActor>>,
    standing_input_id: Option<StandingInputId>,
    handoff: Option<(InputId, ActionId)>,
    retired: bool,
}
struct Custody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    actor: ActorId,
    control: ActorId,
    participation: ParticipationRef,
    generation: Generation,
    establishment: ParticipationEstablishment,
    admission: ReservationId,
    admission_work: ScopedWorkRef,
    pending: Notify,
    notice: ParticipationReadyNotice,
    work_registry: super::participation_host::work_issuer::ParticipationWorkRegistry,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "The original Participation call and capture inventory must remain supervised"]
pub(crate) struct ParticipationHostJobs(Arc<Custody>);
pub(super) struct ParticipationHostJobBinding {
    pub runtime: ActorRuntime<WorkerActor>,
    pub ledger: RuntimeLedger,
    pub actor: ActorId,
    pub control: ActorId,
    pub participation: ParticipationRef,
    pub generation: Generation,
    pub establishment: ParticipationEstablishment,
    pub admission: ReservationId,
    pub admission_work: ScopedWorkRef,
    pub notice: ParticipationReadyNotice,
    pub work_registry: super::participation_host::work_issuer::ParticipationWorkRegistry,
}

#[must_use = "A refusal returns both original opaque captures to their actual caller"]
pub(crate) struct MaterializationCaptureRefusal {
    pub error: ParticipationHostJobError,
    pub _source: ValidatedMaterializationSource,
    pub _factory: Arc<dyn ParticipationHostFactory>,
}
impl std::fmt::Debug for MaterializationCaptureRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializationCaptureRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
enum Operation {
    Materialize(ValidatedMaterializationSource),
    Release(
        Option<ManuallyDrop<Arc<dyn ParticipationHostInstance>>>,
        Option<ManuallyDrop<Arc<dyn crate::TaskExecutionHost>>>,
    ),
    Dispose(Rich),
}
#[must_use = "Return the same original host job on physical queue refusal"]
pub(crate) struct ParticipationHostJob {
    owner: ParticipationHostJobs,
    work: ScopedWorkRef,
    index: usize,
    authority: Option<ExitObligation>,
    sender: Option<OwnedReturnSender<ParticipationHostReturned>>,
    operation: Option<Operation>,
}
impl ParticipationHostJobs {
    pub(super) fn belongs_to(&self, runtime: &ActorRuntime<WorkerActor>, control: ActorId) -> bool {
        self.0.runtime.same_runtime(runtime) && self.0.control == control
    }
    pub(super) fn belongs_to_participation(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        participation: ParticipationRef,
        scope: &ScopeHandle,
    ) -> bool {
        self.0.runtime.same_runtime(runtime)
            && self.0.actor == actor
            && self.0.participation == participation
            && self.0.scope.same_scope(scope)
    }
    /// Observe this same admitted M header, never the Participation lifetime or H.
    pub(super) fn materialization_retirement(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<WorkRetirementObserver, ParticipationHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if !self.matches_materialization(&self.0.runtime, ticket) {
            return Err(ParticipationHostJobError::WrongOwner);
        }
        Ok(self
            .0
            .state
            .lock()
            .expect("original M retirement observer")
            .m
            .retirement
            .clone())
    }
    pub(super) fn materialization_key(&self) -> (ScopeId, WorkId) {
        let work = self.0.state.lock().expect("original M identity").m.work;
        (work.scope, work.work)
    }
    pub(super) fn scope_stopped(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.scope.gates()?.stopped)
    }
    pub(super) fn from_admitted(
        binding: ParticipationHostJobBinding,
        parts: ParticipationIoParts,
    ) -> Self {
        let ParticipationHostJobBinding {
            runtime,
            ledger,
            actor,
            control,
            participation,
            generation,
            establishment,
            admission,
            admission_work,
            notice,
            work_registry,
        } = binding;
        let ParticipationIoParts {
            materialization,
            release,
            holder,
            materialization_input,
            release_input,
        } = parts;
        let scope = holder.work.scope().clone();
        let shared = Arc::new(Custody {
            runtime,
            ledger,
            scope,
            actor,
            control,
            participation,
            generation,
            establishment,
            admission,
            admission_work,
            pending: Notify::default(),
            notice,
            work_registry,
            state: Mutex::new(State {
                m: Call::new(materialization, Some(materialization_input)),
                r: Call::new(release, Some(release_input)),
                h: Call::new(holder, None),
                rich: Rich::default(),
                evidence: None,
                m_issued: false,
                r_issued: false,
                h_issued: false,
                release_requested: false,
                execution_bound: false,
                execution_identity: None,
                domain_borrowers: 0,
                credential_resource: None,
                no_instance_created: false,
                fault: None,
                creator: None,
                failed_return_cursor: 0,
                failed_retirement_cursor: 0,
            }),
            retained: Mutex::new(None),
        });
        *shared
            .retained
            .lock()
            .expect("Participation original retention") = Some(shared.clone());
        Self(shared)
    }
    pub(crate) fn capture_materialization(
        &self,
        source: ValidatedMaterializationSource,
        factory: Arc<dyn ParticipationHostFactory>,
    ) -> Result<ParticipationHostJob, MaterializationCaptureRefusal> {
        if !source.matches(
            &self.0.runtime,
            self.0.control,
            self.0.participation,
            self.0.generation,
            self.0.establishment,
        ) {
            return Err(MaterializationCaptureRefusal {
                error: ParticipationHostJobError::WrongOwner,
                _source: source,
                _factory: factory,
            });
        }
        let mut state = self
            .0
            .state
            .lock()
            .expect("Participation actual host inventory");
        if state
            .creator
            .as_ref()
            .is_none_or(|c| c.input.is_none() || c.request != source.request())
        {
            return Err(MaterializationCaptureRefusal {
                error: ParticipationHostJobError::WrongPhase,
                _source: source,
                _factory: factory,
            });
        }
        if state.m_issued {
            return Err(MaterializationCaptureRefusal {
                error: ParticipationHostJobError::WrongPhase,
                _source: source,
                _factory: factory,
            });
        }
        let (authority, sender) = match state.m.take() {
            Ok(x) => x,
            Err(error) => {
                return Err(MaterializationCaptureRefusal {
                    error,
                    _source: source,
                    _factory: factory,
                });
            }
        };
        state.m_issued = true;
        state.rich.factory = Some(ManuallyDrop::new(factory));
        Ok(ParticipationHostJob {
            owner: self.clone(),
            work: state.m.work,
            index: 0,
            authority: Some(authority),
            sender: Some(sender),
            operation: Some(Operation::Materialize(source)),
        })
    }
    pub(super) fn bind_creator(
        &self,
        ticket: &EffectTicket<WorkerAction>,
        request: RequestReplyTag,
        original_author: InputAuthor,
        author: &WorkObligation,
        notice: ControlRequestNotice,
    ) -> Result<StandingInputId, ParticipationHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.0.control
            || ticket.input != request.token
            || request.operation != self.0.establishment.operation
            || original_author.actor() != self.0.control
            || request.author != original_author.work()
            || !self.matches_materialization(&self.0.runtime, ticket)
        {
            return Err(ParticipationHostJobError::WrongOwner);
        }
        let mut s = self
            .0
            .state
            .lock()
            .expect("original creator return binding");
        if s.creator.is_some() || s.m_issued {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        s.creator = Some(Creator {
            request,
            author: original_author,
            notice,
            permit: None,
            input: None,
            standing_input_id: None,
            handoff: None,
            retired: false,
        });
        let c = s.creator.as_mut().unwrap();
        c.permit = Some(self.0.runtime.reserve_admitted_input(
            self.0.control,
            self.0.actor,
            InputClass::Control,
            author,
        )?);
        match c.permit.take().unwrap().into_standing_input() {
            Ok(input) => {
                c.standing_input_id = Some(input.id());
                c.input = Some(input);
            }
            Err((error, permit)) => {
                c.permit = Some(permit);
                return Err(error.into());
            }
        }
        Ok(c.standing_input_id.unwrap())
    }
    pub(super) fn creator_input_id(&self) -> Option<StandingInputId> {
        self.0
            .state
            .lock()
            .expect("creator original return input")
            .creator
            .as_ref()
            .and_then(|c| c.standing_input_id)
    }
    /// Qualify the actual Root Participation admission, not merely an equal wire
    /// identity or a snapshot of the establishment's neutral metadata.
    pub(super) fn matches_materialization(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> bool {
        self.0.runtime.same_runtime(runtime)
            && ticket.actor == self.0.control
            && ticket.descriptor.work == self.0.admission_work
            && matches!(
                &ticket.effect,
                WorkerAction::Control {
                    effect: ControlEffect::Materialize {
                        participation, establishment, generation, reservation,
                    },
                    admitted: Some(original),
                } if *participation == self.0.participation
                    && *establishment == self.0.establishment
                    && *generation == self.0.generation
                    && *reservation == self.0.admission
                    && original == reservation
            )
    }
    pub(crate) fn source_consumed(&self) -> Result<bool, ParticipationHostJobError> {
        let s = self.0.state.lock().expect("original M source consumption");
        Ok(s.m_issued && s.m.returned.is_some() && s.m.exit_custody.actual_exit_ready()?)
    }
    /// The actual Participation Core decision has reached its original creator
    /// backchannel. An M body return alone supplies no such decision fact.
    pub(crate) fn actual_establishment_decided(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original establishment decision receipt")
            .creator
            .as_ref()
            .is_some_and(|creator| creator.handoff.is_some())
    }
    /// Forward only the verdict of the actual original Participation M input. This
    /// neither reruns host work nor manufactures a second external request.
    pub(super) fn forward_establishment(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ParticipationHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        let mut s = self.0.state.lock().expect("creator Core decision handoff");
        let WorkerAction::Participation(ParticipationEffect::EstablishmentDecided {
            participation,
            decision,
            ..
        }) = &ticket.effect
        else {
            return Err(ParticipationHostJobError::WrongOwner);
        };
        if ticket.actor != self.0.actor
            || *participation != self.0.participation
            || ticket.descriptor.work != s.m.work
            || s.m
                .input
                .as_ref()
                .ok_or(ParticipationHostJobError::WrongPhase)?
                .in_flight()?
                != Some(ticket.input)
        {
            return Err(ParticipationHostJobError::WrongOwner);
        }
        let c = s
            .creator
            .as_mut()
            .ok_or(ParticipationHostJobError::WrongPhase)?;
        let key = (ticket.input, ticket.descriptor.id);
        if c.handoff == Some(key) {
            return Ok(());
        }
        if c.handoff.is_some() {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        let message = WorkerMessage::Control(ControlMessage::Command {
            work: c.author.work(),
            request: None,
            command: ControlInput::EstablishmentDecided {
                participation: *participation,
                decision: decision.clone(),
            },
        });
        self.0
            .runtime
            .fill_from_effect_with_author(
                ticket,
                c.input
                    .as_ref()
                    .ok_or(ParticipationHostJobError::WrongPhase)?,
                &c.author,
                message,
            )
            .map_err(|(e, _)| ParticipationHostJobError::ActorTurn(e))?;
        c.handoff = Some(key);
        c.notice.notify();
        Ok(())
    }
    pub(super) fn retire_creator(&self) -> Result<bool, ParticipationHostJobError> {
        let mut s = self
            .0
            .state
            .lock()
            .expect("original creator return retirement");
        if !s.m_issued || s.m.returned.is_none() || !s.m.exit_custody.actual_exit_ready()? {
            return Ok(false);
        }
        let c = s
            .creator
            .as_mut()
            .ok_or(ParticipationHostJobError::WrongPhase)?;
        if c.retired {
            return Ok(true);
        }
        if c.handoff.is_none() {
            return Ok(false);
        }
        if let Some(input) = c.input.as_mut() {
            if let Some(id) = input.in_flight()? {
                match self.0.runtime.retire_input(self.0.control, id) {
                    Ok(()) => {}
                    Err(
                        ActorTurnError::InputStillQueued
                        | ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain,
                    ) => return Ok(false),
                    Err(e) => return Err(e.into()),
                }
            }
            input.close_after_exit()?;
            c.input = None;
        }
        c.retired = true;
        Ok(true)
    }
    /// Bind the same Node composition once, before R or H can capture it. None
    /// explicitly denotes a cold Node with Task intake unavailable, never a
    /// missing Task or an inferred normal-close result.
    pub(crate) fn bind_execution_host(
        &self,
        host: Option<&Arc<dyn crate::TaskExecutionHost>>,
    ) -> Result<(), ParticipationHostJobError> {
        let mut state = self
            .0
            .state
            .lock()
            .expect("original Participation execution binding");
        if state.execution_bound {
            let same = match (state.execution_identity.as_ref(), host) {
                (Some(original), Some(host)) => Weak::ptr_eq(original, &Arc::downgrade(host)),
                (None, None) => true,
                _ => false,
            };
            return if same {
                Ok(())
            } else {
                Err(ParticipationHostJobError::WrongOwner)
            };
        }
        if state.r_issued || state.h_issued {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        state.execution_identity = host.map(Arc::downgrade);
        state.rich.execution = host.map(|host| ManuallyDrop::new(Arc::clone(host)));
        state.execution_bound = true;
        Ok(())
    }
    /// R can be requested before factory publication. Only the original
    /// pending bit is set; no worker is occupied waiting for a resource.
    pub(super) fn request_release(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ParticipationHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.0.actor
            || !matches!(&ticket.effect,
                WorkerAction::Participation(ParticipationEffect::ReleaseHost { participation, .. })
                    if *participation == self.0.participation)
        {
            return Err(ParticipationHostJobError::WrongOwner);
        }
        self.0
            .state
            .lock()
            .expect("Participation release request")
            .release_requested = true;
        self.wake();
        Ok(())
    }
    /// The original failed-owner stage replaces only the lost Core's release
    /// instruction. The caller still closes actual Task/Root borrowers before
    /// taking R; its original authority and physical return remain mandatory.
    pub(crate) fn request_failed_release(
        &self,
        stage: &ScopePublicationsDrained<'_>,
    ) -> Result<(), ParticipationHostJobError> {
        stage.validate(&self.0.runtime, self.0.actor)?;
        let changed = {
            let mut state = self
                .0
                .state
                .lock()
                .expect("failed Participation release request");
            let changed = !state.release_requested;
            state.release_requested = true;
            changed
        };
        if changed {
            self.wake();
        }
        Ok(())
    }
    /// Whether this original R has accepted an instruction but has not yet
    /// transferred its body to the physical executor.
    pub(super) fn release_pending(&self) -> bool {
        let state = self
            .0
            .state
            .lock()
            .expect("Participation release instruction");
        state.release_requested && !state.r_issued
    }
    pub(crate) fn take_release_job(
        &self,
    ) -> Result<Option<ParticipationHostJob>, ParticipationHostJobError> {
        let mut state = self
            .0
            .state
            .lock()
            .expect("Participation release inventory");
        if !state.release_requested || state.r_issued || state.domain_borrowers != 0 {
            return Ok(None);
        }
        if !state.execution_bound {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        // M publishes its instance before materialize() runs. Presence is
        // not its exit: R must never overlap that actual foreign body/capture.
        if state.m.returned.is_none() || !state.m.exit_custody.actual_exit_ready()? {
            return Ok(None);
        }
        let instance = state
            .rich
            .instance
            .as_ref()
            .map(|v| ManuallyDrop::new(Arc::clone(v)));
        if instance.is_none() && !state.no_instance_created {
            return Ok(None);
        }
        let (authority, sender) = state.r.take()?;
        state.r_issued = true;
        Ok(Some(ParticipationHostJob {
            owner: self.clone(),
            work: state.r.work,
            index: 1,
            authority: Some(authority),
            sender: Some(sender),
            operation: Some(Operation::Release(
                instance,
                state
                    .rich
                    .execution
                    .as_ref()
                    .map(|host| ManuallyDrop::new(Arc::clone(host))),
            )),
        }))
    }
    /// These monotonic instructions are the actual reason a new comparison
    /// cannot borrow H. The Core must decide again after this real stand-down.
    pub(super) fn replay_comparison_retiring(&self) -> bool {
        let state = self.0.state.lock().expect("original comparison stand-down");
        state.release_requested || state.r_issued || state.h_issued
    }
    /// Borrow the original materialized instance before any foreign domain call.
    /// The caller's already admitted R retains this loan until its real host
    /// captures have exited. Release cannot pass an outstanding loan.
    pub(crate) fn borrow_domain_instance(
        &self,
    ) -> Result<ParticipationDomainInstance, ParticipationHostJobError> {
        let mut state = self
            .0
            .state
            .lock()
            .expect("original Participation domain loan");
        if state.release_requested || state.r_issued || state.h_issued {
            return Err(ParticipationHostJobError::WrongPhase);
        }
        let Some(instance) = state.rich.instance.as_ref() else {
            return Err(ParticipationHostJobError::WrongPhase);
        };
        let instance = Arc::clone(instance);
        state.domain_borrowers = state
            .domain_borrowers
            .checked_add(1)
            .unwrap_or_else(|| std::process::abort());
        Ok(ParticipationDomainInstance {
            owner: self.clone(),
            instance: Some(ManuallyDrop::new(instance)),
            comparison: None,
            returned: false,
        })
    }
    /// Borrow only evidence actually returned by this same instance's Release.
    /// Absence is not an empty release observation or another release call.
    /// Only the neutral observation of this original immutable Release return.
    /// No opaque evidence handle, content getter or callback leaves this lock.
    pub(crate) fn released_observation(&self) -> Option<crate::RuntimeFilterReleaseObservation> {
        self.0
            .state
            .lock()
            .expect("original Participation release evidence")
            .evidence
            .as_ref()
            .map(|original| original.runtime_filter_observation())
    }
    pub(crate) fn released_evidence(&self) -> Option<ReleasedParticipationEvidence> {
        self.0
            .state
            .lock()
            .expect("original Participation release evidence")
            .evidence
            .clone()
    }
    pub(crate) fn fault(&self) -> Option<ParticipationHostReturned> {
        self.0
            .state
            .lock()
            .expect("Participation host fault")
            .fault
            .clone()
    }
    fn wake(&self) {
        self.0.pending.notify();
        self.0.notice.notify();
    }

    /// Observe the original M/R callbacks without invoking their host or exit gates.
    pub(super) fn try_execution_callbacks_quiescent(&self) -> Option<bool> {
        let state = self.0.state.try_lock().ok()?;
        // Retired includes the actual role/report/input/header tail. A missing
        // call or an unknown/panicking return never supplies this positive fact.
        Some(
            state.fault.is_none()
                && state.m.retired
                && state.r.retired
                && state.h.retired
                && matches!(
                    state.r.returned,
                    Some(ParticipationHostReturned::Released { .. })
                )
                && matches!(state.h.returned, Some(ParticipationHostReturned::Disposed)),
        )
    }
    /// H owns the moved Root inventory until its actual disposal/report tail ends.
    /// The empty Participation map during that interval is not a zero Root census.
    pub(super) fn try_root_census_ready(&self) -> bool {
        let Ok(state) = self.0.state.try_lock() else {
            return false;
        };
        !state.h_issued
            || (state.h.retired
                && matches!(state.h.returned, Some(ParticipationHostReturned::Disposed)))
    }
    pub(super) fn holder_disposed(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        participation: ParticipationRef,
        child: &ScopeHandle,
    ) -> Result<bool, ParticipationHostJobError> {
        if !self.0.runtime.same_runtime(runtime)
            || self.0.actor != actor
            || self.0.participation != participation
            || self.0.scope.id() != child.id()
        {
            return Err(ParticipationHostJobError::WrongOwner);
        }
        let state = self
            .0
            .state
            .lock()
            .expect("original Participation holder exit");
        Ok(
            matches!(state.h.returned, Some(ParticipationHostReturned::Disposed))
                && state.h.exit_custody.actual_exit_ready()?,
        )
    }

    /// Only these already-paid inputs may carry normal M/R outcomes. Their
    /// author positions were reserved before the first foreign capture.
    pub(crate) fn poll_returns(&self) -> Result<usize, ParticipationHostJobError> {
        let mut state = self.0.state.lock().expect("Participation original returns");
        let mut delivered = 0;
        let mut observed = false;
        for index in 0..3 {
            let call = match index {
                0 => &mut state.m,
                1 => &mut state.r,
                _ => &mut state.h,
            };
            if call.returned.is_none() {
                call.returned = call.reader.try_take()?;
                observed |= call.returned.is_some();
            }
            if call.delivered {
                continue;
            }
            let Some(outcome) = call.returned.clone() else {
                continue;
            };
            let command = match outcome {
                ParticipationHostReturned::Materialized(result) => {
                    Some(ParticipationInput::MaterializationFinished { result })
                }
                ParticipationHostReturned::MaterializationWithdrawn => {
                    Some(ParticipationInput::MaterializationWithdrawn)
                }
                ParticipationHostReturned::Released { .. } => {
                    Some(ParticipationInput::HostReleased)
                }
                ParticipationHostReturned::Disposed => None,
                fault => {
                    state.fault = Some(fault);
                    continue;
                }
            };
            if let Some(command) = command {
                call.input
                    .as_ref()
                    .expect("original M/R callback")
                    .fill(WorkerMessage::Participation(
                        ParticipationMessage::Command {
                            work: call.work,
                            request: None,
                            command,
                        },
                    ))
                    .map_err(|(e, _)| ParticipationHostJobError::ActorTurn(e))?;
            }
            call.delivered = true;
            delivered += 1;
        }
        if observed && let Some(c) = &state.creator {
            c.notice.notify();
        }
        Ok(delivered)
    }
    /// Recover the original M/R returns without filling a dead Core. A return
    /// settles only its Core consumer: the real role, capture, observer and
    /// header must independently exit before callback retirement can finish.
    pub(crate) fn poll_failed_returns(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        budget: usize,
    ) -> Result<FailedHostReturnProgress, ParticipationHostJobError> {
        stage.validate(&self.0.runtime, self.0.actor)?;
        let mut state = self
            .0
            .state
            .lock()
            .expect("failed Participation original returns");
        let mut progress = FailedHostReturnProgress::default();
        for _ in 0..budget.min(3) {
            let index = state.failed_return_cursor;
            state.failed_return_cursor = (index + 1) % 3;
            progress.units += 1;
            let call = match index {
                0 => &mut state.m,
                1 => &mut state.r,
                _ => &mut state.h,
            };
            if call.returned.is_none() {
                call.returned = call.reader.try_take()?;
                progress.observed += usize::from(call.returned.is_some());
            }
            let Some(outcome) = &call.returned else {
                progress.waiting |= !call.retired;
                continue;
            };
            if matches!(
                outcome,
                ParticipationHostReturned::Panicked | ParticipationHostReturned::StartRefused(_)
            ) {
                state.fault = Some(outcome.clone());
                progress.waiting = true;
                continue;
            }
            if call.delivered && (index == 2 || call.core_consumed) {
                continue;
            }
            if index != 2 && !call.core_consumed {
                let consumer = ConsumerId::new(1).expect("original Participation Core consumer");
                if call.report.consumption(consumer)?
                    == novarocks_control_runtime_core::completion::Consumption::Pending
                {
                    let input = call
                        .input
                        .as_ref()
                        .ok_or(ParticipationHostJobError::WrongPhase)?;
                    match input.in_flight()? {
                        Some(input) => {
                            let receipt = self
                                .0
                                .runtime
                                .input_custody(self.0.actor, input)?
                                .ok_or(ParticipationHostJobError::WrongPhase)?;
                            match receipt.phase {
                                InputPhase::Committed => call.report.consume(consumer)?,
                                InputPhase::AbandonedByOwnerFailure => {
                                    call.report.abandon_failed_owner(
                                        consumer,
                                        &self.0.scope,
                                        self.0.generation,
                                    )?
                                }
                                _ => {
                                    progress.waiting = true;
                                    continue;
                                }
                            }
                        }
                        None if !call.delivered => call.report.abandon_failed_owner(
                            consumer,
                            &self.0.scope,
                            self.0.generation,
                        )?,
                        None => return Err(ParticipationHostJobError::WrongPhase),
                    }
                }
                call.core_consumed = true;
            }
            // This records the original return's disposition, not body or
            // capture exit and not successful execution by the failed Core.
            call.delivered = true;
            progress.settled += 1;
        }
        drop(state);
        if progress.observed != 0 || progress.settled != 0 {
            self.wake();
        }
        Ok(progress)
    }
    /// Visit at most two original callbacks in round-robin order. Each visit
    /// reaps at most one real ready header; waiting holders never self-wake.
    pub(crate) fn retire_failed_callbacks(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        budget: usize,
    ) -> Result<bool, ParticipationHostJobError> {
        stage.validate(&self.0.runtime, self.0.actor)?;
        let mut state = self
            .0
            .state
            .lock()
            .expect("failed Participation callback retirement");
        let m_was_retired = state.m.retired;
        let mut progressed = false;
        for _ in 0..budget.min(2) {
            let index = state.failed_retirement_cursor;
            state.failed_retirement_cursor = (index + 1) % 2;
            let call = if index == 0 {
                &mut state.m
            } else {
                &mut state.r
            };
            if call.retired
                || !call.delivered
                || !call.core_consumed
                || !matches!(
                    call.exit_custody.phase(),
                    ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
                )
            {
                continue;
            }
            if !call.closed {
                let input = call
                    .input
                    .as_mut()
                    .ok_or(ParticipationHostJobError::WrongPhase)?;
                if let Some(id) = input.in_flight()? {
                    match self.0.runtime.retire_input(self.0.actor, id) {
                        Ok(()) => {}
                        Err(
                            ActorTurnError::InputStillQueued
                            | ActorTurnError::EffectsRemain
                            | ActorTurnError::ReservationsRemain,
                        ) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                input.close_after_exit()?;
                call.closed = true;
                progressed = true;
            }
            if !call.observer_consumed {
                call.report
                    .consume(ConsumerId::new(2).expect("original Participation host consumer"))?;
                call.observer_consumed = true;
                progressed = true;
            }
            let phase = call.exit_custody.phase();
            if phase != ExitCustodyPhase::Retired && !call.exit_custody.try_retire()? {
                continue;
            }
            progressed |= phase != call.exit_custody.phase();
            if !call.retirement.is_retired() {
                progressed |= self.0.ledger.reap_scope_ready(self.0.scope.id(), 1)? != 0;
            }
            call.retired = call.retirement.is_retired();
        }
        let retired = state.m.retired && state.r.retired;
        let notice = (!m_was_retired && state.m.retired)
            .then(|| state.creator.as_ref().map(|c| c.notice.clone()))
            .flatten();
        drop(state);
        if let Some(notice) = notice {
            notice.notify();
        }
        if progressed {
            self.wake();
        }
        Ok(retired)
    }
    fn retire_call(&self, call: &mut Call, core: bool) -> Result<bool, ParticipationHostJobError> {
        if call.retired {
            return Ok(true);
        }
        if !call.delivered
            || !matches!(
                call.exit_custody.phase(),
                ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
            )
        {
            return Ok(false);
        }
        if !call.closed
            && let Some(input) = &mut call.input
        {
            if let Some(id) = input.in_flight()? {
                match self.0.runtime.retire_input(self.0.actor, id) {
                    Ok(()) => {}
                    Err(
                        ActorTurnError::InputStillQueued
                        | ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain,
                    ) => return Ok(false),
                    Err(e) => return Err(e.into()),
                }
            }
            input.close_after_exit()?;
            call.closed = true;
        }
        if core && !call.core_consumed {
            call.report.consume(ConsumerId::new(1).unwrap())?;
            call.core_consumed = true;
        }
        if !call.observer_consumed {
            call.report.consume(ConsumerId::new(2).unwrap())?;
            call.observer_consumed = true;
        }
        if call.exit_custody.phase() != ExitCustodyPhase::Retired
            && !call.exit_custody.try_retire()?
        {
            return Ok(false);
        }
        if !call.retirement.is_retired() {
            self.0.ledger.reap_scope_ready(self.0.scope.id(), 3)?;
        }
        call.retired = call.retirement.is_retired();
        if !call.retired {
            self.wake();
        }
        Ok(call.retired)
    }
    pub(crate) fn retire_callbacks(&self) -> Result<bool, ParticipationHostJobError> {
        let mut s = self
            .0
            .state
            .lock()
            .expect("Participation callback retirement");
        let was_retired = s.m.retired;
        let m = self.retire_call(&mut s.m, true)?;
        let r = self.retire_call(&mut s.r, true)?;
        let notice = (!was_retired && m)
            .then(|| s.creator.as_ref().map(|c| c.notice.clone()))
            .flatten();
        drop(s);
        if let Some(notice) = notice {
            notice.notify();
        }
        Ok(m && r)
    }
    pub(super) fn take_holder_disposal_with_roots(
        &self,
        roots: &mut super::participation_host::root_read::RootReadState,
    ) -> Result<Option<ParticipationHostJob>, ParticipationHostJobError> {
        self.take_holder_disposal_inner(Some(roots))
    }
    fn take_holder_disposal_inner(
        &self,
        roots: Option<&mut super::participation_host::root_read::RootReadState>,
    ) -> Result<Option<ParticipationHostJob>, ParticipationHostJobError> {
        let mut s = self.0.state.lock().expect("Participation holder disposal");
        if !s.release_requested || !s.m.retired || !s.r.retired || s.h_issued {
            return Ok(None);
        }
        if roots.as_ref().is_some_and(|roots| !roots.readers_empty()) {
            return Ok(None);
        }
        let (authority, sender) = s.h.take()?;
        if let Some(roots) = roots {
            assert!(s.rich.roots.is_none());
            s.rich.roots = roots.take_for_physical_disposal();
            assert!(s.rich.roots.is_some());
        }
        s.h_issued = true;
        let rich = std::mem::take(&mut s.rich);
        Ok(Some(ParticipationHostJob {
            owner: self.clone(),
            work: s.h.work,
            index: 2,
            authority: Some(authority),
            sender: Some(sender),
            operation: Some(Operation::Dispose(rich)),
        }))
    }
    pub(crate) fn retire_holder(&self) -> Result<bool, ParticipationHostJobError> {
        let mut s = self
            .0
            .state
            .lock()
            .expect("Participation holder retirement");
        if !self.retire_call(&mut s.h, false)? {
            return Ok(false);
        }
        drop(s);
        drop(
            self.0
                .retained
                .lock()
                .expect("Participation original retention")
                .take(),
        );
        Ok(true)
    }
}
impl QueuedPreparation for ParticipationHostJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.work.scope, self.work.work)
    }
    fn fairness_group(&self, _: ScopeId) -> HostFairnessGroup {
        HostFairnessGroup::Participation(self.owner.0.participation)
    }
}
impl ParticipationMaterializationControl for ParticipationHostJobs {
    fn is_stopped(&self) -> bool {
        self.0
            .scope
            .gates()
            .expect("the original materialization retains its admitted Scope")
            .stopped
    }
}
impl ParticipationHostJob {
    pub(super) fn belongs_to_jobs(&self, original: &ParticipationHostJobs) -> bool {
        Arc::ptr_eq(&self.owner.0, &original.0)
    }
    pub(super) fn belongs_to(&self, runtime: &ActorRuntime<WorkerActor>, control: ActorId) -> bool {
        self.owner.belongs_to(runtime, control)
    }
    pub(super) fn is_materialization(&self) -> bool {
        self.index == 0
    }
    pub(super) fn scope_stopped(&self) -> Result<bool, RuntimeError> {
        self.owner.scope_stopped()
    }
    fn materialize(&self, source: &ValidatedMaterializationSource) -> ParticipationHostReturned {
        let factory = {
            let s = self
                .owner
                .0
                .state
                .lock()
                .expect("Participation factory loan");
            Arc::clone(s.rich.factory.as_ref().expect("original host factory"))
        };
        let grant = match self.owner.0.work_registry.issue_instance() {
            Ok(grant) => grant,
            Err(crate::ParticipationWorkError::Runtime(RuntimeError::Scope(
                ScopeError::Stopped,
            ))) => {
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("original M no-instance fact")
                    .no_instance_created = true;
                return ParticipationHostReturned::MaterializationWithdrawn;
            }
            Err(error) => return ParticipationHostReturned::WorkRefused(error),
        };
        let instance = match factory.create_instance(self.owner.0.participation, grant) {
            Ok(instance) => instance,
            Err(rejection) => {
                let failure = rejection.operation_failure();
                let mut s = self
                    .owner
                    .0
                    .state
                    .lock()
                    .expect("Participation factory refusal");
                s.no_instance_created = true;
                s.rich.rejection = Some(ManuallyDrop::new(rejection));
                return ParticipationHostReturned::Materialized(Err(AdmissionRefusal::Domain(
                    DomainFailureOutcome::InvalidStateOrRequest,
                    failure,
                )));
            }
        };
        let request = source.shared_facts();
        let credential = request.initial_credential();
        let canonical = CredentialUpdate::new(
            credential.credential_domain_id(),
            credential.epoch(),
            Arc::clone(credential.material()),
        );
        let (closing, resource) = {
            let mut s = self
                .owner
                .0
                .state
                .lock()
                .expect("Participation original instance publication");
            s.rich.instance = Some(ManuallyDrop::new(instance.clone()));
            s.rich.credential = Some(ManuallyDrop::new(canonical));
            s.credential_resource = Some(s.h.work.work.get());
            (s.release_requested, s.h.work.work.get())
        };
        self.owner.wake();
        if closing {
            return ParticipationHostReturned::MaterializationWithdrawn;
        }
        match instance.materialize(request, &self.owner) {
            Ok(()) => ParticipationHostReturned::Materialized(Ok(ParticipationMaterialized {
                initial_credential_resource: resource,
            })),
            Err(rejection) => {
                let failure = rejection.operation_failure();
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("Participation materialization rejection")
                    .rich
                    .rejection = Some(ManuallyDrop::new(rejection));
                ParticipationHostReturned::Materialized(Err(AdmissionRefusal::Domain(
                    DomainFailureOutcome::InvalidStateOrRequest,
                    failure,
                )))
            }
        }
    }
    fn release(
        &self,
        instance: &Option<ManuallyDrop<Arc<dyn ParticipationHostInstance>>>,
        execution: &Option<ManuallyDrop<Arc<dyn crate::TaskExecutionHost>>>,
    ) -> ParticipationHostReturned {
        let evidence = instance
            .as_ref()
            .map_or_else(ReleasedParticipationEvidence::none, |instance| {
                instance.release()
            });
        {
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("Participation original release evidence");
            assert!(state.evidence.is_none(), "one original frozen release fact");
            state.evidence = Some(evidence);
        }
        // The same physical invocation observes the original capability bank
        // only after all Task retirement operations and this real Release.
        let normal_close = execution
            .as_ref()
            .is_some_and(|host| host.retains_normal_close(self.owner.0.participation));
        ParticipationHostReturned::Released { normal_close }
    }
    fn dispose(&self, rich: Rich) -> ParticipationHostReturned {
        // H drops only this original hot holder after M/R actually retired.
        // Inbound normal-close truth remains until the original Core's expiry
        // or real process closure; hot-holder exit does not forget it.
        let Rich {
            factory,
            instance,
            credential,
            rejection,
            panics,
            roots,
            execution,
        } = rich;
        let mut failures: [Option<PanicPayload>; HOLDER_PANIC_SLOTS] = Default::default();
        let mut next = 0;
        let mut observe = |result: std::thread::Result<()>| {
            if let Err(p) = result {
                failures[next] = Some(ManuallyDrop::new(p));
                next += 1;
            }
        };
        observe(catch_unwind(AssertUnwindSafe(|| {
            drop(factory.map(ManuallyDrop::into_inner))
        })));
        observe(catch_unwind(AssertUnwindSafe(|| {
            drop(instance.map(ManuallyDrop::into_inner))
        })));
        observe(catch_unwind(AssertUnwindSafe(|| {
            drop(credential.map(ManuallyDrop::into_inner))
        })));
        observe(catch_unwind(AssertUnwindSafe(|| {
            drop(rejection.map(ManuallyDrop::into_inner))
        })));
        observe(catch_unwind(AssertUnwindSafe(|| {
            if let Some(roots) = roots {
                roots.dispose();
            }
        })));
        observe(catch_unwind(AssertUnwindSafe(|| {
            drop(execution.map(ManuallyDrop::into_inner))
        })));
        for p in panics.into_iter().flatten() {
            observe(catch_unwind(AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(p))
            })));
        }
        if next != 0 {
            let mut s = self
                .owner
                .0
                .state
                .lock()
                .expect("Participation destructor panic retention");
            s.rich.panics = failures;
            ParticipationHostReturned::Panicked
        } else {
            ParticipationHostReturned::Disposed
        }
    }
    pub(crate) fn run_on_host(mut self) {
        let mut authority = self
            .authority
            .take()
            .expect("original Participation job authority");
        let operation = self
            .operation
            .take()
            .expect("original Participation host operation");
        let (cancelled, started) = if self.index == 0 {
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("Participation M start fence");
            if state.release_requested {
                state.no_instance_created = true;
                (true, Ok(()))
            } else {
                let started = authority.start();
                let stopped_before_start = matches!(
                    started,
                    Err(ExitCustodyError::Runtime(RuntimeError::Scope(
                        ScopeError::Stopped
                    )))
                ) && self.owner.0.scope.gates().is_ok_and(|gates| {
                    gates.stopped && !gates.failed && gates.generation == self.owner.0.generation
                });
                if stopped_before_start {
                    // The committed healthy Scope gate won before this
                    // original business role began. No factory was called;
                    // return the actual source captures through M's normal
                    // withdrawal fact rather than inventing an owner fault.
                    state.no_instance_created = true;
                    (true, Ok(()))
                } else {
                    (false, started)
                }
            }
        } else {
            (false, authority.start())
        };
        let result = if cancelled {
            Ok(ParticipationHostReturned::MaterializationWithdrawn)
        } else {
            match started {
                Err(e) => {
                    if self.index == 0 {
                        self.owner
                            .0
                            .state
                            .lock()
                            .expect("unstarted Participation M")
                            .no_instance_created = true;
                    }
                    Ok(ParticipationHostReturned::StartRefused(e))
                }
                Ok(()) if matches!(&operation, Operation::Dispose(_)) => {
                    Ok(ParticipationHostReturned::Disposed)
                }
                Ok(()) => catch_unwind(AssertUnwindSafe(|| match &operation {
                    Operation::Materialize(source) => self.materialize(source),
                    Operation::Release(instance, execution) => self.release(instance, execution),
                    Operation::Dispose(_) => {
                        unreachable!("holder disposal moves its original fields")
                    }
                })),
            }
        };
        // The actual source loan and temporary instance clone return on this
        // host, including after unwind. No raw field is destroyed on a poller.
        let outcome = match (operation, result) {
            (Operation::Dispose(rich), _) if started.is_ok() => self.dispose(rich),
            (Operation::Dispose(rich), Ok(outcome)) => {
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("unstarted Participation holder")
                    .rich = rich;
                outcome
            }
            (operation, Ok(outcome)) => {
                Self::return_captures(operation);
                outcome
            }
            (operation, Err(p)) => {
                Self::return_captures(operation);
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("Participation host panic retention")
                    .rich
                    .panics[self.index] = Some(ManuallyDrop::new(p));
                ParticipationHostReturned::Panicked
            }
        };
        if self.index == 2 && !matches!(outcome, ParticipationHostReturned::Disposed) {
            // At least one opaque destructor did not complete. This original
            // holder remains Unknown; a panic receipt is not capture exit.
            drop(authority);
        } else if let Err(e) = authority.exited() {
            std::mem::forget((e, outcome, self.sender.take()));
            std::process::abort();
        }
        let sender = self
            .sender
            .take()
            .expect("original Participation owned return");
        if let Err(refusal) = sender.publish(outcome) {
            std::mem::forget(refusal);
            std::process::abort();
        }
        self.owner.wake();
    }
    fn return_captures(operation: Operation) {
        match operation {
            Operation::Release(instance, execution) => {
                drop(instance.map(ManuallyDrop::into_inner));
                drop(execution.map(ManuallyDrop::into_inner));
            }
            Operation::Materialize(source) => drop(source),
            Operation::Dispose(_) => unreachable!("holder captures use their own cleanup boundary"),
        }
    }
}
impl Drop for ParticipationHostJob {
    fn drop(&mut self) {
        if self.operation.is_some() || self.sender.is_some() || self.authority.is_some() {
            self.owner
                .0
                .state
                .lock()
                .expect("lost Participation host job")
                .fault = Some(ParticipationHostReturned::Panicked);
            self.owner.wake();
        }
    }
}

/// An actual instance capture, funded and retained by one original domain R.
/// Foreign getters, calls and destruction belong exclusively to a host body.
/// Dropping an unreturned loan retains the physical captures and keeps R/H
/// blocked; it never supplies a positive borrower-exit observation.
pub(crate) struct ParticipationDomainInstance {
    owner: ParticipationHostJobs,
    instance: Option<ManuallyDrop<Arc<dyn ParticipationHostInstance>>>,
    comparison: Option<ManuallyDrop<CredentialUpdate>>,
    returned: bool,
}
impl ParticipationDomainInstance {
    pub(crate) fn matches_owner(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        scope: &ScopeHandle,
        participation: ParticipationRef,
    ) -> bool {
        self.owner.0.runtime.same_runtime(runtime)
            && self.owner.0.scope.same_scope(scope)
            && self.owner.0.participation == participation
            && !self.returned
    }
    /// Capture credential identity, material and resource in one original
    /// owner transaction. Last-reference destruction stays on this host.
    fn capture_credential_on_host(&mut self) -> Result<u64, ParticipationHostJobError> {
        // A previous comparison's last foreign reference must leave on this
        // host before the next exact canonical snapshot is captured.
        if let Some(previous) = self.comparison.take() {
            drop(ManuallyDrop::into_inner(previous));
        }
        let (canonical, resource) = {
            let state = self
                .owner
                .0
                .state
                .lock()
                .expect("same Participation canonical credential");
            let Some(canonical) = state.rich.credential.as_ref() else {
                return Err(ParticipationHostJobError::WrongPhase);
            };
            let Some(resource) = state.credential_resource else {
                return Err(ParticipationHostJobError::WrongPhase);
            };
            (
                CredentialUpdate::new(
                    canonical.credential_domain_id(),
                    canonical.epoch(),
                    Arc::clone(canonical.material()),
                ),
                resource,
            )
        };
        self.comparison = Some(ManuallyDrop::new(canonical));
        Ok(resource)
    }
    pub(crate) fn credential_on_host(
        &mut self,
        update: &CredentialUpdate,
        replacement_resource: u64,
    ) -> Result<novarocks_worker_protocol::domain::CredentialInput, ParticipationHostJobError> {
        let resource = self.capture_credential_on_host()?;
        let canonical = self
            .comparison
            .as_ref()
            .expect("original comparison capture");
        Ok(novarocks_worker_protocol::domain::CredentialInput {
            credential_domain_id: update.credential_domain_id(),
            epoch: update.epoch(),
            current_resource: resource,
            replacement_resource,
            same_material: update.matches_installed(canonical.material().as_ref()),
        })
    }
    /// Compare a replay against this same canonical capture. The exact
    /// current resource and material come from one snapshot; no replacement
    /// resource, domain mutation or second credential observation is needed.
    pub(crate) fn compare_establish_on_host(
        &mut self,
        candidate: &CredentialUpdate,
    ) -> Result<
        novarocks_worker_protocol::participation::EstablishCredentialComparison,
        ParticipationHostJobError,
    > {
        let resource = self.capture_credential_on_host()?;
        let canonical = self
            .comparison
            .as_ref()
            .expect("original comparison capture");
        let comparison = novarocks_worker_protocol::participation::EstablishCredentialComparison {
            current: novarocks_worker_protocol::domain::CredentialState {
                credential_domain_id: canonical.credential_domain_id(),
                epoch: canonical.epoch(),
                resource,
            },
            same_material: candidate.matches_installed(canonical.material().as_ref()),
        };
        self.clear_comparison_on_host();
        Ok(comparison)
    }
    pub(crate) fn credential_state(
        &self,
    ) -> Option<novarocks_worker_protocol::domain::CredentialState> {
        let state = self
            .owner
            .0
            .state
            .lock()
            .expect("same original credential identity");
        let canonical = state.rich.credential.as_ref()?;
        Some(novarocks_worker_protocol::domain::CredentialState {
            credential_domain_id: canonical.credential_domain_id(),
            epoch: canonical.epoch(),
            resource: state.credential_resource?,
        })
    }
    pub(crate) fn clear_comparison_on_host(&mut self) {
        if let Some(original) = self.comparison.take() {
            drop(ManuallyDrop::into_inner(original));
        }
    }
    pub(crate) fn advance_on_host(
        &self,
        update: &novarocks_execution_contract::operation::ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        self.instance
            .as_ref()
            .expect("original Participation domain instance")
            .advance_shared_domain(update)
    }
    /// Transfer the actual applied credential to the H-owned canonical holder.
    /// The returned previous material must be destroyed by this same host body.
    pub(crate) fn adopt_credential_on_host(
        &self,
        update: &CredentialUpdate,
        expected_resource: u64,
        replacement_resource: u64,
    ) -> Result<Option<ManuallyDrop<CredentialUpdate>>, ParticipationHostJobError> {
        let replacement = ManuallyDrop::new(CredentialUpdate::new(
            update.credential_domain_id(),
            update.epoch(),
            Arc::clone(update.material()),
        ));
        let mut state = self
            .owner
            .0
            .state
            .lock()
            .expect("same Participation credential publication");
        if state.credential_resource != Some(expected_resource) {
            drop(state);
            drop(ManuallyDrop::into_inner(replacement));
            return Err(ParticipationHostJobError::WrongPhase);
        }
        let old = state.rich.credential.replace(replacement);
        state.credential_resource = Some(replacement_resource);
        Ok(old)
    }
    pub(crate) fn finish_on_host(&mut self) {
        if self.returned {
            return;
        }
        if let Some(canonical) = self.comparison.take() {
            drop(ManuallyDrop::into_inner(canonical));
        }
        if let Some(instance) = self.instance.take() {
            drop(ManuallyDrop::into_inner(instance));
        }
        {
            let mut state = self
                .owner
                .0
                .state
                .lock()
                .expect("actual Participation domain borrower exit");
            state.domain_borrowers = state
                .domain_borrowers
                .checked_sub(1)
                .unwrap_or_else(|| std::process::abort());
        }
        self.returned = true;
        self.owner.wake();
    }
}
impl Drop for ParticipationDomainInstance {
    fn drop(&mut self) {
        if !self.returned {
            self.owner
                .0
                .state
                .lock()
                .expect("unresolved Participation domain borrower")
                .fault = Some(ParticipationHostReturned::Panicked);
            self.owner.wake();
        }
    }
}
