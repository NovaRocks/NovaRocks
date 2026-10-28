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
    actors::{ContextMessage, ControlMessage, WorkerAction, WorkerActor, WorkerMessage},
    admitted_role::adopt_reported_work,
    context_io::ContextIoParts,
    context_ready::{ContextReadyError, ContextReadyNotice},
    control_request::ControlRequestNotice,
    establish_ingress::ValidatedMaterializationSource,
    preparation::{HostFairnessGroup, QueuedPreparation},
};
use crate::{
    HostRejection, QueryContextHostFactory, QueryContextHostInstance, ReleasedContextEvidence,
    task_completion_roles::AdmittedCompletionRole,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket, InputPermit, InputRole, InputRoleId},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{WorkLease, WorkRetirementObserver},
    notify::Notify,
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ActionId, ActorId, ConsumerId, Generation, InputId, ScopeId, WorkId},
    mailbox::InputClass,
    scope::ScopeError,
};
use novarocks_execution_contract::{
    CredentialUpdate, QueryContextRef, operation::DomainFailureOutcome,
};
use novarocks_worker_protocol::{
    context::{ContextEffect, ContextEstablishment, ContextInput, ContextMaterialized},
    control::{ControlEffect, ControlInput},
    creation::{AdmissionRefusal, RequestContext},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Debug)]
pub(crate) enum ContextHostReturned {
    Materialized(Result<ContextMaterialized, AdmissionRefusal>),
    MaterializationWithdrawn,
    Released,
    Disposed,
    StartRefused(ExitRoleError),
    Panicked,
}
#[derive(Debug)]
pub(crate) enum ContextHostJobError {
    Runtime(RuntimeError),
    Driver(DriverError),
    Exit(ExitRoleError),
    WrongOwner,
    WrongPhase,
    Ready(ContextReadyError),
}
impl From<RuntimeError> for ContextHostJobError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<DriverError> for ContextHostJobError {
    fn from(e: DriverError) -> Self {
        Self::Driver(e)
    }
}
impl From<ExitRoleError> for ContextHostJobError {
    fn from(e: ExitRoleError) -> Self {
        Self::Exit(e)
    }
}

struct Call {
    work: WorkId,
    retirement: WorkRetirementObserver,
    role: ExitRole,
    report: ReportSlot,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<ContextHostReturned>>,
    reader: OwnedReturnReader<ContextHostReturned>,
    returned: Option<ContextHostReturned>,
    input: Option<InputRole<WorkerActor>>,
    delivered: bool,
    closed: bool,
    core_consumed: bool,
    observer_consumed: bool,
    retired: bool,
}
impl Call {
    fn new(original: AdmittedCompletionRole, input: Option<InputRole<WorkerActor>>) -> Self {
        let work = original.work.id();
        let retirement = original.work.retirement_observer();
        let report = original.report.clone();
        let (sender, reader) = report
            .claim_owned_return()
            .expect("fresh original Context return");
        let (role, authority) = ExitRole::from_admitted(adopt_reported_work(
            original.work,
            original.capacity,
            original.report,
        ));
        Self {
            work,
            retirement,
            role,
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
    ) -> Result<(ExitAuthority, OwnedReturnSender<ContextHostReturned>), ContextHostJobError> {
        if self.authority.is_none() || self.sender.is_none() {
            return Err(ContextHostJobError::WrongPhase);
        }
        Ok((self.authority.take().unwrap(), self.sender.take().unwrap()))
    }
}

#[derive(Default)]
struct Rich {
    factory: Option<ManuallyDrop<Arc<dyn QueryContextHostFactory>>>,
    instance: Option<ManuallyDrop<Arc<dyn QueryContextHostInstance>>>,
    credential: Option<ManuallyDrop<CredentialUpdate>>,
    rejection: Option<ManuallyDrop<HostRejection>>,
    evidence: Option<ManuallyDrop<ReleasedContextEvidence>>,
    panics: [Option<PanicPayload>; 8],
}
struct State {
    m: Call,
    r: Call,
    h: Call,
    rich: Rich,
    m_issued: bool,
    r_issued: bool,
    h_issued: bool,
    release_requested: bool,
    // Positive outcome of the actual M boundary, not absence in a directory.
    no_instance_created: bool,
    fault: Option<ContextHostReturned>,
    creator: Option<Creator>,
    removal: Option<OriginalRemoval>,
}
/// This receipt is created only by validating the actual Root effect against
/// this same custodian. The pre-admitted H role owns the resulting cleanup
/// responsibility; these numbers cannot sign a new action or access a source.
#[derive(Clone, Copy, Eq, PartialEq)]
struct OriginalRemoval {
    input: InputId,
    action: ActionId,
    generation: Generation,
}
struct Creator {
    request: RequestContext,
    work: WorkId,
    notice: ControlRequestNotice,
    permit: Option<InputPermit<WorkerActor>>,
    input: Option<InputRole<WorkerActor>>,
    role: Option<InputRoleId>,
    handoff: Option<(InputId, ActionId)>,
    retired: bool,
}
struct Custody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    actor: ActorId,
    control: ActorId,
    context: QueryContextRef,
    generation: Generation,
    establishment: ContextEstablishment,
    pending: Notify,
    notice: ContextReadyNotice,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "The original Context call and capture inventory must remain supervised"]
pub(crate) struct ContextHostJobs(Arc<Custody>);
pub(super) struct ContextHostJobBinding {
    pub runtime: ActorRuntime<WorkerActor>,
    pub ledger: RuntimeLedger,
    pub actor: ActorId,
    pub control: ActorId,
    pub context: QueryContextRef,
    pub generation: Generation,
    pub establishment: ContextEstablishment,
    pub notice: ContextReadyNotice,
}

#[must_use = "A refusal returns both original opaque captures to their actual caller"]
pub(crate) struct MaterializationCaptureRefusal {
    pub error: ContextHostJobError,
    pub source: ValidatedMaterializationSource,
    pub factory: Arc<dyn QueryContextHostFactory>,
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
    Release(Option<ManuallyDrop<Arc<dyn QueryContextHostInstance>>>),
    Dispose(Rich),
}
#[must_use = "Return the same original host job on physical queue refusal"]
pub(crate) struct ContextHostJob {
    owner: ContextHostJobs,
    work: WorkId,
    index: usize,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<ContextHostReturned>>,
    operation: Option<Operation>,
}
impl ContextHostJobs {
    pub(super) fn belongs_to(&self, runtime: &ActorRuntime<WorkerActor>, control: ActorId) -> bool {
        self.0.runtime.same_runtime(runtime) && self.0.control == control
    }
    pub(super) fn materialization_key(&self) -> (ScopeId, WorkId) {
        (
            self.0.scope.id(),
            self.0.state.lock().expect("original M identity").m.work,
        )
    }
    pub(super) fn scope_stopped(&self) -> Result<bool, RuntimeError> {
        Ok(self.0.scope.gates()?.stopped)
    }
    pub(super) fn from_admitted(binding: ContextHostJobBinding, parts: ContextIoParts) -> Self {
        let ContextHostJobBinding {
            runtime,
            ledger,
            actor,
            control,
            context,
            generation,
            establishment,
            notice,
        } = binding;
        let ContextIoParts {
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
            context,
            generation,
            establishment,
            pending: Notify::default(),
            notice,
            state: Mutex::new(State {
                m: Call::new(materialization, Some(materialization_input)),
                r: Call::new(release, Some(release_input)),
                h: Call::new(holder, None),
                rich: Rich::default(),
                m_issued: false,
                r_issued: false,
                h_issued: false,
                release_requested: false,
                no_instance_created: false,
                fault: None,
                creator: None,
                removal: None,
            }),
            retained: Mutex::new(None),
        });
        *shared.retained.lock().expect("Context original retention") = Some(shared.clone());
        Self(shared)
    }
    pub(crate) fn capture_materialization(
        &self,
        source: ValidatedMaterializationSource,
        factory: Arc<dyn QueryContextHostFactory>,
    ) -> Result<ContextHostJob, MaterializationCaptureRefusal> {
        if !source.matches(
            &self.0.runtime,
            self.0.control,
            self.0.context,
            self.0.generation,
            self.0.establishment,
        ) {
            return Err(MaterializationCaptureRefusal {
                error: ContextHostJobError::WrongOwner,
                source,
                factory,
            });
        }
        let mut state = self.0.state.lock().expect("Context actual host inventory");
        if state
            .creator
            .as_ref()
            .is_none_or(|c| c.input.is_none() || c.request != source.request())
        {
            return Err(MaterializationCaptureRefusal {
                error: ContextHostJobError::WrongPhase,
                source,
                factory,
            });
        }
        if state.m_issued {
            return Err(MaterializationCaptureRefusal {
                error: ContextHostJobError::WrongPhase,
                source,
                factory,
            });
        }
        let (authority, sender) = match state.m.take() {
            Ok(x) => x,
            Err(error) => {
                return Err(MaterializationCaptureRefusal {
                    error,
                    source,
                    factory,
                });
            }
        };
        state.m_issued = true;
        state.rich.factory = Some(ManuallyDrop::new(factory));
        Ok(ContextHostJob {
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
        request: RequestContext,
        work: WorkId,
        author: &WorkLease,
        notice: ControlRequestNotice,
    ) -> Result<InputRoleId, ContextHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.0.control
            || ticket.input != request.token
            || request.operation != self.0.establishment.operation
            || !matches!(&ticket.effect,WorkerAction::Control{effect:ControlEffect::Materialize{context,establishment,generation,reservation},admitted:Some(original)}
                if *context==self.0.context && *establishment==self.0.establishment && *generation==self.0.generation && reservation==original)
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        let mut s = self
            .0
            .state
            .lock()
            .expect("original creator return binding");
        if s.creator.is_some() || s.m_issued {
            return Err(ContextHostJobError::WrongPhase);
        }
        s.creator = Some(Creator {
            request,
            work,
            notice,
            permit: None,
            input: None,
            role: None,
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
        match c.permit.take().unwrap().into_role() {
            Ok(input) => {
                c.role = Some(input.id());
                c.input = Some(input);
            }
            Err((error, permit)) => {
                c.permit = Some(permit);
                return Err(error.into());
            }
        }
        Ok(c.role.unwrap())
    }
    pub(super) fn creator_role(&self) -> Option<InputRoleId> {
        self.0
            .state
            .lock()
            .expect("creator original return role")
            .creator
            .as_ref()
            .and_then(|c| c.role)
    }
    pub(super) fn matches_owner(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        root: ActorId,
        context: QueryContextRef,
        generation: Generation,
        establishment: ContextEstablishment,
    ) -> bool {
        self.0.runtime.same_runtime(runtime)
            && self.0.control == root
            && self.0.context == context
            && self.0.generation == generation
            && self.0.establishment == establishment
    }
    pub(crate) fn source_consumed(&self) -> Result<bool, ContextHostJobError> {
        let s = self.0.state.lock().expect("original M source consumption");
        Ok(s.m_issued && s.m.returned.is_some() && s.m.role.actual_exit_ready()?)
    }
    /// Forward only the verdict of the actual original Context M input. This
    /// neither reruns host work nor manufactures a second external request.
    pub(super) fn forward_establishment(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ContextHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        let mut s = self.0.state.lock().expect("creator Core decision handoff");
        let WorkerAction::Context(ContextEffect::EstablishmentDecided {
            context, decision, ..
        }) = &ticket.effect
        else {
            return Err(ContextHostJobError::WrongOwner);
        };
        if ticket.actor != self.0.actor
            || *context != self.0.context
            || ticket.descriptor.work != s.m.work
            || s.m
                .input
                .as_ref()
                .ok_or(ContextHostJobError::WrongPhase)?
                .in_flight()?
                != Some(ticket.input)
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        let c = s.creator.as_mut().ok_or(ContextHostJobError::WrongPhase)?;
        let key = (ticket.input, ticket.descriptor.id);
        if c.handoff == Some(key) {
            return Ok(());
        }
        if c.handoff.is_some() {
            return Err(ContextHostJobError::WrongPhase);
        }
        let message = WorkerMessage::Control(ControlMessage::Command {
            work: c.work,
            request: None,
            command: ControlInput::EstablishmentDecided {
                context: *context,
                decision: decision.clone(),
            },
        });
        self.0
            .runtime
            .fill_from_effect(
                ticket,
                c.input.as_ref().ok_or(ContextHostJobError::WrongPhase)?,
                message,
            )
            .map_err(|(e, _)| ContextHostJobError::Driver(e))?;
        c.handoff = Some(key);
        c.notice.notify();
        Ok(())
    }
    pub(super) fn retire_creator(&self) -> Result<bool, ContextHostJobError> {
        let mut s = self
            .0
            .state
            .lock()
            .expect("original creator return retirement");
        if !s.m_issued || s.m.returned.is_none() || !s.m.role.actual_exit_ready()? {
            return Ok(false);
        }
        let c = s.creator.as_mut().ok_or(ContextHostJobError::WrongPhase)?;
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
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
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
    /// R can be requested before factory publication. Only the original
    /// pending bit is set; no worker is occupied waiting for a resource.
    pub(super) fn request_release(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ContextHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.0.actor
            || !matches!(&ticket.effect,
                WorkerAction::Context(ContextEffect::ReleaseHost { context, .. })
                    if *context == self.0.context)
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        self.0
            .state
            .lock()
            .expect("Context release request")
            .release_requested = true;
        self.wake();
        Ok(())
    }
    pub(crate) fn take_release_job(&self) -> Result<Option<ContextHostJob>, ContextHostJobError> {
        let mut state = self.0.state.lock().expect("Context release inventory");
        if !state.release_requested || state.r_issued {
            return Ok(None);
        }
        let instance = state
            .rich
            .instance
            .as_ref()
            .map(|v| ManuallyDrop::new(Arc::clone(v)));
        if instance.is_none()
            && !(state.no_instance_created
                && state.m.returned.is_some()
                && state.m.role.actual_exit_ready()?)
        {
            return Ok(None);
        }
        let (authority, sender) = state.r.take()?;
        state.r_issued = true;
        Ok(Some(ContextHostJob {
            owner: self.clone(),
            work: state.r.work,
            index: 1,
            authority: Some(authority),
            sender: Some(sender),
            operation: Some(Operation::Release(instance)),
        }))
    }
    pub(crate) fn fault(&self) -> Option<ContextHostReturned> {
        self.0
            .state
            .lock()
            .expect("Context host fault")
            .fault
            .clone()
    }
    fn wake(&self) {
        self.0.pending.notify();
        self.0.notice.notify();
    }
    pub(crate) fn pending_version(&self) -> u64 {
        self.0.pending.version()
    }

    pub(super) fn holder_disposed(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        context: QueryContextRef,
        child: &ScopeHandle,
    ) -> Result<bool, ContextHostJobError> {
        if !self.0.runtime.same_runtime(runtime)
            || self.0.actor != actor
            || self.0.context != context
            || self.0.scope.id() != child.id()
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        let state = self.0.state.lock().expect("original Context holder exit");
        Ok(
            matches!(state.h.returned, Some(ContextHostReturned::Disposed))
                && state.h.role.actual_exit_ready()?,
        )
    }

    /// Only these already-paid inputs may carry normal M/R outcomes. Their
    /// author positions were reserved before the first foreign capture.
    pub(crate) fn poll_returns(&self) -> Result<usize, ContextHostJobError> {
        let mut state = self.0.state.lock().expect("Context original returns");
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
                ContextHostReturned::Materialized(result) => {
                    Some(ContextInput::MaterializationFinished { result })
                }
                ContextHostReturned::MaterializationWithdrawn => {
                    Some(ContextInput::MaterializationWithdrawn)
                }
                ContextHostReturned::Released => Some(ContextInput::HostReleased),
                ContextHostReturned::Disposed => None,
                fault => {
                    state.fault = Some(fault);
                    continue;
                }
            };
            if let Some(command) = command {
                call.input
                    .as_ref()
                    .expect("original M/R callback")
                    .fill(WorkerMessage::Context(ContextMessage::Command {
                        work: call.work,
                        request: None,
                        command,
                    }))
                    .map_err(|(e, _)| ContextHostJobError::Driver(e))?;
            }
            call.delivered = true;
            delivered += 1;
        }
        if observed && let Some(c) = &state.creator {
            c.notice.notify();
        }
        Ok(delivered)
    }
    fn retire_call(&self, call: &mut Call, core: bool) -> Result<bool, ContextHostJobError> {
        if call.retired {
            return Ok(true);
        }
        if !call.delivered
            || !matches!(
                call.role.phase(),
                ExitRolePhase::Exited | ExitRolePhase::Retired
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
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
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
        if call.role.phase() != ExitRolePhase::Retired && !call.role.try_retire()? {
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
    pub(crate) fn retire_callbacks(&self) -> Result<bool, ContextHostJobError> {
        let mut s = self.0.state.lock().expect("Context callback retirement");
        let m = self.retire_call(&mut s.m, true)?;
        let r = self.retire_call(&mut s.r, true)?;
        Ok(m && r)
    }
    /// Transfer the real Root removal intent into its original prepaid H
    /// owner. Root can complete this metadata handoff without waiting for
    /// physical calls, report consumers or a cleanup executor position.
    pub(crate) fn request_holder_disposal(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ContextHostJobError> {
        self.0.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.0.control
            || !matches!(&ticket.effect,WorkerAction::Control{effect:ControlEffect::RemoveHost{context},..} if *context==self.0.context)
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        let original = OriginalRemoval {
            input: ticket.input,
            action: ticket.descriptor.id,
            generation: ticket.generation,
        };
        let mut s = self
            .0
            .state
            .lock()
            .expect("original Context removal handoff");
        if s.removal.is_some_and(|previous| previous != original) {
            return Err(ContextHostJobError::WrongPhase);
        }
        s.removal = Some(original);
        drop(s);
        self.wake();
        Ok(())
    }
    /// The original accepted intent remains valid after its Root effect and
    /// timer input retire. Only M/R's actual complete retirement enables H.
    pub(crate) fn take_holder_disposal(
        &self,
    ) -> Result<Option<ContextHostJob>, ContextHostJobError> {
        let mut s = self.0.state.lock().expect("Context holder disposal");
        if s.removal.is_none() || !s.m.retired || !s.r.retired || s.h_issued {
            return Ok(None);
        }
        let (authority, sender) = s.h.take()?;
        s.h_issued = true;
        let rich = std::mem::take(&mut s.rich);
        Ok(Some(ContextHostJob {
            owner: self.clone(),
            work: s.h.work,
            index: 2,
            authority: Some(authority),
            sender: Some(sender),
            operation: Some(Operation::Dispose(rich)),
        }))
    }
    pub(crate) fn retire_holder(&self) -> Result<bool, ContextHostJobError> {
        let mut s = self.0.state.lock().expect("Context holder retirement");
        if !self.retire_call(&mut s.h, false)? {
            return Ok(false);
        }
        drop(s);
        drop(
            self.0
                .retained
                .lock()
                .expect("Context original retention")
                .take(),
        );
        Ok(true)
    }
}
impl QueuedPreparation for ContextHostJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.owner.0.scope.id(), self.work)
    }
    fn fairness_group(&self, _: ScopeId) -> HostFairnessGroup {
        HostFairnessGroup::Context(self.owner.0.context)
    }
}
impl ContextHostJob {
    pub(super) fn belongs_to_jobs(&self, original: &ContextHostJobs) -> bool {
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
    fn materialize(&self, source: &ValidatedMaterializationSource) -> ContextHostReturned {
        let factory = {
            let s = self.owner.0.state.lock().expect("Context factory loan");
            Arc::clone(s.rich.factory.as_ref().expect("original host factory"))
        };
        let instance = match factory.create_instance(self.owner.0.context) {
            Ok(instance) => instance,
            Err(rejection) => {
                let failure = rejection.operation_failure();
                let mut s = self.owner.0.state.lock().expect("Context factory refusal");
                s.no_instance_created = true;
                s.rich.rejection = Some(ManuallyDrop::new(rejection));
                return ContextHostReturned::Materialized(Err(AdmissionRefusal::Domain(
                    DomainFailureOutcome::InvalidStateOrRequest,
                    failure,
                )));
            }
        };
        let request = source.shared_facts();
        let credential = request.initial_credential();
        let canonical = CredentialUpdate::new(
            credential.lease_id(),
            credential.epoch(),
            Arc::clone(credential.material()),
        );
        let (closing, resource) = {
            let mut s = self
                .owner
                .0
                .state
                .lock()
                .expect("Context original instance publication");
            s.rich.instance = Some(ManuallyDrop::new(instance.clone()));
            s.rich.credential = Some(ManuallyDrop::new(canonical));
            (s.release_requested, s.h.work.get())
        };
        self.owner.wake();
        if closing {
            return ContextHostReturned::MaterializationWithdrawn;
        }
        match instance.materialize(request) {
            Ok(()) => ContextHostReturned::Materialized(Ok(ContextMaterialized {
                initial_credential_resource: resource,
            })),
            Err(rejection) => {
                let failure = rejection.operation_failure();
                self.owner
                    .0
                    .state
                    .lock()
                    .expect("Context materialization rejection")
                    .rich
                    .rejection = Some(ManuallyDrop::new(rejection));
                ContextHostReturned::Materialized(Err(AdmissionRefusal::Domain(
                    DomainFailureOutcome::InvalidStateOrRequest,
                    failure,
                )))
            }
        }
    }
    fn release(
        &self,
        instance: &Option<ManuallyDrop<Arc<dyn QueryContextHostInstance>>>,
    ) -> ContextHostReturned {
        let evidence = instance
            .as_ref()
            .map_or_else(ReleasedContextEvidence::none, |instance| instance.release());
        self.owner
            .0
            .state
            .lock()
            .expect("Context original release evidence")
            .rich
            .evidence = Some(ManuallyDrop::new(evidence));
        ContextHostReturned::Released
    }
    fn dispose(&self, rich: Rich) -> ContextHostReturned {
        let Rich {
            factory,
            instance,
            credential,
            rejection,
            evidence,
            panics,
        } = rich;
        let mut failures: [Option<PanicPayload>; 8] = Default::default();
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
            drop(evidence.map(ManuallyDrop::into_inner))
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
                .expect("Context destructor panic retention");
            s.rich.panics = failures;
            ContextHostReturned::Panicked
        } else {
            ContextHostReturned::Disposed
        }
    }
    pub(crate) fn run_on_host(mut self) {
        let mut authority = self
            .authority
            .take()
            .expect("original Context job authority");
        let operation = self
            .operation
            .take()
            .expect("original Context host operation");
        let (cancelled, started) = if self.index == 0 {
            let mut state = self.owner.0.state.lock().expect("Context M start fence");
            if state.release_requested {
                state.no_instance_created = true;
                (true, Ok(()))
            } else {
                let started = authority.start();
                let stopped_before_start = matches!(
                    started,
                    Err(ExitRoleError::Runtime(RuntimeError::Scope(
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
            Ok(ContextHostReturned::MaterializationWithdrawn)
        } else {
            match started {
                Err(e) => {
                    if self.index == 0 {
                        self.owner
                            .0
                            .state
                            .lock()
                            .expect("unstarted Context M")
                            .no_instance_created = true;
                    }
                    Ok(ContextHostReturned::StartRefused(e))
                }
                Ok(()) if matches!(&operation, Operation::Dispose(_)) => {
                    Ok(ContextHostReturned::Disposed)
                }
                Ok(()) => catch_unwind(AssertUnwindSafe(|| match &operation {
                    Operation::Materialize(source) => self.materialize(source),
                    Operation::Release(instance) => self.release(instance),
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
                    .expect("unstarted Context holder")
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
                    .expect("Context host panic retention")
                    .rich
                    .panics[self.index] = Some(ManuallyDrop::new(p));
                ContextHostReturned::Panicked
            }
        };
        if self.index == 2 && !matches!(outcome, ContextHostReturned::Disposed) {
            // At least one opaque destructor did not complete. This original
            // holder remains Unknown; a panic receipt is not capture exit.
            drop(authority);
        } else if let Err(e) = authority.exited() {
            std::mem::forget((e, outcome, self.sender.take()));
            std::process::abort();
        }
        let sender = self.sender.take().expect("original Context owned return");
        if let Err(refusal) = sender.publish(outcome) {
            std::mem::forget(refusal);
            std::process::abort();
        }
        self.owner.wake();
    }
    fn return_captures(operation: Operation) {
        match operation {
            Operation::Release(instance) => drop(instance.map(ManuallyDrop::into_inner)),
            Operation::Materialize(source) => drop(source),
            Operation::Dispose(_) => unreachable!("holder captures use their own cleanup boundary"),
        }
    }
}
impl Drop for ContextHostJob {
    fn drop(&mut self) {
        if self.operation.is_some() || self.sender.is_some() || self.authority.is_some() {
            self.owner
                .0
                .state
                .lock()
                .expect("lost Context host job")
                .fault = Some(ContextHostReturned::Panicked);
            self.owner.wake();
        }
    }
}
