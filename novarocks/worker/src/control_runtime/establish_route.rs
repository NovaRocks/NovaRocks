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

//! One original RPC lends prepaid storage to its actual Context. The two
//! neutral input legs retire independently; Context retirement never waits
//! for the Root to consume its already accepted backchannel input.

use super::{
    actors::{ContextMessage, ControlMessage, WorkerAction, WorkerActor, WorkerMessage},
    admitted_role::adopt_reported_work,
    control_request::ControlRequestNotice,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{
        ActorRegistration, ActorRuntime, DriverError, EffectTicket, InputPermit, InputRole,
        InputRoleId,
    },
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkLease, WorkRetirementObserver},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ActionId, ActorId, ConsumerId, InputId, WorkId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::QueryContextRef;
use novarocks_worker_protocol::{
    context::{
        ContextEffect, ContextInput, EstablishCredentialComparison, EstablishReplayRequest,
        EstablishReplayResult,
    },
    control::{ControlCore, ControlInput},
    creation::RequestContext,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(crate) const CONTEXT_ROUTE_ESCROW_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ContextActor::MAX_STEP_ACTIONS,
    work: 1,
    completions: 1,
    captures: 1,
    cuts: 0,
    replies: 0,
};
pub(crate) const ROOT_ROUTE_RETURN_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
    work: 0,
    completions: 0,
    captures: 0,
    cuts: 0,
    replies: 0,
};
fn context_consumer() -> ConsumerId {
    ConsumerId::new(1).expect("one original Context route observer")
}

#[derive(Debug)]
pub(crate) enum EstablishRouteError {
    Driver(DriverError),
    Runtime(RuntimeError),
    Exit(ExitRoleError),
    WrongOwner,
    TargetRemoved,
    WrongEffect,
    WrongPhase,
    Capacity,
}
impl From<DriverError> for EstablishRouteError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<RuntimeError> for EstablishRouteError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ExitRoleError> for EstablishRouteError {
    fn from(error: ExitRoleError) -> Self {
        Self::Exit(error)
    }
}

#[derive(Clone)]
enum ProbePhase {
    Constructing,
    InFlight,
    Waiting(EstablishReplayResult),
    Finished,
}
struct ContextLeg {
    scope: Option<ScopeHandle>,
    escrow: Option<CapacityBundle>,
    work: Option<WorkLease>,
    work_id: Option<WorkId>,
    retirement: Option<WorkRetirementObserver>,
    zero: Option<CapacityBundle>,
    report: Option<ReportSlot>,
    role: Option<ExitRole>,
    authority: Option<ExitAuthority>,
    permit: Option<InputPermit<WorkerActor>>,
    input: Option<InputRole<WorkerActor>>,
    phase: ProbePhase,
    handoff: Option<(InputId, ActionId)>,
    cancelled: bool,
    consumed: bool,
    retired: bool,
    partial_body_exited: bool,
    partial_captures_exited: bool,
    partial_report_revoked: bool,
    partial_report_body_exited: bool,
    partial_report_captures_exited: bool,
}
#[derive(Default)]
struct RootLeg {
    permit: Option<InputPermit<WorkerActor>>,
    input: Option<InputRole<WorkerActor>>,
    retired: bool,
    role_id: Option<InputRoleId>,
}

/// Both inventories hold this same original owner. Neither clone issues a
/// request, admits capacity or duplicates a Work/exit authority. There is no
/// operation-result cache here: Waiting only suspends a future neutral probe.
#[must_use = "Retain both original input legs until their real retirement"]
pub(crate) struct EstablishRouteCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    root: ActorId,
    root_work: WorkId,
    context: QueryContextRef,
    actor: ActorId,
    registration: ActorRegistration,
    request: EstablishReplayRequest,
    notice: ControlRequestNotice,
    context_leg: Mutex<ContextLeg>,
    context_retired: AtomicBool,
    root_leg: Mutex<RootLeg>,
}
impl EstablishRouteCustody {
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        root: ActorId,
        root_work: WorkId,
        context: QueryContextRef,
        registration: ActorRegistration,
        request: EstablishReplayRequest,
        escrow: CapacityBundle,
        notice: ControlRequestNotice,
    ) -> Result<Arc<Self>, (EstablishRouteError, CapacityBundle)> {
        if let Err(error) = runtime.validate_registration(&registration) {
            return Err((EstablishRouteError::Driver(error), escrow));
        }
        let actor = registration.actor();
        Ok(Arc::new(Self {
            runtime,
            ledger,
            root,
            root_work,
            context,
            actor,
            registration,
            request,
            notice,
            context_leg: Mutex::new(ContextLeg {
                scope: None,
                escrow: Some(escrow),
                work: None,
                work_id: None,
                retirement: None,
                zero: None,
                report: None,
                role: None,
                authority: None,
                permit: None,
                input: None,
                phase: ProbePhase::Constructing,
                handoff: None,
                cancelled: false,
                consumed: false,
                retired: false,
                partial_body_exited: false,
                partial_captures_exited: false,
                partial_report_revoked: false,
                partial_report_body_exited: false,
                partial_report_captures_exited: false,
            }),
            context_retired: AtomicBool::new(false),
            root_leg: Mutex::new(RootLeg::default()),
        }))
    }
    pub(crate) fn context(&self) -> QueryContextRef {
        self.context
    }
    pub(crate) fn actor(&self) -> ActorId {
        self.actor
    }
    /// Qualify the immutable target without taking either original leg lock.
    /// Equal public IDs cannot replace the admitted Context registration.
    pub(crate) fn target_matches(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
        context: QueryContextRef,
        root: ActorId,
    ) -> bool {
        self.root == root
            && self.runtime.same_runtime(runtime)
            && self.registration.same_registration(registration)
            && self.context == context
    }
    pub(crate) fn request(&self) -> RequestContext {
        self.request.request
    }
    pub(crate) fn context_retired(&self) -> bool {
        self.context_retired.load(Ordering::Acquire)
    }
    /// Address of this original return role, retained after its real close so
    /// the Root owner can remove precisely its progress-index entry.
    pub(super) fn return_role(&self) -> Option<InputRoleId> {
        self.root_leg
            .lock()
            .expect("original Root route return")
            .role_id
    }
    /// Root stores this same Arc before initializing its original backchannel.
    /// This touches only Root's leg and never borrows a Context owner or scope.
    pub(crate) fn initialize_root(
        &self,
        root_author: &WorkLease,
    ) -> Result<(), EstablishRouteError> {
        let mut root = self.root_leg.lock().expect("original Root route return");
        if root.retired
            || root.permit.is_some()
            || root.input.is_some()
            || root.role_id.is_some()
            || self.context_retired.load(Ordering::Acquire)
        {
            return Err(EstablishRouteError::WrongPhase);
        }
        root.permit = Some(self.runtime.reserve_admitted_input(
            self.root,
            self.actor,
            InputClass::Control,
            root_author,
        )?);
        let permit = root.permit.take().expect("actual Root input reservation");
        root.input = Some(match permit.into_role() {
            Ok(input) => input,
            Err((error, permit)) => {
                root.permit = Some(permit);
                return Err(error.into());
            }
        });
        root.role_id = Some(root.input.as_ref().expect("original return role").id());
        Ok(())
    }
    /// Context stores its alias before initializing any Context Work or input.
    /// Every partial stays in this original leg; the Root return is already paid.
    pub(crate) fn initialize_context(&self, parent: &WorkLease) -> Result<(), EstablishRouteError> {
        let mut leg = self.context_leg.lock().expect("original Context route");
        if !matches!(leg.phase, ProbePhase::Constructing)
            || leg.scope.is_some()
            || leg.work_id.is_some()
            || leg.cancelled
            || leg.retired
            || self.context_retired.load(Ordering::Acquire)
        {
            return Err(EstablishRouteError::WrongPhase);
        }
        if !self.registration.matches_scope(parent.scope()) {
            return Err(EstablishRouteError::WrongOwner);
        }
        self.runtime.validate_registration(&self.registration)?;
        {
            // Keep the existing ContextLeg -> RootLeg order whenever both
            // original legs are needed. Root initialization takes Root only.
            let root = self.root_leg.lock().expect("original Root route return");
            if root.retired
                || root.permit.is_some()
                || root.input.is_none()
                || root.role_id.is_none()
            {
                return Err(EstablishRouteError::WrongPhase);
            }
        }
        let scope = parent.scope().clone();
        // The Context now owns all further partial cleanup, even if its next
        // admission refuses. Root may retire only a still-unbound cancelled leg.
        leg.scope = Some(scope.clone());
        let generation = scope.gates()?.generation;
        let guard = scope.reserve_next(
            generation,
            ResourceVector::default(),
            WorkKind::Settlement,
            WorkAdmission::Derived(parent.id()),
        )?;
        let (work, zero) = guard.bind(WorkKind::Settlement)?;
        leg.work_id = Some(work.id());
        leg.retirement = Some(work.retirement_observer());
        leg.work = Some(work);
        leg.zero = Some(zero);
        {
            let ContextLeg { escrow, work, .. } = &mut *leg;
            escrow
                .as_mut()
                .expect("original RPC escrow")
                .transfer(work.as_ref().expect("actual Context route Work"))?;
        }
        Self::release(&mut leg.zero)?;
        leg.report = Some(ReportSlot::reserve_next(
            &scope,
            generation,
            leg.work_id.expect("bound Context route Work"),
            1,
            [(context_consumer(), ConsumerKind::Observer)],
        )?);
        leg.permit = Some(self.runtime.reserve_admitted_input(
            self.actor,
            self.root,
            InputClass::Control,
            leg.work.as_ref().expect("actual Context input author"),
        )?);
        let permit = leg.permit.take().expect("actual Context input reservation");
        leg.input = Some(match permit.into_role() {
            Ok(input) => input,
            Err((error, permit)) => {
                leg.permit = Some(permit);
                return Err(error.into());
            }
        });
        let reported = adopt_reported_work(
            leg.work.take().expect("original Context route Work"),
            leg.escrow.take().expect("transferred original RPC units"),
            leg.report
                .as_ref()
                .expect("original Context report")
                .clone(),
        );
        let (role, authority) = ExitRole::from_admitted(reported);
        leg.role = Some(role);
        leg.authority = Some(authority);
        leg.authority
            .as_mut()
            .expect("original route authority")
            .start()?;
        self.issue_probe(&mut leg, None)
    }
    fn issue_probe(
        &self,
        leg: &mut ContextLeg,
        comparison: Option<EstablishCredentialComparison>,
    ) -> Result<(), EstablishRouteError> {
        if leg.cancelled || leg.retired {
            return Err(EstablishRouteError::WrongPhase);
        }
        let command = WorkerMessage::Context(ContextMessage::Command {
            work: leg.work_id.ok_or(EstablishRouteError::WrongPhase)?,
            request: None,
            command: ContextInput::ProbeEstablish {
                context: self.context,
                request: self.request,
                comparison,
            },
        });
        leg.input
            .as_ref()
            .ok_or(EstablishRouteError::WrongPhase)?
            .fill(command)
            .map_err(|(error, _neutral)| EstablishRouteError::Driver(error))?;
        leg.handoff = None;
        leg.phase = ProbePhase::InFlight;
        Ok(())
    }
    /// Only a real current-credential comparison author may supply Some.
    /// This method creates no host job, source loan or comparison fact.
    pub(crate) fn retry_probe(
        &self,
        comparison: Option<EstablishCredentialComparison>,
    ) -> Result<bool, EstablishRouteError> {
        let mut leg = self.context_leg.lock().expect("original Context route");
        if !matches!(leg.phase, ProbePhase::Waiting(_)) || leg.cancelled {
            return Err(EstablishRouteError::WrongPhase);
        }
        if !self.retire_context_input(&mut leg)? {
            return Ok(false);
        }
        self.issue_probe(&mut leg, comparison)?;
        Ok(true)
    }
    pub(crate) fn waiting(&self) -> Option<EstablishReplayResult> {
        match self
            .context_leg
            .lock()
            .expect("original Context route")
            .phase
            .clone()
        {
            ProbePhase::Waiting(result) => Some(result),
            _ => None,
        }
    }
    /// Only local cleanup metadata is runnable. A live probe still awaiting
    /// its original Core decision must park rather than spin a host poller.
    pub(crate) fn context_cleanup_ready(&self) -> bool {
        let leg = self.context_leg.lock().expect("original Context route");
        leg.retired || leg.cancelled || matches!(leg.phase, ProbePhase::Finished)
    }
    /// The Context metadata lane calls this before completing its original
    /// effect. Successful fill transfers only neutral responsibility to Root;
    /// no Root consumption or physical host exit is inferred.
    pub(crate) fn accept_probe(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), EstablishRouteError> {
        self.runtime.validate_effect_ticket(ticket)?;
        let WorkerAction::Context(ContextEffect::EstablishProbed {
            context,
            request,
            result,
            decided_at,
        }) = &ticket.effect
        else {
            return Err(EstablishRouteError::WrongEffect);
        };
        let mut leg = self.context_leg.lock().expect("original Context route");
        let key = (ticket.input, ticket.descriptor.id);
        if ticket.actor != self.actor
            || *context != self.context
            || *request != self.request.request
            || Some(ticket.descriptor.work) != leg.work_id
            || leg
                .input
                .as_ref()
                .ok_or(EstablishRouteError::WrongPhase)?
                .in_flight()?
                != Some(ticket.input)
        {
            return Err(EstablishRouteError::WrongOwner);
        }
        if leg.handoff == Some(key) {
            return Ok(());
        }
        if !matches!(leg.phase, ProbePhase::InFlight) {
            return Err(EstablishRouteError::WrongPhase);
        }
        leg.phase = if leg.cancelled {
            ProbePhase::Finished
        } else if let EstablishReplayResult::Decided(outcome) = result {
            let root = self.root_leg.lock().expect("original Root route return");
            self.runtime
                .fill_from_effect(
                    ticket,
                    root.input.as_ref().ok_or(EstablishRouteError::WrongPhase)?,
                    WorkerMessage::Control(ControlMessage::Command {
                        work: self.root_work,
                        request: None,
                        command: ControlInput::RoutedEstablishFinished {
                            context: self.context,
                            request: self.request.request,
                            result: Ok(outcome.outcome),
                            acknowledgement: Some(outcome.clone()),
                            decided_at: *decided_at,
                        },
                    }),
                )
                .map_err(|(error, _neutral)| EstablishRouteError::Driver(error))?;
            ProbePhase::Finished
        } else {
            ProbePhase::Waiting(result.clone())
        };
        leg.handoff = Some(key);
        Ok(())
    }
    pub(crate) fn cancel(&self) {
        self.context_leg
            .lock()
            .expect("original Context route")
            .cancelled = true;
        match self.runtime.notify_host_actor(&self.registration) {
            Ok(()) | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
    fn retire_context_input(&self, leg: &mut ContextLeg) -> Result<bool, EstablishRouteError> {
        if let Some(input) = &leg.input
            && let Some(input) = input.in_flight()?
        {
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
        Ok(true)
    }
    fn release(capacity: &mut Option<CapacityBundle>) -> Result<(), RuntimeError> {
        if let Some(capacity) = capacity.as_mut() {
            capacity.release(&capacity.resources()?)?;
        }
        *capacity = None;
        Ok(())
    }
    /// Only cancelled metadata that never entered a Context scope can be
    /// withdrawn by Root. No Context Work, report or body exit is fabricated.
    pub(crate) fn try_retire_unbound_context(&self) -> Result<bool, EstablishRouteError> {
        let mut leg = self.context_leg.lock().expect("original Context route");
        if leg.scope.is_some() || !leg.cancelled {
            return Ok(false);
        }
        if leg.retired {
            return Ok(true);
        }
        Self::release(&mut leg.escrow)?;
        leg.phase = ProbePhase::Finished;
        leg.retired = true;
        self.context_retired.store(true, Ordering::Release);
        drop(leg);
        self.notice.notify();
        Ok(true)
    }
    pub(crate) fn try_retire_context(&self) -> Result<bool, EstablishRouteError> {
        let mut leg = self.context_leg.lock().expect("original Context route");
        if leg.retired {
            return Ok(true);
        }
        if leg.scope.is_none() {
            drop(leg);
            return self.try_retire_unbound_context();
        }
        if !leg.cancelled && !matches!(leg.phase, ProbePhase::Finished) {
            return Ok(false);
        }
        if !self.retire_context_input(&mut leg)? {
            return Ok(false);
        }
        if let Some(input) = leg.input.as_mut() {
            input.close_after_exit()?;
            leg.input = None;
        }
        if let Some(permit) = leg.permit.take() {
            permit.revoke_unused()?;
        }
        if let Some(authority) = leg.authority.take() {
            authority.exited()?;
        }
        if leg.role.is_some() {
            if !leg.consumed {
                leg.report
                    .as_ref()
                    .expect("original Context report")
                    .consume(context_consumer())?;
                leg.consumed = true;
            }
        } else {
            // Construction took no opaque capture and never filled a probe.
            // The actual partial owner can positively dispose unused metadata.
            if !leg.partial_report_revoked {
                if let Some(report) = &leg.report {
                    report.revoke_unused()?;
                }
                leg.partial_report_revoked = true;
            }
            if !leg.partial_body_exited {
                if let Some(work) = leg.work.as_mut() {
                    work.body_exited()?;
                }
                leg.partial_body_exited = true;
            }
            if !leg.partial_captures_exited {
                if let Some(work) = leg.work.as_mut() {
                    work.captures_exited()?;
                }
                leg.partial_captures_exited = true;
            }
            if !leg.partial_report_body_exited {
                if let Some(report) = &leg.report {
                    report.body_exited()?;
                }
                leg.partial_report_body_exited = true;
            }
            if !leg.partial_report_captures_exited {
                if let Some(report) = &leg.report {
                    report.captures_exited()?;
                }
                leg.partial_report_captures_exited = true;
            }
            if let Some(report) = &leg.report
                && !report.try_retire()?
            {
                return Ok(false);
            }
            leg.report = None;
        }
        if let Some(scope) = &leg.scope {
            self.ledger.reap_scope_ready(scope.id(), 1)?;
        }
        if leg
            .retirement
            .as_ref()
            .is_some_and(|observer| !observer.is_retired())
        {
            self.notice.notify();
            self.runtime
                .notify_host_actor(&self.registration)
                .unwrap_or_else(|_| std::process::abort());
            return Ok(false);
        }
        if let Some(role) = &leg.role
            && role.phase() != ExitRolePhase::Retired
            && !role.try_retire()?
        {
            return Ok(false);
        }
        Self::release(&mut leg.zero)?;
        Self::release(&mut leg.escrow)?;
        leg.work = None;
        leg.retired = true;
        self.context_retired.store(true, Ordering::Release);
        self.notice.notify();
        Ok(true)
    }
    /// This is deliberately not a consumer of the Context's report. Root's
    /// original RPC keeps these paid input/batch/effect units until actual
    /// Root consumption and effect retirement, even after Context removal.
    pub(crate) fn try_retire_root(&self) -> Result<bool, EstablishRouteError> {
        if !self.context_retired.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut leg = self.root_leg.lock().expect("original Root route return");
        if leg.retired {
            return Ok(true);
        }
        if let Some(input) = &leg.input
            && let Some(input) = input.in_flight()?
        {
            match self.runtime.retire_input(self.root, input) {
                Ok(()) => {}
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        if let Some(input) = leg.input.as_mut() {
            input.close_after_exit()?;
            leg.input = None;
        }
        if let Some(permit) = leg.permit.take() {
            permit.revoke_unused()?;
        }
        leg.retired = true;
        Ok(true)
    }
}
