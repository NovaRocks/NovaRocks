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

//! One original RPC lends prepaid storage to its actual Participation. The two
//! neutral input legs retire independently; Participation retirement never waits
//! for the Root to consume its already accepted backchannel input.

mod comparison;
use comparison::ComparisonState;
pub(crate) use comparison::EstablishComparisonJob;

use super::{
    actors::{ControlMessage, ParticipationMessage, WorkerAction, WorkerActor, WorkerMessage},
    control_request::ControlRequestNotice,
    establish_ingress::EstablishIngressOwner,
    participation_host_jobs::ParticipationHostJobs,
    participation_ready::ParticipationReadyNotice,
    reported_work_admission::adopt_reported_work,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    actor_turn::{
        ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket, InputAuthor,
        ReservedInputObligation, StandingInputId, StandingInputObligation,
    },
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    header_readiness::{HeaderReadyError, HeaderReadyRegistration, HeaderWaitState},
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    transaction::{DeclaredInput, RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ActionId, ActorId, ConsumerId, InputId, ScopedWorkRef},
    mailbox::InputClass,
    scope::WorkKind,
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_worker_protocol::{
    control::{ControlCore, ControlInput},
    creation::RequestReplyTag,
    participation::{
        EstablishCredentialComparison, EstablishReplayRequest, EstablishReplayResult,
        ParticipationEffect, ParticipationInput,
    },
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

pub(crate) const PARTICIPATION_ROUTE_ESCROW_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: crate::control_runtime::actors::ParticipationActor::MAX_STEP_ACTIONS,
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
fn participation_consumer() -> ConsumerId {
    ConsumerId::new(1).expect("one original Participation route observer")
}

#[derive(Debug)]
pub(crate) enum EstablishRouteError {
    ActorTurn(ActorTurnError),
    Runtime(RuntimeError),
    Exit(ExitCustodyError),
    WrongOwner,
    TargetRemoved,
    WrongEffect,
    WrongPhase,
    Capacity,
    ComparisonUnresolved,
    HeaderReady(HeaderReadyError),
}
impl std::fmt::Display for EstablishRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ActorTurn(error) => write!(f, "Establish route driver failed: {error}"),
            Self::Runtime(error) => write!(f, "Establish route runtime failed: {error}"),
            Self::Exit(error) => write!(f, "Establish route exit failed: {error}"),
            Self::HeaderReady(error) => write!(f, "Establish route header wait failed: {error}"),
            Self::WrongOwner => f.write_str("Establish route has a different original owner"),
            Self::TargetRemoved => f.write_str("Establish route target was removed"),
            Self::WrongEffect => f.write_str("Establish route received a different effect"),
            Self::WrongPhase => f.write_str("Establish route phase does not allow this transition"),
            Self::Capacity => f.write_str("Establish route original storage is unavailable"),
            Self::ComparisonUnresolved => f.write_str("Establish route comparison is unresolved"),
        }
    }
}
impl std::error::Error for EstablishRouteError {}
impl From<ActorTurnError> for EstablishRouteError {
    fn from(error: ActorTurnError) -> Self {
        Self::ActorTurn(error)
    }
}
impl From<RuntimeError> for EstablishRouteError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl From<ExitCustodyError> for EstablishRouteError {
    fn from(error: ExitCustodyError) -> Self {
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
struct ParticipationLeg {
    scope: Option<ScopeHandle>,
    escrow: Option<ReservationObligation>,
    work: Option<WorkObligation>,
    work_id: Option<ScopedWorkRef>,
    retirement: Option<WorkRetirementObserver>,
    header_ready: Option<HeaderReadyRegistration>,
    zero: Option<ReservationObligation>,
    report: Option<ReportSlot>,
    exit_custody: Option<ExitCustody>,
    authority: Option<ExitObligation>,
    permit: Option<ReservedInputObligation<WorkerActor>>,
    input: Option<StandingInputObligation<WorkerActor>>,
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
    jobs: Option<ParticipationHostJobs>,
    participation_notice: Option<ParticipationReadyNotice>,
    bound: bool,
    comparison: ComparisonState,
    owner_failed_returned: bool,
}
#[derive(Default)]
struct RootLeg {
    permit: Option<ReservedInputObligation<WorkerActor>>,
    input: Option<StandingInputObligation<WorkerActor>>,
    retired: bool,
    standing_input_id: Option<StandingInputId>,
}

/// Both inventories hold this same original owner. Neither clone issues a
/// request, admits capacity or duplicates a Work/exit authority. There is no
/// operation-result cache here: Waiting only suspends a future neutral probe.
#[must_use = "Retain both original input legs until their real retirement"]
pub(crate) struct EstablishRouteCustody {
    runtime: ActorRuntime<WorkerActor>,
    ledger: RuntimeLedger,
    root: ActorId,
    root_author: InputAuthor,
    participation: ParticipationRef,
    actor: ActorId,
    registration: ActorRegistration,
    request: EstablishReplayRequest,
    notice: ControlRequestNotice,
    ingress: EstablishIngressOwner,
    participation_leg: Mutex<ParticipationLeg>,
    participation_retired: AtomicBool,
    root_leg: Mutex<RootLeg>,
}
impl EstablishRouteCustody {
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        ledger: RuntimeLedger,
        root: ActorId,
        root_author: InputAuthor,
        participation: ParticipationRef,
        registration: ActorRegistration,
        request: EstablishReplayRequest,
        escrow: ReservationObligation,
        notice: ControlRequestNotice,
        ingress: EstablishIngressOwner,
    ) -> Result<Arc<Self>, (EstablishRouteError, ReservationObligation)> {
        if root_author.actor() != root || root_author.work() != request.request.author {
            return Err((EstablishRouteError::WrongOwner, escrow));
        }
        if let Err(error) = runtime.validate_registration(&registration) {
            return Err((EstablishRouteError::ActorTurn(error), escrow));
        }
        let actor = registration.actor();
        Ok(Arc::new(Self {
            runtime,
            ledger,
            root,
            root_author,
            participation,
            actor,
            registration,
            request,
            notice,
            ingress,
            participation_leg: Mutex::new(ParticipationLeg {
                scope: None,
                escrow: Some(escrow),
                work: None,
                work_id: None,
                retirement: None,
                header_ready: None,
                zero: None,
                report: None,
                exit_custody: None,
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
                jobs: None,
                participation_notice: None,
                bound: false,
                comparison: ComparisonState::Idle,
                owner_failed_returned: false,
            }),
            participation_retired: AtomicBool::new(false),
            root_leg: Mutex::new(RootLeg::default()),
        }))
    }
    /// Qualify the immutable target without taking either original leg lock.
    /// Equal public IDs cannot replace the admitted Participation registration.
    pub(crate) fn target_matches(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
        participation: ParticipationRef,
        root: ActorId,
    ) -> bool {
        self.root == root
            && self.runtime.same_runtime(runtime)
            && self.registration.same_registration(registration)
            && self.participation == participation
    }
    pub(crate) fn request(&self) -> RequestReplyTag {
        self.request.request
    }
    pub(crate) fn participation_retired(&self) -> bool {
        self.participation_retired.load(Ordering::Acquire)
    }
    /// Address of this original return input, retained after its real close so
    /// the Root owner can remove precisely its progress-index entry.
    pub(super) fn return_input_id(&self) -> Option<StandingInputId> {
        self.root_leg
            .lock()
            .expect("original Root route return")
            .standing_input_id
    }
    /// Root stores this same Arc before initializing its original backchannel.
    /// This touches only Root's leg and never borrows a Participation owner or scope.
    pub(crate) fn initialize_root(
        &self,
        root_author: &WorkObligation,
    ) -> Result<(), EstablishRouteError> {
        let mut root = self.root_leg.lock().expect("original Root route return");
        if root.retired
            || root.permit.is_some()
            || root.input.is_some()
            || root.standing_input_id.is_some()
            || self.participation_retired.load(Ordering::Acquire)
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
        root.input = Some(match permit.into_standing_input() {
            Ok(input) => input,
            Err((error, permit)) => {
                root.permit = Some(permit);
                return Err(error.into());
            }
        });
        root.standing_input_id = Some(root.input.as_ref().expect("original return role").id());
        drop(root);
        self.initialize_participation_funding(root_author)
    }
    /// The same original R pays its leg from the process parent. Retained
    /// Participation actors have no execution Scope or lifetime Work to borrow.
    fn initialize_participation_funding(
        &self,
        parent: &WorkObligation,
    ) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        if !matches!(leg.phase, ProbePhase::Constructing)
            || leg.scope.is_some()
            || leg.work_id.is_some()
            || leg.cancelled
            || leg.retired
            || self.participation_retired.load(Ordering::Acquire)
        {
            return Err(EstablishRouteError::WrongPhase);
        }
        self.runtime.validate_registration(&self.registration)?;
        {
            // Keep the existing ContextLeg -> RootLeg order whenever both
            // original legs are needed. Root initialization takes Root only.
            let root = self.root_leg.lock().expect("original Root route return");
            if root.retired
                || root.permit.is_some()
                || root.input.is_none()
                || root.standing_input_id.is_none()
            {
                return Err(EstablishRouteError::WrongPhase);
            }
        }
        let scope = parent.scope().clone();
        // The Participation now owns all further partial cleanup, even if its next
        // admission refuses. Root may retire only a still-unbound cancelled leg.
        leg.scope = Some(scope.clone());
        let generation = scope.gates()?.generation;
        // Transfer only the still-unbound parcel to the true Participation parent,
        // then atomically admit its role. The original Work is never reparented.
        let escrow = leg.escrow.as_mut().expect("original RPC route parcel");
        escrow.transfer(parent)?;
        let resources = escrow.resources()?;
        let reported = scope.reserve_child_from(
            escrow,
            parent,
            DeclaredInput {
                generation,
                kind: WorkKind::Settlement,
                resources,
                consumers: vec![(participation_consumer(), ConsumerKind::Observer)],
            },
        )?;
        let (work, capacity, report) = reported.into_parts();
        leg.work_id = Some(work.reference());
        leg.retirement = Some(work.retirement_observer());
        leg.work = Some(work);
        leg.report = Some(report);
        leg.zero = leg.escrow.take();
        leg.escrow = Some(capacity);
        leg.header_ready = Some(
            leg.work
                .as_mut()
                .expect("original route Work")
                .header_readiness_registration()?,
        );
        Ok(())
    }
    /// Participation stores its alias before binding its exact target input. The
    /// financial Work stays with the same Root/R even after hot Scope removal.
    pub(crate) fn initialize_participation(&self) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        if !leg.bound
            || !matches!(leg.phase, ProbePhase::Constructing)
            || leg.exit_custody.is_some()
            || leg.input.is_some()
            || leg.cancelled
        {
            return Err(EstablishRouteError::WrongPhase);
        }
        self.runtime.validate_registration(&self.registration)?;
        leg.permit = Some(
            self.runtime.reserve_admitted_input_for(
                &self.registration,
                self.root,
                InputClass::Control,
                leg.work
                    .as_ref()
                    .expect("actual Participation input author"),
            )?,
        );
        let permit = leg
            .permit
            .take()
            .expect("actual Participation input reservation");
        leg.input = Some(match permit.into_standing_input() {
            Ok(input) => input,
            Err((error, permit)) => {
                leg.permit = Some(permit);
                return Err(error.into());
            }
        });
        let reported = adopt_reported_work(
            leg.work.take().expect("original Participation route Work"),
            leg.escrow.take().expect("transferred original RPC units"),
            leg.report
                .as_ref()
                .expect("original Participation report")
                .clone(),
        );
        let (role, authority) = ExitCustody::from_admitted(reported);
        leg.exit_custody = Some(role);
        leg.authority = Some(authority);
        leg.authority
            .as_mut()
            .expect("original route authority")
            .start()?;
        self.issue_probe(&mut leg, None)
    }
    fn issue_probe(
        &self,
        leg: &mut ParticipationLeg,
        comparison: Option<EstablishCredentialComparison>,
    ) -> Result<(), EstablishRouteError> {
        if leg.cancelled || leg.retired {
            return Err(EstablishRouteError::WrongPhase);
        }
        let command = WorkerMessage::Participation(ParticipationMessage::Command {
            work: leg.work_id.ok_or(EstablishRouteError::WrongPhase)?,
            request: None,
            command: ParticipationInput::ProbeEstablish {
                participation: self.participation,
                request: self.request,
                comparison,
            },
        });
        leg.input
            .as_ref()
            .ok_or(EstablishRouteError::WrongPhase)?
            .fill(command)
            .map_err(|(error, _neutral)| EstablishRouteError::ActorTurn(error))?;
        leg.handoff = None;
        leg.phase = ProbePhase::InFlight;
        Ok(())
    }
    /// Only local cleanup metadata is runnable. A live probe still awaiting
    /// its original Core decision must park rather than spin a host poller.
    pub(crate) fn participation_cleanup_ready(&self) -> bool {
        let leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        leg.retired
            || leg.cancelled
            || matches!(leg.phase, ProbePhase::Finished)
            || matches!(leg.comparison, ComparisonState::Returned(_))
            || matches!(
                leg.phase,
                ProbePhase::Waiting(EstablishReplayResult::AwaitMaterialization)
            ) && leg
                .jobs
                .as_ref()
                .is_some_and(ParticipationHostJobs::actual_establishment_decided)
    }
    /// The Participation metadata lane calls this before completing its original
    /// effect. Successful fill transfers only neutral responsibility to Root;
    /// no Root consumption or physical host exit is inferred.
    pub(crate) fn accept_probe(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), EstablishRouteError> {
        self.runtime.validate_effect_ticket(ticket)?;
        let WorkerAction::Participation(ParticipationEffect::EstablishProbed {
            participation,
            request,
            result,
            decided_at,
        }) = &ticket.effect
        else {
            return Err(EstablishRouteError::WrongEffect);
        };
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        let key = (ticket.input, ticket.descriptor.id);
        if ticket.actor != self.actor
            || *participation != self.participation
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
                .fill_from_effect_with_author(
                    ticket,
                    root.input.as_ref().ok_or(EstablishRouteError::WrongPhase)?,
                    &self.root_author,
                    WorkerMessage::Control(ControlMessage::Command {
                        work: self.root_author.work(),
                        request: None,
                        command: ControlInput::RoutedEstablishFinished {
                            participation: self.participation,
                            request: self.request.request,
                            result: Ok(outcome.outcome),
                            acknowledgement: Some(outcome.clone()),
                            decided_at: *decided_at,
                        },
                    }),
                )
                .map_err(|(error, _neutral)| EstablishRouteError::ActorTurn(error))?;
            ProbePhase::Finished
        } else {
            ProbePhase::Waiting(result.clone())
        };
        leg.handoff = Some(key);
        Ok(())
    }
    pub(crate) fn cancel(&self) {
        self.participation_leg
            .lock()
            .expect("original Participation route")
            .cancelled = true;
        self.notify_progress();
    }
    fn retire_participation_input(
        &self,
        leg: &mut ParticipationLeg,
    ) -> Result<bool, EstablishRouteError> {
        if let Some(input) = &leg.input
            && let Some(input) = input.in_flight()?
        {
            match self.runtime.retire_input(self.actor, input) {
                Ok(()) => {}
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }
    fn release(capacity: &mut Option<ReservationObligation>) -> Result<(), RuntimeError> {
        if let Some(capacity) = capacity.as_mut() {
            capacity.release_quota(&capacity.resources()?.quota)?;
            capacity.finish()?;
        }
        *capacity = None;
        Ok(())
    }
    /// Only cancelled metadata that never entered a Participation scope can be
    /// withdrawn by Root. No Participation Work, report or body exit is fabricated.
    pub(crate) fn try_retire_unbound_participation(&self) -> Result<bool, EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        if leg.bound || !leg.cancelled {
            return Ok(false);
        }
        if leg.scope.is_some() {
            drop(leg);
            return self.try_retire_participation();
        }
        if leg.retired {
            return Ok(true);
        }
        Self::release(&mut leg.escrow)?;
        leg.phase = ProbePhase::Finished;
        leg.retired = true;
        self.participation_retired.store(true, Ordering::Release);
        drop(leg);
        self.notice.notify();
        Ok(true)
    }
    pub(crate) fn try_retire_participation(&self) -> Result<bool, EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original Participation route");
        if leg.retired {
            return Ok(true);
        }
        if leg.scope.is_none() {
            drop(leg);
            return self.try_retire_unbound_participation();
        }
        if !leg.cancelled && !matches!(leg.phase, ProbePhase::Finished) {
            return Ok(false);
        }
        // A cancelled input is not the exit of its actual comparison body,
        // raw Source loan, Participation instance or exclusive route authority.
        if !matches!(leg.comparison, ComparisonState::Idle) {
            return Ok(false);
        }
        if !self.retire_participation_input(&mut leg)? {
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
        if leg.exit_custody.is_some() {
            if !leg.consumed {
                leg.report
                    .as_ref()
                    .expect("original Participation report")
                    .consume(participation_consumer())?;
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
        if let Some(observer) = leg.retirement.clone()
            && !self.ledger.reap_observed_header(&observer)?
        {
            let wait = leg
                .header_ready
                .as_mut()
                .expect("same original route header registration")
                .arm(&self.runtime, &self.registration, &observer)
                .map_err(EstablishRouteError::HeaderReady)?;
            match wait {
                HeaderWaitState::Waiting => return Ok(false),
                HeaderWaitState::ReadyToReap if !self.ledger.reap_observed_header(&observer)? => {
                    return Ok(false);
                }
                HeaderWaitState::ReadyToReap | HeaderWaitState::Retired => {}
            }
        }
        if let Some(ready) = &mut leg.header_ready {
            ready.cancel();
        }
        if let Some(role) = &leg.exit_custody
            && role.phase() != ExitCustodyPhase::Retired
            && !role.try_retire()?
        {
            return Ok(false);
        }
        Self::release(&mut leg.zero)?;
        Self::release(&mut leg.escrow)?;
        leg.work = None;
        leg.retired = true;
        self.participation_retired.store(true, Ordering::Release);
        self.notice.notify();
        Ok(true)
    }
    /// This is deliberately not a consumer of the Participation's report. Root's
    /// original RPC keeps these paid input/batch/effect units until actual
    /// Root consumption and effect retirement, even after Participation removal.
    pub(crate) fn try_retire_root(&self) -> Result<bool, EstablishRouteError> {
        if !self.participation_retired.load(Ordering::Acquire) {
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
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
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
