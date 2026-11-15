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

//! One serial logical execution owner. All work below is passive state or an
//! ordered intent; the carrier owns waiting, original resources and actual exit.

mod actions;
mod attempts;
mod convergence;
mod facts;
mod native;
mod recovery;
mod results;

use crate::{
    CoreFault, EffectSink, LogicalActionId, StepBudget,
    cancel::LogicalCancellationReason,
    convergence::{ConvergenceRecord, ObligationPresence},
    effect::{
        ConsumerEffect, GovernanceEffect, LogicalEffect, LogicalEffectBody, NativeEffect,
        RelayEffect,
    },
    establish::{EstablishIssueLedger, EstablishLedgerBuilder},
    input::{
        LogicalInput, ObligationKind, QualificationIdentity, RegistryClose,
        RegistryParticipationConvergence,
    },
    pre_ready::PreReadyObservations,
    ready::InternalReady,
    replacement::{PreparationWindow, QualificationState},
    root_control::RootControl,
    stand_down::ParticipationStandDownLedger,
    start::{LogicalConclusion, LogicalOutputMode, LogicalStart},
    state::LogicalExecutionState,
    visibility::VisibilityState,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceClosure, AttemptConvergenceMode,
    AttemptRequestToken, ExecutionFailure, MonotonicInstant, OperationIds, QueryExecutionError,
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_types::QueryExecutionId;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU64,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvariantViolation {
    State,
    Activation,
    OpenAuthorityAfterConclusion,
    VisibleRecovery,
    MissingActionResult,
    PrematureRetirement,
}

struct ClosurePass {
    original: Arc<AttemptConvergenceClosure>,
    index: usize,
    validating: bool,
    retiring: bool,
    seen: BTreeSet<ParticipationRef>,
}

struct Attempt {
    activation: AttemptActivationIdentity,
    participations: Arc<[ParticipationRef]>,
    establish: EstablishIssueLedger,
    stand_down: ParticipationStandDownLedger,
    convergence: ConvergenceRecord,
    root: Option<RootControl>,
    activated: bool,
    relay_started: bool,
    relay_issued: bool,
    native_run_failed: bool,
    native_run_failure: Option<crate::input::FailureReport>,
    native_run_complete: bool,
    closing: Option<AttemptConvergenceMode>,
    closing_notified: bool,
    stop_notified: bool,
    terminal_cut: Option<MonotonicInstant>,
    remote_deadline: Option<MonotonicInstant>,
    initialization_cursor: Option<ParticipationRef>,
    initialized: bool,
    pending_registry: BTreeMap<ParticipationRef, RegistryParticipationConvergence>,
    closure: Option<ClosurePass>,
    diagnostic_emitted: bool,
    relay_stopping: bool,
    runtime_filters: Option<Arc<novarocks_coordination_contract::RuntimeFilterDiagnostic>>,
    ready_cut_recorded: bool,
    tracking_cursor: Option<ParticipationRef>,
    tracking_pass_done: bool,
    pending_withdrawals: Option<(
        Arc<[novarocks_coordination_contract::ResultParticipationWithdrawal]>,
        usize,
    )>,
}

impl Attempt {
    fn new(
        activation: AttemptActivationIdentity,
        participations: Arc<[ParticipationRef]>,
        establish: EstablishIssueLedger,
        start: &LogicalStart,
    ) -> Self {
        let stand_down = ParticipationStandDownLedger::new(
            activation,
            establish.shared_required(),
            start.budgets.abort_per_participation,
        );
        Self {
            activation,
            participations,
            establish,
            stand_down,
            convergence: ConvergenceRecord::new(
                activation,
                start.policy.output != LogicalOutputMode::CompletionOnly,
                true,
                ObligationPresence {
                    running_work: true,
                    retired_attempt: false,
                },
            ),
            root: None,
            activated: false,
            relay_started: false,
            relay_issued: false,
            native_run_failed: false,
            native_run_failure: None,
            native_run_complete: false,
            closing: None,
            closing_notified: false,
            stop_notified: false,
            terminal_cut: None,
            remote_deadline: None,
            initialization_cursor: None,
            initialized: false,
            pending_registry: BTreeMap::new(),
            closure: None,
            diagnostic_emitted: false,
            relay_stopping: false,
            runtime_filters: None,
            ready_cut_recorded: false,
            tracking_cursor: None,
            tracking_pass_done: false,
            pending_withdrawals: None,
        }
    }
}

struct Recovery {
    failed: AttemptActivationIdentity,
    replacement: QueryExecutionId,
    failure: ExecutionFailure,
    prepared: Option<Arc<[ParticipationRef]>>,
    identity: Option<QualificationIdentity>,
    qualification: Option<QualificationState>,
    builder: Option<EstablishLedgerBuilder>,
    build_cursor: usize,
    import_cursor: usize,
    reservation_activated: bool,
    candidate: Option<EstablishIssueLedger>,
    inherited: Vec<novarocks_coordination_contract::AttemptInheritedAdmission>,
    roster_validated: bool,
    prepare_finished: bool,
    prepare_issued: bool,
    qualification_issued: bool,
    abandoning: bool,
    dormant_converged: bool,
}

#[derive(Clone, Copy)]
enum NativeAction {
    Activate(AttemptActivationIdentity),
    Run(AttemptActivationIdentity),
    Prepare(QueryExecutionId),
    Qualify(QualificationIdentity),
    ActivateReservation(QualificationIdentity),
    CancelQualification(QualificationIdentity),
    AbandonReservation(QualificationIdentity),
    Resources(AttemptActivationIdentity),
    Dormant(QueryExecutionId),
    RecordCut,
    RecordSuccess,
    RecordReady(AttemptActivationIdentity),
    RetirePreparation,
}
#[derive(Clone, Copy)]
enum ConsumerAction {
    Schema,
    EndSlot(AttemptActivationIdentity),
    End(AttemptActivationIdentity),
    Output,
    Failure,
    InternalReply,
    Ready,
    Diagnostics,
    Convergence,
}
#[derive(Clone, Copy)]
enum GovernanceAction {
    Register(AttemptActivationIdentity, ObligationKind),
    Resolve(AttemptActivationIdentity, ObligationKind),
    EndTracking(AttemptActivationIdentity, ObligationKind),
    ReleaseStage,
    Cancel,
    CompleteOwner,
}
#[derive(Clone, Copy)]
enum ActionTag {
    Native(NativeAction),
    Relay(AttemptActivationIdentity),
    Consumer(ConsumerAction),
    Governance(GovernanceAction),
}
#[derive(Clone, Copy)]
struct RelayWaiter {
    source: crate::LogicalSourceId,
    token: crate::LogicalRequestToken,
    activation: AttemptActivationIdentity,
}
struct Action {
    tag: ActionTag,
    result_received: bool,
    exited: bool,
}

/// Dynamic state is bounded by original attempts, participations, authorization
/// budgets and the carrier's prepaid source positions. No independent queue or
/// topology quota is introduced. Fanout is represented by resumable cursors.
pub struct LogicalExecutionCore {
    start: LogicalStart,
    state: LogicalExecutionState,
    attempts: BTreeMap<QueryExecutionId, Attempt>,
    visibility: VisibilityState,
    ready: Option<InternalReady>,
    pre_ready: PreReadyObservations,
    pre_ready_cursor: Option<QueryExecutionId>,
    refresh_pre_ready: bool,
    recovery: Option<Recovery>,
    preparation_window: PreparationWindow,
    qualification_history: Vec<QualificationState>,
    ids: OperationIds,
    actions: BTreeMap<LogicalActionId, Action>,
    outbox: VecDeque<LogicalEffect>,
    next_action: u64,
    next_request: u64,
    cancellation: Option<LogicalCancellationReason>,
    scope_cancellation_requested: bool,
    logical_failure: Option<QueryExecutionError>,
    supervision_failure: Option<QueryExecutionError>,
    close: Option<RegistryClose>,
    close_deadline: Option<MonotonicInstant>,
    shortening_pass: bool,
    shortening_cursor: Option<QueryExecutionId>,
    ingress_closed: bool,
    started: bool,
    output_exposed: bool,
    visibility_waiter: Option<RelayWaiter>,
    end_waiter: Option<RelayWaiter>,
    closing_cursor: Option<QueryExecutionId>,
    closing_pass: bool,
    scan_cursor: Option<QueryExecutionId>,
    scan_dirty: bool,
    end_settled: bool,
    preparation_retiring: bool,
    preparation_retired: bool,
    stage_releasing: bool,
    stage_released: bool,
    owner_completing: bool,
    owner_completed: bool,
    completed_attempts: usize,
    retirable: bool,
    continuation: bool,
    now: MonotonicInstant,
}

impl LogicalExecutionCore {
    pub fn new(start: LogicalStart) -> Self {
        start
            .validate()
            .expect("logical start facts satisfy the frozen contract");
        let initial = start.first_attempt.activation;
        let establish = EstablishIssueLedger::new(
            initial,
            start.first_attempt.participations.iter().copied().collect(),
            start.budgets.admission_issues_per_participation,
            start.budgets.establish_per_participation,
        )
        .expect("initial frozen roster is validated before construction");
        let attempt = Attempt::new(
            initial,
            Arc::from(start.first_attempt.participations.clone()),
            establish,
            &start,
        );
        let state = LogicalExecutionState::new(
            initial,
            start.policy.recovery,
            start.policy.effect,
            start.policy.output,
            start.budgets.max_attempts.get(),
        )
        .expect("validated initial logical policy");
        let visibility = VisibilityState::new(start.policy.output, start.policy.root_carrier);
        let ready = start
            .policy
            .internal_plan
            .map(|plan| InternalReady::new(plan, start.budgets.observation));
        let ids = OperationIds::new(start.id_seed);
        let now = start.started_at;
        Self {
            start,
            state,
            attempts: BTreeMap::from([(initial.execution(), attempt)]),
            visibility,
            ready,
            pre_ready: PreReadyObservations::default(),
            pre_ready_cursor: None,
            refresh_pre_ready: false,
            recovery: None,
            preparation_window: PreparationWindow::default(),
            qualification_history: Vec::new(),
            ids,
            actions: BTreeMap::new(),
            outbox: VecDeque::new(),
            next_action: 1,
            next_request: 1,
            cancellation: None,
            scope_cancellation_requested: false,
            logical_failure: None,
            supervision_failure: None,
            close: None,
            close_deadline: None,
            shortening_pass: false,
            shortening_cursor: None,
            ingress_closed: false,
            started: false,
            output_exposed: false,
            end_settled: false,
            visibility_waiter: None,
            end_waiter: None,
            closing_cursor: None,
            closing_pass: false,
            scan_cursor: None,
            scan_dirty: false,
            preparation_retiring: false,
            preparation_retired: false,
            stage_releasing: false,
            stage_released: false,
            owner_completing: false,
            owner_completed: false,
            completed_attempts: 0,
            retirable: false,
            continuation: false,
            now,
        }
    }

    pub fn handle(
        &mut self,
        now: MonotonicInstant,
        input: LogicalInput,
        budget: &mut dyn StepBudget,
        out: &mut dyn EffectSink,
    ) -> Result<(), CoreFault> {
        assert!(
            now >= self.now,
            "logical monotonic input cannot move backwards"
        );
        self.now = now;
        self.continuation = false;
        if !matches!(input, LogicalInput::Timer) {
            // Preserve the fair cursor, but revisit the earlier prefix after
            // this pass when a new fact may have changed its decisions.
            self.scan_dirty = true;
        }
        self.apply(input)?;
        self.flush(budget, out);
        self.drive(budget)?;
        self.flush(budget, out);
        Ok(())
    }

    fn apply(&mut self, input: LogicalInput) -> Result<(), CoreFault> {
        match input {
            LogicalInput::Started => {
                if self.started {
                    return Err(CoreFault::UnexpectedInput);
                }
                self.started = true;
                let activation = self.start.first_attempt.activation;
                self.queue(LogicalEffectBody::Governance(
                    GovernanceEffect::RegisterObligation {
                        activation,
                        kind: ObligationKind::RunningWork,
                    },
                ));
                if self.start.policy.output == LogicalOutputMode::ResultStream {
                    self.queue(LogicalEffectBody::Consumer(ConsumerEffect::ExposeOutput {
                        activation,
                    }));
                }
                self.queue(LogicalEffectBody::Native(NativeEffect::ActivateAttempt {
                    activation,
                    inherited_admissions: Vec::new(),
                }));
            }
            LogicalInput::AttemptPort(message) => self.apply_attempt(*message)?,
            LogicalInput::ActionResult { action, result } => self.apply_result(action, result)?,
            LogicalInput::ActionExited { action } => self.apply_exit(action)?,
            LogicalInput::Relay {
                source,
                token,
                activation,
                request,
            } => self.apply_relay(source, token, activation, request)?,
            LogicalInput::Internal {
                source,
                token,
                activation,
                request,
            } => self.apply_internal(source, token, activation, request)?,
            LogicalInput::RegistryParticipation {
                activation,
                participation,
                convergence,
            } => {
                let now = self.now;
                let attempt = self.attempt_mut(activation)?;
                if !attempt
                    .establish
                    .required_participations()
                    .contains(&participation)
                {
                    return Err(CoreFault::ForeignExecution);
                }
                if attempt.initialized {
                    attempt
                        .stand_down
                        .observe_registry(now, participation, convergence)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                } else {
                    if convergence == RegistryParticipationConvergence::RemoteTrackingEnded
                        && attempt
                            .remote_deadline
                            .is_none_or(|deadline| now < deadline)
                    {
                        return Err(CoreFault::UnexpectedInput);
                    }
                    // Before initialization this is the same monotonic closure
                    // join as the live ledger: Unknown cannot retract proof.
                    attempt
                        .pending_registry
                        .entry(participation)
                        .and_modify(|old| {
                            if *old == RegistryParticipationConvergence::RemoteTrackingEnded {
                                *old = convergence;
                            }
                        })
                        .or_insert(convergence);
                }
            }
            LogicalInput::Cancelled(reason) => self.cancel(reason),
            LogicalInput::RegistryClose(close) => {
                self.close_deadline = Some(
                    self.close_deadline
                        .map_or(self.now, |old| old.min(self.now)),
                );
                self.shortening_pass = true;
                self.shortening_cursor = None;
                if self.close != Some(RegistryClose::Shutdown) {
                    self.close = Some(close);
                }
                if (close == RegistryClose::Shutdown || self.state.conclusion().is_none())
                    && !self.scope_cancellation_requested
                {
                    self.scope_cancellation_requested = true;
                    self.queue(LogicalEffectBody::Governance(
                        GovernanceEffect::RequestShutdownCancellation,
                    ));
                }
            }
            LogicalInput::RegistryJoinReadiness {
                source,
                token,
                close_ingress,
            } => {
                let ready = self.protocol_join_ready();
                if ready && close_ingress {
                    self.ingress_closed = true;
                }
                self.queue(LogicalEffectBody::RegistryJoinReply {
                    source,
                    token,
                    ready,
                });
            }
            LogicalInput::ConsumerClosed { source: _ } => {
                if !self.scope_cancellation_requested
                    && matches!(
                        self.state.conclusion(),
                        None | Some(LogicalConclusion::ComputationReady)
                    )
                {
                    self.scope_cancellation_requested = true;
                    self.queue(LogicalEffectBody::Governance(
                        GovernanceEffect::RequestClientCancellation,
                    ));
                }
            }
            LogicalInput::Snapshot { source, token } => {
                self.queue(LogicalEffectBody::SnapshotReply { source, token })
            }
            LogicalInput::Timer => {}
        }
        Ok(())
    }

    fn attempt_mut(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<&mut Attempt, CoreFault> {
        if activation.execution().query_id() != self.start.identity {
            return Err(CoreFault::ForeignExecution);
        }
        let held = self
            .attempts
            .get_mut(&activation.execution())
            .ok_or(CoreFault::ForeignActivation)?;
        if held.activation != activation {
            return Err(CoreFault::ForeignActivation);
        }
        Ok(held)
    }
    fn queue(&mut self, body: LogicalEffectBody) {
        let action = self.issue_action();
        self.queue_reserved(action, body);
    }
    fn queue_reserved(&mut self, action: LogicalActionId, body: LogicalEffectBody) {
        self.outbox.push_back(LogicalEffect { action, body });
    }
    fn fresh_action(&self) -> LogicalActionId {
        LogicalActionId::new(
            NonZeroU64::new(self.next_action).expect("logical action identity remains nonzero"),
        )
    }
    fn issue_action(&mut self) -> LogicalActionId {
        let id = self.fresh_action();
        self.next_action = self
            .next_action
            .checked_add(1)
            .expect("logical action identity exhausted");
        id
    }
    fn issue_request(&mut self) -> AttemptRequestToken {
        let id = AttemptRequestToken::new(
            NonZeroU64::new(self.next_request).expect("logical request identity remains nonzero"),
        );
        self.next_request = self
            .next_request
            .checked_add(1)
            .expect("logical request identity exhausted");
        id
    }
    fn flush(&mut self, budget: &mut dyn StepBudget, out: &mut dyn EffectSink) {
        while !self.outbox.is_empty() {
            if !budget.charge(1) {
                self.continuation = true;
                break;
            }
            let LogicalEffect { action, body } = self
                .outbox
                .pop_front()
                .expect("checked nonempty ordered effect intents");
            if self.state.conclusion().is_some() && Self::cancelled_business(&body) {
                self.discard_unissued_business(&body);
                continue;
            }
            match &body {
                LogicalEffectBody::Native(NativeEffect::PrepareSuccessor {
                    replacement, ..
                }) => {
                    self.recovery
                        .as_mut()
                        .filter(|r| r.replacement == *replacement)
                        .expect("one original queued preparation")
                        .prepare_issued = true;
                }
                LogicalEffectBody::Native(NativeEffect::DispatchQualification {
                    identity, ..
                }) => {
                    self.recovery
                        .as_mut()
                        .filter(|r| r.identity == Some(*identity))
                        .expect("one original queued qualification")
                        .qualification_issued = true;
                }
                LogicalEffectBody::Relay(RelayEffect::Start { activation }) => {
                    self.attempts
                        .get_mut(&activation.execution())
                        .expect("original relay attempt")
                        .relay_issued = true;
                }
                _ => {}
            }
            if let Some(tag) = Self::tag(&body) {
                self.actions.insert(
                    action,
                    Action {
                        tag,
                        result_received: false,
                        exited: false,
                    },
                );
            }
            out.emit(LogicalEffect { action, body });
        }
    }
    fn discard_unissued_business(&mut self, body: &LogicalEffectBody) {
        match body {
            LogicalEffectBody::Native(NativeEffect::PrepareSuccessor { replacement, .. }) => {
                if self
                    .recovery
                    .as_ref()
                    .is_some_and(|r| r.replacement == *replacement && !r.prepare_issued)
                {
                    // No host ever received this intent, so no candidate or
                    // preparation work exists. This is not an exit receipt.
                    self.recovery = None;
                }
            }
            LogicalEffectBody::Relay(RelayEffect::Start { activation }) => {
                let attempt = self
                    .attempts
                    .get_mut(&activation.execution())
                    .expect("queued relay retains its attempt");
                assert!(
                    !attempt.relay_issued,
                    "only unissued relay work can be discarded"
                );
                attempt.relay_started = false;
                attempt.convergence.observe_relay_stopped(true);
            }
            _ => {}
        }
    }
    fn cancelled_business(body: &LogicalEffectBody) -> bool {
        matches!(
            body,
            LogicalEffectBody::Native(
                NativeEffect::ActivateAttempt { .. }
                    | NativeEffect::RunAttempt { .. }
                    | NativeEffect::PrepareSuccessor { .. }
                    | NativeEffect::DispatchQualification { .. }
                    | NativeEffect::ActivateReservation { .. }
            ) | LogicalEffectBody::Relay(RelayEffect::Start { .. })
        )
    }

    pub fn facts(&self) -> crate::facts::LogicalFacts<'_> {
        crate::facts::LogicalFacts::new(self)
    }
}
