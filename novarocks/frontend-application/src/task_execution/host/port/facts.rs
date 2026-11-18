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

//! Three original AP-to-LP positions. Only the fact role serializes ordered
//! lifecycle facts; authorization and pre-ready each have one ordinary role.
//! The caller retains its exclusive source ticket until synchronous handoff.

mod failed_receiver;

use crate::control_runtime::actor::{FrontendActor, FrontendEffect, FrontendInput};
use crate::query_execution::logical_binding::OriginalAttemptObservation;
pub(crate) use failed_receiver::FailedReceiverLoanObligation;
use failed_receiver::FailedReceiverState;
use novarocks_control_runtime::{
    actor_turn::{
        ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket, StandingInputObligation,
    },
    obligation::WorkObligation,
};
use novarocks_control_runtime_core::{identity::InputId, mailbox::InputClass};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceClosure,
    logical_port::{AttemptToLogical, AttemptToLogicalBody},
};
use novarocks_logical_execution_protocol::input::LogicalInput;
use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    task::Waker,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FactRole {
    Lifecycle,
    Authorization,
    PreReady,
}
impl FactRole {
    pub(crate) const ALL: [Self; 3] = [Self::Lifecycle, Self::Authorization, Self::PreReady];
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Lifecycle => 0,
            Self::Authorization => 1,
            Self::PreReady => 2,
        }
    }

    pub(crate) fn for_protocol_effect(
        body: &novarocks_attempt_protocol::effect::EffectBody,
    ) -> Option<Self> {
        use novarocks_attempt_protocol::effect::EffectBody;
        use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Body;
        match body {
            EffectBody::EstablishAuthorization { .. } => Some(Self::Authorization),
            EffectBody::Port(message) => Some(match &message.body {
                Body::AuthorizationRequest { .. } | Body::EstablishSettlement { .. } => {
                    Self::Authorization
                }
                Body::PreReadyReplacementQuery { .. } => Self::PreReady,
                _ => Self::Lifecycle,
            }),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FactError {
    ForeignActivation,
    WrongRole,
    WrongSource,
    WrongMessage,
    ConflictingCompletion,
    Closed,
    Runtime(ActorTurnError),
}
#[must_use = "Partial binding retains the same original paid standing positions"]
pub(crate) struct FactBindRefusal {
    pub(crate) error: ActorTurnError,
    pub(crate) positions: Vec<StandingInputObligation<FrontendActor>>,
}
impl fmt::Debug for FactBindRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FactBindRefusal")
            .field("error", &self.error)
            .field("positions", &self.positions.len())
            .finish()
    }
}
#[derive(Debug)]
#[must_use = "Retain the rejected original passive fact with its still-owned source ticket"]
pub(crate) struct FactPublishRefusal {
    pub(crate) error: FactError,
    pub(crate) message: AttemptToLogical,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FactProgress {
    pub(crate) units: usize,
    pub(crate) progressed: bool,
    pub(crate) more: bool,
    pub(crate) complete: bool,
}
/// The same immutable AP proof after this bank's original role accepted it.
/// This is neither local Work exit nor an independently constructed verdict.
#[derive(Clone, Debug)]
pub(crate) struct OriginalAttemptCompletionReceipt {
    activation: AttemptActivationIdentity,
    action: AttemptActionId,
    closure: Arc<AttemptConvergenceClosure>,
}
impl OriginalAttemptCompletionReceipt {
    pub(crate) fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    #[cfg(test)]
    pub(crate) fn action(&self) -> AttemptActionId {
        self.action
    }
    pub(crate) fn closure(&self) -> &Arc<AttemptConvergenceClosure> {
        &self.closure
    }
    fn same_original(&self, other: &Self) -> bool {
        self.activation == other.activation
            && self.action == other.action
            && Arc::ptr_eq(&self.closure, &other.closure)
    }
}
/// Minted only after the SAME current-ticket root Publication was handed to
/// the LP inlet (or its original failed-receiver cell). This certifies no LP
/// consumption, visibility, Worker closure or actual work exit.
pub(crate) struct RootStatusFactReceipt {
    activation: AttemptActivationIdentity,
    original: Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
}
impl RootStatusFactReceipt {
    pub(crate) fn confirm(
        self,
        activation: AttemptActivationIdentity,
        original: &Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
    ) -> Result<(), FactError> {
        if self.activation == activation && Arc::ptr_eq(&self.original, original) {
            Ok(())
        } else {
            Err(FactError::WrongMessage)
        }
    }
}
/// Original root reply after real LP fact handoff. The bound root command
/// table must still acknowledge its exact token before an AP ACK can be built.
pub(crate) struct RootReplyFactHandoff {
    activation: AttemptActivationIdentity,
    original: Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope>,
}
impl RootReplyFactHandoff {
    pub(super) fn binding(
        &self,
    ) -> (
        AttemptActivationIdentity,
        novarocks_coordination_contract::AttemptRequestToken,
        bool,
    ) {
        use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Original;
        let novarocks_attempt_protocol::effect::EffectBody::Port(original) =
            &self.original.protocol.body
        else {
            unreachable!("validated original root reply handoff");
        };
        let (token, seal) = match original.body {
            Original::SealResult { token, .. } => (token, true),
            Original::TerminalControlResult { token, .. } => (token, false),
            _ => unreachable!("validated original root reply kind"),
        };
        (self.activation, token, seal)
    }
    pub(super) fn into_original(
        self,
    ) -> Arc<crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope> {
        self.original
    }
}
fn same_root_reply(
    original: &crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    message: &AttemptToLogical,
) -> bool {
    use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Original;
    let novarocks_attempt_protocol::effect::EffectBody::Port(original) = &original.protocol.body
    else {
        return false;
    };
    match (&original.body, &message.body) {
        (
            Original::SealResult {
                token: a,
                result: before,
            },
            AttemptToLogicalBody::SealResult {
                token: b,
                result: after,
            },
        ) => {
            a == b
                && match (before, after) {
                    (Ok(a), Ok(b)) => Arc::ptr_eq(a, b),
                    (Err(_), Err(_)) => true,
                    _ => false,
                }
        }
        (
            Original::TerminalControlResult {
                token: a,
                result: before,
            },
            AttemptToLogicalBody::TerminalControlResult {
                token: b,
                result: after,
            },
        ) => a == b && before.is_ok() == after.is_ok(),
        _ => false,
    }
}
fn root_reply_handoff(
    activation: AttemptActivationIdentity,
    ticket: &EffectTicket<FrontendEffect>,
) -> RootReplyFactHandoff {
    let FrontendEffect::Attempt(original) = &ticket.effect else {
        unreachable!("same root reply ticket")
    };
    RootReplyFactHandoff {
        activation,
        original: original.clone(),
    }
}
fn is_root_status_fact(ticket: &EffectTicket<FrontendEffect>) -> bool {
    use novarocks_attempt_protocol::effect::EffectBody;
    use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Body;
    matches!(&ticket.effect, FrontendEffect::Attempt(effect) if matches!(&effect.protocol.body,
        EffectBody::Port(novarocks_attempt_protocol::port_output::AttemptToLogical { body: Body::RootStatus(_), .. })))
}
fn root_status_receipt(
    activation: AttemptActivationIdentity,
    ticket: &EffectTicket<FrontendEffect>,
) -> RootStatusFactReceipt {
    let FrontendEffect::Attempt(original) = &ticket.effect else {
        unreachable!("the same validated original root Publication ticket");
    };
    RootStatusFactReceipt {
        activation,
        original: original.clone(),
    }
}

struct Slot {
    position: Option<StandingInputObligation<FrontendActor>>,
    submitted: Option<InputId>,
    prepared: Option<OriginalAttemptCompletionReceipt>,
    closed: bool,
    attempt_waiting: bool,
    logical_waiting: bool,
    failure_waiting: bool,
}
struct State {
    slots: [Slot; 3],
    closing: bool,
    cursor: usize,
    remaining: usize,
    rescan_requested: bool,
    live: usize,
    accepted: Option<OriginalAttemptCompletionReceipt>,
    producer_exited: bool,
}
/// O(1) passive projection retained before moving the original message. Only
/// the successful current-ticket commit invokes this observation tail.
struct ConvergenceObservation {
    row: Arc<OriginalAttemptObservation>,
    activation: AttemptActivationIdentity,
    local: bool,
    closure: bool,
    lifecycle: Option<Arc<novarocks_coordination_contract::RuntimeFilterDiagnostic>>,
}
impl ConvergenceObservation {
    fn committed(self) {
        self.row
            .convergence_committed(self.activation, self.local, self.closure, self.lifecycle);
    }
}
pub(crate) struct AttemptFactBank {
    observation: OnceLock<Arc<OriginalAttemptObservation>>,
    activation: AttemptActivationIdentity,
    attempt: ActorRegistration,
    logical: ActorRegistration,
    state: Mutex<State>,
    attempt_wake: Waker,
    logical_wake: Waker,
    changed: Notify,
    owner_failure_changed: Notify,
    owner_failure_waiting: std::sync::atomic::AtomicBool,
    failed_receiver: Mutex<FailedReceiverState>,
}
impl AttemptFactBank {
    pub(crate) fn bind_observation(&self, original: Arc<OriginalAttemptObservation>) {
        assert_eq!(original.activation(), self.activation);
        assert!(
            self.observation.set(original).is_ok(),
            "one original fact-bank observation row"
        );
    }
    fn prepare_observation(&self, message: &AttemptToLogical) -> Option<ConvergenceObservation> {
        let AttemptToLogicalBody::ConvergenceProgress {
            local_exited,
            closure,
            lifecycle_runtime_filters,
        } = &message.body
        else {
            return None;
        };
        self.observation.get().map(|row| ConvergenceObservation {
            row: row.clone(),
            activation: message.activation,
            local: *local_exited,
            closure: closure.is_some(),
            lifecycle: lifecycle_runtime_filters.clone(),
        })
    }
    /// The caller holds the original activated row's author loan. This method
    /// binds only its already-paid positions and never reserves Work or mints
    /// activation authority. Both registrations remain exact original objects.
    pub(crate) fn reserve(
        runtime: &ActorRuntime<FrontendActor>,
        logical: &ActorRegistration,
        attempt: &ActorRegistration,
        activation: AttemptActivationIdentity,
        author: &WorkObligation,
        attempt_wake: Waker,
        logical_wake: Waker,
    ) -> Result<Arc<Self>, FactBindRefusal> {
        let mut positions = Vec::with_capacity(3);
        for class in [
            InputClass::Lifecycle,
            InputClass::Ordinary,
            InputClass::Ordinary,
        ] {
            let reserved =
                match runtime.reserve_admitted_input_for(logical, attempt.actor(), class, author) {
                    Ok(value) => value,
                    Err(ActorTurnError::ActorFailed) => {
                        return Self::bind_failed_receiver(
                            runtime,
                            logical,
                            attempt,
                            activation,
                            author,
                            (attempt_wake, logical_wake),
                            positions,
                        );
                    }
                    Err(error) => return Err(FactBindRefusal { error, positions }),
                };
            match reserved.into_standing_input() {
                Ok(value) => positions.push(value),
                Err((error, unused)) => {
                    unused
                        .revoke_unused()
                        .expect("original unbound fact position has no captures");
                    if error == ActorTurnError::ActorFailed {
                        return Self::bind_failed_receiver(
                            runtime,
                            logical,
                            attempt,
                            activation,
                            author,
                            (attempt_wake, logical_wake),
                            positions,
                        );
                    }
                    return Err(FactBindRefusal { error, positions });
                }
            }
        }
        Ok(Self::from_original_positions(
            logical,
            attempt,
            activation,
            attempt_wake,
            logical_wake,
            positions,
        ))
    }
    fn from_original_positions(
        logical: &ActorRegistration,
        attempt: &ActorRegistration,
        activation: AttemptActivationIdentity,
        attempt_wake: Waker,
        logical_wake: Waker,
        positions: Vec<StandingInputObligation<FrontendActor>>,
    ) -> Arc<Self> {
        let live = positions.len();
        let mut positions = positions.into_iter();
        Arc::new(Self {
            observation: OnceLock::new(),
            activation,
            attempt: attempt.clone(),
            logical: logical.clone(),
            state: Mutex::new(State {
                slots: std::array::from_fn(|_| {
                    let position = positions.next();
                    let closed = position.is_none();
                    Slot {
                        position,
                        submitted: None,
                        prepared: None,
                        closed,
                        attempt_waiting: false,
                        logical_waiting: false,
                        failure_waiting: false,
                    }
                }),
                closing: false,
                cursor: 0,
                remaining: 0,
                rescan_requested: false,
                live,
                accepted: None,
                producer_exited: false,
            }),
            attempt_wake,
            logical_wake,
            changed: Notify::new(),
            owner_failure_changed: Notify::new(),
            owner_failure_waiting: std::sync::atomic::AtomicBool::new(false),
            failed_receiver: Mutex::new(FailedReceiverState::default()),
        })
    }
    pub(crate) fn matches_pair(
        &self,
        logical: &ActorRegistration,
        attempt: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.activation == activation
            && self.logical.same_registration(logical)
            && self.attempt.same_registration(attempt)
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub(crate) fn completion_receipt(&self) -> Option<OriginalAttemptCompletionReceipt> {
        let committed = self.committed_failed_receiver_receipt();
        let mut state = self.lock();
        if let Some(receipt) = committed {
            match &state.accepted {
                Some(old) => assert!(
                    old.same_original(&receipt),
                    "same original committed completion"
                ),
                None => state.accepted = Some(receipt),
            }
        }
        state.accepted.clone()
    }
    /// One persistent observer belongs to the original failure job. notify_one
    /// retains a permit if a real change precedes its next state inspection.
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }
    fn notify_change(&self) {
        self.changed.notify_one();
        self.notify_owner_failure_credit();
    }
    fn notify_owner_failure_credit(&self) {
        if self
            .owner_failure_waiting
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.owner_failure_changed.notify_one();
        }
    }
    /// The original AP failure job owns this distinct readiness position.
    /// Its wait never consumes the LP failure observer's changed() permit.
    pub(crate) async fn publish_owner_lost(
        self: &Arc<Self>,
        runtime: &ActorRuntime<FrontendActor>,
        session: &mut novarocks_control_runtime::publication::FailurePublicationSession<
            FrontendActor,
        >,
        message: AttemptToLogical,
    ) -> Result<(), FactPublishRefusal> {
        let error = if message.activation != self.activation {
            Some(FactError::ForeignActivation)
        } else if !session.registration().same_registration(&self.attempt) {
            Some(FactError::WrongSource)
        } else if !matches!(message.body, AttemptToLogicalBody::AttemptLost { .. }) {
            Some(FactError::WrongMessage)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(FactPublishRefusal { error, message });
        }
        let mut original = Some(message);
        loop {
            let changed = self.owner_failure_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.owner_failure_waiting
                .store(true, std::sync::atomic::Ordering::Release);
            if self.lock().producer_exited {
                return Err(FactPublishRefusal {
                    error: FactError::Closed,
                    message: original.take().unwrap(),
                });
            }
            let state = match runtime.snapshot(self.logical.actor()) {
                Ok(state) => state,
                Err(error) => {
                    return Err(FactPublishRefusal {
                        error: FactError::Runtime(error),
                        message: original.take().unwrap(),
                    });
                }
            };
            let result = if state.failed {
                let proof = match runtime.failure_proof_for(&self.logical) {
                    Ok(proof) => proof,
                    Err(error) => {
                        return Err(FactPublishRefusal {
                            error: FactError::Runtime(error),
                            message: original.take().unwrap(),
                        });
                    }
                };
                let loan = match self.try_failed_receiver_loan(runtime, &proof, FactRole::Lifecycle)
                {
                    Ok(loan) => loan,
                    Err(error) => {
                        return Err(FactPublishRefusal {
                            error: FactError::Runtime(error),
                            message: original.take().unwrap(),
                        });
                    }
                };
                loan.map(|mut loan| {
                    loan.publish_from_failed_owner(session, &proof, original.take().unwrap())
                })
            } else {
                self.try_loan(FactRole::Lifecycle).map(|mut loan| {
                    loan.publish_from_failed_owner(session, original.take().unwrap())
                        .map(|_| ())
                })
            };
            match result {
                Some(Ok(())) => {
                    self.owner_failure_waiting
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(());
                }
                Some(Err(refused))
                    if matches!(
                        refused.error,
                        FactError::Runtime(ActorTurnError::ActorFailed) | FactError::Closed
                    ) =>
                {
                    if runtime.failure_proof_for(&self.logical).is_err() {
                        return Err(refused);
                    }
                    original = Some(refused.message);
                    continue;
                }
                Some(Err(refused)) => return Err(refused),
                None => changed.await,
            }
        }
    }
    /// A temporary Full preserves the caller's original ticket. Only an actual
    /// slot retirement or requested short-loan return wakes that same AP host.
    pub(crate) fn try_loan(self: &Arc<Self>, role: FactRole) -> Option<FactPositionLoanObligation> {
        let mut state = self.lock();
        let closing = state.closing;
        let slot = &mut state.slots[role.index()];
        let busy = slot.submitted.is_some() || slot.prepared.is_some() || slot.position.is_none();
        if closing || slot.closed || busy {
            if !closing && !slot.closed {
                slot.attempt_waiting = true;
            }
            // Copy only passive scalars. Tracing must not run under the bank
            // mutex or expose its original payload/position owners.
            let snapshot = (
                slot.closed,
                slot.submitted,
                slot.prepared.is_some(),
                slot.position.is_some(),
                slot.attempt_waiting,
                slot.logical_waiting,
                slot.failure_waiting,
            );
            drop(state);
            tracing::debug!(
                target: "novarocks_original_wait",
                activation = ?self.activation,
                role = ?role,
                closing,
                closed = snapshot.0,
                submitted = ?snapshot.1,
                prepared = snapshot.2,
                position = snapshot.3,
                attempt_waiting = snapshot.4,
                logical_waiting = snapshot.5,
                failure_waiting = snapshot.6,
                "attempt original fact prepay refused at standing credit"
            );
            return None;
        }
        Some(FactPositionLoanObligation {
            bank: Arc::clone(self),
            role,
            position: slot.position.take(),
            submitted: None,
            prepared: None,
            changed: false,
        })
    }
    pub(crate) fn close_admission(&self) {
        let wakes = {
            let mut state = self.lock();
            if state.closing {
                return;
            }
            state.closing = true;
            state.remaining = 3;
            let mut waiting = false;
            for slot in &mut state.slots {
                waiting |= std::mem::take(&mut slot.attempt_waiting);
            }
            waiting
        };
        self.notify_change();
        if wakes {
            self.attempt_wake.wake_by_ref();
        }
        self.logical_wake.wake_by_ref();
    }
    /// The original AP producer has actually stopped. Closing intake can have
    /// happened earlier when LP failed; that transition cannot supply this
    /// later observation or its wake. Original positions still retire below.
    pub(crate) fn original_producer_exited(&self) {
        self.close_admission();
        let changed = {
            let mut state = self.lock();
            if state.producer_exited {
                false
            } else {
                state.producer_exited = true;
                state.remaining = 3;
                true
            }
        };
        if changed {
            self.notify_change();
            self.logical_wake.wake_by_ref();
        }
    }
    /// A real consumer retirement notification owes a finite role pass.
    /// Preserve it through an older partial pass; it grants no input credit.
    pub(crate) fn request_rescan(&self) {
        let mut state = self.lock();
        if state.live != 0 {
            state.rescan_requested = true;
        }
    }

    /// Called by the LP Source under its bounded port-family scan: the actual
    /// input/effect retirement wakes THIS host first. producer_exited is the
    /// original AP producer's actual exit observation, never LP's generic cut.
    pub(crate) fn progress(
        self: &Arc<Self>,
        budget: usize,
        producer_exited: bool,
        failed: bool,
    ) -> Result<FactProgress, ActorTurnError> {
        let was_closed = self.is_closed();
        if failed {
            self.close_admission();
        }
        if producer_exited {
            self.lock().producer_exited = true;
        }
        let cleanup_unit = self.progress_failed_receiver(budget, producer_exited)?;
        let units = {
            let mut state = self.lock();
            if state.remaining == 0 && state.live != 0 {
                state.remaining = 3;
                state.rescan_requested = false;
            }
            budget.saturating_sub(cleanup_unit).min(state.remaining)
        };
        let mut progressed = cleanup_unit != 0;
        for _ in 0..units {
            let (index, mut loan) = {
                let mut state = self.lock();
                let index = state.cursor;
                state.cursor = (index + 1) % 3;
                state.remaining -= 1;
                let slot = &mut state.slots[index];
                if slot.closed {
                    continue;
                }
                let Some(position) = slot.position.take() else {
                    slot.logical_waiting = true;
                    continue;
                };
                (
                    index,
                    FactPositionLoanObligation {
                        bank: Arc::clone(self),
                        role: match index {
                            0 => FactRole::Lifecycle,
                            1 => FactRole::Authorization,
                            _ => FactRole::PreReady,
                        },
                        position: Some(position),
                        submitted: None,
                        prepared: slot.prepared.take(),
                        changed: false,
                    },
                )
            };
            let actual = loan
                .position
                .as_ref()
                .expect("same original fact position")
                .in_flight()?;
            let recovered_retirement = {
                let mut state = self.lock();
                let slot = &mut state.slots[index];
                match (slot.submitted, actual) {
                    (Some(old), Some(real)) if old != real => {
                        return Err(ActorTurnError::InputAuthorMismatch);
                    }
                    (None, Some(real)) => {
                        slot.submitted = Some(real);
                        false
                    }
                    (Some(_), None) => {
                        slot.submitted = None;
                        true
                    }
                    _ => false,
                }
            };
            if let Some(input) = actual {
                loan.accept_prepared(input);
            }
            // An interrupted fill with no observed occurrence is UNKNOWN.
            // Only an explicit unsubmitted refusal can withdraw Prepared.
            let unresolved_preparation = loan.prepared.is_some();
            let position = loan.position.as_mut().expect("same original fact position");
            let mut retired = recovered_retirement;
            if actual.is_some() {
                if failed {
                    match position.take_failed_role_payload() {
                        Ok(Some(original)) => {
                            original.dispose()?;
                            progressed = true;
                        }
                        Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                        Err(error) => return Err(error),
                    }
                }
                if position.retire_ready_after_exit()? {
                    self.lock().slots[index].submitted = None;
                    retired = true;
                    progressed = true;
                }
            }
            if retired {
                loan.changed = true;
                progressed = true;
                let wake = std::mem::take(&mut self.lock().slots[index].attempt_waiting);
                if wake {
                    self.attempt_wake.wake_by_ref();
                }
            }
            let close = {
                let state = self.lock();
                !unresolved_preparation
                    && state.closing
                    && producer_exited
                    && state.slots[index].submitted.is_none()
                    && self.failed_receiver_retired()
            };
            if close {
                position.close_after_exit()?;
                {
                    let mut state = self.lock();
                    state.slots[index].closed = true;
                    state.live -= 1;
                }
                progressed = true;
                loan.changed = true;
            }
        }
        let mut state = self.lock();
        if state.remaining == 0 && state.live != 0 && std::mem::take(&mut state.rescan_requested) {
            state.remaining = 3;
        }
        let progress = FactProgress {
            units: units + cleanup_unit,
            progressed,
            more: state.remaining != 0 && state.live != 0,
            complete: state.live == 0 && state.producer_exited && self.failed_receiver_retired(),
        };
        drop(state);
        // This includes partial bindings with no original standing positions:
        // their actual passive-cell retirement is still the closure author.
        if progress.complete && !was_closed {
            self.attempt_wake.wake_by_ref();
        }
        Ok(progress)
    }
    pub(crate) fn is_closed(&self) -> bool {
        let state = self.lock();
        state.live == 0 && state.producer_exited && self.failed_receiver_retired()
    }
}

/// Checked by Drop: every branch returns the same standing role before waking
/// either owner. It owns no ticket and supplies no producer/capture exit proof.
#[must_use = "Return the same original fact-position loan"]
pub(crate) struct FactPositionLoanObligation {
    bank: Arc<AttemptFactBank>,
    role: FactRole,
    position: Option<StandingInputObligation<FrontendActor>>,
    submitted: Option<InputId>,
    prepared: Option<OriginalAttemptCompletionReceipt>,
    changed: bool,
}
impl FactPositionLoanObligation {
    fn accept_prepared(&mut self, _: InputId) {
        let Some(prepared) = self.prepared.take() else {
            return;
        };
        {
            let mut state = self.bank.lock();
            if let Some(accepted) = &state.accepted {
                assert!(
                    accepted.same_original(&prepared),
                    "same original accepted AP completion"
                );
            } else {
                state.accepted = Some(prepared);
            }
        }
        self.changed = true;
        self.bank.notify_change();
    }
    fn prepare_completion(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        message: &AttemptToLogical,
    ) -> Result<(), FactError> {
        let checked = (|| {
            if message.activation != self.bank.activation {
                return Err(FactError::ForeignActivation);
            }
            if role_for(&message.body) != self.role {
                return Err(FactError::WrongRole);
            }
            if self.bank.lock().closing {
                return Err(FactError::Closed);
            }
            let origin = runtime
                .registration_from_effect(ticket)
                .map_err(FactError::Runtime)?;
            if !origin.same_registration(&self.bank.attempt) {
                return Err(FactError::WrongSource);
            }
            let FrontendEffect::Attempt(effect) = &ticket.effect else {
                return Err(FactError::WrongMessage);
            };
            let original = checked_original_message(effect, self.bank.activation, message)?;
            let prepared = failed_receiver::exact_original_completion(original, message)?;
            if self.prepared.is_some()
                || self.submitted.is_some()
                || self
                    .position
                    .as_ref()
                    .expect("same original fact loan")
                    .in_flight()
                    .map_err(FactError::Runtime)?
                    .is_some()
            {
                return Err(FactError::Runtime(ActorTurnError::StandingInputBusy));
            }
            if let Some(prepared) = &prepared
                && self
                    .bank
                    .lock()
                    .accepted
                    .as_ref()
                    .is_some_and(|old| !old.same_original(prepared))
            {
                return Err(FactError::ConflictingCompletion);
            }
            Ok(prepared)
        })();
        self.prepared = checked?;
        self.changed = self.prepared.is_some();
        Ok(())
    }
    /// Only the original AP failure session can publish its own lost fact.
    /// No normal Core ticket or positive convergence receipt is constructed.
    pub(crate) fn publish_from_failed_owner(
        &mut self,
        session: &mut novarocks_control_runtime::publication::FailurePublicationSession<
            FrontendActor,
        >,
        message: AttemptToLogical,
    ) -> Result<InputId, FactPublishRefusal> {
        let error = if message.activation != self.bank.activation {
            Some(FactError::ForeignActivation)
        } else if self.role != FactRole::Lifecycle {
            Some(FactError::WrongRole)
        } else if !session.registration().same_registration(&self.bank.attempt) {
            Some(FactError::WrongSource)
        } else if !matches!(message.body, AttemptToLogicalBody::AttemptLost { .. }) {
            Some(FactError::WrongMessage)
        } else if self.bank.lock().closing {
            Some(FactError::Closed)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(FactPublishRefusal { error, message });
        }
        let input = FrontendInput::Logical(Box::new(LogicalInput::AttemptPort(Box::new(message))));
        match session.fill_standing(
            self.position
                .as_ref()
                .expect("same paid failure fact position"),
            None,
            input,
        ) {
            Ok(input) => {
                self.submitted = Some(input);
                self.changed = true;
                Ok(input)
            }
            Err((error, FrontendInput::Logical(input))) => {
                let LogicalInput::AttemptPort(original) = *input else {
                    unreachable!("same original failure fact")
                };
                Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message: *original,
                })
            }
            Err(_) => unreachable!("same original failed-owner payload"),
        }
    }
    pub(crate) fn publish_root_reply_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        message: AttemptToLogical,
    ) -> Result<RootReplyFactHandoff, FactPublishRefusal> {
        let valid = matches!(&ticket.effect, FrontendEffect::Attempt(effect) if same_root_reply(effect, &message));
        if !valid {
            return Err(FactPublishRefusal {
                error: FactError::WrongMessage,
                message,
            });
        }
        self.publish_now(runtime, ticket, message)?;
        Ok(root_reply_handoff(self.bank.activation, ticket))
    }
    pub(crate) fn publish_root_status_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        message: AttemptToLogical,
    ) -> Result<RootStatusFactReceipt, FactPublishRefusal> {
        if !is_root_status_fact(ticket)
            || !matches!(message.body, AttemptToLogicalBody::RootStatus(_))
        {
            return Err(FactPublishRefusal {
                error: FactError::WrongMessage,
                message,
            });
        }
        self.publish_now(runtime, ticket, message)?;
        Ok(root_status_receipt(self.bank.activation, ticket))
    }
    pub(crate) fn publish_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        message: AttemptToLogical,
    ) -> Result<InputId, FactPublishRefusal> {
        if let Err(error) = self.prepare_completion(runtime, ticket, &message) {
            return Err(FactPublishRefusal { error, message });
        }
        let observation = self.bank.prepare_observation(&message);
        let input = FrontendInput::Logical(Box::new(LogicalInput::AttemptPort(Box::new(message))));
        match runtime.fill_from_effect(
            ticket,
            self.position.as_ref().expect("same original fact loan"),
            input,
        ) {
            Ok(input) => {
                self.submitted = Some(input);
                self.changed = true;
                self.accept_prepared(input);
                if let Some(original) = observation {
                    original.committed();
                }
                Ok(input)
            }
            Err((error, FrontendInput::Logical(input))) => {
                let LogicalInput::AttemptPort(message) = *input else {
                    unreachable!("original passive fact payload")
                };
                // The kernel returned the original unsubmitted value. Unlike
                // unwind after commit, this typed branch proves no handoff.
                self.prepared = None;
                self.changed = false;
                Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message: *message,
                })
            }
            Err(_) => unreachable!("kernel returns the original fact payload"),
        }
    }
}
impl Drop for FactPositionLoanObligation {
    fn drop(&mut self) {
        let (attempt, logical, failed) = {
            let mut state = self.bank.lock();
            let slot = &mut state.slots[self.role.index()];
            assert!(slot.position.is_none(), "one original fact-position loan");
            slot.position = self.position.take();
            assert!(
                slot.prepared.is_none(),
                "one original completion preparation"
            );
            slot.prepared = self.prepared.take();
            if let Some(input) = self.submitted {
                assert!(
                    slot.submitted.replace(input).is_none(),
                    "one original fact occurrence"
                );
            }
            let logical = std::mem::take(&mut slot.logical_waiting);
            let attempt = if slot.submitted.is_none() && slot.prepared.is_none() {
                std::mem::take(&mut slot.attempt_waiting)
            } else {
                false
            };
            let failed = std::mem::take(&mut slot.failure_waiting);
            (attempt, logical, failed)
        };
        wake_returned(|| {
            if self.changed || failed {
                self.bank.notify_change();
            } else {
                self.bank.notify_owner_failure_credit();
            }
            if attempt {
                self.bank.attempt_wake.wake_by_ref();
            }
            if logical {
                self.bank.logical_wake.wake_by_ref();
            }
        });
    }
}
fn same_arc<T>(left: &Option<Arc<T>>, right: &Option<Arc<T>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}
fn wake_returned(wake: impl FnOnce()) {
    if std::thread::panicking() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wake));
    } else {
        wake();
    }
}
/// Validate the SAME AP port effect before publishing; compare its complete
/// status/failure value without constructing another protocol message or
/// cloning rich metadata. No retired Publication opcode is accepted.
fn checked_original_message<'a>(
    effect: &'a crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    activation: AttemptActivationIdentity,
    message: &AttemptToLogical,
) -> Result<
    &'a novarocks_attempt_protocol::port_output::AttemptToLogical<
        novarocks_attempt_protocol::failure::AttemptFailure,
    >,
    FactError,
> {
    use novarocks_attempt_protocol::effect::EffectBody;
    if message.activation != activation || message.action != effect.protocol.id {
        return Err(FactError::WrongMessage);
    }
    let original = match &effect.protocol.body {
        EffectBody::Port(original)
        | EffectBody::EstablishAuthorization {
            request: original, ..
        } => original,
        _ => return Err(FactError::WrongMessage),
    };
    if let novarocks_attempt_protocol::port_output::AttemptToLogicalBody::RootStatus(root) =
        &original.body
        && !matches!(&message.body, AttemptToLogicalBody::RootStatus(actual) if actual == root)
    {
        return Err(FactError::WrongMessage);
    }
    if original.activation != message.activation || original.action != message.action {
        return Err(FactError::WrongMessage);
    }
    if matches!(
        original.body,
        novarocks_attempt_protocol::port_output::AttemptToLogicalBody::SealResult { .. }
            | novarocks_attempt_protocol::port_output::AttemptToLogicalBody::TerminalControlResult { .. }
    ) && !same_root_reply(effect, message)
    {
        return Err(FactError::WrongMessage);
    }
    Ok(original)
}

pub(crate) fn role_for(body: &AttemptToLogicalBody) -> FactRole {
    match body {
        AttemptToLogicalBody::AuthorizationRequest { .. }
        | AttemptToLogicalBody::EstablishSettlement { .. } => FactRole::Authorization,
        AttemptToLogicalBody::PreReadyReplacementQuery { .. } => FactRole::PreReady,
        _ => FactRole::Lifecycle,
    }
}

#[cfg(test)]
mod tests;
