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

//! The same bank's passive handoff after its original LP receiver is fenced.
//! No failed Core is polled or filled; absent standing positions stay absent.

use super::*;
use novarocks_control_runtime::{
    actor_turn::{ActorFailureProof, FailurePublicationSession},
    publication::{ReceiverFailedSnapshotObligation, ReceiverFailedSnapshotReader},
};

#[derive(Default)]
pub(super) struct FailedReceiverState {
    cell: Option<ReceiverFailedSnapshotObligation<FailedReceiverFact>>,
    reader: Option<ReceiverFailedSnapshotReader<FailedReceiverFact>>,
    loaned: bool,
    retired: bool,
    retiring: bool,
}
#[derive(Default)]
struct FailedReceiverFact {
    // Keep the actual committed original, including non-completion observations.
    original: Option<AttemptToLogical>,
    completion: Option<OriginalAttemptCompletionReceipt>,
}

impl AttemptFactBank {
    pub(super) fn bind_failed_receiver(
        runtime: &ActorRuntime<FrontendActor>,
        logical: &ActorRegistration,
        attempt: &ActorRegistration,
        activation: AttemptActivationIdentity,
        author: &WorkObligation,
        wakes: (Waker, Waker),
        positions: Vec<StandingInputObligation<FrontendActor>>,
    ) -> Result<Arc<Self>, FactBindRefusal> {
        let proof = match runtime.failure_proof_for(logical) {
            Ok(proof) => proof,
            Err(error) => return Err(FactBindRefusal { error, positions }),
        };
        let cell = match runtime.receiver_failed_snapshot_for_admitted_work(
            &proof,
            attempt,
            author,
            Arc::new(FailedReceiverFact::default()),
        ) {
            Ok(cell) => cell,
            Err((error, _)) => return Err(FactBindRefusal { error, positions }),
        };
        let bank = Self::from_original_positions(
            logical, attempt, activation, wakes.0, wakes.1, positions,
        );
        {
            let mut failed = bank
                .failed_receiver
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            failed.reader = Some(cell.reader());
            failed.cell = Some(cell);
        }
        bank.close_admission();
        Ok(bank)
    }

    /// Borrow one original passive cell. A normal-position loan or unresolved
    /// handoff must first return and retire before this same role can publish.
    pub(crate) fn try_failed_receiver_loan(
        self: &Arc<Self>,
        runtime: &ActorRuntime<FrontendActor>,
        proof: &ActorFailureProof,
        role: FactRole,
    ) -> Result<Option<FailedReceiverLoanObligation>, ActorTurnError> {
        if !proof.registration().same_registration(&self.logical) {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        let mut state = self.lock();
        let slot = &state.slots[role.index()];
        if state.producer_exited {
            return Ok(None);
        }
        if !slot.closed && slot.position.is_none() {
            state.slots[role.index()].failure_waiting = true;
            state.slots[role.index()].attempt_waiting = true;
            return Ok(None);
        }
        if slot.submitted.is_some() || slot.prepared.is_some() {
            state.slots[role.index()].attempt_waiting = true;
            return Ok(None);
        }
        if let Some(position) = slot.position.as_ref()
            && position.in_flight()?.is_some()
        {
            state.slots[role.index()].attempt_waiting = true;
            return Ok(None);
        }
        let mut failed = self
            .failed_receiver
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if failed.retired || failed.loaned || failed.retiring {
            return Ok(None);
        }
        if failed.cell.is_none() {
            // A healthy binding always owns its original Lifecycle role. Late
            // partial bindings installed their prepaid-Work cell at construction.
            let Some(original) = state.slots[0].position.as_ref() else {
                state.slots[0].failure_waiting = true;
                state.slots[0].attempt_waiting = true;
                return Ok(None);
            };
            failed.cell = Some(
                runtime
                    .receiver_failed_snapshot_for(
                        original,
                        &self.attempt,
                        Arc::new(FailedReceiverFact::default()),
                    )
                    .map_err(|(error, _)| error)?,
            );
        }
        if failed.reader.is_none() {
            failed.reader = failed.cell.as_ref().map(|cell| cell.reader());
        }
        let cell = failed.cell.take();
        failed.loaned = true;
        Ok(Some(FailedReceiverLoanObligation {
            bank: Arc::clone(self),
            role,
            cell,
        }))
    }

    pub(super) fn committed_failed_receiver_receipt(
        &self,
    ) -> Option<OriginalAttemptCompletionReceipt> {
        let reader = self
            .failed_receiver
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reader
            .clone();
        reader.and_then(|reader| reader.read().completion.clone())
    }
    pub(super) fn failed_receiver_retired(&self) -> bool {
        let state = self
            .failed_receiver
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        !state.loaned && !state.retiring && (state.retired || state.cell.is_none())
    }
    pub(super) fn claim_failed_receiver_retirement(
        &self,
    ) -> Option<FailedReceiverRetirementObligation<'_>> {
        let state = self.lock();
        if !state.producer_exited {
            return None;
        }
        let mut failed = self
            .failed_receiver
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if failed.loaned || failed.retired || failed.retiring {
            return None;
        }
        let Some(cell) = failed.cell.take() else {
            failed.retired = true;
            return None;
        };
        failed.retiring = true;
        Some(FailedReceiverRetirementObligation { bank: self, cell })
    }
    pub(super) fn progress_failed_receiver(
        &self,
        budget: usize,
        producer_exited: bool,
    ) -> Result<usize, ActorTurnError> {
        if budget == 0 || !producer_exited {
            return Ok(0);
        }
        if let Some(original) = self.claim_failed_receiver_retirement() {
            original.retire()?;
            Ok(1)
        } else {
            Ok(0)
        }
    }
    fn retain_failed_receiver_completion(
        &self,
        original: &FailedReceiverFact,
    ) -> Result<(), ActorTurnError> {
        if let Some(receipt) = &original.completion {
            let mut state = self.lock();
            match &state.accepted {
                Some(old) if !old.same_original(receipt) => {
                    return Err(ActorTurnError::PublicationTargetMismatch);
                }
                Some(_) => {}
                None => state.accepted = Some(receipt.clone()),
            }
        }
        Ok(())
    }
}

/// Explicitly checked by the original bank: an unconsumed obligation leaves
/// retiring=true and its original header unresolved. It has no Drop exit check.
#[must_use = "Retire the same actual passive cell before publishing bank closure"]
pub(super) struct FailedReceiverRetirementObligation<'a> {
    bank: &'a AttemptFactBank,
    cell: ReceiverFailedSnapshotObligation<FailedReceiverFact>,
}
impl FailedReceiverRetirementObligation<'_> {
    pub(super) fn retire(self) -> Result<(), ActorTurnError> {
        let committed = self.cell.read();
        self.bank.retain_failed_receiver_completion(&committed)?;
        drop(self.cell.retire());
        {
            let mut failed = self
                .bank
                .failed_receiver
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                failed.retiring && !failed.loaned && failed.cell.is_none(),
                "same original cell retirement"
            );
            failed.retiring = false;
            failed.retired = true;
        }
        self.bank.notify_change();
        Ok(())
    }
}

impl super::FactPositionLoanObligation {
    /// Convert the still-unsubmitted original position after its exact receiver
    /// failed. This Source already holds the sole producer loan. Use that SAME
    /// paid position to bind the passive cell, without waiting on an unrelated
    /// Lifecycle reader loan or releasing the current AP ticket.
    pub(crate) fn into_failed_receiver(
        self,
        runtime: &ActorRuntime<FrontendActor>,
        proof: &ActorFailureProof,
    ) -> Result<FailedReceiverLoanObligation, (ActorTurnError, Self)> {
        if !proof.registration().same_registration(&self.bank.logical)
            || self.submitted.is_some()
            || self.prepared.is_some()
        {
            return Err((ActorTurnError::PublicationTargetMismatch, self));
        }
        let bound = (|| {
            let state = self.bank.lock();
            let mut failed = self
                .bank
                .failed_receiver
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if state.producer_exited || failed.retired || failed.retiring || failed.loaned {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            if failed.cell.is_none() {
                failed.cell = Some(
                    runtime
                        .receiver_failed_snapshot_for(
                            self.position
                                .as_ref()
                                .expect("same unsubmitted original position"),
                            &self.bank.attempt,
                            Arc::new(FailedReceiverFact::default()),
                        )
                        .map_err(|(error, _)| error)?,
                );
            }
            if failed.reader.is_none() {
                failed.reader = failed.cell.as_ref().map(|cell| cell.reader());
            }
            failed.loaned = true;
            Ok(FailedReceiverLoanObligation {
                bank: self.bank.clone(),
                role: self.role,
                cell: failed.cell.take(),
            })
        })();
        match bound {
            Ok(original) => {
                drop(self);
                Ok(original)
            }
            Err(error) => Err((error, self)),
        }
    }
}

/// Checked by Drop: the same passive cell returns before any notification.
/// No Drop of this loan proves actual AP producer exit or releases its header.
#[must_use = "Return the same original failed-receiver custody cell"]
pub(crate) struct FailedReceiverLoanObligation {
    bank: Arc<AttemptFactBank>,
    role: FactRole,
    cell: Option<ReceiverFailedSnapshotObligation<FailedReceiverFact>>,
}
impl FailedReceiverLoanObligation {
    pub(crate) fn publish_root_reply_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        failure: &ActorFailureProof,
        message: AttemptToLogical,
    ) -> Result<RootReplyFactHandoff, FactPublishRefusal> {
        let valid = matches!(&ticket.effect, FrontendEffect::Attempt(effect) if same_root_reply(effect, &message));
        if !valid {
            return Err(FactPublishRefusal {
                error: FactError::WrongMessage,
                message,
            });
        }
        self.publish_now(runtime, ticket, failure, message)?;
        Ok(root_reply_handoff(self.bank.activation, ticket))
    }
    pub(crate) fn publish_root_status_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        failure: &ActorFailureProof,
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
        self.publish_now(runtime, ticket, failure, message)?;
        Ok(root_status_receipt(self.bank.activation, ticket))
    }
    pub(crate) fn publish_now(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        failure: &ActorFailureProof,
        message: AttemptToLogical,
    ) -> Result<(), FactPublishRefusal> {
        let checked = (|| {
            if message.activation != self.bank.activation {
                return Err(FactError::ForeignActivation);
            }
            if role_for(&message.body) != self.role {
                return Err(FactError::WrongRole);
            }
            if !runtime
                .registration_from_effect(ticket)
                .map_err(FactError::Runtime)?
                .same_registration(&self.bank.attempt)
            {
                return Err(FactError::WrongSource);
            }
            let FrontendEffect::Attempt(effect) = &ticket.effect else {
                return Err(FactError::WrongMessage);
            };
            let original = checked_original_message(effect, self.bank.activation, &message)?;
            let completion = exact_original_completion(original, &message)?;
            if let Some(receipt) = &completion
                && self
                    .bank
                    .lock()
                    .accepted
                    .as_ref()
                    .is_some_and(|old| !old.same_original(receipt))
            {
                return Err(FactError::ConflictingCompletion);
            }
            Ok(completion)
        })();
        let completion = match checked {
            Ok(completion) => completion,
            Err(error) => return Err(FactPublishRefusal { error, message }),
        };
        let cell = self.cell.as_ref().expect("original failed-receiver loan");
        let observation = self.bank.prepare_observation(&message);
        let previous = cell.read();
        let completion = completion.or_else(|| previous.completion.clone());
        let next = Arc::new(FailedReceiverFact {
            original: Some(message),
            completion,
        });
        match runtime.commit_receiver_failed_snapshot(ticket, failure, cell, next) {
            Ok(old) => {
                // Commit and original publication are already immutable before
                // callbacks or replaced values can unwind this caller.
                let original = cell.read();
                self.bank
                    .retain_failed_receiver_completion(&original)
                    .expect("same original committed completion");
                if let Some(original) = observation {
                    original.committed();
                }
                drop(old);
                Ok(())
            }
            Err((error, original)) => {
                let original = Arc::try_unwrap(original)
                    .ok()
                    .expect("unsubmitted original passive fact");
                Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message: original.original.expect("same rejected fact"),
                })
            }
        }
    }

    /// A failed AP may retain its original residual report, never fabricate a
    /// positive convergence proof. Its actual session remains the only author.
    pub(crate) fn publish_from_failed_owner(
        &mut self,
        session: &mut FailurePublicationSession<FrontendActor>,
        failure: &ActorFailureProof,
        message: AttemptToLogical,
    ) -> Result<(), FactPublishRefusal> {
        let error = if message.activation != self.bank.activation {
            Some(FactError::ForeignActivation)
        } else if role_for(&message.body) != self.role {
            Some(FactError::WrongRole)
        } else if !session.registration().same_registration(&self.bank.attempt) {
            Some(FactError::WrongSource)
        } else if !matches!(message.body, AttemptToLogicalBody::AttemptLost { .. }) {
            Some(FactError::WrongMessage)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(FactPublishRefusal { error, message });
        }
        let cell = self.cell.as_ref().expect("original failed-receiver loan");
        let previous = cell.read();
        let next = Arc::new(FailedReceiverFact {
            original: Some(message),
            completion: previous.completion.clone(),
        });
        match session.commit_receiver_failed_snapshot(failure, cell, next) {
            Ok(old) => {
                drop(old);
                Ok(())
            }
            Err((error, original)) => {
                let original = Arc::try_unwrap(original)
                    .ok()
                    .expect("unsubmitted original passive fact");
                Err(FactPublishRefusal {
                    error: FactError::Runtime(error),
                    message: original.original.expect("same rejected fact"),
                })
            }
        }
    }
}
impl Drop for FailedReceiverLoanObligation {
    fn drop(&mut self) {
        // Even an interrupted call can have crossed the real snapshot fence.
        let original = self
            .cell
            .as_ref()
            .expect("same original passive cell")
            .read();
        self.bank
            .retain_failed_receiver_completion(&original)
            .expect("same original committed completion");
        {
            let mut state = self
                .bank
                .failed_receiver
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                state.loaned && state.cell.is_none(),
                "one original passive cell loan"
            );
            state.cell = self.cell.take();
            state.loaned = false;
        }
        wake_returned(|| {
            self.bank.notify_change();
            self.bank.logical_wake.wake_by_ref();
            self.bank.attempt_wake.wake_by_ref();
        });
    }
}

pub(super) fn exact_original_completion<F>(
    original: &novarocks_attempt_protocol::port_output::AttemptToLogical<F>,
    message: &AttemptToLogical,
) -> Result<Option<OriginalAttemptCompletionReceipt>, FactError> {
    use novarocks_attempt_protocol::port_output::AttemptToLogicalBody as Original;
    match (&original.body, &message.body) {
        (
            Original::ConvergenceProgress {
                local_exited: old_local,
                closure: old_closure,
                lifecycle_runtime_filters: old_filters,
            },
            AttemptToLogicalBody::ConvergenceProgress {
                local_exited,
                closure,
                lifecycle_runtime_filters,
            },
        ) => {
            if old_local != local_exited
                || !same_arc(old_closure, closure)
                || !same_arc(old_filters, lifecycle_runtime_filters)
            {
                return Err(FactError::WrongMessage);
            }
            Ok(old_closure
                .as_ref()
                .map(|closure| OriginalAttemptCompletionReceipt {
                    activation: original.activation,
                    action: original.action,
                    closure: Arc::clone(closure),
                }))
        }
        (Original::ConvergenceProgress { .. }, _)
        | (_, AttemptToLogicalBody::ConvergenceProgress { .. }) => Err(FactError::WrongMessage),
        _ => Ok(None),
    }
}
