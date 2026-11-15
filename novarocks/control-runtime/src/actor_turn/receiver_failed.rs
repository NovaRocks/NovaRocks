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

//! A receiver-failed handoff retains passive publisher custody without
//! delivering business input to the fenced receiver.

use super::*;
use crate::publication::{PublishedSnapshot, ReceiverFailedSnapshotObligation};

impl<A: Actor> ActorRuntime<A> {
    /// Bind a passive cell to the SAME paid standing position and its original
    /// sender. No new position, Work, input or capacity is reserved here.
    pub fn receiver_failed_snapshot_for<T>(
        &self,
        position: &StandingInputObligation<A>,
        sender: &ActorRegistration,
        initial: Arc<T>,
    ) -> Result<ReceiverFailedSnapshotObligation<T>, (ActorTurnError, Arc<T>)> {
        let checked = (|| {
            self.validate_registration(sender)?;
            let receiver = position.original_node()?;
            if !Arc::ptr_eq(&self.shared, &position.runtime.shared) || sender.actor == receiver.id {
                return Err(ActorTurnError::PublicationTargetMismatch);
            }
            let inbox = receiver.inbox.lock().expect("receiver inbox lock");
            let role = inbox
                .standing_inputs
                .get(&position.id())
                .ok_or(ActorTurnError::UnknownStandingInput)?;
            if !position.active || role.source != sender.actor {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            Ok(ActorRegistration {
                actor: receiver.id,
                attachment: receiver.attachment,
                scope: receiver.scope.clone(),
                origin: receiver.origin.clone(),
            })
        })();
        match checked {
            Ok(receiver) => Ok(ReceiverFailedSnapshotObligation {
                sender: sender.clone(),
                receiver,
                role: Some(position.id()),
                header: None,
                snapshot: PublishedSnapshot::new_for_actor(sender, initial),
            }),
            Err(error) => Err((error, initial)),
        }
    }

    /// A late original publisher may retain passive custody when its receiver
    /// failed before the paid positions were bound. Borrow only the SAME live
    /// prepaid receiver Work; pinning its header grants no input capacity.
    pub fn receiver_failed_snapshot_for_admitted_work<T>(
        &self,
        failure: &ActorFailureProof,
        sender: &ActorRegistration,
        work: &crate::obligation::WorkObligation,
        initial: Arc<T>,
    ) -> Result<ReceiverFailedSnapshotObligation<T>, (ActorTurnError, Arc<T>)> {
        let checked = (|| {
            self.validate_registration(sender)?;
            let receiver = self.node(failure.registration.actor)?;
            failure.validate_consumer(failure.registration(), &work.scope().runtime)?;
            let inbox = receiver.inbox.lock().expect("receiver inbox lock");
            if sender.actor == receiver.id
                || !receiver.scope.current_scope().same_scope(work.scope())
                || !Arc::ptr_eq(&receiver.origin, &failure.registration.origin)
                || !inbox.failed
                || inbox.generation != failure.failed_epoch
            {
                return Err(ActorTurnError::PublicationTargetMismatch);
            }
            let mut ledger = work
                .scope()
                .runtime
                .shared
                .state
                .lock()
                .expect("runtime ledger lock");
            let financial = ledger
                .scopes
                .get(&work.scope().id())
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            let record = financial
                .work(work.id())
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            if record.unbound_guard
                || record.body_exited
                || record.captures_exited
                || ledger.unresolved.contains(&(work.scope().id(), work.id()))
            {
                return Err(ActorTurnError::InputAuthorInactive);
            }
            let header =
                crate::obligation::WorkHeaderObligation::for_work_locked(work, &mut ledger)?;
            Ok(header)
        })();
        match checked {
            Ok(header) => Ok(ReceiverFailedSnapshotObligation {
                sender: sender.clone(),
                receiver: failure.registration.clone(),
                role: None,
                header: Some(header),
                snapshot: PublishedSnapshot::new_for_actor(sender, initial),
            }),
            Err(error) => Err((error, initial)),
        }
    }

    /// Atomically qualify the original healthy publisher ticket, original paid
    /// receiver position and exact receiver failure fence before custody moves.
    /// This is a snapshot publication, never a successful receiver input fill.
    pub fn commit_receiver_failed_snapshot<T>(
        &self,
        ticket: &EffectTicket<A::Effect>,
        failure: &ActorFailureProof,
        target: &ReceiverFailedSnapshotObligation<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let checked = (|| {
            let source = self.node(ticket.actor)?;
            let receiver = self.node(target.receiver.actor)?;
            if source.id == receiver.id
                || !target.sender.same_registration(&ActorRegistration {
                    actor: source.id,
                    attachment: source.attachment,
                    scope: source.scope.clone(),
                    origin: source.origin.clone(),
                })
            {
                return Err(ActorTurnError::PublicationTargetMismatch);
            }
            Ok((source, receiver))
        })();
        let (source, receiver) = match checked {
            Ok(pair) => pair,
            Err(error) => return Err((error, next)),
        };
        let result = if source.id < receiver.id {
            let mut source_inbox = source.inbox.lock().expect("publisher inbox lock");
            let receiver_inbox = receiver.inbox.lock().expect("receiver inbox lock");
            self.commit_receiver_failed_locked(
                (&source, &mut source_inbox),
                (&receiver, &receiver_inbox),
                ticket,
                failure,
                target,
                next,
            )
        } else {
            let receiver_inbox = receiver.inbox.lock().expect("receiver inbox lock");
            let mut source_inbox = source.inbox.lock().expect("publisher inbox lock");
            self.commit_receiver_failed_locked(
                (&source, &mut source_inbox),
                (&receiver, &receiver_inbox),
                ticket,
                failure,
                target,
                next,
            )
        };
        if result.is_ok() {
            self.shared
                .state
                .lock()
                .expect("runtime scheduler lock")
                .effect_ready
                .notify(ticket.actor)
                .expect("live publication actor");
            self.shared.changed.notify();
        }
        result
    }

    fn commit_receiver_failed_locked<T>(
        &self,
        source: (&ActorCell<A>, &mut Inbox<A>),
        receiver: (&ActorCell<A>, &Inbox<A>),
        ticket: &EffectTicket<A::Effect>,
        failure: &ActorFailureProof,
        target: &ReceiverFailedSnapshotObligation<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let next = ManuallyDrop::new(next);
        let execution_scope = source.0.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let checked = (|| {
            self.validate_publication(source.0, source.1, &ledger, ticket, &target.snapshot.owner)?;
            Self::validate_target_binding(
                source.0,
                source.1,
                &ledger,
                Some(ticket),
                &target.snapshot.owner,
                &target.snapshot.binding,
                false,
            )?;
            Self::validate_receiver_failed_target(source.0, receiver, &ledger, failure, target)?;
            Ok(())
        })();
        if let Err(error) = checked {
            return Err((error, ManuallyDrop::into_inner(next)));
        }
        let mut value = target
            .snapshot
            .value
            .lock()
            .expect("published snapshot lock");
        if let Err(error) = source
            .1
            .batches
            .get_mut(&ticket.input)
            .expect("validated publication batch")
            .state
            .published(ticket.descriptor.id)
        {
            return Err((error.into(), ManuallyDrop::into_inner(next)));
        }
        self.shared.record(|| HistoryFact::Effect {
            target: source.0.history_target(),
            input: ticket.input,
            action: ticket.descriptor.id,
            actor_generation: ticket.generation,
            financial_work: ticket.descriptor.work,
            author_generation: ticket.author_generation,
            phase: novarocks_control_runtime_core::history::EffectHistoryPhase::Published,
        });
        // No callback or destructor follows the publication fence while locked.
        let old = ManuallyDrop::new(std::mem::replace(
            &mut *value,
            ManuallyDrop::into_inner(next),
        ));
        drop(value);
        drop(ledger);
        Ok(ManuallyDrop::into_inner(old))
    }
    fn validate_receiver_failed_target<T>(
        source: &ActorCell<A>,
        receiver: (&ActorCell<A>, &Inbox<A>),
        ledger: &crate::transaction::LedgerState,
        failure: &ActorFailureProof,
        target: &ReceiverFailedSnapshotObligation<T>,
    ) -> Result<(), ActorTurnError> {
        failure.validate_consumer(&target.receiver, &source.scope.current_scope().runtime)?;
        if receiver.0.attachment != target.receiver.attachment
            || !Arc::ptr_eq(&receiver.0.origin, &target.receiver.origin)
            || !Arc::ptr_eq(&receiver.0.scope, &target.receiver.scope)
            || !receiver.1.failed
            || receiver.1.generation != failure.failed_epoch
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        if let Some(id) = target.role {
            let role = receiver
                .1
                .standing_inputs
                .get(&id)
                .ok_or(ActorTurnError::UnknownStandingInput)?;
            if role.source != source.id || role.in_flight.is_some() {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
        } else {
            let header = target
                .header
                .as_ref()
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            let work = header.reference();
            let financial = ledger
                .scopes
                .get(&work.scope)
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            let record = financial
                .work(work.work)
                .ok_or(ActorTurnError::InputAuthorInactive)?;
            if !header.belongs_to(&receiver.0.scope.current_scope())
                || record.unbound_guard
                || record.body_exited
                || record.captures_exited
                || ledger.unresolved.contains(&(work.scope, work.work))
            {
                return Err(ActorTurnError::InputAuthorInactive);
            }
        }
        Ok(())
    }
}

impl<A: Actor> FailurePublicationSession<A> {
    /// A failed original publisher may hand off passive cleanup observations
    /// under its SAME active failure session. This grants no positive business
    /// authority and does not deliver an input to the failed receiver.
    pub fn commit_receiver_failed_snapshot<T>(
        &mut self,
        failure: &ActorFailureProof,
        target: &ReceiverFailedSnapshotObligation<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let checked = (|| {
            let node = self.runtime.node(self.actor)?;
            let receiver = self.runtime.node(target.receiver.actor)?;
            if node.id == receiver.id || !target.sender.same_registration(&self.registration) {
                return Err(ActorTurnError::PublicationTargetMismatch);
            }
            Ok((node, receiver))
        })();
        let (node, receiver) = match checked {
            Ok(pair) => pair,
            Err(error) => return Err((error, next)),
        };
        let result = if node.id < receiver.id {
            let mut inbox = node.inbox.lock().expect("publisher inbox lock");
            let receiver_inbox = receiver.inbox.lock().expect("receiver inbox lock");
            self.commit_receiver_failed_locked(
                (&node, &mut inbox),
                (&receiver, &receiver_inbox),
                failure,
                target,
                next,
            )
        } else {
            let receiver_inbox = receiver.inbox.lock().expect("receiver inbox lock");
            let mut inbox = node.inbox.lock().expect("publisher inbox lock");
            self.commit_receiver_failed_locked(
                (&node, &mut inbox),
                (&receiver, &receiver_inbox),
                failure,
                target,
                next,
            )
        };
        if result.is_ok() {
            self.runtime.shared.changed.notify();
        }
        result
    }

    fn commit_receiver_failed_locked<T>(
        &mut self,
        source: (&ActorCell<A>, &mut Inbox<A>),
        destination: (&ActorCell<A>, &Inbox<A>),
        failure: &ActorFailureProof,
        target: &ReceiverFailedSnapshotObligation<T>,
        next: Arc<T>,
    ) -> Result<Arc<T>, (ActorTurnError, Arc<T>)> {
        let (node, inbox) = source;
        let (receiver, receiver_inbox) = destination;
        let next = ManuallyDrop::new(next);
        let execution_scope = node.scope.current_scope();
        let ledger = execution_scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let action = match self
            .validate(node, inbox, &ledger, &target.snapshot.owner)
            .and_then(|()| {
                ActorRuntime::<A>::validate_target_binding(
                    node,
                    inbox,
                    &ledger,
                    None,
                    &target.snapshot.owner,
                    &target.snapshot.binding,
                    true,
                )
            })
            .and_then(|()| {
                ActorRuntime::<A>::validate_receiver_failed_target(
                    node,
                    (receiver, receiver_inbox),
                    &ledger,
                    failure,
                    target,
                )
            })
            .and_then(|()| Self::next_publication_action(inbox))
        {
            Ok(action) => action,
            Err(error) => return Err((error, ManuallyDrop::into_inner(next))),
        };
        let mut value = target
            .snapshot
            .value
            .lock()
            .expect("published snapshot lock");
        inbox.failure_action_watermark = action.get();
        self.runtime.shared.record(|| HistoryFact::FailurePublication {
            target: node.history_target(),
            actor_generation: self.generation,
            financial_work: self.work,
            financial_generation: match self.claim {
                FailurePublicationOrigin::StartedCleanup { financial_generation, .. } => financial_generation,
                FailurePublicationOrigin::SupervisorEffect { .. } => ledger.scopes
                    .get(&self.work.scope).expect("the validated original failure Work Scope").generation(),
            },
            action,
            origin: match self.claim {
                FailurePublicationOrigin::SupervisorEffect { input, action } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::SupervisorEffect {
                        input, action,
                    },
                FailurePublicationOrigin::StartedCleanup { header, .. } =>
                    novarocks_control_runtime_core::history::FailurePublicationHistoryOrigin::StartedCleanup {
                        header,
                    },
            },
        });
        // All fallible metadata work precedes the no-callback/no-destructor
        // actual swap. Failure and readers are still excluded by these gates.
        let old = ManuallyDrop::new(std::mem::replace(
            &mut *value,
            ManuallyDrop::into_inner(next),
        ));
        drop(value);
        drop(ledger);
        Ok(ManuallyDrop::into_inner(old))
    }
}
