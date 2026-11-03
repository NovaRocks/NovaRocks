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

//! Atomic exit of the actual capture-free execution metadata owners and the
//! same actor's transition to retained operation. No physical body is canceled.

use super::*;
use crate::obligation::{ReservationObligation, WorkObligation};

impl<A: Actor> ActorHostTurn<A> {
    /// Retire only the caller's actual capture-free metadata Work hierarchy,
    /// then detach this same actor without an intervening protocol turn. The
    /// host has stopped using these metadata owners; this is not authority to
    /// force-exit a Task, callback, capture, report or asynchronous physical body.
    /// Any ordinary refusal leaves every original lease and bundle unchanged.
    pub fn detach_execution_with_metadata(
        &mut self,
        process: &ScopeHandle,
        owners: &mut [(&mut WorkObligation, &mut ReservationObligation)],
    ) -> Result<(), ActorTurnError> {
        self.detach_execution_with_metadata_sources(process, owners, &mut [])
    }
    /// Close the same metadata owners' actual idle Source positions in the
    /// detachment transaction. A protocol turn can neither fill a closed old
    /// Source nor start between its closure and the change to retained mode.
    /// Process-funded roles and the builtin Timer keep their original custody.
    pub fn detach_execution_with_metadata_sources(
        &mut self,
        process: &ScopeHandle,
        owners: &mut [(&mut WorkObligation, &mut ReservationObligation)],
        sources: &mut [&mut StandingInputObligation<A>],
    ) -> Result<(), ActorTurnError> {
        self.detach_execution_metadata(process, owners, sources, None)
    }
    /// Transfer only the actual failed actor's affiliation after its original
    /// publication stage and every input/Source have retired. The host has
    /// stopped using the exclusive capture-free metadata owners supplied here.
    /// Failure never authorizes forced Task, capture, report or callback exit.
    /// The failed Core and this same registration remain fenced and owned.
    pub fn detach_failed_execution_for_custodian(
        &mut self,
        proof: &ActorFailureProof,
        process: &ScopeHandle,
        owners: &mut [(&mut WorkObligation, &mut ReservationObligation)],
    ) -> Result<(), ActorTurnError> {
        self.detach_execution_metadata(process, owners, &mut [], Some(proof))
    }
    fn detach_execution_metadata(
        &mut self,
        process: &ScopeHandle,
        owners: &mut [(&mut WorkObligation, &mut ReservationObligation)],
        sources: &mut [&mut StandingInputObligation<A>],
        failed: Option<&ActorFailureProof>,
    ) -> Result<(), ActorTurnError> {
        if !self.active {
            return Err(ActorTurnError::ActorNotRetired);
        }
        if let Some(proof) = failed {
            let registration = proof.registration();
            if registration.actor != self.node.id
                || registration.attachment != self.node.attachment
                || !Arc::ptr_eq(&registration.origin, &self.node.origin)
                || !Arc::ptr_eq(&registration.scope, &self.node.scope)
            {
                return Err(ActorTurnError::EffectTicketOriginMismatch);
            }
            proof.validate_consumer(registration, &process.runtime)?;
        }
        // These values are observations of the supplied exclusive originals.
        // Sort only local neutral metadata, never the owners or their payloads.
        let mut order: Vec<_> = owners
            .iter()
            .map(|(work, bundle)| (work.id(), bundle.id))
            .collect();
        order.sort_unstable_by_key(|(work, _)| *work);
        if order.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let mut children: BTreeMap<_, usize> = order.iter().map(|(work, _)| (*work, 0)).collect();
        let mut reservations = std::collections::BTreeSet::new();
        if order.iter().any(|(_, id)| !reservations.insert(*id)) {
            return Err(ActorTurnError::ActorNotRetired);
        }

        let node = &self.node;
        let mut source_roles = std::collections::BTreeSet::new();
        for source in sources.iter() {
            if !source.active {
                return Err(ActorTurnError::UnknownStandingInput);
            }
            if !self.runtime.same_runtime(&source.runtime)
                || source.actor != node.id
                || !Arc::ptr_eq(&source.origin, &node.origin)
            {
                return Err(ActorTurnError::EffectTicketOriginMismatch);
            }
            if !source_roles.insert(source.standing_input_id) {
                return Err(ActorTurnError::ActorNotRetired);
            }
        }
        let mut credit_wakes = Vec::new();
        let mut inbox = node.inbox.lock().expect("actor inbox lock");
        let mut binding = node
            .scope
            .execution
            .lock()
            .expect("actor execution binding lock");
        let ActorExecution::Live { scope, .. } = &*binding else {
            return Err(ActorTurnError::ExecutionAlreadyDetached);
        };
        // Keep the ledger guard independent of the binding value which this
        // same transaction replaces after retiring the original attachment.
        let scope = scope.clone();
        if scope.same_scope(process) || !Arc::ptr_eq(&scope.runtime.shared, &process.runtime.shared)
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        if let Some(proof) = failed {
            if !inbox.failed || inbox.generation != proof.failed_epoch() {
                return Err(EffectError::StaleGeneration.into());
            }
            if !matches!(
                inbox.failure_publication.as_ref(),
                Some(FailurePublicationRecord::Closed)
            ) {
                return Err(ActorTurnError::FailurePublicationRequired);
            }
            // Unlike healthy detach, a failed actor may retain no R, Source
            // or committed batch here. They must first complete publication,
            // explicit failed-consumer abandonment and real holder exit.
            if inbox.sealed
                || !inbox.inputs.is_empty()
                || !inbox.payloads.is_empty()
                || !inbox.standing_inputs.is_empty()
                || !inbox.mailbox.is_empty()
                || !inbox.batches.is_empty()
                || inbox.failure.is_some()
                || inbox.failure_input.is_some()
                || inbox.timer_input.is_some()
            {
                return Err(ActorTurnError::ActorNotRetired);
            }
        } else if inbox.failed
            || inbox.sealed
            || inbox.inputs.values().any(|input| {
                [input.author.as_ref(), input.producer.as_ref()]
                    .into_iter()
                    .flatten()
                    .any(|author| author.work.scope == scope.id())
                    || input.author.is_none()
            })
            || inbox
                .standing_inputs
                .iter()
                .any(|(id, standing_input_record)| {
                    standing_input_record
                        .producer
                        .as_ref()
                        .is_none_or(|author| {
                            author.work.scope == scope.id() && !source_roles.contains(id)
                        })
                })
            || inbox.batches.values().any(|batch| {
                batch.state.has_unfinished_author_scope(scope.id())
                    || batch.state.has_unfinished_business()
                    || !batch.reservations.is_empty()
            })
            || inbox.failure.is_some()
            || inbox.failure_input.is_some()
            || inbox.failure_publication.is_some()
            || inbox.timer_input.is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        let mut ledger = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let original = ledger
            .scopes
            .get(&scope.id())
            .ok_or(RuntimeError::UnknownScope)?;
        let scope_fenced = if let Some(proof) = failed {
            original.failed() && original.generation() == proof.failed_epoch()
        } else {
            !original.failed() && original.generation() == inbox.generation
        };
        if original.external_open()
            || !scope_fenced
            || original.work_count() != owners.len()
            // The exact registration's attachment is still held. Any bound
            // report, capture object or other storage adds another real slot.
            || original.storage_count() != 1
            || ledger.ledger.reservation_count_for(scope.id()) != owners.len()
            || ledger
                .unresolved
                .range(
                    (scope.id(), WorkId::new(1).expect("minimum Work identity"))
                        ..=(
                            scope.id(),
                            WorkId::new(u64::MAX).expect("maximum Work identity"),
                        ),
                )
                .next()
                .is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        if failed.is_some() && original.parent() != Some(process.id()) {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        let admission_generation = failed
            .map(ActorFailureProof::initial_epoch)
            .unwrap_or_else(|| original.generation());
        for (work, bundle) in owners.iter() {
            if !work.scope().same_scope(&scope)
                || !bundle.scope.same_scope(&scope)
                || !bundle.active
            {
                return Err(ActorTurnError::ReservationOwnerMismatch);
            }
            let record = ledger
                .ledger
                .reservation(bundle.id)
                .ok_or(ActorTurnError::ReservationOwnerMismatch)?;
            if record.scope != scope.id()
                || record.work != work.id()
                || record.generation != admission_generation
                || !record.bound
                || !record.quota.is_empty()
            {
                return Err(ActorTurnError::ReservationOwnerMismatch);
            }
            let metadata = original
                .work(work.id())
                .ok_or(ActorTurnError::ActorNotRetired)?;
            if !matches!(metadata.kind, WorkKind::Step | WorkKind::Cleanup)
                || metadata.unbound_guard
                || metadata.body_exited
                || metadata.captures_exited
                || original
                    .captures_bound(work.id())
                    .map_err(RuntimeError::from)?
            {
                return Err(ActorTurnError::ActorNotRetired);
            }
            if let Some(parent) = metadata.parent {
                // Real child IDs are admitted after their parent. This also
                // proves the local child-first retirement order is acyclic.
                if parent >= work.id() {
                    return Err(ActorTurnError::ActorNotRetired);
                }
                let count = children
                    .get_mut(&parent)
                    .ok_or(ActorTurnError::ActorNotRetired)?;
                *count = count
                    .checked_add(1)
                    .ok_or(ActorTurnError::ActorNotRetired)?;
            }
        }
        let mut source_pins: std::collections::BTreeMap<WorkId, usize> =
            children.keys().map(|id| (*id, 0)).collect();
        let mut source_credits = [0usize; 4];
        for source in sources.iter() {
            let standing_input_record = inbox
                .standing_inputs
                .get(&source.standing_input_id)
                .ok_or(ActorTurnError::UnknownStandingInput)?;
            if standing_input_record.in_flight.is_some() {
                return Err(ActorTurnError::StandingInputBusy);
            }
            let author = standing_input_record
                .producer
                .as_ref()
                .ok_or(ActorTurnError::InputAuthorMismatch)?;
            // The original producer proof was minted from this actual Source
            // position and one of the exclusive metadata Work owners above.
            // A neutral scope/work pair alone cannot qualify this transition.
            if author.actor != node.id
                || !Arc::ptr_eq(&author.origin, &node.origin)
                || author.actor_generation != inbox.generation
                || author.input != source.standing_input_id.0
                || !author.scope.same_scope(&scope)
                || author.work.scope != scope.id()
                || author.generation != original.generation()
                || !children.contains_key(&author.work.work)
            {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            let pin = standing_input_record
                .producer_header
                .as_ref()
                .ok_or(ActorTurnError::InputAuthorMismatch)?;
            if !pin.belongs_to(&scope) || pin.reference() != author.work {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            let count = source_pins
                .get_mut(&author.work.work)
                .expect("preflighted actual metadata owner");
            *count = count
                .checked_add(1)
                .ok_or(ActorTurnError::ActorNotRetired)?;
            source_credits[standing_input_record.class.index()] = source_credits
                [standing_input_record.class.index()]
            .checked_add(1)
            .ok_or(ActorTurnError::ActorNotRetired)?;
        }
        if InputClass::ALL
            .iter()
            .any(|class| source_credits[class.index()] > inbox.counts[class.index()])
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        for (id, expected) in &source_pins {
            if original
                .work(*id)
                .expect("actual metadata header")
                .header_pin_count()
                != *expected
            {
                return Err(ActorTurnError::ActorNotRetired);
            }
        }
        for (id, expected) in &children {
            if original
                .work(*id)
                .expect("qualified metadata Work")
                .children
                != *expected
            {
                return Err(ActorTurnError::ActorNotRetired);
            }
        }
        let process_state = ledger
            .scopes
            .get(&process.id())
            .ok_or(RuntimeError::UnknownScope)?;
        if process_state.failed() {
            return Err(ActorTurnError::ActorFailed);
        }
        if failed.is_some() && process_state.parent().is_some() {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        let scheduler = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime scheduler lock");
        if !scheduler
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
            || !scheduler.ready.is_idle(node.id)?
            || !scheduler.effect_ready.is_claimed(node.id)?
        {
            return Err(ActorTurnError::ActorNotRetired);
        }

        // All ordinary refusals precede mutation. No caller code, foreign
        // destructor or publicly locking lifecycle method enters this tail.
        // Return the actual idle roles' class credits through the same window
        // and original grant queue. Wakes cannot reenter the scheduler while
        // this transaction still owns its scheduler gate.
        for source in sources.iter_mut() {
            let original = inbox
                .standing_inputs
                .remove(&source.standing_input_id)
                .expect("preflighted original idle metadata Source");
            credit_wakes.extend(inbox.return_input_credit(original.class));
            original
                .producer_header
                .expect("preflighted original source pin")
                .release_locked(&mut ledger);
            source.active = false;
        }
        let original_scope = scope.id();
        let history = &scope.runtime.shared;
        for (work, reservation) in order.iter().rev() {
            let (capacity, original) = ledger
                .parts(original_scope)
                .expect("preflighted original execution scope");
            original
                .body_exited(*work)
                .expect("preflighted live metadata body");
            original
                .captures_exited(*work)
                .expect("preflighted capture-free metadata owner");
            capacity
                .finish(original, *reservation)
                .expect("preflighted zero-quota original child-free carrier");
            let ready_parent = original
                .retire_work(*work)
                .expect("preflighted actual metadata exit");
            assert!(
                ready_parent.is_none(),
                "metadata parent exits after its children"
            );
            ledger.pending_retirement.remove(&(original_scope, *work));
            ledger.abandoned_bundles.remove(reservation);
            history.record(|| HistoryFact::BodyExited {
                scope: original_scope,
                work: *work,
            });
            history.record(|| HistoryFact::CapturesExited {
                scope: original_scope,
                work: *work,
            });
            history.record(|| HistoryFact::WorkRetired {
                scope: original_scope,
                work: *work,
            });
        }
        ledger
            .scopes
            .get_mut(&original_scope)
            .expect("preflighted original execution scope")
            .retire_slot(node.attachment)
            .expect("the same registration retains its actual attachment");
        history.record(|| HistoryFact::SlotRetired {
            slot: node.attachment,
        });
        inbox.failure_position = None;
        // These are the same exclusive local owners whose real records were
        // just retired. Updating flags cannot publish or infer any new exit.
        for (work, bundle) in owners.iter_mut() {
            work.note_metadata_exit_observed();
            bundle.active = false;
        }
        *binding = ActorExecution::Retained {
            process: process.clone(),
        };
        drop(scheduler);
        drop(ledger);
        drop(binding);
        drop(inbox);
        self.runtime.notify_metadata_credit_sources(credit_wakes);
        process.runtime.shared.changed.notify();
        self.runtime.shared.changed.notify();
        Ok(())
    }
}
