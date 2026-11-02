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

//! An actor failure target and its actual running cleanup author have
//! independent lifetimes. Claiming or ending publication exits neither one.

use super::*;
use novarocks_control_runtime_core::owned_storage::{OwnedPositionId, OwnedSlotKind};

impl<A: Actor> ActorRuntime<A> {
    /// Claim publication for this actual failed actor using an independently
    /// admitted, already Started Cleanup Work. The financial Scope stays healthy.
    pub fn claim_actor_failure_publication(
        &self,
        proof: &ActorFailureProof,
        cleanup: &crate::lease::WorkLease,
    ) -> Result<FailurePublicationSession<A>, DriverError> {
        let registration = proof.registration();
        let node = self.node(registration.actor)?;
        if node.attachment != registration.attachment
            || !Arc::ptr_eq(&node.origin, &registration.origin)
            || !Arc::ptr_eq(&node.scope, &registration.scope)
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        proof.validate_consumer(registration, &cleanup.scope().runtime)?;
        let affiliation = node.scope.current_scope();
        if !Arc::ptr_eq(&affiliation.runtime.shared, &cleanup.scope().runtime.shared) {
            return Err(DriverError::PublicationTargetMismatch);
        }
        let mut inbox = node
            .inbox
            .lock()
            .expect("actor failure publication claim lock");
        if !inbox.failed
            || inbox.generation != proof.failed_epoch()
            || inbox.generation <= registration.initial_epoch()
        {
            return Err(EffectError::StaleGeneration.into());
        }
        if inbox.failure_publication.is_some() {
            return Err(DriverError::FailurePublicationAlreadyClaimed);
        }
        let ledger = cleanup
            .scope()
            .runtime
            .shared
            .state
            .lock()
            .expect("actual failure cleanup ledger lock");
        let (financial_generation, header) =
            Self::started_failure_cleanup(&ledger, cleanup.scope().id(), cleanup.reference())?;
        let claim = FailurePublicationOrigin::StartedCleanup {
            financial_generation,
            header,
        };
        inbox.failure_publication = Some(FailurePublicationRecord::Active {
            generation: proof.failed_epoch(),
            origin: claim,
            work: cleanup.reference(),
        });
        Ok(FailurePublicationSession {
            runtime: self.clone(),
            registration: registration.clone(),
            actor: registration.actor,
            origin: registration.origin.clone(),
            scope: cleanup.scope().clone(),
            generation: proof.failed_epoch(),
            claim,
            work: cleanup.reference(),
        })
    }
    pub(super) fn started_failure_cleanup(
        ledger: &crate::transaction::LedgerState,
        financial_scope: ScopeId,
        work: ScopedWorkRef,
    ) -> Result<(Generation, OwnedPositionId), DriverError> {
        if work.scope != financial_scope {
            return Err(DriverError::FailureWorkInactive);
        }
        let scope = ledger
            .scopes
            .get(&financial_scope)
            .ok_or(DriverError::FailureWorkInactive)?;
        let original = scope
            .work(work.work)
            .ok_or(DriverError::FailureWorkInactive)?;
        let header = original.header_identity();
        if scope.failed()
            || original.kind != WorkKind::Cleanup
            || original.phase != WorkPhase::Started
            || original.unbound_guard
            || original.body_exited
            || original.captures_exited
            || ledger.unresolved.contains(&(work.scope, work.work))
            || header.kind() != OwnedSlotKind::WorkHeader
            || header.allocation().is_none()
                && (header.scope() != Some(work.scope)
                    || header.ordinal_or_work() != work.work.get())
        {
            return Err(DriverError::FailureWorkInactive);
        }
        Ok((scope.generation(), header))
    }
}
impl<A: Actor> FailurePublicationSession<A> {
    /// End only this original publication phase. The terminal claim prevents
    /// reopening it; actual Cleanup body/captures/report still need their real exit.
    /// Refusal returns the same session. Drop cannot complete this phase.
    pub fn finish(self) -> Result<(), (DriverError, Self)> {
        let result: Result<(), DriverError> = (|| {
            let node = self.runtime.node(self.actor)?;
            let mut inbox = node
                .inbox
                .lock()
                .expect("actor failure publication close lock");
            let ledger = self
                .scope
                .runtime
                .shared
                .state
                .lock()
                .expect("actual failure cleanup close ledger lock");
            self.validate(&node, &inbox, &ledger, &self.scope)?;
            inbox.failure_publication = Some(FailurePublicationRecord::Closed);
            drop(ledger);
            drop(inbox);
            self.runtime.shared.changed.notify();
            Ok(())
        })();
        result.map_err(|error| (error, self))
    }
}
