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

//! The actual failed owner preserves its original metadata, retirement origin
//! and timer. Root receipts observe only the same original lifecycle handoff.

use super::*;
use crate::control_runtime::control_internal::RootLifecycleConsumption;
use novarocks_control_runtime::driver::ActorFailureProof;
use novarocks_control_runtime_core::identity::Tick;

impl ContextLifecycle {
    fn validate_failed_owner(&self, proof: &ActorFailureProof) -> Result<(), ContextHostJobError> {
        if !proof.registration().same_registration(&self.actor) {
            return Err(ContextHostJobError::WrongOwner);
        }
        let actual = self.runtime.failure_proof_for(&self.actor)?;
        if actual.initial_epoch() != proof.initial_epoch()
            || actual.failed_epoch() != proof.failed_epoch()
        {
            return Err(ContextHostJobError::WrongOwner);
        }
        Ok(())
    }

    /// This callback has no foreign capture. Failure withdraws an unused
    /// producer without inventing a callback, execution exit or retention ACK.
    /// A filled producer still waits for its original scan and actual input.
    pub(in crate::control_runtime) fn withdraw_failed_callback(
        &mut self,
        proof: &ActorFailureProof,
    ) -> Result<bool, ContextHostJobError> {
        self.validate_failed_owner(proof)?;
        if self.callback_closed {
            return Ok(true);
        }
        if let Some(callback) = self.callback.as_mut() {
            if let Some(input) = callback.in_flight()? {
                if self.scan_input == Some(input) || self.outgoing.is_some() {
                    return Ok(false);
                }
                match self.runtime.retire_input(self.actor.actor(), input) {
                    Ok(()) => {}
                    Err(
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain
                        | DriverError::MetadataStillPending,
                    ) => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
            }
            callback.close_after_exit()?;
            self.callback = None;
        }
        if let Some(permit) = self.permit.as_ref() {
            let custody = self
                .runtime
                .input_custody(self.actor.actor(), permit.id())?
                .ok_or(ContextHostJobError::WrongOwner)?;
            if custody.phase != novarocks_control_runtime::driver::InputPhase::Reserved {
                return Err(ContextHostJobError::WrongPhase);
            }
            self.permit
                .take()
                .expect("same unused callback permit")
                .revoke_unused()?;
        }
        self.callback_closed = true;
        Ok(true)
    }

    /// Only the caller that actually removed the execution child supplies
    /// this instant. An already cold original retains its identity and H.
    pub(in crate::control_runtime) fn record_failed_execution_retired(
        &mut self,
        proof: &ActorFailureProof,
        close_epoch: Option<Generation>,
        retired_at: MonotonicInstant,
    ) -> Result<(), ContextHostJobError> {
        self.validate_failed_owner(proof)?;
        match self.child.gates() {
            Err(RuntimeError::UnknownScope) => {}
            Ok(_) => return Err(ContextHostJobError::WrongPhase),
            Err(error) => return Err(error.into()),
        }
        if let Some((identity, _original)) = self.retirement {
            if let Some(close_epoch) = close_epoch
                && identity != (ContextRetirementIdentity::Protocol { close_epoch })
            {
                return Err(ContextHostJobError::WrongPhase);
            }
            self.failed_custodian = true;
            return Ok(());
        }
        let identity = close_epoch.map_or_else(
            || ContextRetirementIdentity::OwnerFailure {
                actor: proof.registration().actor(),
                initial_epoch: proof.initial_epoch(),
                failed_epoch: proof.failed_epoch(),
            },
            |close_epoch| ContextRetirementIdentity::Protocol { close_epoch },
        );
        self.retirement = Some((identity, RetentionOrigin::new(retired_at, self.horizon)));
        self.failed_custodian = true;
        Ok(())
    }

    /// The caller drives the existing metadata FIFO first. This method never
    /// consumes a ticket, skips a failed destination, or clears an intent on
    /// read. Each next fact waits for the previous Root input's real retirement.
    pub(in crate::control_runtime) fn failed_lifecycle_intent(
        &mut self,
        proof: &ActorFailureProof,
        now: MonotonicInstant,
    ) -> Result<Option<RootLifecycleFact>, ContextHostJobError> {
        self.validate_failed_owner(proof)?;
        self.failed_metadata_drained = self.outgoing.is_none()
            && self.scan_input.is_none()
            && self
                .runtime
                .committed_batches(self.actor.actor(), self.scan_after, 1)?
                .is_empty();
        if !self.failed_metadata_drained {
            return Ok(None);
        }
        if let Some((fact, receipt)) = &self.failed_receipt {
            match receipt.consumption() {
                RootLifecycleConsumption::Queued | RootLifecycleConsumption::Submitted(_) => {
                    return Ok(None);
                }
                RootLifecycleConsumption::Consumed(_) | RootLifecycleConsumption::Retired => {
                    match fact {
                        RootLifecycleFact::ExecutionRetired { .. } => self.retained = true,
                        RootLifecycleFact::ContextExpired { .. } => self.expired = true,
                        RootLifecycleFact::HostRemoved { .. } => {
                            return Err(ContextHostJobError::WrongOwner);
                        }
                    }
                }
            }
            if !receipt.is_retired() {
                return Ok(None);
            }
            self.failed_receipt = None;
        }
        if let Some(original) = self.failed_intent {
            return Ok(Some(original));
        }
        let Some((retirement, origin)) = self.retirement else {
            return Ok(None);
        };
        if self.actor.execution_attached() {
            return Err(ContextHostJobError::WrongPhase);
        }
        if !self.expired {
            let deadline = Tick(
                u64::try_from(origin.expires_at().since_origin().as_nanos()).unwrap_or(u64::MAX),
            );
            self.runtime
                .arm_failed_custodian_deadline(proof, deadline)?;
            if !self.failed_deadline_due {
                match self.runtime.take_failed_custodian_deadline(proof) {
                    Ok(Some(actual)) if actual == deadline => self.failed_deadline_due = true,
                    Ok(Some(_)) => return Err(ContextHostJobError::WrongOwner),
                    Ok(None) | Err(DriverError::ActorNotRetired) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let fact = if !self.retained {
            Some(RootLifecycleFact::ExecutionRetired {
                context: self.context,
                retirement,
            })
        } else if !self.expired && self.failed_deadline_due && origin.has_expired(now) {
            Some(RootLifecycleFact::ContextExpired {
                context: self.context,
                retirement,
            })
        } else {
            None
        };
        self.failed_intent = fact;
        Ok(fact)
    }

    /// Refusal returns the same accepted Root receipt to its caller. Only a
    /// receipt for this unchanged original intent can move into this owner.
    pub(in crate::control_runtime) fn install_failed_lifecycle_receipt(
        &mut self,
        proof: &ActorFailureProof,
        fact: RootLifecycleFact,
        receipt: RootLifecycleReceipt,
    ) -> Result<(), (ContextHostJobError, RootLifecycleReceipt)> {
        if let Err(error) = self.validate_failed_owner(proof) {
            return Err((error, receipt));
        }
        if self.failed_receipt.is_some()
            || self.failed_intent != Some(fact)
            || receipt.fact() != fact
        {
            return Err((ContextHostJobError::WrongPhase, receipt));
        }
        self.failed_receipt = Some((fact, receipt));
        self.failed_intent = None;
        Ok(())
    }
}
