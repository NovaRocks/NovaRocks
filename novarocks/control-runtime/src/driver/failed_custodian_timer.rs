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

//! The original failed actor's timer position can wake its trusted host.
//! It never resumes the failed Core or owns execution work.

use super::*;

#[derive(Clone, Copy)]
enum CustodianDeadlinePhase {
    Armed,
    Due,
    Taken,
}

#[derive(Clone, Copy)]
pub(super) struct FailedCustodianTimer {
    deadline: Tick,
    phase: CustodianDeadlinePhase,
}
impl FailedCustodianTimer {
    pub(super) fn is_pending(&self) -> bool {
        !matches!(self.phase, CustodianDeadlinePhase::Taken)
    }
}

impl<A: Actor> ActorRuntime<A> {
    fn failed_custodian_node(
        &self,
        proof: &ActorFailureProof,
    ) -> Result<Arc<ActorCell<A>>, DriverError> {
        let registration = proof.registration();
        let node = self.node(registration.actor)?;
        if node.attachment != registration.attachment
            || !Arc::ptr_eq(&node.origin, &registration.origin)
            || !Arc::ptr_eq(&node.scope, &registration.scope)
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        proof.validate_consumer(registration, &node.scope.current_scope().runtime)?;
        if !node.scope.retained() {
            return Err(DriverError::RetainedInputRequired);
        }
        Ok(node)
    }

    fn validate_failed_custodian(
        inbox: &Inbox<A>,
        proof: &ActorFailureProof,
    ) -> Result<(), DriverError> {
        if !inbox.failed
            || inbox.generation != proof.failed_epoch()
            || inbox.generation <= proof.initial_epoch()
        {
            return Err(EffectError::StaleGeneration.into());
        }
        if inbox.sealed || inbox.timer_closed {
            return Err(DriverError::Closed);
        }
        Ok(())
    }

    /// Arm the original retained actor's host deadline after its actual
    /// failure. The first deadline is immutable, including after delivery.
    /// This issues no Work, input, publication or failed-Core execution right.
    pub fn arm_failed_custodian_deadline(
        &self,
        proof: &ActorFailureProof,
        deadline: Tick,
    ) -> Result<(), DriverError> {
        let node = self.failed_custodian_node(proof)?;
        let mut inbox = node.inbox.lock().expect("failed custodian timer arm lock");
        Self::validate_failed_custodian(&inbox, proof)?;
        if let Some(original) = &inbox.failed_custodian_timer {
            return if original.deadline == deadline {
                Ok(())
            } else {
                Err(DriverError::CustodianDeadlineMismatch)
            };
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, &node))
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        // The same index preserves an older already-filled Core Tick. Its
        // actual retirement, not arming this deadline, returns that position.
        state.timers.set(node.id, deadline)?;
        inbox.failed_custodian_timer = Some(FailedCustodianTimer {
            deadline,
            phase: CustodianDeadlinePhase::Armed,
        });
        drop(state);
        drop(inbox);
        self.shared.changed.notify();
        Ok(())
    }

    /// Take the original due receipt exactly once. No ordinary Timer input
    /// is consumed, and no Work body/capture/report exit is observed here.
    pub fn take_failed_custodian_deadline(
        &self,
        proof: &ActorFailureProof,
    ) -> Result<Option<Tick>, DriverError> {
        let node = self.failed_custodian_node(proof)?;
        let mut inbox = node.inbox.lock().expect("failed custodian timer take lock");
        Self::validate_failed_custodian(&inbox, proof)?;
        let Some(original) = inbox.failed_custodian_timer else {
            return Ok(None);
        };
        if !matches!(original.phase, CustodianDeadlinePhase::Due) {
            return Ok(None);
        }
        if inbox.timer_input.is_some() {
            return Err(DriverError::ActorNotRetired);
        }
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, &node))
        {
            return Err(DriverError::EffectTicketOriginMismatch);
        }
        if !state.timers.acknowledge_tick(node.id) {
            return Err(DriverError::CustodianTimerReceiptMissing);
        }
        inbox.failed_custodian_timer = Some(FailedCustodianTimer {
            phase: CustodianDeadlinePhase::Taken,
            ..original
        });
        drop(state);
        drop(inbox);
        self.shared.changed.notify();
        Ok(Some(original.deadline))
    }

    /// Called only for the actual node selected by the original TimerIndex.
    /// Failed actors use their host readiness, never the Core mailbox.
    pub(super) fn deliver_failed_custodian_timer(
        &self,
        node: &Arc<ActorCell<A>>,
        inbox: &mut Inbox<A>,
        now: Tick,
    ) -> Result<bool, DriverError> {
        let mut state = self.shared.state.lock().expect("runtime scheduler lock");
        if !state
            .actors
            .get(&node.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, node))
        {
            return Ok(false);
        }
        let original = if inbox.sealed || inbox.timer_closed {
            None
        } else {
            inbox.failed_custodian_timer
        };
        match original {
            Some(original)
                if matches!(original.phase, CustodianDeadlinePhase::Armed)
                    && now >= original.deadline =>
            {
                if inbox.timer_input.is_some() {
                    return Err(DriverError::ActorNotRetired);
                }
                // A previously selected Core timer can race the first arm.
                // Coalesce its due position with this same expired deadline.
                state.timers.cancel(node.id);
                state.effect_ready.notify(node.id)?;
                inbox.failed_custodian_timer = Some(FailedCustodianTimer {
                    phase: CustodianDeadlinePhase::Due,
                    ..original
                });
                Ok(true)
            }
            Some(original) if matches!(original.phase, CustodianDeadlinePhase::Due) => {
                // This exact neutral receipt still owns the pending position.
                Ok(false)
            }
            _ => {
                if inbox.timer_input.is_none() {
                    // An old selected-but-unfilled Core tick is withdrawn.
                    // A real filled Core input is never acknowledged here.
                    state.timers.acknowledge_tick(node.id);
                }
                Ok(false)
            }
        }
    }
}
