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

use super::{
    OriginalTransportJoinObligation, TransportJoinHandoff, TransportJoinRegistration,
    TransportRegistrationState,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendInput},
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::host::transport::{ActorSendReports, BodyOutcome, GrantedInputs, SendKey},
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, QueuedInputPayload},
    input::{AttemptInput, ExecutorFact, QueuedInputGrantRef, WithdrawalOutcome},
};
use novarocks_control_runtime::{
    actor_turn::InputAuthor,
    causal_input::{CausalFillFailure, CausalInputObligation},
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_resource_contract::{ExistingOwnerRetentionPin, RetentionPinError};
use std::{num::NonZeroU64, sync::Arc};

/// The original result position is held independently of the worker future.
/// A missing outcome after a positively observed exit is left to failure
/// custody. In particular, it never implies that a send was withdrawn.
#[must_use = "finish the original result input or give it to failure custody"]
pub(crate) struct ReturnedActorTransportResult {
    pub(crate) effect: Arc<AttemptEffectEnvelope>,
    pub(crate) original: CausalInputObligation<FrontendActor>,
    pub(crate) outcome: Option<BodyOutcome>,
    pub(crate) send_reports: Option<Arc<ActorSendReports>>,
    /// On refusal the same prepared event and original position remain
    /// available to the Source's failure/retirement path.
    pub(crate) withdrawal: Option<Result<InputId, CausalFillFailure<FrontendInput>>>,
}
pub(super) enum ActorResultState {
    Unbound,
    Held(ReturnedActorTransportResult),
    Taken,
}
impl TransportJoinHandoff {
    /// Transfer the paid actor position before the factory can run, fail or
    /// destroy its captures. Refusal preserves the original prepared job.
    pub(crate) fn bind_actor_result(
        &self,
        effect: Arc<AttemptEffectEnvelope>,
        expected: &InputAuthor,
        original: CausalInputObligation<FrontendActor>,
        send_reports: Option<Arc<ActorSendReports>>,
    ) -> Result<(), CausalInputObligation<FrontendActor>> {
        let Some(record) = self.registration.original.upgrade() else {
            return Err(original);
        };
        if effect.handoff().is_none()
            || !original
                .event_author()
                .is_ok_and(|author| author.same_binding(expected))
        {
            return Err(original);
        }
        let mut result = record
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !matches!(*result, ActorResultState::Unbound) {
            return Err(original);
        }
        *result = ActorResultState::Held(ReturnedActorTransportResult {
            effect,
            original,
            outcome: None,
            send_reports,
            withdrawal: None,
        });
        Ok(())
    }
}
impl TransportJoinRegistration {
    pub(crate) fn stage_actor_outcome(&self, outcome: BodyOutcome) -> Result<(), BodyOutcome> {
        let Some(record) = self.original.upgrade() else {
            return Err(outcome);
        };
        let mut state = record
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let ActorResultState::Held(original) = &mut *state else {
            return Err(outcome);
        };
        if original.outcome.is_some() {
            return Err(outcome);
        }
        original.outcome = Some(outcome);
        Ok(())
    }
}
impl OriginalTransportJoinObligation {
    /// For a send, this is after the final original WC queue exit. An input
    /// admission without a Native row uses its own actual Join and report.
    /// Positive unused evidence is published here using the same original
    /// position. A missing outcome or raw failure never certifies withdrawal.
    pub(crate) fn take_actor_result(&self) -> Option<ReturnedActorTransportResult> {
        if !matches!(
            *self.native.state.lock().unwrap_or_else(|e| e.into_inner()),
            TransportRegistrationState::Issued
        ) {
            return None;
        }
        let mut state = self
            .native
            .actor_result
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !matches!(*state, ActorResultState::Held(_)) {
            return None;
        }
        let ActorResultState::Held(mut original) =
            std::mem::replace(&mut *state, ActorResultState::Taken)
        else {
            unreachable!("qualified original result");
        };
        drop(state);
        original.effect.retire_handoff();
        original.publish_withdrawal();
        Some(original)
    }

    pub(super) fn actor_result_held(&self) -> bool {
        matches!(
            *self
                .native
                .actor_result
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            ActorResultState::Held(_)
        )
    }
}

impl ReturnedActorTransportResult {
    fn publish_withdrawal(&mut self) {
        if !matches!(&self.outcome, Some(BodyOutcome::Unused(_))) {
            return;
        }
        let fact = match &self.effect.protocol.body {
            EffectBody::SendBatch { .. } => ExecutorFact::Withdrawn {
                effect: self.effect.protocol.id,
                outcome: WithdrawalOutcome::DefinitelyUnsent,
            },
            EffectBody::AdmitQueuedInput {
                operation,
                generation,
                ..
            } => ExecutorFact::QueuedInputWithdrawn {
                operation: operation.operation_id(),
                generation: *generation,
            },
            _ => unreachable!("only original send and admission jobs bind transport results"),
        };
        // There is no preceding receipt for a never-started operation.
        let event = FrontendInput::Attempt(Box::new(AttemptInput::Executor(fact)));
        self.withdrawal = Some(
            self.original
                .fill_with_receipt(None, event)
                .map(|(_, input)| input),
        );
    }

    /// Install the actual original WC object before exposing its passive
    /// address. For a first payload the caller supplies the canonical
    /// operation's retention cell, which outlives this send's queue record.
    pub(crate) fn publish_admission(
        &mut self,
        inputs: &mut GrantedInputs,
        payload_retention: Option<&mut Option<ExistingOwnerRetentionPin>>,
    ) -> Result<Option<AdmissionPublication>, AdmissionPublicationError> {
        let Some(BodyOutcome::InputAdmitted(original)) = self.outcome.as_ref() else {
            return Ok(None);
        };
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            payload,
            ..
        } = &self.effect.protocol.body
        else {
            return Err(AdmissionPublicationError::WrongAdmission);
        };
        let key = SendKey {
            operation: operation.operation_id(),
            generation: *generation,
        };
        if original.key() != key || generation.admission_effect() != self.effect.protocol.id {
            return Err(AdmissionPublicationError::WrongAdmission);
        }
        let record =
            NonZeroU64::new(self.effect.protocol.id.get()).expect("original nonzero effect");
        let grant = QueuedInputGrantRef::new(record, *generation);
        if inputs.contains(grant) {
            return Err(AdmissionPublicationError::DuplicateGrant);
        }
        if matches!(
            payload,
            QueuedInputPayload::NewCreation(_) | QueuedInputPayload::NewTaskUpdate(_)
        ) {
            let slot = payload_retention
                .filter(|slot| slot.is_none())
                .ok_or(AdmissionPublicationError::MissingPayloadRetention)?;
            *slot = Some(
                original
                    .retain_payload()
                    .map_err(AdmissionPublicationError::PayloadRetention)?,
            );
        }
        let Some(BodyOutcome::InputAdmitted(original)) = self.outcome.take() else {
            unreachable!("qualified original grant");
        };
        inputs.install(grant, original);
        let event = FrontendInput::Attempt(Box::new(AttemptInput::Executor(
            ExecutorFact::QueuedInputGranted {
                operation: key.operation,
                grant,
                queued_bytes: operation.queued_bytes(),
            },
        )));
        let delivery = self
            .original
            .fill_with_receipt(None, event)
            .map(|(_, input)| input);
        Ok(Some(AdmissionPublication { grant, delivery }))
    }
}

#[must_use = "retain the original refused event or retire the delivered input"]
pub(crate) struct AdmissionPublication {
    pub(crate) grant: QueuedInputGrantRef,
    pub(crate) delivery: Result<InputId, CausalFillFailure<FrontendInput>>,
}

#[derive(Debug)]
pub(crate) enum AdmissionPublicationError {
    WrongAdmission,
    DuplicateGrant,
    MissingPayloadRetention,
    PayloadRetention(RetentionPinError),
}
