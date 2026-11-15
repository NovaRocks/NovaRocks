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

use super::*;
use crate::{
    effect::RelayResponse,
    failure::LogicalRequestFailure,
    input::{ActionResult, ConsumerFact, GovernanceFact, RelayFact},
};

impl LogicalExecutionCore {
    pub(super) fn tag(body: &LogicalEffectBody) -> Option<ActionTag> {
        Some(match body {
            LogicalEffectBody::Native(effect) => ActionTag::Native(match effect {
                NativeEffect::ActivateAttempt { activation, .. } => {
                    NativeAction::Activate(*activation)
                }
                NativeEffect::RunAttempt { activation } => NativeAction::Run(*activation),
                NativeEffect::PrepareSuccessor { replacement, .. } => {
                    NativeAction::Prepare(*replacement)
                }
                NativeEffect::DispatchQualification { identity, .. } => {
                    NativeAction::Qualify(*identity)
                }
                NativeEffect::ActivateReservation { identity, .. } => {
                    NativeAction::ActivateReservation(*identity)
                }
                NativeEffect::CancelQualification { identity, .. } => {
                    NativeAction::CancelQualification(*identity)
                }
                NativeEffect::AbandonReservation { identity } => {
                    NativeAction::AbandonReservation(*identity)
                }
                NativeEffect::FinishResources { activation }
                | NativeEffect::AbandonResources { activation } => {
                    NativeAction::Resources(*activation)
                }
                NativeEffect::ConvergeDormant { execution, .. } => {
                    NativeAction::Dormant(*execution)
                }
                NativeEffect::RecordTerminalCut { .. } => NativeAction::RecordCut,
                NativeEffect::RecordStreamingSuccess { .. } => NativeAction::RecordSuccess,
                NativeEffect::RecordComputationReady { cut } => {
                    NativeAction::RecordReady(cut.activation)
                }
                NativeEffect::RetirePreparation => NativeAction::RetirePreparation,
            }),
            LogicalEffectBody::Consumer(effect) => ActionTag::Consumer(match effect {
                ConsumerEffect::WriteSchema { .. } => ConsumerAction::Schema,
                ConsumerEffect::ReserveEndSlot { activation } => {
                    ConsumerAction::EndSlot(*activation)
                }
                ConsumerEffect::EnqueueEnd { activation } => ConsumerAction::End(*activation),
                ConsumerEffect::ExposeOutput { .. } => ConsumerAction::Output,
                ConsumerEffect::PublishCompletion
                | ConsumerEffect::PublishFailure(_)
                | ConsumerEffect::PublishAttemptFailure { .. } => ConsumerAction::Failure,
                ConsumerEffect::InternalReply { .. } => ConsumerAction::InternalReply,
                ConsumerEffect::PublishReady { .. } => ConsumerAction::Ready,
                ConsumerEffect::PublishDiagnostics { .. } => ConsumerAction::Diagnostics,
                ConsumerEffect::PublishConvergence { .. } => ConsumerAction::Convergence,
            }),
            LogicalEffectBody::Relay(effect) => ActionTag::Relay(match effect {
                RelayEffect::Start { activation }
                | RelayEffect::Stop { activation }
                | RelayEffect::Reply { activation, .. } => *activation,
            }),
            LogicalEffectBody::Governance(effect) => ActionTag::Governance(match effect {
                GovernanceEffect::RegisterObligation { activation, kind } => {
                    GovernanceAction::Register(*activation, *kind)
                }
                GovernanceEffect::ResolveObligation { activation, kind } => {
                    GovernanceAction::Resolve(*activation, *kind)
                }
                GovernanceEffect::EndRemoteTracking { activation, kind } => {
                    GovernanceAction::EndTracking(*activation, *kind)
                }
                GovernanceEffect::ReleaseExecutionStage => GovernanceAction::ReleaseStage,
                GovernanceEffect::RequestShutdownCancellation
                | GovernanceEffect::RequestClientCancellation => GovernanceAction::Cancel,
                GovernanceEffect::CompleteWorkOwner => GovernanceAction::CompleteOwner,
            }),
            LogicalEffectBody::AttemptPort(_)
            | LogicalEffectBody::AttemptLifecycleDiagnostic(_)
            | LogicalEffectBody::SnapshotReply { .. }
            | LogicalEffectBody::RegistryJoinReply { .. }
            | LogicalEffectBody::Retirable
            | LogicalEffectBody::SupervisionFailed(_) => return None,
        })
    }

    pub(super) fn apply_result(
        &mut self,
        action: LogicalActionId,
        result: ActionResult,
    ) -> Result<(), CoreFault> {
        let held = self
            .actions
            .get_mut(&action)
            .ok_or(CoreFault::UnknownAction)?;
        if held.result_received {
            return Err(CoreFault::UnexpectedInput);
        }
        held.result_received = true;
        let tag = held.tag;
        match (tag, result) {
            (ActionTag::Native(kind), ActionResult::Native(fact)) => {
                self.apply_native(action, kind, fact)?
            }
            (ActionTag::Consumer(kind), ActionResult::Consumer(fact)) => {
                self.apply_consumer(kind, fact)?
            }
            (ActionTag::Governance(kind), ActionResult::Governance(fact)) => {
                self.apply_governance(action, kind, fact)?
            }
            (ActionTag::Relay(expected), ActionResult::Relay(fact)) => match fact {
                RelayFact::Applied => {}
                RelayFact::Stopped {
                    activation,
                    all_deliveries_settled,
                } => {
                    if expected != activation {
                        return Err(CoreFault::ForeignActivation);
                    }
                    self.attempt_mut(activation)?
                        .convergence
                        .observe_relay_stopped(all_deliveries_settled);
                }
                RelayFact::Failed {
                    activation,
                    failure,
                    all_deliveries_settled,
                } => {
                    if expected != activation {
                        return Err(CoreFault::ForeignActivation);
                    }
                    self.attempt_mut(activation)?
                        .convergence
                        .observe_relay_stopped(all_deliveries_settled);
                    self.accept_attempt_failure(activation, failure)?;
                }
            },
            _ => return Err(CoreFault::UnexpectedInput),
        }
        self.retire_action(action);
        Ok(())
    }

    pub(super) fn apply_exit(&mut self, action: LogicalActionId) -> Result<(), CoreFault> {
        let held = self
            .actions
            .get_mut(&action)
            .ok_or(CoreFault::UnknownAction)?;
        if held.exited {
            return Err(CoreFault::UnexpectedInput);
        }
        held.exited = true;
        let tag = held.tag;
        let activation = match tag {
            ActionTag::Native(NativeAction::Resources(a)) => Some(a),
            ActionTag::Governance(
                GovernanceAction::Resolve(a, _) | GovernanceAction::EndTracking(a, _),
            ) => Some(a),
            _ => None,
        };
        if let Some(activation) = activation {
            self.attempt_mut(activation)?
                .convergence
                .observe_action_exit(action)
                .map_err(|_| CoreFault::UnexpectedInput)?;
            if matches!(tag, ActionTag::Native(NativeAction::Resources(_))) {
                self.mark_dormant_local_exit_if_complete(action, activation)?;
            }
        }
        self.retire_action(action);
        Ok(())
    }
    fn retire_action(&mut self, action: LogicalActionId) {
        if self
            .actions
            .get(&action)
            .is_some_and(|a| a.exited && a.result_received)
            && let Some(Action {
                tag: ActionTag::Native(NativeAction::AbandonReservation(identity)),
                ..
            }) = self.actions.remove(&action)
            && let Some(q) = self
                .recovery
                .as_mut()
                .filter(|r| r.identity == Some(identity))
                .and_then(|r| r.qualification.as_mut())
        {
            q.close();
        }
    }

    fn apply_governance(
        &mut self,
        action: LogicalActionId,
        kind: GovernanceAction,
        fact: GovernanceFact,
    ) -> Result<(), CoreFault> {
        match (kind, fact) {
            (
                GovernanceAction::Register(expected, expected_kind),
                GovernanceFact::ObligationRegistered { activation, kind },
            ) => {
                if expected != activation || expected_kind != kind {
                    return Err(CoreFault::ForeignActivation);
                }
                self.attempt_mut(activation)?
                    .convergence
                    .observe_obligation_registered(kind)
                    .map_err(|_| CoreFault::UnexpectedInput)?;
            }
            (
                GovernanceAction::Resolve(expected, expected_kind),
                GovernanceFact::ObligationResolved { activation, kind },
            )
            | (
                GovernanceAction::EndTracking(expected, expected_kind),
                GovernanceFact::RemoteTrackingEnded { activation, kind },
            ) => {
                if expected != activation || expected_kind != kind {
                    return Err(CoreFault::ForeignActivation);
                }
                self.attempt_mut(activation)?
                    .convergence
                    .observe_governance_result(action, kind, Ok(()))
                    .map_err(|_| CoreFault::UnexpectedInput)?;
            }
            (GovernanceAction::ReleaseStage, GovernanceFact::StageReleased) => {
                self.stage_released = true
            }
            (GovernanceAction::CompleteOwner, GovernanceFact::WorkOwnerCompleted) => {
                self.owner_completed = true
            }
            (GovernanceAction::Cancel, GovernanceFact::CancellationRequested) => {}
            (kind, GovernanceFact::Failed(error)) => {
                match kind {
                    GovernanceAction::Resolve(a, k) | GovernanceAction::EndTracking(a, k) => {
                        let _retained = self.attempt_mut(a)?.convergence.observe_governance_result(
                            action,
                            k,
                            Err(error.clone()),
                        );
                    }
                    GovernanceAction::Register(a, k) => self
                        .attempt_mut(a)?
                        .convergence
                        .observe_obligation_not_created(k)
                        .map_err(|_| CoreFault::UnexpectedInput)?,
                    _ => {}
                }
                self.report_supervision_failure(error);
            }
            _ => return Err(CoreFault::UnexpectedInput),
        }
        Ok(())
    }

    fn apply_consumer(
        &mut self,
        kind: ConsumerAction,
        fact: ConsumerFact,
    ) -> Result<(), CoreFault> {
        match (kind, fact) {
            (ConsumerAction::Schema, ConsumerFact::SchemaWritten) => {
                self.visibility
                    .schema_written()
                    .map_err(|_| CoreFault::UnexpectedInput)?;
            }
            (ConsumerAction::EndSlot(activation), ConsumerFact::EndSlotReady) => {
                if self.state.conclusion().is_none()
                    && self.state.current_activation() == Some(activation)
                    && !self.scope_cancellation_requested
                    && !self.visibility.frozen(activation)
                {
                    self.visibility
                        .commit_end_slot(activation)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                    self.state
                        .commit_stream_success(activation)
                        .expect("validated stream success commits at the reserved End slot");
                    self.begin_conclusion();
                    self.queue(LogicalEffectBody::Native(
                        NativeEffect::RecordStreamingSuccess { activation },
                    ));
                    self.queue(LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd {
                        activation,
                    }));
                    if let Some(waiter) = self.end_waiter.take() {
                        self.relay_reply(waiter, RelayResponse::Applied);
                    }
                }
            }
            (ConsumerAction::End(activation), ConsumerFact::EndSettled) => {
                self.attempt_mut(activation)?;
                if self.visibility.root(activation).is_none() || !self.visibility.committed() {
                    return Err(CoreFault::UnexpectedInput);
                }
                self.end_settled = true;
            }
            (ConsumerAction::Output, ConsumerFact::OutputExposed) => self.output_exposed = true,
            (ConsumerAction::Ready, ConsumerFact::ReadyHandedOff) => {}
            (ConsumerAction::Diagnostics, ConsumerFact::DiagnosticsPublished) => {
                if !self
                    .ready
                    .as_mut()
                    .is_some_and(InternalReady::diagnostics_published)
                {
                    return Err(CoreFault::UnexpectedInput);
                }
            }
            (ConsumerAction::Convergence, ConsumerFact::ConvergencePublished) => {
                if !self
                    .ready
                    .as_mut()
                    .is_some_and(InternalReady::convergence_published)
                {
                    return Err(CoreFault::UnexpectedInput);
                }
            }
            (ConsumerAction::Failure | ConsumerAction::InternalReply, ConsumerFact::Applied) => {}
            (kind, ConsumerFact::Failed(error)) => {
                if matches!(kind, ConsumerAction::End(_)) {
                    self.end_settled = true;
                }
                if self.state.conclusion().is_some()
                    || matches!(
                        kind,
                        ConsumerAction::Output
                            | ConsumerAction::Diagnostics
                            | ConsumerAction::Convergence
                    )
                {
                    self.report_supervision_failure(error);
                } else {
                    self.conclude_failure(error);
                }
            }
            _ => return Err(CoreFault::UnexpectedInput),
        }
        Ok(())
    }

    pub(super) fn request_error(&self) -> QueryExecutionError {
        self.logical_failure
            .clone()
            .unwrap_or_else(|| LogicalRequestFailure::ExecutionConcluded.error())
    }
}
