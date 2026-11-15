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
    input::{FailureReport, NativeFact},
    replacement::ReplacementQualificationFailure,
};

impl LogicalExecutionCore {
    pub(super) fn apply_native(
        &mut self,
        action: LogicalActionId,
        kind: NativeAction,
        fact: NativeFact,
    ) -> Result<(), CoreFault> {
        match (kind, fact) {
            (
                NativeAction::Activate(expected),
                NativeFact::AttemptActivated { activation, root },
            ) => {
                if expected != activation {
                    return Err(CoreFault::ForeignActivation);
                }
                let attempt = self.attempt_mut(activation)?;
                attempt.activated = true;
                if let Some(root) = root {
                    if root.query_execution_id() != activation.execution() {
                        return Err(CoreFault::ForeignExecution);
                    }
                    if attempt.root.as_ref().is_some_and(|r| r.root() != root) {
                        return Err(CoreFault::UnexpectedInput);
                    }
                    attempt.root.get_or_insert_with(|| RootControl::new(root));
                } else if self.start.policy.output != LogicalOutputMode::CompletionOnly {
                    return Err(CoreFault::UnexpectedInput);
                }
                if self.state.conclusion().is_none() {
                    self.state
                        .mark_running(activation)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                    self.queue(LogicalEffectBody::Native(NativeEffect::RunAttempt {
                        activation,
                    }));
                }
            }
            (NativeAction::Run(expected), NativeFact::RunCompleted { activation }) => {
                if expected != activation {
                    return Err(CoreFault::ForeignActivation);
                }
                self.attempt_mut(activation)?.native_run_complete = true;
            }
            (
                NativeAction::Run(expected),
                NativeFact::NativeRunFailed {
                    activation,
                    failure,
                },
            ) => {
                if expected != activation {
                    return Err(CoreFault::ForeignActivation);
                }
                self.observe_native_run_failure(activation, failure)?;
            }
            (
                NativeAction::Prepare(expected),
                NativeFact::SuccessorPrepared {
                    execution,
                    participations,
                },
            ) => {
                if expected != execution {
                    return Err(CoreFault::ForeignExecution);
                }
                let r = self
                    .recovery
                    .as_mut()
                    .filter(|r| r.replacement == execution)
                    .ok_or(CoreFault::UnexpectedInput)?;
                if r.prepared.is_some() {
                    return Err(CoreFault::UnexpectedInput);
                }
                r.prepared = Some(participations);
                r.builder = Some(EstablishLedgerBuilder::new(execution));
                r.prepare_finished = true;
            }
            (
                NativeAction::Qualify(expected),
                NativeFact::QualificationSettled {
                    identity,
                    operation_id,
                    dispatched_at,
                    settlement,
                },
            ) => {
                if expected != identity {
                    return Err(CoreFault::ForeignActivation);
                }
                let now = self.now;
                let concluded = self.state.conclusion().is_some();
                let r = self
                    .recovery
                    .as_mut()
                    .filter(|r| r.identity == Some(identity))
                    .ok_or(CoreFault::UnexpectedInput)?;
                let q = r.qualification.as_mut().ok_or(CoreFault::UnexpectedInput)?;
                if operation_id != q.request().operation_id {
                    return Err(CoreFault::UnexpectedInput);
                }
                if concluded {
                    self.queue(LogicalEffectBody::Native(
                        NativeEffect::AbandonReservation { identity },
                    ));
                } else {
                    let dispatched = dispatched_at
                        .map_or(Ok(()), |at| q.record_dispatched(identity, operation_id, at));
                    match dispatched {
                        Err(ReplacementQualificationFailure::Expired) => {
                            self.conclude_failure(ReplacementQualificationFailure::Expired.error())
                        }
                        Err(_) => return Err(CoreFault::UnexpectedInput),
                        Ok(()) => {
                            if let Err(error) = q.settle(identity, operation_id, settlement, now) {
                                self.conclude_failure(error.error());
                            }
                        }
                    }
                }
            }
            (
                NativeAction::ActivateReservation(expected),
                NativeFact::ReservationActivated {
                    identity,
                    operation_id,
                },
            ) => {
                if expected != identity {
                    return Err(CoreFault::ForeignActivation);
                }
                let now = self.now;
                let r = self
                    .recovery
                    .as_mut()
                    .filter(|r| r.identity == Some(identity))
                    .ok_or(CoreFault::UnexpectedInput)?;
                let result = r
                    .qualification
                    .as_mut()
                    .ok_or(CoreFault::UnexpectedInput)?
                    .activation_completed(identity, operation_id, now);
                r.reservation_activated = true;
                if let Err(error) = result {
                    self.conclude_failure(error.error());
                }
            }
            (NativeAction::Dormant(expected), NativeFact::DormantConverged { execution }) => {
                if expected != execution {
                    return Err(CoreFault::ForeignExecution);
                }
                if let Some(r) = self
                    .recovery
                    .as_mut()
                    .filter(|r| r.replacement == execution)
                {
                    r.dormant_converged = true;
                } else {
                    return Err(CoreFault::UnexpectedInput);
                }
            }
            (NativeAction::Resources(activation), NativeFact::DormantConverged { execution }) => {
                if activation.execution() != execution {
                    return Err(CoreFault::ForeignExecution);
                }
                self.attempt_mut(activation)?
                    .convergence
                    .observe_resource_result(action, Ok(()))
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                self.mark_dormant_local_exit_if_complete(action, activation)?;
            }
            (NativeAction::Resources(activation), NativeFact::Applied) => {
                self.attempt_mut(activation)?
                    .convergence
                    .observe_resource_result(action, Ok(()))
                    .map_err(|_| CoreFault::UnexpectedInput)?;
            }
            (NativeAction::RecordReady(activation), NativeFact::Applied) => {
                self.attempt_mut(activation)?.ready_cut_recorded = true
            }
            (NativeAction::RetirePreparation, NativeFact::Applied) => {
                self.preparation_retired = true
            }
            (NativeAction::CancelQualification(identity), NativeFact::Applied) => {
                if !self
                    .recovery
                    .as_ref()
                    .is_some_and(|r| r.identity == Some(identity))
                {
                    return Err(CoreFault::ForeignActivation);
                }
            }
            (NativeAction::AbandonReservation(identity), NativeFact::Applied) => {
                let r = self
                    .recovery
                    .as_mut()
                    .filter(|r| r.identity == Some(identity))
                    .ok_or(CoreFault::ForeignActivation)?;
                // A returned abandon result is not a real work-exit receipt.
                // Keep qualification metadata until this action actually exits.
                let _retained = r.qualification.as_ref().ok_or(CoreFault::UnexpectedInput)?;
            }
            (NativeAction::RecordCut | NativeAction::RecordSuccess, NativeFact::Applied) => {}
            (kind, NativeFact::Failed { execution, failure }) => match kind {
                NativeAction::Run(activation) => {
                    if activation.execution() != execution {
                        return Err(CoreFault::ForeignExecution);
                    }
                    self.attempt_mut(activation)?.native_run_complete = true;
                    self.accept_attempt_failure(activation, failure)?;
                }
                NativeAction::Resources(activation) => {
                    if activation.execution() != execution {
                        return Err(CoreFault::ForeignExecution);
                    }
                    let _retained = self
                        .attempt_mut(activation)?
                        .convergence
                        .observe_resource_result(action, Err(failure.error.clone()));
                    self.report_supervision_failure(failure.error);
                }
                NativeAction::Prepare(expected) => {
                    if expected != execution {
                        return Err(CoreFault::ForeignExecution);
                    }
                    if let Some(r) = self
                        .recovery
                        .as_mut()
                        .filter(|r| r.replacement == execution)
                    {
                        r.prepare_finished = true;
                    }
                    self.conclude_failure(failure.error);
                }
                NativeAction::Activate(activation) => {
                    if activation.execution() != execution {
                        return Err(CoreFault::ForeignExecution);
                    }
                    self.accept_attempt_failure(activation, failure)?;
                }
                NativeAction::RecordReady(_)
                | NativeAction::RecordCut
                | NativeAction::RecordSuccess
                | NativeAction::Dormant(_)
                | NativeAction::CancelQualification(_)
                | NativeAction::AbandonReservation(_)
                | NativeAction::RetirePreparation => self.report_supervision_failure(failure.error),
                _ => {
                    if self.state.conclusion().is_some() {
                        self.report_supervision_failure(failure.error);
                    } else {
                        self.conclude_failure(failure.error);
                    }
                }
            },
            _ => return Err(CoreFault::UnexpectedInput),
        }
        Ok(())
    }
    fn observe_native_run_failure(
        &mut self,
        activation: AttemptActivationIdentity,
        failure: FailureReport,
    ) -> Result<(), CoreFault> {
        let attempt = self.attempt_mut(activation)?;
        attempt.native_run_failed = true;
        attempt.native_run_failure = Some(failure.clone());
        if self.state.conclusion().is_some() {
            return Ok(());
        }
        if let Some(r) = self.recovery.as_ref().filter(|r| r.failed == activation) {
            self.conclude_failure(r.failure.error().clone());
            return Ok(());
        }
        if self.state.current_activation() != Some(activation) {
            return Ok(());
        }
        if let Some(original) = self
            .state
            .attempt(activation.execution())
            .and_then(|a| a.accepted_failure.clone())
        {
            self.conclude_failure(QueryExecutionError::from_execution(original));
        } else {
            self.accept_attempt_failure(activation, failure)?;
        }
        Ok(())
    }
    pub(super) fn mark_dormant_local_exit_if_complete(
        &mut self,
        action: LogicalActionId,
        activation: AttemptActivationIdentity,
    ) -> Result<(), CoreFault> {
        if self
            .actions
            .get(&action)
            .is_some_and(|a| a.exited && a.result_received)
            && !self.attempt_mut(activation)?.activated
        {
            self.attempt_mut(activation)?
                .convergence
                .observe_attempt_local_exit();
        }
        Ok(())
    }
}
