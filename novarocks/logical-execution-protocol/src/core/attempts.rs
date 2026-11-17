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
    failure::LogicalRequestFailure,
    input::FailureReport,
    root_control::{RootControlKind, RootObservation},
    start::LogicalFinishBoundary,
};
use novarocks_coordination_contract::logical_port::{
    AttemptAuthorization, AttemptAuthorizationFailure as PortAuthorizationFailure, AttemptTerminal,
    AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
};

impl LogicalExecutionCore {
    pub(super) fn attempt_reply(
        &mut self,
        activation: AttemptActivationIdentity,
        body: LogicalToAttemptBody,
    ) {
        self.queue(LogicalEffectBody::AttemptPort(Box::new(LogicalToAttempt {
            activation,
            body,
        })));
    }
    pub(super) fn apply_attempt(&mut self, message: AttemptToLogical) -> Result<(), CoreFault> {
        let activation = message.activation;
        self.attempt_mut(activation)?;
        match message.body {
            AttemptToLogicalBody::AuthorizationRequest { token, request } => {
                let closed = self.business_closed();
                let now = self.now;
                let attempt = self.attempt_mut(activation)?;
                let decision =
                    if closed && !matches!(request, AttemptAuthorization::SettleAdmission { .. }) {
                        Err(crate::establish::EstablishIssueError::IssueAuthorityRevoked)
                    } else {
                        attempt.establish.authorize(now, request)
                    };
                // Returning this authorization already permits AP to hand off
                // the exact operation. Freeze possible transmission in this
                // SAME serial turn, as the former bridge's try_submit did;
                // no later host/guard event may move this boundary.
                let decision = decision.and_then(|decision| {
                    if let novarocks_coordination_contract::AttemptAuthorizationDecision::EstablishAuthorized(reference) = decision {
                        let (participation, operation_id) = match request {
                            AttemptAuthorization::Establish { participation, operation_id, .. }
                            | AttemptAuthorization::ReauthorizeEstablish { participation, operation_id } => (participation, operation_id),
                            _ => unreachable!("Establish authorization belongs to its exact request"),
                        };
                        attempt.establish.settle_establish(reference, participation, operation_id,
                            novarocks_coordination_contract::AttemptIssueSettlement::TransportOwned)?;
                    }
                    Ok(decision)
                });
                let refresh = match request {
                    AttemptAuthorization::SettleAdmission { participation, .. }
                    | AttemptAuthorization::Establish { participation, .. }
                    | AttemptAuthorization::ReauthorizeEstablish { participation, .. } => {
                        Some(participation)
                    }
                    AttemptAuthorization::BeginAdmission(request) => Some(request.participation()),
                };
                if let Some(p) = refresh
                    && attempt.initialized
                {
                    let fact = attempt
                        .establish
                        .stand_down_fact(p)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                    attempt
                        .stand_down
                        .refresh_establish_fact(p, fact)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                }
                let decision = decision.map_err(|error| match error {
                    crate::establish::EstablishIssueError::AuthorizationBudgetExhausted => {
                        PortAuthorizationFailure::EstablishBudgetExhausted(error.failure())
                    }
                    _ => PortAuthorizationFailure::Rejected(error.failure()),
                });
                self.attempt_reply(
                    activation,
                    LogicalToAttemptBody::AuthorizationReply { token, decision },
                );
            }
            AttemptToLogicalBody::EstablishSettlement {
                token,
                authorization,
                participation,
                operation_id,
                settlement,
            } => {
                let attempt = self.attempt_mut(activation)?;
                let result = attempt.establish.settle_establish(
                    authorization,
                    participation,
                    operation_id,
                    settlement,
                );
                if result.is_ok() && attempt.initialized {
                    let fact = attempt
                        .establish
                        .stand_down_fact(participation)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                    attempt
                        .stand_down
                        .refresh_establish_fact(participation, fact)
                        .map_err(|_| CoreFault::UnexpectedInput)?;
                }
                self.attempt_reply(
                    activation,
                    LogicalToAttemptBody::EstablishSettlementReply {
                        token,
                        authorization,
                        result: result.map(|_| ()).map_err(|error| error.failure()),
                    },
                );
                self.refresh_pre_ready = true;
                self.pre_ready_cursor = None;
            }
            AttemptToLogicalBody::PreReadyReplacementQuery { token, refusal } => {
                let correlated = self
                    .attempt_mut(activation)?
                    .establish
                    .correlates(refusal.input());
                let mut opportunity = self.pre_ready_opportunity();
                opportunity.concluded |= self.supervision_failure.is_some();
                let allowed = self
                    .pre_ready
                    .query(activation, token, refusal.input(), opportunity, correlated)
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                self.attempt_reply(
                    activation,
                    LogicalToAttemptBody::PreReadyReplacementReply {
                        token,
                        may_observe: allowed,
                    },
                );
            }
            AttemptToLogicalBody::AbortSettlement {
                abort_action,
                participation,
                operation_id,
                settlement,
            } => {
                let now = self.now;
                let result = self.attempt_mut(activation)?.stand_down.settle(
                    now,
                    activation,
                    abort_action,
                    participation,
                    operation_id,
                    settlement,
                );
                if let Err(error) = result {
                    self.conclude_failure(Self::local_error(&error));
                }
            }
            AttemptToLogicalBody::RootStatus(projection) => {
                let root = projection.status.identity();
                if root.query_execution_id() != activation.execution() {
                    return Err(CoreFault::ForeignExecution);
                }
                let attempt = self.attempt_mut(activation)?;
                let control = attempt.root.get_or_insert_with(|| RootControl::new(root));
                let observation = control.observe(*projection);
                match observation {
                    Ok(RootObservation::Live | RootObservation::Ignored) => {}
                    Ok(RootObservation::PendingFailure) => {
                        // Even a visible stream waits for the original authoritative
                        // cause. It must not turn a derived placeholder into final failure.
                        let current = self.state.require_running(activation).is_ok();
                        if current && !self.business_closed() {
                            let _frozen = self.visibility.failure(activation, true, false);
                            self.stop_relay(activation);
                        }
                    }
                    Ok(RootObservation::Authoritative(failure)) => self.accept_attempt_failure(
                        activation,
                        FailureReport::from_attempt(&failure),
                    )?,
                    Err(_) => self
                        .conclude_failure(LogicalRequestFailure::ResultProtocolViolation.error()),
                }
            }
            AttemptToLogicalBody::SealResult { token, result } => {
                let succeeded = result.is_ok();
                let attempt = self.attempt_mut(activation)?;
                let root = attempt.root.as_mut().ok_or(CoreFault::UnexpectedInput)?;
                root.settle(RootControlKind::Seal, token, succeeded)
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                match result {
                    Ok(withdrawals) => {
                        if attempt.pending_withdrawals.is_some() {
                            return Err(CoreFault::UnexpectedInput);
                        }
                        attempt.pending_withdrawals = Some((withdrawals, 0));
                    }
                    Err(failure) => self.accept_attempt_failure(
                        activation,
                        FailureReport::from_attempt(&failure),
                    )?,
                }
            }
            AttemptToLogicalBody::TerminalControlResult { token, result } => {
                self.attempt_mut(activation)?
                    .root
                    .as_mut()
                    .ok_or(CoreFault::UnexpectedInput)?
                    .settle(RootControlKind::TerminalDemand, token, result.is_ok())
                    .map_err(|_| CoreFault::UnexpectedInput)?;
                if let Err(failure) = result {
                    self.accept_attempt_failure(activation, FailureReport::from_attempt(&failure))?;
                }
            }
            AttemptToLogicalBody::AttemptTerminal(terminal) => match terminal {
                AttemptTerminal::Completed => {
                    if self.state.require_running(activation).is_ok()
                        && self.start.policy.finish == LogicalFinishBoundary::TaskCompletion
                        && !self.business_closed()
                        && !self.scope_cancellation_requested
                    {
                        match self
                            .attempt_mut(activation)?
                            .establish
                            .ensure_success_ready()
                        {
                            Ok(()) => {
                                self.state.conclude(activation,LogicalConclusion::Succeeded).expect("completion-only success uses the original running activation");
                                self.begin_conclusion();
                                self.queue(LogicalEffectBody::Consumer(
                                    ConsumerEffect::PublishCompletion,
                                ));
                            }
                            Err(error) => self.conclude_failure(error.failure().error().clone()),
                        }
                    }
                }
                AttemptTerminal::Failed(failure) | AttemptTerminal::Stopped { failure, .. } => {
                    self.accept_attempt_failure(activation, FailureReport::from_attempt(&failure))?;
                }
            },
            AttemptToLogicalBody::AttemptLost { failure, .. } => {
                // The host-authored value never inspects the failed attempt Core.
                self.accept_attempt_failure(activation, FailureReport::from_attempt(&failure))?;
            }
            AttemptToLogicalBody::ConvergenceProgress {
                local_exited,
                closure,
                lifecycle_runtime_filters,
            } => {
                let attempt = self.attempt_mut(activation)?;
                if local_exited {
                    attempt.convergence.observe_attempt_local_exit();
                }
                if let Some(closure) = closure {
                    if attempt.closure.is_some() {
                        return Err(CoreFault::UnexpectedInput);
                    }
                    attempt.closure = Some(ClosurePass {
                        original: closure,
                        index: 0,
                        validating: true,
                        retiring: false,
                        seen: BTreeSet::new(),
                    });
                }
                if let Some(runtime_filters) = lifecycle_runtime_filters {
                    attempt.runtime_filters = Some(runtime_filters);
                }
            }
        }
        Ok(())
    }
    pub(super) fn local_error(
        error: &impl novarocks_execution_contract::failure::FailureProjection,
    ) -> QueryExecutionError {
        QueryExecutionError::from_execution(
            novarocks_coordination_contract::ExecutionFailureInput::new(
                error.failure_fact(),
                novarocks_execution_contract::SafeDetail::truncating(
                    "logical execution cleanup protocol failed",
                ),
                novarocks_coordination_contract::ExecutionFailureSite::Coordinator,
            ),
        )
    }
}
