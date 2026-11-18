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
    LogicalRequestToken, LogicalSourceId,
    effect::{InternalResponse, RelayResponse},
    failure::LogicalRequestFailure,
    input::{InternalRequest, RelayRequest},
    ready::{ReadyProposal, ReadyRefusal},
    root_control::RootControlKind,
    visibility::{EndSummary, OpenDecision, ResultPathRefusal},
};

impl LogicalExecutionCore {
    pub(super) fn relay_reply(&mut self, waiter: RelayWaiter, response: RelayResponse) {
        self.queue(LogicalEffectBody::Relay(RelayEffect::Reply {
            source: waiter.source,
            token: waiter.token,
            activation: waiter.activation,
            response,
        }));
    }
    fn internal_reply(
        &mut self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        response: InternalResponse,
    ) {
        self.queue(LogicalEffectBody::Consumer(ConsumerEffect::InternalReply {
            source,
            token,
            activation,
            response,
        }));
    }
    pub(super) fn apply_relay(
        &mut self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: RelayRequest,
    ) -> Result<(), CoreFault> {
        self.attempt_mut(activation)?;
        let waiter = RelayWaiter {
            source,
            token,
            activation,
        };
        if let Some(error) = self.supervision_failure.clone()
            && matches!(
                request,
                RelayRequest::OpenDelivery { .. }
                    | RelayRequest::RequestVisibility
                    | RelayRequest::EndConsumed { .. }
                    | RelayRequest::RootWithdrawn { .. }
            )
        {
            self.relay_reply(waiter, RelayResponse::Rejected(error));
            return Ok(());
        }
        let current = self.state.require_running(activation).is_ok();
        let concluded = self.state.conclusion().is_some();
        match request {
            RelayRequest::OpenDelivery { root, carrier } => {
                match self
                    .visibility
                    .open_delivery(activation, root, carrier, current, concluded)
                {
                    Ok(decision) => {
                        if self
                            .attempt_mut(activation)?
                            .root
                            .as_ref()
                            .is_none_or(|r| r.root() != root)
                        {
                            self.conclude_failure(
                                LogicalRequestFailure::ResultProtocolViolation.error(),
                            );
                            self.relay_reply(waiter, RelayResponse::Rejected(self.request_error()));
                        } else {
                            if decision == OpenDecision::WriteSchema {
                                self.queue(LogicalEffectBody::Consumer(
                                    ConsumerEffect::WriteSchema { activation },
                                ));
                            }
                            self.relay_reply(
                                waiter,
                                if decision == OpenDecision::DeliveryGranted {
                                    RelayResponse::DeliveryGranted
                                } else {
                                    RelayResponse::Opened
                                },
                            );
                        }
                    }
                    Err(error) => {
                        self.relay_reply(waiter, RelayResponse::Rejected(self.path_error(error)))
                    }
                }
            }
            RelayRequest::RequestVisibility => {
                match self
                    .visibility
                    .grant_visibility(activation, current, concluded)
                {
                    Ok(()) => {
                        self.state
                            .mark_output_visible(activation)
                            .expect("visibility is granted by the current running owner");
                        self.refresh_pre_ready = true;
                        self.pre_ready_cursor = None;
                        self.relay_reply(waiter, RelayResponse::VisibilityGranted);
                    }
                    Err(ResultPathRefusal::SchemaPending) => {
                        if self.visibility_waiter.is_some() {
                            self.relay_reply(
                                waiter,
                                RelayResponse::Rejected(LogicalRequestFailure::WrongPhase.error()),
                            );
                        } else {
                            self.visibility_waiter = Some(waiter);
                        }
                    }
                    Err(error) => {
                        self.relay_reply(waiter, RelayResponse::Rejected(self.path_error(error)))
                    }
                }
            }
            RelayRequest::DeliveryFailed(failure) => {
                self.accept_attempt_failure(activation, failure)?;
                self.relay_reply(waiter, RelayResponse::Applied);
            }
            RelayRequest::RootWithdrawn { root } => {
                if !current || concluded {
                    self.relay_reply(waiter, RelayResponse::Rejected(self.request_error()));
                } else {
                    let control = self
                        .attempt_mut(activation)?
                        .root
                        .as_mut()
                        .ok_or(CoreFault::UnexpectedInput)?;
                    if control.root() != root {
                        return Err(CoreFault::ForeignExecution);
                    }
                    control.request_terminal();
                    let _pending = self.visibility.failure(activation, true, false);
                    self.reject_visibility_waiter(
                        LogicalRequestFailure::RootAttemptTerminal.error(),
                    );
                    self.relay_reply(waiter, RelayResponse::Applied);
                    self.stop_relay(activation);
                }
            }
            RelayRequest::EndConsumed {
                root,
                end,
                accepted_packets,
                delivered_through,
            } => {
                let summary = EndSummary {
                    end,
                    accepted_packets,
                    delivered_through,
                };
                if let Err(error) = self
                    .visibility
                    .end_consumed(activation, root, summary, current, concluded)
                {
                    let error = self.path_error(error);
                    self.conclude_failure(error.clone());
                    self.relay_reply(waiter, RelayResponse::Rejected(error));
                } else {
                    self.attempt_mut(activation)?
                        .root
                        .as_mut()
                        .ok_or(CoreFault::UnexpectedInput)?
                        .request_seal();
                    if self.start.policy.output == LogicalOutputMode::InternalResultStream {
                        self.relay_reply(waiter, RelayResponse::Applied);
                    } else if self.end_waiter.is_some() {
                        self.relay_reply(
                            waiter,
                            RelayResponse::Rejected(LogicalRequestFailure::WrongPhase.error()),
                        );
                    } else {
                        self.end_waiter = Some(waiter);
                    }
                }
            }
            RelayRequest::Stopped {
                all_deliveries_settled,
            } => {
                self.attempt_mut(activation)?
                    .convergence
                    .observe_relay_stopped(all_deliveries_settled);
                self.relay_reply(waiter, RelayResponse::Applied);
            }
        }
        Ok(())
    }
    fn path_error(&self, error: ResultPathRefusal) -> QueryExecutionError {
        match error {
            ResultPathRefusal::Concluded => self.request_error(),
            ResultPathRefusal::Frozen => LogicalRequestFailure::RootAttemptTerminal.error(),
            ResultPathRefusal::ForeignActivation => LogicalRequestFailure::StaleAuthority.error(),
            ResultPathRefusal::NotOpened
            | ResultPathRefusal::SchemaPending
            | ResultPathRefusal::DuplicateOpen => LogicalRequestFailure::WrongPhase.error(),
            _ => LogicalRequestFailure::ResultProtocolViolation.error(),
        }
    }

    pub(super) fn apply_internal(
        &mut self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: InternalRequest,
    ) -> Result<(), CoreFault> {
        self.attempt_mut(activation)?;
        if self.ready.is_none() {
            self.internal_reply(
                source,
                token,
                activation,
                InternalResponse::Rejected(LogicalRequestFailure::WrongPhase.error()),
            );
            return Ok(());
        }
        if let Some(error) = self.supervision_failure.clone()
            && matches!(
                request,
                InternalRequest::RegisterSource { .. } | InternalRequest::AcceptReady { .. }
            )
        {
            self.internal_reply(source, token, activation, InternalResponse::Rejected(error));
            return Ok(());
        }
        let current = self.state.require_running(activation).is_ok();
        let concluded = self.state.conclusion().is_some();
        match request {
            InternalRequest::RegisterSource {
                root,
                plan,
                supervised_exit,
            } => {
                match self.ready.as_mut().unwrap().register(
                    source,
                    activation,
                    root,
                    plan,
                    supervised_exit,
                    current,
                    concluded,
                ) {
                    Ok(retired) => {
                        if let Some(old) = retired {
                            self.internal_reply(
                                old.source,
                                old.token,
                                old.activation,
                                InternalResponse::Rejected(
                                    LogicalRequestFailure::StaleAuthority.error(),
                                ),
                            );
                        }
                        self.internal_reply(
                            source,
                            token,
                            activation,
                            InternalResponse::SourceRegistered,
                        );
                    }
                    Err(error) => self.reject_internal(source, token, activation, error),
                }
            }
            InternalRequest::EndDecoded { receipt, end } => {
                match self
                    .ready
                    .as_mut()
                    .unwrap()
                    .decoded_end(source, activation, receipt, end)
                {
                    Ok(()) => {
                        self.internal_reply(source, token, activation, InternalResponse::Applied)
                    }
                    Err(error) => self.reject_internal(source, token, activation, error),
                }
            }
            InternalRequest::AcceptReady {
                root,
                plan,
                receipt,
                end,
            } => {
                let proposal = ReadyProposal {
                    source,
                    token,
                    activation,
                    root,
                    plan,
                    receipt,
                    end,
                };
                match self.ready.as_mut().unwrap().propose(proposal, concluded) {
                    Ok(Some(a)) => {
                        self.queue(LogicalEffectBody::Consumer(ConsumerEffect::PublishReady {
                            source: a.source,
                            token: a.token,
                            cut: a.cut,
                        }))
                    }
                    Ok(None) => {}
                    Err(error) => self.reject_internal(source, token, activation, error),
                }
            }
            InternalRequest::Failed(failure) => {
                if concluded {
                    self.internal_reply(
                        source,
                        token,
                        activation,
                        InternalResponse::Rejected(self.request_error()),
                    );
                    return Ok(());
                }
                if self.ready.as_ref().unwrap().is_source(source, activation) {
                    if let Some(input) = failure.original_attempt_failure.as_deref() {
                        self.state
                            .record_original_attempt_failure(activation, input.clone())
                            .map_err(|_| CoreFault::ForeignActivation)?;
                    }
                    self.conclude_failure(failure.error);
                    self.internal_reply(source, token, activation, InternalResponse::Applied);
                } else {
                    self.reject_internal(source, token, activation, ReadyRefusal::ForeignSource);
                }
            }
            InternalRequest::SourceClosed => {
                match self
                    .ready
                    .as_mut()
                    .unwrap()
                    .source_closed(source, activation)
                {
                    Ok(error) => {
                        if let Some(error) = error {
                            self.conclude_failure(error);
                        }
                        self.internal_reply(source, token, activation, InternalResponse::Applied);
                    }
                    Err(error) => self.reject_internal(source, token, activation, error),
                }
            }
            InternalRequest::ExitObserved { outcome } => {
                match self
                    .ready
                    .as_mut()
                    .unwrap()
                    .exit_observed(source, activation, outcome)
                {
                    Ok(error) => {
                        if let Some(error) = error {
                            self.conclude_failure(error);
                        }
                        self.internal_reply(source, token, activation, InternalResponse::Applied);
                    }
                    Err(error) => self.reject_internal(source, token, activation, error),
                }
            }
        }
        Ok(())
    }
    fn reject_internal(
        &mut self,
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        refusal: ReadyRefusal,
    ) {
        let error = match refusal {
            ReadyRefusal::ForeignSource
            | ReadyRefusal::ForeignActivation
            | ReadyRefusal::ForeignRoot
            | ReadyRefusal::ForeignPlan => LogicalRequestFailure::StaleAuthority.error(),
            ReadyRefusal::Concluded => self.request_error(),
            ReadyRefusal::DuplicateSource | ReadyRefusal::DuplicateProposal => {
                LogicalRequestFailure::WrongPhase.error()
            }
            ReadyRefusal::MissingEndReceipt | ReadyRefusal::ConflictingEndReceipt => {
                let error = LogicalRequestFailure::ResultProtocolViolation.error();
                self.conclude_failure(error.clone());
                error
            }
        };
        self.internal_reply(source, token, activation, InternalResponse::Rejected(error));
    }

    pub(super) fn drive_results(&mut self, budget: &mut dyn StepBudget) -> Result<(), CoreFault> {
        if let Some(waiter) = self.visibility_waiter {
            let current = self.state.require_running(waiter.activation).is_ok();
            let concluded = self.state.conclusion().is_some();
            match self
                .visibility
                .grant_visibility(waiter.activation, current, concluded)
            {
                Ok(()) => {
                    self.visibility_waiter = None;
                    self.state
                        .mark_output_visible(waiter.activation)
                        .expect("pending visibility retains the original running owner");
                    self.relay_reply(waiter, RelayResponse::VisibilityGranted);
                    self.refresh_pre_ready = true;
                    self.pre_ready_cursor = None;
                }
                Err(ResultPathRefusal::SchemaPending) => {}
                Err(error) => {
                    self.visibility_waiter = None;
                    self.relay_reply(waiter, RelayResponse::Rejected(self.path_error(error)));
                }
            }
        }
        let Some(activation) = self.state.current_activation() else {
            return Ok(());
        };
        if self.business_closed() {
            return Ok(());
        }
        let attempt = self
            .attempts
            .get(&activation.execution())
            .ok_or(CoreFault::ForeignActivation)?;
        let finished = attempt.root.as_ref().is_some_and(RootControl::finished);
        let sealed = attempt.root.as_ref().is_some_and(RootControl::sealed)
            && attempt.pending_withdrawals.is_none();
        if !finished || !sealed || self.visibility.end(activation).is_none() {
            return Ok(());
        }
        let establish_ready = match attempt.establish.ensure_success_ready() {
            Ok(()) => true,
            Err(crate::establish::EstablishIssueError::EstablishNotSettled) => false,
            Err(error) => {
                self.conclude_failure(error.failure().error().clone());
                return Ok(());
            }
        };
        if self.start.policy.output == LogicalOutputMode::ResultStream
            && self.end_waiter.is_some()
            && self
                .visibility
                .request_end_slot(activation, finished, sealed, establish_ready)
                .map_err(|_| CoreFault::UnexpectedInput)?
        {
            self.queue(LogicalEffectBody::Consumer(
                ConsumerEffect::ReserveEndSlot { activation },
            ));
        }
        if self
            .ready
            .as_ref()
            .is_some_and(InternalReady::has_pending_proposal)
        {
            if !budget.charge(1) {
                self.continuation = true;
                return Ok(());
            }
            let end = self.visibility.end(activation).map(|summary| summary.end);
            let frozen = self.visibility.frozen(activation) || self.scope_cancellation_requested;
            match self.ready.as_mut().unwrap().try_accept(
                self.now,
                activation,
                finished,
                sealed,
                establish_ready,
                end,
                frozen,
                false,
            ) {
                Ok(Some(a)) => {
                    self.state.accept_computation_ready(activation).expect(
                        "validated source and protocol facts accept one original Ready cut",
                    );
                    self.visibility.commit_ready();
                    self.begin_conclusion();
                    self.queue(LogicalEffectBody::Native(
                        NativeEffect::RecordComputationReady { cut: a.cut },
                    ));
                    self.queue(LogicalEffectBody::Consumer(ConsumerEffect::PublishReady {
                        source: a.source,
                        token: a.token,
                        cut: a.cut,
                    }));
                }
                Ok(None) => {}
                Err(error) => self.conclude_failure(error),
            }
        }
        Ok(())
    }
    pub(super) fn drive_root_control(&mut self, execution: QueryExecutionId) {
        let pending = !self.business_closed()
            && self
                .attempts
                .get(&execution)
                .filter(|a| a.closing.is_none())
                .and_then(|a| a.root.as_ref())
                .is_some_and(RootControl::has_request);
        if !pending {
            return;
        }
        let token = self.issue_request();
        let attempt = self.attempts.get_mut(&execution).unwrap();
        let Some(request) = attempt.root.as_mut().unwrap().next_request(token) else {
            return;
        };
        let activation = attempt.activation;
        let body=match request.kind {
            RootControlKind::TerminalDemand=>novarocks_coordination_contract::logical_port::LogicalToAttemptBody::TerminalControlDemand{token:request.token,root:request.root},
            RootControlKind::Seal=>novarocks_coordination_contract::logical_port::LogicalToAttemptBody::SealRequest{token:request.token},
        };
        self.attempt_reply(activation, body);
    }
    pub(super) fn reject_visibility_waiter(&mut self, error: QueryExecutionError) {
        if let Some(waiter) = self.visibility_waiter.take() {
            self.relay_reply(waiter, RelayResponse::Rejected(error));
        }
    }
    pub(super) fn reject_pending_result_requests(&mut self, error: QueryExecutionError) {
        self.reject_visibility_waiter(error.clone());
        if let Some(waiter) = self.end_waiter.take() {
            self.relay_reply(waiter, RelayResponse::Rejected(error.clone()));
        }
        if let Some(old) = self
            .ready
            .as_mut()
            .and_then(InternalReady::take_pending_proposal)
        {
            self.internal_reply(
                old.source,
                old.token,
                old.activation,
                InternalResponse::Rejected(error),
            );
        }
    }
}
