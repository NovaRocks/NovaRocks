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

use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

use novarocks_coordination_contract::{AttemptActivationIdentity, MonotonicInstant};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};
use novarocks_types::identity::QueryExecutionId;
use tokio::sync::{mpsc, oneshot, watch};

use crate::api::{
    ComputationReadySource, ComputationReadyToken, DecodedInternalEndReceipt,
    InternalConsumerActivation, InternalConsumerSeal, InternalEndAcknowledgement,
    InternalEndDelivery, InternalResultStream, PlanSeal, QueryExecutionError,
    ResultDeliveryDisposition, ResultDeliveryReceipt,
};

use super::{
    ActorCommand, ActorReply, AttemptLedgers, DeferredInternalDelivery, DeliveryPermit,
    EstablishIssueError, InFlightResult, LogicalConclusion, LogicalExecutionActorError,
    LogicalExecutionState, PendingResult, PermitLifetime, ResultRuntime, ResultSlot,
    ResultTransport, conclude_result_failure, identity_of, logical_owner_failure, request,
    result_actor_failure, result_receipt_failure, revoke_all_establish_authority,
    root_success_facts_complete, verify_running_activation,
};

/// Concrete QA mailbox capability. Application consumers cannot construct a
/// source or substitute an arbitrary readiness policy behind this port.
#[derive(Debug)]
pub(crate) struct InternalReadyActorPort {
    source: Arc<InternalConsumerSeal>,
    mailbox: mpsc::Sender<ActorCommand>,
}

impl InternalReadyActorPort {
    pub(crate) fn origin(&self) -> &InternalConsumerSeal {
        &self.source
    }

    pub(crate) async fn accept(
        &self,
        decoded: DecodedInternalEndReceipt,
    ) -> Result<ComputationReadyToken, QueryExecutionError> {
        request(&self.mailbox, |reply| {
            ActorCommand::AcceptComputationReady(ReadyProposal {
                source: Arc::clone(&self.source),
                decoded,
                reply,
            })
        })
        .await
        .map_err(result_actor_failure)
    }

    pub(crate) async fn supervise_exit(
        &self,
        receiver: oneshot::Receiver<Result<(), QueryExecutionError>>,
    ) -> Result<(), LogicalExecutionActorError> {
        request(&self.mailbox, |reply| {
            ActorCommand::SuperviseInternalConsumer {
                source: Arc::clone(&self.source),
                receiver,
                reply,
            }
        })
        .await
    }

    pub(crate) async fn fail(&self, error: QueryExecutionError) -> Result<(), QueryExecutionError> {
        request(&self.mailbox, |reply| {
            ActorCommand::FailInternalComputation {
                source: Arc::clone(&self.source),
                error,
                reply,
            }
        })
        .await
        .map_err(result_actor_failure)
    }
}

pub(super) struct BindConsumer {
    pub(super) activation: AttemptActivationIdentity,
    pub(super) root: TaskIdentity,
    pub(super) lifetime: Arc<PermitLifetime>,
    pub(super) mailbox: mpsc::Sender<ActorCommand>,
    pub(super) reply: ActorReply<InternalConsumerActivation>,
}

pub(super) struct ReadyProposal {
    pub(super) source: Arc<InternalConsumerSeal>,
    pub(super) decoded: DecodedInternalEndReceipt,
    pub(super) reply: ActorReply<ComputationReadyToken>,
}

pub(super) struct InternalRuntime {
    plan: PlanSeal,
    capacity: NonZeroUsize,
    budgets: crate::api::ComputationObservationBudgets,
    pub(super) consumer: Option<ConsumerRecord>,
}

pub(super) struct ConsumerRecord {
    pub(super) source: Arc<InternalConsumerSeal>,
    lifetime: Arc<PermitLifetime>,
    liveness: Option<oneshot::Receiver<()>>,
    supervised: bool,
    exit: Option<oneshot::Receiver<Result<(), QueryExecutionError>>>,
    ready: watch::Sender<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
    pub(super) decoded_end: Option<RootResultEnd>,
    pending: Option<ReadyProposal>,
    accepted: Option<ComputationReadyToken>,
}

impl InternalRuntime {
    pub(super) fn new(
        plan: PlanSeal,
        capacity: NonZeroUsize,
        budgets: crate::api::ComputationObservationBudgets,
    ) -> Self {
        Self {
            plan,
            capacity,
            budgets,
            consumer: None,
        }
    }
    pub(super) fn exit_supervised(&self) -> bool {
        self.consumer
            .as_ref()
            .is_some_and(|record| record.supervised)
    }

    pub(super) fn source_pending(&self) -> bool {
        self.consumer.as_ref().is_some_and(|record| {
            if record.supervised {
                record.exit.is_some()
            } else {
                record.liveness.is_some()
            }
        })
    }
    pub(super) fn idle(&self) -> bool {
        self.consumer.as_ref().is_none_or(|record| {
            record.pending.is_none()
                && if record.supervised {
                    record.exit.is_none()
                } else {
                    record.liveness.is_none()
                }
        })
    }
}

pub(super) fn bind(
    state: &LogicalExecutionState,
    runtime: &mut ResultRuntime,
    binding: BindConsumer,
) {
    let BindConsumer {
        activation,
        root,
        lifetime,
        mailbox,
        reply,
    } = binding;
    let result = (|| {
        verify_running_activation(state, activation)?;
        if root.query_execution_id() != activation.execution()
            || runtime.pending.is_some()
            || runtime.in_flight.is_some()
            || runtime.deferred_internal.is_some()
        {
            return Err(LogicalExecutionActorError::ResultProtocolViolation);
        }
        let internal = runtime
            .internal
            .as_mut()
            .ok_or(LogicalExecutionActorError::WrongPhase)?;
        if internal
            .consumer
            .as_ref()
            .is_some_and(|record| record.source.activation == activation)
        {
            return Err(LogicalExecutionActorError::WrongPhase);
        }
        let source = Arc::new(InternalConsumerSeal {
            activation,
            root,
            plan: internal.plan,
        });
        let port = InternalReadyActorPort {
            source: Arc::clone(&source),
            mailbox,
        };
        let (proof_source, liveness) = ComputationReadySource::issued(port);
        let (transport, failure_sender, stream) =
            InternalResultStream::channel(Arc::clone(&source), runtime.carrier, internal.capacity);
        let (ready_sender, ready) = watch::channel(None);
        internal.consumer = Some(ConsumerRecord {
            source,
            lifetime,
            liveness: Some(liveness),
            supervised: false,
            exit: None,
            ready: ready_sender,
            decoded_end: None,
            pending: None,
            accepted: None,
        });
        runtime.transport = ResultTransport::Internal(transport);
        runtime.failure_sender = Some(failure_sender);
        Ok(InternalConsumerActivation {
            stream,
            source: proof_source,
            ready,
        })
    })();
    let _ = reply.send(result);
}

pub(super) fn propose(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
    proposal: ReadyProposal,
) {
    let Some(record) = runtime
        .internal
        .as_mut()
        .and_then(|internal| internal.consumer.as_mut())
    else {
        let _ = proposal
            .reply
            .send(Err(LogicalExecutionActorError::WrongPhase));
        return;
    };
    if !Arc::ptr_eq(&record.source, &proposal.source) {
        let _ = proposal
            .reply
            .send(Err(LogicalExecutionActorError::StaleAuthority));
        return;
    }
    if !record
        .decoded_end
        .is_some_and(|end| proposal.decoded.matches(&record.source, end))
    {
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "internal proof has no accepted End receipt",
            result_actor_failure(LogicalExecutionActorError::ResultProtocolViolation),
        );
        let _ = proposal
            .reply
            .send(Err(LogicalExecutionActorError::ResultProtocolViolation));
        return;
    }
    if let Some(token) = record.accepted {
        let _ = proposal.reply.send(Ok(token));
    } else if let Some(conclusion) = state.conclusion() {
        let _ = proposal
            .reply
            .send(Err(LogicalExecutionActorError::ExecutionConcluded(
                conclusion,
            )));
    } else if record.pending.is_some() {
        let _ = proposal
            .reply
            .send(Err(LogicalExecutionActorError::WrongPhase));
    } else {
        record.pending = Some(proposal);
    }
}

pub(super) fn try_accept(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: Option<&mut ResultRuntime>,
    now: MonotonicInstant,
) {
    if state.conclusion().is_some() {
        return;
    }
    let Some(runtime) = runtime else {
        return;
    };
    if runtime.attempt_decision_pending.is_some()
        || runtime.deferred_internal.is_some()
        || runtime.attempt_error.is_some()
        || !root_success_facts_complete(Some(runtime))
    {
        return;
    }
    let Some(internal) = runtime.internal.as_mut() else {
        return;
    };
    let budgets = internal.budgets;
    let Some(record) = internal.consumer.as_mut() else {
        return;
    };
    if record.pending.is_none() {
        return;
    }
    if !matches!(
        record.liveness.as_mut().map(oneshot::Receiver::try_recv),
        Some(Err(oneshot::error::TryRecvError::Empty))
    ) {
        if record.supervised {
            // The process owner still holds the reserved actual-exit receipt.
            // Source loss prevents Ready, but must not race and replace that
            // consumer's original typed verdict with a destructor observation.
            return;
        }
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "internal proof source exited before its cut",
            logical_owner_failure(
                crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                "internal proof source exited before its cut",
            ),
        );
        return;
    }
    let activation = record.source.activation;
    let readiness = attempts
        .get_mut(&activation.execution())
        .ok_or(EstablishIssueError::UnknownContext)
        .and_then(AttemptLedgers::ensure_success_ready);
    match readiness {
        Err(EstablishIssueError::EstablishNotSettled) => return,
        Err(error) => {
            revoke_all_establish_authority(attempts);
            conclude_result_failure(
                state,
                runtime,
                "internal proof has invalid Establish settlement",
                result_actor_failure(LogicalExecutionActorError::Establish(error)),
            );
            return;
        }
        Ok(()) => {}
    }
    let capability = match state.attempt_capability(activation.execution()) {
        Ok(capability) if identity_of(&capability) == activation => capability,
        _ => return,
    };
    // Readiness and the exact running capability have been validated. Close
    // transport issue authority before the terminal cut or either reply.
    revoke_all_establish_authority(attempts);
    if let Err(error) = state.accept_computation_ready(&capability) {
        conclude_result_failure(
            state,
            runtime,
            "internal computation cut could not be accepted",
            result_actor_failure(error.into()),
        );
        return;
    }
    let proposal = record
        .pending
        .take()
        .expect("validated internal proof remains owned");
    let end = record
        .decoded_end
        .expect("accepted internal proof has its original End");
    let token = ComputationReadyToken::accepted(&record.source, now, end, budgets);
    record.accepted = Some(token);
    record.lifetime.settle();
    record.ready.send_replace(Some(Ok(token)));
    let _ = proposal.reply.send(Ok(token));
}

pub(super) fn fail(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
    source: Arc<InternalConsumerSeal>,
    error: QueryExecutionError,
    reply: ActorReply<()>,
) {
    let current = runtime
        .internal
        .as_ref()
        .and_then(|internal| internal.consumer.as_ref())
        .is_some_and(|record| Arc::ptr_eq(&record.source, &source));
    if !current {
        let _ = reply.send(Err(LogicalExecutionActorError::StaleAuthority));
    } else if let Some(conclusion) = state.conclusion() {
        let _ = reply.send(Err(LogicalExecutionActorError::ExecutionConcluded(
            conclusion,
        )));
    } else {
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "the internal domain rejected its computation",
            error,
        );
        let _ = reply.send(Ok(()));
    }
}

pub(super) fn supervise_exit(
    state: &LogicalExecutionState,
    runtime: &mut ResultRuntime,
    source: Arc<InternalConsumerSeal>,
    receiver: oneshot::Receiver<Result<(), QueryExecutionError>>,
    reply: ActorReply<()>,
) {
    let result = (|| {
        verify_running_activation(state, source.activation)?;
        let record = runtime
            .internal
            .as_mut()
            .and_then(|internal| internal.consumer.as_mut())
            .ok_or(LogicalExecutionActorError::StaleAuthority)?;
        if !Arc::ptr_eq(&record.source, &source) {
            return Err(LogicalExecutionActorError::StaleAuthority);
        }
        if record.supervised {
            return Err(LogicalExecutionActorError::WrongPhase);
        }
        record.supervised = true;
        record.exit = Some(receiver);
        Ok(())
    })();
    let _ = reply.send(result);
}

pub(super) struct ConsumerExit {
    source: Arc<InternalConsumerSeal>,
    outcome: Option<Result<(), QueryExecutionError>>,
}

pub(super) async fn wait_for_source_exit(internal: Option<&mut InternalRuntime>) -> ConsumerExit {
    let record = internal
        .and_then(|internal| internal.consumer.as_mut())
        .expect("source wait is gated by its consumer");
    let outcome = if record.supervised {
        let outcome = record
            .exit
            .as_mut()
            .expect("supervised consumer owns its reserved exit receipt")
            .await
            .ok();
        record.exit.take();
        outcome
    } else {
        let _ = record
            .liveness
            .as_mut()
            .expect("source wait has its reserved receiver")
            .await;
        record.liveness.take();
        None
    };
    ConsumerExit {
        source: Arc::clone(&record.source),
        outcome,
    }
}

pub(super) fn source_exited(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: Option<&mut ResultRuntime>,
    exit: ConsumerExit,
) {
    let Some(runtime) = runtime else {
        return;
    };
    if state.conclusion().is_some() {
        return;
    }
    let current = runtime
        .internal
        .as_ref()
        .and_then(|internal| internal.consumer.as_ref())
        .is_some_and(|record| Arc::ptr_eq(&record.source, &exit.source));
    if current {
        let error = match exit.outcome {
            Some(Err(error)) => error,
            Some(Ok(())) | None => logical_owner_failure(
                crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                "the internal consumer exited before readiness",
            ),
        };
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "the internal consumer exited before readiness",
            error,
        );
    }
}

fn terminal_error(runtime: &ResultRuntime) -> QueryExecutionError {
    runtime
        .accepted_error
        .clone()
        .or_else(|| {
            runtime
                .attempt_error
                .as_ref()
                .map(|failure| failure.error.clone())
        })
        .unwrap_or_else(|| {
            logical_owner_failure(
                crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                "internal computation ended before readiness",
            )
        })
}

/// One actor-selected logical outcome survives every activation retirement.
/// Publishing after terminal delivery settlement keeps all observers on the
/// same accepted cause; a late observation can never replace this value.
pub(super) fn publish_outcome(state: &LogicalExecutionState, runtime: Option<&ResultRuntime>) {
    let Some(runtime) = runtime else {
        return;
    };
    let Some(sender) = runtime.internal_outcome.as_ref() else {
        return;
    };
    if sender.borrow().is_some() {
        return;
    }
    let outcome = match state.conclusion() {
        Some(LogicalConclusion::ComputationReady) => {
            let token = runtime
                .internal
                .as_ref()
                .and_then(|internal| internal.consumer.as_ref())
                .and_then(|record| record.accepted)
                .expect("actor-accepted internal readiness retains its original token");
            Ok(token)
        }
        Some(LogicalConclusion::Failed | LogicalConclusion::Cancelled) => {
            Err(terminal_error(runtime))
        }
        Some(LogicalConclusion::Succeeded) | None => return,
    };
    sender.send_replace(Some(outcome));
}

pub(super) fn notify_terminal(state: &LogicalExecutionState, runtime: &mut ResultRuntime) {
    let Some(conclusion) = state.conclusion() else {
        return;
    };
    if conclusion == LogicalConclusion::ComputationReady {
        return;
    }
    let error = terminal_error(runtime);
    if let Some(record) = runtime
        .internal
        .as_mut()
        .and_then(|internal| internal.consumer.as_mut())
    {
        if record.ready.borrow().is_none() {
            record.ready.send_replace(Some(Err(error)));
        }
        if let Some(proposal) = record.pending.take() {
            let _ = proposal
                .reply
                .send(Err(LogicalExecutionActorError::ExecutionConcluded(
                    conclusion,
                )));
        }
        record.lifetime.settle();
    }
}

/// Retire only the old consumer and correlation metadata. Original CPU jobs
/// and material stay under their existing domain/Work custody until actual exit.
pub(super) fn retire(runtime: &mut ResultRuntime, activation: AttemptActivationIdentity) {
    let Some(internal) = runtime.internal.as_mut() else {
        return;
    };
    if !internal
        .consumer
        .as_ref()
        .is_some_and(|record| record.source.activation == activation)
    {
        return;
    }
    let error = runtime
        .attempt_error
        .as_ref()
        .filter(|failure| failure.activation == activation)
        .map(|failure| failure.error.clone())
        .unwrap_or_else(|| {
            logical_owner_failure(
                crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                "internal consumer belongs to a retired attempt",
            )
        });
    if let Some(mut record) = internal.consumer.take() {
        // Retirement is not a logical terminal verdict. Closing this
        // activation's observer must not publish a statement failure while
        // the supervisor is evaluating replacement.
        if let Some(proposal) = record.pending.take() {
            let _ = proposal
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
        }
    }
    if let Some(sender) = runtime.failure_sender.take() {
        sender.send_replace(Some(error));
    }
    runtime.transport = ResultTransport::UnboundInternal;
    if let Some(deferred) = runtime.deferred_internal.take() {
        deferred.retire();
    }
    if let Some(pending) = runtime.pending.take() {
        super::reject_pending_result_for_retirement(pending);
    }
    if let Some(in_flight) = runtime.in_flight.take() {
        super::retire_in_flight_result(in_flight);
    }
}

pub(super) struct EndRequest {
    pub(super) activation: AttemptActivationIdentity,
    pub(super) root: TaskIdentity,
    pub(super) end: RootResultEnd,
    pub(super) reply: ActorReply<()>,
}

pub(super) struct PendingEnd {
    source: Arc<InternalConsumerSeal>,
    end: RootResultEnd,
    pub(super) reply: ActorReply<()>,
}

pub(super) struct EndInFlight {
    source: Arc<InternalConsumerSeal>,
    end: RootResultEnd,
    permit: DeliveryPermit<()>,
    pub(super) receipt: ResultDeliveryReceipt,
    pub(super) checked: InternalEndAcknowledgement,
    pub(super) reply: ActorReply<()>,
}

pub(super) fn queue_end(
    state: &LogicalExecutionState,
    runtime: &mut ResultRuntime,
    request: EndRequest,
) {
    let result = (|| {
        verify_running_activation(state, request.activation)?;
        let record = runtime
            .internal
            .as_ref()
            .and_then(|internal| internal.consumer.as_ref())
            .ok_or(LogicalExecutionActorError::WrongPhase)?;
        let sequence =
            novarocks_execution_contract::ResultPacketSequence::new(request.end.sequence.get() - 1);
        if record.source.activation != request.activation || record.source.root != request.root {
            return Err(LogicalExecutionActorError::StaleAuthority);
        }
        if runtime.pending.is_some()
            || runtime.in_flight.is_some()
            || runtime.deferred_internal.is_some()
            || record.decoded_end.is_some()
            || sequence.get() != state.accepted_result_packets()
            || (sequence.get() > 0 && state.delivered_result_through() != Some(sequence.get() - 1))
            || !runtime.root_success.as_ref().is_some_and(|gate| {
                gate.activation == request.activation
                    && gate.root == request.root
                    && gate.final_eos_ack == Some(sequence)
                    && gate.root_output_rows == Some(request.end.output_rows)
            })
        {
            return Err(LogicalExecutionActorError::ResultProtocolViolation);
        }
        Ok(Arc::clone(&record.source))
    })();
    match result {
        Ok(source) => {
            runtime.pending = Some(PendingResult::InternalEnd(PendingEnd {
                source,
                end: request.end,
                reply: request.reply,
            }))
        }
        Err(error) => {
            let _ = request.reply.send(Err(error));
        }
    }
}

pub(super) fn enqueue_end(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
    slot: ResultSlot,
    pending: PendingEnd,
) {
    let sequence = pending.end.sequence.get() - 1;
    let capability = match state.attempt_capability(pending.source.activation.execution()) {
        Ok(capability) if identity_of(&capability) == pending.source.activation => capability,
        _ => {
            let _ = pending
                .reply
                .send(Err(LogicalExecutionActorError::StaleAuthority));
            return;
        }
    };
    let permit = match state.begin_internal_end(&capability, sequence) {
        Ok(permit) => permit,
        Err(error) => {
            let _ = pending
                .reply
                .send(Err(ActorStateError::Delivery(error.error()).into()));
            return;
        }
    };
    if !matches!(
        (&runtime.transport, &slot),
        (ResultTransport::Internal(_), ResultSlot::Internal(_))
    ) {
        // The original enqueue will reject this mismatched slot. Close the
        // source before that by-value refusal drops its delivery/receipt.
        revoke_all_establish_authority(attempts);
    }
    let (delivery, receipt, checked) =
        InternalEndDelivery::new(Arc::clone(&pending.source), pending.end);
    if let Err(error) = runtime.transport.enqueue_internal_end(slot, delivery) {
        revoke_all_establish_authority(attempts);
        let _ = state.fail_result_delivery(permit);
        runtime.accepted_error = Some(error);
        let _ = pending
            .reply
            .send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
        return;
    }
    runtime.in_flight = Some(InFlightResult::InternalEnd(EndInFlight {
        source: pending.source,
        end: pending.end,
        permit,
        receipt,
        checked,
        reply: pending.reply,
    }));
}

pub(super) fn settle_end(
    state: &mut LogicalExecutionState,
    attempts: &mut BTreeMap<QueryExecutionId, AttemptLedgers>,
    runtime: &mut ResultRuntime,
    end: EndInFlight,
    disposition: Result<ResultDeliveryDisposition, ()>,
) {
    let current = runtime
        .internal
        .as_ref()
        .and_then(|internal| internal.consumer.as_ref())
        .is_some_and(|record| Arc::ptr_eq(&record.source, &end.source));
    if !current || state.conclusion().is_some() {
        let refusal = state.conclusion().map_or(
            LogicalExecutionActorError::StaleAuthority,
            LogicalExecutionActorError::ExecutionConcluded,
        );
        let error = runtime
            .accepted_error
            .clone()
            .unwrap_or_else(|| result_actor_failure(refusal));
        let _ = end.checked.send(Err(error));
        let _ = end.reply.send(Err(refusal));
        return;
    }
    let failure = result_receipt_failure(disposition);
    if let Some(error) = failure {
        revoke_all_establish_authority(attempts);
        let _ = state.fail_result_delivery(end.permit);
        runtime.accepted_error = Some(error.clone());
        let _ = end.checked.send(Err(error));
        let _ = end
            .reply
            .send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
        return;
    }
    if let Err(error) = state.complete_result_delivery(end.permit) {
        let error = result_actor_failure(ActorStateError::Delivery(error.error()).into());
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "internal End consumption could not be completed",
            error.clone(),
        );
        let _ = end.checked.send(Err(error));
        let _ = end
            .reply
            .send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
        return;
    }
    let record = runtime
        .internal
        .as_mut()
        .and_then(|internal| internal.consumer.as_mut())
        .expect("current internal End has its source");
    record.decoded_end = Some(end.end);
    if end
        .checked
        .send(Ok(DecodedInternalEndReceipt::issued(end.source, end.end)))
        .is_err()
    {
        if runtime.internal_exit_supervised() {
            // The checked receipt's destructor cannot choose a domain cause.
            // Keep the pump's reply until actual consumer exit or control.
            debug_assert!(runtime.deferred_internal.is_none());
            runtime.deferred_internal = Some(DeferredInternalDelivery::EndAckLost(end.reply));
            return;
        }
        revoke_all_establish_authority(attempts);
        conclude_result_failure(
            state,
            runtime,
            "internal End acknowledgement was abandoned",
            logical_owner_failure(
                crate::execution_error::QueryFailureReason::LogicalOwnerStopped,
                "internal End acknowledgement was abandoned before readiness",
            ),
        );
        let _ = end
            .reply
            .send(Err(LogicalExecutionActorError::ResultDeliveryFailed));
    } else {
        let _ = end.reply.send(Ok(()));
    }
}

use super::ActorStateError;
