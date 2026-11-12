// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Apply the original finite Native ACK prefix before the rest of the round.
//! QA sees only the recorded settlement debts, after the round tail or its
//! early error. Raw inputs stay with their original mailbox and Source.

use super::{OperationTarget, QueryTaskExecution};
use crate::{
    authorization::observations::{JournalError, ObserverDebt, ObserverJournal},
    effect::EffectBody,
    error::TaskExecutionError,
    input::{
        AttemptInputId, AttemptSourceCut, AttemptSourceId, ExecutorFact, OperationObservation,
    },
};
use novarocks_execution_contract::OperationKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Idle,
    Prefix(AttemptInputId),
    RoundTail,
    Settlement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AckPhaseError {
    WrongSource,
    WrongPhase,
    WrongFact,
    Journal(JournalError),
}

impl From<JournalError> for AckPhaseError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

#[derive(Debug)]
pub(crate) struct NativeAckPhase {
    source: AttemptSourceId,
    phase: Phase,
    observer: ObserverJournal,
    first_error: Option<TaskExecutionError>,
}

impl NativeAckPhase {
    pub(crate) fn new(source: AttemptSourceId, mailbox: [usize; 4]) -> Result<Self, AckPhaseError> {
        Ok(Self {
            source,
            phase: Phase::Idle,
            observer: ObserverJournal::new(mailbox)?,
            first_error: None,
        })
    }

    /// Called once at the Native drain boundary with the original pre-pop
    /// sample. Further samples while processing this prefix cannot extend it.
    pub(crate) fn begin(&mut self, cut: AttemptSourceCut) -> Result<(), AckPhaseError> {
        if cut.source != self.source {
            return Err(AckPhaseError::WrongSource);
        }
        if self.phase != Phase::Idle {
            return Err(AckPhaseError::WrongPhase);
        }
        self.observer.begin_cut()?;
        if let Some(tail) = cut.tail {
            self.phase = Phase::Prefix(tail);
        } else {
            self.finish_prefix()?;
        }
        Ok(())
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.phase == Phase::Idle
    }

    pub(crate) fn selected_source(&self) -> Option<AttemptSourceId> {
        matches!(self.phase, Phase::Prefix(_)).then_some(self.source)
    }

    /// Apply one fully charged primary input from the selected FIFO. The
    /// parent supplies its original in-Core Abort owner before dispatcher
    /// accounting, since that owner also handles late Abort receipts.
    pub(crate) fn apply(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInputId,
        fact: &ExecutorFact,
        execution: &mut QueryTaskExecution,
        observe_abort: impl FnOnce(
            &mut QueryTaskExecution,
            &OperationObservation,
        ) -> Result<(), TaskExecutionError>,
    ) -> Result<Option<EffectBody>, AckPhaseError> {
        let Phase::Prefix(tail) = self.phase else {
            return Err(AckPhaseError::WrongPhase);
        };
        if source != self.source {
            return Err(AckPhaseError::WrongSource);
        }

        let (action, result) = match fact {
            ExecutorFact::Operation(observation) => {
                if execution.dispatched_reply_needs_observer(observation) {
                    self.observer.observe(&observation.reply)?;
                }
                let abort = if observation.reply.kind() == OperationKind::AbortParticipation {
                    observe_abort(execution, observation)
                } else {
                    Ok(())
                };
                let (action, domain) = execution.apply_dispatched_reply(observation);
                (action, abort.and(domain))
            }
            ExecutorFact::SendStarted { effect, .. } => {
                execution.observe_send_started(*effect);
                (None, Ok(()))
            }
            ExecutorFact::EstablishRpcStarted {
                effect,
                operation,
                participation,
                ..
            } => {
                let result = if execution
                    .dispatcher
                    .establish_rpc_start_is_current(*effect, *operation)
                {
                    execution.observe_send_started(*effect);
                    match execution.operation_targets.get(operation) {
                        Some(OperationTarget::Participation(target)) if target == participation => {
                            execution.establish_send_started(*participation, *operation)
                        }
                        _ => Err(TaskExecutionError::UnknownOperation),
                    }
                } else {
                    // Its logical owner can already have settled a preceding
                    // verdict from another physical send of the same operation.
                    Ok(())
                };
                (None, result)
            }
            _ => return Err(AckPhaseError::WrongFact),
        };
        self.record_error(result);
        if input == tail {
            self.finish_prefix()?;
        }
        Ok(action)
    }

    pub(crate) fn complete_deferred_ack(
        &mut self,
        result: Result<(), TaskExecutionError>,
    ) -> Result<(), AckPhaseError> {
        if !matches!(self.phase, Phase::Prefix(_) | Phase::RoundTail) {
            return Err(AckPhaseError::WrongPhase);
        }
        self.record_error(result);
        Ok(())
    }

    fn record_error(&mut self, result: Result<(), TaskExecutionError>) {
        if let Err(error) = result {
            self.first_error.get_or_insert(error);
        }
    }

    fn finish_prefix(&mut self) -> Result<(), AckPhaseError> {
        self.observer.finish_recording()?;
        self.phase = Phase::RoundTail;
        Ok(())
    }

    pub(crate) fn run_round_tail(&self) -> Result<bool, AckPhaseError> {
        if self.phase != Phase::RoundTail {
            return Err(AckPhaseError::WrongPhase);
        }
        Ok(self.first_error.is_none())
    }

    pub(crate) fn fail_round_tail(
        &mut self,
        error: TaskExecutionError,
    ) -> Result<(), AckPhaseError> {
        if self.phase != Phase::RoundTail {
            return Err(AckPhaseError::WrongPhase);
        }
        self.record_error(Err(error));
        Ok(())
    }

    /// The parent calls this after the actual round tail, including its
    /// publication barrier, or immediately when a prefix error skips the tail.
    pub(crate) fn begin_settlement(
        &mut self,
        round_result: Result<(), TaskExecutionError>,
    ) -> Result<(), AckPhaseError> {
        if self.phase != Phase::RoundTail {
            return Err(AckPhaseError::WrongPhase);
        }
        self.record_error(round_result);
        self.phase = Phase::Settlement;
        Ok(())
    }

    pub(crate) fn current(&self) -> Result<Option<ObserverDebt>, AckPhaseError> {
        if self.phase != Phase::Settlement {
            return Err(AckPhaseError::WrongPhase);
        }
        Ok(self.observer.current()?)
    }

    /// Advance only when the selected debt has finished its original
    /// settlement, including an actual QA reply if one was needed.
    pub(crate) fn settled_current(
        &mut self,
        result: Result<(), TaskExecutionError>,
    ) -> Result<(), AckPhaseError> {
        if self.phase != Phase::Settlement {
            return Err(AckPhaseError::WrongPhase);
        }
        self.observer.settled_current()?;
        self.record_error(result);
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<Result<(), TaskExecutionError>, AckPhaseError> {
        if self.phase != Phase::Settlement {
            return Err(AckPhaseError::WrongPhase);
        }
        self.observer.finish_cut()?;
        self.phase = Phase::Idle;
        Ok(self.first_error.take().map_or(Ok(()), Err))
    }
}

#[cfg(test)]
pub(crate) use tests::{admission_intent, dispatch_admission, emit_quiesce, queue_establish};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        authorization::observations::ObservedDispatch,
        effect::OperationIntent,
        input::{
            AckPayload, OperationReply, OperationReplyResult, QueuedInputGrantRef, SendGeneration,
        },
        participation_owner::ParticipationEstablishFacts,
        start::PayloadRef,
    };
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{
        AdmissionTicketId, OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationRef,
        operation::OperationResult,
    };
    use std::num::NonZeroU64;

    fn id(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    pub(crate) fn admission_intent(
        execution: &mut QueryTaskExecution,
        participation: ParticipationRef,
    ) -> novarocks_execution_contract::AcquireParticipationAdmissionTicket {
        let OperationIntent::AcquireParticipationAdmissionTicket(request) = execution
            .owners
            .get_mut(&participation)
            .unwrap()
            .admission_intent(execution.now, &mut execution.ids)
            .unwrap()
            .unwrap()
        else {
            panic!("expected original admission intent");
        };
        request
    }

    fn emit_admission(
        execution: &mut QueryTaskExecution,
        participation: ParticipationRef,
        sequence: u64,
        outcome: OperationOutcome,
    ) -> OperationObservation {
        let request = admission_intent(execution, participation);
        dispatch_admission(execution, request, sequence, outcome)
    }

    pub(crate) fn dispatch_admission(
        execution: &mut QueryTaskExecution,
        request: novarocks_execution_contract::AcquireParticipationAdmissionTicket,
        sequence: u64,
        outcome: OperationOutcome,
    ) -> OperationObservation {
        let participation = request.participation();
        let operation = request.envelope().operation_id();
        let generation = SendGeneration::new(AttemptActionId::new(id(sequence)));
        execution
            .dispatcher
            .enqueue_reserved(
                OperationIntent::AcquireParticipationAdmissionTicket(request),
                execution.now,
                QueuedInputGrantRef::new(id(sequence), generation),
            )
            .unwrap();
        execution
            .operation_targets
            .insert(operation, OperationTarget::Participation(participation));
        let batch = execution.dispatcher.take_batch().unwrap();
        execution
            .dispatcher
            .accept_emitted(&batch, AttemptActionId::new(id(sequence + 100)))
            .unwrap();
        let acknowledgement = (outcome == OperationOutcome::Accepted).then(|| {
            AckPayload::AdmissionTicket(ParticipationAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([sequence as u8; 16]).unwrap(),
                participation,
                request.valid_for(),
            ))
        });
        OperationObservation {
            generation,
            reply: OperationReply {
                operation_id: operation,
                kind: OperationKind::AcquireParticipationAdmissionTicket,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome,
                    detail: None,
                    acknowledgement,
                }),
            },
        }
    }

    pub(crate) fn queue_establish(
        execution: &mut QueryTaskExecution,
        participation: ParticipationRef,
        grant: QueuedInputGrantRef,
    ) -> OperationIntent {
        let intent = execution
            .owners
            .get_mut(&participation)
            .unwrap()
            .establish_intent(
                ParticipationEstablishFacts {
                    material: PayloadRef(id(90)),
                    queued_bytes: 256,
                },
                execution.now,
                &mut execution.ids,
            )
            .unwrap()
            .unwrap();
        execution
            .dispatcher
            .enqueue_reserved(intent.clone(), execution.now, grant)
            .unwrap();
        execution.operation_targets.insert(
            intent.operation_id(),
            OperationTarget::Participation(participation),
        );
        intent
    }

    pub(crate) fn emit_quiesce(
        execution: &mut QueryTaskExecution,
        receipt: std::sync::Arc<novarocks_execution_contract::QuiesceParticipationReceipt>,
    ) -> OperationObservation {
        let participation = receipt.participation();
        let intent = execution
            .owners
            .get_mut(&participation)
            .unwrap()
            .quiesce_intent(&mut execution.ids)
            .unwrap();
        let operation = intent.operation_id();
        let generation = SendGeneration::new(AttemptActionId::new(id(200)));
        let grant = QueuedInputGrantRef::new(id(201), generation);
        execution
            .dispatcher
            .enqueue_reserved(intent, execution.now, grant)
            .unwrap();
        execution
            .operation_targets
            .insert(operation, OperationTarget::Participation(participation));
        let batch = execution.dispatcher.take_batch().unwrap();
        execution
            .dispatcher
            .accept_emitted(&batch, AttemptActionId::new(id(202)))
            .unwrap();
        OperationObservation {
            generation,
            reply: OperationReply {
                operation_id: operation,
                kind: OperationKind::QuiesceParticipation,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(AckPayload::Quiesce(receipt)),
                }),
            },
        }
    }

    fn unexpected_abort(
        _: &mut QueryTaskExecution,
        _: &OperationObservation,
    ) -> Result<(), TaskExecutionError> {
        panic!("admission receipts must not reach the Abort owner");
    }

    #[test]
    fn frozen_tail_preserves_suffix_verdict_and_first_error_before_qa_settlement() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let participants: Vec<_> = execution.graph().participations().copied().collect();
        let failed = emit_admission(
            &mut execution,
            participants[0],
            1,
            OperationOutcome::OwnerFailed,
        );
        let accepted = emit_admission(
            &mut execution,
            participants[1],
            2,
            OperationOutcome::Accepted,
        );
        let next = emit_admission(
            &mut execution,
            participants[2],
            3,
            OperationOutcome::Accepted,
        );
        let source = AttemptSourceId(id(7));
        // Input identity is opaque. Only exact equality completes the cut.
        let first = AttemptInputId(id(49));
        let tail = AttemptInputId(id(12));
        let later = AttemptInputId(id(100));
        let mut phase = NativeAckPhase::new(source, [1, 1, 2, 1]).unwrap();

        phase
            .begin(AttemptSourceCut {
                source,
                tail: Some(tail),
            })
            .unwrap();
        assert_eq!(phase.selected_source(), Some(source));
        assert!(
            phase
                .apply(
                    source,
                    first,
                    &ExecutorFact::Operation(failed.clone()),
                    &mut execution,
                    unexpected_abort
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(phase.run_round_tail(), Err(AckPhaseError::WrongPhase));
        assert_eq!(
            phase.begin(AttemptSourceCut {
                source,
                tail: Some(later)
            }),
            Err(AckPhaseError::WrongPhase)
        );
        assert_eq!(
            phase.begin_settlement(Ok(())),
            Err(AckPhaseError::WrongPhase)
        );

        assert!(
            phase
                .apply(
                    source,
                    tail,
                    &ExecutorFact::Operation(accepted.clone()),
                    &mut execution,
                    unexpected_abort
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(phase.selected_source(), None);
        assert_eq!(phase.run_round_tail(), Ok(false));
        assert!(
            phase
                .apply(
                    source,
                    later,
                    &ExecutorFact::Operation(next.clone()),
                    &mut execution,
                    unexpected_abort
                )
                .is_err()
        );
        assert!(execution.dispatched_reply_needs_observer(&next));
        assert_eq!(phase.current(), Err(AckPhaseError::WrongPhase));

        // The valid suffix reached the real domain owner despite the first
        // failure. This owner can now derive Establish from its actual ticket.
        assert!(
            execution
                .owners
                .get_mut(&participants[1])
                .unwrap()
                .establish_intent(
                    ParticipationEstablishFacts {
                        material: PayloadRef(id(90)),
                        queued_bytes: 256
                    },
                    execution.now,
                    &mut execution.ids,
                )
                .unwrap()
                .is_some()
        );

        phase.begin_settlement(Ok(())).unwrap();
        let Some(ObserverDebt::Admission(first_debt)) = phase.current().unwrap() else {
            panic!("expected original failed verdict");
        };
        assert_eq!(first_debt.operation_id, failed.reply.operation_id());
        assert_eq!(
            first_debt.result,
            ObservedDispatch::Worker(OperationOutcome::OwnerFailed)
        );
        assert_eq!(
            phase.current(),
            Ok(Some(ObserverDebt::Admission(first_debt)))
        );
        assert!(phase.finish().is_err());
        phase
            .settled_current(Err(TaskExecutionError::UnknownOperation))
            .unwrap();
        let Some(ObserverDebt::Admission(second_debt)) = phase.current().unwrap() else {
            panic!("expected the successful suffix debt");
        };
        assert_eq!(second_debt.operation_id, accepted.reply.operation_id());
        assert!(second_debt.ticket.is_some());
        phase.settled_current(Ok(())).unwrap();
        assert!(
            matches!(phase.finish().unwrap(), Err(TaskExecutionError::OperationFailed {
            operation_id, ..
        }) if operation_id == failed.reply.operation_id())
        );

        phase
            .begin(AttemptSourceCut {
                source,
                tail: Some(later),
            })
            .unwrap();
        phase
            .apply(
                source,
                later,
                &ExecutorFact::Operation(next.clone()),
                &mut execution,
                unexpected_abort,
            )
            .unwrap();
        assert_eq!(phase.run_round_tail(), Ok(true));
        assert_eq!(phase.current(), Err(AckPhaseError::WrongPhase));
        phase.begin_settlement(Ok(())).unwrap();
        assert!(
            matches!(phase.current().unwrap(), Some(ObserverDebt::Admission(debt)) if debt.operation_id == next.reply.operation_id())
        );
        phase.settled_current(Ok(())).unwrap();
        assert_eq!(phase.finish(), Ok(Ok(())));
    }

    #[test]
    fn establish_prefix_keeps_the_valid_peer_receipt_after_the_first_error() {
        use crate::dispatch::DispatchOperationState;
        use novarocks_execution_contract::{
            LeaseReceipt, ParticipationReceipt, ParticipationState,
        };
        use std::time::Duration;

        for invalid_start in [false, true] {
            let mut execution = crate::test_support::execution_with_strict_input();
            let participants: Vec<_> = execution.graph().participations().copied().collect();
            let [first, peer] = [participants[0], participants[1]];
            let mut emit = |participation, sequence| {
                let receipt = emit_admission(
                    &mut execution,
                    participation,
                    sequence,
                    OperationOutcome::Accepted,
                );
                execution.apply_dispatched_reply(&receipt).1.unwrap();
                let effect = AttemptActionId::new(id(sequence + 20));
                let generation = SendGeneration::new(AttemptActionId::new(id(sequence + 10)));
                let intent = queue_establish(
                    &mut execution,
                    participation,
                    QueuedInputGrantRef::new(id(sequence + 10), generation),
                );
                let batch = execution.dispatcher.take_batch().unwrap();
                execution.dispatcher.accept_emitted(&batch, effect).unwrap();
                (intent, effect, generation)
            };
            let (first_intent, first_send, first_generation) = emit(first, 1);
            let (peer_intent, peer_send, peer_generation) = emit(peer, 2);
            let first_operation = first_intent.operation_id();
            let peer_operation = peer_intent.operation_id();
            let OperationIntent::EstablishParticipation {
                initial_lease_sequence,
                initial_lease_valid_for,
                ..
            } = peer_intent
            else {
                panic!("expected the peer's frozen Establish");
            };
            let source = AttemptSourceId(id(7));
            let tail = AttemptInputId(id(4));
            let now = execution.now;
            let mut phase = NativeAckPhase::new(source, [1, 1, 3, 1]).unwrap();
            phase
                .begin(AttemptSourceCut {
                    source,
                    tail: Some(tail),
                })
                .unwrap();
            let started = |effect, operation, participation| ExecutorFact::EstablishRpcStarted {
                effect,
                operation,
                participation,
                started_at: now,
            };
            let verdict = |operation_id, generation, outcome, acknowledgement| {
                ExecutorFact::Operation(OperationObservation {
                    generation,
                    reply: OperationReply {
                        operation_id,
                        kind: OperationKind::UpdateParticipation,
                        result: OperationReplyResult::Worker(OperationResult::Verdict {
                            outcome,
                            detail: None,
                            acknowledgement,
                        }),
                    },
                })
            };
            let mut apply = |input, fact| {
                phase
                    .apply(source, input, &fact, &mut execution, unexpected_abort)
                    .unwrap();
            };
            // The malformed start names the wrong participant while the send
            // and operation still identify the current original row.
            let first_reported = if invalid_start { peer } else { first };
            apply(
                AttemptInputId(id(1)),
                started(first_send, first_operation, first_reported),
            );
            if !invalid_start {
                apply(
                    AttemptInputId(id(2)),
                    verdict(
                        first_operation,
                        first_generation,
                        OperationOutcome::InvalidStateOrRequest,
                        None,
                    ),
                );
            }
            apply(
                AttemptInputId(id(3)),
                started(peer_send, peer_operation, peer),
            );
            let active = ParticipationReceipt::new(peer, ParticipationState::Active).with_lease(
                LeaseReceipt::new(
                    initial_lease_sequence,
                    initial_lease_valid_for,
                    Duration::from_secs(9),
                ),
            );
            apply(
                tail,
                verdict(
                    peer_operation,
                    peer_generation,
                    OperationOutcome::Accepted,
                    Some(AckPayload::Participation(active)),
                ),
            );

            assert_eq!(execution.owners[&peer].state(), ParticipationState::Active);
            assert!(execution.needs_covered_subscription(peer));
            assert_eq!(
                execution
                    .dispatcher
                    .operation_state(peer_operation)
                    .unwrap(),
                DispatchOperationState::Absent
            );
            assert_eq!(phase.run_round_tail(), Ok(false));
            phase.begin_settlement(Ok(())).unwrap();
            if !invalid_start {
                assert!(execution.owners[&first].is_released());
                let Some(ObserverDebt::Establish(debt)) = phase.current().unwrap() else {
                    panic!("expected the original rejected Establish debt");
                };
                assert_eq!(debt.operation_id, first_operation);
                assert_eq!(
                    debt.result,
                    ObservedDispatch::Worker(OperationOutcome::InvalidStateOrRequest)
                );
                phase.settled_current(Ok(())).unwrap();
            }
            let Some(ObserverDebt::Establish(debt)) = phase.current().unwrap() else {
                panic!("expected the peer's accepted Establish debt");
            };
            assert_eq!(debt.operation_id, peer_operation);
            assert_eq!(
                debt.result,
                ObservedDispatch::Worker(OperationOutcome::Accepted)
            );
            phase.settled_current(Ok(())).unwrap();
            assert_eq!(phase.current(), Ok(None));
            let error = phase.finish().unwrap().unwrap_err();
            if invalid_start {
                assert_eq!(error, TaskExecutionError::UnknownOperation);
            } else {
                assert!(matches!(
                    error,
                    TaskExecutionError::PreReadyEstablishRejected { participation, operation_id, .. }
                        if participation == first && operation_id == first_operation
                ));
            }
        }
    }

    #[test]
    fn only_current_establish_rpc_first_poll_can_open_create_pipeline() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let participation = *execution.graph().participations().next().unwrap();
        let admission =
            emit_admission(&mut execution, participation, 1, OperationOutcome::Accepted);
        let (release, result) = execution.apply_dispatched_reply(&admission);
        assert!(release.is_none());
        result.unwrap();
        let now = execution.now;
        let intent = execution
            .owners
            .get_mut(&participation)
            .unwrap()
            .establish_intent(
                ParticipationEstablishFacts {
                    material: PayloadRef(id(90)),
                    queued_bytes: 256,
                },
                now,
                &mut execution.ids,
            )
            .unwrap()
            .unwrap();
        let operation = intent.operation_id();
        let effect = AttemptActionId::new(id(44));
        let generation = SendGeneration::new(AttemptActionId::new(id(2)));
        execution
            .dispatcher
            .enqueue_reserved(intent, now, QueuedInputGrantRef::new(id(2), generation))
            .unwrap();
        execution
            .operation_targets
            .insert(operation, OperationTarget::Participation(participation));
        let batch = execution.dispatcher.take_batch().unwrap();
        execution.dispatcher.accept_emitted(&batch, effect).unwrap();

        let source = AttemptSourceId(id(7));
        let tail = AttemptInputId(id(3));
        let mut phase = NativeAckPhase::new(source, [1, 1, 1, 1]).unwrap();
        phase
            .begin(AttemptSourceCut {
                source,
                tail: Some(tail),
            })
            .unwrap();
        let started = ExecutorFact::SendStarted {
            effect,
            started_at: now,
        };
        phase
            .apply(
                source,
                AttemptInputId(id(1)),
                &started,
                &mut execution,
                unexpected_abort,
            )
            .unwrap();
        assert!(!execution.owners[&participation].may_pipeline_first_create());

        let rpc_started = |effect| ExecutorFact::EstablishRpcStarted {
            effect,
            operation,
            participation,
            started_at: now,
        };
        phase
            .apply(
                source,
                AttemptInputId(id(2)),
                &rpc_started(AttemptActionId::new(id(45))),
                &mut execution,
                unexpected_abort,
            )
            .unwrap();
        assert!(!execution.owners[&participation].may_pipeline_first_create());
        phase
            .apply(
                source,
                tail,
                &rpc_started(effect),
                &mut execution,
                unexpected_abort,
            )
            .unwrap();
        assert!(execution.owners[&participation].may_pipeline_first_create());
        assert_eq!(phase.run_round_tail(), Ok(true));
    }

    #[test]
    fn empty_cut_requires_original_source_and_round_tail_before_settlement() {
        let source = AttemptSourceId(id(7));
        let mut phase = NativeAckPhase::new(source, [1, 1, 1, 1]).unwrap();
        assert_eq!(
            phase.begin(AttemptSourceCut {
                source: AttemptSourceId(id(8)),
                tail: None
            }),
            Err(AckPhaseError::WrongSource)
        );
        phase
            .begin(AttemptSourceCut { source, tail: None })
            .unwrap();
        assert_eq!(phase.selected_source(), None);
        assert_eq!(phase.run_round_tail(), Ok(true));
        assert_eq!(phase.current(), Err(AckPhaseError::WrongPhase));
        phase.begin_settlement(Ok(())).unwrap();
        assert_eq!(phase.current(), Ok(None));
        assert_eq!(phase.finish(), Ok(Ok(())));
    }
}
