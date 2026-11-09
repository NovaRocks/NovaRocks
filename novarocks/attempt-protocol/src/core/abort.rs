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

//! Route the logical owner's exact Abort through Native without taking its
//! original submission, late settlement, or remote stand-down authority.

use crate::{
    effect::{EffectBody, OperationIntent, QueuedInputPayload},
    error::TaskExecutionError,
    execution::QueryTaskExecution,
    input::{AckPayload, OperationObservation, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptActionId, AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    AbortParticipation, OperationKind, OperationOutcome, ParticipationRef, ParticipationState,
    TaskOperationId,
};
use std::collections::{BTreeMap, BTreeSet};

use super::schedule;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Pending,
    Admitting,
    Granted(QueuedInputGrantRef),
    Queued,
}

#[derive(Clone, Copy, Debug)]
struct Current {
    action: AttemptActionId,
    generation: SendGeneration,
    phase: Phase,
}

#[derive(Debug)]
struct Row {
    request: AbortParticipation,
    current: Option<Current>,
    unknown: Option<Current>,
    closed: bool,
    failed_closed: bool,
}

#[derive(Debug, Default)]
pub(super) struct Aborts {
    rows: BTreeMap<TaskOperationId, Row>,
    operations: BTreeMap<ParticipationRef, TaskOperationId>,
    granted: BTreeSet<TaskOperationId>,
    pending: BTreeSet<(AttemptActionId, TaskOperationId)>,
}

#[derive(Debug)]
pub(super) struct AbortResult<F> {
    pub(super) settlement: Option<AttemptToLogicalBody<F>>,
    pub(super) cleanup: Option<EffectBody>,
    pub(super) domain: Result<(), TaskExecutionError>,
}

impl Aborts {
    pub(super) fn begin(
        &mut self,
        execution: &QueryTaskExecution,
        action: AttemptActionId,
        request: AbortParticipation,
        generation: SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        let participation = request.participation();
        if execution.owner(participation).is_none() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let operation = request.envelope().operation_id();
        let original = self.operations.entry(participation).or_insert(operation);
        if *original != operation {
            return Err(schedule(
                "actor Abort changed the original participation operation",
            ));
        }
        let row = self.rows.entry(operation).or_insert(Row {
            request,
            current: None,
            unknown: None,
            closed: false,
            failed_closed: false,
        });
        if row.request.participation() != participation
            || row.request.envelope() != request.envelope()
            || row.request.cause() != request.cause()
            || row.current.is_some()
            || row.closed
            || row.failed_closed
        {
            return Err(schedule(
                "actor Abort cannot start another original generation",
            ));
        }
        row.current = Some(Current {
            action,
            generation,
            phase: Phase::Pending,
        });
        self.pending
            .insert((generation.admission_effect(), operation));
        Ok(())
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn admit_next(&mut self) -> Option<crate::effect::AttemptEffect> {
        let (id, operation) = self.pending.pop_first()?;
        let row = self
            .rows
            .get_mut(&operation)
            .expect("original pending Abort");
        let current = row.current.as_mut().expect("pending Abort generation");
        current.phase = Phase::Admitting;
        let intent = OperationIntent::AbortParticipation(row.request);
        let lane = intent.lane();
        Some(crate::effect::AttemptEffect {
            id,
            class: crate::effect::EffectClass::Settlement,
            body: EffectBody::AdmitQueuedInput {
                operation: intent,
                generation: current.generation,
                payload: QueuedInputPayload::AlreadyRetained,
                lane,
            },
        })
    }

    pub(super) fn owns(&self, operation: TaskOperationId) -> bool {
        self.rows.contains_key(&operation)
    }

    pub(super) fn grant(
        &mut self,
        execution: &mut QueryTaskExecution,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<Option<EffectBody>, TaskExecutionError> {
        if execution.actor_abort_tracks_input(operation, grant) {
            return Ok(None);
        }
        let Some(row) = self.rows.get_mut(&operation) else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        let Some(current) = row
            .current
            .as_mut()
            .filter(|current| current.generation == grant.generation())
        else {
            return Ok(Some(EffectBody::ReleaseQueuedInput { grant }));
        };
        match current.phase {
            Phase::Admitting => {
                current.phase = Phase::Granted(grant);
                self.granted.insert(operation);
                self.advance_grant(execution, operation)?;
                Ok(None)
            }
            Phase::Granted(original) if original == grant => Ok(None),
            _ => Ok(Some(EffectBody::ReleaseQueuedInput { grant })),
        }
    }

    pub(super) fn next_granted(&self, after: Option<TaskOperationId>) -> Option<TaskOperationId> {
        use std::ops::Bound::{Excluded, Unbounded};
        match after {
            Some(after) => self
                .granted
                .range((Excluded(after), Unbounded))
                .next()
                .copied(),
            None => self.granted.first().copied(),
        }
    }

    pub(super) fn advance_grant(
        &mut self,
        execution: &mut QueryTaskExecution,
        operation: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        let row = self.rows.get_mut(&operation).expect("original Abort");
        let current = row.current.as_mut().expect("original granted Abort");
        let Phase::Granted(grant) = current.phase else {
            return Err(schedule("Abort admission lost its original grant"));
        };
        if execution.queue_actor_abort(row.request, grant)? {
            self.granted.remove(&operation);
            current.phase = Phase::Queued;
        }
        Ok(())
    }

    pub(super) fn unsent<F>(
        &mut self,
        operation: TaskOperationId,
    ) -> Result<AttemptToLogicalBody<F>, TaskExecutionError> {
        let row = self
            .rows
            .get_mut(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let current = row
            .current
            .take()
            .ok_or(TaskExecutionError::UnknownOperation)?;
        Ok(settlement(
            row.request,
            current,
            AbortIssueSettlement::DefinitelyUnsent,
        ))
    }

    pub(super) fn admission_failed<F>(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
    ) -> Option<AttemptToLogicalBody<F>> {
        let row = self.rows.get_mut(&operation)?;
        let current = row
            .current
            .filter(|c| c.generation == generation && c.phase == Phase::Admitting)?;
        row.current = None;
        row.failed_closed = true;
        Some(settlement(
            row.request,
            current,
            AbortIssueSettlement::FailedClosed,
        ))
    }

    pub(super) fn observe<F>(
        &mut self,
        execution: &mut QueryTaskExecution,
        observation: &OperationObservation,
    ) -> AbortResult<F> {
        let mut result = AbortResult {
            settlement: None,
            cleanup: None,
            domain: Ok(()),
        };
        let reply = &observation.reply;
        let operation = reply.operation_id();
        let Some(row) = self.rows.get_mut(&operation) else {
            result.domain = Err(TaskExecutionError::UnknownOperation);
            return result;
        };
        let participation = row.request.participation();
        if row.closed {
            if reply.dispatch_result() != OperationDispatchResult::TransportUnknown
                && closing_outcome(participation, reply).is_none()
            {
                result.domain = Err(schedule(
                    "Abort receipt conflicts with its original closing fact",
                ));
            }
            return result;
        }
        if row.failed_closed {
            return result;
        }

        let current = row.current.filter(|c| {
            c.generation == observation.generation
                && c.phase == Phase::Queued
                && execution.actor_abort_in_flight(operation, c.generation)
        });
        let earlier = row
            .unknown
            .filter(|c| observation.generation <= c.generation);
        let Some(guard) = current.or(earlier) else {
            return result;
        };
        if reply.dispatch_result() == OperationDispatchResult::TransportUnknown {
            if current.is_some() {
                row.current = None;
                row.unknown = current;
                result.settlement = Some(settlement(
                    row.request,
                    guard,
                    AbortIssueSettlement::TransportUnknown,
                ));
            }
            return result;
        }
        // An older non-Worker refusal supplies no result for a newer send.
        if current.is_none()
            && reply.worker_outcome().is_none()
            && (row.current.is_some()
                || (reply.host_failure().is_none() && reply.local_failure().is_none())
                || reply.dispatch_result() != OperationDispatchResult::NonWorkerRejected)
        {
            return result;
        }

        let held = row.current.take();
        row.unknown = None;
        if current.is_none()
            && let Some(held) = held
        {
            result.cleanup = match held.phase {
                Phase::Pending => {
                    self.pending
                        .remove(&(held.generation.admission_effect(), operation));
                    None
                }
                Phase::Admitting => Some(EffectBody::Withdraw {
                    effect: held.generation.admission_effect(),
                }),
                Phase::Granted(grant) => {
                    self.granted.remove(&operation);
                    Some(EffectBody::ReleaseQueuedInput { grant })
                }
                // The dispatcher releases or withdraws this exact replay.
                Phase::Queued => None,
            };
        }
        if let Some(outcome) = closing_outcome(participation, reply) {
            row.closed = true;
            result.domain = execution.observe_actor_abort_participation_closed(participation);
            let mut settled = settlement(row.request, guard, AbortIssueSettlement::Worker(outcome));
            if let AttemptToLogicalBody::AbortSettlement {
                receipt_generation, ..
            } = &mut settled
            {
                *receipt_generation = Some(observation.generation.admission_effect());
            }
            result.settlement = Some(settled);
        } else {
            row.failed_closed = true;
            result.settlement = Some(settlement(
                row.request,
                guard,
                AbortIssueSettlement::FailedClosed,
            ));
            result.domain = Err(
                if reply.worker_outcome() == Some(OperationOutcome::OwnerFailed) {
                    TaskExecutionError::OperationFailed {
                        task: None,
                        participation,
                        operation_id: operation,
                        kind: OperationKind::AbortParticipation,
                        rejection: reply.worker_header().expect("original OwnerFailed verdict"),
                    }
                } else if reply.worker_outcome().is_none() {
                    crate::error::dispatch_rejection(reply, OperationKind::AbortParticipation)
                } else {
                    schedule("Abort receipt does not prove original participation closure")
                },
            );
        }
        result
    }
}

fn settlement<F>(
    request: AbortParticipation,
    current: Current,
    settlement: AbortIssueSettlement,
) -> AttemptToLogicalBody<F> {
    AttemptToLogicalBody::AbortSettlement {
        abort_action: current.action,
        generation: current.generation.admission_effect(),
        receipt_generation: None,
        participation: request.participation(),
        operation_id: request.envelope().operation_id(),
        settlement,
    }
}

fn closing_outcome(
    participation: ParticipationRef,
    reply: &crate::input::OperationReply,
) -> Option<OperationOutcome> {
    let outcome = reply.worker_outcome()?;
    if !matches!(
        outcome,
        OperationOutcome::Accepted
            | OperationOutcome::Idempotent
            | OperationOutcome::ParticipationTerminalReceipt
            | OperationOutcome::LeaseExpired
            | OperationOutcome::Gone
    ) {
        return None;
    }
    let AckPayload::Participation(receipt) = reply.payload() else {
        return None;
    };
    (receipt.participation() == participation
        && matches!(
            receipt.state(),
            ParticipationState::Aborting
                | ParticipationState::Releasing
                | ParticipationState::TerminalRetained
                | ParticipationState::Gone
        ))
    .then_some(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        capacity::{QueueStage, ReservationToken, StepContext},
        config::DispatchLane,
        dispatch::{BatchDispatch, BatchDispatchPass},
        failure::AttemptFailure,
        input::{LocalDispatchResult, OperationReply, OperationReplyResult},
    };
    use novarocks_execution_contract::{
        AbortCause, ParticipationReceipt, operation::OperationResult,
    };
    use novarocks_types::BackendProcessId;
    use std::num::NonZeroU64;

    fn id(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }
    struct Turn;
    impl StepContext for Turn {
        fn charge(&mut self, _: usize) -> bool {
            true
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            control: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            assert!(control);
            QueueStage::Granted(ReservationToken(id(4)))
        }
    }

    #[test]
    fn definitive_older_abort_withdraws_pending_replay_without_reusing_grant() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let participation = *execution.graph().participations().next().unwrap();
        let operation = execution.issue_operation_id();
        let request = AbortParticipation::new(operation, participation, AbortCause::QueryFailed);
        let action = |n| AttemptActionId::new(id(n));
        let generation = |n| SendGeneration::new(action(n));
        let mut aborts = Aborts::default();
        aborts
            .begin(&execution, action(10), request, generation(1))
            .unwrap();
        let first = aborts.admit_next().unwrap();
        assert_eq!(first.id, action(1));
        let first_grant = QueuedInputGrantRef::new(id(8), generation(1));
        assert!(
            aborts
                .grant(&mut execution, operation, first_grant)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            execution
                .advance_send_batch(
                    &mut BatchDispatchPass::new(),
                    action(11),
                    &mut Turn,
                    |_| Ok(())
                )
                .unwrap(),
            BatchDispatch::Ready(_)
        ));
        let unknown = OperationObservation {
            generation: generation(1),
            reply: OperationReply {
                operation_id: operation,
                kind: OperationKind::AbortParticipation,
                result: OperationReplyResult::Local {
                    result: LocalDispatchResult::TransportUnknown,
                    failure: None,
                    protocol_failure: None,
                    detail: None,
                },
            },
        };
        let applied = aborts.observe::<AttemptFailure>(&mut execution, &unknown);
        assert!(
            matches!(applied.settlement, Some(AttemptToLogicalBody::AbortSettlement {
            abort_action, settlement: AbortIssueSettlement::TransportUnknown, ..
        }) if abort_action == action(10))
        );
        applied.domain.unwrap();
        execution.apply_dispatched_reply(&unknown).1.unwrap();
        aborts
            .begin(&execution, action(20), request, generation(2))
            .unwrap();
        assert_eq!(aborts.admit_next().unwrap().id, action(2));
        let ignored = aborts.observe::<AttemptFailure>(&mut execution, &unknown);
        assert!(ignored.settlement.is_none() && ignored.cleanup.is_none());

        let closing = OperationObservation {
            generation: generation(1),
            reply: OperationReply {
                operation_id: operation,
                kind: OperationKind::AbortParticipation,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(AckPayload::Participation(ParticipationReceipt::new(
                        participation,
                        ParticipationState::Aborting,
                    ))),
                }),
            },
        };
        let resolved = aborts.observe::<AttemptFailure>(&mut execution, &closing);
        resolved.domain.unwrap();
        assert!(
            matches!(resolved.cleanup, Some(EffectBody::Withdraw { effect }) if effect == action(2))
        );
        assert!(
            matches!(resolved.settlement, Some(AttemptToLogicalBody::AbortSettlement {
            abort_action, receipt_generation: Some(original), settlement: AbortIssueSettlement::Worker(OperationOutcome::Accepted), ..
        }) if abort_action == action(10) && original == action(1))
        );
        assert!(execution.owner(participation).unwrap().is_released());
        let late_grant = QueuedInputGrantRef::new(id(9), generation(2));
        assert!(
            matches!(aborts.grant(&mut execution, operation, late_grant).unwrap(),
            Some(EffectBody::ReleaseQueuedInput { grant }) if grant == late_grant)
        );
        assert_eq!(execution.dispatcher().queued_items(), 0);
    }
}
