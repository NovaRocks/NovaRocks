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

//! Route the original Abort through Native. Only explicit logical-owner
//! failure gives this attempt authority to issue stand-down operations.

use crate::{
    capacity::StepContext,
    effect::{EffectBody, OperationIntent, QueuedInputPayload},
    error::TaskExecutionError,
    execution::QueryTaskExecution,
    input::{AckPayload, OperationObservation, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::{
    ABORT_RETRY_BACKOFF, AbortIssueSettlement, AttemptActionId, AttemptToLogicalBody,
    MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, OperationKind, OperationOutcome, ParticipationRef,
    ParticipationState, TaskOperationId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

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
    action: Option<AttemptActionId>,
    generation: SendGeneration,
    phase: Phase,
}

#[derive(Debug)]
struct Row {
    request: AbortParticipation,
    current: Option<Current>,
    unknown: Option<Current>,
    actor_unknown: Option<Current>,
    closed: bool,
    failed_closed: bool,
    generations: usize,
    retry_at: Option<MonotonicInstant>,
}

#[derive(Debug)]
enum StandDown {
    Inactive,
    Scanning(Option<ParticipationRef>),
    Scanned,
}

#[derive(Debug)]
pub(super) struct Aborts {
    stand_down: StandDown,
    limit: NonZeroUsize,
    retries: BTreeSet<(MonotonicInstant, TaskOperationId)>,
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
    pub(super) fn new(limit: NonZeroUsize) -> Self {
        Self {
            stand_down: StandDown::Inactive,
            limit,
            retries: BTreeSet::new(),
            rows: BTreeMap::new(),
            operations: BTreeMap::new(),
            granted: BTreeSet::new(),
            pending: BTreeSet::new(),
        }
    }

    pub(super) fn start_stand_down(&mut self) {
        if matches!(self.stand_down, StandDown::Inactive) {
            self.stand_down = StandDown::Scanning(None);
        }
    }

    pub(super) fn taking_over(&self) -> bool {
        !matches!(self.stand_down, StandDown::Inactive)
    }

    pub(super) fn stand_down_deadline(&self, now: MonotonicInstant) -> Option<MonotonicInstant> {
        match self.stand_down {
            StandDown::Inactive => None,
            StandDown::Scanning(_) => Some(now),
            StandDown::Scanned => self.retries.first().map(|&(at, _)| at),
        }
    }

    /// Select at most one admission, preserving the charged frozen-row scan
    /// and independent timed retries across yields.
    pub(super) fn advance_stand_down(
        &mut self,
        execution: &mut QueryTaskExecution,
        generation: SendGeneration,
        context: &mut dyn StepContext,
    ) -> Result<bool, TaskExecutionError> {
        while let StandDown::Scanning(after) = self.stand_down {
            let Some(participation) = execution.participation_after(after) else {
                self.stand_down = StandDown::Scanned;
                break;
            };
            if !context.charge(1) {
                return Ok(false);
            }
            self.stand_down = StandDown::Scanning(Some(participation));
            if !Self::still_requires_abort(execution, participation) {
                continue;
            }
            if let Some(operation) = self.operations.get(&participation) {
                let row = &self.rows[operation];
                if row.retry_at.is_none() && self.can_issue(row) {
                    self.begin_generation(row.request, None, generation)?;
                    return Ok(true);
                }
                continue;
            }
            let operation = execution.issue_operation_id();
            let request =
                AbortParticipation::new(operation, participation, AbortCause::QueryFailed);
            self.begin_generation(request, None, generation)?;
            return Ok(true);
        }
        let now = execution.monotonic_now();
        while let Some(&(at, operation)) = self.retries.first() {
            if !self.taking_over() || !now.has_reached(at) {
                break;
            }
            if !context.charge(1) {
                return Ok(false);
            }
            self.retries.pop_first();
            let row = self.rows.get_mut(&operation).expect("original retry row");
            row.retry_at = None;
            let request = row.request;
            if self.can_issue(&self.rows[&operation])
                && Self::still_requires_abort(execution, request.participation())
            {
                self.begin_generation(request, None, generation)?;
                break;
            }
        }
        Ok(true)
    }

    fn still_requires_abort(
        execution: &QueryTaskExecution,
        participation: ParticipationRef,
    ) -> bool {
        execution
            .owner(participation)
            .is_some_and(|owner| !owner.is_released())
            && !execution.participation_never_established(participation)
            && execution
                .participation_convergence_receipt(participation)
                .is_none()
    }

    fn can_issue(&self, row: &Row) -> bool {
        row.current.is_none()
            && !row.closed
            && !row.failed_closed
            && row.generations < self.limit.get()
    }

    pub(super) fn retired_request(&self, request: AbortParticipation) -> bool {
        self.rows
            .get(&request.envelope().operation_id())
            .is_some_and(|row| {
                (row.closed || row.failed_closed)
                    && row.request.participation() == request.participation()
                    && row.request.envelope() == request.envelope()
                    && row.request.cause() == request.cause()
            })
    }

    pub(super) fn begin(
        &mut self,
        execution: &QueryTaskExecution,
        action: AttemptActionId,
        request: AbortParticipation,
        generation: SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        if execution.owner(request.participation()).is_none() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.begin_generation(request, Some(action), generation)
    }

    fn begin_generation(
        &mut self,
        request: AbortParticipation,
        action: Option<AttemptActionId>,
        generation: SendGeneration,
    ) -> Result<(), TaskExecutionError> {
        let participation = request.participation();
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
            actor_unknown: None,
            closed: false,
            failed_closed: false,
            generations: 0,
            retry_at: None,
        });
        if row.request.participation() != participation
            || row.request.envelope() != request.envelope()
            || row.request.cause() != request.cause()
            || row.current.is_some()
            || row.closed
            || row.failed_closed
        {
            return Err(schedule("Abort cannot start another original generation"));
        }
        if let Some(at) = row.retry_at.take() {
            self.retries.remove(&(at, operation));
        }
        row.generations += 1;
        row.current = Some(Current {
            action,
            generation,
            phase: Phase::Pending,
        });
        self.pending
            .insert((generation.admission_effect(), operation));
        Ok(())
    }

    fn retry(&mut self, operation: TaskOperationId, at: MonotonicInstant) {
        let row = self.rows.get_mut(&operation).expect("original Abort retry");
        if !row.closed && !row.failed_closed && row.generations < self.limit.get() {
            debug_assert!(row.retry_at.is_none());
            row.retry_at = Some(at);
            self.retries.insert((at, operation));
        }
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
        now: MonotonicInstant,
    ) -> Result<Option<AttemptToLogicalBody<F>>, TaskExecutionError> {
        let row = self
            .rows
            .get_mut(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let current = row
            .current
            .take()
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let settled = settlement(row.request, current, AbortIssueSettlement::DefinitelyUnsent);
        self.retry(operation, now);
        Ok(settled)
    }

    pub(super) fn admission_failed<F>(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
    ) -> Option<Option<AttemptToLogicalBody<F>>> {
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
        let closing = closing_outcome(participation, reply);
        if row.failed_closed && closing.is_none() {
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
                if guard.action.is_some() {
                    row.actor_unknown = current;
                }
                result.settlement =
                    settlement(row.request, guard, AbortIssueSettlement::TransportUnknown);
                self.retry(
                    operation,
                    execution
                        .monotonic_now()
                        .saturating_add(ABORT_RETRY_BACKOFF),
                );
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
        if let Some(at) = row.retry_at.take() {
            self.retries.remove(&(at, operation));
        }
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
        if let Some(outcome) = closing {
            row.closed = true;
            result.domain = execution.observe_actor_abort_participation_closed(participation);
            let settlement_guard = row
                .actor_unknown
                .filter(|actor| {
                    guard.action.is_none() && observation.generation <= actor.generation
                })
                .unwrap_or(guard);
            row.unknown = None;
            row.actor_unknown = None;
            let mut settled = settlement(
                row.request,
                settlement_guard,
                AbortIssueSettlement::Worker(outcome),
            );
            if let Some(AttemptToLogicalBody::AbortSettlement {
                receipt_generation, ..
            }) = &mut settled
            {
                *receipt_generation = Some(observation.generation.admission_effect());
            }
            result.settlement = settled;
        } else {
            row.failed_closed = true;
            result.settlement = settlement(row.request, guard, AbortIssueSettlement::FailedClosed);
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
) -> Option<AttemptToLogicalBody<F>> {
    Some(AttemptToLogicalBody::AbortSettlement {
        abort_action: current.action?,
        generation: current.generation.admission_effect(),
        receipt_generation: None,
        participation: request.participation(),
        operation_id: request.envelope().operation_id(),
        settlement,
    })
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
    fn a_ticket_received_after_production_stops_still_requires_abort() {
        use crate::execution::ack_phase::{admission_intent, dispatch_admission};
        let mut execution = crate::test_support::execution_with_strict_input();
        let participation = *execution.graph().participations().next().unwrap();
        let admission = admission_intent(&mut execution, participation);
        let late = dispatch_admission(&mut execution, admission, 1, OperationOutcome::Accepted);
        execution.begin_terminal_cleanup();
        while execution.has_pending_terminal_cleanup() {
            execution.advance_terminal_cleanup(&mut Turn).unwrap();
        }
        execution.apply_dispatched_reply(&late).1.unwrap();
        assert!(!execution.participation_never_established(participation));

        let mut aborts = Aborts::new(NonZeroUsize::MIN);
        aborts.start_stand_down();
        let generation = SendGeneration::new(AttemptActionId::new(id(2)));
        assert!(
            aborts
                .advance_stand_down(&mut execution, generation, &mut Turn)
                .unwrap()
        );
        assert!(matches!(
            aborts.admit_next().unwrap().body,
            EffectBody::AdmitQueuedInput {
                operation: OperationIntent::AbortParticipation(request),
                ..
            } if request.participation() == participation
        ));
        assert!(
            aborts
                .advance_stand_down(&mut execution, generation, &mut Turn)
                .unwrap()
        );
        assert!(!aborts.has_pending());
    }

    #[test]
    fn logical_owner_loss_retries_the_original_abort_under_a_new_grant() {
        use crate::execution::ack_phase::{admission_intent, dispatch_admission};
        let mut execution = crate::test_support::execution_with_strict_input();
        let participation = *execution.graph().participations().next().unwrap();
        let admission = admission_intent(&mut execution, participation);
        let accepted = dispatch_admission(&mut execution, admission, 1, OperationOutcome::Accepted);
        execution.apply_dispatched_reply(&accepted).1.unwrap();

        let operation = execution.issue_operation_id();
        let request = AbortParticipation::new(operation, participation, AbortCause::QueryFailed);
        let action = |n| AttemptActionId::new(id(n));
        let generation = |n| SendGeneration::new(action(n));
        let mut aborts = Aborts::new(NonZeroUsize::new(2).unwrap());
        aborts
            .begin(&execution, action(10), request, generation(2))
            .unwrap();
        aborts.admit_next().unwrap();
        let grant = QueuedInputGrantRef::new(id(20), generation(2), 64);
        aborts.grant(&mut execution, operation, grant).unwrap();
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
            generation: generation(2),
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
        let prior = aborts.observe::<AttemptFailure>(&mut execution, &unknown);
        assert!(prior.settlement.is_some());
        execution.apply_dispatched_reply(&unknown).1.unwrap();
        aborts.start_stand_down();
        assert!(
            aborts
                .advance_stand_down(&mut execution, generation(3), &mut Turn)
                .unwrap()
        );
        assert!(!aborts.has_pending());
        let now = execution.monotonic_now();
        let retry = now.saturating_add(ABORT_RETRY_BACKOFF);
        assert_eq!(aborts.stand_down_deadline(now), Some(retry));
        execution.set_now(retry).unwrap();
        assert!(
            aborts
                .advance_stand_down(&mut execution, generation(4), &mut Turn)
                .unwrap()
        );
        assert!(matches!(
            aborts.admit_next().unwrap().body,
            EffectBody::AdmitQueuedInput {
                operation: OperationIntent::AbortParticipation(replayed),
                generation: physical,
                payload: QueuedInputPayload::AlreadyRetained,
                ..
            } if replayed.envelope() == request.envelope() && physical == generation(4)
        ));
        assert!(
            aborts
                .observe::<AttemptFailure>(&mut execution, &unknown)
                .settlement
                .is_none()
        );
        let closing = OperationObservation {
            generation: generation(2),
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
            matches!(resolved.cleanup, Some(EffectBody::Withdraw { effect }) if effect == action(4))
        );
        assert!(
            matches!(resolved.settlement, Some(AttemptToLogicalBody::AbortSettlement {
            abort_action, receipt_generation: Some(original), ..
        }) if abort_action == action(10) && original == action(2))
        );
        assert!(
            execution
                .participation_convergence_receipt(participation)
                .is_none()
        );
        assert_eq!(aborts.stand_down_deadline(retry), None);
    }

    #[test]
    fn definitive_older_abort_withdraws_pending_replay_without_reusing_grant() {
        for replay_admitted in [false, true] {
            definitive_older_abort_at_replay_cut(replay_admitted);
        }
    }

    fn definitive_older_abort_at_replay_cut(replay_admitted: bool) {
        let mut execution = crate::test_support::execution_with_strict_input();
        let participation = *execution.graph().participations().next().unwrap();
        let operation = execution.issue_operation_id();
        let request = AbortParticipation::new(operation, participation, AbortCause::QueryFailed);
        let action = |n| AttemptActionId::new(id(n));
        let generation = |n| SendGeneration::new(action(n));
        let mut aborts = Aborts::new(NonZeroUsize::new(3).unwrap());
        aborts
            .begin(&execution, action(10), request, generation(1))
            .unwrap();
        let first = aborts.admit_next().unwrap();
        assert_eq!(first.id, action(1));
        let first_grant = QueuedInputGrantRef::new(id(8), generation(1), 64);
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
        if replay_admitted {
            assert_eq!(aborts.admit_next().unwrap().id, action(2));
        }
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
        if replay_admitted {
            assert!(
                matches!(resolved.cleanup, Some(EffectBody::Withdraw { effect }) if effect == action(2))
            );
        } else {
            assert!(resolved.cleanup.is_none());
        }
        assert!(!aborts.has_pending());
        assert!(aborts.admit_next().is_none());
        assert!(
            matches!(resolved.settlement, Some(AttemptToLogicalBody::AbortSettlement {
            abort_action, receipt_generation: Some(original), settlement: AbortIssueSettlement::Worker(OperationOutcome::Accepted), ..
        }) if abort_action == action(10) && original == action(1))
        );
        assert!(execution.owner(participation).unwrap().is_released());
        assert!(aborts.retired_request(request));
        assert!(!aborts.retired_request(AbortParticipation::new(
            execution.issue_operation_id(),
            participation,
            AbortCause::QueryFailed,
        )));
        let late_grant = QueuedInputGrantRef::new(id(9), generation(2), 64);
        assert!(
            matches!(aborts.grant(&mut execution, operation, late_grant).unwrap(),
            Some(EffectBody::ReleaseQueuedInput { grant }) if grant == late_grant)
        );
        assert_eq!(execution.dispatcher().queued_items(), 0);
    }
}
