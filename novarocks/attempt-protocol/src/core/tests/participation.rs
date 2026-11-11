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
    AttemptCore,
    capacity::ReservationToken,
    effect::{EffectClass, Publication},
    execution::{
        CreateGrantProgress, ReleaseCheck, ReleaseProgress,
        ack_phase::{admission_intent, dispatch_admission, emit_quiesce},
    },
    input::{
        AckPayload, AttemptInput, AttemptInputClaim, LocalDispatchResult, OperationReply,
        OperationReplyResult, QueuedInputGrantRef, SendGeneration,
    },
};
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptStopReason, AttemptTerminal, AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    AbortParticipation, LeaseReceipt, OperationOutcome, ParticipationReceipt, ParticipationState,
    QuiesceParticipationReceipt, ReleaseOutcome, operation::OperationResult,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum BeforeStop {
    Pristine,
    Ticket,
    EstablishUnknown,
    Active,
    Quiescing,
    ReleaseInFlight,
    Released,
}

impl BeforeStop {
    fn state(self) -> ParticipationState {
        match self {
            Self::Pristine | Self::Ticket => ParticipationState::Absent,
            Self::EstablishUnknown => ParticipationState::Establishing,
            Self::Active => ParticipationState::Active,
            Self::Quiescing | Self::ReleaseInFlight => ParticipationState::Quiescing,
            Self::Released => ParticipationState::TerminalRetained,
        }
    }

    fn owes_abort(self) -> bool {
        !matches!(self, Self::Pristine | Self::Released)
    }
}

struct Turn {
    units: Units,
    control: bool,
}

impl StepContext for Turn {
    fn charge(&mut self, units: usize) -> bool {
        self.units.charge(units)
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control: bool,
        _: BackendProcessId,
        records: usize,
        _: usize,
    ) -> QueueStage {
        assert_eq!(lane, DispatchLane::Lifecycle);
        assert_eq!(records, 1);
        assert_eq!(control, self.control);
        QueueStage::Granted(ReservationToken(id(900)))
    }
}

struct Fixture {
    core: Core,
    sources: AttemptSources,
    activation: AttemptActivationIdentity,
    now: MonotonicInstant,
    participation: ParticipationRef,
    inputs: u64,
    units: usize,
}

impl Fixture {
    fn new(state: BeforeStop, units: usize) -> Self {
        let mut start = start();
        // The only task never acquired Worker ownership. Its original empty
        // Quiesce fence is sufficient for the normal Release candidate.
        let root = start.plan.graph.root;
        start
            .plan
            .graph
            .tasks
            .retain(|task, _| *task == root.task_id());
        start
            .plan
            .graph
            .stages
            .retain(|stage, _| *stage == root.stage_id());
        start.plan.graph.edges.clear();
        let participation = start.plan.graph.tasks[&root.task_id()].participation;
        start
            .plan
            .establish_material
            .retain(|part, _| *part == participation);
        start
            .plan
            .admission_epochs
            .retain(|process, _| *process == root.backend_process_id());
        start
            .plan
            .preparation_positions
            .retain(|process, _| *process == root.backend_process_id());
        let mut fixture = Self {
            sources: start.sources,
            activation: start.activation.identity,
            now: start.initial_now,
            participation,
            core: Core::new(start).unwrap(),
            inputs: 0,
            units,
        };
        fixture.prepare(state);
        let owner = fixture.core.execution.owner(participation).unwrap();
        assert_eq!(owner.state(), state.state(), "{state:?}");
        assert_eq!(owner.is_released(), matches!(state, BeforeStop::Released));
        assert_eq!(
            fixture
                .core
                .execution
                .participation_never_established(participation),
            matches!(state, BeforeStop::Pristine),
        );
        assert_eq!(
            fixture.core.execution.dispatcher().in_flight_operations(),
            usize::from(matches!(state, BeforeStop::ReleaseInFlight)),
        );
        fixture.step(fixture.sources.actor, AttemptInput::Bootstrap);
        fixture
    }

    // Prepare exact participation facts with the same owners and dispatcher
    // used by the existing Core tests. The Stop and every later input use
    // AttemptCore::handle.
    fn prepare(&mut self, state: BeforeStop) {
        use BeforeStop::*;
        if matches!(state, Pristine) {
            return;
        }
        let execution = &mut self.core.execution;
        let part = self.participation;
        let admission = admission_intent(execution, part);
        let ticket = dispatch_admission(execution, admission, 701, OperationOutcome::Accepted);
        execution.apply_dispatched_reply(&ticket).1.unwrap();
        if matches!(state, Ticket) {
            return;
        }
        let establish = execution
            .next_authorization_intent(part, self.core.establish_material[&part])
            .unwrap()
            .unwrap();
        let OperationIntent::EstablishParticipation {
            initial_lease_sequence,
            initial_lease_valid_for,
            ..
        } = establish
        else {
            panic!("expected Establish for the original ticket");
        };
        let generation = self.emit_fixture_send(&establish, 710);
        let execution = &mut self.core.execution;
        execution
            .establish_send_started(part, establish.operation_id())
            .unwrap();
        let result = if matches!(state, EstablishUnknown) {
            OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                failure: None,
                protocol_failure: None,
                detail: None,
            }
        } else {
            worker(AckPayload::Participation(
                ParticipationReceipt::new(part, ParticipationState::Active).with_lease(
                    LeaseReceipt::new(
                        initial_lease_sequence,
                        initial_lease_valid_for,
                        Duration::from_secs(9),
                    ),
                ),
            ))
        };
        execution
            .apply_dispatched_reply(&reply(&establish, generation, result))
            .1
            .unwrap();
        if matches!(state, EstablishUnknown | Active) {
            return;
        }

        execution.begin_normal_drain();
        assert!(execution.normal_drain_started());
        let quiesce = emit_quiesce(
            execution,
            Arc::new(QuiesceParticipationReceipt::new(
                part,
                1,
                vec![],
                ParticipationState::Quiescing,
            )),
        );
        execution.apply_dispatched_reply(&quiesce).1.unwrap();
        execution.advance_quiesce(&mut Units(32)).unwrap().unwrap();
        if matches!(state, Quiescing) {
            return;
        }
        let ReleaseProgress::Done(Some(release)) = execution
            .advance_release_candidate(part, &mut ReleaseCheck::default(), &mut Units(32))
            .unwrap()
        else {
            panic!("original empty Quiesce fence makes Release eligible");
        };
        assert!(matches!(release, OperationIntent::ReleaseParticipation(_)));
        let generation = self.emit_fixture_send(&release, 720);
        if matches!(state, Released) {
            let closed = worker(AckPayload::Release {
                receipt: ParticipationReceipt::new(part, ParticipationState::TerminalRetained),
                outcome: ReleaseOutcome::Released,
                runtime_filter: None,
            });
            self.core
                .execution
                .apply_dispatched_reply(&reply(&release, generation, closed))
                .1
                .unwrap();
        }
    }

    fn emit_fixture_send(&mut self, operation: &OperationIntent, sequence: u64) -> SendGeneration {
        let effect = AttemptActionId::new(id(sequence));
        let generation = SendGeneration::new(effect);
        let grant = QueuedInputGrantRef::new(id(sequence + 1), generation);
        let execution = &mut self.core.execution;
        execution
            .begin_lifecycle_admission(operation.clone(), effect)
            .unwrap();
        assert!(matches!(
            execution
                .observe_lifecycle_grant(operation.operation_id(), grant)
                .unwrap(),
            CreateGrantProgress::Queued
        ));
        let send = AttemptActionId::new(id(sequence + 2));
        let BatchDispatch::Ready(AttemptEffect {
            body: EffectBody::SendBatch { batch, .. },
            ..
        }) = execution
            .advance_send_batch(
                &mut self.core.dispatch,
                send,
                &mut Turn {
                    units: Units(128),
                    control: operation.requires_control_progress(),
                },
                |_| Ok(()),
            )
            .unwrap()
        else {
            panic!("expected exact original fixture dispatch");
        };
        assert_eq!(batch.queued_inputs.as_ref(), [grant]);
        assert_eq!(batch.operations[0].operation_id(), operation.operation_id());
        execution.observe_send_started(send);
        generation
    }

    fn step(&mut self, source: AttemptSourceId, input: AttemptInput) -> AttemptStep {
        self.inputs += 1;
        let input_id = AttemptInputId(id(self.inputs));
        self.core
            .handle(
                self.now,
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: (source == self.sources.native).then_some(input_id),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: None,
                    }),
                },
                input,
                &mut Turn {
                    units: Units(self.units),
                    control: true,
                },
            )
            .unwrap()
    }

    fn stop(&mut self, reason: AttemptStopReason) {
        let stop = self.step(
            self.sources.actor,
            AttemptInput::Port(LogicalToAttempt {
                activation: self.activation,
                body: LogicalToAttemptBody::Stop {
                    reason,
                    failure: AttemptFailure::TaskProtocol(Box::new(schedule("original stop"))),
                },
            }),
        );
        assert!(stop.stop_producing);
        assert!(stop.effect.is_none());
    }

    fn drain(&mut self, mut observe: impl FnMut(AttemptEffect)) {
        let mut next = None;
        for _ in 0..128 {
            let turn = if let Some(effect) = next.take() {
                self.step(
                    self.sources.publication,
                    AttemptInput::Executor(ExecutorFact::PublicationApplied {
                        effect,
                        result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                    }),
                )
            } else if self.core.next_deadline() == Some(self.now) {
                self.step(self.sources.actor, AttemptInput::Timer)
            } else {
                return;
            };
            assert!(turn.abort_settlement.is_none(), "{turn:?}");
            assert!(
                turn.round_result.as_ref().is_none_or(Result::is_ok),
                "{turn:?}"
            );
            match turn.effect {
                Some(AttemptEffect {
                    id,
                    body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                    ..
                }) => next = Some(id),
                Some(effect) => observe(effect),
                None => {}
            }
        }
        panic!("original bounded turn did not settle");
    }
}

fn worker(acknowledgement: AckPayload) -> OperationReplyResult {
    OperationReplyResult::Worker(OperationResult::Verdict {
        outcome: OperationOutcome::Accepted,
        detail: None,
        acknowledgement: Some(acknowledgement),
    })
}

fn reply(
    operation: &OperationIntent,
    generation: SendGeneration,
    result: OperationReplyResult,
) -> OperationObservation {
    OperationObservation {
        generation,
        reply: OperationReply {
            operation_id: operation.operation_id(),
            kind: operation.kind(),
            result,
        },
    }
}

fn take_abort_admission(effect: AttemptEffect) -> (AbortParticipation, SendGeneration) {
    let AttemptEffect {
        id,
        class: EffectClass::Settlement,
        body:
            EffectBody::AdmitQueuedInput {
                operation: OperationIntent::AbortParticipation(request),
                generation,
                lane: DispatchLane::Lifecycle,
                ..
            },
    } = effect
    else {
        panic!("expected exact Lifecycle Abort admission, got {effect:?}");
    };
    assert_eq!(generation.admission_effect(), id);
    (request, generation)
}

#[test]
fn logical_failure_stands_down_every_potential_participation_without_claiming_exit() {
    use BeforeStop::*;
    for state in [
        Pristine,
        Ticket,
        EstablishUnknown,
        Active,
        Quiescing,
        ReleaseInFlight,
        Released,
    ] {
        for reason in [
            AttemptStopReason::LogicalExecutionFailed,
            AttemptStopReason::Client,
        ] {
            for units in [4, 128] {
                let mut fixture = Fixture::new(state, units);
                fixture.stop(reason);
                let mut abort = None;
                let mut terminal = false;
                fixture.drain(|effect| match effect.body {
                    EffectBody::ReleaseCreationPayload { .. } => {}
                    EffectBody::Port(port) => {
                        assert!(matches!(
                            port.body,
                            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(
                                AttemptFailure::TaskProtocol(ref error)
                            )) if **error == schedule("original stop")
                        ));
                        assert!(!std::mem::replace(&mut terminal, true));
                    }
                    _ => assert!(abort.replace(take_abort_admission(effect)).is_none()),
                });
                assert!(terminal, "{state:?}, {reason:?}, budget {units}");
                assert_eq!(
                    abort.map(|(request, _)| (request.participation(), request.cause())),
                    (reason == AttemptStopReason::LogicalExecutionFailed && state.owes_abort())
                        .then_some((fixture.participation, AbortCause::QueryFailed)),
                    "{state:?}, {reason:?}, budget {units}"
                );
                assert!(
                    fixture
                        .core
                        .execution
                        .participation_convergence_receipt(fixture.participation)
                        .is_none()
                );
                assert_eq!(
                    fixture
                        .core
                        .execution
                        .owner(fixture.participation)
                        .unwrap()
                        .is_released(),
                    matches!(state, Released)
                );
                assert_eq!(
                    fixture.core.execution.dispatcher().in_flight_operations(),
                    usize::from(matches!(state, ReleaseInFlight))
                );
            }
        }
    }
}

#[test]
fn stop_dispatches_the_original_actor_abort_after_terminal_cleanup() {
    for reason in [
        AttemptStopReason::LogicalExecutionFailed,
        AttemptStopReason::Client,
    ] {
        let mut fixture = Fixture::new(BeforeStop::Active, 128);
        let actor_action = AttemptActionId::new(id(801));
        let operation_id = fixture.core.execution.issue_operation_id();
        let request =
            AbortParticipation::new(operation_id, fixture.participation, AbortCause::QueryFailed);
        fixture.step(
            fixture.sources.actor,
            AttemptInput::Port(LogicalToAttempt {
                activation: fixture.activation,
                body: LogicalToAttemptBody::Abort {
                    action: actor_action,
                    request,
                },
            }),
        );
        let mut admission = None;
        fixture.drain(|effect| {
            assert!(admission.replace(take_abort_admission(effect)).is_none());
        });
        let (admitted, generation) = admission.unwrap();
        assert_eq!(admitted.envelope(), request.envelope());
        fixture.stop(reason);
        fixture.drain(|effect| {
            assert!(matches!(
                effect.body,
                EffectBody::ReleaseCreationPayload { .. } | EffectBody::Port(_)
            ));
        });
        assert!(!fixture.core.execution.has_pending_terminal_cleanup());

        let operation = OperationIntent::AbortParticipation(request);
        let grant = QueuedInputGrantRef::new(id(802), generation);
        let granted = fixture.step(
            fixture.sources.actor,
            AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                operation: operation_id,
                grant,
                queued_bytes: operation.queued_bytes(),
            }),
        );
        assert!(granted.effect.is_none());
        let mut sent = false;
        fixture.drain(|effect| {
            let AttemptEffect {
                class: EffectClass::Settlement,
                body: EffectBody::SendBatch { batch, .. },
                ..
            } = effect
            else {
                panic!("unexpected post-cleanup effect {effect:?}");
            };
            assert!(!std::mem::replace(&mut sent, true));
            assert_eq!(batch.lane, DispatchLane::Lifecycle);
            assert!(batch.control_progress);
            assert_eq!(batch.backend, request.participation().backend_process_id());
            assert_eq!(batch.queued_inputs.as_ref(), [grant]);
            let [OperationIntent::AbortParticipation(dispatched)] = batch.operations.as_ref()
            else {
                panic!("expected just the original actor Abort");
            };
            assert_eq!(dispatched.envelope(), request.envelope());
            assert_eq!(dispatched.participation(), request.participation());
            assert_eq!(dispatched.cause(), request.cause());
        });
        assert!(sent, "{reason:?}");
        let receipt = worker(AckPayload::Participation(ParticipationReceipt::new(
            request.participation(),
            ParticipationState::Aborting,
        )));
        let done = fixture.step(
            fixture.sources.native,
            AttemptInput::Executor(ExecutorFact::Operation(reply(
                &operation, generation, receipt,
            ))),
        );
        assert!(matches!(
            done.abort_settlement,
            Some(AttemptEffect { body: EffectBody::Port(port), .. })
                if matches!(port.body, AttemptToLogicalBody::AbortSettlement {
                    abort_action, generation: settled, receipt_generation: Some(receipt),
                    operation_id: actual, participation, settlement: AbortIssueSettlement::Worker(OperationOutcome::Accepted)
                } if abort_action == actor_action
                    && settled == generation.admission_effect()
                    && receipt == generation.admission_effect()
                    && actual == operation_id
                    && participation == request.participation())
        ));
        assert_eq!(
            fixture.core.execution.dispatcher().in_flight_operations(),
            0
        );
        assert!(
            fixture
                .core
                .execution
                .owner(fixture.participation)
                .unwrap()
                .is_released()
        );
        assert!(
            fixture
                .core
                .execution
                .participation_convergence_receipt(fixture.participation)
                .is_none()
        );
    }
}
