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

use crate::task_execution::intent::{AckPayload, OperationAcknowledgement};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, Publication},
    error::TaskExecutionError,
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, PublicationApplied, SendGeneration,
    },
    start::{AttemptSources, AttemptStart},
};
use novarocks_control_runtime::obligation::StepRegistration;
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptActionId, AttemptActivationIdentity, AttemptToLogicalBody,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortParticipation, OperationKind, OperationOutcome, ParticipationReceipt, ParticipationState,
    SafeDetail,
};
use novarocks_types::BackendProcessId;
use std::num::{NonZeroU64, NonZeroUsize};

fn positive(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

struct Turn<'a> {
    queue: Option<&'a StepRegistration>,
    backend: BackendProcessId,
}

impl StepContext for Turn<'_> {
    fn charge(&mut self, _: usize) -> bool {
        true
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control: bool,
        backend: BackendProcessId,
        records: usize,
        _: usize,
    ) -> QueueStage {
        assert_eq!(lane, DispatchLane::Lifecycle);
        assert!(control);
        assert_eq!(backend, self.backend);
        assert_eq!(records, 1);
        QueueStage::Granted(ReservationToken(positive(
            self.queue
                .take()
                .expect("original paid queue stage")
                .reservation_id()
                .get(),
        )))
    }
}

/// This fixture drives Core at the passive source boundary while the real
/// Host owns both queue handoffs and the original unary receipt.
pub(super) struct LateAbortCore {
    core: Core,
    sources: AttemptSources,
    activation: AttemptActivationIdentity,
    request: AbortParticipation,
    now: MonotonicInstant,
    ordinal: u64,
}

impl LateAbortCore {
    pub(super) fn new(
        mut start: AttemptStart,
        activation: AttemptActivationIdentity,
        request: AbortParticipation,
    ) -> Self {
        start.activation.identity = activation;
        start.plan.authorization_limits.activation = activation;
        start.plan.authorization_limits.abort_per_participation = NonZeroUsize::new(2).unwrap();
        let mut trace = Self {
            sources: start.sources,
            activation,
            request,
            now: start.initial_now,
            core: Core::new(start).unwrap(),
            ordinal: 100,
        };
        trace.step(trace.sources.actor, AttemptInput::Bootstrap, None);
        trace
    }

    fn step(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInput,
        queue: Option<&StepRegistration>,
    ) -> AttemptStep {
        self.ordinal += 1;
        let input_id = AttemptInputId(positive(self.ordinal));
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
                    queue,
                    backend: self.request.participation().backend_process_id(),
                },
            )
            .unwrap()
    }

    fn continue_turn(
        &mut self,
        step: AttemptStep,
        queue: Option<&StepRegistration>,
    ) -> AttemptStep {
        match step.effect {
            Some(AttemptEffect {
                id: effect,
                body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                ..
            }) => self.step(
                self.sources.publication,
                AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                }),
                None,
            ),
            None => self.step(self.sources.actor, AttemptInput::Timer, queue),
            other => panic!("unexpected continuation before the original Abort settles: {other:?}"),
        }
    }

    fn wait_for_native(&mut self, mut step: AttemptStep) {
        for _ in 0..16 {
            assert!(
                !step.round_result.as_ref().is_some_and(Result::is_err),
                "{step:?}"
            );
            if step.selection == InputSelection::Prefer(self.sources.native) {
                assert!(step.effect.is_none(), "{step:?}");
                return;
            }
            step = self.continue_turn(step, None);
        }
        panic!("the original round did not return to Native");
    }

    fn take_effect(
        &mut self,
        mut step: AttemptStep,
        queue: Option<&StepRegistration>,
        expected: impl Fn(&EffectBody) -> bool,
    ) -> AttemptEffect {
        for _ in 0..16 {
            if let Some(effect) = &step.effect {
                if expected(&effect.body) {
                    return step.effect.unwrap();
                }
            }
            step = self.continue_turn(step, queue);
        }
        panic!("the original Abort did not produce its transport effect");
    }

    pub(super) fn begin(&mut self, number: u64) -> AttemptEffect {
        let step = self.step(
            self.sources.actor,
            AttemptInput::Port(LogicalToAttempt {
                activation: self.activation,
                body: LogicalToAttemptBody::Abort {
                    action: AttemptActionId::new(positive(number)),
                    request: self.request,
                },
            }),
            None,
        );
        let effect = self.take_effect(step, None, |body| {
            matches!(body, EffectBody::AdmitQueuedInput { .. })
        });
        let next = self.step(self.sources.actor, AttemptInput::Timer, None);
        self.wait_for_native(next);
        effect
    }

    pub(super) fn send(&mut self, grant: ExecutorFact, queue: &StepRegistration) -> AttemptEffect {
        let step = self.step(self.sources.transport, AttemptInput::Executor(grant), None);
        let effect = self.take_effect(step, Some(queue), |body| {
            matches!(body, EffectBody::SendBatch { .. })
        });
        let next = self.step(self.sources.actor, AttemptInput::Timer, None);
        self.wait_for_native(next);
        effect
    }

    pub(super) fn observe_first(
        &mut self,
        started: ExecutorFact,
        unknown: ExecutorFact,
        generation: SendGeneration,
    ) {
        let ExecutorFact::SendStarted { started_at, .. } = started else {
            panic!("the first Native send physically started");
        };
        self.now = started_at;
        let first = self.step(self.sources.native, AttemptInput::Executor(started), None);
        self.wait_for_native(first);
        let settled = self.step(self.sources.native, AttemptInput::Executor(unknown), None);
        self.assert_abort_settlement(
            &settled,
            generation,
            None,
            AbortIssueSettlement::TransportUnknown,
        );
        self.wait_for_native(settled);
    }

    pub(super) fn resolve_older(
        &mut self,
        first_generation: SendGeneration,
        replay_send: AttemptActionId,
        verdict: OperationOutcome,
    ) -> AttemptEffect {
        let operation = self.request.envelope().operation_id();
        let ack = OperationAcknowledgement::worker_receipt(
            operation,
            OperationKind::AbortParticipation,
            verdict,
            if verdict == OperationOutcome::OwnerFailed {
                AckPayload::None
            } else {
                assert_eq!(verdict, OperationOutcome::ParticipationTerminalReceipt);
                AckPayload::Participation(ParticipationReceipt::new(
                    self.request.participation(),
                    ParticipationState::TerminalRetained,
                ))
            },
        )
        .with_detail(Some(SafeDetail::truncating("original late Abort receipt")));
        // A passive Core fixture input. The actual unary row already
        // returned its one original result, TransportUnknown.
        let late = ack
            .attempt_observation(operation, first_generation, None, None)
            .unwrap();
        let mut selected = self.step(
            self.sources.native,
            AttemptInput::Executor(ExecutorFact::Operation(late)),
            None,
        );
        if verdict == OperationOutcome::OwnerFailed {
            assert!(selected.stop_producing);
            self.assert_abort_settlement(
                &selected,
                first_generation,
                None,
                AbortIssueSettlement::FailedClosed,
            );
        } else {
            assert!(!selected.stop_producing);
            self.assert_abort_settlement(
                &selected,
                first_generation,
                Some(first_generation),
                AbortIssueSettlement::Worker(verdict),
            );
        }
        let withdrawal = selected
            .effect
            .take()
            .expect("the late original receipt must withdraw its replay");
        assert!(
            matches!(withdrawal.body, EffectBody::Withdraw { effect } if effect == replay_send)
        );
        if verdict == OperationOutcome::OwnerFailed {
            let expected =
                AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::OperationFailed {
                    task: None,
                    participation: self.request.participation(),
                    operation_id: operation,
                    kind: OperationKind::AbortParticipation,
                    rejection: ack.worker_header().unwrap(),
                }));
            for _ in 0..16 {
                if let Some(round) = &selected.round_result {
                    assert_eq!(round, &Err(expected));
                    return withdrawal;
                }
                selected = self.continue_turn(selected, None);
            }
            panic!("the original round lost its typed OwnerFailed");
        }
        withdrawal
    }

    fn assert_abort_settlement(
        &self,
        step: &AttemptStep,
        generation: SendGeneration,
        receipt_generation: Option<SendGeneration>,
        expected: AbortIssueSettlement,
    ) {
        let Some(AttemptEffect {
            body: EffectBody::Port(port),
            ..
        }) = &step.abort_settlement
        else {
            panic!("the original Abort requires its QA settlement");
        };
        assert_eq!(port.activation, self.activation);
        assert!(matches!(port.body,
            AttemptToLogicalBody::AbortSettlement {
                abort_action, generation: actual, receipt_generation: receipt,
                participation, operation_id, settlement,
            } if abort_action == AttemptActionId::new(positive(1))
                && actual == generation.admission_effect()
                && receipt == receipt_generation.map(SendGeneration::admission_effect)
                && participation == self.request.participation()
                && operation_id == self.request.envelope().operation_id()
                && settlement == expected
        ));
    }
}
