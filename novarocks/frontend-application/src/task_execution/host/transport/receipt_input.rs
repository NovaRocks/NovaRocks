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

//! Own the original paid Native input and its one physical send's receipt.
use super::SendKey;
use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::intent::{OperationAcknowledgement, attempt_receipt::AttemptReceiptError},
};
use novarocks_attempt_protocol::{
    failure::HostFailureRef,
    input::{AttemptInput, ExecutorFact},
    start::PayloadRef,
};
use novarocks_control_runtime::actor_turn::{ActorTurnError, StandingInputObligation};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_execution_contract::OperationKind;

#[derive(Debug)]
pub(crate) enum NativeReceiptError {
    WrongSend,
    AlreadyRecorded,
    Projection(AttemptReceiptError),
}

enum Receipt {
    Prepared {
        original: OperationAcknowledgement,
        value: FrontendInput,
    },
    Submitted {
        original: OperationAcknowledgement,
        input: InputId,
    },
}
impl Receipt {
    fn into_retired(self) -> RetiredNativeReceipt {
        match self {
            Self::Prepared { original, value } => RetiredNativeReceipt {
                original,
                unpublished: Some(value),
            },
            Self::Submitted { original, .. } => RetiredNativeReceipt {
                original,
                unpublished: None,
            },
        }
    }
}

/// Carry the same raw acknowledgement and any rejected passive backing to
/// their actual final owner. Closing the mailbox position drops neither here.
#[must_use]
pub(crate) struct RetiredNativeReceipt {
    pub(crate) original: OperationAcknowledgement,
    pub(crate) unpublished: Option<FrontendInput>,
}

/// The original Source must keep this row through actual producer exit and
/// input retirement. One row can submit at most one original receipt.
#[must_use]
pub(crate) struct NativeReceiptInput {
    key: SendKey,
    kind: OperationKind,
    position: StandingInputObligation<FrontendActor>,
    receipt: Option<Receipt>,
}
impl NativeReceiptInput {
    pub(super) fn key(&self) -> SendKey {
        self.key
    }
    pub(super) fn kind(&self) -> OperationKind {
        self.kind
    }
    pub(in crate::task_execution::host) fn new(
        key: SendKey,
        kind: OperationKind,
        position: StandingInputObligation<FrontendActor>,
    ) -> Self {
        Self {
            key,
            kind,
            position,
            receipt: None,
        }
    }

    /// Freeze the passive fact on the executor before filling the mailbox.
    /// Refusal returns the same raw acknowledgement without changing the row.
    pub(crate) fn record(
        &mut self,
        key: SendKey,
        original: OperationAcknowledgement,
        filter: Option<PayloadRef>,
        workload: Option<HostFailureRef>,
    ) -> Result<(), (NativeReceiptError, OperationAcknowledgement)> {
        if key != self.key
            || original.operation_id() != self.key.operation
            || original.kind() != self.kind
        {
            return Err((NativeReceiptError::WrongSend, original));
        }
        if self.receipt.is_some() {
            return Err((NativeReceiptError::AlreadyRecorded, original));
        }
        let observation = match original.attempt_observation(
            self.key.operation,
            self.key.generation,
            filter,
            workload,
        ) {
            Ok(observation) => observation,
            Err(error) => return Err((NativeReceiptError::Projection(error), original)),
        };
        self.receipt = Some(Receipt::Prepared {
            original,
            value: FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                ExecutorFact::Operation(observation),
            ))),
        });
        Ok(())
    }

    /// The actual fill decides Native FIFO order. On runtime refusal retain
    /// both originals, including the already materialized passive input.
    pub(crate) fn submit(&mut self) -> Result<InputId, ActorTurnError> {
        let Some(receipt) = self.receipt.take() else {
            return Err(ActorTurnError::InputNotReserved);
        };
        match receipt {
            Receipt::Prepared { original, value } => match self.position.fill(value) {
                Ok(input) => {
                    self.receipt = Some(Receipt::Submitted { original, input });
                    Ok(input)
                }
                Err((error, value)) => {
                    self.receipt = Some(Receipt::Prepared { original, value });
                    Err(error)
                }
            },
            Receipt::Submitted { input, .. } => {
                self.receipt = Some(receipt);
                Ok(input)
            }
        }
    }

    /// The caller already owns proof of producer and capture exit. The
    /// original standing position independently requires input retirement.
    /// The raw ACK can outlive this mailbox role in the original error owner.
    pub(crate) fn retire_after_exit(
        mut self,
    ) -> Result<Option<RetiredNativeReceipt>, (ActorTurnError, Self)> {
        if let Err(error) = self.position.close_after_exit() {
            return Err((error, self));
        }
        Ok(self.receipt.take().map(Receipt::into_retired))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::{FrontendEffect, TestActor};
    use crate::task_execution::intent::AckPayload;
    use novarocks_attempt_protocol::input::SendGeneration;
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::ActorRuntime,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        identity::{ActorId, Generation, Tick},
        mailbox::{InputClass, InputEnvelope},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{OperationOutcome, TaskOperationId};
    use std::{
        num::NonZeroU64,
        sync::{Arc, Mutex},
    };

    type Record = (
        InputEnvelope,
        Option<InputId>,
        SendGeneration,
        Option<OperationOutcome>,
    );
    struct Recorder(Arc<Mutex<Vec<Record>>>);
    impl TestActor for Recorder {
        fn handle(
            &mut self,
            _: Tick,
            envelope: InputEnvelope,
            value: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            let FrontendInput::Attempt(value) = value else {
                panic!("expected a Native receipt")
            };
            let AttemptInput::Executor(ExecutorFact::Operation(receipt)) = *value else {
                panic!("expected the passive operation observation")
            };
            let cut = step.input_source_cut().expect("original Native cut");
            self.0.lock().unwrap().push((
                envelope,
                cut.tail,
                receipt.generation,
                receipt.reply.worker_outcome(),
            ));
        }
    }

    #[test]
    fn exact_native_input_keeps_fill_order_and_original_ack_until_retirement() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let epoch = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, epoch).unwrap();
        let (mut work, mut capacity) = scope
            .reserve_next(
                epoch,
                ReservationResources::for_work(ResourceVector::default()),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        let actor = ActorId::new(1).unwrap();
        let source = ActorId::new(2).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let registration = runtime
            .register_bound_with_source_cut(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Recorder(observed.clone()))),
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                source,
            )
            .unwrap_or_else(|(error, _)| panic!("{error}"));
        let operation = TaskOperationId::new_v7();
        let kind = OperationKind::CreateTask;
        let key = |n| SendKey {
            operation,
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(n).unwrap())),
        };
        let reserve = |key| {
            let position = runtime
                .reserve_admitted_input_for(&registration, source, InputClass::Lifecycle, &work)
                .unwrap()
                .into_standing_input()
                .unwrap();
            NativeReceiptInput::new(key, kind, position)
        };
        let mut old = reserve(key(1));
        let mut latest = reserve(key(2));
        let positive = OperationAcknowledgement::worker_receipt(
            operation,
            kind,
            OperationOutcome::Accepted,
            AckPayload::None,
        );
        let unknown = OperationAcknowledgement::transport_unknown(operation, kind);
        let (refused, returned) = latest
            .record(key(1), unknown.clone(), None, None)
            .unwrap_err();
        assert!(matches!(refused, NativeReceiptError::WrongSend));
        old.record(key(1), returned, None, None).unwrap();
        latest.record(key(2), positive.clone(), None, None).unwrap();
        let latest_input = latest.submit().unwrap();
        let old_input = old.submit().unwrap();
        assert_eq!(latest.submit().unwrap(), latest_input);
        assert_ne!(latest_input, old_input);
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        let latest = match latest.retire_after_exit() {
            Err((ActorTurnError::StandingInputBusy, retained)) => retained,
            _ => panic!("producer exit leaves the queued original input live"),
        };
        assert!(runtime.poll_one(Tick(1)).unwrap());
        {
            let seen = observed.lock().unwrap();
            assert_eq!(seen.len(), 2);
            let expected = [
                (
                    latest_input,
                    key(2).generation,
                    Some(OperationOutcome::Accepted),
                ),
                (old_input, key(1).generation, None),
            ];
            for ((envelope, cut, generation, verdict), (input, exact_generation, exact_verdict)) in
                seen.iter().zip(expected)
            {
                assert_eq!(envelope.source, source);
                assert_eq!(envelope.class, InputClass::Lifecycle);
                assert_eq!((envelope.input, *cut), (input, Some(old_input)));
                assert_eq!((*generation, *verdict), (exact_generation, exact_verdict));
            }
        }
        runtime.retire_input(actor, latest_input).unwrap();
        runtime.retire_input(actor, old_input).unwrap();
        for (receipt, expected) in [(latest, positive), (old, unknown)] {
            let retired = receipt
                .retire_after_exit()
                .unwrap_or_else(|(error, _)| panic!("{error}"))
                .expect("the same original acknowledgement");
            assert_eq!(retired.original, expected);
            assert!(retired.unpublished.is_none());
        }
        capacity.finish().unwrap();
        scope.close_external().unwrap();
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        while let Some(turn) = runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        runtime.remove_actor(actor).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(runtime.is_empty() && ledger.is_empty());
    }
}
