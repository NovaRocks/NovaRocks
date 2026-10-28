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

//! Pure lifecycle decisions from the Context's already-bound owner-fence
//! input. Explicit model host facts prove no physical Native/Scope exit.

use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, CancelReason, ContentFingerprint, CredentialEpoch,
    CredentialLeaseId, LeaseValidFor, QueryContextRef, QueryContextState, TaskIdentity,
    TaskOperationId, TaskState, TerminationDetail,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_worker_protocol::{
    context::{
        ContextCommand, ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextInput, ContextMaterialized, ContextStep,
    },
    control::EstablishSemanticFacts,
    creation::{
        AdmissionNeed, AdmissionPort, AdmissionRefusal, CreationCost, PayloadId, RequestContext,
    },
    lease::{LeaseBounds, MonotonicInstant},
};
use std::{collections::VecDeque, time::Duration};

fn bytes(n: u8) -> [u8; 16] {
    let mut bytes = [n; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes
}
fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn operation(n: u8) -> TaskOperationId {
    TaskOperationId::try_from_bytes(bytes(n)).unwrap()
}
fn context() -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(8211, 8212), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(bytes(1)).unwrap(),
        BackendProcessId::try_from_bytes(bytes(2)).unwrap(),
    )
}
fn task() -> TaskIdentity {
    TaskIdentity::new(
        context().query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        context().backend_process_id(),
    )
}
#[derive(Default)]
struct Port {
    admitted: u64,
}
impl AdmissionPort for Port {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        self.admitted += 1;
        Ok(ReservationId::new(self.admitted).unwrap())
    }
}
struct Model {
    core: ContextCore,
    port: Port,
    next_token: u64,
}
impl Model {
    fn new() -> Self {
        Self {
            core: ContextCore::new(
                context(),
                Generation::new(1).unwrap(),
                ContextEstablishment {
                    operation: operation(1),
                    ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
                    payload: PayloadId(1),
                    elected_at: at(0),
                    semantics: EstablishSemanticFacts {
                        catalog: ContentFingerprint::from_bytes([1; 16]),
                        shared_filter: ContentFingerprint::from_bytes([2; 16]),
                        query_options: ContentFingerprint::from_bytes([3; 16]),
                        credential_lease: CredentialLeaseId::new(1),
                        credential_epoch: CredentialEpoch::FIRST,
                        initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30))
                            .unwrap(),
                    },
                },
                ContextConfig {
                    max_tasks: 1,
                    max_creation_waiters: 2,
                    max_pending_requests: 4,
                    max_preparing: 1,
                    max_task_bytes: 100,
                    step_budget: ContextCore::MAX_STEP_WORK,
                    lease_bounds: LeaseBounds::DEFAULT,
                    metric_publish_interval: Duration::from_millis(100),
                },
            )
            .unwrap(),
            port: Port::default(),
            next_token: 1,
        }
    }
    fn active() -> Self {
        let mut model = Self::new();
        model.fact(
            0,
            ContextInput::MaterializationFinished {
                result: Ok(ContextMaterialized {
                    initial_credential_resource: 101,
                }),
            },
        );
        assert_eq!(model.core.state(), QueryContextState::Active);
        model
    }
    fn check(step: &ContextStep) {
        assert!(step.charged_work() <= ContextCore::MAX_STEP_WORK);
    }
    fn fact(&mut self, now: u64, input: ContextInput) -> ContextStep {
        let step = self.core.handle(at(now), input, &mut self.port).unwrap();
        Self::check(&step);
        step
    }
    fn fence(&mut self, now: u64) -> ContextStep {
        let admitted = self.port.admitted;
        let step = self.fact(now, ContextInput::OwnerFenced);
        assert!(
            step.requests.is_empty(),
            "an owner fence creates no RPC reply role"
        );
        assert_eq!(self.port.admitted, admitted);
        step
    }
    fn command(&mut self, now: u64, command: ContextCommand) -> ContextStep {
        let token = self.next_token;
        self.next_token += 1;
        let step = self
            .core
            .handle_request(
                at(now),
                RequestContext {
                    token: InputId::new(token).unwrap(),
                    operation: operation(u8::try_from(token + 10).unwrap()),
                    deadline: at(100),
                },
                command,
                &mut self.port,
            )
            .unwrap();
        Self::check(&step);
        step
    }
    fn accept_task(&mut self) {
        self.command(
            1,
            ContextCommand::Create {
                task: task(),
                payload: PayloadId(2),
            },
        );
        let mut loaded = self.fact(
            1,
            ContextInput::Loaded {
                task: task(),
                result: Ok(CreationCost {
                    input_retained_bytes: 1,
                    ingress_retained_bytes: 1,
                    structural_bytes: 1,
                    inbound_records: 0,
                    inbound_bytes: 0,
                }),
            },
        );
        for _ in 0..4 {
            if !loaded
                .effects
                .iter()
                .any(|effect| matches!(effect, ContextEffect::ContinueReplies { .. }))
            {
                assert!(self.core.status(task()).is_some());
                return;
            }
            loaded = self.fact(1, ContextInput::ContinueReplies { task: task() });
        }
        panic!("the original single Create reply did not settle");
    }
    fn follow_close(&mut self, now: u64, effects: Vec<ContextEffect>) -> Vec<ContextEffect> {
        let mut pending = VecDeque::from(effects);
        let mut observed = Vec::new();
        for _ in 0..16 {
            let Some(effect) = pending.pop_front() else {
                return observed;
            };
            let input = match &effect {
                ContextEffect::ContinueClose { .. } => Some(ContextInput::ContinueClose),
                ContextEffect::ApplyTaskClose { task, generation } => {
                    Some(ContextInput::ApplyTaskClose {
                        task: *task,
                        generation: *generation,
                    })
                }
                _ => None,
            };
            observed.push(effect);
            if let Some(input) = input {
                let next = self.fact(now, input);
                assert!(next.requests.is_empty());
                pending.extend(next.effects);
            }
        }
        panic!("the one-task close exceeded its finite effect bound");
    }
}
fn closes_started(effects: &[ContextEffect]) -> usize {
    effects
        .iter()
        .filter(|effect| matches!(effect, ContextEffect::CloseExternalAdmission { .. }))
        .count()
}

#[test]
fn establishing_and_active_fences_close_once_without_rpc_and_do_not_reopen_terminal_contexts() {
    for active in [false, true] {
        let mut model = if active {
            Model::active()
        } else {
            Model::new()
        };
        let original = model.core.generation();
        let first = model.fence(1);
        assert_eq!(model.core.state(), QueryContextState::Aborting);
        assert_eq!(model.core.generation(), original.next().unwrap());
        assert_eq!(closes_started(&first.effects), 1);
        assert_eq!(first.effects.len(), 3);
        let close_epoch = model.core.generation();
        let repeated = model.fence(1);
        assert!(repeated.effects.is_empty());
        assert_eq!(model.core.generation(), close_epoch);
        let applied = model.follow_close(1, first.effects);
        assert_eq!(
            applied
                .iter()
                .filter(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
                .count(),
            1
        );
        assert!(model.fence(2).effects.is_empty());
        // This explicit pure environment fact is not a Native or Work exit
        // receipt. It exercises the existing TerminalRetained protocol state.
        model.fact(2, ContextInput::HostReleased);
        assert_eq!(model.core.state(), QueryContextState::TerminalRetained);
        let late = model.fence(31);
        assert!(late.effects.is_empty());
        assert_eq!(model.core.state(), QueryContextState::TerminalRetained);
        assert_eq!(model.core.generation(), close_epoch);
    }
}

#[test]
fn an_owner_fence_upgrades_a_really_quiescing_context_through_the_existing_abort_path() {
    let mut model = Model::active();
    let quiesce = model.command(1, ContextCommand::Quiesce);
    assert_eq!(model.core.state(), QueryContextState::Quiescing);
    assert!(quiesce.effects.contains(&ContextEffect::ContinueQuiesce));
    let decided = model.fact(1, ContextInput::ContinueQuiesce);
    assert_eq!(
        decided.requests.len(),
        1,
        "only the original Quiesce RPC replies"
    );
    let before = model.core.generation();
    let fenced = model.fence(2);
    assert_eq!(model.core.state(), QueryContextState::Aborting);
    assert_eq!(model.core.generation(), before.next().unwrap());
    assert_eq!(closes_started(&fenced.effects), 1);
    let closing = model.core.generation();
    model.follow_close(2, fenced.effects);
    assert!(model.fence(3).effects.is_empty());
    assert_eq!(model.core.generation(), closing);
}

#[test]
fn lease_deadline_and_an_existing_close_keep_the_original_cause_before_owner_fence() {
    for (now, prior_expiry, expected) in [
        (2, false, AbortCause::QueryFailed),
        (30, false, AbortCause::LeaseExpired),
        (31, true, AbortCause::LeaseExpired),
    ] {
        let mut model = Model::active();
        model.accept_task();
        let original = model.core.generation();
        let mut effects = Vec::new();
        if prior_expiry {
            let expiry = model.fact(30, ContextInput::Tick);
            assert!(expiry.requests.is_empty());
            effects.extend(expiry.effects);
        }
        let fenced = model.fence(now);
        effects.extend(fenced.effects);
        assert_eq!(model.core.generation(), original.next().unwrap());
        assert_eq!(closes_started(&effects), 1);
        model.follow_close(now, effects);
        assert_eq!(
            model.core.status(task()).unwrap().termination(),
            Some(&TerminationDetail::Aborted(expected))
        );
        let generation = model.core.generation();
        assert!(model.fence(now).effects.is_empty());
        assert_eq!(model.core.generation(), generation);
    }
}

#[test]
fn owner_fencing_does_not_replace_an_already_stable_task_cancellation() {
    let mut model = Model::active();
    model.accept_task();
    model.command(
        1,
        ContextCommand::Cancel {
            task: task(),
            reason: CancelReason::UpstreamNoLongerNeeded,
        },
    );
    model.fact(
        1,
        ContextInput::FinishWithOutput {
            task: task(),
            output: None,
        },
    );
    assert_eq!(
        model.core.status(task()).unwrap().state(),
        TaskState::Canceled
    );
    let fenced = model.fence(2);
    model.follow_close(2, fenced.effects);
    assert_eq!(
        model.core.status(task()).unwrap().state(),
        TaskState::Canceled
    );
    assert_eq!(
        model.core.status(task()).unwrap().termination(),
        Some(&TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        ))
    );
    assert!(model.core.task(task()).unwrap().conclusion.sealed);
}
