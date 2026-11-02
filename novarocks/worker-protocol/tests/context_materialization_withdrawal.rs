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

//! Pure Context owner decisions after an admitted M returns without invoking
//! materialization I/O. Host exit itself remains an explicit world fact.

use novarocks_control_runtime_core::identity::{
    Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, WorkId,
};
use novarocks_execution_contract::{
    AdmissionTicketId, ContentFingerprint, CredentialEpoch, CredentialLeaseId, LeaseValidFor,
    OperationOutcome, QueryContextRef, QueryContextState, TaskOperationId,
};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
};
use novarocks_worker_protocol::{
    context::{
        ContextCommand, ContextConfig, ContextCore, ContextEffect, ContextEstablishment,
        ContextInput, ContextMaterialized, ContextStep, CoreError,
    },
    control::EstablishSemanticFacts,
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestContext},
    domain::CredentialState,
    lease::{LeaseBounds, MonotonicInstant},
};
use std::time::Duration;

// One explicit model process owns these requests. The fixture's non-reused
// input issuance ordinal also names its original R Work; this is plain protocol
// metadata and cannot create runtime Work, capacity or an InputAuthor proof.
fn model_request_author(token: InputId) -> ScopedWorkRef {
    ScopedWorkRef {
        scope: ScopeId::new(1).expect("the model process Scope"),
        work: WorkId::new(token.get()).expect("the original model request ordinal"),
    }
}

fn at(seconds: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_secs(seconds))
}
fn operation(value: u8) -> TaskOperationId {
    let mut bytes = [value; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    TaskOperationId::try_from_bytes(bytes).unwrap()
}
fn core() -> ContextCore {
    let mut process = [1; 16];
    process[6] = 0x70;
    process[8] = 0x80;
    let identity = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(100, 101), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(process).unwrap(),
        BackendProcessId::try_from_bytes(process).unwrap(),
    );
    ContextCore::new(
        identity,
        Generation::new(1).unwrap(),
        ContextEstablishment {
            operation: operation(10),
            ticket: AdmissionTicketId::try_from_bytes([20; 16]).unwrap(),
            payload: PayloadId(30),
            semantics: EstablishSemanticFacts {
                catalog: ContentFingerprint::from_bytes([40; 16]),
                shared_filter: ContentFingerprint::from_bytes([41; 16]),
                query_options: ContentFingerprint::from_bytes([42; 16]),
                credential_lease: CredentialLeaseId::new(43),
                credential_epoch: CredentialEpoch::FIRST,
                initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            },
            elected_at: at(0),
        },
        ContextConfig {
            max_tasks: 8,
            max_creation_waiters: 8,
            max_pending_requests: 8,
            max_preparing: 2,
            max_task_bytes: 1024,
            step_budget: ContextCore::MAX_STEP_WORK,
            lease_bounds: LeaseBounds::DEFAULT,
            metric_publish_interval: Duration::from_millis(100),
            request_horizon: novarocks_worker_protocol::lease::RequestHorizon::DEFAULT,
        },
    )
    .unwrap()
}

fn command_step(
    core: &mut ContextCore,
    now: u64,
    token: u64,
    operation: TaskOperationId,
    command: ContextCommand,
) -> ContextStep {
    let step = core
        .handle_request(
            at(now),
            RequestContext {
                author: model_request_author(InputId::new(token).unwrap()),
                token: InputId::new(token).unwrap(),
                operation,
                deadline: at(10_000),
            },
            command,
            &mut NoAdmission,
        )
        .unwrap();
    assert!(step.charged_work() <= ContextCore::MAX_BODY_WORK);
    step
}

struct NoAdmission;
impl AdmissionPort for NoAdmission {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        panic!("withdrawal and closing decisions cannot acquire capacity");
    }
}
fn step(core: &mut ContextCore, now: u64, input: ContextInput) -> ContextStep {
    let step = core.handle(at(now), input, &mut NoAdmission).unwrap();
    assert!(step.charged_work() <= ContextCore::MAX_BODY_WORK);
    step
}
fn assert_decision(core: &ContextCore, step: &ContextStep, expected: OperationOutcome) {
    let decisions: Vec<_> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ContextEffect::EstablishmentDecided {
                context,
                close_epoch,
                decision,
            } => Some((*context, *close_epoch, decision)),
            _ => None,
        })
        .collect();
    assert_eq!(decisions.len(), 1);
    let (context, generation, decision) = decisions[0];
    assert_eq!(context, core.identity());
    assert_eq!(generation, core.generation());
    assert_eq!(decision.state, core.state());
    assert_eq!(decision.outcome, Ok(expected));
    assert!(step.requests.is_empty());
    assert_ne!(core.state(), QueryContextState::Active);
}

#[test]
fn withdrawal_preserves_the_original_lease_expiry_instead_of_a_host_guessed_terminal_verdict() {
    for host_released in [false, true] {
        let mut core = core();
        assert_eq!(core.current_credential(), None);
        assert_eq!(core.next_deadline(), Some(at(30)));
        step(&mut core, 30, ContextInput::Tick);
        assert_eq!(core.state(), QueryContextState::Aborting);
        if host_released {
            // A pure environment fact, not a fabricated Native call or Work exit.
            step(&mut core, 30, ContextInput::HostReleased);
            assert_eq!(core.state(), QueryContextState::TerminalRetained);
        }
        let established = core.establishment();
        let returned = step(&mut core, 31, ContextInput::MaterializationWithdrawn);
        assert_decision(&core, &returned, OperationOutcome::LeaseExpired);
        assert_eq!(core.current_credential(), None);
        assert_eq!(core.establishment(), established);
        assert_eq!(core.next_deadline(), None);
    }
}

#[test]
fn a_quiesce_won_withdrawal_keeps_normal_closure_after_the_lease_deadline_unless_a_tick_upgraded_it()
 {
    for phase in 0..3 {
        let mut core = core();
        command_step(&mut core, 1, 10001, operation(60), ContextCommand::Quiesce);
        assert_eq!(core.state(), QueryContextState::Quiescing);
        if phase >= 1 {
            step(&mut core, 1, ContextInput::ContinueQuiesce);
            let release = command_step(&mut core, 1, 10002, operation(61), ContextCommand::Release);
            assert_eq!(core.state(), QueryContextState::Releasing);
            assert!(
                release
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, ContextEffect::ReleaseHost { .. }))
            );
        }
        if phase == 2 {
            step(&mut core, 1, ContextInput::HostReleased);
            assert_eq!(core.state(), QueryContextState::TerminalRetained);
        }
        let returned = step(&mut core, 31, ContextInput::MaterializationWithdrawn);
        assert_decision(&core, &returned, OperationOutcome::ContextTerminalReceipt);
        assert_eq!(core.current_credential(), None);
    }

    let mut upgraded = core();
    command_step(
        &mut upgraded,
        1,
        10003,
        operation(62),
        ContextCommand::Quiesce,
    );
    step(&mut upgraded, 30, ContextInput::Tick);
    assert_eq!(upgraded.state(), QueryContextState::Aborting);
    let returned = step(&mut upgraded, 31, ContextInput::MaterializationWithdrawn);
    assert_decision(&upgraded, &returned, OperationOutcome::LeaseExpired);
    assert_eq!(upgraded.current_credential(), None);
}

#[test]
fn an_establishing_or_really_materialized_active_context_rejects_withdrawal() {
    for materialized in [false, true] {
        let mut core = core();
        if materialized {
            let installed = step(
                &mut core,
                1,
                ContextInput::MaterializationFinished {
                    result: Ok(ContextMaterialized {
                        initial_credential_resource: 700,
                    }),
                },
            );
            assert_eq!(core.state(), QueryContextState::Active);
            assert_eq!(
                core.current_credential(),
                Some(CredentialState {
                    lease: core.establishment().semantics.credential_lease,
                    epoch: core.establishment().semantics.credential_epoch,
                    resource: 700,
                })
            );
            assert!(installed.effects.iter().any(|effect| matches!(
                effect,
                ContextEffect::EstablishmentDecided { decision, .. }
                    if decision.outcome == Ok(OperationOutcome::Accepted)
            )));
        }
        let original_credential = core.current_credential();
        if !materialized {
            assert_eq!(original_credential, None);
        }
        let original_state = core.state();
        let original_record = core.establishment();
        let original_deadline = core.next_deadline();
        let error = core
            .handle(
                at(2),
                ContextInput::MaterializationWithdrawn,
                &mut NoAdmission,
            )
            .err();
        assert_eq!(error, Some(CoreError::IllegalFact));
        assert_eq!(core.current_credential(), original_credential);
        assert_eq!(core.state(), original_state);
        assert_eq!(core.establishment(), original_record);
        assert_eq!(core.next_deadline(), original_deadline);
    }
}
