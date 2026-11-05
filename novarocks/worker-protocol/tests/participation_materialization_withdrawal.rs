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

//! Pure Participation owner decisions after an admitted M returns without invoking
//! materialization I/O. Host exit itself remains an explicit world fact.

use novarocks_control_runtime_core::identity::{
    Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, WorkId,
};
use novarocks_execution_contract::{
    AdmissionTicketId, ContentFingerprint, CredentialDomainId, CredentialEpoch, LeaseValidFor,
    OperationOutcome, ParticipationRef, ParticipationState, TaskOperationId,
};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
};
use novarocks_worker_protocol::{
    control::EstablishSemanticFacts,
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestReplyTag},
    domain::CredentialState,
    lease::{LeaseBounds, MonotonicInstant},
    participation::{
        CoreError, ParticipationCommand, ParticipationConfig, ParticipationCore,
        ParticipationEffect, ParticipationEstablishment, ParticipationInput,
        ParticipationMaterialized, ParticipationStep,
    },
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
fn core() -> ParticipationCore {
    let mut process = [1; 16];
    process[6] = 0x70;
    process[8] = 0x80;
    let identity = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(100, 101), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(process).unwrap(),
        BackendProcessId::try_from_bytes(process).unwrap(),
    );
    ParticipationCore::new(
        identity,
        Generation::new(1).unwrap(),
        ParticipationEstablishment {
            operation: operation(10),
            ticket: AdmissionTicketId::try_from_bytes([20; 16]).unwrap(),
            payload: PayloadId(30),
            semantics: EstablishSemanticFacts {
                catalog: ContentFingerprint::from_bytes([40; 16]),
                shared_filter: ContentFingerprint::from_bytes([41; 16]),
                query_options: ContentFingerprint::from_bytes([42; 16]),
                credential_domain_id: CredentialDomainId::new(43),
                credential_epoch: CredentialEpoch::FIRST,
                initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            },
            elected_at: at(0),
        },
        ParticipationConfig {
            max_tasks: 8,
            max_creation_waiters: 8,
            max_pending_requests: 8,
            max_preparing: 2,
            max_task_bytes: 1024,
            step_budget: ParticipationCore::MAX_STEP_WORK,
            lease_bounds: LeaseBounds::DEFAULT,
            metric_publish_interval: Duration::from_millis(100),
            request_horizon: novarocks_worker_protocol::lease::RequestHorizon::DEFAULT,
        },
    )
    .unwrap()
}

fn command_step(
    core: &mut ParticipationCore,
    now: u64,
    token: u64,
    operation: TaskOperationId,
    command: ParticipationCommand,
) -> ParticipationStep {
    let step = core
        .handle_request(
            at(now),
            RequestReplyTag {
                author: model_request_author(InputId::new(token).unwrap()),
                token: InputId::new(token).unwrap(),
                operation,
                deadline: at(10_000),
            },
            command,
            &mut NoAdmission,
        )
        .unwrap();
    assert!(step.charged_work() <= ParticipationCore::MAX_BODY_WORK);
    step
}

struct NoAdmission;
impl AdmissionPort for NoAdmission {
    fn try_reserve(&mut self, _: AdmissionNeed) -> Result<ReservationId, AdmissionRefusal> {
        panic!("withdrawal and closing decisions cannot acquire capacity");
    }
}
fn step(core: &mut ParticipationCore, now: u64, input: ParticipationInput) -> ParticipationStep {
    let step = core.handle(at(now), input, &mut NoAdmission).unwrap();
    assert!(step.charged_work() <= ParticipationCore::MAX_BODY_WORK);
    step
}
fn assert_decision(core: &ParticipationCore, step: &ParticipationStep, expected: OperationOutcome) {
    let decisions: Vec<_> = step
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ParticipationEffect::EstablishmentDecided {
                participation,
                close_epoch,
                decision,
            } => Some((*participation, *close_epoch, decision)),
            _ => None,
        })
        .collect();
    assert_eq!(decisions.len(), 1);
    let (participation, generation, decision) = decisions[0];
    assert_eq!(participation, core.identity());
    assert_eq!(generation, core.generation());
    assert_eq!(decision.state, core.state());
    assert_eq!(decision.outcome, Ok(expected));
    assert!(step.requests.is_empty());
    assert_ne!(core.state(), ParticipationState::Active);
}

#[test]
fn withdrawal_preserves_the_original_lease_expiry_instead_of_a_host_guessed_terminal_verdict() {
    for host_released in [false, true] {
        let mut core = core();
        assert_eq!(core.current_credential(), None);
        assert_eq!(core.next_deadline(), Some(at(30)));
        step(&mut core, 30, ParticipationInput::Tick);
        assert_eq!(core.state(), ParticipationState::Aborting);
        if host_released {
            // A pure environment fact, not a fabricated Native call or Work exit.
            step(&mut core, 30, ParticipationInput::HostReleased);
            assert_eq!(core.state(), ParticipationState::TerminalRetained);
        }
        let established = core.establishment();
        let returned = step(&mut core, 31, ParticipationInput::MaterializationWithdrawn);
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
        command_step(
            &mut core,
            1,
            10001,
            operation(60),
            ParticipationCommand::Quiesce,
        );
        assert_eq!(core.state(), ParticipationState::Quiescing);
        if phase >= 1 {
            step(&mut core, 1, ParticipationInput::ContinueQuiesce);
            let release = command_step(
                &mut core,
                1,
                10002,
                operation(61),
                ParticipationCommand::Release,
            );
            assert_eq!(core.state(), ParticipationState::Releasing);
            assert!(
                release
                    .effects
                    .iter()
                    .any(|effect| matches!(effect, ParticipationEffect::ReleaseHost { .. }))
            );
        }
        if phase == 2 {
            step(&mut core, 1, ParticipationInput::HostReleased);
            assert_eq!(core.state(), ParticipationState::TerminalRetained);
        }
        let returned = step(&mut core, 31, ParticipationInput::MaterializationWithdrawn);
        assert_decision(
            &core,
            &returned,
            OperationOutcome::ParticipationTerminalReceipt,
        );
        assert_eq!(core.current_credential(), None);
    }

    let mut upgraded = core();
    command_step(
        &mut upgraded,
        1,
        10003,
        operation(62),
        ParticipationCommand::Quiesce,
    );
    step(&mut upgraded, 30, ParticipationInput::Tick);
    assert_eq!(upgraded.state(), ParticipationState::Aborting);
    let returned = step(
        &mut upgraded,
        31,
        ParticipationInput::MaterializationWithdrawn,
    );
    assert_decision(&upgraded, &returned, OperationOutcome::LeaseExpired);
    assert_eq!(upgraded.current_credential(), None);
}

#[test]
fn an_establishing_or_really_materialized_active_participation_rejects_withdrawal() {
    for materialized in [false, true] {
        let mut core = core();
        if materialized {
            let installed = step(
                &mut core,
                1,
                ParticipationInput::MaterializationFinished {
                    result: Ok(ParticipationMaterialized {
                        initial_credential_resource: 700,
                    }),
                },
            );
            assert_eq!(core.state(), ParticipationState::Active);
            assert_eq!(
                core.current_credential(),
                Some(CredentialState {
                    credential_domain_id: core.establishment().semantics.credential_domain_id,
                    epoch: core.establishment().semantics.credential_epoch,
                    resource: 700,
                })
            );
            assert!(installed.effects.iter().any(|effect| matches!(
                effect,
                ParticipationEffect::EstablishmentDecided { decision, .. }
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
                ParticipationInput::MaterializationWithdrawn,
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
