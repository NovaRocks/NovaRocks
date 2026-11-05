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

//! Pure owner sequences. Resource IDs and comparison results are explicit
//! symbolic world facts; these tests do not execute opaque host comparisons.
use novarocks_control_runtime_core::identity::{
    Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, WorkId,
};
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, ContentFingerprint, CredentialDomainId, CredentialEpoch,
    DomainProgression, DomainVersion, LeaseValidFor, OperationOutcome, ParticipationRef,
    ParticipationState, TaskOperationId,
};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
};
use novarocks_worker_protocol::{
    control::EstablishSemanticFacts,
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestReplyTag},
    domain::{CredentialInput, CredentialState, ParticipationDomainInput, ParticipationDomains},
    lease::{LeaseBounds, MonotonicInstant},
    participation::{
        CoreError, EstablishCredentialComparison, EstablishReplayRequest, EstablishReplayResult,
        ParticipationCommand, ParticipationConfig, ParticipationCore, ParticipationEffect,
        ParticipationEstablishment, ParticipationInput, ParticipationMaterialized,
        ParticipationStep,
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
fn participation(query: i64) -> ParticipationRef {
    let mut bytes = [1; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(query, query + 1), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(bytes).unwrap(),
        BackendProcessId::try_from_bytes(bytes).unwrap(),
    )
}
fn original() -> ParticipationEstablishment {
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
    }
}
fn core() -> ParticipationCore {
    ParticipationCore::new(
        participation(100),
        Generation::new(1).unwrap(),
        original(),
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
        panic!("a pure Establish probe must not acquire capacity");
    }
}
fn step(core: &mut ParticipationCore, now: u64, input: ParticipationInput) -> ParticipationStep {
    let result = core.handle(at(now), input, &mut NoAdmission).unwrap();
    assert!(result.charged_work() <= ParticipationCore::MAX_BODY_WORK);
    result
}
fn request() -> EstablishReplayRequest {
    EstablishReplayRequest {
        request: RequestReplyTag {
            author: model_request_author(InputId::new(101).unwrap()),
            token: InputId::new(101).unwrap(),
            operation: original().operation,
            // The Root owns this deadline. A probe never settles this RPC.
            deadline: at(0),
        },
        ticket: original().ticket,
        payload: PayloadId(900),
        semantics: original().semantics,
    }
}
fn probe(
    core: &mut ParticipationCore,
    now: u64,
    request: EstablishReplayRequest,
    comparison: Option<EstablishCredentialComparison>,
) -> EstablishReplayResult {
    let participation = core.identity();
    let result = step(
        core,
        now,
        ParticipationInput::ProbeEstablish {
            participation,
            request,
            comparison,
        },
    );
    assert_eq!(result.records_visited, 1);
    assert!(result.requests.is_empty());
    assert_eq!(result.effects.len(), 1);
    match result.effects.into_iter().next().unwrap() {
        ParticipationEffect::EstablishProbed {
            participation: actual,
            request: correlated,
            result,
            decided_at,
        } => {
            assert_eq!(actual, participation);
            assert_eq!(correlated, request.request);
            assert_eq!(decided_at, at(now));
            result
        }
        _ => panic!("one original neutral probe result"),
    }
}
fn materialize(core: &mut ParticipationCore, resource: u64, now: u64) {
    step(
        core,
        now,
        ParticipationInput::MaterializationFinished {
            result: Ok(ParticipationMaterialized {
                initial_credential_resource: resource,
            }),
        },
    );
}
fn active(resource: u64) -> ParticipationCore {
    let mut core = core();
    materialize(&mut core, resource, 0);
    assert_eq!(core.state(), ParticipationState::Active);
    core
}
fn expected(resource: u64, epoch: CredentialEpoch) -> CredentialState {
    CredentialState {
        credential_domain_id: original().semantics.credential_domain_id,
        epoch,
        resource,
    }
}
fn comparison(
    current: CredentialState,
    same_material: bool,
) -> Option<EstablishCredentialComparison> {
    Some(EstablishCredentialComparison {
        current,
        same_material,
    })
}
fn update(
    core: &mut ParticipationCore,
    input: ParticipationDomainInput,
    now: u64,
    token: u64,
) -> DomainProgression {
    let initiated = command_step(
        core,
        now,
        token,
        operation(50),
        ParticipationCommand::UpdateParticipation {
            payload: PayloadId(901),
            update: input,
        },
    );
    let progression = initiated.effects.iter().find_map(|effect| match effect {
        ParticipationEffect::ApplyParticipationDomain { progression, .. } => Some(*progression),
        _ => None,
    });
    let progression = progression.expect("the symbolic domain change was admitted");
    step(
        core,
        now,
        ParticipationInput::ParticipationUpdateApplied {
            request: InputId::new(token).unwrap(),
            result: Ok(()),
        },
    );
    progression
}

#[test]
fn materialization_installs_the_initial_domains_only_after_real_success() {
    assert_eq!(ParticipationDomains::default().credential(), None);
    let mut core = core();
    assert_eq!(
        probe(&mut core, 0, request(), None),
        EstablishReplayResult::AwaitMaterialization
    );
    materialize(&mut core, 700, 1);
    let initial = expected(700, CredentialEpoch::FIRST);
    assert_eq!(
        probe(&mut core, 1, request(), None),
        EstablishReplayResult::CompareCredential { expected: initial }
    );
    let facts = original().semantics;
    for (token, input) in [
        ParticipationDomainInput::Catalog {
            version: DomainVersion::FIRST,
            fingerprint: facts.catalog,
        },
        ParticipationDomainInput::Filter {
            version: DomainVersion::FIRST,
            fingerprint: facts.shared_filter,
        },
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            update(&mut core, input, 1, 20_000 + token as u64),
            DomainProgression::Idempotent
        );
    }
    // This explicit symbolic fact says the canonical owner compared the
    // candidate material against precisely resource 700 at epoch FIRST.
    assert!(
        matches!(probe(&mut core, 1, request(), comparison(initial, true)), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::Idempotent)
    );
    assert_eq!(core.establishment(), original());
}

#[test]
fn every_immutable_establish_field_conflicts_but_a_new_carrier_is_valid() {
    let mut core = active(700);
    let current = expected(700, CredentialEpoch::FIRST);
    for changed in 0..8 {
        let mut candidate = request();
        match changed {
            0 => candidate.request.operation = operation(11),
            1 => candidate.ticket = AdmissionTicketId::try_from_bytes([21; 16]).unwrap(),
            2 => candidate.semantics.catalog = ContentFingerprint::from_bytes([99; 16]),
            3 => candidate.semantics.shared_filter = ContentFingerprint::from_bytes([99; 16]),
            4 => candidate.semantics.query_options = ContentFingerprint::from_bytes([99; 16]),
            5 => candidate.semantics.credential_domain_id = CredentialDomainId::new(44),
            6 => candidate.semantics.credential_epoch = CredentialEpoch::new(2).unwrap(),
            7 => {
                candidate.semantics.initial_lease_valid_for =
                    LeaseValidFor::new(Duration::from_secs(29)).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(probe(&mut core, 1, candidate, comparison(current, true)), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
        );
    }
    for payload in [PayloadId(900), PayloadId(901), original().payload] {
        let mut candidate = request();
        candidate.payload = payload;
        assert!(
            matches!(probe(&mut core, 1, candidate, comparison(current, true)), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::Idempotent)
        );
    }
    assert!(
        matches!(probe(&mut core, 1, request(), comparison(current, false)), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
    );
}

#[test]
fn an_initial_probe_waits_without_accepting_uninstalled_comparison_facts() {
    let mut core = core();
    assert_eq!(
        probe(
            &mut core,
            1,
            request(),
            comparison(expected(999, CredentialEpoch::FIRST), true)
        ),
        EstablishReplayResult::AwaitMaterialization
    );
    let mut changed = request();
    changed.ticket = AdmissionTicketId::try_from_bytes([21; 16]).unwrap();
    assert!(
        matches!(probe(&mut core, 1, changed, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
    );
    assert_eq!(core.state(), ParticipationState::Establishing);
}

#[test]
fn credential_rotation_requires_a_current_comparison_and_repeated_materialization_cannot_reset_it()
{
    let mut core = active(700);
    let old = expected(700, CredentialEpoch::FIRST);
    let epoch = CredentialEpoch::FIRST.next().unwrap();
    assert_eq!(
        update(
            &mut core,
            ParticipationDomainInput::Credential(CredentialInput {
                credential_domain_id: old.credential_domain_id,
                epoch,
                current_resource: old.resource,
                replacement_resource: 701,
                same_material: false,
            }),
            1,
            30_000,
        ),
        DomainProgression::Apply
    );
    materialize(&mut core, 999, 1);
    let current = expected(701, epoch);
    for stale_material in [false, true] {
        assert_eq!(
            probe(&mut core, 1, request(), comparison(old, stale_material)),
            EstablishReplayResult::CompareCredential { expected: current }
        );
    }
    assert_eq!(
        probe(&mut core, 1, request(), None),
        EstablishReplayResult::CompareCredential { expected: current }
    );
    // Even equal material cannot make the old initial epoch current again.
    assert!(
        matches!(probe(&mut core, 1, request(), comparison(current, true)), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
    );
    assert_eq!(core.establishment(), original());
}

#[test]
fn matching_epoch_or_material_does_not_authorize_a_different_resource_or_lease() {
    let mut core = active(700);
    let current = expected(700, CredentialEpoch::FIRST);
    for wrong in [
        CredentialState {
            resource: 701,
            ..current
        },
        CredentialState {
            epoch: CredentialEpoch::new(2).unwrap(),
            ..current
        },
        CredentialState {
            credential_domain_id: CredentialDomainId::new(44),
            ..current
        },
    ] {
        for same_material in [false, true] {
            assert_eq!(
                probe(&mut core, 1, request(), comparison(wrong, same_material)),
                EstablishReplayResult::CompareCredential { expected: current }
            );
        }
    }
}

#[test]
fn closed_state_priority_does_not_reinterpret_payload_conflicts_or_lease_expiry() {
    let mut mismatched = request();
    mismatched.ticket = AdmissionTicketId::try_from_bytes([21; 16]).unwrap();
    mismatched.semantics.catalog = ContentFingerprint::from_bytes([99; 16]);
    for started in [false, true] {
        let mut core = if started { active(700) } else { core() };
        command_step(
            &mut core,
            1,
            10002,
            operation(60),
            ParticipationCommand::Quiesce,
        );
        materialize(&mut core, 999, 1);
        assert_eq!(core.state(), ParticipationState::Quiescing);
        assert!(
            matches!(probe(&mut core, 1, mismatched, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationTerminalReceipt)
        );
    }
    let mut unmaterialized = core();
    command_step(
        &mut unmaterialized,
        1,
        10003,
        operation(62),
        ParticipationCommand::Abort {
            cause: AbortCause::QueryFailed,
        },
    );
    materialize(&mut unmaterialized, 999, 1);
    assert!(
        matches!(probe(&mut unmaterialized, 1, mismatched, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::InvalidStateOrRequest)
    );
    let mut core = active(700);
    step(&mut core, 30, ParticipationInput::Tick);
    assert_eq!(core.state(), ParticipationState::Aborting);
    assert!(
        matches!(probe(&mut core, 30, mismatched, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::InvalidStateOrRequest)
    );
    // Normal release reaches Releasing through the actual Quiesce protocol,
    // independently of the Aborting state above.
    let mut core = active(700);
    command_step(
        &mut core,
        1,
        10004,
        operation(60),
        ParticipationCommand::Quiesce,
    );
    step(&mut core, 1, ParticipationInput::ContinueQuiesce);
    command_step(
        &mut core,
        1,
        10005,
        operation(61),
        ParticipationCommand::Release,
    );
    assert_eq!(core.state(), ParticipationState::Releasing);
    assert!(
        matches!(probe(&mut core, 1, mismatched, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::InvalidStateOrRequest)
    );
    step(&mut core, 1, ParticipationInput::HostReleased);
    assert_eq!(core.state(), ParticipationState::TerminalRetained);
    materialize(&mut core, 999, 1);
    assert!(
        matches!(probe(&mut core, 1, mismatched, None), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationTerminalReceipt)
    );
}

#[test]
fn probe_identity_and_time_are_checked_without_creating_root_rpc_state() {
    let mut core = active(700);
    let error = core
        .handle(
            at(31),
            ParticipationInput::ProbeEstablish {
                participation: participation(200),
                request: request(),
                comparison: None,
            },
            &mut NoAdmission,
        )
        .unwrap_err();
    assert_eq!(error, CoreError::WrongIdentity);
    assert_eq!(core.state(), ParticipationState::Active);
    assert_eq!(
        probe(&mut core, 1, request(), None),
        EstablishReplayResult::CompareCredential {
            expected: expected(700, CredentialEpoch::FIRST)
        }
    );
    let error = core
        .handle(
            at(0),
            ParticipationInput::ProbeEstablish {
                participation: core.identity(),
                request: request(),
                comparison: None,
            },
            &mut NoAdmission,
        )
        .unwrap_err();
    assert_eq!(error, CoreError::TimeRegressed);
    // Repeating the same original Root token is a new observation, not a
    // Participation handle_request join or a duplicate-token failure.
    assert_eq!(
        probe(&mut core, 1, request(), None),
        EstablishReplayResult::CompareCredential {
            expected: expected(700, CredentialEpoch::FIRST)
        }
    );
}
