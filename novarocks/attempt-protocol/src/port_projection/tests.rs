// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::*;
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptActionId, AttemptActivationIdentity, AttemptRequestToken,
    AttemptToLogical, AttemptToLogicalBody, ExecutionFailure, SelectedTaskTermination,
    logical_port as lp,
};
use novarocks_execution_contract::{
    ParticipationReceipt, ParticipationState, TaskFailure, TaskIdentity, TerminationDetail,
};
use std::num::NonZeroU64;
fn action(n: u64) -> AttemptActionId {
    AttemptActionId::new(NonZeroU64::new(n).unwrap())
}
fn identity() -> (AttemptActivationIdentity, ParticipationRef, TaskOperationId) {
    let mut execution = crate::test_support::execution_with_strict_input();
    let p = *execution.graph().participations().next().unwrap();
    (
        AttemptActivationIdentity::from_parts(72, p.query_execution_id(), 9),
        p,
        execution.issue_operation_id(),
    )
}
fn abort(
    settlement: AbortIssueSettlement,
    receipt_generation: Option<AttemptActionId>,
) -> AttemptToLogical<AttemptFailure> {
    let (a, p, op) = identity();
    AttemptToLogical {
        activation: a,
        action: action(99),
        body: AttemptToLogicalBody::AbortSettlement {
            abort_action: action(10),
            generation: action(30),
            receipt_generation,
            participation: p,
            operation_id: op,
            settlement,
        },
    }
}
fn receipt(
    message: &AttemptToLogical<AttemptFailure>,
    outcome: OperationOutcome,
    state: ParticipationState,
) -> AbortReceiptFact {
    let AttemptToLogicalBody::AbortSettlement {
        participation,
        operation_id,
        receipt_generation,
        ..
    } = message.body
    else {
        unreachable!()
    };
    AbortReceiptFact {
        generation: receipt_generation.unwrap(),
        operation_id,
        kind: OperationKind::AbortParticipation,
        outcome,
        receipt: Some(ParticipationReceipt::new(participation, state)),
    }
}
#[test]
fn exact_older_physical_worker_receipt_preserves_logical_abort_and_all_closing_states() {
    for outcome in [
        OperationOutcome::Accepted,
        OperationOutcome::Idempotent,
        OperationOutcome::ParticipationTerminalReceipt,
        OperationOutcome::LeaseExpired,
        OperationOutcome::Gone,
    ] {
        for state in [
            ParticipationState::Aborting,
            ParticipationState::Releasing,
            ParticipationState::TerminalRetained,
            ParticipationState::Gone,
        ] {
            let message = abort(AbortIssueSettlement::Worker(outcome), Some(action(20)));
            let original = receipt(&message, outcome, state);
            let expected = lp::AbortWorkerClosure::try_from(state).unwrap();
            let mut resolved = 0;
            let projected = project_abort_settlement(&message, &mut |p, op, generation| {
                resolved += 1;
                assert_eq!(generation, action(20));
                assert_eq!(p, original.receipt.as_ref().unwrap().participation());
                assert_eq!(op, original.operation_id);
                Some(original.clone())
            })
            .unwrap();
            assert!(
                matches!(projected,lp::AttemptToLogicalBody::AbortSettlement{abort_action,operation_id,settlement:lp::AbortIssueSettlement::Worker{outcome:actual,closure},..}if abort_action==action(10)&&operation_id==original.operation_id&&actual==outcome&&closure==expected)
            );
            assert_eq!(resolved, 1);
        }
    }
}
#[test]
fn every_original_abort_receipt_axis_is_checked_before_positive_projection() {
    for fault in 0..8 {
        let message = abort(
            AbortIssueSettlement::Worker(OperationOutcome::Accepted),
            Some(action(20)),
        );
        let mut original = receipt(
            &message,
            OperationOutcome::Accepted,
            ParticipationState::Aborting,
        );
        let expected = match fault {
            0 => {
                original.generation = action(21);
                AbortProjectionError::WrongReceiptGeneration
            }
            1 => {
                original.operation_id =
                    crate::ids::OperationIds::new(crate::ids::IdSeed::new(999, 999)).issue();
                AbortProjectionError::WrongOperation
            }
            2 => {
                original.kind = OperationKind::UpdateParticipation;
                AbortProjectionError::WrongKind
            }
            3 => {
                original.outcome = OperationOutcome::Idempotent;
                AbortProjectionError::WrongOutcome
            }
            4 => {
                original.receipt = None;
                AbortProjectionError::MissingParticipationReceipt
            }
            5 => {
                let (_, p, _) = identity();
                let foreign = ParticipationRef::new(
                    p.query_execution_id(),
                    p.frontend_process_id(),
                    crate::test_support::backend(999),
                );
                original.receipt = Some(ParticipationReceipt::new(
                    foreign,
                    ParticipationState::Aborting,
                ));
                AbortProjectionError::WrongParticipation
            }
            6 => {
                original.receipt = Some(ParticipationReceipt::new(
                    original.receipt.as_ref().unwrap().participation(),
                    ParticipationState::Active,
                ));
                AbortProjectionError::NonClosingReceipt
            }
            7 => AbortProjectionError::MissingOriginalReceipt,
            _ => unreachable!(),
        };
        assert_eq!(
            project_abort_settlement(&message, &mut |_, _, _| if fault == 7 {
                None
            } else {
                Some(original.clone())
            })
            .unwrap_err(),
            expected
        );
    }
    let message = abort(
        AbortIssueSettlement::Worker(OperationOutcome::OwnerFailed),
        Some(action(20)),
    );
    let original = receipt(
        &message,
        OperationOutcome::OwnerFailed,
        ParticipationState::Aborting,
    );
    assert_eq!(
        project_abort_settlement(&message, &mut |_, _, _| Some(original.clone())).unwrap_err(),
        AbortProjectionError::NonClosingOutcome
    );
}
#[test]
fn local_abort_outcomes_never_resolve_a_worker_or_gain_positive_closure() {
    for input in [
        AbortIssueSettlement::DefinitelyUnsent,
        AbortIssueSettlement::TransportUnknown,
        AbortIssueSettlement::FailedClosed,
    ] {
        let message = abort(input, None);
        let projected = project_abort_settlement(&message, &mut |_, _, _| {
            panic!("local outcome has no Worker receipt")
        })
        .unwrap();
        assert!(matches!(
            (input, projected),
            (
                AbortIssueSettlement::DefinitelyUnsent,
                lp::AttemptToLogicalBody::AbortSettlement {
                    settlement: lp::AbortIssueSettlement::DefinitelyUnsent,
                    ..
                }
            ) | (
                AbortIssueSettlement::TransportUnknown,
                lp::AttemptToLogicalBody::AbortSettlement {
                    settlement: lp::AbortIssueSettlement::TransportUnknown,
                    ..
                }
            ) | (
                AbortIssueSettlement::FailedClosed,
                lp::AttemptToLogicalBody::AbortSettlement {
                    settlement: lp::AbortIssueSettlement::FailedClosed,
                    ..
                }
            )
        ));
        let with_receipt = abort(input, Some(action(20)));
        assert_eq!(
            project_abort_settlement(&with_receipt, &mut |_, _, _| panic!(
                "unexpected receipt must not be resolved"
            ))
            .unwrap_err(),
            AbortProjectionError::UnexpectedReceiptGeneration
        );
    }
}
fn roundtrip_failure(original: ExecutionFailure) {
    let (a, _, _) = identity();
    let message = lp::LogicalToAttempt {
        activation: a,
        body: lp::LogicalToAttemptBody::Stop {
            reason: novarocks_coordination_contract::AttemptStopReason::LogicalExecutionFailed,
            failure: original.clone(),
        },
    };
    let projected = project_logical_message(&message);
    let novarocks_coordination_contract::LogicalToAttemptBody::Stop { failure, .. } =
        projected.body
    else {
        unreachable!()
    };
    assert_eq!(
        project_failure(&failure, &mut |_| panic!(
            "passive logical cause must not mint or resolve a HostRef"
        ))
        .unwrap(),
        original
    );
    let returned = AttemptToLogical {
        activation: a,
        action: action(11),
        body: AttemptToLogicalBody::AttemptTerminal(
            novarocks_coordination_contract::AttemptTerminal::Failed(failure),
        ),
    };
    let passive = project_attempt_message(
        &returned,
        &mut |_| panic!("original passive failure"),
        &mut |_, _| None,
        &mut |_, _, _| None,
    )
    .unwrap();
    assert_eq!(passive.activation, a);
    assert_eq!(passive.action, action(11));
    assert!(
        matches!(passive.body,lp::AttemptToLogicalBody::AttemptTerminal(lp::AttemptTerminal::Failed(actual))if actual==original)
    );
}
#[test]
fn logical_selected_task_input_roundtrips_without_host_reference_or_fact_downgrade() {
    let (a, p, _) = identity();
    let task = TaskIdentity::new(
        a.execution(),
        novarocks_types::StageId::new(1).unwrap(),
        novarocks_types::TaskId::new(1).unwrap(),
        p.backend_process_id(),
    );
    let selected = SelectedTaskTermination::new(
        task,
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("original selected Task data-plane contract cause"),
        )),
    );
    let original = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    assert!(matches!(original.site(),ExecutionFailureSite::Task{identity,..}if identity==task));
    roundtrip_failure(ExecutionFailure::from_input(original));
}
#[test]
fn logical_replacement_witness_roundtrips_with_same_definitive_input() {
    let (_, p, op) = identity();
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Invariant,
            novarocks_coordination_contract::TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating(
            "original CreateTask identity mismatch with independent replacement",
        ),
        ExecutionFailureSite::Operation {
            participation: p,
            operation_id: op,
            kind: OperationKind::CreateTask,
        },
    );
    let witness = PreReadyProcessReplacementWitness::try_new(
        &input,
        p,
        op,
        crate::test_support::backend(999),
    )
    .unwrap();
    let mut original = ExecutionFailure::from_input(input);
    original.attach_pre_ready_replacement(witness).unwrap();
    roundtrip_failure(original);
}
#[test]
fn reverse_budget_and_settlement_failures_keep_exact_passive_author_and_token() {
    let (a, _, _) = identity();
    let token = AttemptRequestToken::new(NonZeroU64::new(19).unwrap());
    let original = ExecutionFailure::from_input(failure_input(
        QueryFailureReason::TransportCapacity,
        "original independent Establish budget refusal",
        ExecutionFailureSite::Coordinator,
    ));
    let passive = lp::LogicalToAttempt {
        activation: a,
        body: lp::LogicalToAttemptBody::AuthorizationReply {
            token,
            decision: Err(lp::AttemptAuthorizationFailure::EstablishBudgetExhausted(
                original.clone(),
            )),
        },
    };
    let reverse = project_logical_message(&passive);
    assert!(
        matches!(reverse.body,novarocks_coordination_contract::LogicalToAttemptBody::AuthorizationReply{token:actual,decision:Err(novarocks_coordination_contract::AttemptAuthorizationFailure::EstablishBudgetExhausted(AttemptFailure::Logical(error)))}if actual==token&&error==original)
    );
    let authorization =
        novarocks_coordination_contract::AttemptAuthorizationRef::new(NonZeroU64::new(23).unwrap());
    let passive = lp::LogicalToAttempt {
        activation: a,
        body: lp::LogicalToAttemptBody::EstablishSettlementReply {
            token,
            authorization,
            result: Err(original.clone()),
        },
    };
    let reverse = project_logical_message(&passive);
    assert_eq!(reverse.activation, a);
    let novarocks_coordination_contract::LogicalToAttemptBody::EstablishSettlementReply {
        token: actual_token,
        authorization: actual_authorization,
        result: Err(AttemptFailure::Logical(error)),
    } = reverse.body
    else {
        panic!("same original settlement rejection");
    };
    assert_eq!(actual_token, token);
    assert_eq!(actual_authorization, authorization);
    assert_eq!(error, original);
}
#[test]
fn missing_original_establish_and_obsolete_detach_are_explicit_not_guessed() {
    let (a, p, op) = identity();
    let token = AttemptRequestToken::new(NonZeroU64::new(19).unwrap());
    let message = AttemptToLogical {
        activation: a,
        action: action(12),
        body: AttemptToLogicalBody::AuthorizationRequest {
            token,
            request: novarocks_coordination_contract::AttemptAuthorization::Establish {
                participation: p,
                operation_id: op,
            },
        },
    };
    assert_eq!(
        project_attempt_message(&message, &mut |_| None, &mut |_, _| None, &mut |_, _, _| {
            None
        })
        .unwrap_err(),
        PortProjectionError::MissingOriginalEstablish {
            participation: p,
            operation_id: op
        }
    );
    let detached = AttemptToLogical {
        activation: a,
        action: action(13),
        body: AttemptToLogicalBody::DetachEstablish {
            token,
            authorization: novarocks_coordination_contract::AttemptAuthorizationRef::new(
                NonZeroU64::new(17).unwrap(),
            ),
            participation: p,
            operation_id: op,
        },
    };
    assert_eq!(
        project_attempt_message(
            &detached,
            &mut |_| None,
            &mut |_, _| None,
            &mut |_, _, _| None
        )
        .unwrap_err(),
        PortProjectionError::UnsupportedDetachEstablish
    );
}

#[test]
fn authoritative_root_projection_retains_complete_selected_task_beside_original_status() {
    let (a, p, _) = identity();
    let task = TaskIdentity::new(
        a.execution(),
        novarocks_types::StageId::new(1).unwrap(),
        novarocks_types::TaskId::new(1).unwrap(),
        p.backend_process_id(),
    );
    let selected = SelectedTaskTermination::new(
        task,
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("selected root producer cause"),
        )),
    );
    let status = novarocks_execution_contract::TaskStatus::try_new(
        task,
        novarocks_execution_contract::TaskStatusVersion::new(4).unwrap(),
        novarocks_execution_contract::TaskState::Failed,
        Some(selected.detail().clone()),
        novarocks_execution_contract::TaskOutputFacts::new(false),
    )
    .unwrap();
    let original = Box::new(novarocks_coordination_contract::AttemptRootProjection {
        status,
        failure: novarocks_coordination_contract::AttemptRootFailure::Authoritative(selected),
    });
    let message = AttemptToLogical {
        activation: a,
        action: action(50),
        body: AttemptToLogicalBody::RootStatus(original.clone()),
    };
    let actual = project_attempt_message(
        &message,
        &mut |_| panic!("selected task has no host reference"),
        &mut |_, _| None,
        &mut |_, _, _| None,
    )
    .unwrap();
    assert!(
        matches!(actual.body,lp::AttemptToLogicalBody::RootStatus(projected)if projected==original)
    );
}

#[test]
fn establish_projection_requires_the_same_original_full_immutable_request_facts() {
    use novarocks_execution_contract::{
        AdmissionTicketId, CodecOwnedContent, ConfidentialContent, ContentFingerprint,
        CredentialDomainId, CredentialEpoch, CredentialUpdate, EstablishParticipation,
        LeaseValidFor,
    };
    use std::{sync::Arc, time::Duration};
    #[derive(Debug)]
    struct Metadata(ContentFingerprint);
    impl CodecOwnedContent for Metadata {
        fn fingerprint(&self) -> ContentFingerprint {
            self.0
        }
        fn encoded_len(&self) -> usize {
            1
        }
    }
    struct Secret;
    impl ConfidentialContent for Secret {
        fn encoded_len(&self) -> usize {
            1
        }
        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }
    let (a, p, op) = identity();
    let ticket = AdmissionTicketId::try_from_bytes([7; 16]).unwrap();
    let content = || {
        Arc::new(Metadata(ContentFingerprint::from_bytes([9; 16]))) as Arc<dyn CodecOwnedContent>
    };
    let original = EstablishParticipation::new(
        op,
        p,
        ticket,
        content(),
        content(),
        content(),
        CredentialUpdate::new(
            CredentialDomainId::new(3),
            CredentialEpoch::new(1).unwrap(),
            Arc::new(Secret),
        ),
        LeaseValidFor::new(Duration::from_secs(1)).unwrap(),
    );
    let facts = EstablishAuthorizationFacts {
        participation: p,
        operation_id: op,
        ticket_id: ticket,
        semantics: original.semantic_identity(),
        native_compatibility_id: novarocks_types::NativeCompatibilityId::new([11; 32]),
    };
    let token = AttemptRequestToken::new(NonZeroU64::new(19).unwrap());
    let message = AttemptToLogical {
        activation: a,
        action: action(70),
        body: AttemptToLogicalBody::AuthorizationRequest {
            token,
            request: novarocks_coordination_contract::AttemptAuthorization::Establish {
                participation: p,
                operation_id: op,
            },
        },
    };
    let actual = project_attempt_message(
        &message,
        &mut |_| None,
        &mut |part, operation| {
            assert_eq!(part, p);
            assert_eq!(operation, op);
            Some(facts)
        },
        &mut |_, _, _| None,
    )
    .unwrap();
    assert!(
        matches!(actual.body,lp::AttemptToLogicalBody::AuthorizationRequest{token:t,request:lp::AttemptAuthorization::Establish{participation,operation_id,ticket_id,semantics,native_compatibility_id}}if t==token&&participation==p&&operation_id==op&&ticket_id==ticket&&semantics==original.semantic_identity()&&native_compatibility_id==facts.native_compatibility_id)
    );
    let mut wrong = facts;
    wrong.operation_id = crate::ids::OperationIds::new(crate::ids::IdSeed::new(999, 999)).issue();
    assert_eq!(
        project_attempt_message(
            &message,
            &mut |_| None,
            &mut |_, _| Some(wrong),
            &mut |_, _, _| None
        )
        .unwrap_err(),
        PortProjectionError::MismatchedOriginalEstablish
    );
}
