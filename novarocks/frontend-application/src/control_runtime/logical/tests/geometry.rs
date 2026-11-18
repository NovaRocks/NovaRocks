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

//! Successor fan-in uses the new original roster on the same prepaid geometry.
use super::progress::{assert_actual_exit, completed, finish_with_history, until};
use crate::fixtures::{
    AttemptCase, AttemptScript, FrontendFixture, LogicalCase, LogicalScript, ProgressBudget,
    ProgressSelection, SuccessorScript, participations,
};
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptConvergenceClosure, AttemptInheritedAdmission,
    AttemptIssueSettlement, AttemptParticipationConvergence, AttemptRequestToken, ExecutionFailure,
    ExecutionFailureInput, ExecutionFailureSite, IdSeed, OperationIds, QueryFailureReason,
    RecoveryMode,
    logical_port::{
        AttemptAuthorization, AttemptTerminal, AttemptToLogicalBody, LogicalToAttemptBody,
    },
};
use novarocks_execution_contract::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialDomainId,
    CredentialEpoch, CredentialUpdate, EstablishParticipation, LeaseValidFor, OperationOutcome,
    ParticipationRef, SafeDetail, TaskOperationId, failure::FailureProjection,
};
use novarocks_logical_execution_protocol::effect::{
    ConsumerEffect, LogicalEffectBody, NativeEffect,
};
use novarocks_query_application::coordination::executor::GovernanceObligationState;
use novarocks_types::{AttemptId, QueryExecutionId};
use std::{num::NonZeroU64, sync::Arc, time::Duration};

fn original_failure(participation: ParticipationRef, detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Backend {
            process: participation.backend_process_id(),
        },
    ))
}
fn remote_positive(roster: &[ParticipationRef], established: bool) -> AttemptToLogicalBody {
    AttemptToLogicalBody::ConvergenceProgress {
        local_exited: false,
        closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(
            roster
                .iter()
                .copied()
                .map(|p| {
                    (
                        p,
                        if established {
                            AttemptParticipationConvergence::WorkerStoppedAndParticipationFenced
                        } else {
                            AttemptParticipationConvergence::NeverEstablished
                        },
                    )
                })
                .collect(),
        ))),
        lifecycle_runtime_filters: None,
    }
}
async fn settle_script_attempt(
    fixture: &mut FrontendFixture,
    logical: &LogicalCase,
    attempt: &AttemptCase,
    roster: &[ParticipationRef],
    established: bool,
) {
    // The script models the remote Worker closure separately from the local
    // attempt lifetime. Neither this Evidence nor RunCompleted is local exit.
    let remote = attempt.script.enqueue(remote_positive(roster, established));
    logical.script.run.open();
    let remote_settled = until(fixture, logical, |actual| {
        let original = actual
            .governor
            .iter()
            .filter(|row| row.activation == attempt.activation)
            .collect::<Vec<_>>();
        attempt.script.published().contains(&remote)
            && !original.is_empty()
            && original
                .iter()
                .all(|row| row.state == GovernanceObligationState::Resolved)
            && actual.native_cleanup.iter().any(|effect| {
                matches!(effect,
                NativeEffect::FinishResources { activation } if *activation == attempt.activation)
            })
    })
    .await;
    assert!(!remote_settled.effects.iter().any(|effect| matches!(effect,
        LogicalEffectBody::AttemptLifecycleDiagnostic(diagnostic) if diagnostic.activation == attempt.activation)),
        "remote closure and the original Run completion must not manufacture the independent local exit");
    // This is an explicit fact from the approved AP backend model after its
    // scripted local work settles. It is not inferred from Scope closure or
    // from the Native Run result; actual AP HostExit is checked after finish.
    let local = attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: None,
            lifecycle_runtime_filters: None,
        });
    until(fixture, logical, |actual| attempt.script.published().contains(&local)
        && actual.effects.iter().any(|effect| matches!(effect,
            LogicalEffectBody::AttemptLifecycleDiagnostic(diagnostic) if diagnostic.activation == attempt.activation))).await;
}

fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn operation(tag: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(tag, 701)).issue()
}
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
fn original_establish(grant: AttemptInheritedAdmission, tag: u8) -> AttemptAuthorization {
    let content = |offset| {
        Arc::new(Metadata(ContentFingerprint::from_bytes([tag + offset; 16])))
            as Arc<dyn CodecOwnedContent>
    };
    let original = EstablishParticipation::new(
        operation(9000 + u128::from(tag)),
        grant.participation,
        grant.receipt.ticket_id(),
        content(0),
        content(1),
        content(2),
        CredentialUpdate::new(
            CredentialDomainId::new(u64::from(tag)),
            CredentialEpoch::new(1).unwrap(),
            Arc::new(Secret),
        ),
        LeaseValidFor::new(Duration::from_secs(1)).unwrap(),
    );
    AttemptAuthorization::Establish {
        participation: grant.participation,
        operation_id: original.envelope().operation_id(),
        ticket_id: original.admission_ticket_id(),
        semantics: original.semantic_identity(),
        native_compatibility_id: LogicalScript::native_compatibility_id(),
    }
}
async fn establish_all(
    fixture: &mut FrontendFixture,
    logical: &LogicalCase,
    attempt: &AttemptCase,
    grants: &[AttemptInheritedAdmission],
) {
    for (index, grant) in grants.iter().copied().enumerate() {
        let request = original_establish(grant, 40 + u8::try_from(index).unwrap());
        let AttemptAuthorization::Establish { operation_id, .. } = request else {
            unreachable!()
        };
        let asked = token(9000 + index as u64 * 3);
        attempt
            .script
            .enqueue(AttemptToLogicalBody::AuthorizationRequest {
                token: asked,
                request,
            });
        until(fixture, logical, |_| attempt.script.received().iter().any(|message|
            matches!(message.body, LogicalToAttemptBody::AuthorizationReply { token: actual, .. } if actual == asked))).await;
        let authorization = attempt
            .script
            .received()
            .into_iter()
            .find_map(|message| match message.body {
                LogicalToAttemptBody::AuthorizationReply {
                    token: actual,
                    decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(authorization)),
                } if actual == asked => Some(authorization),
                _ => None,
            })
            .expect("the exact successor ticket must authorize its own participation");
        for (offset, settlement) in [
            (1, AttemptIssueSettlement::TransportOwned),
            (
                2,
                AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
            ),
        ] {
            let asked = token(9000 + index as u64 * 3 + offset);
            attempt
                .script
                .enqueue(AttemptToLogicalBody::EstablishSettlement {
                    token: asked,
                    authorization,
                    participation: grant.participation,
                    operation_id,
                    settlement,
                });
            until(fixture, logical, |_| attempt.script.received().iter().any(|message|
                matches!(message.body, LogicalToAttemptBody::EstablishSettlementReply { token: actual, .. } if actual == asked))).await;
            assert!(attempt.script.received().iter().any(|message| matches!(&message.body,
                LogicalToAttemptBody::EstablishSettlementReply { token: actual, authorization: exact, result: Ok(()) }
                if *actual == asked && *exact == authorization)),
                "each original successor authorization has its own successful exact settlement reply");
        }
    }
}

#[tokio::test]
async fn a_larger_qualified_successor_delivers_every_original_member_and_late_predecessor_facts_stay_local()
 {
    let mut fixture = FrontendFixture::new();
    let script = LogicalScript::default();
    let mut start = LogicalScript::start(8301, 1, 2);
    start.policy.recovery = RecoveryMode::RestartAttemptBeforeVisibility;
    start.budgets.replacement_reservation_valid_for = Some(Duration::from_secs(1));
    let replacement = QueryExecutionId::new(start.identity, AttemptId::new(2).unwrap()).unwrap();
    let replacement_roster = participations(replacement, 3);
    script
        .successors
        .lock()
        .unwrap()
        .push_back(SuccessorScript {
            execution: replacement,
            participations: replacement_roster.clone(),
        });
    let logical = fixture.install_logical(start, script);
    let old = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let old_roster = logical.start.first_attempt.participations.clone();
    let mailbox = fixture.observe(&logical).geometry_mailbox;
    let first_failure = original_failure(old_roster[0], "original predecessor failure");
    old.script.enqueue(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(first_failure.clone()),
    ));
    let prepared = until(&mut fixture, &logical, |actual| {
        actual.effects.iter().any(|effect|
        matches!(effect, LogicalEffectBody::Native(NativeEffect::ActivateAttempt { activation, .. })
            if activation.execution() == replacement))
    })
    .await;
    let actual_successor = logical
        .script
        .successor_activation()
        .expect("the real executed successor Activate call must expose its actual identity");
    assert_eq!(actual_successor.execution(), replacement);
    assert_ne!(actual_successor, old.activation);
    let (activation, grants) = prepared
        .effects
        .iter()
        .find_map(|effect| match effect {
            LogicalEffectBody::Native(NativeEffect::ActivateAttempt {
                activation,
                inherited_admissions,
            }) if activation.execution() == replacement => {
                Some((*activation, inherited_admissions.clone()))
            }
            _ => None,
        })
        .expect("the actual Core-issued successor activation and inherited tickets");
    assert_eq!(
        activation, actual_successor,
        "the inherited-ticket packet and executed successor call must name the same exact activation"
    );
    assert_eq!(grants.len(), 3);
    assert!(replacement_roster.iter().all(|member| {
        grants
            .iter()
            .filter(|grant| grant.participation == *member)
            .count()
            == 1
    }));
    let current = fixture
        .activate_attempt(&logical, activation, AttemptScript::default())
        .await;
    assert_eq!(
        fixture.observe(&logical).geometry_mailbox,
        mailbox,
        "the already paid logical geometry must not be resized from first-roster fan-in"
    );
    let late = old.script.enqueue(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Failed(original_failure(
            old_roster[0],
            "late predecessor must not become successor failure",
        )),
    ));
    for _ in 0..64 {
        fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
        if old.script.published().contains(&late) {
            break;
        }
    }
    assert!(old.script.published().contains(&late));
    settle_script_attempt(&mut fixture, &logical, &old, &old_roster, false).await;
    assert!(
        !completed(&fixture.observe(&logical)),
        "the old attempt's real cleanup cannot satisfy the successor's required Establish ledger"
    );
    establish_all(&mut fixture, &logical, &current, &grants).await;
    current
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    logical.script.run.open();
    let actual = until(&mut fixture, &logical, completed).await;
    assert!(!actual.failed);
    assert_eq!(
        actual
            .effects
            .iter()
            .filter(|effect| matches!(
                effect,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
            ))
            .count(),
        1
    );
    assert!(!actual.effects.iter().any(|effect| matches!(
        effect,
        LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(_))
    )));
    assert!(
        current
            .script
            .received()
            .iter()
            .all(|message| message.activation == activation)
    );
    settle_script_attempt(&mut fixture, &logical, &current, &replacement_roster, true).await;
    let final_observation = until(&mut fixture, &logical, |actual| {
        actual.effects.iter().any(|effect| {
            matches!(effect, LogicalEffectBody::AttemptLifecycleDiagnostic(diagnostic)
            if diagnostic.activation == old.activation)
        })
    })
    .await;
    let old_diagnostic = final_observation
        .effects
        .iter()
        .find_map(|effect| match effect {
            LogicalEffectBody::AttemptLifecycleDiagnostic(diagnostic)
                if diagnostic.activation == old.activation =>
            {
                Some(diagnostic)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        old_diagnostic.accepted_failure.as_ref(),
        Some(first_failure.input()),
        "the actual old-attempt diagnostic must retain its first typed cause, not the late predecessor error"
    );
    let history = finish_with_history(&mut fixture).await;
    assert_actual_exit(&fixture, &logical, &old, &history);
    assert_actual_exit(&fixture, &logical, &current, &history);
    assert!(
        history.actor_targets().len() >= 3,
        "one logical actor and both real original attempt actors must be in independent history"
    );
}

#[tokio::test]
async fn fixed_logical_mailbox_capacity_is_independent_of_the_initial_participation_fan_in() {
    let mut fixture = FrontendFixture::new();
    let mut first_start = LogicalScript::start(8302, 1, 2);
    first_start.policy.recovery = RecoveryMode::NoRecovery;
    first_start.budgets.replacement_reservation_valid_for = None;
    let mut larger_start = LogicalScript::start(8303, 4, 2);
    larger_start.policy.recovery = RecoveryMode::NoRecovery;
    larger_start.budgets.replacement_reservation_valid_for = None;
    let first = fixture.install_logical(first_start, LogicalScript::default());
    let larger = fixture.install_logical(larger_start, LogicalScript::default());
    let first_attempt = fixture
        .activate_attempt(
            &first,
            first.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    let larger_attempt = fixture
        .activate_attempt(
            &larger,
            larger.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    assert_eq!(
        fixture.observe(&first).geometry_mailbox,
        fixture.observe(&larger).geometry_mailbox
    );
    for (logical, attempt) in [(&first, &first_attempt), (&larger, &larger_attempt)] {
        let roster = &logical.start.first_attempt.participations;
        let failure = original_failure(roster[0], "non-recovering original fixture failure");
        let terminal = attempt
            .script
            .enqueue(AttemptToLogicalBody::AttemptTerminal(
                AttemptTerminal::Failed(failure.clone()),
            ));
        let accepted = until(&mut fixture, logical, |actual| {
            attempt.script.published().contains(&terminal)
                && actual.effects.iter().any(|effect| {
                    matches!(effect,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error))
                if error.execution_failure() == Some(failure.input()))
                })
        })
        .await;
        assert!(
            !accepted.failed,
            "an original business failure is not a Core infrastructure failure"
        );
        assert!(!accepted.effects.iter().any(|effect| matches!(
            effect,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )));
        settle_script_attempt(&mut fixture, logical, attempt, roster, false).await;
    }
    let history = finish_with_history(&mut fixture).await;
    assert_actual_exit(&fixture, &first, &first_attempt, &history);
    assert_actual_exit(&fixture, &larger, &larger_attempt, &history);
}
