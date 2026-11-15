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

use crate::recovery::contract::*;
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptAuthorizationDecision, AttemptIssueSettlement, ExecutionFailure,
    ResultParticipationWithdrawal,
    logical_port::{AttemptAuthorization, AttemptAuthorizationFailure},
};
use novarocks_execution_contract::{OperationOutcome, ParticipationRef};
use novarocks_logical_execution_protocol::{CoreFault, facts::ParticipationClosureState};
use std::{num::NonZeroU64, sync::Arc};

fn stream() -> RecoveryFixture {
    RecoveryFixture::with_policy(
        LogicalOutputMode::ResultStream,
        RecoveryMode::NoRecovery,
        ExecutionEffect::None,
    )
}
fn root_issue(r: &mut RecoveryFixture) -> Issued {
    let p = participant(1, 1);
    let grant = r.grant(activation(1), p, 61);
    let issue = r.issue(activation(1), p, grant, 61);
    r.settle(issue, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    issue
}
fn end_and_seal_request(r: &mut RecoveryFixture) -> AttemptRequestToken {
    r.f.finished_root();
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    r.relay(
        activation(1),
        RelayRequest::EndConsumed {
            root: root(1),
            end: end(0, 0),
            accepted_packets: 0,
            delivered_through: None,
        },
    );
    let seal = r.f.take(|b| {
        matches!(
            b, LogicalEffectBody::AttemptPort(p)
                if matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
        )
    });
    let LogicalEffectBody::AttemptPort(seal) = seal.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::SealRequest { token } = seal.body else {
        unreachable!()
    };
    token
}
fn seal_peer(r: &mut RecoveryFixture, token: AttemptRequestToken) {
    r.send_port(
        activation(1),
        AttemptToLogicalBody::SealResult {
            token,
            result: Ok(Arc::from([ResultParticipationWithdrawal::new(
                activation(1),
                root(1),
                participant(1, 2),
            )])),
        },
    );
}
fn no_end(r: &RecoveryFixture) {
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(
            ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
        )
    )));
}
fn commit_end(r: &mut RecoveryFixture) {
    let slot = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    assert_eq!(r.f.core.facts().conclusion(), None);
    r.f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert!(r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
    assert!(!r.f.core.facts().protocol_join_ready());
}

#[test]
fn exact_seal_withdrawal_keeps_no_grant_unsent_and_unknown_original_responsibility() {
    for mode in 0..3 {
        let mut r = stream();
        let root = root_issue(&mut r);
        let peer = participant(1, 2);
        let peer_issue = if mode == 0 {
            None
        } else {
            let grant = r.grant(activation(1), peer, 62);
            let issue = r.issue(activation(1), peer, grant, 62);
            r.settle(issue, AttemptIssueSettlement::TransportOwned)
                .unwrap();
            r.settle(
                issue,
                if mode == 1 {
                    AttemptIssueSettlement::DefinitelyUnsent
                } else {
                    AttemptIssueSettlement::TransportUnknown
                },
            )
            .unwrap();
            Some(issue)
        };
        let seal = end_and_seal_request(&mut r);
        seal_peer(&mut r, seal);
        assert_eq!(r.f.core.facts().conclusion(), None);
        no_end(&r);
        r.settle(
            root,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
        commit_end(&mut r);
        let state = r.f.core.facts().participation(activation(1), peer).unwrap();
        assert_eq!(
            state.closure,
            if mode == 0 {
                ParticipationClosureState::NoRemoteResponsibility
            } else {
                ParticipationClosureState::AbortPending
            }
        );
        assert_eq!(
            state.abort_authorizations, 0,
            "normal success never sends Abort"
        );
        assert!(!r.f.core.facts().attempts().next().unwrap().cleanup_complete);
        if let Some(issue) = peer_issue {
            if mode == 1 {
                // Withdrawal did not turn DefinitelyUnsent into an invented
                // original Worker receipt or a transport-owned issue.
                assert!(
                    r.settle(
                        issue,
                        AttemptIssueSettlement::Worker(OperationOutcome::Accepted)
                    )
                    .is_err()
                );
            } else {
                r.settle(
                    issue,
                    AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
                )
                .unwrap();
                r.settle(
                    issue,
                    AttemptIssueSettlement::Worker(OperationOutcome::Idempotent),
                )
                .unwrap();
            }
            assert_eq!(
                r.f.core.facts().conclusion(),
                Some(LogicalConclusion::Succeeded)
            );
        }
    }
}

#[test]
fn eof_and_success_seal_still_wait_for_the_original_required_establish_receipt() {
    let mut r = stream();
    let root = root_issue(&mut r);
    let grant = r.grant(activation(1), participant(1, 2), 63);
    let peer = r.issue(activation(1), participant(1, 2), grant, 63);
    r.settle(peer, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    r.settle(
        peer,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    let token = end_and_seal_request(&mut r);
    r.send_port(
        activation(1),
        AttemptToLogicalBody::SealResult {
            token,
            result: Ok(Arc::from([])),
        },
    );
    assert_eq!(r.f.core.facts().conclusion(), None);
    no_end(&r);
    r.settle(
        root,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    commit_end(&mut r);
}

fn admission_refusal(
    r: &mut RecoveryFixture,
    p: ParticipationRef,
    mode: usize,
    tag: u8,
) -> ExecutionFailure {
    let request = admission(p, tag, 1000);
    let AttemptAuthorizationDecision::AdmissionStarted(issue) = r
        .authorize(
            activation(1),
            AttemptAuthorization::BeginAdmission(request.request),
        )
        .unwrap()
    else {
        panic!("original admission reference expected")
    };
    let error = r
        .authorize(
            activation(1),
            AttemptAuthorization::SettleAdmission {
                issue,
                participation: p,
                operation_id: request.request.envelope().operation_id(),
                outcome: (mode == 0).then_some(OperationOutcome::ParticipationConflict),
                receipt: None,
                prior_transport_unknown: mode == 2,
            },
        )
        .unwrap_err();
    let AttemptAuthorizationFailure::Rejected(error) = error else {
        panic!("original admission refusal expected")
    };
    error
}

#[test]
fn sealed_peer_withdrawal_keeps_late_establish_and_admission_refusals_diagnostic() {
    for mode in 0..5 {
        let mut r = stream();
        let root = root_issue(&mut r);
        let peer = participant(1, 2);
        if mode < 2 {
            let grant = r.grant(activation(1), peer, 64);
            let issue = r.issue(activation(1), peer, grant, 64);
            r.settle(issue, AttemptIssueSettlement::TransportOwned)
                .unwrap();
            r.settle(issue, AttemptIssueSettlement::TransportUnknown)
                .unwrap();
            r.settle(
                issue,
                if mode == 0 {
                    AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict)
                } else {
                    AttemptIssueSettlement::PreWorkerRejected
                },
            )
            .unwrap();
        } else {
            let refusal = admission_refusal(&mut r, peer, mode - 2, 65);
            assert_eq!(
                refusal.input().detail().as_str(),
                "Worker rejected participation admission"
            );
        }
        assert_eq!(r.f.core.facts().conclusion(), None);
        let token = end_and_seal_request(&mut r);
        seal_peer(&mut r, token);
        assert_eq!(r.f.core.facts().conclusion(), None);
        no_end(&r);
        r.settle(
            root,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
        commit_end(&mut r);
        assert!(r.f.core.facts().failure().is_none());
        assert_eq!(
            r.f.core
                .facts()
                .participation(activation(1), peer)
                .unwrap()
                .closure,
            if mode == 2 || mode == 3 {
                ParticipationClosureState::NoRemoteResponsibility
            } else {
                ParticipationClosureState::AbortPending
            }
        );
    }
}

#[test]
fn peer_withdrawal_never_exempts_required_root_admission_refusal() {
    for mode in 0..3 {
        let mut r = stream();
        let refusal = admission_refusal(&mut r, participant(1, 1), mode, 66);
        let token = end_and_seal_request(&mut r);
        seal_peer(&mut r, token);
        assert_eq!(
            r.f.core.facts().conclusion(),
            Some(LogicalConclusion::Failed)
        );
        assert_eq!(r.f.core.facts().failure(), Some(refusal.error()));
        no_end(&r);
    }
}

#[test]
fn original_protocol_fault_after_seal_and_before_slot_cannot_hide_behind_peer_refusal() {
    let mut r = stream();
    let first = root_issue(&mut r);
    r.settle(first, AttemptIssueSettlement::TransportUnknown)
        .unwrap();
    let AttemptAuthorizationDecision::EstablishAuthorized(reference) = r
        .authorize(
            activation(1),
            AttemptAuthorization::ReauthorizeEstablish {
                participation: first.participation,
                operation_id: first.operation,
            },
        )
        .unwrap()
    else {
        panic!("exact root replay reference expected")
    };
    let replay = Issued {
        authorization: reference,
        ..first
    };
    r.settle(replay, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    r.settle(
        replay,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    )
    .unwrap();
    let peer_grant = r.grant(activation(1), participant(1, 2), 67);
    let peer = r.issue(activation(1), participant(1, 2), peer_grant, 67);
    r.settle(peer, AttemptIssueSettlement::TransportOwned)
        .unwrap();
    r.settle(
        peer,
        AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict),
    )
    .unwrap();
    let token = end_and_seal_request(&mut r);
    seal_peer(&mut r, token);
    let slot = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::ReserveEndSlot { .. })
        )
    });
    let original = r
        .settle(
            first,
            AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict),
        )
        .unwrap_err();
    assert_eq!(
        original.input().detail().as_str(),
        "Establish issue received conflicting Worker settlements"
    );
    // The reply goes to AP, which remains the original terminal author. It
    // reports that exact protocol error before the pending consumer slot.
    r.fail(activation(1), original.clone());
    r.f.ack(
        slot.action,
        ActionResult::Consumer(ConsumerFact::EndSlotReady),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    assert_eq!(r.f.core.facts().failure(), Some(original.error()));
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
    )));
}

#[derive(Default)]
struct Effects(Vec<LogicalEffect>);
impl EffectSink for Effects {
    fn emit(&mut self, e: LogicalEffect) {
        self.0.push(e);
    }
}
#[test]
fn root_foreign_and_stale_seal_withdrawals_are_core_faults_without_success_authority() {
    for mode in 0..4 {
        let mut r = stream();
        let peer_grant = r.grant(activation(1), participant(1, 2), 68);
        let peer = r.issue(activation(1), participant(1, 2), peer_grant, 68);
        r.settle(peer, AttemptIssueSettlement::TransportOwned)
            .unwrap();
        r.settle(
            peer,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
        let token = end_and_seal_request(&mut r);
        let bad = match mode {
            0 => ResultParticipationWithdrawal::new(activation(1), root(1), participant(1, 1)),
            1 => ResultParticipationWithdrawal::new(activation(1), root(1), participant(1, 99)),
            2 => ResultParticipationWithdrawal::new(
                AttemptActivationIdentity::from_parts(
                    activation(1).actor().get(),
                    activation(1).execution(),
                    activation(1).generation() + 1,
                ),
                root(1),
                participant(1, 2),
            ),
            _ => ResultParticipationWithdrawal::new(activation(1), root(2), participant(1, 2)),
        };
        let mut out = Effects::default();
        let result = r.f.core.handle(
            r.f.now,
            LogicalInput::AttemptPort(Box::new(AttemptToLogical {
                activation: activation(1),
                action: AttemptActionId::new(NonZeroU64::new(9000).unwrap()),
                body: AttemptToLogicalBody::SealResult {
                    token,
                    result: Ok(Arc::from([bad])),
                },
            })),
            &mut Budget(32),
            &mut out,
        );
        assert_eq!(
            result,
            Err(CoreFault::UnexpectedInput),
            "bad original author mode {mode}"
        );
        assert_eq!(r.f.core.facts().conclusion(), None);
        assert!(!r.f.core.facts().output_visible());
        assert!(out.0.iter().all(|e| !matches!(
            e.body,
            LogicalEffectBody::Consumer(
                ConsumerEffect::ReserveEndSlot { .. } | ConsumerEffect::EnqueueEnd { .. }
            )
        )));
        no_end(&r);
    }
}

#[test]
fn one_schema_receipt_survives_a_to_b_recovery_and_empty_b_eof() {
    use novarocks_logical_execution_protocol::facts::LogicalSchemaState;
    let mut r = RecoveryFixture::new(LogicalOutputMode::ResultStream);
    r.establish_initial(AttemptIssueSettlement::TransportUnknown);
    r.f.finished_root();
    r.relay(
        activation(1),
        RelayRequest::OpenDelivery {
            root: root(1),
            carrier: carrier(false),
        },
    );
    let schema = r.f.take(|b| {
        matches!(
            b,
            LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
        )
    });
    r.f.ack(
        schema.action,
        ActionResult::Consumer(ConsumerFact::SchemaWritten),
    );
    assert_eq!(r.f.core.facts().schema(), LogicalSchemaState::Done);
    let first = backend_failure(1, "A failed after the one original schema receipt");
    r.fail(activation(1), first.clone());
    let (dispatch, _) = r.prepare(roster(2));
    let grants = dispatch
        .replacement
        .iter()
        .enumerate()
        .map(|(i, p)| admission(*p, 70 + i as u8, 1000))
        .collect::<Vec<_>>();
    r.f.now = at(10);
    r.qualify(&dispatch, grants.clone(), at(5));
    let (b, inherited) = r.activate(&dispatch);
    assert_eq!(inherited.len(), 2);
    for (i, grant) in grants.iter().enumerate() {
        let issued = r.issue(b, grant.request.participation(), *grant, 70 + i as u8);
        r.settle(issued, AttemptIssueSettlement::TransportOwned)
            .unwrap();
        r.settle(
            issued,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        )
        .unwrap();
    }
    let status = TaskStatus::try_new(
        root(2),
        TaskStatusVersion::new(6).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    r.send_port(
        b,
        AttemptToLogicalBody::RootStatus(Box::new(
            novarocks_coordination_contract::AttemptRootProjection {
                status,
                failure: novarocks_coordination_contract::AttemptRootFailure::None,
            },
        )),
    );
    r.relay(
        b,
        RelayRequest::OpenDelivery {
            root: root(2),
            carrier: carrier(false),
        },
    );
    assert_eq!(r.f.core.facts().schema(), LogicalSchemaState::Done);
    assert!(!r.f.effects.iter().any(|e| matches!(
        e.body,
        LogicalEffectBody::Consumer(ConsumerEffect::WriteSchema { .. })
    )));
    assert!(
        !r.f.core.facts().output_visible(),
        "empty B has no visible data packet"
    );
    r.relay(
        b,
        RelayRequest::EndConsumed {
            root: root(2),
            end: end(0, 0),
            accepted_packets: 0,
            delivered_through: None,
        },
    );
    let seal = r.f.take(|bdy| {
        matches!(
            bdy, LogicalEffectBody::AttemptPort(p)
                if p.activation == b && matches!(p.body, LogicalToAttemptBody::SealRequest { .. })
        )
    });
    let LogicalEffectBody::AttemptPort(seal) = seal.body else {
        unreachable!()
    };
    let LogicalToAttemptBody::SealRequest { token } = seal.body else {
        unreachable!()
    };
    r.send_port(
        b,
        AttemptToLogicalBody::SealResult {
            token,
            result: Ok(Arc::from([])),
        },
    );
    commit_end(&mut r);
    let enqueue = r.f.take(|bdy| matches!(
        bdy, LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { activation }) if *activation == b
    ));
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    r.f.ack(
        enqueue.action,
        ActionResult::Consumer(ConsumerFact::EndSettled),
    );
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Succeeded)
    );
    assert_eq!(r.f.core.facts().schema(), LogicalSchemaState::Done);
    assert!(r.f.core.facts().failure().is_none());
    assert_eq!(
        r.f.core
            .facts()
            .attempts()
            .find(|a| a.activation == activation(1))
            .unwrap()
            .accepted_failure,
        Some(first.input()),
    );
}

#[test]
fn current_establish_refusal_waits_for_original_authoritative_task_terminal() {
    use novarocks_coordination_contract::{
        ExecutionFailureInput, QueryFailureReason, SelectedTaskTermination,
    };
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TerminationDetail, failure::FailureProjection,
    };
    let mut r = stream();
    let root_authorization = root_issue(&mut r);
    r.settle(
        root_authorization,
        AttemptIssueSettlement::Worker(OperationOutcome::ParticipationConflict),
    )
    .unwrap();
    assert_eq!(r.f.core.facts().conclusion(), None);
    assert!(r.f.core.facts().failure().is_none());
    let selected = SelectedTaskTermination::new(
        root(1),
        TerminationDetail::Failed(TaskFailure::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("original Task terminal after current Establish refusal"),
        )),
    );
    let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
    let original = ExecutionFailure::from_input(input.clone());
    r.fail(activation(1), original.clone());
    assert_eq!(
        r.f.core.facts().conclusion(),
        Some(LogicalConclusion::Failed)
    );
    assert_eq!(r.f.core.facts().failure(), Some(original.error()));
    assert_eq!(
        r.f.core.facts().attempts().next().unwrap().accepted_failure,
        Some(&input),
    );
    assert_eq!(
        r.f.effects
            .iter()
            .filter_map(|e| match &e.body {
                LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) => Some(error),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![original.error()],
    );
    no_end(&r);
}
