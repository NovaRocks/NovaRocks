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

use crate::operation::WorkerReceiptOutcome;
use std::num::{NonZeroU64, NonZeroUsize};

fn gate(m: usize) -> EstablishAuthorization {
    gate_with_seed(m, 17)
}
fn gate_with_seed(m: usize, seed: u128) -> EstablishAuthorization {
    let (graph, activation) = crate::test_support::graph_with_strict_input(false);
    let participation = *graph.participations().next().unwrap();
    let mut ids = crate::ids::OperationIds::new(crate::ids::IdSeed::new(seed, 200));
    EstablishAuthorizationStorage::new(
        activation,
        AttemptAuthorizationLimits {
            activation,
            establish_per_participation: NonZeroUsize::new(m).unwrap(),
            abort_per_participation: NonZeroUsize::new(m).unwrap(),
        },
        participation,
    )
    .unwrap()
    .bind(ids.issue())
}
fn failure(n: u64) -> AttemptFailure {
    AttemptFailure::Host(crate::failure::HostProjectedFailure {
        original: crate::failure::HostFailureRef(NonZeroU64::new(n).unwrap()),
        kind: crate::failure::HostFailureKind::EstablishAuthorization,
        class: novarocks_coordination_contract::AttemptFailureClass::RecoverableInfrastructure,
        site: crate::failure::AttemptFailureSite::Attempt,
    })
}
fn authorize(g: &mut EstablishAuthorization, n: u64, replay: bool) {
    let Directive::Request(request) = g.authorize(token(n)) else {
        panic!("one original authorization")
    };
    assert!(match request {
        AttemptAuthorization::Establish {
            participation,
            operation_id,
        } if !replay => participation == g.participation && operation_id == g.operation_id,
        AttemptAuthorization::ReauthorizeEstablish {
            participation,
            operation_id,
        } if replay => participation == g.participation && operation_id == g.operation_id,
        _ => false,
    });
    assert_eq!(
        g.authorized(
            token(n),
            Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(n)))
        )
        .unwrap(),
        AuthorizationReply::IssueRecorded(issue(n))
    );
}
fn selected(
    g: &mut EstablishAuthorization,
    n: u64,
    expected: u64,
    settlement: AttemptIssueSettlement,
) {
    assert_eq!(
        g.advance(token(n)),
        Step::Settle {
            token: token(n),
            authorization: issue(expected),
            settlement
        }
    );
    assert_eq!(g.advance(token(n + 1000)), Step::Waiting);
    assert!(!g.permits_protocol_turn());
}
fn send_generation(n: u64) -> SendGeneration {
    SendGeneration::new(novarocks_coordination_contract::AttemptActionId::new(
        std::num::NonZeroU64::new(n).unwrap(),
    ))
}
fn unknown(g: &mut EstablishAuthorization, n: u64) {
    g.emitted(issue(n), send_generation(n)).unwrap();
    g.begin_settlement(OperationDispatchResult::TransportUnknown)
        .unwrap();
    selected(g, n + 100, n, AttemptIssueSettlement::TransportUnknown);
    assert_eq!(
        g.settled(token(n + 100), issue(n), Ok(())).unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: false
        }
    );
}
fn worker() -> OperationDispatchResult {
    OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(
        OperationOutcome::Accepted,
    ))
}
#[test]
fn full_and_late_success_do_not_create_another_call_or_reopen_closing() {
    let mut g = gate(3);
    assert!(matches!(
        g.authorize(token(1)),
        Directive::Request(AttemptAuthorization::Establish { .. })
    ));
    for n in 2..99 {
        assert!(matches!(g.authorize(token(n)), Directive::Waiting));
    }
    assert_eq!(
        g.authorized(
            token(2),
            Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(2)))
        )
        .unwrap(),
        AuthorizationReply::Stale
    );
    g.close();
    assert_eq!(g.advance(token(100)), Step::Waiting);
    assert_eq!(
        g.authorized(
            token(1),
            Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(1)))
        )
        .unwrap(),
        AuthorizationReply::IssueRecorded(issue(1))
    );
    assert!(matches!(g.authorize(token(101)), Directive::Retired));
    assert!(g.emitted(issue(1), send_generation(1)).is_err());
    selected(&mut g, 102, 1, AttemptIssueSettlement::DefinitelyUnsent);
    // The original prepare path ignores this actual guard-method error only.
    assert_eq!(
        g.settled(token(102), issue(1), Err(failure(1))).unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: false
        }
    );
    assert_eq!(g.advance(token(103)), Step::Ready);
}
#[test]
fn unknowns_then_worker_consume_oldest_first_and_keep_first_error() {
    let mut g = gate(3);
    authorize(&mut g, 1, false);
    unknown(&mut g, 1);
    authorize(&mut g, 2, true);
    unknown(&mut g, 2);
    authorize(&mut g, 3, true);
    g.begin_settlement(worker()).unwrap();
    for (n, expected) in [(10, 1), (11, 2), (12, 3)] {
        selected(
            &mut g,
            n,
            expected,
            AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
        );
        assert_eq!(
            g.settled(token(n + 1), issue(expected), Ok(())).unwrap(),
            SettlementReply::Stale
        );
        let result = g
            .settled(token(n), issue(expected), Err(failure(n)))
            .unwrap();
        if n == 12 {
            assert_eq!(
                result,
                SettlementReply::Complete {
                    failure: Some(failure(10)),
                    retire_queued_authorization: true
                }
            );
        } else {
            assert_eq!(result, SettlementReply::Progress);
        }
    }
    assert!(matches!(g.authorize(token(20)), Directive::Retired));
    assert!(g.late.is_empty() && g.current.is_none());
}
#[test]
fn ingress_rejection_selects_current_else_only_oldest_unknown() {
    let mut g = gate(3);
    authorize(&mut g, 1, false);
    unknown(&mut g, 1);
    authorize(&mut g, 2, true);
    unknown(&mut g, 2);
    authorize(&mut g, 3, true);
    g.begin_settlement(OperationDispatchResult::NonWorkerRejected)
        .unwrap();
    selected(&mut g, 10, 3, AttemptIssueSettlement::PreWorkerRejected);
    g.settled(token(10), issue(3), Err(failure(10))).unwrap();
    assert_eq!(
        g.late.iter().copied().collect::<Vec<_>>(),
        [issue(1), issue(2)]
    );
    g.begin_settlement(OperationDispatchResult::NonWorkerRejected)
        .unwrap();
    selected(&mut g, 11, 1, AttemptIssueSettlement::PreWorkerRejected);
    g.settled(token(11), issue(1), Ok(())).unwrap();
    assert_eq!(g.late.front(), Some(&issue(2)));
    // A later Worker verdict still reaches the remaining residual guard.
    g.begin_settlement(worker()).unwrap();
    selected(
        &mut g,
        12,
        2,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    );
    g.settled(token(12), issue(2), Ok(())).unwrap();
}

fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
fn issue(n: u64) -> AttemptAuthorizationRef {
    AttemptAuthorizationRef::new(NonZeroU64::new(n).unwrap())
}
#[test]
fn final_unknown_uses_original_m_capacity_and_typed_exhaustion_preserves_error() {
    let mut g = gate(3);
    let capacity = g.late.capacity();
    let pointer = g.late.as_slices().0.as_ptr();
    for n in 1..=3 {
        authorize(&mut g, n, n != 1);
        unknown(&mut g, n);
    }
    assert_eq!(g.late.len(), 3);
    assert_eq!(g.late.capacity(), capacity);
    assert_eq!(g.late.as_slices().0.as_ptr(), pointer);
    // M is a storage fact, not permission or a local retry rejection policy.
    assert!(matches!(
        g.authorize(token(4)),
        Directive::Request(AttemptAuthorization::ReauthorizeEstablish { .. })
    ));
    let passive = crate::test_support::establish_budget_failure();
    assert_eq!(
        g.authorized(
            token(4),
            Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(
                passive.clone()
            ))
        )
        .unwrap(),
        AuthorizationReply::Failed(AttemptFailure::EstablishReplayExhausted {
            original: passive,
            participation: g.participation,
            operation_id: g.operation_id
        })
    );
    let mut clean = gate(3);
    clean.authorize(token(1));
    assert_eq!(
        clean
            .authorized(
                token(1),
                Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(
                    crate::test_support::establish_budget_failure()
                ))
            )
            .unwrap(),
        AuthorizationReply::Failed(AttemptFailure::Logical(
            crate::test_support::establish_budget_failure()
        ))
    );
    assert!(matches!(clean.authorize(token(2)), Directive::Retired));
}
#[test]
fn unknown_failure_consumes_only_current_and_duplicate_unknown_never_consumes_late() {
    let mut g = gate(3);
    authorize(&mut g, 1, false);
    unknown(&mut g, 1);
    authorize(&mut g, 2, true);
    g.begin_settlement(OperationDispatchResult::TransportUnknown)
        .unwrap();
    selected(&mut g, 10, 2, AttemptIssueSettlement::TransportUnknown);
    assert_eq!(
        g.settled(token(10), issue(2), Err(failure(10))).unwrap(),
        SettlementReply::Complete {
            failure: Some(failure(10)),
            retire_queued_authorization: false
        }
    );
    g.begin_settlement(OperationDispatchResult::TransportUnknown)
        .unwrap();
    assert_eq!(g.advance(token(11)), Step::Ready);
    assert_eq!(
        g.finish_empty().unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: false
        }
    );
    assert_eq!(g.late.front(), Some(&issue(1)));
}
#[test]
fn closing_emitted_waits_for_real_withdrawal_then_forgets_each_local_late_ref() {
    let mut g = gate(3);
    authorize(&mut g, 1, false);
    unknown(&mut g, 1);
    authorize(&mut g, 2, true);
    unknown(&mut g, 2);
    authorize(&mut g, 3, true);
    g.emitted(issue(3), send_generation(3)).unwrap();
    assert!(g.forget_one_closed_late_ref().is_err());
    g.close();
    assert!(g.forget_one_closed_late_ref().is_err());
    assert_eq!(g.late.len(), 2);
    for n in 10..100 {
        assert_eq!(g.advance(token(n)), Step::WaitingTransport);
    }
    assert!(!g.withdrawn(send_generation(2)));
    assert_eq!(g.advance(token(100)), Step::WaitingTransport);
    assert!(g.withdrawn(send_generation(3)));
    assert!(!g.withdrawn(send_generation(3)));
    selected(&mut g, 100, 3, AttemptIssueSettlement::DefinitelyUnsent);
    g.settled(token(100), issue(3), Ok(())).unwrap();
    assert_eq!(g.advance(token(101)), Step::Ready);
    for guard in [1, 2] {
        assert_eq!(g.forget_one_closed_late_ref().unwrap(), Some(issue(guard)));
        assert!(
            g.late_entry,
            "a local pop does not complete whole-gate preparation"
        );
        assert!(g.permits_protocol_turn());
    }
    assert_eq!(g.forget_one_closed_late_ref().unwrap(), None);
    assert!(g.late_entry);
    assert!(g.forget_closed_local_entry());
    assert!(!g.late_entry);
    assert_eq!(g.advance(token(103)), Step::Ready);
    assert!(g.permits_protocol_turn());
}
#[test]
fn invalid_reply_shapes_do_not_release_original_call_and_closed_ack_remains_reachable() {
    let mut g = gate(3);
    g.authorize(token(1));
    assert!(
        g.authorized(token(1), Ok(AttemptAuthorizationDecision::AdmissionGranted))
            .is_err()
    );
    assert!(!g.permits_protocol_turn());
    assert!(g.begin_settlement(worker()).is_err());
    g.authorized(
        token(1),
        Ok(AttemptAuthorizationDecision::EstablishAuthorized(issue(1))),
    )
    .unwrap();
    g.emitted(issue(1), send_generation(1)).unwrap();
    g.close();
    g.begin_settlement(worker()).unwrap();
    selected(
        &mut g,
        2,
        1,
        AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
    );
    assert!(g.finish_empty().is_err());
    assert!(matches!(
        g.settled(token(3), issue(1), Ok(())).unwrap(),
        SettlementReply::Stale
    ));
    assert!(matches!(
        g.settled(token(2), issue(2), Ok(())).unwrap(),
        SettlementReply::Stale
    ));
    assert!(!g.permits_protocol_turn());
    assert!(g.finish_empty().is_err());
    assert!(g.forget_one_closed_late_ref().is_err());
    g.settled(token(2), issue(1), Ok(())).unwrap();
    assert_eq!(g.advance(token(3)), Step::Ready);
}
#[test]
fn unowned_ack_does_not_retire_future_authorization_but_empty_late_entry_still_owns_ack() {
    let mut g = gate(3);
    g.begin_settlement(worker()).unwrap();
    assert_eq!(g.advance(token(1)), Step::Ready);
    assert_eq!(
        g.finish_empty().unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: false
        }
    );
    authorize(&mut g, 2, false);
    unknown(&mut g, 2);
    g.begin_settlement(OperationDispatchResult::NonWorkerRejected)
        .unwrap();
    selected(&mut g, 3, 2, AttemptIssueSettlement::PreWorkerRejected);
    g.settled(token(3), issue(2), Ok(())).unwrap();
    assert!(g.late.is_empty());
    g.begin_settlement(worker()).unwrap();
    assert_eq!(g.advance(token(4)), Step::Ready);
    assert_eq!(
        g.finish_empty().unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: true
        }
    );
}
#[test]
fn rejected_authorization_is_not_reissued_by_a_repeated_carrier() {
    let mut g = gate(3);
    g.authorize(token(1));
    assert_eq!(
        g.authorized(
            token(1),
            Err(AttemptAuthorizationFailure::Rejected(
                crate::test_support::logical_failure("original logical authorization refused")
            ))
        )
        .unwrap(),
        AuthorizationReply::Failed(AttemptFailure::Logical(
            crate::test_support::logical_failure("original logical authorization refused")
        ))
    );
    assert!(matches!(g.authorize(token(2)), Directive::Retired));
    // Still permit observed old ACKs; rejection is not remote/actual exit.
    g.begin_settlement(worker()).unwrap();
    assert_eq!(g.advance(token(3)), Step::Ready);
    assert_eq!(
        g.finish_empty().unwrap(),
        SettlementReply::Complete {
            failure: None,
            retire_queued_authorization: false
        }
    );
}
#[test]
fn final_whole_gate_preparation_clears_only_closed_local_correlation() {
    for late_ref_remaining in [false, true] {
        let mut g = gate(2);
        authorize(&mut g, 1, false);
        unknown(&mut g, 1);
        assert!(!g.forget_closed_local_entry());
        if !late_ref_remaining {
            g.begin_settlement(OperationDispatchResult::NonWorkerRejected)
                .unwrap();
            selected(&mut g, 2, 1, AttemptIssueSettlement::PreWorkerRejected);
            g.settled(token(2), issue(1), Ok(())).unwrap();
        }
        g.close();
        assert_eq!(g.advance(token(3)), Step::Ready);
        if late_ref_remaining {
            assert!(!g.forget_closed_local_entry());
            assert_eq!(g.forget_one_closed_late_ref().unwrap(), Some(issue(1)));
        }
        assert_eq!(g.forget_one_closed_late_ref().unwrap(), None);
        assert_eq!(g.advance(token(4)), Step::Ready);
        assert!(
            g.late_entry,
            "row ready alone cannot clear whole-gate ownership"
        );
        assert!(g.forget_closed_local_entry());
        g.begin_settlement(worker()).unwrap();
        assert_eq!(g.advance(token(5)), Step::Ready);
        assert_eq!(
            g.finish_empty().unwrap(),
            SettlementReply::Complete {
                failure: None,
                retire_queued_authorization: false
            }
        );
    }
}

#[test]
fn registration_preallocates_before_original_operation_exists_and_binding_moves_same_storage() {
    let (graph, activation) = crate::test_support::graph_with_strict_input(false);
    let participation = *graph.participations().next().unwrap();
    let limits = AttemptAuthorizationLimits {
        activation,
        establish_per_participation: NonZeroUsize::new(3).unwrap(),
        abort_per_participation: NonZeroUsize::new(3).unwrap(),
    };
    let storage = EstablishAuthorizationStorage::new(activation, limits, participation).unwrap();
    let capacity = storage.late.capacity();
    let pointer = storage.late.as_slices().0.as_ptr();
    let mut ids = crate::ids::OperationIds::new(crate::ids::IdSeed::new(17, 200));
    let original_admission = ids.issue();
    let establish = ids.issue();
    let mut gate = storage.bind(establish);
    assert_eq!(gate.late.capacity(), capacity);
    assert_eq!(gate.late.as_slices().0.as_ptr(), pointer);
    assert_ne!(original_admission, gate.operation_id);
    assert!(
        matches!(gate.authorize(token(1)), Directive::Request(AttemptAuthorization::Establish { participation: part, operation_id })
        if part == participation && operation_id == establish)
    );
    let wrong = AttemptActivationIdentity::from_parts(
        activation.actor().get(),
        activation.execution(),
        activation.generation() + 1,
    );
    assert!(EstablishAuthorizationStorage::new(wrong, limits, participation).is_err());
}

#[test]
fn deferred_journal_preserves_ingress_worker_order_and_selects_guards_only_at_settlement() {
    use crate::authorization::observations::{ObserverDebt, ObserverJournal};
    use crate::input::{LocalDispatchResult, OperationReply, OperationReplyResult};
    use novarocks_execution_contract::{OperationKind, operation::OperationResult};
    for worker_first in [false, true] {
        let mut g = gate(2);
        authorize(&mut g, 1, false);
        unknown(&mut g, 1);
        authorize(&mut g, 2, true);
        let ingress = OperationReply {
            operation_id: g.operation_id,
            kind: OperationKind::UpdateParticipation,
            result: OperationReplyResult::Local {
                result: LocalDispatchResult::NonWorkerRejected,
                failure: None,
                protocol_failure: None,
                detail: None,
            },
        };
        let worker = OperationReply {
            operation_id: g.operation_id,
            kind: OperationKind::UpdateParticipation,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: None,
            }),
        };
        let replies = if worker_first {
            [&worker, &ingress]
        } else {
            [&ingress, &worker]
        };
        let mut journal = ObserverJournal::new([1; 4]).unwrap();
        journal.begin_cut().unwrap();
        for reply in replies {
            journal.observe(reply).unwrap();
            assert!(g.permits_protocol_turn());
            assert_eq!(g.current.unwrap().authorization, issue(2));
            assert_eq!(g.late.front(), Some(&issue(1)));
        }
        journal.finish_recording().unwrap();
        let mut trace = Vec::new();
        let mut n = 300;
        while let Some(ObserverDebt::Establish(observed)) = journal.current().unwrap() {
            g.begin_observed_settlement(observed).unwrap();
            loop {
                n += 1;
                let result = match g.advance(token(n)) {
                    Step::Settle {
                        token,
                        authorization,
                        settlement,
                    } => {
                        trace.push((authorization, settlement));
                        assert_eq!(
                            journal.current().unwrap(),
                            Some(ObserverDebt::Establish(observed))
                        );
                        g.settled(token, authorization, Ok(())).unwrap()
                    }
                    Step::Ready => g.finish_empty().unwrap(),
                    other => panic!("unexpected settlement state: {other:?}"),
                };
                if matches!(result, SettlementReply::Complete { .. }) {
                    break;
                }
            }
            journal.settled_current().unwrap();
        }
        journal.finish_cut().unwrap();
        let expected = if worker_first {
            vec![
                (
                    issue(1),
                    AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
                ),
                (
                    issue(2),
                    AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
                ),
            ]
        } else {
            vec![
                (issue(2), AttemptIssueSettlement::PreWorkerRejected),
                (
                    issue(1),
                    AttemptIssueSettlement::Worker(OperationOutcome::Accepted),
                ),
            ]
        };
        assert_eq!(trace, expected);
        assert!(g.current.is_none() && g.late.is_empty() && g.permits_protocol_turn());
    }
}

#[test]
fn only_typed_budget_and_same_operation_unknown_history_author_replay_exhaustion() {
    let original = crate::test_support::establish_budget_failure();
    // Identical fact, code, detail and site cannot substitute for the enum
    // author, nor can another operation's genuine transport-unknown history.
    for late in [false, true] {
        for budget in [false, true] {
            let mut other = gate_with_seed(3, 98);
            authorize(&mut other, 1, false);
            unknown(&mut other, 1);
            let mut g = gate(3);
            assert_ne!(g.operation_id, other.operation_id);
            if late {
                authorize(&mut g, 1, false);
                unknown(&mut g, 1);
            }
            assert!(matches!(g.authorize(token(2)), Directive::Request(_)));
            let rejection = if budget {
                AttemptAuthorizationFailure::EstablishBudgetExhausted(original.clone())
            } else {
                AttemptAuthorizationFailure::Rejected(original.clone())
            };
            assert_eq!(
                g.authorized(token(99), Err(rejection.clone())).unwrap(),
                AuthorizationReply::Stale
            );
            assert_eq!(g.authorization_flight, Some(token(2)));
            let AuthorizationReply::Failed(failure) =
                g.authorized(token(2), Err(rejection)).unwrap()
            else {
                panic!("the exact original refusal must settle this authorization");
            };
            if budget && late {
                assert_eq!(
                    failure,
                    AttemptFailure::EstablishReplayExhausted {
                        original: original.clone(),
                        participation: g.participation,
                        operation_id: g.operation_id,
                    }
                );
                let projected = crate::port_projection::project_failure(&failure, &mut |_| {
                    panic!("no invented HostRef")
                })
                .unwrap();
                assert_eq!(
                    projected.input(),
                    &crate::port_projection::failure_input(
                        novarocks_coordination_contract::QueryFailureReason::ObservationLost,
                        "frontend task protocol could not continue",
                        novarocks_coordination_contract::ExecutionFailureSite::Backend {
                            process: g.participation.backend_process_id()
                        },
                    )
                );
            } else {
                assert_eq!(failure, AttemptFailure::Logical(original.clone()));
                assert_eq!(
                    crate::port_projection::project_failure(&failure, &mut |_| panic!(
                        "no HostRef"
                    ))
                    .unwrap(),
                    original
                );
            }
            assert_eq!(other.late.len(), 1);
            assert!(matches!(g.authorize(token(3)), Directive::Retired));
        }
    }
}
