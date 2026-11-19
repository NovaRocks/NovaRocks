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

//! Deterministic ledger sequences protect protocol decisions. The host owns
//! rich receipt retention, actual transport exit and physical request aliases.
use super::*;
use novarocks_coordination_contract::IdSeed;
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::time::Duration;

struct Budget(usize);
impl StepBudget for Budget {
    fn charge(&mut self, n: usize) -> bool {
        if self.0 < n {
            false
        } else {
            self.0 -= n;
            true
        }
    }
}
fn time(ms: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(ms))
}
fn execution() -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(17, 23), AttemptId::new(1).unwrap()).unwrap()
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(1, execution(), 1)
}
fn p(n: u8) -> ParticipationRef {
    let mut bytes = [0; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = n;
    ParticipationRef::new(
        execution(),
        FrontendProcessId::try_from_bytes(bytes).unwrap(),
        BackendProcessId::try_from_bytes(bytes).unwrap(),
    )
}
fn action(n: u64) -> AttemptActionId {
    AttemptActionId::new(std::num::NonZeroU64::new(n).unwrap())
}
fn ids() -> OperationIds {
    OperationIds::new(IdSeed::new(19, 29))
}
fn ledger(n: u8, limit: usize, mode: StandDownMode) -> ParticipationStandDownLedger {
    let required = Arc::new((1..=n).map(p).collect());
    let mut ledger = ParticipationStandDownLedger::new(
        activation(),
        required,
        NonZeroUsize::new(limit).unwrap(),
    );
    ledger.begin(mode, time(100)).unwrap();
    for i in 1..=n {
        ledger
            .initialize_participation(p(i), EstablishStandDownFact::AbortRequired)
            .unwrap();
    }
    ledger.finish_initialization().unwrap();
    ledger
}
fn cancelled() -> StandDownMode {
    StandDownMode::Abort(ParticipationStandDownCause::LogicalExecutionCancelled)
}
fn issue(
    l: &mut ParticipationStandDownLedger,
    now: u64,
    n: u64,
    ids: &mut OperationIds,
) -> AbortDispatch {
    for _ in 0..8 {
        match l
            .poll_next_abort(time(now), action(n), ids, &mut Budget(1))
            .unwrap()
        {
            AbortPoll::Issued(dispatch) => return dispatch,
            AbortPoll::Progressed => {}
            other => panic!("expected a ready Abort, got {other:?}"),
        }
    }
    panic!("fixture expected bounded ready progress")
}
fn settle(
    l: &mut ParticipationStandDownLedger,
    now: u64,
    d: AbortDispatch,
    s: AbortIssueSettlement,
) -> Result<(), ParticipationStandDownError> {
    l.settle(
        time(now),
        activation(),
        d.action,
        d.request.participation(),
        d.request.envelope().operation_id(),
        s,
    )
}
fn worker(closure: AbortWorkerClosure) -> AbortIssueSettlement {
    AbortIssueSettlement::Worker {
        outcome: OperationOutcome::Accepted,
        closure,
    }
}

#[test]
fn no_grant_and_empty_frozen_set_have_no_remote_responsibility() {
    let mut l = ParticipationStandDownLedger::new(
        activation(),
        Arc::new(BTreeSet::from([p(1)])),
        NonZeroUsize::MIN,
    );
    l.begin(cancelled(), time(100)).unwrap();
    assert!(!l.progress().initialized);
    l.initialize_participation(p(1), EstablishStandDownFact::NoGrant)
        .unwrap();
    l.finish_initialization().unwrap();
    assert!(l.residual_resource_settled());
    assert!(matches!(
        l.poll_next_abort(time(0), action(1), &mut ids(), &mut Budget(1))
            .unwrap(),
        AbortPoll::Idle
    ));
    let empty = ledger(0, 1, cancelled());
    assert!(empty.residual_resource_settled());
}

#[test]
fn initialization_is_incremental_and_rejects_duplicates_missing_and_foreign_rows() {
    let mut l = ParticipationStandDownLedger::new(
        activation(),
        Arc::new(BTreeSet::from([p(1), p(2)])),
        NonZeroUsize::MIN,
    );
    l.begin(cancelled(), time(100)).unwrap();
    l.initialize_participation(p(1), EstablishStandDownFact::AbortRequired)
        .unwrap();
    assert_eq!(
        l.initialize_participation(p(1), EstablishStandDownFact::NoGrant),
        Err(ParticipationStandDownError::DuplicateParticipation)
    );
    assert_eq!(
        l.initialize_participation(p(3), EstablishStandDownFact::NoGrant),
        Err(ParticipationStandDownError::UnknownParticipation)
    );
    assert_eq!(
        l.finish_initialization(),
        Err(ParticipationStandDownError::InitializationIncomplete)
    );
    let foreign = ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(3, 4), AttemptId::new(1).unwrap()).unwrap(),
        p(1).frontend_process_id(),
        p(1).backend_process_id(),
    );
    assert_eq!(
        l.initialize_participation(foreign, EstablishStandDownFact::NoGrant),
        Err(ParticipationStandDownError::ActivationExecutionMismatch)
    );
    l.initialize_participation(p(2), EstablishStandDownFact::NoGrant)
        .unwrap();
    l.finish_initialization().unwrap();
    assert!(!l.residual_resource_settled());
}

#[test]
fn authorized_unsettled_establish_is_already_an_abort_responsibility() {
    // The P03 fact for IssueAuthorized is AbortRequired, not an awaiting gate.
    let mut l = ledger(1, 2, cancelled());
    let d = issue(&mut l, 0, 1, &mut ids());
    assert_eq!(d.request.participation(), p(1));
    assert!(!l.residual_resource_settled());
}

#[test]
fn held_ticket_remains_responsible_even_after_definitely_unsent_establish() {
    let mut l = ledger(1, 2, cancelled());
    l.refresh_establish_fact(p(1), EstablishStandDownFact::NoGrant)
        .unwrap();
    assert_eq!(
        issue(&mut l, 0, 1, &mut ids()).request.participation(),
        p(1)
    );
}

#[test]
fn abort_authorization_is_round_robin_and_reuses_the_seeded_operation() {
    let mut l = ledger(2, 3, cancelled());
    let mut ids = ids();
    let first = issue(&mut l, 0, 1, &mut ids);
    settle(&mut l, 0, first, AbortIssueSettlement::TransportUnknown).unwrap();
    let second = issue(&mut l, 0, 2, &mut ids);
    assert_ne!(
        first.request.participation(),
        second.request.participation()
    );
    settle(&mut l, 0, second, AbortIssueSettlement::DefinitelyUnsent).unwrap();
    let replay = issue(&mut l, 50, 3, &mut ids);
    assert_eq!(
        first.request.participation(),
        replay.request.participation()
    );
    assert_eq!(
        first.request.envelope().operation_id(),
        replay.request.envelope().operation_id()
    );
    assert_ne!(first.action, replay.action);
}

#[test]
fn both_real_retry_settlements_wait_fifty_ms_from_arrival_and_duplicates_do_not_renew() {
    for settlement in [
        AbortIssueSettlement::TransportUnknown,
        AbortIssueSettlement::DefinitelyUnsent,
    ] {
        let mut l = ledger(1, 3, cancelled());
        let mut ids = ids();
        let d = issue(&mut l, 0, 1, &mut ids);
        settle(&mut l, 9, d, settlement).unwrap();
        assert_eq!(l.next_abort_at(time(10)), Some(time(59)));
        settle(&mut l, 20, d, settlement).unwrap();
        assert_eq!(l.next_abort_at(time(20)), Some(time(59)));
        assert!(matches!(
            l.poll_next_abort(time(58), action(2), &mut ids, &mut Budget(1))
                .unwrap(),
            AbortPoll::Idle
        ));
        assert_eq!(
            issue(&mut l, 59, 2, &mut ids)
                .request
                .envelope()
                .operation_id(),
            d.request.envelope().operation_id()
        );
    }
}

#[test]
fn budget_exhaustion_does_not_issue_or_scan_other_rows() {
    let mut l = ledger(2, 1, cancelled());
    assert!(matches!(
        l.poll_next_abort(time(0), action(1), &mut ids(), &mut Budget(0))
            .unwrap(),
        AbortPoll::Yielded
    ));
    assert_eq!(l.snapshot(p(1)).unwrap().authorizations, 0);
    assert_eq!(l.snapshot(p(2)).unwrap().authorizations, 0);
    let mut budget = Budget(1);
    assert!(matches!(
        l.poll_next_abort(time(0), action(1), &mut ids(), &mut budget)
            .unwrap(),
        AbortPoll::Issued(_)
    ));
    assert_eq!(budget.0, 0);
}

#[test]
fn exhausted_participation_does_not_block_its_sibling() {
    let mut l = ledger(2, 1, cancelled());
    let mut ids = ids();
    let first = issue(&mut l, 0, 1, &mut ids);
    settle(&mut l, 0, first, AbortIssueSettlement::TransportUnknown).unwrap();
    let sibling = issue(&mut l, 0, 2, &mut ids);
    assert_ne!(
        first.request.participation(),
        sibling.request.participation()
    );
    assert!(matches!(
        l.poll_next_abort(time(50), action(3), &mut ids, &mut Budget(1))
            .unwrap(),
        AbortPoll::Progressed
    ));
    assert!(matches!(
        l.poll_next_abort(time(50), action(3), &mut ids, &mut Budget(1))
            .unwrap(),
        AbortPoll::Progressed
    ));
    assert_eq!(
        l.snapshot(first.request.participation()).unwrap().closure,
        ParticipationClosureState::AbortIssueExhausted
    );
    assert!(!l.residual_resource_settled());
}

#[test]
fn every_worker_closing_state_only_settles_abort_responsibility() {
    for closure in [
        AbortWorkerClosure::Aborting,
        AbortWorkerClosure::Releasing,
        AbortWorkerClosure::TerminalRetained,
        AbortWorkerClosure::Gone,
    ] {
        let mut l = ledger(1, 2, cancelled());
        let d = issue(&mut l, 0, 1, &mut ids());
        settle(&mut l, 1, d, worker(closure)).unwrap();
        assert!(l.responsibility_settled());
        assert!(!l.residual_resource_settled());
        assert!(!l.remote_tracking_ended());
        assert_eq!(l.next_abort_at(time(100)), None);
    }
}

#[test]
fn invalid_worker_outcome_and_foreign_actions_preserve_original_issue() {
    let mut l = ledger(1, 2, cancelled());
    let d = issue(&mut l, 0, 1, &mut ids());
    assert_eq!(
        settle(
            &mut l,
            0,
            d,
            AbortIssueSettlement::Worker {
                outcome: OperationOutcome::OwnerFailed,
                closure: AbortWorkerClosure::Gone
            }
        ),
        Err(ParticipationStandDownError::InvalidWorkerSettlement)
    );
    assert_eq!(
        l.settle(
            time(0),
            activation(),
            action(9),
            p(1),
            d.request.envelope().operation_id(),
            worker(AbortWorkerClosure::Gone)
        ),
        Err(ParticipationStandDownError::UnknownAction)
    );
    assert_eq!(
        l.settle(
            time(0),
            activation(),
            d.action,
            p(2),
            d.request.envelope().operation_id(),
            worker(AbortWorkerClosure::Gone)
        ),
        Err(ParticipationStandDownError::IdentityMismatch)
    );
    assert_eq!(
        l.snapshot(p(1)).unwrap().issue_state,
        Some(AbortParticipationIssueState::AwaitingSettlement)
    );
}

#[test]
fn late_worker_receipt_settles_original_operation_without_regressing_the_newer_action() {
    let mut l = ledger(1, 3, cancelled());
    let mut ids = ids();
    let first = issue(&mut l, 0, 1, &mut ids);
    settle(&mut l, 0, first, AbortIssueSettlement::TransportUnknown).unwrap();
    let second = issue(&mut l, 50, 2, &mut ids);
    settle(
        &mut l,
        51,
        first,
        worker(AbortWorkerClosure::TerminalRetained),
    )
    .unwrap();
    settle(&mut l, 52, second, AbortIssueSettlement::DefinitelyUnsent).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().closure,
        ParticipationClosureState::TerminalRetained
    );
    assert_eq!(
        l.snapshot(p(1)).unwrap().issue_state,
        Some(AbortParticipationIssueState::WorkerSettled)
    );
    assert_eq!(l.next_abort_at(time(52)), None);
}

#[test]
fn stale_transport_settlement_cannot_retract_new_authority_or_reset_its_retry() {
    let mut l = ledger(1, 3, cancelled());
    let mut ids = ids();
    let first = issue(&mut l, 0, 1, &mut ids);
    settle(&mut l, 0, first, AbortIssueSettlement::TransportUnknown).unwrap();
    let second = issue(&mut l, 50, 2, &mut ids);
    settle(&mut l, 55, first, AbortIssueSettlement::DefinitelyUnsent).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().issue_state,
        Some(AbortParticipationIssueState::AwaitingSettlement)
    );
    settle(&mut l, 60, second, AbortIssueSettlement::TransportUnknown).unwrap();
    settle(&mut l, 80, first, AbortIssueSettlement::TransportUnknown).unwrap();
    assert_eq!(l.next_abort_at(time(80)), Some(time(110)));
}

#[test]
fn worker_closure_retains_first_branch_even_when_terminal_arrives_first() {
    let mut l = ledger(1, 2, cancelled());
    let d = issue(&mut l, 0, 1, &mut ids());
    settle(&mut l, 0, d, worker(AbortWorkerClosure::TerminalRetained)).unwrap();
    settle(&mut l, 1, d, worker(AbortWorkerClosure::Aborting)).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().closure,
        ParticipationClosureState::TerminalRetained
    );
    assert_eq!(
        settle(&mut l, 2, d, worker(AbortWorkerClosure::Releasing)),
        Err(ParticipationStandDownError::ConflictingWorkerSettlement)
    );
    settle(&mut l, 3, d, worker(AbortWorkerClosure::Gone)).unwrap();
    settle(&mut l, 4, d, worker(AbortWorkerClosure::TerminalRetained)).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().closure,
        ParticipationClosureState::Gone
    );
}

#[test]
fn failed_closed_preserves_residual_and_positive_registry_fact_can_finish_it() {
    let mut l = ledger(1, 2, cancelled());
    let d = issue(&mut l, 0, 1, &mut ids());
    settle(&mut l, 0, d, AbortIssueSettlement::FailedClosed).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().issue_state,
        Some(AbortParticipationIssueState::FailedClosed)
    );
    assert!(!l.responsibility_settled());
    l.observe_registry(
        time(1),
        p(1),
        RegistryParticipationConvergence::WorkerProcessReplaced,
    )
    .unwrap();
    assert!(l.residual_resource_settled());
    settle(&mut l, 2, d, worker(AbortWorkerClosure::Releasing)).unwrap();
    assert_eq!(
        l.snapshot(p(1)).unwrap().closure,
        ParticipationClosureState::WorkerProcessReplaced
    );
}

#[test]
fn finite_tracking_is_not_a_resource_proof_and_never_erases_positive_evidence() {
    let mut l = ledger(2, 1, cancelled());
    assert_eq!(
        l.observe_registry(
            time(99),
            p(1),
            RegistryParticipationConvergence::RemoteTrackingEnded
        ),
        Err(ParticipationStandDownError::WrongState)
    );
    l.observe_registry(
        time(0),
        p(1),
        RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced,
    )
    .unwrap();
    l.observe_registry(
        time(100),
        p(1),
        RegistryParticipationConvergence::RemoteTrackingEnded,
    )
    .unwrap();
    l.observe_registry(
        time(100),
        p(2),
        RegistryParticipationConvergence::RemoteTrackingEnded,
    )
    .unwrap();
    assert!(l.remote_tracking_ended());
    assert!(!l.residual_resource_settled());
    assert_eq!(
        l.snapshot(p(1)).unwrap().closure,
        ParticipationClosureState::WorkerStoppedAndParticipationFenced
    );
    l.observe_registry(
        time(101),
        p(2),
        RegistryParticipationConvergence::WorkerProcessReplaced,
    )
    .unwrap();
    assert!(l.residual_resource_settled());
}

#[test]
fn successful_cleanup_observes_remote_facts_without_stop_or_abort() {
    let mut l = ledger(1, 2, StandDownMode::SuccessfulCleanup);
    assert!(matches!(
        l.poll_next_abort(time(0), action(1), &mut ids(), &mut Budget(1))
            .unwrap(),
        AbortPoll::Idle
    ));
    assert_eq!(l.next_abort_at(time(100)), None);
    l.observe_registry(
        time(1),
        p(1),
        RegistryParticipationConvergence::NeverEstablished,
    )
    .unwrap();
    assert!(l.residual_resource_settled());
}

#[test]
fn deadline_only_shortens_and_idle_does_not_self_wake() {
    let mut l = ledger(1, 2, cancelled());
    let d = issue(&mut l, 0, 1, &mut ids());
    assert_eq!(l.next_abort_at(time(0)), None);
    l.shorten_remote_deadline(time(500));
    assert_eq!(
        l.observe_registry(
            time(99),
            p(1),
            RegistryParticipationConvergence::RemoteTrackingEnded
        ),
        Err(ParticipationStandDownError::WrongState)
    );
    l.shorten_remote_deadline(time(50));
    l.observe_registry(
        time(50),
        p(1),
        RegistryParticipationConvergence::RemoteTrackingEnded,
    )
    .unwrap();
    settle(&mut l, 51, d, AbortIssueSettlement::TransportUnknown).unwrap();
    assert_eq!(l.next_abort_at(time(51)), None);
}
