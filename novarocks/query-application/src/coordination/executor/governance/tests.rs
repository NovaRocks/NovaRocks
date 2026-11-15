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
use novarocks_types::identity::{AttemptId, QueryExecutionId};
use novarocks_workload_control::{
    ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};

fn control() -> WorkloadControl {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 128,
            control_bytes: 16,
            per_scope_bytes: 112,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    control
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        3,
        QueryExecutionId::new(QueryId::new(4, 8), AttemptId::new(1).unwrap()).unwrap(),
        2,
    )
}
fn executor(control: &WorkloadControl) -> GovernanceExecutor {
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
    root.business.release();
    GovernanceExecutor::new(
        activation().execution().query_id(),
        activation().actor(),
        root.owner,
        stage,
    )
    .unwrap()
}
fn register(executor: &GovernanceExecutor, kind: ObligationKind) {
    assert!(matches!(
        executor
            .apply(GovernanceEffect::RegisterObligation {
                activation: activation(),
                kind
            })
            .unwrap(),
        GovernanceFact::ObligationRegistered { .. }
    ));
}
fn finish(executor: &GovernanceExecutor) {
    executor
        .apply(GovernanceEffect::ReleaseExecutionStage)
        .unwrap();
    executor.apply(GovernanceEffect::CompleteWorkOwner).unwrap();
}

#[test]
fn constructor_returns_original_owner_and_foreign_stage() {
    let control = control();
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let original = first.owner.scope();
    let stage = second.owner.scope().try_acquire(Stage::Execution).unwrap();
    let refusal = GovernanceExecutor::new(
        activation().execution().query_id(),
        activation().actor(),
        first.owner,
        stage,
    )
    .err()
    .unwrap();
    assert!(refusal.owner.is_for_scope(&original));
    assert!(
        refusal
            .stage
            .check(&second.owner.scope(), Stage::Execution)
            .is_ok()
    );
    assert!(original.check().is_ok());
    drop(refusal.stage);
    refusal.owner.complete();
    first.business.release();
    second.owner.complete();
    second.business.release();
}

#[test]
fn both_original_obligation_kinds_have_distinct_lifetimes() {
    let control = control();
    let executor = executor(&control);
    register(&executor, ObligationKind::RunningWork);
    register(&executor, ObligationKind::RetiredAttempt);
    assert_eq!(executor.lock().obligations.len(), 2);
    assert!(executor.apply(GovernanceEffect::CompleteWorkOwner).is_err());
    executor
        .apply(GovernanceEffect::ResolveObligation {
            activation: activation(),
            kind: ObligationKind::RunningWork,
        })
        .unwrap();
    assert!(executor.apply(GovernanceEffect::CompleteWorkOwner).is_err());
    executor
        .apply(GovernanceEffect::EndRemoteTracking {
            activation: activation(),
            kind: ObligationKind::RetiredAttempt,
        })
        .unwrap();
    // Matching replays are idempotent; contradictory endings cannot replace
    // the original positive fact versus the end-of-tracking decision.
    executor
        .apply(GovernanceEffect::EndRemoteTracking {
            activation: activation(),
            kind: ObligationKind::RetiredAttempt,
        })
        .unwrap();
    assert!(
        executor
            .apply(GovernanceEffect::ResolveObligation {
                activation: activation(),
                kind: ObligationKind::RetiredAttempt
            })
            .is_err()
    );
    assert!(executor.lock().owner.is_some());
    finish(&executor);
    assert!(executor.lock().owner.is_none());
}

#[test]
fn false_original_settlement_retains_record_and_blocks_owner_completion() {
    for end in [End::Resolved, End::RemoteTrackingEnded] {
        let control = control();
        let executor = executor(&control);
        register(&executor, ObligationKind::RunningWork);
        let key = executor
            .key(activation(), ObligationKind::RunningWork)
            .unwrap();
        let original = executor.lock().obligations[&key]
            .original
            .as_ref()
            .unwrap()
            .clone();
        assert!(original.resolve()); // Another original consumer won the race.
        let error = executor
            .settle(activation(), ObligationKind::RunningWork, end)
            .unwrap_err();
        let fact = error.execution_failure().unwrap().fact();
        assert_eq!(fact.reason().code(), 93);
        assert_eq!(fact.class(), FailureClass::Unexpected);
        let mut state = executor.lock();
        let record = &state.obligations[&key];
        assert!(record.original.is_some());
        assert_eq!(record.ended, None);
        assert!(state.owner.is_some());
        // Test cleanup consumes the already-resolved fixture, without asking
        // product code to manufacture an end fact for the false settlement.
        state.obligations.clear();
        drop(state);
        finish(&executor);
    }
}

#[test]
fn client_close_requests_original_latch_and_preserves_first_winner() {
    let control = control();
    let executor = executor(&control);
    let cancellation = executor.scope().cancellation().unwrap();
    executor
        .apply(GovernanceEffect::RequestClientCancellation)
        .unwrap();
    executor
        .apply(GovernanceEffect::RequestShutdownCancellation)
        .unwrap();
    assert_eq!(
        cancellation.reason(),
        Some(CancellationReason::ClientDisconnected)
    );
    finish(&executor);
}

#[test]
fn foreign_activation_cannot_register_an_obligation() {
    let control = control();
    let executor = executor(&control);
    let foreign = AttemptActivationIdentity::from_parts(9, activation().execution(), 2);
    assert!(
        executor
            .apply(GovernanceEffect::RegisterObligation {
                activation: foreign,
                kind: ObligationKind::RunningWork
            })
            .is_err()
    );
    assert!(executor.lock().obligations.is_empty());
    finish(&executor);
}

#[test]
fn cancellation_during_install_returns_both_original_objects() {
    let control = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let stage = scope.try_acquire(Stage::Execution).unwrap();
    root.owner.cancel(CancellationReason::Requested);
    let refusal = GovernanceExecutor::new(
        activation().execution().query_id(),
        activation().actor(),
        root.owner,
        stage,
    )
    .err()
    .unwrap();
    assert!(refusal.owner.is_for_scope(&scope));
    // The returned stage remains the original paid stage even though its
    // cancellation check now refuses further work.
    assert!(refusal.stage.check(&scope, Stage::Execution).is_err());
    drop(refusal.stage);
    refusal.owner.complete_after_terminal_cancel_settled();
    root.business.release();
}

#[test]
fn original_terminal_cancel_control_is_settled_only_by_explicit_owner_completion() {
    let control = control();
    let executor = executor(&control);
    let cancellation = executor.scope().cancellation().unwrap();
    executor
        .apply(GovernanceEffect::RequestShutdownCancellation)
        .unwrap();
    assert_eq!(
        cancellation.reason(),
        Some(CancellationReason::ServerShutdown)
    );
    assert_eq!(control.snapshot().root_responsibilities, 1);
    assert!(executor.apply(GovernanceEffect::CompleteWorkOwner).is_err());
    assert_eq!(control.snapshot().root_responsibilities, 1);
    finish(&executor);
    let snapshot = control.snapshot();
    assert_eq!(snapshot.root_responsibilities, 0);
    assert!(snapshot.scopes.is_empty());
}

#[test]
fn successful_original_owner_completion_never_manufactures_cancellation() {
    let control = control();
    let executor = executor(&control);
    let cancellation = executor.scope().cancellation().unwrap();
    executor
        .lock()
        .owner
        .as_ref()
        .unwrap()
        .success_sealer()
        .seal()
        .unwrap();
    finish(&executor);
    assert_eq!(cancellation.reason(), None);
    assert_eq!(control.snapshot().root_responsibilities, 0);
}
