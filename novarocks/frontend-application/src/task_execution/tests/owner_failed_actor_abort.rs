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

//! Owner failure settles the exact Abort send without inventing convergence.

use super::*;
use crate::task_execution::{execution::ActorAbortDispatchState, round::TaskRound};
use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_execution_contract::{SafeDetail, failure::FailureClass};
use novarocks_query_application::{
    coordination::{AbortQueryContextIssueState, ContextClosureState, ContextStandDownSnapshot},
    execution_error::ExecutionFailureSite,
    test_support::LogicalExecutionTestHarness,
};

fn owner_failed_ack(operation: TaskOperationId, kind: OperationKind) -> OperationAcknowledgement {
    OperationAcknowledgement::worker_receipt(
        operation,
        kind,
        OperationOutcome::OwnerFailed,
        AckPayload::None,
    )
    .with_detail(Some(SafeDetail::truncating(
        "the original Abort owner failed",
    )))
}

fn assert_exact_owner_failure(
    error: TaskExecutionError,
    original: &OperationAcknowledgement,
    expected_context: QueryContextRef,
) {
    let TaskExecutionError::OperationFailed {
        context,
        operation_id,
        kind,
        rejection,
    } = &error
    else {
        panic!("OwnerFailed lost its exact Worker verdict: {error:?}");
    };
    assert_eq!(*context, expected_context);
    assert_eq!(*operation_id, original.operation_id());
    assert_eq!(*kind, OperationKind::AbortQueryContext);
    assert_eq!(Some(rejection.clone()), original.worker_header());
    let input = error.preparation_input();
    assert_eq!(input.fact().class(), FailureClass::Invariant);
    assert_eq!(input.fact().reason().code(), 28);
    assert_eq!(
        input.attempt_class(),
        AttemptFailureClass::ContractViolation
    );
    assert_eq!(
        input.site(),
        ExecutionFailureSite::Operation {
            context: expected_context,
            operation_id: original.operation_id(),
            kind: OperationKind::AbortQueryContext,
        }
    );
}

async fn finish_residual(
    round: TaskRound,
    mut logical: LogicalExecutionTestHarness,
    context: QueryContextRef,
) {
    // The failed runner releases its actual queued/frontend carriers. Only
    // the fixture's independent process replacement resolves remote custody.
    drop(round);
    logical
        .observe_worker_process_replaced(context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

async fn wait_for_adapter_replay(round: &TaskRound) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while round.queued_abort_effects() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original actor authorizes a bounded replay");
}

async fn wait_for_owner_failure(
    logical: &LogicalExecutionTestHarness,
    context: QueryContextRef,
) -> Result<ContextStandDownSnapshot, tokio::time::error::Elapsed> {
    // Settlement queues an event for the logical actor. Its snapshot command
    // is a separate input and does not acknowledge consumption of that event.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = logical.stand_down_snapshot(context).await.unwrap().unwrap();
            if snapshot.issue_state() == Some(AbortQueryContextIssueState::FailedClosed)
                && snapshot.closure() == ContextClosureState::AbortIssueExhausted
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_owner_failed_abort_settles_dispatch_and_preserves_remote_responsibility() {
    let (mut round, _sink, acknowledgements, logical, context, abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    let ack = owner_failed_ack(abort.operation_id(), OperationKind::AbortQueryContext);
    acknowledgements.publish(ack.clone());
    let result = round.turn();
    let dispatch = round
        .execution()
        .actor_abort_dispatch_state(abort.operation_id());
    let released = round.execution().owner(context).unwrap().is_released();
    let converged = round
        .execution()
        .context_convergence_receipt(context)
        .is_some();
    let snapshot = wait_for_owner_failure(&logical, context).await;
    finish_residual(round, logical, context).await;

    assert_exact_owner_failure(result.unwrap_err(), &ack, context);
    assert!(matches!(
        dispatch,
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert!(!released);
    assert!(!converged);
    let snapshot = snapshot.expect("the original logical actor consumes the failed-closed event");
    assert_eq!(
        snapshot.issue_state(),
        Some(AbortQueryContextIssueState::FailedClosed)
    );
    assert_eq!(snapshot.closure(), ContextClosureState::AbortIssueExhausted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_owner_failed_abort_settles_its_original_late_authority_without_a_closure_receipt() {
    let (mut round, _sink, acknowledgements, logical, context, abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    acknowledgements.publish(OperationAcknowledgement::transport_unknown(
        abort.operation_id(),
        OperationKind::AbortQueryContext,
    ));
    round
        .turn()
        .expect("the original transport exits with an unknown outcome");
    let ack = owner_failed_ack(abort.operation_id(), OperationKind::AbortQueryContext);
    acknowledgements.publish(ack.clone());
    let result = round.turn();
    let dispatch = round
        .execution()
        .actor_abort_dispatch_state(abort.operation_id());
    let released = round.execution().owner(context).unwrap().is_released();
    let converged = round
        .execution()
        .context_convergence_receipt(context)
        .is_some();
    let snapshot = wait_for_owner_failure(&logical, context).await;
    finish_residual(round, logical, context).await;

    assert_exact_owner_failure(result.unwrap_err(), &ack, context);
    assert!(matches!(
        dispatch,
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert!(!released);
    assert!(!converged);
    let snapshot = snapshot.expect("the original logical actor consumes the failed-closed event");
    assert_eq!(
        snapshot.issue_state(),
        Some(AbortQueryContextIssueState::FailedClosed)
    );
    assert_eq!(snapshot.closure(), ContextClosureState::AbortIssueExhausted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_failed_late_abort_cancels_the_actual_dispatcher_queued_replay() {
    let (mut round, sink, acknowledgements, logical, context, abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    acknowledgements.publish(OperationAcknowledgement::transport_unknown(
        abort.operation_id(),
        OperationKind::AbortQueryContext,
    ));
    round
        .turn()
        .expect("the original transport exits with an unknown outcome");
    wait_for_adapter_replay(&round).await;
    for intent in sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
    {
        if intent.kind() == OperationKind::UpdateQueryContext {
            acknowledgements.publish(establish_ack(&intent));
        }
    }
    sink.set_submit_open(false);
    round
        .turn()
        .expect("the same replay remains dispatcher-queued under backpressure");
    let queued = round
        .execution()
        .actor_abort_dispatch_state(abort.operation_id());
    let paid_queued = sink.live_queue_permits();
    let ack = owner_failed_ack(abort.operation_id(), OperationKind::AbortQueryContext);
    acknowledgements.publish(ack.clone());
    let result = round.turn();
    let dispatch = round
        .execution()
        .actor_abort_dispatch_state(abort.operation_id());
    let paid_after = sink.live_queue_permits();
    let released = round.execution().owner(context).unwrap().is_released();
    let converged = round
        .execution()
        .context_convergence_receipt(context)
        .is_some();
    let snapshot = wait_for_owner_failure(&logical, context).await;
    finish_residual(round, logical, context).await;

    assert_eq!(queued.unwrap(), ActorAbortDispatchState::Queued);
    assert_eq!(paid_queued, 1);
    assert_exact_owner_failure(result.unwrap_err(), &ack, context);
    assert!(matches!(
        dispatch,
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert_eq!(paid_after, 0);
    assert!(!released);
    assert!(!converged);
    let snapshot = snapshot.expect("the original logical actor consumes the failed-closed event");
    assert_eq!(
        snapshot.issue_state(),
        Some(AbortQueryContextIssueState::FailedClosed)
    );
    assert_eq!(snapshot.closure(), ContextClosureState::AbortIssueExhausted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_failed_abort_still_requires_its_original_operation_and_kind() {
    for wrong_kind in [true, false] {
        let (mut round, _sink, acknowledgements, logical, context, abort, _clock) =
            actor_abort_round_at_first_dispatch().await;
        let (operation, kind) = if wrong_kind {
            (abort.operation_id(), OperationKind::CancelTask)
        } else {
            (TaskOperationId::new_v7(), OperationKind::AbortQueryContext)
        };
        acknowledgements.publish(owner_failed_ack(operation, kind));
        let result = round.turn();
        let dispatch = round
            .execution()
            .actor_abort_dispatch_state(abort.operation_id());
        let released = round.execution().owner(context).unwrap().is_released();
        let converged = round
            .execution()
            .context_convergence_receipt(context)
            .is_some();
        finish_residual(round, logical, context).await;

        if wrong_kind {
            assert!(matches!(result, Err(TaskExecutionError::Schedule(_))));
        } else {
            assert!(matches!(result, Err(TaskExecutionError::UnknownOperation)));
        }
        assert_eq!(dispatch.unwrap(), ActorAbortDispatchState::InFlight);
        assert!(!released);
        assert!(!converged);
    }
}
