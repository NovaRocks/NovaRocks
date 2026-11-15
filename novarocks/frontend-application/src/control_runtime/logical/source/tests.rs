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

//! Real logical Core, original Source, paid roles, HostRegistry and Tokio Join.
//! Fake application facts do not prove Native/Worker or P11 failure projection.

mod fixture;
use super::*;
use fixture::*;
use novarocks_control_runtime::actor_turn::{FailureSourceState, InputPhase};
use novarocks_coordination_contract::{
    QueryExecutionErrorKind,
    logical_port::{AttemptTerminal, AttemptToLogicalBody},
};
use novarocks_logical_execution_protocol::{
    cancel::LogicalCancellationReason,
    effect::{ConsumerEffect, GovernanceEffect, LogicalEffectBody, NativeEffect},
    input::RegistryClose,
};
use novarocks_query_application::api::{
    InstalledLogicalExecution, LogicalExit, LogicalHostFact, LogicalJoinReadiness,
    LogicalSourceKind,
};
use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
};

#[tokio::test]
async fn completion_retires_original_source_actor_host_and_scope_after_committed_registry_close() {
    let mut f = Fixture::new(false);
    f.convert_timers = false;
    f.run.open();
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
            )
        })
    })
    .await;
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ))
    .await;
    f.until(|f|f.has(|e|matches!(e,LogicalEffectBody::AttemptPort(p) if matches!(p.body,novarocks_coordination_contract::logical_port::LogicalToAttemptBody::BeginConvergence{..})))).await;
    f.local_converged().await;
    f.close_ap();
    f.until(|f| f.has(|e| matches!(e, LogicalEffectBody::AttemptLifecycleDiagnostic(_))))
        .await;
    for _ in 0..32 {
        f.round().await;
    }
    assert_eq!(
        f.runtime.snapshot(f.registration.actor()).unwrap().inputs,
        0
    );
    assert_eq!(
        f.runtime.next_deadline(),
        Some(Tick(1)),
        "Core retains an immediate continuation; polling ready actors alone never converts it"
    );
    assert!(!f.has(|e| matches!(
        e,
        LogicalEffectBody::Governance(GovernanceEffect::ReleaseExecutionStage)
    )));
    // Match the production poller: convert due timers under its finite budget,
    // without advancing to the 100ms remote observation deadline.
    f.convert_timers = true;

    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Governance(GovernanceEffect::CompleteWorkOwner)
            )
        })
    })
    .await;
    assert_eq!(
        f.count(|e| matches!(
            e,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )),
        1
    );
    assert_eq!(
        f.count(|e| matches!(
            e,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(_))
        )),
        0
    );
    assert!(
        !f.runtime.is_empty() && !f.host.is_empty() && !f.ledger.is_empty(),
        "business completion alone cannot remove the registered original"
    );
    f.until(|f| f.join_ready.load(std::sync::atomic::Ordering::Acquire))
        .await;
    let installed = Arc::clone(&f.state);
    assert_eq!(
        f.wait(installed.join_readiness(false)).await.unwrap(),
        LogicalJoinReadiness::Ready
    );
    assert!(matches!(
        installed.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    installed.request_close(RegistryClose::Retire);
    assert_eq!(
        f.wait(installed.join_readiness(true)).await.unwrap(),
        LogicalJoinReadiness::Ready
    );
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
    assert_eq!(
        installed.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(LogicalExit::Completed)
    );
    assert_eq!(
        f.executor_drops.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "original application is actually disposed under its own controlled cleanup"
    );
    assert!(!f.failure.failed());
    assert!(!f.host.contains_logical(activation().actor()));
}

#[tokio::test]
async fn one_full_snapshot_and_long_native_job_do_not_block_original_cancellation_source() {
    let mut f = Fixture::new(false);
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
            )
        })
    })
    .await;
    let mut snapshot = f.state.source(LogicalSourceKind::Snapshot).unwrap();
    let snapshot_source =
        novarocks_control_runtime_core::identity::ActorId::new(snapshot.id().get()).unwrap();
    let first = snapshot.send(LogicalHostFact::Snapshot {
        token: request_token(701),
    });
    f.wait_named("snapshot 701 first handoff while job held", first)
        .await
        .unwrap();
    f.until(|f| f.count(|e| matches!(e, LogicalEffectBody::SnapshotReply { .. })) == 1)
        .await;
    assert!(
        !f.snapshot.is_open(),
        "the first actual job retains its original Work after handoff retires the input"
    );
    let mut second = snapshot.send(LogicalHostFact::Snapshot {
        token: request_token(702),
    });
    assert!(poll(second.as_mut()).is_pending());
    for _ in 0..8 {
        f.round().await;
    }
    assert_eq!(
        f.count(|e| matches!(e, LogicalEffectBody::SnapshotReply { .. })),
        1,
        "the second effect cannot reuse the held purpose's original result and exit positions"
    );
    let occupied = f
        .runtime
        .standing_input_inventory(f.registration.actor(), None, usize::MAX)
        .unwrap()
        .into_iter()
        .find(|input| input.source == snapshot_source && input.in_flight.is_some())
        .expect("second request still occupies its original standing")
        .in_flight
        .unwrap();
    drop(second); // cancellation of this borrowed wait cannot retire its input
    let mut request = snapshot.send(LogicalHostFact::Snapshot {
        token: request_token(703),
    });
    assert!(
        poll(request.as_mut()).is_pending(),
        "a third fact waits outside the one occupied position"
    );
    let mut cancellation = f.state.source(LogicalSourceKind::Cancellation).unwrap();
    f.wait_named(
        "independent cancellation while snapshot 702 occupied and 703 waiting",
        cancellation.send(LogicalHostFact::Cancelled(
            LogicalCancellationReason::Requested,
        )),
    )
    .await
    .unwrap();
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(_))
            )
        })
    })
    .await;
    assert!(
        !f.run.is_open() && !f.snapshot.is_open(),
        "neither original application job has been released"
    );
    assert!(poll(request.as_mut()).is_pending());
    assert!(
        f.runtime
            .standing_input_inventory(f.registration.actor(), None, usize::MAX)
            .unwrap()
            .iter()
            .any(|input| input.source == snapshot_source && input.in_flight == Some(occupied)),
        "independent cancellation does not counterfeit retirement of the full source"
    );
    assert!(f.has(|e|matches!(e,LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(error)) if error.kind()==QueryExecutionErrorKind::Cancelled)));
    assert!(
        !f.failure.failed(),
        "ordinary cancellation is not owner failure"
    );
    f.snapshot.open();
    f.wait_named(
        "snapshot 703 after releasing the original snapshot job",
        request,
    )
    .await
    .unwrap();
    f.until(|f| f.count(|e| matches!(e, LogicalEffectBody::SnapshotReply { .. })) == 3)
        .await;
    f.run.open();
    f.local_converged().await;
    f.close_ap();
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Governance(GovernanceEffect::CompleteWorkOwner)
            )
        })
    })
    .await;
    f.until(|f| f.join_ready.load(std::sync::atomic::Ordering::Acquire))
        .await;
    let installed = Arc::clone(&f.state);
    installed.request_close(RegistryClose::Retire);
    assert_eq!(
        f.wait_named(
            "snapshot cancellation final registry join readiness",
            installed.join_readiness(true)
        )
        .await
        .unwrap(),
        LogicalJoinReadiness::Ready
    );
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
    assert_eq!(
        installed.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(LogicalExit::Completed)
    );
}

#[tokio::test]
async fn real_native_result_position_is_committed_before_source_supplies_separate_actual_exit_position()
 {
    let mut f = Fixture::new(false);
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
            )
        })
    })
    .await;
    let source = f.geometry.source(1).unwrap();
    assert!(
        f.runtime
            .input_inventory(f.registration.actor(), None, usize::MAX)
            .unwrap()
            .iter()
            .all(|i| i.source != source || i.phase != InputPhase::Filled)
    );
    f.run.open();
    // Let the actual controlled job return, but do NOT drive Source/Host. The
    // result is published from that original body; Source alone owns its Join.
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    let before = f
        .runtime
        .input_inventory(f.registration.actor(), None, usize::MAX)
        .unwrap();
    let result = before
        .iter()
        .filter(|i| i.source == source && i.phase == InputPhase::Filled)
        .collect::<Vec<_>>();
    assert_eq!(
        result.len(),
        1,
        "only the original result occurrence is filled before supervising Join"
    );
    let result_id = result[0].input;
    f.actors_only();
    assert_eq!(
        f.runtime
            .input_custody(f.registration.actor(), result_id)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Committed
    );
    let before_exit = f
        .runtime
        .input_inventory(f.registration.actor(), None, usize::MAX)
        .unwrap();
    assert!(
        before_exit
            .iter()
            .all(|i| i.source != source || i.input == result_id || i.phase != InputPhase::Filled)
    );
    assert!(!f.state.retirable.load(std::sync::atomic::Ordering::Acquire));
    for _ in 0..32 {
        if let Some(turn) = f.runtime.claim_host_actor() {
            f.host.drive(Tick(1), turn);
        } else {
            break;
        }
    }
    let after = f
        .runtime
        .input_inventory(f.registration.actor(), None, usize::MAX)
        .unwrap();
    assert!(
        after
            .iter()
            .any(|i| i.source == source && i.input != result_id && i.phase == InputPhase::Filled),
        "Source fills a distinct actual-exit occurrence after consuming the actual Join"
    );
    // Finish through the same public completion and registry boundaries.
    f.port(AttemptToLogicalBody::AttemptTerminal(
        AttemptTerminal::Completed,
    ))
    .await;
    f.local_converged().await;
    f.close_ap();
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Governance(GovernanceEffect::CompleteWorkOwner)
            )
        })
    })
    .await;
    f.until(|f| f.join_ready.load(std::sync::atomic::Ordering::Acquire))
        .await;
    let installed = Arc::clone(&f.state);
    installed.request_close(RegistryClose::Retire);
    f.wait(installed.join_readiness(true)).await.unwrap();
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
}

#[tokio::test]
async fn original_executor_panic_keeps_failure_cause_and_retires_only_after_actual_source_custody()
{
    let mut f = Fixture::new(true);
    f.until(|f| f.failure.failed()).await;
    f.until(|f| {
        f.runtime
            .snapshot(f.registration.actor())
            .is_ok_and(|s| s.failed)
    })
    .await;
    let original_failure = f.runtime.failure_proof_for(&f.registration).unwrap();
    let budget = start().budgets.remote_cleanup_timeout;
    let original_cut = LogicalFailureCut::from_original(&original_failure, budget);
    assert_eq!(
        original_cut.cut.since_origin(),
        std::time::Duration::from_nanos(original_failure.failed_at().0)
    );
    assert_eq!(
        original_cut
            .remote_deadline
            .saturating_duration_since(original_cut.cut),
        budget
    );
    for _ in 0..4 {
        f.round().await;
    }
    let later_failure = f.runtime.failure_proof_for(&f.registration).unwrap();
    let later_cut = LogicalFailureCut::from_original(&later_failure, budget);
    assert_eq!(later_cut.cut, original_cut.cut);
    assert_eq!(later_cut.remote_deadline, original_cut.remote_deadline);
    assert!(
        Arc::strong_count(&f.panic) >= 3,
        "the original panic Arc remains in raw failure custody"
    );
    assert_ne!(
        f.runtime
            .failure_source_state(f.registration.actor())
            .unwrap(),
        FailureSourceState::Open
    );
    assert!(!f.runtime.is_empty() && !f.host.is_empty() && !f.ledger.is_empty());
    assert!(
        matches!(
            f.state.poll_exit(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ),
        "the original AP producer custody still prevents a physical exit"
    );
    assert!(!f.state.retirable.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        poll(Box::pin(f.state.stopped()).as_mut()).is_ready(),
        "failure stops passive observers without claiming Core retirable"
    );
    assert!(!f.has(|e| matches!(e, LogicalEffectBody::Retirable)));
    assert_eq!(
        f.count(|e| matches!(
            e,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )),
        0
    );
    f.close_ap();
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
    assert!(matches!(
        f.state.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(LogicalExit::OwnerFailed(_))
    ));
    assert_eq!(
        f.executor_drops.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the original App Drop completed under its own prepaid disposal Work"
    );
}

#[test]
fn fixed_logical_geometry_does_not_grow_with_participation_fan_in() {
    let empty = start();
    let mut many = empty.clone();
    many.first_attempt.participations = participations(257).into_boxed_slice();
    let scope = novarocks_control_runtime_core::identity::ScopeId::new(41).unwrap();
    let first = LogicalGeometry::derive(scope, &empty).unwrap();
    let second = LogicalGeometry::derive(scope, &many).unwrap();
    assert_eq!(first.attempts, second.attempts);
    assert_eq!(first.purposes, second.purposes);
    assert_eq!(first.config.mailbox, second.config.mailbox);
    assert_eq!(first.config.max_batches, second.config.max_batches);
    assert_eq!(first.config.max_step_work, second.config.max_step_work);
}

#[tokio::test]
async fn core_turn_panic_preserves_running_work_until_its_actual_original_exit() {
    let mut f = Fixture::new(false);
    f.until(|f| {
        f.has(|e| {
            matches!(
                e,
                LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
            )
        })
    })
    .await;
    f.failure.inject_core_turn_panic();
    f.until(|f| {
        f.runtime
            .snapshot(f.registration.actor())
            .is_ok_and(|s| s.failed)
    })
    .await;
    f.close_ap();
    for _ in 0..32 {
        f.round().await;
    }
    assert_eq!(
        f.executor_drops.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(matches!(
        f.state.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert!(!f.has(|e| matches!(e, LogicalEffectBody::Retirable)));
    f.run.open();
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
    assert!(matches!(
        f.state.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(LogicalExit::OwnerFailed(_))
    ));
    assert_eq!(
        f.executor_drops.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
#[tokio::test]
async fn executor_exit_without_required_result_marks_invariant_failure_and_drains_originals() {
    let mut f = Fixture::new(false);
    f.omit_run_result
        .store(true, std::sync::atomic::Ordering::Release);
    f.until(|f| {
        f.runtime
            .snapshot(f.registration.actor())
            .is_ok_and(|s| s.failed)
    })
    .await;
    let error = f.failure.error();
    assert_eq!(
        error.execution_failure().unwrap().fact().class(),
        novarocks_execution_contract::failure::FailureClass::Invariant
    );
    assert_eq!(
        f.count(|e| matches!(
            e,
            LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
        )),
        0
    );
    f.close_ap();
    f.until(|f| f.runtime.is_empty() && f.host.is_empty() && f.ledger.is_empty())
        .await;
    assert!(matches!(
        f.state.poll_exit(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(LogicalExit::OwnerFailed(_))
    ));
    assert_eq!(
        f.executor_drops.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
