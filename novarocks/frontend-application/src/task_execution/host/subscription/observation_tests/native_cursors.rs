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

//! Actual immutable RPCs follow explicit current-AP generation decisions.
use super::native_all_frames::{consume_actual_inputs, retire_actual_work, start_generation};
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        custody::tests::original_transport_join_id,
        subscriptions::{SubscriptionEnd, SubscriptionSource},
    },
};
use novarocks_attempt_protocol::{
    ids::IdSeed,
    input::ReceivedObservation,
    observation::{
        SubscriptionRequest,
        controller::{
            CoveredSubscription, ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
        },
    },
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::{
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, ParticipationConvergenceState,
    ParticipationConvergenceVersion, TaskStatus, TaskStatusCursor, TaskStatusVersion,
};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, decode_covered_subscribe_task_status,
    encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;

#[derive(Clone, Copy)]
enum Case {
    LatestApplied,
    ParticipationCursor,
    TaskReconciliation,
    DroppedStatus,
}
async fn actual_cursor_reopen(case: Case) {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let original_key = key();
    let f = Fixture::make_with_captures(
        8,
        8,
        true,
        Some(liveness::Trace {
            key: original_key,
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let mut peer = Loopback::start().await;
    let participation = f.key.participation;
    let task = f.input.root_binding().root();
    let has_status = !matches!(case, Case::ParticipationCursor);
    let has_participation = matches!(case, Case::ParticipationCursor | Case::TaskReconciliation);
    let eof = matches!(case, Case::ParticipationCursor | Case::DroppedStatus);
    let original_status = TaskStatus::created(task);
    let original_receipt = ParticipationConvergenceReceipt::new(
        participation,
        ParticipationConvergenceVersion::FIRST,
        ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
    );
    let mut events = Vec::new();
    if has_status {
        events.push(CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Status(original_status.clone()),
            source_revision: Some(1),
        });
    }
    if has_participation {
        events.push(CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::ParticipationConvergence(original_receipt),
            source_revision: Some(2),
        });
    }
    let wire = events
        .iter()
        .map(|event| encode_covered_status_event(event).unwrap())
        .collect();
    if eof {
        peer.expect_covered_frames_then_close(wire);
    } else {
        peer.expect_covered_frames_then_hold(wire);
    }
    peer.expect_covered_frames_then_hold(Vec::new());
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    wc::bind(&scope, &mut driver);
    let grant = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let cohort = wc::cohort(&scope, 1, &mut driver);
    let source =
        SubscriptionSource::from_cohort(participation, &scope, &cohort, f.input.clone()).unwrap();
    let source_scope = source.scope().unwrap();
    let first_generation = NonZeroU64::new(3).unwrap();
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation: first_generation,
        status_cursors: if has_status {
            Box::new([TaskStatusCursor::unobserved(task)])
        } else {
            Box::new([])
        },
        task_convergence_cursors: Box::new([]),
        participation_cursor: has_participation
            .then(|| ParticipationConvergenceCursor::unobserved(participation)),
        quiesce_cursor: None,
        required_identities: if has_status {
            Box::new([task])
        } else {
            Box::new([])
        },
    });
    let mut policy = CoveredSubscription::new(participation, IdSeed::new(32, 1), 4).unwrap();
    policy.activate();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, first_generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open {
            generation: first_generation
        })
    );
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let mut first = start_generation(&f, &source, request.clone(), client.clone());
    let first_join = original_transport_join_id(&first.role).unwrap();
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            consume_actual_inputs(&f, &capture, &[&first.phase], &mut observed);
            if observed.len() == events.len() + usize::from(eof)
                && peer.subscription_requests().len() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let physical = decode_covered_subscribe_task_status(
        &peer.subscription_requests()[0],
        FieldPath::root("first_original_subscription"),
    )
    .unwrap();
    assert_eq!(physical.participation, participation);
    assert_eq!(physical.generation, first_generation);
    if has_status {
        assert_eq!(physical.status_cursors[0].identity(), task);
        assert_eq!(physical.status_cursors[0].current_version(), None);
    } else {
        assert!(physical.status_cursors.is_empty());
    }
    assert_eq!(
        physical
            .participation_cursor
            .and_then(|c| c.current_version()),
        None
    );
    assert_eq!(observed.len(), events.len() + usize::from(eof));
    for (row, expected) in observed.iter().zip(events.iter()) {
        assert_eq!(row.participation, participation);
        assert_eq!(row.generation, first_generation);
        match (&row.event, &expected.fact) {
            (
                ObservationEvent::Fact {
                    fact: ObservationFact::Status(actual),
                    source_revision,
                },
                CoveredStatusStreamFact::Status(expected),
            ) => {
                assert_eq!(actual, expected);
                assert_eq!(*source_revision, Some(1));
            }
            (
                ObservationEvent::Fact {
                    fact: ObservationFact::ParticipationConvergence(actual),
                    source_revision,
                },
                CoveredStatusStreamFact::ParticipationConvergence(expected),
            ) => {
                assert_eq!(actual, expected);
                assert_eq!(*source_revision, Some(2));
            }
            _ => panic!("the exact original real RPC facts must reach their one ordered route"),
        }
        assert!(matches!(
            policy.observe(row.clone()).unwrap(),
            ObservationDisposition::ApplyEntity(_)
        ));
    }
    let applied = Arc::new(SubscriptionRequest {
        status_cursors: if has_status {
            Box::new([TaskStatusCursor::at(task, TaskStatusVersion::FIRST)])
        } else {
            Box::new([])
        },
        participation_cursor: has_participation.then(|| {
            ParticipationConvergenceCursor::at(
                participation,
                ParticipationConvergenceVersion::FIRST,
            )
        }),
        ..(*request).clone()
    });
    source.adjust_read_window(applied.clone()).unwrap();
    assert!(Arc::ptr_eq(
        &source.applied_request(first_generation).unwrap(),
        &applied
    ));
    assert_eq!(
        peer.subscription_requests().len(),
        1,
        "applied cursor update cannot start another actual RPC"
    );
    if eof {
        let closed = observed.last().unwrap();
        assert_eq!(closed.event, ObservationEvent::Closed);
        assert_eq!(
            policy.observe(closed.clone()).unwrap(),
            ObservationDisposition::Ignore
        );
        assert!(
            matches!(policy.status(), SubscriptionStatus::Backoff { .. }),
            "a lost observation channel is not an entity failure"
        );
    } else {
        assert_eq!(policy.status(), SubscriptionStatus::Live);
        policy.reconcile(MonotonicInstant::ORIGIN);
    }
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, first_generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Close {
            generation: first_generation
        })
    );
    assert_eq!(
        policy.next_deadline(MonotonicInstant::ORIGIN),
        None,
        "no expired backoff poll while original job exit is outstanding"
    );
    assert_eq!(
        policy
            .drive(
                MonotonicInstant::from_origin(Duration::from_secs(100)),
                first_generation,
                None
            )
            .unwrap(),
        None
    );
    assert_eq!(peer.subscription_requests().len(), 1);
    source.close_generation(first_generation).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            poll_fn(|cx| first.role.poll_shared_completion(cx))
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(original_transport_join_id(&first.role), Some(first_join));
    assert_eq!(
        *first.result.lock().unwrap(),
        Some(if eof {
            SubscriptionEnd::RemoteClosed
        } else {
            SubscriptionEnd::CoreClosed
        })
    );
    consume_actual_inputs(&f, &capture, &[&first.phase], &mut observed);
    f.clear_progress_debts_after_publishers_exit();
    retire_actual_work(&f, &first);
    assert!(Arc::ptr_eq(
        &source.reap_generation(first_generation).unwrap(),
        &applied
    ));
    policy.observe_actual_exit(first_generation).unwrap();
    let at = policy.next_deadline(MonotonicInstant::ORIGIN).unwrap();
    *f.clock.0.lock().unwrap() = at.since_origin();
    let Some(SubscriptionDirective::Open {
        generation: second_generation,
    }) = policy.drive(at, first_generation, None).unwrap()
    else {
        panic!("the current AP selects one successor only after original actual exit");
    };
    assert_eq!(second_generation.get(), 4);
    let replay = Arc::new(SubscriptionRequest {
        generation: second_generation,
        ..(*applied).clone()
    });
    let mut second = start_generation(&f, &source, replay.clone(), client);
    let second_join = original_transport_join_id(&second.role).unwrap();
    assert_ne!(first_join, second_join);
    tokio::time::timeout(Duration::from_secs(3), async {
        while peer.subscription_requests().len() != 2
            || peer.open_covered_subscriptions() != 1
            || f.input
                .read_state(
                    StreamKey {
                        participation,
                        generation: second_generation,
                    },
                    token(8),
                    at,
                )
                .unwrap()
                .opened_at
                .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second_physical = decode_covered_subscribe_task_status(
        &peer.subscription_requests()[1],
        FieldPath::root("same_applied_cursor_successor"),
    )
    .unwrap();
    assert_eq!(second_physical.participation, participation);
    assert_eq!(second_physical.generation, second_generation);
    if has_status {
        assert_eq!(second_physical.status_cursors.len(), 1);
        assert_eq!(second_physical.status_cursors[0].identity(), task);
        assert_eq!(
            second_physical.status_cursors[0].current_version(),
            Some(TaskStatusVersion::FIRST)
        );
    } else {
        assert!(second_physical.status_cursors.is_empty());
    }
    assert_eq!(
        second_physical
            .participation_cursor
            .and_then(|c| c.current_version()),
        has_participation.then_some(ParticipationConvergenceVersion::FIRST)
    );
    assert!(Arc::ptr_eq(
        &source.applied_request(second_generation).unwrap(),
        &replay
    ));
    assert_eq!(peer.subscription_requests().len(), 2);
    assert!(
        peer.applied_requests().is_empty() && peer.control_requests() == 0,
        "receive only publishes observations; it cannot settle, Abort or renew"
    );
    source.close_generation(second_generation).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            poll_fn(|cx| second.role.poll_shared_completion(cx))
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(original_transport_join_id(&second.role), Some(second_join));
    assert_eq!(
        *second.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    consume_actual_inputs(&f, &capture, &[&second.phase], &mut observed);
    f.clear_progress_debts_after_publishers_exit();
    assert_eq!(
        observed
            .iter()
            .filter(|row| matches!(row.event, ObservationEvent::Fact { .. }))
            .count(),
        events.len(),
        "reopen does not duplicate any already applied entity"
    );
    drop(observed);
    retire_actual_work(&f, &second);
    assert!(Arc::ptr_eq(
        &source.reap_generation(second_generation).unwrap(),
        &replay
    ));
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((
        first,
        second,
        request,
        applied,
        replay,
        source,
        source_scope,
        cohort,
        grant,
        capacity,
        scope,
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    native.drain_original_apply_until(deadline).await.unwrap();
    native.drain_native_outgoing_until(deadline).await.unwrap();
    assert_eq!(peer.subscription_requests().len(), 2);
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_automatic_reconnect_uses_the_latest_applied_cursor_without_restarting_early() {
    actual_cursor_reopen(Case::LatestApplied).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_participation_aware_stream_retains_convergence_and_reconnects_from_its_cursor() {
    actual_cursor_reopen(Case::ParticipationCursor).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_reconciliation_reopens_one_stream_and_preserves_the_participation_cursor() {
    actual_cursor_reopen(Case::TaskReconciliation).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_status_stream_resubscribes_from_its_cursors() {
    actual_cursor_reopen(Case::DroppedStatus).await;
}
