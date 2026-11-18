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

//! A real old Native read loan returns before the next immutable generation.
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
use novarocks_attempt_protocol::{input::ReceivedObservation, observation::SubscriptionRequest};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::{TaskStatus, TaskStatusCursor};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::{
    CoveredObservationBookmark, CoveredStatusStreamEvent, CoveredStatusStreamFact,
    decode_covered_subscribe_task_status, encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_reconciliation_releases_an_unread_stream_permit() {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let k = key();
    let f = Fixture::make_with_captures(
        1,
        3,
        true,
        Some(liveness::Trace {
            key: k,
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let p = f.key.participation;
    let task = f.input.root_binding().root();
    let status = TaskStatus::created(task);
    let mut peer = Loopback::start().await;
    peer.expect_covered_frames_then_hold(vec![
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Status(status.clone()),
            source_revision: Some(1),
        })
        .unwrap(),
    ]);
    peer.expect_covered_frames_then_hold(vec![
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: 3,
                sequence: 1,
                covered_prefix: 0,
                source_cut: 1,
            }),
            source_revision: None,
        })
        .unwrap(),
    ]);
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
    let source = SubscriptionSource::from_cohort(p, &scope, &cohort, f.input.clone()).unwrap();
    let child = source.scope().unwrap();
    let old = NonZeroU64::new(2).unwrap();
    let next = NonZeroU64::new(3).unwrap();
    let old_key = StreamKey {
        participation: p,
        generation: old,
    };
    let new_key = StreamKey {
        participation: p,
        generation: next,
    };
    let request = Arc::new(SubscriptionRequest {
        participation: p,
        generation: old,
        status_cursors: Box::new([TaskStatusCursor::unobserved(task)]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([task]),
    });
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(p.backend_process_id())]),
        0,
        native.clone(),
    );
    let mut first = start_generation(&f, &source, request.clone(), client.clone());
    let first_id = original_transport_join_id(&first.role).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let held = f
                .bank
                .state
                .lock()
                .unwrap()
                .claims
                .values()
                .filter(|key| **key == Some(old_key))
                .count();
            if held == 2 && peer.subscription_requests().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(capture.lock().unwrap().is_empty());
    assert_eq!(
        f.input
            .0
            .state
            .lock()
            .unwrap()
            .streams
            .get(&old_key)
            .unwrap()
            .accepted_unretired,
        1
    );
    source.close_generation(old).unwrap();
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
    assert_eq!(
        *first.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    assert_eq!(original_transport_join_id(&first.role), Some(first_id));
    assert_eq!(
        f.bank
            .state
            .lock()
            .unwrap()
            .claims
            .values()
            .filter(|key| **key == Some(old_key))
            .count(),
        1,
        "actual old pending network read loan returns; original queued status stays paid"
    );
    assert!(
        source.reap_generation(old).is_err(),
        "actual Join cannot retire the unconsumed old metadata prefix"
    );
    let replay = Arc::new(SubscriptionRequest {
        generation: next,
        ..(*request).clone()
    });
    let mut second = start_generation(&f, &source, replay.clone(), client);
    let second_id = original_transport_join_id(&second.role).unwrap();
    assert_ne!(first_id, second_id);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if f.bank
                .state
                .lock()
                .unwrap()
                .claims
                .values()
                .filter(|key| **key == Some(new_key))
                .count()
                == 1
                && f.input
                    .0
                    .state
                    .lock()
                    .unwrap()
                    .streams
                    .get(&new_key)
                    .unwrap()
                    .accepted_unretired
                    == 1
                && peer.subscription_requests().len() == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        capture.lock().unwrap().is_empty(),
        "new actual read acquired the returned original permit before any AP metadata consumption"
    );
    assert_eq!(f.bank.state.lock().unwrap().claims.len(), 2);
    let physical = decode_covered_subscribe_task_status(
        &peer.subscription_requests()[1],
        FieldPath::root("same_unread_cursor_reconciliation"),
    )
    .unwrap();
    assert_eq!(physical.generation, next);
    assert_eq!(physical.status_cursors[0].current_version(), None);
    assert!(Arc::ptr_eq(&source.applied_request(old).unwrap(), &request));
    assert!(Arc::ptr_eq(&source.applied_request(next).unwrap(), &replay));
    source.close_generation(next).unwrap();
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
    assert_eq!(
        *second.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    consume_actual_inputs(&f, &capture, &[&first.phase, &second.phase], &mut observed);
    f.drain_progress();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].generation, old);
    assert_eq!(
        observed[0].event,
        ObservationEvent::Fact {
            fact: ObservationFact::Status(status),
            source_revision: Some(1)
        }
    );
    assert_eq!(observed[1].generation, next);
    assert!(matches!(
        observed[1].event,
        ObservationEvent::Fact {
            fact: ObservationFact::Bookmark {
                generation: 3,
                sequence: 1,
                ..
            },
            source_revision: None
        }
    ));
    drop(observed);
    retire_actual_work(&f, &first);
    retire_actual_work(&f, &second);
    assert!(Arc::ptr_eq(&source.reap_generation(old).unwrap(), &request));
    assert!(Arc::ptr_eq(&source.reap_generation(next).unwrap(), &replay));
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), child.wait_released())
        .await
        .unwrap();
    assert!(peer.applied_requests().is_empty() && peer.control_requests() == 0);
    drop((
        first, second, request, replay, source, child, cohort, grant, capacity, scope,
    ));
    let until = std::time::Instant::now() + Duration::from_secs(3);
    native.drain_original_apply_until(until).await.unwrap();
    native.drain_native_outgoing_until(until).await.unwrap();
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
