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

//! The actual Native receive F only commits paid ordered observations.
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
use novarocks_execution_contract::{TaskIdentity, TaskStatus};
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, encode_covered_status_event,
};
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_status_receive_path_only_enqueues_and_wakes() {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let original_key = key();
    let f = Fixture::make_with_captures(
        4,
        4,
        true,
        Some(liveness::Trace {
            key: original_key,
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let participation = f.key.participation;
    let first = f.input.root_binding().root();
    let second = TaskIdentity::new(
        first.query_execution_id(),
        first.stage_id(),
        TaskId::new(2).unwrap(),
        first.backend_process_id(),
    );
    let statuses = [TaskStatus::created(first), TaskStatus::created(second)];
    let mut peer = Loopback::start().await;
    peer.expect_covered_frames_then_hold(
        statuses
            .iter()
            .enumerate()
            .map(|(index, status)| {
                encode_covered_status_event(&CoveredStatusStreamEvent {
                    fact: CoveredStatusStreamFact::Status(status.clone()),
                    source_revision: Some(index as u64 + 1),
                })
                .unwrap()
            })
            .collect(),
    );
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
    let generation = NonZeroU64::new(3).unwrap();
    let stream = StreamKey {
        participation,
        generation,
    };
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([first, second]),
    });
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let mut original = start_generation(&f, &source, request.clone(), client);
    let id = original_transport_join_id(&original.role).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let accepted = {
                let state = f.input.0.state.lock().unwrap();
                state.streams.get(&stream).unwrap().accepted_unretired
            };
            if accepted == 2 && peer.subscription_requests().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        capture.lock().unwrap().is_empty(),
        "the native receive future cannot consume AP input or run entity decisions"
    );
    assert!(
        f.bank
            .state
            .lock()
            .unwrap()
            .claims
            .values()
            .filter(|key| **key == Some(stream))
            .count()
            >= 2,
        "both original received frames keep their paid ordinary positions before AP consumption"
    );
    let current = f
        .input
        .read_state(stream, token(9), MonotonicInstant::ORIGIN)
        .unwrap();
    assert!(!current.closed);
    assert!(current.opened_at.is_some());
    assert_eq!(peer.subscription_requests().len(), 1);
    assert_eq!(peer.open_covered_subscriptions(), 1);
    assert!(
        peer.applied_requests().is_empty() && peer.control_requests() == 0,
        "receive cannot settle acknowledgements, cancel, Abort or touch leases"
    );
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    consume_actual_inputs(&f, &capture, &[&original.phase], &mut observed);
    f.drain_progress();
    assert_eq!(
        observed.len(),
        2,
        "the actual runtime was woken with both ordered original facts"
    );
    for (index, (row, status)) in observed.iter().zip(statuses.iter()).enumerate() {
        assert_eq!(row.participation, participation);
        assert_eq!(row.generation, generation);
        assert_eq!(
            row.event,
            ObservationEvent::Fact {
                fact: ObservationFact::Status(status.clone()),
                source_revision: Some(index as u64 + 1)
            }
        );
    }
    assert!(peer.applied_requests().is_empty() && peer.control_requests() == 0);
    assert_eq!(*original.result.lock().unwrap(), None);
    assert_eq!(original_transport_join_id(&original.role), Some(id));
    source.close_generation(generation).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            poll_fn(|cx| original.role.poll_shared_completion(cx))
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(
        *original.result.lock().unwrap(),
        Some(SubscriptionEnd::CoreClosed)
    );
    consume_actual_inputs(&f, &capture, &[&original.phase], &mut observed);
    f.drain_progress();
    assert_eq!(
        observed.len(),
        2,
        "local close never manufactures a terminal entity or EOF"
    );
    drop(observed);
    retire_actual_work(&f, &original);
    assert!(Arc::ptr_eq(
        &source.reap_generation(generation).unwrap(),
        &request
    ));
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((
        original,
        request,
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
    assert!(peer.applied_requests().is_empty() && peer.control_requests() == 0);
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
