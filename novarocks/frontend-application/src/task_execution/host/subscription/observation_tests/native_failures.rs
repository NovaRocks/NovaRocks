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

//! Actual connection failures and remote rejection keep their original typed causes.
use super::native_all_frames::{consume_actual_inputs, retire_actual_work, start_generation};
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime,
        task_transport::tests::Loopback,
        transport::{ChannelAcquisitionError, Client},
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        custody::tests::original_transport_join_id,
        subscriptions::{SubscriptionEnd, SubscriptionSource},
    },
};
use novarocks_attempt_protocol::{
    ids::IdSeed,
    input::{ObservationFailureKind, ReceivedObservation},
    observation::{
        SubscriptionRequest,
        controller::{
            CoveredSubscription, ObservationDisposition, SubscriptionDirective, SubscriptionStatus,
        },
    },
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::TaskStatusCursor;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{error::Error, future::poll_fn, time::Duration};
use tokio::runtime::Handle;
#[derive(Clone, Copy)]
enum Case {
    ClosedEndpoint,
    RemoteUnavailable,
    Rejected,
}
async fn original_native_failure(case: Case) {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let original_key = key();
    let f = Fixture::make_with_captures(
        4,
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
    let descriptor = peer.descriptor(participation.backend_process_id());
    let peer_closed = matches!(case, Case::ClosedEndpoint);
    if peer_closed {
        assert!(peer.settle().await);
    } else {
        peer.expect_covered_rejection(if matches!(case, Case::Rejected) {
            tonic::Code::InvalidArgument
        } else {
            tonic::Code::Unavailable
        });
    }
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
    let request = Arc::new(SubscriptionRequest {
        participation,
        generation,
        status_cursors: Box::new([TaskStatusCursor::unobserved(task)]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([task]),
    });
    let mut policy = CoveredSubscription::new(
        participation,
        IdSeed::new(34, 1),
        if matches!(case, Case::Rejected) { 4 } else { 1 },
    )
    .unwrap();
    policy.activate();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open { generation })
    );
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(Arc::from([descriptor]), 0, native.clone());
    let mut original = start_generation(&f, &source, request.clone(), client);
    let join = original_transport_join_id(&original.role).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(8),
            poll_fn(|cx| original.role.poll_shared_completion(cx))
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(original_transport_join_id(&original.role), Some(join));
    assert_eq!(
        *original.result.lock().unwrap(),
        Some(SubscriptionEnd::Failed)
    );
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    consume_actual_inputs(&f, &capture, &[&original.phase], &mut observed);
    f.drain_progress();
    let [row] = observed.as_slice() else {
        panic!("one actual opening failure reaches one ordered original input");
    };
    assert_eq!(row.participation, participation);
    assert_eq!(row.generation, generation);
    assert_eq!(row.opening.started_at, MonotonicInstant::ORIGIN);
    assert_eq!(row.opening.opened_at, None);
    let ObservationEvent::Failed(failure) = row.event else {
        panic!("opening failure cannot manufacture an entity or EOF");
    };
    let expected = if matches!(case, Case::Rejected) {
        ObservationFailureKind::RpcRejected
    } else {
        ObservationFailureKind::RecoverableTransport {
            observed_connection_loss: peer_closed,
        }
    };
    assert_eq!(failure.kind, expected);
    assert_eq!(failure.cause.original, original.failure.reference());
    assert_eq!(
        policy.observe(row.clone()).unwrap(),
        ObservationDisposition::Ignore
    );
    assert_eq!(
        policy.status(),
        match case {
            Case::ClosedEndpoint => SubscriptionStatus::ConnectionLossExhausted,
            Case::RemoteUnavailable => SubscriptionStatus::BudgetExhausted,
            Case::Rejected => SubscriptionStatus::Fatal(failure),
        }
    );
    let projected = original.failure.project(failure.cause.original).unwrap();
    let raw = original.failure.take(failure.cause.original).unwrap();
    assert_eq!(raw.preparation_input(), projected);
    let cause = raw.source().expect("the actual typed Native opening cause");
    match case {
        Case::ClosedEndpoint => {
            assert!(cause.downcast_ref::<ChannelAcquisitionError>().is_some());
        }
        Case::RemoteUnavailable | Case::Rejected => {
            assert_eq!(
                cause.downcast_ref::<tonic::Status>().unwrap().code(),
                if matches!(case, Case::Rejected) {
                    tonic::Code::InvalidArgument
                } else {
                    tonic::Code::Unavailable
                }
            );
        }
    }
    assert!(original.failure.take(failure.cause.original).is_none());
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Close { generation })
    );
    source.close_generation(generation).unwrap();
    // Only the SAME actual Join above supplies this controller's local exit.
    policy.observe_actual_exit(generation).unwrap();
    assert_eq!(
        policy
            .drive(
                MonotonicInstant::from_origin(Duration::from_secs(100)),
                generation,
                None
            )
            .unwrap(),
        None
    );
    assert_eq!(policy.next_deadline(MonotonicInstant::ORIGIN), None);
    assert_eq!(
        peer.subscription_requests().len(),
        usize::from(!peer_closed),
        "a closed endpoint reaches no server; one typed rejection is never replayed"
    );
    assert!(peer.applied_requests().is_empty() && peer.control_requests() == 0);
    drop((raw, observed));
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
    if !peer_closed {
        assert!(peer.settle().await);
    }
    wc::finish(control, root, driver);
    f.finish();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_original_closed_endpoint_retains_typed_connection_loss() {
    original_native_failure(Case::ClosedEndpoint).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_plain_remote_unavailable_is_not_original_connection_loss() {
    original_native_failure(Case::RemoteUnavailable).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_subscription_is_not_resubscribed() {
    original_native_failure(Case::Rejected).await;
}
