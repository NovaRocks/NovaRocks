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

//! Actual stalled Native opening, original paid confirmation and exact budget.
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
        controller::{CoveredSubscription, SubscriptionDirective, SubscriptionStatus},
    },
};
use novarocks_control_runtime::history::HistoryCapture;
use novarocks_execution_contract::TaskStatusCursor;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{future::poll_fn, time::Duration};
use tokio::runtime::Handle;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covered_subscription_blackhole_opening_exhausts_its_bounded_budget() {
    let capture: BatchCapture = Arc::new(Mutex::new(Vec::with_capacity(8)));
    let k = key();
    let f = Fixture::make_with_captures(
        4,
        4,
        true,
        Some(liveness::Trace {
            key: k,
            history: HistoryCapture::new(4096),
            capture: liveness::Capture::default(),
        }),
        Some(capture.clone()),
    );
    let mut peer = Loopback::start().await;
    peer.expect_covered_opening_blackhole();
    let p = f.key.participation;
    let task = f.input.root_binding().root();
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
    let generation = NonZeroU64::new(3).unwrap();
    let stream = StreamKey {
        participation: p,
        generation,
    };
    let request = Arc::new(SubscriptionRequest {
        participation: p,
        generation,
        status_cursors: Box::new([TaskStatusCursor::unobserved(task)]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([task]),
    });
    let mut policy = CoveredSubscription::new(p, IdSeed::new(35, 1), 1).unwrap();
    policy.activate();
    assert_eq!(
        policy
            .drive(MonotonicInstant::ORIGIN, generation, None)
            .unwrap(),
        Some(SubscriptionDirective::Open { generation })
    );
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(p.backend_process_id())]),
        0,
        native.clone(),
    );
    let mut original = start_generation(&f, &source, request.clone(), client);
    let id = original_transport_join_id(&original.role).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while peer.subscription_requests().len() != 1
            || f.input
                .read_state(stream, token(9), MonotonicInstant::ORIGIN)
                .unwrap()
                .opening_started_at
                .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Inspect the actual original phase input before its real retirement.
    f.runtime.poll_one(Tick(1)).unwrap();
    let phase = capture
        .lock()
        .unwrap()
        .iter()
        .find_map(|(_, input)| match input {
            AttemptInput::Executor(ExecutorFact::SubscriptionProgress(snapshot)) => Some(*snapshot),
            _ => None,
        })
        .expect("actual first Native poll reports its phase through the paid Lifecycle role");
    assert_eq!(phase.participation, p);
    assert_eq!(phase.generation, generation);
    assert_eq!(phase.opening_started_at, MonotonicInstant::ORIGIN);
    assert_eq!(phase.opened_at, None);
    policy.observe_phase(phase).unwrap();
    let mut observed: Vec<ReceivedObservation> = Vec::new();
    consume_actual_inputs(&f, &capture, &[&original.phase], &mut observed);
    f.drain_progress();
    assert!(observed.is_empty());
    let deadline = policy.next_deadline(MonotonicInstant::ORIGIN).unwrap();
    assert_eq!(
        deadline,
        MonotonicInstant::from_origin(Duration::from_secs(5))
    );
    *f.clock.0.lock().unwrap() = deadline.since_origin();
    let request_token = token(21);
    let Some(SubscriptionDirective::Confirm(confirmation)) = policy
        .drive(deadline, generation, Some(request_token))
        .unwrap()
    else {
        panic!("the current AP requests its exact original read state");
    };
    assert_eq!(confirmation.generation, generation);
    assert_eq!(confirmation.token, request_token);
    let mut reply = f.confirmation();
    let input_id = f
        .input
        .confirm(stream, request_token, deadline, &mut reply)
        .await
        .unwrap_or_else(|_| panic!("same original causal read confirmation must be accepted"));
    f.runtime.poll_one(Tick(1)).unwrap();
    let (envelope, input) = {
        let mut slots = capture.lock().unwrap();
        assert_eq!(slots.len(), 1);
        slots.remove(0)
    };
    assert_eq!(envelope.input, input_id);
    assert_eq!(envelope.source, ActorId::new(3).unwrap());
    assert_eq!(envelope.class, InputClass::Lifecycle);
    let AttemptInput::Executor(ExecutorFact::ReadState(snapshot)) = &input else {
        panic!("same original read-confirmation result");
    };
    assert_eq!(snapshot.participation, p);
    assert_eq!(snapshot.generation, generation);
    assert_eq!(snapshot.request, request_token);
    assert_eq!(snapshot.opening_started_at, Some(MonotonicInstant::ORIGIN));
    assert_eq!(snapshot.opened_at, None);
    assert_eq!(
        snapshot.received_through_snapshot,
        ReceptionCoverage::Complete
    );
    policy.observe_read_state(*snapshot).unwrap();
    f.runtime.retire_input(actor(), input_id).unwrap();
    drop(input);
    reply.close_after_exit().unwrap();
    assert_eq!(policy.status(), SubscriptionStatus::BudgetExhausted);
    assert_eq!(original_transport_join_id(&original.role), Some(id));
    assert_eq!(*original.result.lock().unwrap(), None);
    assert!(!original.role.try_retire().unwrap());
    assert!(source.reap_generation(generation).is_err());
    assert_eq!(
        policy.drive(deadline, generation, None).unwrap(),
        Some(SubscriptionDirective::Close { generation })
    );
    assert_eq!(
        peer.subscription_requests().len(),
        1,
        "one opening spends the exact one failure budget; Native cannot reopen"
    );
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
    assert_eq!(original_transport_join_id(&original.role), Some(id));
    consume_actual_inputs(&f, &capture, &[&original.phase], &mut observed);
    f.drain_progress();
    assert!(
        observed.is_empty(),
        "budget and local cancellation cannot invent a wire failure/entity/EOF"
    );
    policy.observe_actual_exit(generation).unwrap();
    assert_eq!(
        policy
            .drive(
                deadline.saturating_add(Duration::from_secs(100)),
                generation,
                None
            )
            .unwrap(),
        None
    );
    assert_eq!(policy.next_deadline(deadline), None);
    assert!(peer.applied_requests().is_empty() && peer.control_requests() == 0);
    drop(observed);
    retire_actual_work(&f, &original);
    assert!(Arc::ptr_eq(
        &source.reap_generation(generation).unwrap(),
        &request
    ));
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), child.wait_released())
        .await
        .unwrap();
    drop((
        original, request, source, child, cohort, grant, capacity, scope,
    ));
    let until = std::time::Instant::now() + Duration::from_secs(3);
    native.drain_original_apply_until(until).await.unwrap();
    native.drain_native_outgoing_until(until).await.unwrap();
    assert_eq!(peer.subscription_requests().len(), 1);
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
