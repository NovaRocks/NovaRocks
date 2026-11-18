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

//! A real Native RPC with its original generation F held during actual Drop.
//! This is an FE transport/custody component, not a full installed LP/AP gate.
use super::*;
use crate::{
    native::{
        data_runtime::FrontendDataRuntime, task_transport::tests::Loopback, transport::Client,
    },
    query_execution::native_execution_adapter::source_cohort::tests as wc,
    task_execution::host::{
        custody::{OriginalTransportJoinObligation, tests::original_transport_join_id},
        subscriptions::{SubscriptionEnd, SubscriptionFailureRecord, SubscriptionSource},
    },
};
use novarocks_attempt_protocol::observation::SubscriptionRequest;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_work::{PanicSink, run_future},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{TaskConvergenceCursor, TaskStatusCursor, TaskStatusVersion};
use novarocks_proto_codec::FieldPath;
use novarocks_task_codec::operation::decode_covered_subscribe_task_status;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{
    future::poll_fn,
    sync::{
        Condvar,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{runtime::Handle, sync::Notify};

struct Hold {
    entered: Notify,
    released: Mutex<bool>,
    changed: Condvar,
    drop_thread: Mutex<Option<std::thread::ThreadId>>,
    exited: AtomicBool,
}
impl Hold {
    fn release(&self) {
        *self.released.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}
struct ReleaseOnExit(Arc<Hold>);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct HeldOriginal<F> {
    inner: Pin<Box<F>>,
    hold: Arc<Hold>,
}
impl<F: Future> Future for HeldOriginal<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
impl<F> Drop for HeldOriginal<F> {
    fn drop(&mut self) {
        *self.hold.drop_thread.lock().unwrap() = Some(std::thread::current().id());
        self.hold.entered.notify_one();
        let mut released = self.hold.released.lock().unwrap_or_else(|e| e.into_inner());
        while !*released {
            released = self
                .hold
                .changed
                .wait(released)
                .unwrap_or_else(|e| e.into_inner());
        }
        self.hold.exited.store(true, Ordering::Release);
        // The SAME actual Native generation F fields drop only after this
        // original controlled destructor returns; no publisher exit is forged.
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_covered_rpc_abort_keeps_original_publisher_until_future_destruction_exits() {
    let f = Fixture::new(1, 4);
    let mut peer = Loopback::start().await;
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
        SubscriptionSource::from_cohort(f.key.participation, &scope, &cohort, f.input.clone())
            .unwrap();
    let source_scope = source.scope().unwrap();
    let key = StreamKey {
        generation: NonZeroU64::new(3).unwrap(),
        ..f.key
    };
    let phase = f.phase(3);
    let task = f.input.root_binding().root();
    let request = Arc::new(SubscriptionRequest {
        participation: key.participation,
        generation: key.generation,
        status_cursors: Box::new([TaskStatusCursor::at(task, TaskStatusVersion::FIRST)]),
        task_convergence_cursors: Box::new([TaskConvergenceCursor::at(
            task,
            novarocks_execution_contract::TaskConvergenceVersion::FIRST,
        )]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([task]),
    });
    let generation = source.prepare(request.clone(), &phase).unwrap();
    let other = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..f.key
    };
    f.input.register_stream(other).unwrap();
    assert!(!source.can_register_generation());
    let mut native = FrontendDataRuntime::new(Handle::current());
    let client = Client::for_backend_in(
        Arc::from([peer.descriptor(key.participation.backend_process_id())]),
        0,
        native.clone(),
    );
    let failure =
        SubscriptionFailureRecord::new(AttemptActionId::new(NonZeroU64::new(71).unwrap()), key);
    let (work, mut payer) = f
        .scope
        .reserve_next(
            Generation::new(1).unwrap(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::Derived(f.work.id()),
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let work_id = work.id();
    let header = work.retirement_observer();
    let observer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut payer,
        Generation::new(1).unwrap(),
        1,
        [(observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let panic = PanicSink::new();
    let raw_health = panic.clone();
    let (mut original, exit, handoff) = OriginalTransportJoinObligation::prepare(
        ReportedWork::try_new(work, payer, report.clone()).unwrap(),
        observer,
        panic.clone(),
    )
    .unwrap_or_else(|e| panic!("same original Native subscription Work: {}", e.error));
    assert!(handoff.owns_exit(&exit));
    let hold = Arc::new(Hold {
        entered: Notify::new(),
        released: Mutex::new(false),
        changed: Condvar::new(),
        drop_thread: Mutex::new(None),
        exited: AtomicBool::new(false),
    });
    let release_on_exit = ReleaseOnExit(hold.clone());
    let watchdog_hold = hold.clone();
    let watchdog = std::thread::spawn(move || {
        let released = watchdog_hold
            .released
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (released, expired) = watchdog_hold
            .changed
            .wait_timeout_while(released, Duration::from_secs(3), |v| !*v)
            .unwrap_or_else(|e| e.into_inner());
        drop(released);
        if expired.timed_out() {
            watchdog_hold.release();
        }
    });
    let result = Arc::new(Mutex::new(None));
    let result_out = result.clone();
    let failures = failure.clone();
    let hold_in = hold.clone();
    handoff
        .spawn(&Handle::current(), exit, move |exit, _registration| {
            let same = HeldOriginal {
                inner: Box::pin(generation.run_native(client, failures)),
                hold: hold_in,
            };
            let body = run_future(exit, same, panic);
            async move {
                let Ok(Ok(end)) = body.await else {
                    panic!("the same actual Native RPC must close normally")
                };
                *result_out.lock().unwrap() = Some(end);
            }
        })
        .unwrap();
    let original_join = original_transport_join_id(&original).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while peer.subscription_requests().len() != 1
            || peer.open_covered_subscriptions() != 1
            || f.input
                .read_state(key, token(71), MonotonicInstant::ORIGIN)
                .unwrap()
                .opened_at
                .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let wire = peer.subscription_requests().pop().unwrap();
    let decoded = decode_covered_subscribe_task_status(
        &wire,
        FieldPath::root("same_original_native_request"),
    )
    .unwrap();
    assert_eq!(decoded.participation, request.participation);
    assert_eq!(decoded.generation, request.generation);
    assert_eq!(
        decoded.status_cursors.as_slice(),
        request.status_cursors.as_ref()
    );
    assert_eq!(
        decoded.task_convergence_cursors.as_slice(),
        request.task_convergence_cursors.as_ref()
    );
    assert_eq!(
        decoded.required_identities.as_slice(),
        request.required_identities.as_ref()
    );
    assert_eq!(decoded.participation_cursor, request.participation_cursor);
    assert_eq!(
        decoded
            .quiesce_cursor
            .map(|cursor| (cursor.participation, cursor.fence_version)),
        request
            .quiesce_cursor
            .map(|cursor| (cursor.participation, cursor.fence_version))
    );
    source.close_generation(key.generation).unwrap();
    tokio::time::timeout(Duration::from_secs(2), hold.entered.notified())
        .await
        .unwrap();
    assert_ne!(
        hold.drop_thread.lock().unwrap().unwrap(),
        std::thread::current().id()
    );
    assert!(!hold.exited.load(Ordering::Acquire));
    // Cancel only this borrowing wait while the same F is physically held
    // in Drop. A timer on the blocked runtime worker is not an exit oracle.
    let mut first = Box::pin(poll_fn(|cx| original.poll_shared_completion(cx)));
    assert!(futures::poll!(first.as_mut()).is_pending());
    drop(first);
    assert_eq!(original_transport_join_id(&original), Some(original_join));
    assert!(!original.try_retire().unwrap());
    assert_eq!(
        report.publication_state().unwrap(),
        novarocks_control_runtime_core::completion::SlotState::Reserved
    );
    assert!(!source.can_register_generation());
    assert!(source.reap_generation(key.generation).is_err());
    assert!(source.close_after_jobs().is_err());
    assert!(source_scope.check().is_ok());
    hold.release();
    let complete = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| original.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(complete.is_none());
    assert_eq!(original_transport_join_id(&original), Some(original_join));
    assert_eq!(*result.lock().unwrap(), Some(SubscriptionEnd::CoreClosed));
    assert!(hold.exited.load(Ordering::Acquire));
    assert!(!raw_health.has_panic() && original.take_join_failure().is_none());
    assert!(
        failure.take(failure.reference()).is_none(),
        "normal close creates no Native failure"
    );
    assert!(original.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&header).unwrap());
    assert!(f.scope.work_lifecycle(work_id).unwrap().is_none());
    f.drain_phase(&phase);
    f.drain_progress();
    let applied = source.reap_generation(key.generation).unwrap();
    assert!(Arc::ptr_eq(&applied, &request));
    drop(applied);
    assert!(source.can_register_generation());
    let recovered = StreamKey {
        generation: NonZeroU64::new(4).unwrap(),
        ..f.key
    };
    f.input.register_stream(recovered).unwrap();
    assert!(
        !source.can_register_generation(),
        "the recovered original publisher position is actually reusable"
    );
    // This independently admitted metadata publisher never issued any job/F.
    f.input.job_closed(recovered).unwrap();
    f.input.retire_stream(recovered).unwrap();
    assert_eq!(
        peer.subscription_requests().len(),
        1,
        "Native close cannot open a replacement stream"
    );
    f.input.job_closed(other).unwrap();
    f.input.retire_stream(other).unwrap();
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), source_scope.wait_released())
        .await
        .unwrap();
    drop((
        original,
        phase,
        source,
        source_scope,
        cohort,
        grant,
        capacity,
        scope,
        request,
    ));
    drop(release_on_exit);
    watchdog.join().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    native.drain_original_apply_until(deadline).await.unwrap();
    native.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.settle().await);
    wc::finish(control, root, driver);
    f.finish();
}
