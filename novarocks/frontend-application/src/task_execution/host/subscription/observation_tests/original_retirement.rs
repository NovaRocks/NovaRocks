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

//! Original opaque opening-F custody through the current subscription Source.
//! No second subscription registry/reaper or Native retry policy is installed.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests as wc;
use crate::task_execution::host::{
    custody::{OriginalTransportJoinObligation, tests::original_transport_join_id},
    subscriptions::{
        CoveredSubscriptionIo, CoveredSubscriptionStream, SubscriptionEnd,
        SubscriptionFailureCustody, SubscriptionIoFuture, SubscriptionSource,
    },
};
use novarocks_attempt_protocol::observation::SubscriptionRequest;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_work::{PanicSink, run_future},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use novarocks_task_codec::operation::DecodedCoveredSubscription;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{
    future::poll_fn,
    sync::{
        Condvar,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{runtime::Handle, sync::Notify, task::Id};

struct Hold {
    entered: Notify,
    dropped: AtomicBool,
    original_id: Mutex<Option<Id>>,
    drop_thread: Mutex<Option<std::thread::ThreadId>>,
    release: Mutex<bool>,
    changed: Condvar,
}
impl Hold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            dropped: AtomicBool::new(false),
            original_id: Mutex::new(None),
            drop_thread: Mutex::new(None),
            release: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.release.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
    fn watchdog(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let same = self.clone();
        std::thread::spawn(move || {
            let release = same.release.lock().unwrap_or_else(|e| e.into_inner());
            let (release, expired) = same
                .changed
                .wait_timeout_while(release, Duration::from_secs(3), |v| !*v)
                .unwrap_or_else(|e| e.into_inner());
            drop(release);
            if expired.timed_out() {
                same.release();
            }
        })
    }
}
struct ReleaseOnExit(Arc<Hold>);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct OriginalOpen {
    hold: Arc<Hold>,
    hold_drop: bool,
    polls: Arc<AtomicUsize>,
}
impl Future for OriginalOpen {
    type Output = Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            *self.hold.original_id.lock().unwrap() = Some(tokio::task::id());
            self.hold.entered.notify_one();
        }
        Poll::Pending
    }
}
impl Drop for OriginalOpen {
    fn drop(&mut self) {
        *self.hold.drop_thread.lock().unwrap() = Some(std::thread::current().id());
        if self.hold_drop {
            self.hold.entered.notify_one();
            let mut released = self.hold.release.lock().unwrap_or_else(|e| e.into_inner());
            while !*released {
                released = self
                    .hold
                    .changed
                    .wait(released)
                    .unwrap_or_else(|e| e.into_inner());
            }
        }
        self.hold.dropped.store(true, Ordering::Release);
    }
}
struct Io {
    hold: Arc<Hold>,
    hold_drop: bool,
    polls: Arc<AtomicUsize>,
    opens: AtomicUsize,
}
impl CoveredSubscriptionIo for Io {
    fn open(
        &self,
        _: DecodedCoveredSubscription,
    ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
    {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Box::pin(OriginalOpen {
            hold: self.hold.clone(),
            hold_drop: self.hold_drop,
            polls: self.polls.clone(),
        })
    }
}
// No wire frame is decoded in these opaque-future custody tests. A callback
// would be a real unexpected path, never a manufactured protocol failure.
struct NoFrames;
impl SubscriptionFailureCustody for NoFrames {
    fn malformed(
        &self,
        _: StreamKey,
        _: novarocks_proto_codec::ProtocolError,
    ) -> ObservationFailure {
        panic!("no original wire frame")
    }
    fn ingress_rejected(&self, _: StreamKey, _: ObservationError) -> ObservationFailure {
        panic!("no refused original ingress")
    }
    fn conflicting(
        &self,
        _: StreamKey,
        _: crate::native::task_transport::wire::CoveredEventValidationError,
    ) -> ObservationFailure {
        panic!("no original conflicting frame")
    }
}

async fn original_stop(hold_drop: bool, cancelled_borrow: bool, publisher_full: bool) {
    let f = Fixture::new(if publisher_full { 1 } else { 8 }, 4);
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
    let original_scope = source.scope().unwrap();
    let key = StreamKey {
        generation: NonZeroU64::new(2).unwrap(),
        ..f.key
    };
    let phase = f.phase(2);
    let request = Arc::new(SubscriptionRequest {
        participation: key.participation,
        generation: key.generation,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    let generation = source.prepare(request.clone(), &phase).unwrap();
    let other = StreamKey {
        generation: NonZeroU64::new(3).unwrap(),
        ..f.key
    };
    if publisher_full {
        // Original Fixture stream1 + this real Source stream2 + one unused
        // admitted publisher3 occupy the exact fixed three metadata positions.
        f.input.register_stream(other).unwrap();
        assert!(!source.can_register_generation());
    }
    let (work, mut parcel) = f
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
    let original_work = work.reference();
    let header = work.retirement_observer();
    let observer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut parcel,
        Generation::new(1).unwrap(),
        1,
        [(observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let panic = PanicSink::new();
    let (mut role, exit, handoff) = OriginalTransportJoinObligation::prepare(
        ReportedWork::try_new(work, parcel, report.clone()).unwrap(),
        observer,
        panic.clone(),
    )
    .unwrap_or_else(|e| panic!("same original transport role: {}", e.error));
    assert!(handoff.owns_exit(&exit));
    let hold = Hold::new();
    let release_on_exit = ReleaseOnExit(hold.clone());
    let watchdog = hold.watchdog();
    let io = Arc::new(Io {
        hold: hold.clone(),
        hold_drop,
        polls: Arc::new(AtomicUsize::new(0)),
        opens: AtomicUsize::new(0),
    });
    let result = Arc::new(Mutex::new(None));
    let result_out = result.clone();
    let io_in = io.clone();
    let raw_health = panic.clone();
    // This make callback constructs the obligated wrapper before the original
    // carrier receives it; only its actual first poll starts the original F.
    handoff
        .spawn(&Handle::current(), exit, move |exit, _registration| {
            let body = run_future(exit, generation.run(io_in, Arc::new(NoFrames)), panic);
            async move {
                let Ok(Ok(end)) = body.await else {
                    panic!("same original subscription body must close normally");
                };
                *result_out.lock().unwrap() = Some(end);
            }
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), hold.entered.notified())
        .await
        .unwrap();
    let original_id = hold.original_id.lock().unwrap().unwrap();
    assert_eq!(original_transport_join_id(&role), Some(original_id));
    assert_eq!(io.opens.load(Ordering::SeqCst), 1);
    assert!(io.polls.load(Ordering::SeqCst) != 0);
    assert!(!role.try_retire().unwrap());
    assert!(source.reap_generation(key.generation).is_err());
    source.close_generation(key.generation).unwrap();
    if hold_drop {
        tokio::time::timeout(Duration::from_secs(2), hold.entered.notified())
            .await
            .unwrap();
        assert!(!hold.dropped.load(Ordering::Acquire));
        assert_ne!(
            hold.drop_thread.lock().unwrap().unwrap(),
            std::thread::current().id(),
            "the held original F destructor cannot run on its borrowed observer"
        );
        assert_eq!(
            report.publication_state().unwrap(),
            novarocks_control_runtime_core::completion::SlotState::Reserved
        );
        assert!(!role.try_retire().unwrap());
        assert!(source.reap_generation(key.generation).is_err());
        assert!(source.close_after_jobs().is_err());
        assert!(original_scope.check().is_ok());
        assert!(!f.bank.stream_claims_exited(key));
        assert_eq!(f.input.retire_stream(key), Err(ObservationError::Busy));
        if publisher_full {
            assert!(!source.can_register_generation());
        }
        if cancelled_borrow {
            let mut borrowed = Box::pin(poll_fn(|cx| role.poll_shared_completion(cx)));
            assert!(
                borrowed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(borrowed);
            assert_eq!(original_transport_join_id(&role), Some(original_id));
        }
        // A short physical observer deadline neither joins nor retires the F.
        let first = tokio::time::timeout(
            Duration::from_millis(10),
            poll_fn(|cx| role.poll_shared_completion(cx)),
        )
        .await;
        assert!(first.is_err());
        assert_eq!(original_transport_join_id(&role), Some(original_id));
        assert!(source.scope().is_ok());
    }
    hold.release();
    let joined = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| role.poll_shared_completion(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        joined.is_none(),
        "one original RPC job has no Apply attachment"
    );
    assert_eq!(original_transport_join_id(&role), Some(original_id));
    assert_eq!(*result.lock().unwrap(), Some(SubscriptionEnd::CoreClosed));
    assert!(!raw_health.has_panic());
    assert!(role.take_join_failure().is_none());
    assert!(hold.dropped.load(Ordering::Acquire));
    assert_eq!(
        io.opens.load(Ordering::SeqCst),
        1,
        "actual close never requests another generation/RPC"
    );
    assert_eq!(
        report.consumption(observer).unwrap(),
        novarocks_control_runtime_core::completion::Consumption::Consumed
    );
    assert!(role.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&header).unwrap());
    assert!(
        f.scope
            .work_lifecycle(original_work.work)
            .unwrap()
            .is_none()
    );
    f.drain_phase(&phase);
    f.drain_progress();
    let reaped = source.reap_generation(key.generation).unwrap();
    assert!(
        Arc::ptr_eq(&reaped, &request),
        "reaping returns the exact original applied request allocation"
    );
    drop(reaped);
    if publisher_full {
        assert!(source.can_register_generation());
        f.input.job_closed(other).unwrap();
        f.input.retire_stream(other).unwrap();
    }
    assert_eq!(cohort.remaining(), 0);
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), original_scope.wait_released())
        .await
        .unwrap();
    assert!(matches!(
        original_scope.check(),
        Err(novarocks_workload_control::WorkError::Released)
    ));
    drop((
        role,
        phase,
        source,
        original_scope,
        cohort,
        grant,
        capacity,
        scope,
    ));
    drop(release_on_exit);
    watchdog.join().unwrap();
    wc::finish(control, root, driver);
    f.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_stop_joins_same_original_handle_before_child_completion() {
    original_stop(false, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn future_destructor_remains_off_waiter_and_same_child_until_actual_exit() {
    original_stop(true, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_publisher_position_survives_original_abort_and_held_future_drop() {
    original_stop(true, false, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_borrowed_drain_retains_same_original_reaper_and_records() {
    // The old separate reaper is retired. The actual retained original Join,
    // generation record, prepaid read claim and Work are the current owners.
    original_stop(true, true, false).await;
}
