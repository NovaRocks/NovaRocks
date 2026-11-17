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

//! Exact opaque panic boxes and original Source responsibility during disposal.
//! Local exit, retained cause disposal and unresolved destruction are distinct.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests as wc;
use crate::task_execution::host::{
    custody::{CustodyError, HostDisposal, OriginalTransportJoinObligation},
    subscriptions::{
        CoveredSubscriptionIo, CoveredSubscriptionStream, SubscriptionFailureCustody,
        SubscriptionIoFuture, SubscriptionSource,
    },
};
use novarocks_attempt_protocol::observation::SubscriptionRequest;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::ExitCustodyPhase,
    exit_work::{PanicPayload, PanicSink, run_future},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use novarocks_task_codec::operation::DecodedCoveredSubscription;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkRequest};
use std::{
    future::poll_fn,
    sync::{
        Condvar,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::runtime::Handle;

struct Hold {
    entered: AtomicBool,
    exited: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Hold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicBool::new(false),
            exited: AtomicBool::new(false),
            released: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn watchdog(self: &Arc<Self>) -> std::thread::JoinHandle<()> {
        let same = self.clone();
        std::thread::spawn(move || {
            let released = same.released.lock().unwrap();
            let (released, expired) = same
                .changed
                .wait_timeout_while(released, Duration::from_secs(3), |v| !*v)
                .unwrap();
            drop(released);
            if expired.timed_out() {
                same.release();
            }
        })
    }
}
struct RawCause {
    stage: &'static str,
    hold: Arc<Hold>,
}
impl Drop for RawCause {
    fn drop(&mut self) {
        self.hold.entered.store(true, Ordering::Release);
        let mut released = self.hold.released.lock().unwrap();
        while !*released {
            released = self.hold.changed.wait(released).unwrap();
        }
        self.hold.exited.store(true, Ordering::Release);
    }
}
struct ReleaseOnExit([Arc<Hold>; 2]);
impl Drop for ReleaseOnExit {
    fn drop(&mut self) {
        for hold in &self.0 {
            hold.release();
        }
    }
}
struct OriginalOpen {
    body: Option<RawCause>,
    cleanup: Option<RawCause>,
    entered: Arc<AtomicBool>,
}
impl Future for OriginalOpen {
    type Output = Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.entered.store(true, Ordering::Release);
        if let Some(original) = self.body.take() {
            std::panic::panic_any(original);
        }
        Poll::Pending
    }
}
impl Drop for OriginalOpen {
    fn drop(&mut self) {
        if let Some(original) = self.cleanup.take() {
            std::panic::panic_any(original);
        }
    }
}
struct Io(Mutex<Option<OriginalOpen>>);
impl CoveredSubscriptionIo for Io {
    fn open(
        &self,
        _: DecodedCoveredSubscription,
    ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
    {
        Box::pin(
            self.0
                .lock()
                .unwrap()
                .take()
                .expect("one original open future"),
        )
    }
}
pub(super) struct NoFrames;
impl SubscriptionFailureCustody for NoFrames {
    fn malformed(
        &self,
        _: StreamKey,
        _: novarocks_proto_codec::ProtocolError,
    ) -> ObservationFailure {
        panic!("no wire frame")
    }
    fn ingress_rejected(&self, _: StreamKey, _: ObservationError) -> ObservationFailure {
        panic!("no ingress rejection")
    }
    fn conflicting(
        &self,
        _: StreamKey,
        _: crate::native::task_transport::wire::CoveredEventValidationError,
    ) -> ObservationFailure {
        panic!("no conflicting frame")
    }
}
fn original_work(
    f: &Fixture,
) -> (
    ReportedWork,
    novarocks_control_runtime::obligation::WorkRetirementObserver,
) {
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
    let header = work.retirement_observer();
    let report = ReportSlot::reserve_next(
        &mut parcel,
        Generation::new(1).unwrap(),
        1,
        [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
    )
    .unwrap();
    (ReportedWork::try_new(work, parcel, report).unwrap(), header)
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn dispose_original(
    f: &Fixture,
    payload: PanicPayload,
    hold: &Arc<Hold>,
    other: Option<&Arc<Hold>>,
) {
    dispose_original_checked(f, payload, hold, other, || {}).await;
}
async fn dispose_original_checked(
    f: &Fixture,
    payload: PanicPayload,
    hold: &Arc<Hold>,
    other: Option<&Arc<Hold>>,
    held_check: impl FnOnce(),
) {
    dispose_value_checked(f, payload, drop, hold, other, held_check).await;
}
async fn dispose_value_checked<T: Send + 'static>(
    f: &Fixture,
    payload: T,
    dispose: fn(T),
    hold: &Arc<Hold>,
    other: Option<&Arc<Hold>>,
    held_check: impl FnOnce(),
) {
    let (work, header) = original_work(f);
    let disposal = HostDisposal::start(
        &Handle::current(),
        work,
        ConsumerId::new(1).unwrap(),
        payload,
        dispose,
    )
    .unwrap_or_else(|(error, _original)| panic!("original cause disposal: {}", error.error));
    until(|| hold.entered.load(Ordering::Acquire)).await;
    assert!(!hold.exited.load(Ordering::Acquire));
    if let Some(other) = other {
        assert!(
            !other.entered.load(Ordering::Acquire),
            "the second original cause is still retained"
        );
    }
    held_check();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            poll_fn(|cx| disposal.poll_complete(cx))
        )
        .await
        .is_err()
    );
    hold.release();
    tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| disposal.poll_complete(cx)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(hold.exited.load(Ordering::Acquire));
    let (handoff, join, panic) = disposal.take_failure();
    assert!(handoff.is_none() && join.is_none() && panic.is_none());
    assert!(f.scope.reap_observed_header(&header).unwrap());
}
async fn source_cleanup_only() {
    let f = Fixture::new(8, 4);
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
    let child = source.scope().unwrap();
    let generation = NonZeroU64::new(2).unwrap();
    let phase = f.phase(2);
    let request = Arc::new(SubscriptionRequest {
        participation: f.key.participation,
        generation,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    let job = source.prepare(request.clone(), &phase).unwrap();
    let body_hold = Hold::new();
    let cleanup_hold = Hold::new();
    let release = ReleaseOnExit([body_hold.clone(), cleanup_hold.clone()]);
    let watchdogs = [body_hold.watchdog(), cleanup_hold.watchdog()];
    let entered = Arc::new(AtomicBool::new(false));
    let io = Arc::new(Io(Mutex::new(Some(OriginalOpen {
        body: None,
        cleanup: Some(RawCause {
            stage: "cleanup",
            hold: cleanup_hold.clone(),
        }),
        entered: entered.clone(),
    }))));
    let (work, header) = original_work(&f);
    let panic = PanicSink::new();
    let (mut role, exit, handoff) =
        OriginalTransportJoinObligation::prepare(work, ConsumerId::new(1).unwrap(), panic.clone())
            .unwrap_or_else(|e| panic!("original role: {}", e.error));
    let original_custody = role.exit_custody();
    let panic_in = panic.clone();
    handoff
        .spawn(&Handle::current(), exit, move |exit, _| {
            let original = run_future(exit, job.run(io, Arc::new(NoFrames)), panic_in);
            async move {
                let _ = original.await;
            }
        })
        .unwrap();
    until(|| entered.load(Ordering::Acquire)).await;
    source.close_generation(generation).unwrap();
    until(|| panic.has_panic()).await;
    // Observe the SAME failed original Join. This does not turn its original
    // business health into success or dispose any original foreign cause.
    let joined = tokio::time::timeout(
        Duration::from_secs(2),
        poll_fn(|cx| role.poll_shared_completion(cx)),
    )
    .await
    .unwrap();
    assert!(joined.unwrap().is_none());
    let failed_join = role
        .take_join_failure()
        .expect("actual original failed carrier");
    assert!(failed_join.is_panic());
    drop(failed_join);
    assert!(!body_hold.entered.load(Ordering::Acquire));
    assert!(!cleanup_hold.entered.load(Ordering::Acquire));
    assert!(source.close_after_jobs().is_err());
    assert!(
        control
            .snapshot()
            .scopes
            .iter()
            .any(|s| s.id == child.id() && s.parent == Some(scope.id()))
    );
    assert!(child.check().is_ok());
    let first = role.take_work_panic().unwrap();
    assert_eq!(first.downcast_ref::<RawCause>().unwrap().stage, "cleanup");
    assert!(role.take_work_panic().is_none());
    assert!(panic.take_cleanup().is_none());
    assert!(!panic.has_panic());
    assert!(child.check().is_ok());
    dispose_original(&f, first, &cleanup_hold, None).await;
    drop(release);
    for watchdog in watchdogs {
        watchdog.join().unwrap();
    }
    source.close_new_jobs();
    f.drain_phase(&phase);
    f.drain_progress();
    assert!(child.check().is_ok());
    // Successful F destruction signs local exit independently of the raw
    // failed Join and causes. Do not relabel this actual exit Unknown.
    assert_eq!(original_custody.phase(), ExitCustodyPhase::Exited);
    assert!(role.try_retire().unwrap());
    assert!(f.scope.reap_observed_header(&header).unwrap());
    assert!(Arc::ptr_eq(
        &source.reap_generation(generation).unwrap(),
        &request
    ));
    source.close_after_jobs().unwrap();
    tokio::time::timeout(Duration::from_secs(2), child.wait_released())
        .await
        .unwrap();
    assert!(matches!(
        child.check(),
        Err(novarocks_workload_control::WorkError::Released)
    ));
    drop((
        role,
        original_custody,
        request,
        phase,
        source,
        child,
        cohort,
        grant,
        capacity,
        scope,
    ));
    wc::finish(control, root, driver);
    f.finish();
}
#[path = "panic_retirement/direct.rs"]
mod direct;

#[test]
fn original_panic_payload_destructor_is_actually_retired_before_scope_release() {
    direct::run_process_case(
        module_path!(),
        "original_panic_payload_destructor_is_actually_retired_before_scope_release",
        |runtime| {
            runtime.block_on(direct::direct_causes(true, false));
        },
    );
}
#[test]
fn future_destructor_panic_cannot_destroy_second_original_raw_cause() {
    direct::run_process_case(
        module_path!(),
        "future_destructor_panic_cannot_destroy_second_original_raw_cause",
        |runtime| {
            runtime.block_on(direct::direct_causes(true, true));
        },
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_only_panic_rejects_same_state_while_successor_still_held() {
    // The original named row had one cleanup-only F and no successor. Its
    // retired Native Live/Rejected projection is not a second AP author.
    source_cleanup_only().await;
}
#[test]
fn cleanup_only_original_f_drop_keeps_no_body_cause_and_unresolved_captures() {
    direct::run_process_case(
        module_path!(),
        "cleanup_only_original_f_drop_keeps_no_body_cause_and_unresolved_captures",
        |runtime| {
            runtime.block_on(direct::direct_causes(false, true));
        },
    );
}
#[test]
fn opening_poll_panic_keeps_poisoned_original_stream_unreapable_after_actual_exit() {
    direct::run_process_case(
        module_path!(),
        "opening_poll_panic_keeps_poisoned_original_stream_unreapable_after_actual_exit",
        |runtime| {
            runtime.block_on(direct::source_open_poison());
        },
    );
}
