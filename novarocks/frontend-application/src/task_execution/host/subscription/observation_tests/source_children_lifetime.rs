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

//! Same paid WC cohort/child and actual credentialed subscription work.
//! Legacy per-RPC child tables/reapers are deliberately not reconstructed.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::{
    NativeSourceCohort, tests as wc,
};
use crate::task_execution::{
    blocking_io::{ConnectorBlockingIoAdmission, ConnectorBlockingIoResponsibility},
    host::{
        custody::OriginalTransportJoinObligation,
        subscriptions::{
            CoveredSubscriptionIo, CoveredSubscriptionStream, SubscriptionEnd,
            SubscriptionFailureCustody, SubscriptionIoFuture, SubscriptionPhasePublisher,
            SubscriptionSource,
        },
    },
};
use novarocks_attempt_protocol::observation::SubscriptionRequest;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_work::{PanicSink, run_future},
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{completion::ConsumerKind, identity::ConsumerId};
use novarocks_task_codec::operation::DecodedCoveredSubscription;
use novarocks_workload_control::{ResultWindowClass, WorkClass, WorkError, WorkRequest};
use std::{
    future::poll_fn,
    sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, sync::Notify};

#[derive(Default)]
struct Probe {
    opens: AtomicUsize,
    polls: AtomicUsize,
    drops: AtomicUsize,
    entered: Notify,
    wake: futures::task::AtomicWaker,
}
struct Io {
    probe: Arc<Probe>,
    ready: bool,
}
struct Open {
    probe: Arc<Probe>,
    ready: bool,
}
struct Eof;
impl CoveredSubscriptionStream for Eof {
    fn next(
        &mut self,
    ) -> SubscriptionIoFuture<
        '_,
        Result<
            Option<novarocks_proto_models::novarocks::TaskStatusStreamEvent>,
            ObservationFailure,
        >,
    > {
        Box::pin(async { Ok(None) })
    }
}
impl Future for Open {
    type Output = Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.probe.wake.register(cx.waker());
        self.probe.polls.fetch_add(1, Ordering::SeqCst);
        self.probe.entered.notify_waiters();
        if self.ready {
            Poll::Ready(Ok(Box::new(Eof)))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Open {
    fn drop(&mut self) {
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl CoveredSubscriptionIo for Io {
    fn open(
        &self,
        _: DecodedCoveredSubscription,
    ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
    {
        self.probe.opens.fetch_add(1, Ordering::SeqCst);
        Box::pin(Open {
            probe: self.probe.clone(),
            ready: self.ready,
        })
    }
}
struct NoWire;
impl SubscriptionFailureCustody for NoWire {
    fn malformed(
        &self,
        _: StreamKey,
        _: novarocks_proto_codec::ProtocolError,
    ) -> ObservationFailure {
        panic!("no fixture wire frame")
    }
    fn ingress_rejected(&self, _: StreamKey, _: ObservationError) -> ObservationFailure {
        panic!("no fixture wire refusal")
    }
    fn conflicting(
        &self,
        _: StreamKey,
        _: crate::native::task_transport::wire::CoveredEventValidationError,
    ) -> ObservationFailure {
        panic!("no fixture wire conflict")
    }
}
struct Job {
    role: OriginalTransportJoinObligation,
    phase: Arc<SubscriptionPhasePublisher<Recorder>>,
    request: Arc<SubscriptionRequest>,
    header: WorkRetirementObserver,
    work: novarocks_control_runtime_core::identity::WorkId,
    result: Arc<Mutex<Option<SubscriptionEnd>>>,
    panic: PanicSink,
}
struct Case {
    f: Option<Fixture>,
    control: Option<novarocks_workload_control::WorkloadControl>,
    root: Option<novarocks_workload_control::RootWork>,
    driver: Option<novarocks_workload_control::joint_admission::JointProgressDriver>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
    cohort: Option<NativeSourceCohort>,
    source: Option<Arc<SubscriptionSource<Recorder>>>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    scan: Option<ConnectorBlockingIoResponsibility>,
    scan_admission: Option<ConnectorBlockingIoAdmission>,
    jobs: [Option<Job>; 2],
}
// Assertion unwind retains the complete original owner, work and actual Join;
// no fragment drop/abort/async close/positive exit is manufactured here.
struct Retained(Option<Box<Case>>);
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNKNOWN: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                std::sync::OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
impl Case {
    fn prepare_retained(count: usize) -> Retained {
        let (control, mut driver, capacity) = wc::fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let parent = root.owner.scope();
        wc::bind(&parent, &mut driver);
        let cohort = wc::cohort(&parent, count, &mut driver);
        wc::drive(&mut driver);
        Retained(Some(Box::new(Self {
            f: Some(Fixture::new(8, 4)),
            control: Some(control),
            root: Some(root),
            driver: Some(driver),
            capacity: Some(capacity),
            cohort: Some(cohort),
            source: None,
            window: None,
            scan: None,
            scan_admission: None,
            jobs: [None, None],
        })))
    }
    fn start(&mut self, slot: usize, generation: u64, probe: Arc<Probe>, ready: bool) {
        let f = self.f.as_ref().unwrap();
        let source = self.source.as_ref().unwrap();
        let generation = NonZeroU64::new(generation).unwrap();
        let phase = f.phase(generation.get());
        let request = Arc::new(SubscriptionRequest {
            participation: f.key.participation,
            generation,
            status_cursors: Box::new([]),
            task_convergence_cursors: Box::new([]),
            participation_cursor: None,
            quiesce_cursor: None,
            required_identities: Box::new([]),
        });
        let body = source.prepare(request.clone(), &phase).unwrap();
        assert_eq!(
            probe.opens.load(Ordering::SeqCst),
            0,
            "metadata preparation performs no RPC"
        );
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
        let work_id = work.id();
        let observer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut parcel,
            Generation::new(1).unwrap(),
            1,
            [(observer, ConsumerKind::Observer)],
        )
        .unwrap();
        let panic = PanicSink::new();
        let (role, exit, handoff) = OriginalTransportJoinObligation::prepare(
            ReportedWork::try_new(work, parcel, report).unwrap(),
            observer,
            panic.clone(),
        )
        .unwrap_or_else(|error| panic!("same original WC source job: {}", error.error));
        assert!(handoff.owns_exit(&exit));
        let result = Arc::new(Mutex::new(None));
        let output = result.clone();
        let raw = panic.clone();
        // Original role/report/Join custody is stable BEFORE the carrier can
        // receive or first-poll the original obligated body.
        assert!(
            self.jobs[slot]
                .replace(Job {
                    role,
                    phase,
                    request,
                    header,
                    work: work_id,
                    result,
                    panic
                })
                .is_none()
        );
        handoff
            .spawn(&Handle::current(), exit, move |exit, _| {
                let body = run_future(
                    exit,
                    body.run(Arc::new(Io { probe, ready }), Arc::new(NoWire)),
                    raw,
                );
                async move {
                    let Ok(Ok(end)) = body.await else {
                        panic!("original source job must close normally");
                    };
                    *output.lock().unwrap() = Some(end);
                }
            })
            .unwrap();
    }
    async fn join(
        &mut self,
        slot: usize,
        deadline: Instant,
        expected: SubscriptionEnd,
    ) -> Arc<SubscriptionRequest> {
        let job = self.jobs[slot].as_mut().unwrap();
        let actual = tokio::time::timeout_at(
            deadline.into(),
            poll_fn(|cx| job.role.poll_shared_completion(cx)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            actual.is_none(),
            "a subscription job has no Apply attachment"
        );
        assert_eq!(*job.result.lock().unwrap(), Some(expected));
        assert!(!job.panic.has_panic());
        assert!(job.role.take_join_failure().is_none());
        assert!(job.role.try_retire().unwrap());
        let f = self.f.as_ref().unwrap();
        assert!(f.scope.reap_observed_header(&job.header).unwrap());
        assert!(f.scope.work_lifecycle(job.work).unwrap().is_none());
        f.drain_phase(&job.phase);
        while f.input.0.state.lock().unwrap().in_flight.is_some() {
            f.consume(true);
        }
        f.drain_progress();
        let original = self
            .source
            .as_ref()
            .unwrap()
            .reap_generation(job.request.generation)
            .unwrap();
        assert!(Arc::ptr_eq(&original, &job.request));
        drop(self.jobs[slot].take());
        original
    }
    async fn finish(&mut self) {
        assert!(self.jobs.iter().all(Option::is_none));
        let source = self.source.take().unwrap();
        let child = source.scope().unwrap();
        source.close_new_jobs();
        source.close_after_jobs().unwrap();
        wc::drive(self.driver.as_mut().unwrap());
        tokio::time::timeout(Duration::from_secs(2), child.wait_released())
            .await
            .unwrap();
        assert!(matches!(child.check(), Err(WorkError::Released)));
        drop((
            source,
            child,
            self.scan.take(),
            self.scan_admission.take(),
            self.window.take(),
            self.cohort.take(),
            self.capacity.take(),
        ));
        wc::drive(self.driver.as_mut().unwrap());
        let driver = self.driver.take().unwrap();
        wc::finish(
            self.control.take().unwrap(),
            self.root.take().unwrap(),
            driver,
        );
        self.f.take().unwrap().finish();
    }
}
async fn polled(probe: &Probe, deadline: Instant) {
    tokio::time::timeout_at(deadline.into(), async {
        loop {
            let changed = probe.entered.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if probe.polls.load(Ordering::SeqCst) != 0 {
                return;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
}

struct StableReceiverWake {
    receiver: Mutex<Option<Weak<SubscriptionSource<Recorder>>>>,
    observation: novarocks_workload_control::WorkloadObservationHandle,
    parent: novarocks_workload_control::WorkId,
    cohort: NativeSourceCohort,
    calls: AtomicUsize,
}
impl StableReceiverWake {
    fn inspect(&self) {
        let source = self
            .receiver
            .try_lock()
            .expect("receiver unlocked before same notification")
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let child = source.scope().unwrap();
        let snapshot = self.observation.snapshot();
        let actual = snapshot
            .scopes
            .iter()
            .find(|row| row.id == child.id())
            .unwrap();
        assert_eq!(actual.parent, Some(self.parent));
        assert_eq!(actual.class, WorkClass::Query);
        assert_eq!(
            self.cohort.remaining(),
            0,
            "cohort lock released before original notification"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl std::task::Wake for StableReceiverWake {
    fn wake(self: Arc<Self>) {
        self.inspect();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.inspect();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_parent_subscription_starts_after_same_child_receipt_with_other_builders_live() {
    let mut retained = Case::prepare_retained(1);
    let c = retained.0.as_mut().unwrap();
    let parent = c.root.as_ref().unwrap().owner.scope();
    assert_eq!(parent.child_inherited().err(), Some(WorkError::NotReady));
    let builders = [
        parent.begin_control_binding().unwrap(),
        parent.begin_control_binding().unwrap(),
    ];
    assert_eq!(
        c.control.as_ref().unwrap().snapshot().scopes.len(),
        1,
        "prepaid cohort is not a live installed Source"
    );
    let wake = Arc::new(StableReceiverWake {
        receiver: Mutex::new(None),
        observation: c.control.as_ref().unwrap().observation(),
        parent: parent.id(),
        cohort: c.cohort.as_ref().unwrap().clone(),
        calls: AtomicUsize::new(0),
    });
    let progress = c.control.as_ref().unwrap().deadline_expiry_handle();
    let revision = c.control.as_ref().unwrap().progress_revision();
    let mut changed = Box::pin(progress.wait_state_change(revision));
    let waker = std::task::Waker::from(wake.clone());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut original_owner = None;
    let notice = c
        .cohort
        .as_ref()
        .unwrap()
        .take_child_in(&parent, &mut original_owner)
        .unwrap();
    c.source = Some(SubscriptionSource::from_owner(
        c.f.as_ref().unwrap().key.participation,
        original_owner.take().unwrap(),
        c.f.as_ref().unwrap().input.clone(),
    ));
    *wake.receiver.lock().unwrap() = Some(Arc::downgrade(c.source.as_ref().unwrap()));
    assert_eq!(
        wake.calls.load(Ordering::SeqCst),
        0,
        "committed original receipt is not its notice"
    );
    notice.notify();
    assert!(wake.calls.load(Ordering::SeqCst) > 0);
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop((changed, builders));
    let child = c.source.as_ref().unwrap().scope().unwrap();
    assert!(
        child.check().is_ok(),
        "other builders cannot revoke the installed child"
    );
    let probe = Arc::new(Probe::default());
    let deadline = Instant::now() + Duration::from_secs(3);
    c.start(0, 2, probe.clone(), true);
    c.join(0, deadline, SubscriptionEnd::RemoteClosed).await;
    assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
    assert_eq!(probe.polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.drops.load(Ordering::SeqCst),
        1,
        "actual same opening F destructor exited"
    );
    drop((wake, child, parent));
    c.finish().await;
    drop(retained.0.take());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_and_original_subscription_consume_same_cohort_with_other_control_builders_live() {
    let mut retained = Case::prepare_retained(2);
    let c = retained.0.as_mut().unwrap();
    let parent = c.root.as_ref().unwrap().owner.scope();
    c.window = Some(
        c.capacity
            .as_ref()
            .unwrap()
            .try_acquire(&parent, ResultWindowClass::Client)
            .unwrap(),
    );
    let alias = c.window.as_ref().unwrap().retain_alias();
    c.scan_admission = Some(
        ConnectorBlockingIoResponsibility::request_from_cohort(
            &parent,
            &alias,
            c.cohort.as_ref().unwrap().clone(),
        )
        .unwrap(),
    );
    assert_eq!(
        c.cohort.as_ref().unwrap().remaining(),
        2,
        "original scan constructor is metadata-only"
    );
    c.scan = Some(c.scan_admission.as_mut().unwrap().ready().await.unwrap());
    assert_eq!(c.cohort.as_ref().unwrap().remaining(), 1);
    let builders = [
        parent.begin_control_binding().unwrap(),
        parent.begin_control_binding().unwrap(),
    ];
    c.source = Some(
        SubscriptionSource::from_cohort(
            c.f.as_ref().unwrap().key.participation,
            &parent,
            c.cohort.as_ref().unwrap(),
            c.f.as_ref().unwrap().input.clone(),
        )
        .unwrap(),
    );
    assert_eq!(
        c.cohort.as_ref().unwrap().remaining(),
        0,
        "same cohort pays scan plus stable Source once each"
    );
    let child = c.source.as_ref().unwrap().scope().unwrap();
    let probe = Arc::new(Probe::default());
    let deadline = Instant::now() + Duration::from_secs(3);
    c.start(0, 2, probe.clone(), false);
    polled(&probe, deadline).await;
    drop(c.scan.take());
    wc::drive(c.driver.as_mut().unwrap());
    assert!(
        child.check().is_ok(),
        "scan exit cannot retire another original Source"
    );
    assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
    assert!(!c.jobs[0].as_ref().unwrap().role.try_retire().unwrap());
    c.source
        .as_ref()
        .unwrap()
        .close_generation(NonZeroU64::new(2).unwrap())
        .unwrap();
    c.join(0, deadline, SubscriptionEnd::CoreClosed).await;
    assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    assert!(
        child.check().is_ok(),
        "generation exit still does not close the stable Source"
    );
    drop((builders, alias, child, parent));
    c.finish().await;
    drop(retained.0.take());
}

#[path = "source_children_s0.rs"]
mod s0;
