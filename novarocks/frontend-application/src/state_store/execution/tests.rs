// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner};
use novarocks_state_store_api::StateStoreProviderId;
use novarocks_workload_control::{
    CancellationReason, ControlStorageConfig, WorkClass, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Condvar, atomic::AtomicUsize, mpsc},
    task::Wake as TaskWake,
    time::Duration,
};
use tokio::sync::Notify;

const PROVIDER: StateStoreProviderId = StateStoreProviderId::new("port-fixture");
fn vocabulary() -> Vocabulary {
    Vocabulary::for_provider::<TestOriginal, TestOriginal, TestOriginal>(PROVIDER)
}
fn original_failure() -> Failure {
    // API authors the fact; the fixture only retains that same neutral cause.
    SharedFailureCause::from_static_detail(
        StateStoreError::new(StateStoreErrorKind::Cancelled, "original fixture stopped"),
        "original fixture stopped",
    )
}
fn worker() -> (QueryBlockingExecutorOwner, QueryBlockingExecutor) {
    let owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = owner.executor();
    (owner, executor)
}
async fn shutdown(owner: &mut QueryBlockingExecutorOwner) {
    owner
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .expect("same physical workers exit");
}
fn poll_once(owner: &StateStoreOriginalOwner) -> Result<bool, Failure> {
    owner.poll_progress(&mut Context::from_waker(Waker::noop()))
}
async fn closed(owner: &mut StateStoreOriginalOwner) {
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| owner.poll_closed(cx)),
    )
    .await
    .expect("original receiving must progress by its actual wakes")
    .expect("original exit");
}
fn cell(owner: &StateStoreOriginalOwner) -> Arc<Cell> {
    owner
        .port
        .inventory
        .lock()
        .unwrap()
        .entries
        .values()
        .next()
        .unwrap()
        .clone()
}

// The only physical test operation: borrow the same original input, record its
// output, and return. This is not a transaction or a copy of provider logic.
struct Gate {
    open: Mutex<bool>,
    condition: Condvar,
    entered: AtomicBool,
    notification: Notify,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            condition: Condvar::new(),
            entered: AtomicBool::new(false),
            notification: Notify::new(),
        })
    }
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        self.notification.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.condition.wait(open).unwrap();
        }
    }
    async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notice = self.notification.notified();
                tokio::pin!(notice);
                notice.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    break;
                }
                notice.await;
            }
        })
        .await
        .expect("actual worker reached its rescueable gate");
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.condition.notify_all();
    }
}
struct Rescue(Arc<Gate>);
impl Drop for Rescue {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Result(bool),
    Exit(Exit),
}
struct TestOriginal {
    origin: StateStoreBlockingOrigin,
    request: Request,
    input: Arc<[u8]>,
    output: Mutex<Option<usize>>,
    gate: Option<Arc<Gate>>,
    stop: AtomicBool,
    stop_cause: Failure,
    prepared: AtomicUsize,
    runs: AtomicUsize,
    interrupted: AtomicUsize,
    complete: AtomicBool,
    events: Mutex<Vec<Event>>,
    result_failure: Mutex<Option<Failure>>,
    slot: Arc<Mutex<Option<Box<dyn StateStoreBlockingSlot>>>>,
}
impl TestOriginal {
    fn new(origin: StateStoreBlockingOrigin, phase: Phase, gate: Option<Arc<Gate>>) -> Arc<Self> {
        Arc::new(Self {
            origin,
            request: Request {
                phase,
                revision: NonZeroU64::new(1).unwrap(),
            },
            input: Arc::from(&b"the same original input"[..]),
            output: Mutex::new(None),
            gate,
            stop: AtomicBool::new(false),
            stop_cause: original_failure(),
            prepared: AtomicUsize::new(0),
            runs: AtomicUsize::new(0),
            interrupted: AtomicUsize::new(0),
            complete: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
            result_failure: Mutex::new(None),
            slot: Arc::new(Mutex::new(None)),
        })
    }
    fn install(self: &Arc<Self>, owner: &StateStoreOriginalOwner) {
        let context = owner.context(Instant::now() + Duration::from_secs(5));
        let original: Arc<dyn StateStoreBlockingOriginal> = self.clone();
        let mut slot = None;
        context.bind_original_in(&original, &mut slot).unwrap();
        *self.slot.lock().unwrap() = slot;
        context.request_progress(); // Original receiving is stable first.
    }
    fn register(&self, waker: &Waker) {
        assert!(
            self.slot
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .poll_phase(&mut Context::from_waker(waker), self.request)
                .is_pending()
        );
    }
}
impl StateStoreBlockingOriginal for TestOriginal {
    fn origin(&self) -> StateStoreBlockingOrigin {
        self.origin
    }
    fn requested(&self) -> Option<Request> {
        (!self.complete.load(Ordering::Acquire)).then_some(self.request)
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        assert_eq!(request, self.request);
        self.stop
            .load(Ordering::Acquire)
            .then(|| self.stop_cause.clone())
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        assert_eq!(request, self.request);
        assert!(
            self.slot.lock().unwrap().is_some(),
            "source receiving precedes physical preparation"
        );
        assert_eq!(self.prepared.fetch_add(1, Ordering::AcqRel), 0);
        Ok(())
    }
    fn run_original_phase(&self, request: Request) {
        assert_eq!(request, self.request);
        assert_eq!(self.runs.fetch_add(1, Ordering::AcqRel), 0);
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        *self.output.lock().unwrap() = Some(self.input.len());
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        assert_eq!(request, self.request);
        let mut events = self.events.lock().unwrap();
        assert!(events.is_empty(), "result is received once before exit");
        events.push(Event::Result(result.is_ok()));
        if let Err(failure) = result {
            *self.result_failure.lock().unwrap() = Some(failure);
        }
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        assert_eq!(request, self.request);
        let mut events = self.events.lock().unwrap();
        assert!(matches!(events.as_slice(), [Event::Result(_)]));
        events.push(Event::Exit(exit.expect("real test physical receipt")));
        self.complete.store(true, Ordering::Release);
    }
    fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    fn local_failure(&self) -> Option<Failure> {
        None
    }
    fn request_close(&self) {
        self.request_interrupt();
    }
    fn request_interrupt(&self) {
        self.interrupted.fetch_add(1, Ordering::AcqRel);
        self.stop.store(true, Ordering::Release);
        // Deliberately does not open Gate: stop is not actual physical exit.
    }
}
struct NotSelected(Arc<TestOriginal>);
impl StateStoreBlockingOriginal for NotSelected {
    fn origin(&self) -> StateStoreBlockingOrigin {
        self.0.origin()
    }
    fn requested(&self) -> Option<Request> {
        self.0.requested()
    }
    fn publication_refusal(&self, _: Request) -> Option<Failure> {
        panic!("foreign purpose must be refused before progress")
    }
    fn before_publish(&self, _: Request) -> Result<(), Failure> {
        panic!("foreign purpose must not prepare")
    }
    fn run_original_phase(&self, _: Request) {
        panic!("foreign purpose must not run")
    }
    fn receive_result(&self, _: Request, _: Result<(), Failure>) {
        panic!("foreign purpose has no receiver")
    }
    fn receive_exit(&self, _: Request, _: Result<Exit, Failure>) {
        panic!("foreign purpose has no exit")
    }
    fn is_complete(&self) -> bool {
        false
    }
    fn local_failure(&self) -> Option<Failure> {
        None
    }
    fn request_close(&self) {
        panic!("foreign purpose was never installed")
    }
    fn request_interrupt(&self) {
        panic!("foreign purpose was never installed")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn selected_closed_vocabulary_rejects_foreign_type_and_origin_before_install() {
    let (mut workers, executor) = worker();
    let owner = StateStoreOriginalOwner::opening(
        &executor,
        vocabulary(),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Opening(PROVIDER),
        Phase::Open,
        None,
    );
    let wrong: Arc<dyn StateStoreBlockingOriginal> = Arc::new(NotSelected(source.clone()));
    let context = owner.context(Instant::now() + Duration::from_secs(5));
    let mut target = None;
    assert!(context.bind_original_in(&wrong, &mut target).is_err());
    assert!(target.is_none());
    let other = TestOriginal::new(
        StateStoreBlockingOrigin::Opening(StateStoreProviderId::new("other-provider")),
        Phase::Open,
        None,
    );
    let wrong: Arc<dyn StateStoreBlockingOriginal> = other.clone();
    assert!(context.bind_original_in(&wrong, &mut target).is_err());
    assert!(target.is_none());
    assert!(owner.port.inventory.lock().unwrap().entries.is_empty());
    assert_eq!(source.prepared.load(Ordering::Acquire), 0);
    assert_eq!(source.runs.load(Ordering::Acquire), 0);
    assert_eq!(other.runs.load(Ordering::Acquire), 0);
    drop(owner);
    shutdown(&mut workers).await;
}

#[derive(Debug)]
struct Snapshot {
    result_ready: bool,
    exit: Option<Exit>,
}
struct ReadOnWake {
    cell: Weak<Cell>,
    slot: Arc<Mutex<Option<Box<dyn StateStoreBlockingSlot>>>>,
    request: Request,
    sender: Mutex<mpsc::Sender<Snapshot>>,
}
impl TaskWake for ReadOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let slot = self.slot.clone();
        let cell = self.cell.clone();
        let request = self.request;
        // A joined observer thread is not an execution worker or source.
        // The callback must see restored fields without a later manual poll.
        let snapshot = std::thread::spawn(move || {
            // A regression holding the receiving lock must fail, not leave
            // this joined observer waiting forever for its own wake caller.
            let Some(cell) = cell.upgrade() else {
                return Snapshot {
                    result_ready: false,
                    exit: None,
                };
            };
            let restored = cell.body.try_lock().is_ok_and(|body| body.is_some());
            if !restored {
                return Snapshot {
                    result_ready: false,
                    exit: None,
                };
            }
            let Ok(mut slot) = slot.try_lock() else {
                return Snapshot {
                    result_ready: false,
                    exit: None,
                };
            };
            let slot = slot.as_mut().unwrap();
            let mut cx = Context::from_waker(Waker::noop());
            let result_ready = slot.poll_phase(&mut cx, request).is_ready();
            let exit = match slot.poll_exit(&mut cx, request) {
                Poll::Ready(Ok(exit)) => Some(exit),
                _ => None,
            };
            Snapshot { result_ready, exit }
        })
        .join()
        .expect("immediate observer joins");
        self.sender.lock().unwrap().send(snapshot).unwrap();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn full_source_stop_withdraws_without_capacity_and_wakes_only_restored_borrower() {
    let (mut workers, executor) = worker();
    let owner = StateStoreOriginalOwner::opening(
        &executor,
        vocabulary(),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let held = owner.port.executor.try_reserve().unwrap();
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Opening(PROVIDER),
        Phase::Open,
        None,
    );
    let input = source.input.as_ptr();
    source.install(&owner);
    let original_cell = cell(&owner);
    assert!(!poll_once(&owner).unwrap()); // Actual same queue reserve F is Pending.
    assert!(
        original_cell
            .body
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .phase
            .as_ref()
            .unwrap()
            .reservation
            .is_none()
    );
    let (sender, observed) = mpsc::channel();
    source.register(&Waker::from(Arc::new(ReadOnWake {
        cell: Arc::downgrade(&original_cell),
        slot: source.slot.clone(),
        request: source.request,
        sender: Mutex::new(sender),
    })));
    source.stop.store(true, Ordering::Release);
    owner.port.request_progress();
    assert!(poll_once(&owner).unwrap());
    let snapshot = observed
        .try_recv()
        .expect("the actual callback must have read receiving");
    assert!(snapshot.result_ready);
    assert_eq!(snapshot.exit, Some(Exit::NeverPublishedReturned));
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        [
            Event::Result(false),
            Event::Exit(Exit::NeverPublishedReturned)
        ]
    );
    assert_eq!(
        source.result_failure.lock().unwrap().as_ref().unwrap(),
        &source.stop_cause
    );
    assert_eq!(source.prepared.load(Ordering::Acquire), 0);
    assert_eq!(source.runs.load(Ordering::Acquire), 0);
    assert_eq!(source.input.as_ptr(), input);
    assert!(
        owner.port.executor.try_reserve().is_err(),
        "the original queue slot is still independently held"
    );
    drop(held);
    drop(owner.port.executor.try_reserve().unwrap());
    drop(owner);
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_root_cancel_interrupts_once_but_keeps_original_until_actual_exit() {
    let (mut workers, executor) = worker();
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 4,
        business_limit: 4,
        scope_records_limit: 4,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_responsibility, _driver) = control
        .configure_responsibility(
            novarocks_workload_control::joint_admission::ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(4, &[]))
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let instance = InstanceScope::new();
    let mut owner = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary(),
        instance,
        scope,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let foreign_instance = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(InstanceScope::new()),
        Phase::Get,
        None,
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = foreign_instance.clone();
    let mut target = None;
    assert!(
        owner
            .context(Instant::now() + Duration::from_secs(5))
            .bind_original_in(&original, &mut target)
            .is_err()
    );
    assert!(target.is_none());
    assert!(owner.port.inventory.lock().unwrap().entries.is_empty());
    let gate = Gate::new();
    let _rescue = Rescue(gate.clone());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::Get,
        Some(gate.clone()),
    );
    source.install(&owner);
    let original_cell = cell(&owner);
    assert!(!poll_once(&owner).unwrap());
    gate.reached().await;
    // Queue pop and physical exit are different frontiers.
    let held = owner
        .port
        .executor
        .try_reserve()
        .expect("pop returned original queue position");
    root.owner.cancel(CancellationReason::Requested);
    assert!(!poll_once(&owner).unwrap());
    assert!(!poll_once(&owner).unwrap());
    assert_eq!(source.interrupted.load(Ordering::Acquire), 1);
    assert!(source.events.lock().unwrap().is_empty());
    assert!(!source.is_complete());
    assert_eq!(owner.port.inventory.lock().unwrap().entries.len(), 1);
    {
        let body = original_cell.body.lock().unwrap();
        let phase = body.as_ref().unwrap().phase.as_ref().unwrap();
        assert!(phase.published);
        assert!(
            phase
                .receipt
                .as_ref()
                .unwrap()
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    gate.release();
    closed(&mut owner).await;
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        [Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    assert_eq!(*source.output.lock().unwrap(), Some(source.input.len()));
    drop(held);
    drop(owner);
    root.owner.complete_after_terminal_cancel_settled();
    drop(root.business);
    shutdown(&mut workers).await;
}

struct PauseWake {
    once: AtomicBool,
    gate: Arc<Gate>,
}
impl TaskWake for PauseWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.once.swap(true, Ordering::AcqRel) {
            self.gate.wait();
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn stable_result_is_received_before_exit_and_does_not_retire_paused_worker() {
    let (mut workers, executor) = worker();
    let mut owner = StateStoreOriginalOwner::opening(
        &executor,
        vocabulary(),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let call_gate = Gate::new();
    let exit_gate = Gate::new();
    let _call_rescue = Rescue(call_gate.clone());
    let _exit_rescue = Rescue(exit_gate.clone());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Opening(PROVIDER),
        Phase::Open,
        Some(call_gate.clone()),
    );
    source.install(&owner);
    let original_cell = cell(&owner);
    let wake = Waker::from(Arc::new(PauseWake {
        once: AtomicBool::new(false),
        gate: exit_gate.clone(),
    }));
    assert!(
        !owner
            .poll_progress(&mut Context::from_waker(&wake))
            .unwrap()
    );
    call_gate.reached().await;
    call_gate.release();
    exit_gate.reached().await; // Actual QB result notification is still on worker.
    assert!(!poll_once(&owner).unwrap());
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        [Event::Result(true)]
    );
    assert!(!source.is_complete());
    {
        let body = original_cell.body.lock().unwrap();
        let phase = body.as_ref().unwrap().phase.as_ref().unwrap();
        assert!(phase.result.as_ref().unwrap().is_ok());
        assert!(phase.exit.is_none());
        assert!(
            phase
                .receipt
                .as_ref()
                .unwrap()
                .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    exit_gate.release();
    closed(&mut owner).await;
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        [Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    drop(owner);
    shutdown(&mut workers).await;
}

// The same real worker panic becomes Source-local settlement health. This
// adapter changes only when that already received cause is observable; the
// physical call, receiving fields, and exit proof remain the existing fixture.
struct SettlementHealthOriginal {
    original: Arc<TestOriginal>,
    expose_health: AtomicBool,
}
impl StateStoreBlockingOriginal for SettlementHealthOriginal {
    fn origin(&self) -> StateStoreBlockingOrigin {
        self.original.origin()
    }
    fn requested(&self) -> Option<Request> {
        self.original.requested()
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        self.original.publication_refusal(request)
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        self.original.before_publish(request)
    }
    fn run_original_phase(&self, request: Request) {
        self.original.run_original_phase(request);
        std::panic::panic_any(PanicTag);
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        self.original.receive_result(request, result);
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        self.original.receive_exit(request, exit);
    }
    fn is_complete(&self) -> bool {
        self.original.is_complete()
    }
    fn local_failure(&self) -> Option<Failure> {
        if self.expose_health.load(Ordering::Acquire) {
            self.original.result_failure.lock().unwrap().clone()
        } else {
            None
        }
    }
    fn request_close(&self) {
        self.original.request_close();
    }
    fn request_interrupt(&self) {
        self.original.request_interrupt();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn settlement_health_waits_for_same_published_footer_before_outward_failure() {
    for health_visible_with_result in [true, false] {
        let (mut workers, executor) = worker();
        let owner = StateStoreOriginalOwner::opening(
            &executor,
            Vocabulary::for_provider::<
                SettlementHealthOriginal,
                SettlementHealthOriginal,
                SettlementHealthOriginal,
            >(PROVIDER),
            novarocks_state_store_runtime::StateStoreRunPolicy::default(),
        )
        .unwrap();
        let call_gate = Gate::new();
        let exit_gate = Gate::new();
        let _call_rescue = Rescue(call_gate.clone());
        let _exit_rescue = Rescue(exit_gate.clone());
        let source = Arc::new(SettlementHealthOriginal {
            original: TestOriginal::new(
                StateStoreBlockingOrigin::Opening(PROVIDER),
                Phase::Close,
                Some(call_gate.clone()),
            ),
            expose_health: AtomicBool::new(health_visible_with_result),
        });
        let context = owner.context(Instant::now() + Duration::from_secs(5));
        let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
        let mut slot = None;
        context.bind_original_in(&original, &mut slot).unwrap();
        *source.original.slot.lock().unwrap() = slot;
        context.request_progress();
        let original_cell = cell(&owner);
        let wake = Waker::from(Arc::new(PauseWake {
            once: AtomicBool::new(false),
            gate: exit_gate.clone(),
        }));
        assert!(
            !owner
                .poll_progress(&mut Context::from_waker(&wake))
                .unwrap()
        );
        call_gate.reached().await;
        call_gate.release();
        exit_gate.reached().await; // Real QA result send has not returned yet.

        assert!(!poll_once(&owner).expect("health cannot abandon the accepted footer"));
        assert_eq!(
            source.original.events.lock().unwrap().as_slice(),
            [Event::Result(false)]
        );
        let cause = source
            .original
            .result_failure
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert!(
            cause
                .downcast_ref::<QueryBlockingFailure>()
                .unwrap()
                .original_payload_is::<PanicTag>()
        );
        assert_eq!(
            cause.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Unexpected
        );
        assert!(owner.local_health_failure().is_none());
        assert!(
            !owner
                .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
                .unwrap()
        );
        if !health_visible_with_result {
            source.expose_health.store(true, Ordering::Release);
        }
        // Also exercises health first observed at the next poll's entry.
        assert!(!poll_once(&owner).expect("only the same receipts may progress"));
        assert!(owner.local_health_failure().is_none());
        {
            let body = original_cell.body.lock().unwrap();
            let body = body.as_ref().unwrap();
            assert_eq!(body.health.as_ref(), Some(&cause));
            let phase = body.phase.as_ref().unwrap();
            assert!(phase.published);
            assert_eq!(phase.result.as_ref().unwrap().as_ref().unwrap_err(), &cause);
            assert!(phase.exit.is_none());
            assert!(
                phase
                    .receipt
                    .as_ref()
                    .unwrap()
                    .poll_actual_exit(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        assert_eq!(source.original.prepared.load(Ordering::Acquire), 1);
        assert_eq!(source.original.runs.load(Ordering::Acquire), 1);
        assert!(!source.is_complete());

        exit_gate.release();
        let reported = tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| match owner.poll_progress(cx) {
                Ok(false) => {
                    assert!(owner.local_health_failure().is_none());
                    Poll::Pending
                }
                Ok(true) => panic!("same failed original must not be pruned"),
                Err(error) => {
                    assert!(
                        source.is_complete(),
                        "health follows the real received footer"
                    );
                    Poll::Ready(error)
                }
            }),
        )
        .await
        .expect("actual footer wake must resume the same receiver");
        assert_eq!(reported, cause);
        assert_eq!(owner.local_health_failure(), Some(cause.clone()));
        assert_eq!(
            owner
                .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
                .unwrap_err(),
            cause
        );
        assert_eq!(poll_once(&owner).unwrap_err(), cause);
        assert_eq!(
            source.original.events.lock().unwrap().as_slice(),
            [Event::Result(false), Event::Exit(Exit::ActualExited)]
        );
        assert_eq!(owner.port.inventory.lock().unwrap().entries.len(), 1);
        {
            let body = original_cell.body.lock().unwrap();
            let body = body.as_ref().unwrap();
            assert_eq!(body.health.as_ref(), Some(&cause));
            assert_eq!(
                body.phase.as_ref().unwrap().exit,
                Some(Ok(Exit::ActualExited))
            );
        }
        assert_eq!(source.original.prepared.load(Ordering::Acquire), 1);
        assert_eq!(source.original.runs.load(Ordering::Acquire), 1);
        shutdown(&mut workers).await;
        drop(owner); // Explicit test teardown after proving real physical exit.
    }
}

struct PanicTag;
struct PanicWake(AtomicUsize);
impl TaskWake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
        std::panic::panic_any(PanicTag);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn observer_wake_panic_preserves_same_original_cause_and_cell_after_withdrawal() {
    let (mut workers, executor) = worker();
    let owner = StateStoreOriginalOwner::opening(
        &executor,
        vocabulary(),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let held = owner.port.executor.try_reserve().unwrap();
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Opening(PROVIDER),
        Phase::Open,
        None,
    );
    source.install(&owner);
    assert!(!poll_once(&owner).unwrap());
    let original_cell = cell(&owner);
    let panic_wake = Arc::new(PanicWake(AtomicUsize::new(0)));
    source.register(&Waker::from(panic_wake.clone()));
    source.stop.store(true, Ordering::Release);
    let first = poll_once(&owner).expect_err("observer panic is retained as original health");
    assert!(
        first
            .downcast_ref::<QueryBlockingFailure>()
            .unwrap()
            .original_payload_is::<PanicTag>()
    );
    let later = poll_once(&owner).unwrap_err();
    assert_eq!(first, later);
    {
        let body = original_cell.body.lock().unwrap();
        assert_eq!(
            body.as_ref()
                .unwrap()
                .notification_failure
                .as_ref()
                .unwrap(),
            &first
        );
        assert_eq!(body.as_ref().unwrap().health.as_ref().unwrap(), &first);
    }
    assert_eq!(
        source.result_failure.lock().unwrap().as_ref().unwrap(),
        &source.stop_cause
    );
    assert_eq!(panic_wake.0.load(Ordering::Acquire), 1);
    assert_eq!(owner.port.inventory.lock().unwrap().entries.len(), 1);
    assert!(
        source.is_complete(),
        "business withdrawal does not erase notification health"
    );
    assert_eq!(source.runs.load(Ordering::Acquire), 0);
    drop(held);
    drop(owner); // Test teardown after proving the unpublished physical tuple returned.
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn host_evidence_accepts_only_its_actual_instance_author_and_settlement_phases() {
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let selected = Vocabulary::for_provider::<NotSelected, NotSelected, TestOriginal>(PROVIDER);
    let mut owner = StateStoreOriginalOwner::instance_evidence(
        &executor,
        selected,
        instance,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let context = owner.context(Instant::now() - Duration::from_secs(1));
    for phase in [
        Phase::Open,
        Phase::BeginRead,
        Phase::BeginWrite,
        Phase::Get,
        Phase::Range,
        Phase::Put,
        Phase::Delete,
        Phase::Commit,
        Phase::Rollback,
    ] {
        let original: Arc<dyn StateStoreBlockingOriginal> =
            TestOriginal::new(StateStoreBlockingOrigin::Instance(instance), phase, None);
        let mut target = None;
        assert!(context.bind_original_in(&original, &mut target).is_err());
        assert!(target.is_none());
    }
    for original in [
        TestOriginal::new(
            StateStoreBlockingOrigin::Instance(InstanceScope::new()),
            Phase::EvidenceRead,
            None,
        ) as Arc<dyn StateStoreBlockingOriginal>,
        Arc::new(NotSelected(TestOriginal::new(
            StateStoreBlockingOrigin::Instance(instance),
            Phase::EvidenceRead,
            None,
        ))) as Arc<dyn StateStoreBlockingOriginal>,
    ] {
        let mut target = None;
        assert!(context.bind_original_in(&original, &mut target).is_err());
        assert!(target.is_none());
    }
    assert!(owner.port.inventory.lock().unwrap().entries.is_empty());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::EvidenceRead,
        None,
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let mut target = None;
    context.bind_original_in(&original, &mut target).unwrap();
    *source.slot.lock().unwrap() = target;
    let progress = owner.progress_handle();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match progress.poll_progress(cx) {
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => Poll::Pending,
            Err(cause) => Poll::Ready(Err(cause)),
        }),
    )
    .await
    .expect("same evidence inventory actual exit")
    .unwrap();
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    closed(&mut owner).await;
    drop(owner);
    assert!(
        progress
            .poll_progress(&mut Context::from_waker(Waker::noop()))
            .is_err(),
        "weak cadence cannot replace missing Host custody"
    );
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_read_port_has_exact_instance_type_and_read_only_phases() {
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let selected = Vocabulary::for_provider::<NotSelected, TestOriginal, NotSelected>(PROVIDER);
    let mut owner = StateStoreOriginalOwner::bootstrap_read(
        &executor,
        selected,
        instance,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let context = owner.context(Instant::now() + Duration::from_secs(5));
    for phase in [
        Phase::Open,
        Phase::BeginWrite,
        Phase::Put,
        Phase::Delete,
        Phase::Commit,
        Phase::EvidenceRead,
        Phase::EvidenceRelease,
    ] {
        let source = TestOriginal::new(StateStoreBlockingOrigin::Instance(instance), phase, None);
        let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
        let mut target = None;
        assert!(context.bind_original_in(&original, &mut target).is_err());
        assert!(target.is_none());
        assert_eq!(source.runs.load(Ordering::Acquire), 0);
        assert!(source.events.lock().unwrap().is_empty());
    }
    for original in [
        TestOriginal::new(
            StateStoreBlockingOrigin::Instance(InstanceScope::new()),
            Phase::BeginRead,
            None,
        ) as Arc<dyn StateStoreBlockingOriginal>,
        TestOriginal::new(
            StateStoreBlockingOrigin::Opening(PROVIDER),
            Phase::BeginRead,
            None,
        ) as Arc<dyn StateStoreBlockingOriginal>,
        Arc::new(NotSelected(TestOriginal::new(
            StateStoreBlockingOrigin::Instance(instance),
            Phase::BeginRead,
            None,
        ))) as Arc<dyn StateStoreBlockingOriginal>,
    ] {
        let mut target = None;
        assert!(context.bind_original_in(&original, &mut target).is_err());
        assert!(target.is_none());
    }
    let expired = owner.context(Instant::now() - Duration::from_secs(1));
    let original: Arc<dyn StateStoreBlockingOriginal> = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::BeginRead,
        None,
    );
    let mut target = None;
    assert!(expired.bind_original_in(&original, &mut target).is_err());
    assert!(target.is_none());
    assert!(owner.port.inventory.lock().unwrap().entries.is_empty());

    // These are the existing port's real QB receipt/exit components, not a
    // SQLite transaction or a synthetic success for Catalog initialization.
    let sources = [
        Phase::BeginRead,
        Phase::Get,
        Phase::Range,
        Phase::Rollback,
        Phase::Close,
    ]
    .into_iter()
    .map(|phase| {
        let source = TestOriginal::new(StateStoreBlockingOrigin::Instance(instance), phase, None);
        source.install(&owner);
        source
    })
    .collect::<Vec<_>>();
    let progress = owner.progress_handle();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match progress.poll_progress(cx) {
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => Poll::Pending,
            Err(cause) => Poll::Ready(Err(cause)),
        }),
    )
    .await
    .expect("actual bootstrap read physical calls exit")
    .unwrap();
    for source in sources {
        assert_eq!(source.runs.load(Ordering::Acquire), 1);
        assert_eq!(
            source.events.lock().unwrap().as_slice(),
            &[Event::Result(true), Event::Exit(Exit::ActualExited)]
        );
    }
    closed(&mut owner).await;
    let mut target = None;
    assert!(context.bind_original_in(&original, &mut target).is_err());
    assert!(target.is_none());
    drop(owner);
    assert!(
        progress
            .poll_progress(&mut Context::from_waker(Waker::noop()))
            .is_err()
    );
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_close_keeps_published_call_until_its_same_actual_footer() {
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let selected = Vocabulary::for_provider::<NotSelected, TestOriginal, NotSelected>(PROVIDER);
    let mut owner = StateStoreOriginalOwner::bootstrap_read(
        &executor,
        selected,
        instance,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let gate = Gate::new();
    let _rescue = Rescue(gate.clone());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::BeginRead,
        Some(gate.clone()),
    );
    source.install(&owner);
    assert!(!poll_once(&owner).unwrap());
    gate.reached().await;
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    assert!(
        owner
            .poll_closed(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(!source.is_complete());
    assert!(source.events.lock().unwrap().is_empty());
    assert_eq!(owner.port.inventory.lock().unwrap().entries.len(), 1);
    gate.release();
    closed(&mut owner).await;
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    assert!(owner.port.inventory.lock().unwrap().entries.is_empty());
    drop(owner);
    shutdown(&mut workers).await;
}

// A closed two-phase port probe: actual evidence read, idle retained obligation,
// then actual close. Both phases use the same existing TestOriginal operation
// and physical executor. This is not a fabricated SQLite Unknown proof.
struct IdleObligation {
    read: Arc<TestOriginal>,
    close: Arc<TestOriginal>,
    closing: AtomicBool,
}
impl IdleObligation {
    fn new(instance: InstanceScope, gate: Arc<Gate>) -> Arc<Self> {
        let read = TestOriginal::new(
            StateStoreBlockingOrigin::Instance(instance),
            Phase::EvidenceRead,
            Some(gate),
        );
        let mut close = TestOriginal::new(
            StateStoreBlockingOrigin::Instance(instance),
            Phase::Close,
            None,
        );
        let close_fields = Arc::get_mut(&mut close).unwrap();
        close_fields.request.revision = NonZeroU64::new(2).unwrap();
        close_fields.slot = read.slot.clone();
        Arc::new(Self {
            read,
            close,
            closing: AtomicBool::new(false),
        })
    }
    fn phase(&self, request: Request) -> &TestOriginal {
        if request == self.read.request {
            &self.read
        } else {
            assert_eq!(request, self.close.request);
            &self.close
        }
    }
}
impl StateStoreBlockingOriginal for IdleObligation {
    fn origin(&self) -> StateStoreBlockingOrigin {
        self.read.origin()
    }
    fn requested(&self) -> Option<Request> {
        if !self.read.is_complete() {
            self.read.requested()
        } else if self.closing.load(Ordering::Acquire) {
            self.close.requested()
        } else {
            None
        }
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        self.phase(request).publication_refusal(request)
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        self.phase(request).before_publish(request)
    }
    fn run_original_phase(&self, request: Request) {
        self.phase(request).run_original_phase(request);
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        self.phase(request).receive_result(request, result);
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        self.phase(request).receive_exit(request, exit);
    }
    fn is_complete(&self) -> bool {
        self.close.is_complete()
    }
    fn local_failure(&self) -> Option<Failure> {
        None
    }
    fn request_close(&self) {
        self.closing.store(true, Ordering::Release);
    }
    fn request_interrupt(&self) {
        self.read.request_interrupt();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_quiescence_separates_idle_obligation_from_complete_and_new_request() {
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let selected = Vocabulary::for_provider::<NotSelected, NotSelected, IdleObligation>(PROVIDER);
    let mut owner = StateStoreOriginalOwner::instance_evidence(
        &executor,
        selected,
        instance,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let gate = Gate::new();
    let _rescue = Rescue(gate.clone());
    let source = IdleObligation::new(instance, gate.clone());
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let context = owner.context(Instant::now() + Duration::from_secs(5));
    let mut slot = None;
    context.bind_original_in(&original, &mut slot).unwrap();
    *source.read.slot.lock().unwrap() = slot;
    context.request_progress();
    assert!(
        !owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    gate.reached().await;
    assert!(
        !owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    assert_eq!(source.read.runs.load(Ordering::Acquire), 1);
    gate.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match owner.poll_actual_quiescent(cx) {
            Ok(true) => Poll::Ready(()),
            Ok(false) => Poll::Pending,
            Err(cause) => panic!("same original must be healthy: {cause}"),
        }),
    )
    .await
    .expect("same physical footer wakes the cut");
    assert!(!source.is_complete());
    assert_eq!(
        source.read.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    assert_eq!(owner.port.inventory.lock().unwrap().entries.len(), 1);
    // The old footer is still present; a new same-Source revision forbids a
    // positive cut even before the driver installs its next PhysicalPhase.
    source.request_close();
    assert!(!cell(&owner).actual_quiescent().unwrap());
    assert_eq!(source.close.runs.load(Ordering::Acquire), 0);
    // A concurrent loan cannot be mistaken for an empty inventory.
    let original_cell = cell(&owner);
    let body = original_cell.body.lock().unwrap().take().unwrap();
    assert!(!original_cell.actual_quiescent().unwrap());
    *original_cell.body.lock().unwrap() = Some(body);
    closed(&mut owner).await;
    assert!(source.is_complete());
    assert_eq!(source.close.runs.load(Ordering::Acquire), 1);
    assert!(
        owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    drop(owner);
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn read_request_uses_independent_transaction_time_but_absolute_context_never_renews() {
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let mut owner = StateStoreOriginalOwner::bootstrap_read(
        &executor,
        vocabulary(),
        instance,
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    let limit = Duration::from_secs(4);
    let request = owner.read_execution(limit).unwrap();
    let first_before = Instant::now();
    let first = request.read_transaction_context().unwrap();
    assert!(first.deadline() >= first_before.checked_add(limit).unwrap());
    let next_before = Instant::now();
    let second = request.read_transaction_context().unwrap();
    assert!(second.deadline() >= next_before.checked_add(limit).unwrap());
    assert!(second.shares_owner(&first));
    assert_eq!(
        first.read_transaction_context().unwrap().deadline(),
        first.deadline()
    );
    let expired = owner.context(Instant::now() - Duration::from_secs(1));
    let absolute = expired.read_transaction_context().unwrap();
    assert_eq!(absolute.deadline(), expired.deadline());
    assert_eq!(
        absolute.with_deadline(second.deadline()).deadline(),
        expired.deadline()
    );
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::BeginRead,
        None,
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let mut target = None;
    assert!(absolute.bind_original_in(&original, &mut target).is_err());
    assert!(target.is_none());
    second.bind_original_in(&original, &mut target).unwrap();
    *source.slot.lock().unwrap() = target;
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match owner.poll_progress(cx) {
            Ok(true) => Poll::Ready(()),
            Ok(false) => Poll::Pending,
            Err(cause) => panic!("same read Source must complete: {cause}"),
        }),
    )
    .await
    .unwrap();
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    closed(&mut owner).await;
    assert!(
        request.read_transaction_context().is_err(),
        "close forbids renewing business work"
    );
    drop(owner);
    assert!(
        request.read_transaction_context().is_err(),
        "weak request retains no owner"
    );
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn root_read_requests_keep_independent_limits_and_cancel_waits_for_actual_footer() {
    let (mut workers, executor) = worker();
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 4,
        business_limit: 4,
        scope_records_limit: 4,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_responsibility, _driver) = control
        .configure_responsibility(
            novarocks_workload_control::joint_admission::ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(1).unwrap(),
            },
        )
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(4, &[]))
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let instance = InstanceScope::new();
    let policy = novarocks_state_store_runtime::StateStoreRunPolicy::default();
    let mut owner = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary(),
        instance,
        root.owner.scope(),
        policy,
    )
    .unwrap();
    let read_limit = Duration::from_secs(31);
    let requests = owner.execution_requests(read_limit, None).unwrap();
    let before = Instant::now();
    let read = requests.read_transaction_context().unwrap();
    assert!(
        read.deadline() >= before + read_limit,
        "no statement cap must not borrow the shorter write-operation timeout"
    );
    let next = Instant::now();
    let second = requests.clone().read_transaction_context().unwrap();
    assert!(second.deadline() >= next + read_limit);
    assert!(read.shares_owner(&second));
    let before_write = Instant::now();
    let write = requests.write_operation_context().unwrap();
    let after_write = Instant::now();
    assert!(write.deadline() >= before_write + policy.operation_timeout());
    assert!(write.deadline() <= after_write + policy.operation_timeout());
    assert!(write.shares_owner(&read));
    assert_eq!(
        read.read_transaction_context().unwrap().deadline(),
        read.deadline(),
        "pages and fixed-context operations never renew the original deadline"
    );
    let cap = Instant::now() + Duration::from_secs(1);
    let capped = owner.execution_requests(read_limit, Some(cap)).unwrap();
    assert_eq!(capped.read_transaction_context().unwrap().deadline(), cap);
    assert_eq!(capped.write_operation_context().unwrap().deadline(), cap);

    let call_gate = Gate::new();
    let exit_gate = Gate::new();
    let _call_rescue = Rescue(call_gate.clone());
    let _exit_rescue = Rescue(exit_gate.clone());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::Get,
        Some(call_gate.clone()),
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let mut slot = None;
    read.bind_original_in(&original, &mut slot).unwrap();
    *source.slot.lock().unwrap() = slot;
    read.request_progress();
    let wake = Waker::from(Arc::new(PauseWake {
        once: AtomicBool::new(false),
        gate: exit_gate.clone(),
    }));
    assert!(
        !owner
            .poll_progress(&mut Context::from_waker(&wake))
            .unwrap()
    );
    call_gate.reached().await;
    call_gate.release();
    exit_gate.reached().await; // The actual QB worker is paused in result notification.
    assert!(
        !owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true)]
    );
    root.owner.cancel(CancellationReason::Requested);
    assert!(requests.read_transaction_context().is_err());
    assert!(requests.write_operation_context().is_err());
    assert!(
        !owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    assert!(
        owner.local_health_failure().is_none(),
        "ordinary stop is not raw health"
    );
    assert!(!source.is_complete());
    exit_gate.release();
    closed(&mut owner).await;
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    drop(owner);
    assert!(
        requests.read_transaction_context().is_err(),
        "borrower keeps no owner alive"
    );
    assert!(requests.write_operation_context().is_err());
    root.owner.complete_after_terminal_cancel_settled();
    drop(root.business);
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_read_owner_cannot_export_business_write_requests() {
    let (mut workers, executor) = worker();
    let mut owner = StateStoreOriginalOwner::bootstrap_read(
        &executor,
        vocabulary(),
        InstanceScope::new(),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .unwrap();
    assert!(
        owner
            .execution_requests(Duration::from_secs(3), None)
            .is_err()
    );
    assert!(
        owner
            .read_execution(Duration::from_secs(3))
            .unwrap()
            .read_transaction_context()
            .is_ok()
    );
    closed(&mut owner).await;
    drop(owner);
    shutdown(&mut workers).await;
}

// The same original MIO author supplies a positive payload; this helper does
// not infer a publication verdict or synthesize a settlement grant.
fn admitted_metadata(
    root: &novarocks_workload_control::WorkOwner,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
) -> Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission> {
    use crate::query_execution::native_execution_adapter::source_cohort::tests::drive;
    let io = crate::query_execution::maintenance::io_admission::MetadataIoAdmission::for_original_source(
        root, vec![novarocks_resource_contract::SharedDependency::LocalHost],
    ).unwrap();
    for _ in 0..256 {
        if io
            .poll_progress(root, &mut Context::from_waker(Waker::noop()))
            .unwrap()
        {
            return io;
        }
        drive(driver);
    }
    panic!("original metadata payload did not become ready");
}

#[tokio::test(flavor = "current_thread")]
async fn existing_metadata_obligation_uses_same_root_queue_and_receives_post_stop_footer() {
    use crate::query_execution::native_execution_adapter::source_cohort::tests as cohort;
    use novarocks_spi::connector::{
        ConnectorIoAdmissionPort, ConnectorOperationControl, ConnectorRequestContext,
        ConnectorStopOwner,
    };
    let (control, mut driver, _) = cohort::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let io = admitted_metadata(&root.owner, &mut driver);
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let policy = novarocks_state_store_runtime::StateStoreRunPolicy::default();
    let mut owner = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary(),
        instance,
        root.owner.scope(),
        policy,
    )
    .unwrap();
    let ordinary = owner
        .execution_requests(Duration::from_secs(31), None)
        .unwrap();
    let business_stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        business_stop.view(),
        1024,
        4096,
    )
    .unwrap()
    .with_io_admission(io.clone());
    assert!(
        io.existing_metadata_obligation(&root.owner.scope(), &context)
            .is_err(),
        "positive bytes alone are not a registered obligation"
    );
    io.publication_obligation_started();
    let foreign_context =
        ConnectorRequestContext::try_new(context.deadline(), business_stop.view(), 1024, 4096)
            .unwrap();
    assert!(
        io.existing_metadata_obligation(&root.owner.scope(), &foreign_context)
            .is_err(),
        "the context must name this same original MIO"
    );
    let expired = context.clone().with_earlier_deadline(Instant::now());
    assert!(
        io.existing_metadata_obligation(&root.owner.scope(), &expired)
            .is_err(),
        "capture cannot renew an expired obligation deadline"
    );
    let obligation_stop = ConnectorStopOwner::new();
    let obligation = context.for_existing_obligation(context.deadline(), obligation_stop.view());
    let token = io
        .existing_metadata_obligation(&root.owner.scope(), &obligation)
        .unwrap();
    let foreign_root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let foreign_owner = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary(),
        instance,
        foreign_root.owner.scope(),
        policy,
    )
    .unwrap();
    assert!(
        foreign_owner
            .existing_obligation_requests(Duration::from_secs(31), token.clone())
            .is_err()
    );
    assert!(
        foreign_owner
            .port
            .inventory
            .lock()
            .unwrap()
            .entries
            .is_empty()
    );
    drop(foreign_owner);
    foreign_root.owner.complete();
    foreign_root.business.release();
    let requests = owner
        .existing_obligation_requests(Duration::from_secs(31), token)
        .unwrap();
    let read = requests.read_transaction_context().unwrap();
    assert_eq!(read.deadline(), obligation.deadline());
    let before = Instant::now();
    let write = requests.write_operation_context().unwrap();
    assert!(write.deadline() >= before + policy.operation_timeout());
    assert!(write.deadline() <= obligation.deadline());
    assert!(read.shares_owner(&write), "one stable request wrapper");
    assert_eq!(
        read.read_transaction_context().unwrap().deadline(),
        read.deadline()
    );

    root.owner.cancel(CancellationReason::Requested);
    business_stop.request_stop();
    assert!(ordinary.read_transaction_context().is_err());
    assert!(ordinary.write_operation_context().is_err());
    assert!(requests.read_transaction_context().is_ok());
    assert!(requests.write_operation_context().is_ok());
    let run_gate = Gate::new();
    let footer = Gate::new();
    let _run_rescue = Rescue(run_gate.clone());
    let _footer_rescue = Rescue(footer.clone());
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::Get,
        Some(run_gate.clone()),
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let mut slot = None;
    read.bind_original_in(&original, &mut slot).unwrap();
    *source.slot.lock().unwrap() = slot;
    read.request_progress();
    let wake = Waker::from(Arc::new(PauseWake {
        once: AtomicBool::new(false),
        gate: footer.clone(),
    }));
    assert!(
        !owner
            .poll_progress(&mut Context::from_waker(&wake))
            .unwrap()
    );
    run_gate.reached().await;
    assert_eq!(
        source.interrupted.load(Ordering::Acquire),
        0,
        "Root cancellation cannot interrupt the admitted obligation"
    );
    run_gate.release();
    footer.reached().await;
    assert!(
        !owner
            .poll_actual_quiescent(&mut Context::from_waker(Waker::noop()))
            .unwrap()
    );
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true)]
    );
    obligation_stop.request_stop();
    assert!(!poll_once(&owner).unwrap());
    assert!(!poll_once(&owner).unwrap());
    assert_eq!(source.interrupted.load(Ordering::Acquire), 1);
    let refusal = match requests.write_operation_context() {
        Err(cause) => cause,
        Ok(_) => panic!("the original obligation stop must refuse another operation"),
    };
    assert_eq!(
        refusal.failure_fact(),
        obligation.check_active().unwrap_err().failure_fact()
    );
    assert!(owner.local_health_failure().is_none());
    assert!(!source.is_complete());
    footer.release();
    closed(&mut owner).await;
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[Event::Result(true), Event::Exit(Exit::ActualExited)]
    );
    assert_eq!(source.runs.load(Ordering::Acquire), 1);
    drop(owner);
    assert!(
        requests.read_transaction_context().is_err(),
        "wrapper cannot retain the Port"
    );
    drop((requests, ordinary, read, write, io));
    cohort::finish(control, root, driver);
    shutdown(&mut workers).await;
}

#[tokio::test(flavor = "current_thread")]
async fn obligation_stop_wakes_full_queue_and_normally_returns_same_unpublished_input() {
    use crate::query_execution::native_execution_adapter::source_cohort::tests as cohort;
    use novarocks_spi::connector::{
        ConnectorIoAdmissionPort, ConnectorOperationControl, ConnectorRequestContext,
        ConnectorStopOwner,
    };
    struct Notified(AtomicUsize);
    impl TaskWake for Notified {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let (control, mut driver, _) = cohort::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let io = admitted_metadata(&root.owner, &mut driver);
    let (mut workers, executor) = worker();
    let instance = InstanceScope::new();
    let mut owner = StateStoreOriginalOwner::for_root(
        &executor,
        vocabulary(),
        instance,
        root.owner.scope(),
        Default::default(),
    )
    .unwrap();
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        1024,
        4096,
    )
    .unwrap()
    .with_io_admission(io.clone());
    io.publication_obligation_started();
    let token = io
        .existing_metadata_obligation(&root.owner.scope(), &context)
        .unwrap();
    let requests = owner
        .existing_obligation_requests(Duration::from_secs(31), token)
        .unwrap();
    let request = requests.write_operation_context().unwrap();
    let held = owner.port.executor.try_reserve().unwrap();
    let source = TestOriginal::new(
        StateStoreBlockingOrigin::Instance(instance),
        Phase::Put,
        None,
    );
    let original: Arc<dyn StateStoreBlockingOriginal> = source.clone();
    let input = source.input.as_ptr();
    let mut slot = None;
    request.bind_original_in(&original, &mut slot).unwrap();
    *source.slot.lock().unwrap() = slot;
    request.request_progress();
    root.owner.cancel(CancellationReason::Requested);
    let notice = Arc::new(Notified(AtomicUsize::new(0)));
    let wake = Waker::from(notice.clone());
    assert!(
        !owner
            .poll_progress(&mut Context::from_waker(&wake))
            .unwrap()
    );
    assert_eq!(source.prepared.load(Ordering::Acquire), 0);
    assert!(source.events.lock().unwrap().is_empty());
    let before_stop = notice.0.load(Ordering::Acquire);
    stop.request_stop();
    assert!(
        notice.0.load(Ordering::Acquire) > before_stop,
        "the same control wakes the existing queue waiter without a retry timer"
    );
    assert!(poll_once(&owner).unwrap());
    assert_eq!(
        source.events.lock().unwrap().as_slice(),
        &[
            Event::Result(false),
            Event::Exit(Exit::NeverPublishedReturned)
        ]
    );
    assert_eq!(source.runs.load(Ordering::Acquire), 0);
    assert_eq!(source.prepared.load(Ordering::Acquire), 0);
    assert_eq!(source.input.as_ptr(), input);
    let first = source.result_failure.lock().unwrap().clone().unwrap();
    assert_eq!(
        first.failure_fact(),
        context.check_active().unwrap_err().failure_fact()
    );
    assert!(owner.local_health_failure().is_none());
    assert!(
        owner.port.executor.try_reserve().is_err(),
        "no new physical capacity"
    );
    drop(held);
    closed(&mut owner).await;
    let mut refused = None;
    assert!(request.bind_original_in(&original, &mut refused).is_err());
    assert!(refused.is_none());
    drop(owner);
    drop((requests, request, io));
    cohort::finish(control, root, driver);
    shutdown(&mut workers).await;
}
