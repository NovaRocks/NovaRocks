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

use super::*;
use crate::native::{
    data_runtime::FrontendDataRuntime,
    task_transport::{NativeCallSource, NativePhysicalOffer},
    transport::{Client, NativeChannelSource, NativeDialAttempt},
    transport_capacity::{
        NativeTransportCapacity, TOTAL_BYTES, TOTAL_ITEMS, TransportCapacityLane,
    },
};
use crate::task_execution::host::{
    custody::{
        self, OriginalDisposalObligation, OriginalTransportJoinObligation,
        PhysicalInputRetirementObligation,
    },
    transport::{self, ReturnedQuota, SourceLayouts},
};
use crate::task_execution::intent::OperationIntent;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitObligation},
    exit_work::{GatedWork, HandoffCell, PanicSink},
    obligation::{ReservationObligation, StepRegistration},
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ReservationId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution_contract::{CancelReason, CancelTask, TaskIdentity, TaskOperationId};
use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeRpcMethod};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
    joint_admission::{
        JointLane, JointProgress, JointProgressDriver, NativeAsyncActionAuthority,
        NativeQueuedInputGrant, NativeQueuedInputQuote, ResponsibilityConfig,
    },
};
use std::{
    alloc::Layout,
    collections::VecDeque,
    future::{Future, poll_fn, ready},
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tokio::runtime::Handle;

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite WC proposal did not settle");
}
struct Governed {
    control: WorkloadControl,
    driver: JointProgressDriver,
    root: novarocks_workload_control::RootWork,
    window: novarocks_workload_control::ResultWindowGrant,
    listener: std::net::TcpListener,
}
impl Governed {
    fn new() -> Self {
        // Use the existing Native author's actual proposal-observer geometry;
        // this fixture creates no alternate WC grant or process quota wallet.
        let source =
            crate::native::task_transport::NativeTaskOperationSink::proposal_wait_source_geometry(
                novarocks_task_codec::TransportBudget::DEFAULT,
            )
            .unwrap();
        let backing = Layout::array::<u8>(source.resident_bytes().unwrap()).unwrap();
        let storage = ControlStorageConfig::new(8, &[]).with_process_source_backing(
            novarocks_workload_control::PollRoleSourceBacking::from_layouts(&[], &[backing])
                .unwrap(),
        );
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 8,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(31).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        // A first control binding must contain the actual source dependency.
        // Use the same Native origin author as governed_wire; the listener
        // retains this exact fixture endpoint through its real cleanup.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = novarocks_execution_contract::RuntimeEndpoint::from_socket_addr(
            listener.local_addr().unwrap(),
        );
        let origin = novarocks_native_adapter::FrontendNativeTransport::plaintext()
            .selected_network_origin(endpoint.native_endpoint())
            .unwrap()
            .into_owned();
        let mut binding = scope.begin_control_binding().unwrap();
        binding
            .push_dependency(&novarocks_resource_contract::SharedDependency::Network(
                origin,
            ))
            .unwrap();
        binding.end_change().unwrap();
        let mut pending = binding.submit().unwrap();
        drive(&mut driver);
        let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("original selected Native dependency binding grants"),
        };
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop((ready, pending));
        drive(&mut driver);
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        Self {
            control,
            driver,
            root,
            window,
            listener,
        }
    }
    fn authority(&self) -> NativeAsyncActionAuthority {
        self.window
            .retain_alias()
            .native_action_authority()
            .unwrap()
    }
    fn admit(&mut self, quote: NativeQueuedInputQuote) -> NativeQueuedInputGrant {
        let mut pending = self
            .authority()
            .begin_queued_input_when_available(JointLane::Settlement, quote)
            .unwrap();
        for _ in 0..100_000 {
            match pending
                .poll_with_slot_observer(&mut Context::from_waker(Waker::noop()), &mut |_, _| {
                    Poll::Pending
                }) {
                Poll::Pending => drive(&mut self.driver),
                Poll::Ready(Err(error)) => panic!("original WC input: {:?}", error.error),
                Poll::Ready(Ok(mut ready)) => {
                    let mut grant = None;
                    ready.install_in(&mut grant).unwrap().notify();
                    return grant.unwrap();
                }
            }
        }
        panic!("original WC input did not grant");
    }
    fn finish(self) -> bool {
        let Self {
            control,
            mut driver,
            root,
            window,
            listener,
        } = self;
        drop((window, listener));
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        control.close_admission();
        drive(&mut driver);
        control.shutdown().is_ok()
    }
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn observer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn role_slots() -> OwnedSlotNeed {
    OwnedSlotNeed {
        work_headers: 1,
        completions: 1,
        captures: 1,
        ..Default::default()
    }
}
struct Roles {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    layout: NativeTransportCapacity,
}
impl Roles {
    fn new() -> Self {
        let layout = NativeTransportCapacity::from_transport(TransportBudget::DEFAULT).unwrap();
        let ledger =
            RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
                .unwrap();
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        Self {
            ledger,
            scope,
            layout,
        }
    }
    fn bind(
        &self,
        guard: StepRegistration,
    ) -> (ReportedWork, ReservationObligation, ReservationId) {
        let (work, mut capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let principal = capacity.id();
        let resources = capacity.resources().unwrap();
        let quota = capacity
            .split_next(&ReservationResources {
                quota: resources.quota,
                keyed: resources.keyed,
                owned_slots: Default::default(),
            })
            .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        (
            ReportedWork::try_new(work, capacity, report).unwrap(),
            quota,
            principal,
        )
    }
    fn parent(
        &self,
        backend: BackendProcessId,
        bytes: usize,
    ) -> (ReportedWork, ReservationObligation, ReservationId) {
        let mut resources = self
            .layout
            .queue_resources(backend, TransportCapacityLane::Control, 1, bytes)
            .unwrap();
        resources.owned_slots = role_slots();
        self.bind(
            self.scope
                .reserve_next(
                    generation(),
                    resources,
                    WorkKind::Cleanup,
                    WorkAdmission::External,
                )
                .unwrap(),
        )
    }
    fn cleanup(&self, parent: WorkId) -> ReportedWork {
        let guard = self
            .scope
            .reserve_next(
                generation(),
                ReservationResources::new(ResourceVector::default(), role_slots()),
                WorkKind::Cleanup,
                WorkAdmission::Derived(parent),
            )
            .unwrap();
        let (work, mut capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(observer(), ConsumerKind::Observer)],
        )
        .unwrap();
        ReportedWork::try_new(work, capacity, report).unwrap()
    }
    fn encoding(
        &self,
        parent: ReservationId,
        backend: BackendProcessId,
        bytes: usize,
    ) -> StepRegistration {
        let mut resources = self
            .layout
            .encoding_resources(backend, TransportCapacityLane::Control, bytes)
            .unwrap();
        resources.owned_slots = role_slots();
        self.scope
            .reserve_child_next(generation(), parent, resources, WorkKind::Cleanup)
            .unwrap()
    }
    fn finish(self) {
        self.ledger.reap_scope_ready(self.scope.id(), 128).unwrap();
        self.scope.close_external().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Encoded,
    Unused,
}
struct EncodingChild {
    custody: ExitCustody,
    exit: ExitObligation,
    quota: ReservationObligation,
}
struct Captures {
    preparation: SharedApplyPreparation,
    mode: Mode,
    encoded: Arc<Mutex<Option<EncodingChild>>>,
}
fn transport_factory(
    exit: ExitObligation,
    captures: Captures,
    panic: PanicSink,
) -> impl Future<Output = ()> {
    let handoff = Arc::new(HandoffCell::new());
    if matches!(captures.mode, Mode::Unused) {
        handoff.withdraw();
    }
    let work = GatedWork::new(exit, captures, handoff, panic);
    let future = work.run(generation(), ready(()), fixture_body);
    async move {
        future.await.unwrap();
    }
}
fn fixture_body(captures: Captures, (): ()) -> impl Future<Output = ()> {
    async move {
        if matches!(captures.mode, Mode::Encoded) {
            let (work, mut capacity) = captures
                .preparation
                .take_encoding()
                .unwrap()
                .bind(WorkKind::Cleanup)
                .unwrap();
            let resources = capacity.resources().unwrap();
            let quota = capacity
                .split_next(&ReservationResources {
                    quota: resources.quota,
                    keyed: resources.keyed,
                    owned_slots: Default::default(),
                })
                .unwrap();
            let report = ReportSlot::reserve_next(
                &mut capacity,
                generation(),
                1,
                [(observer(), ConsumerKind::Observer)],
            )
            .unwrap();
            let (custody, exit) =
                ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report).unwrap());
            *captures.encoded.lock().unwrap() = Some(EncodingChild {
                custody,
                exit,
                quota,
            });
        }
    }
}
fn actual_gated_backing<F: Future<Output = ()>>(body: fn(Captures, ()) -> F) -> [Layout; 4] {
    GatedWork::<Captures>::backing_layouts::<std::future::Ready<()>, F>(body)
}
fn actual_future_layout<F: Future<Output = ()>>(
    _: fn(ExitObligation, Captures, PanicSink) -> F,
) -> Layout {
    Layout::new::<F>()
}
fn source_layouts() -> Vec<Layout> {
    let source = custody::storage_layouts().unwrap();
    let native = shared_apply_layouts().unwrap();
    let mut layouts = vec![
        source.transport_record,
        source.transport_registration,
        source.exit_custody,
        source.panic_sink,
        native.origin,
        native.physical_return_cell,
        native.reply_backing,
        native.disposal_input,
        // Transition constructs next input before dropping previous/completion.
        native.disposal_input,
        native.native_value,
        native.physical_value,
        native.action_value,
        native.reply_value,
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            Mutex<Option<EncodingChild>>,
        >())
        .unwrap(),
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<DropGate>())
            .unwrap(),
        Layout::new::<AfterOriginalDrop>(),
        Layout::new::<Row>(),
        Layout::array::<ReportedWork>(4).unwrap(),
        Layout::array::<OriginalDisposalObligation>(4).unwrap(),
    ];
    // Keep every completed real cleanup holder in this canonical fixture row.
    // Inline role/retirement/settlement handles are already part of Row.
    for _ in 0..4 {
        layouts.extend([
            source.disposal_record,
            source.exit_custody,
            source.panic_sink,
            source.disposal_carrier,
        ]);
    }
    layouts.extend(actual_gated_backing(fixture_body));
    layouts
}
fn cancel(backend: BackendProcessId) -> OperationIntent {
    let execution =
        QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap();
    OperationIntent::CancelTask(CancelTask::new(
        TaskOperationId::new_v7(),
        TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        ),
        CancelReason::UpstreamNoLongerNeeded,
    ))
}
struct Row {
    source: OriginalTransportJoinObligation,
    retirement: SharedApplyRetirement,
    child: Option<(ExitCustody, ExitObligation)>,
    publisher: Option<OriginalReplyPublisher>,
    cleanup: VecDeque<ReportedWork>,
    completed: Vec<OriginalDisposalObligation>,
    settlement: Option<PhysicalInputRetirementObligation>,
}
async fn row(roles: &Roles, runtime: &FrontendDataRuntime, wc: &mut Governed, mode: Mode) -> Row {
    let backend = BackendProcessId::new_v7();
    let operation = cancel(backend);
    let layouts = source_layouts();
    let (quote, plan) = transport::original_run_for_factory(
        &[operation.clone()],
        TransportBudget::DEFAULT,
        actual_future_layout(transport_factory),
        SourceLayouts { original: &layouts },
    )
    .unwrap();
    // This fixture's queue author stores the actual raw holder in its one-item
    // Native Vec. It does not fabricate a production OriginalQueuedInput.
    let input_quote =
        NativeQueuedInputQuote::for_original_native_input(
            std::num::NonZeroU64::new(
                Layout::array::<NativeQueuedInputGrant>(1).unwrap().size() as u64
            )
            .unwrap(),
            0,
        );
    let input = wc.admit(input_quote);
    let (parent, queue, principal) = roles.parent(backend, operation.queued_bytes());
    let parent_id = parent.work().id();
    let cleanup = (0..4).map(|_| roles.cleanup(parent_id)).collect();
    let mut action = wc
        .authority()
        .action_source(JointLane::Settlement, quote)
        .unwrap()
        .begin_when_available()
        .unwrap_or_else(|e| panic!("{:?}", e.error));
    drive(&mut wc.driver);
    let mut offer = match action
        .poll_local_offer(&mut Context::from_waker(Waker::noop()), &mut |_, _| {
            Poll::Pending
        }) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original Native action offer"),
    };
    let encoding = roles.encoding(principal, backend, plan.protobuf_bytes());
    let slot = runtime.apply_send_port().reserve_original_slot(1).unwrap();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let (mut source, exit, launch) =
        OriginalTransportJoinObligation::prepare(parent, observer(), panic)
            .unwrap_or_else(|e| panic!("{}", e.error));
    let registration = source.registration();
    let encoding_id = encoding.reservation_id();
    let (slot, encoding) = match registration.attach_shared(slot, Vec::new(), encoding) {
        Err((RegistrationFailure::MissingOriginalCharge, slot, inputs, guard)) => {
            assert!(inputs.is_empty());
            assert_eq!(guard.reservation_id(), encoding_id);
            (slot, guard)
        }
        _ => panic!("the actual attachment requires the original paid input"),
    };
    let preparation = registration
        .attach_shared(slot, vec![input], encoding)
        .unwrap_or_else(|_| panic!("same original Shared preparation"));
    let endpoint =
        novarocks_types::NativeEndpoint::from_socket_addr(wc.listener.local_addr().unwrap());
    let client = Client::for_endpoint(
        endpoint,
        NativeEndpointDomain::BackendControl,
        backend,
        runtime.clone(),
    );
    let mut selected = Some(
        client
            .prepare_channel_source(NativeRpcMethod::ApplyTaskControlOperations)
            .unwrap(),
    );
    let mut handoff = None;
    assert!(matches!(
        NativeChannelSource::try_reserve_in(&mut selected, &mut handoff).unwrap(),
        NativeDialAttempt::Installed
    ));
    let mut physical = Some(NativePhysicalOffer {
        encoding: None,
        call: Some(NativeCallSource::Cold(handoff.take().unwrap())),
    });
    preparation
        .accept_native_in(&mut offer, &mut physical)
        .unwrap()
        .notify();
    drop(offer);
    let (publisher, a, r) = preparation.prepare_native_in(&action, 1).unwrap();
    a.notify();
    r.notify();
    drop(action);
    let encoded = Arc::new(Mutex::new(None));
    let cell = Arc::clone(&encoded);
    launch
        .spawn(&Handle::current(), exit, move |exit, _| {
            transport_factory(
                exit,
                Captures {
                    preparation,
                    mode,
                    encoded: cell,
                },
                inner,
            )
        })
        .unwrap();
    let completion = poll_fn(|cx| source.poll_shared_completion(cx))
        .await
        .unwrap()
        .expect("actual same row Join");
    let mut bound = encoded.lock().unwrap().take();
    if let Some(original) = bound.take() {
        let facts = completion.join_facts();
        let queue_id = queue.id();
        let (completion, funds) =
            match complete_shared_join(completion, SharedApplyFunding::UnusedQueue(queue)) {
                Err(original) => original,
                Ok(_) => panic!("Taken guard rejects unused funding"),
            };
        assert_eq!(completion.join_facts(), facts);
        let SharedApplyFunding::UnusedQueue(queue) = funds else {
            panic!("same funding returned")
        };
        assert_eq!(queue.id(), queue_id);
        let retirement = complete_shared_join(
            completion,
            SharedApplyFunding::Encoded(ReturnedQuota {
                queue,
                encoding: original.quota,
            }),
        )
        .unwrap_or_else(|_| panic!("same encoded proof accepted"));
        Row {
            source,
            retirement,
            child: Some((original.custody, original.exit)),
            publisher: Some(publisher),
            cleanup,
            completed: Vec::with_capacity(4),
            settlement: None,
        }
    } else {
        let retirement = complete_shared_join(completion, SharedApplyFunding::UnusedQueue(queue))
            .unwrap_or_else(|_| panic!("same unused proof accepted"));
        Row {
            source,
            retirement,
            child: None,
            publisher: Some(publisher),
            cleanup,
            completed: Vec::with_capacity(4),
            settlement: None,
        }
    }
}
struct DropGate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
}
impl DropGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            changed: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct ReleaseOnDrop(Arc<DropGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct AfterOriginalDrop {
    original: Option<RawValue>,
    gate: Arc<DropGate>,
}
impl Drop for AfterOriginalDrop {
    fn drop(&mut self) {
        drop(self.original.take());
        self.gate.entered.notify_one();
        let mut released = self.gate.released.lock().unwrap();
        while !*released {
            released = self.gate.changed.wait(released).unwrap();
        }
    }
}
fn hold_after_original_drop(request: &NativeDisposalRequest, gate: &Arc<DropGate>) {
    // Wrap, never clone or replace, the SAME opaque phase object. The wrapper's
    // actual layout was included in the original fixture Source quote.
    let mut value = request.original.value.lock().unwrap();
    let original = value.take().unwrap();
    *value = Some(Box::new(AfterOriginalDrop {
        original: Some(original),
        gate: Arc::clone(gate),
    }));
}
async fn dispose(
    row: &mut Row,
    request: NativeDisposalRequest,
) -> custody::NativeDisposalCompletion {
    let role = row
        .cleanup
        .pop_front()
        .expect("original prepaid cleanup role");
    let mut original =
        OriginalDisposalObligation::start(&Handle::current(), role, observer(), request)
            .unwrap_or_else(|_| panic!("same original paid disposal"));
    let completion = poll_fn(|cx| original.poll_complete(cx)).await.unwrap();
    row.completed.push(original);
    completion
}
// Independent observation of the real WC release notification. Reading the
// original ledger grants no capacity and never participates in job custody.
struct QueueExitWake {
    ledger: RuntimeLedger,
    expected: [u64; 2],
    wakes: AtomicUsize,
    premature: AtomicBool,
}
impl Wake for QueueExitWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let observed = self
            .ledger
            .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
            .unwrap()
            .unwrap();
        self.wakes.fetch_add(1, Ordering::SeqCst);
        if observed.used != self.expected {
            self.premature.store(true, Ordering::SeqCst);
        }
    }
}
fn original_quota(original: &SharedPhysicalReturn) -> [u64; 2] {
    let mut released = [0_u64; 2];
    let mut add = |leg: &ReservationObligation| {
        let resource = leg.resources().unwrap();
        for (sum, dimension) in released.iter_mut().zip([TOTAL_ITEMS, TOTAL_BYTES]) {
            *sum = sum.checked_add(resource.quota.get(dimension)).unwrap();
        }
    };
    match original.original.financial.as_ref().unwrap() {
        SharedApplyFunding::Encoded(original) => {
            add(&original.encoding);
            add(&original.queue);
        }
        SharedApplyFunding::UnusedQueue(original) => add(original),
    }
    released
}
async fn finish_row(
    mut row: Row,
    roles: &Roles,
    wc: &mut Governed,
    mut oracles: Vec<bool>,
    remaining: &[DisposalPhase],
) -> Vec<bool> {
    // Native, Physical, Action use real run_blocking, report and actual Join.
    for &expected in remaining {
        let request = row
            .retirement
            .take_disposal_request()
            .expect("next original phase");
        oracles.push(request.original.phase == expected);
        let completion = dispose(&mut row, request).await;
        row.retirement
            .complete_disposal(completion)
            .unwrap_or_else(|_| panic!("same original completion"));
    }
    oracles.push(row.retirement.take_disposal_request().is_none());
    let returned = row
        .retirement
        .take_physical_return()
        .expect("actual physical completion");
    let released = original_quota(&returned);
    row.settlement = Some(PhysicalInputRetirementObligation::new(returned));
    // The packet proves Native return. Source separately observes this SAME
    // encoding role's actual body/capture exit before financial settlement.
    oracles.push(!row.source.try_retire().unwrap());
    if let Some((child, exit)) = row.child.take() {
        exit.revoke_unused_and_exit().unwrap();
        oracles.push(child.try_retire().unwrap());
        oracles.push(!row.source.try_retire().unwrap());
    }
    drop(row.publisher.take());
    let reply = row
        .retirement
        .take_disposal_request()
        .expect("last real reply alias exited");
    oracles.push(reply.original.phase == DisposalPhase::Reply);
    let completion = dispose(&mut row, reply).await;
    row.retirement
        .complete_disposal(completion)
        .unwrap_or_else(|_| panic!("same original reply completion"));
    row.retirement
        .try_retire()
        .unwrap_or_else(|_| panic!("actual Native row retired"));
    roles
        .ledger
        .reap_scope_ready(roles.scope.id(), 128)
        .unwrap();
    // All local cleanup roles and the Native row have exited, but the
    // original queue grants and their funding still belong to the Source.
    oracles.push(!row.source.try_retire().unwrap());
    drive(&mut wc.driver);
    let before = roles
        .ledger
        .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
        .unwrap()
        .unwrap();
    let wake = Arc::new(QueueExitWake {
        ledger: roles.ledger.clone(),
        expected: [
            before.used[0].checked_sub(released[0]).unwrap(),
            before.used[1].checked_sub(released[1]).unwrap(),
        ],
        wakes: AtomicUsize::new(0),
        premature: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let wait = wc.control.wait_progress(wc.control.progress_revision());
    let mut wait = std::pin::pin!(wait);
    let armed = wait
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending();
    oracles.push(row.settlement.as_mut().unwrap().finish_one().unwrap());
    oracles.push(
        armed && wake.wakes.load(Ordering::SeqCst) > 0 && !wake.premature.load(Ordering::SeqCst),
    );
    drop(wait);
    oracles.push(row.source.try_retire().unwrap());
    drop(row.source);
    drop(row.completed);
    drop(row.settlement);
    oracles
}
#[tokio::test(flavor = "multi_thread")]
async fn unused_shared_row_disposes_same_guard_then_returns_funds_before_wc_queue_exit() {
    let roles = Roles::new();
    let mut wc = Governed::new();
    let mut runtime = FrontendDataRuntime::new(Handle::current());
    let original = row(&roles, &runtime, &mut wc, Mode::Unused).await;
    roles.scope.close_external().unwrap();
    let before = roles
        .ledger
        .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
        .unwrap()
        .unwrap();
    let oracles = finish_row(
        original,
        &roles,
        &mut wc,
        vec![before.used[0] == 1, before.used[1] > 0],
        &[
            DisposalPhase::Native,
            DisposalPhase::Physical,
            DisposalPhase::Action,
        ],
    )
    .await;
    let after = roles
        .ledger
        .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
        .unwrap()
        .unwrap();
    runtime
        .drain_original_apply_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .drain_original_subscriptions_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime.close_native_outgoing().unwrap();
    runtime
        .drain_native_outgoing_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    roles.finish();
    let wc_closed = wc.finish();
    assert!(oracles.into_iter().all(|value| value));
    assert_eq!(after.used, [0, 0]);
    assert!(wc_closed);
}

#[tokio::test(flavor = "multi_thread")]
async fn encoded_local_join_precedes_child_exit_and_foreign_disposal_keeps_same_proof() {
    let roles = Roles::new();
    let mut wc = Governed::new();
    let mut runtime = FrontendDataRuntime::new(Handle::current());
    let mut encoded = row(&roles, &runtime, &mut wc, Mode::Encoded).await;
    // The local Source Join completed while this same original encoding
    // child and its financial objects remain live.
    let mut oracles = vec![
        encoded.child.is_some(),
        !encoded.source.try_retire().unwrap(),
    ];
    let request = encoded.retirement.take_disposal_request().unwrap();
    let completion = dispose(&mut encoded, request).await;
    // The first cold reservation has now actually exited. Reuse this same
    // endpoint only afterward: its earlier follower cannot elect a new dial.
    let mut other = row(&roles, &runtime, &mut wc, Mode::Unused).await;
    roles.scope.close_external().unwrap();
    let completion = match other.retirement.complete_disposal(completion) {
        Err(same) => {
            oracles.push(true);
            same
        }
        Ok(()) => panic!("foreign row consumed another original completion"),
    };
    encoded
        .retirement
        .complete_disposal(completion)
        .unwrap_or_else(|_| panic!("same proof correct-row resubmission"));
    let physical = encoded.retirement.take_disposal_request().unwrap();
    oracles.push(physical.original.phase == DisposalPhase::Physical);
    let gate = DropGate::new();
    let release = ReleaseOnDrop(Arc::clone(&gate));
    hold_after_original_drop(&physical, &gate);
    let role = encoded.cleanup.pop_front().unwrap();
    let mut disposal =
        OriginalDisposalObligation::start(&Handle::current(), role, observer(), physical)
            .unwrap_or_else(|_| panic!("same paid physical disposal"));
    gate.entered.notified().await;
    // The real physical carrier has returned its exact objects into the row,
    // but this actual callback/capture/Join is still held by the RAII gate.
    let packet_staged = {
        let state = encoded.retirement.core.state.lock().unwrap();
        let record = state.records[encoded.retirement.index].as_ref().unwrap();
        let OriginalState::Shared(shared) = &record.state else {
            panic!("same Shared phase")
        };
        shared.returned.lock().unwrap().is_some()
    };
    oracles.push(packet_staged);
    oracles.push(encoded.retirement.take_physical_return().is_none());
    oracles.push(
        disposal
            .poll_complete(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
    );
    let during = roles
        .ledger
        .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
        .unwrap()
        .unwrap();
    oracles.push(during.used[0] == 2 && during.used[1] > 0);
    gate.release();
    drop(release);
    let completion = poll_fn(|cx| disposal.poll_complete(cx)).await.unwrap();
    encoded.completed.push(disposal);
    encoded
        .retirement
        .complete_disposal(completion)
        .unwrap_or_else(|_| panic!("actual physical completion"));
    let oracles = finish_row(encoded, &roles, &mut wc, oracles, &[DisposalPhase::Action]).await;
    let oracles = finish_row(
        other,
        &roles,
        &mut wc,
        oracles,
        &[
            DisposalPhase::Native,
            DisposalPhase::Physical,
            DisposalPhase::Action,
        ],
    )
    .await;
    let after = roles
        .ledger
        .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
        .unwrap()
        .unwrap();
    runtime
        .drain_original_apply_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .drain_original_subscriptions_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime.close_native_outgoing().unwrap();
    runtime
        .drain_native_outgoing_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    roles.finish();
    let wc_closed = wc.finish();
    assert!(oracles.into_iter().all(|value| value));
    assert_eq!(after.used, [0, 0]);
    assert!(wc_closed);
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_attachment_survives_withdrawal_and_releases_only_after_join() {
    use crate::native::apply_send_owner::{ApplySendOwner, DrainObservation};
    use novarocks_control_runtime::exit_work::{GatedNotStarted, GatedOutcome, Withdrawal};
    let roles = Roles::new();
    let mut wc = Governed::new();
    let (mut apply, port) =
        ApplySendOwner::new(Handle::current(), TransportBudget::DEFAULT).unwrap();
    apply.start_reaper().unwrap();
    let backend = BackendProcessId::new_v7();
    let operation = cancel(backend);
    let (parent, queue, principal) = roles.parent(backend, operation.queued_bytes());
    let queue_id = queue.id();
    let encoding = roles.encoding(principal, backend, 64);
    let input = wc.admit(NativeQueuedInputQuote::for_original_native_input(
        NonZeroU64::new(Layout::new::<NativeQueuedInputGrant>().size() as u64).unwrap(),
        0,
    ));
    let slot = port.reserve_original_slot(1).unwrap();
    apply.close();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let (mut source, exit, launch) =
        OriginalTransportJoinObligation::prepare(parent, observer(), panic)
            .unwrap_or_else(|error| panic!("{}", error.error));
    let withdrawal = Arc::new(HandoffCell::new());
    let arbitration = withdrawal.clone();
    let (entered, refusal_observed) = tokio::sync::oneshot::channel();
    launch
        .spawn(
            &Handle::current(),
            exit,
            move |exit, registration| async move {
                let before_handoff = async move {
                    assert!(matches!(
                        registration.attach_send(slot, vec![input], encoding, queue_id),
                        Err(RegistrationFailure::Closed)
                    ));
                    entered.send(()).unwrap();
                    std::future::pending::<()>().await;
                };
                let outcome = GatedWork::new(exit, (), arbitration, inner)
                    .run(generation(), before_handoff, |(), ()| ready(()))
                    .await
                    .unwrap();
                assert_eq!(
                    outcome,
                    GatedOutcome::NotStarted(GatedNotStarted::Withdrawn)
                );
            },
        )
        .unwrap();
    refusal_observed.await.unwrap();
    let queue = match source.retire_refused_attachment(queue) {
        Err(original) => original,
        Ok(_) => panic!("a prepared refusal does not prove the original Join"),
    };
    assert!(!source.try_retire().unwrap());
    assert_eq!(withdrawal.withdraw(), Withdrawal::Withdrawn);
    assert!(
        poll_fn(|cx| source.poll_shared_completion(cx))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!source.try_retire().unwrap());
    let (error, mut retirement) = source
        .retire_refused_attachment(queue)
        .unwrap_or_else(|_| panic!("joined original refusal retains the same partition"));
    assert_eq!(error, RegistrationFailure::Closed);
    assert!(!source.try_retire().unwrap());
    assert!(matches!(
        apply
            .drain_until(Instant::now() + Duration::from_secs(2))
            .await,
        DrainObservation::Complete
    ));

    drive(&mut wc.driver);
    let wake = Arc::new(QueueExitWake {
        ledger: roles.ledger.clone(),
        expected: [0, 0],
        wakes: AtomicUsize::new(0),
        premature: AtomicBool::new(false),
    });
    let waker = Waker::from(wake.clone());
    {
        let wait = wc.control.wait_progress(wc.control.progress_revision());
        let mut wait = std::pin::pin!(wait);
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(retirement.finish_one().unwrap());
    }
    assert!(wake.wakes.load(Ordering::SeqCst) > 0);
    assert!(!wake.premature.load(Ordering::SeqCst));
    roles.ledger.reap_scope_ready(roles.scope.id(), 8).unwrap();
    assert!(source.try_retire().unwrap());
    roles.finish();
    assert!(wc.finish());
}
