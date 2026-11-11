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
    collections::{BTreeMap, VecDeque},
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
        Self::for_control_origin(None)
    }

    fn for_control_origin(
        peer_endpoint: Option<&novarocks_execution_contract::RuntimeEndpoint>,
    ) -> Self {
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
            .selected_network_origin(peer_endpoint.unwrap_or(&endpoint).native_endpoint())
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
    fn parent(
        &self,
        backend: BackendProcessId,
        bytes: usize,
    ) -> (ReportedWork, transport::QueueWindow, ReservationId) {
        self.parent_with_items(backend, 1, bytes)
    }
    fn parent_with_items(
        &self,
        backend: BackendProcessId,
        items: usize,
        bytes: usize,
    ) -> (ReportedWork, transport::QueueWindow, ReservationId) {
        let lane = TransportCapacityLane::Control;
        let mut resources = self
            .layout
            .queue_resources(backend, lane, items, bytes)
            .unwrap();
        resources.owned_slots = role_slots();
        let guard = self
            .scope
            .reserve_next(
                generation(),
                resources,
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap();
        let (work, mut capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let principal = capacity.id();
        let queue = transport::QueueWindow::split(
            &mut capacity,
            self.layout.clone(),
            backend,
            lane,
            items,
            bytes,
        )
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
            queue,
            principal,
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
        kind: WorkKind,
    ) -> StepRegistration {
        let mut resources = self
            .layout
            .encoding_resources(backend, TransportCapacityLane::Control, bytes)
            .unwrap();
        resources.owned_slots = role_slots();
        self.scope
            .reserve_child_next(generation(), parent, resources, kind)
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
    InlineEncoded { fail_scope: bool },
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
    inline: Option<(custody::TransportJoinRegistration, custody::EncodingBinding)>,
    fail_scope: Option<ScopeHandle>,
    _queue: Option<transport::QueueCustody>,
    panic: PanicSink,
}
fn transport_factory(
    exit: ExitObligation,
    captures: Captures,
    panic: PanicSink,
    actor_result: Option<custody::TransportJoinRegistration>,
) -> impl Future<Output = ()> {
    let handoff = Arc::new(HandoffCell::new());
    if matches!(captures.mode, Mode::Unused) {
        handoff.withdraw();
    }
    let work = GatedWork::new(exit, captures, handoff, panic);
    let future = work.run(generation(), ready(()), fixture_body);
    async move {
        future.await.unwrap();
        if let Some(actor_result) = actor_result {
            assert!(
                actor_result
                    .stage_actor_outcome(transport::BodyOutcome::ReceiptsRecorded)
                    .is_ok()
            );
        }
    }
}
fn fixture_body(captures: Captures, (): ()) -> impl Future<Output = ()> {
    async move {
        if let Some((source, binding)) = captures.inline {
            if let Some(scope) = captures.fail_scope {
                assert_ne!(scope.fail().unwrap(), generation());
            }
            let guard = captures.preparation.take_encoding().unwrap();
            let job = source.bind_encoding(guard, binding).unwrap();
            let encoding = transport::EncodingCustody::new(job.window, job.returned);
            novarocks_control_runtime::exit_work::run_future(
                job.exit,
                inline_encoding_body(encoding),
                captures.panic,
            )
            .await
            .unwrap();
            return;
        }
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
fn inline_encoding_body(mut encoding: transport::EncodingCustody) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        encoding.shrink_to(1).unwrap();
        Poll::Ready(())
    })
}
fn inline_encoding_layout<F: Future<Output = ()>>(
    _: fn(transport::EncodingCustody) -> F,
) -> Layout {
    Layout::new::<F>()
}
fn actual_gated_backing<F: Future<Output = ()>>(body: fn(Captures, ()) -> F) -> [Layout; 4] {
    GatedWork::<Captures>::backing_layouts::<std::future::Ready<()>, F>(body)
}
fn actual_future_layout<F: Future<Output = ()>>(
    _: fn(ExitObligation, Captures, PanicSink, Option<custody::TransportJoinRegistration>) -> F,
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
        custody::TransportRetirement::wake_layout(),
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
fn send_key(operation: &OperationIntent) -> transport::SendKey {
    use novarocks_attempt_protocol::input::SendGeneration;
    use novarocks_coordination_contract::AttemptActionId;
    transport::SendKey {
        operation: operation.operation_id(),
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
    }
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
struct PendingRow {
    source: OriginalTransportJoinObligation,
    publisher: OriginalReplyPublisher,
    reply_input: Option<novarocks_control_runtime_core::identity::InputId>,
    cleanup: VecDeque<ReportedWork>,
    legacy_queue: Option<ReservationObligation>,
    encoded: Arc<Mutex<Option<EncodingChild>>>,
}
fn pending_row(
    roles: &Roles,
    runtime: &FrontendDataRuntime,
    wc: &mut Governed,
    mode: Mode,
    actor: Option<&custody::tests::WithdrawalInbox>,
) -> PendingRow {
    let backend = BackendProcessId::new_v7();
    let operation = cancel(backend);
    let mut layouts = source_layouts();
    if matches!(mode, Mode::InlineEncoded { .. }) {
        layouts.extend(custody::encoding_owned_layouts().unwrap());
        layouts.push(inline_encoding_layout(inline_encoding_body));
    }
    if actor.is_some() {
        layouts.extend(transport::actor_send_report_layouts(1, 0, 0).unwrap());
        layouts.push(Layout::new::<custody::TransportRetirement>());
    }
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
    let positions = actor.map(|inbox| {
        (
            inbox.reserve(&parent),
            inbox.reserve_standing(parent.work()),
            inbox.reserve_native_receipt(parent.work(), send_key(&operation), operation.kind()),
        )
    });
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
    let encoding_kind = if matches!(mode, Mode::InlineEncoded { .. }) {
        WorkKind::Settlement
    } else {
        WorkKind::Cleanup
    };
    let encoding = roles.encoding(principal, backend, plan.protobuf_bytes(), encoding_kind);
    let slot = runtime.apply_send_port().reserve_original_slot(1).unwrap();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let (mut source, exit, launch) =
        OriginalTransportJoinObligation::prepare(parent, observer(), panic)
            .unwrap_or_else(|e| panic!("{}", e.error));
    let registration = source.registration();
    let (queue_capture, legacy_queue) = if !matches!(mode, Mode::Encoded) {
        (
            Some(transport::QueueCustody::new(
                queue,
                registration.clone(),
                Arc::from([send_key(&operation)]),
            )),
            None,
        )
    } else {
        (None, Some(queue.into_original()))
    };
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
    let reply_input = positions.map(|(result, start, mut receipt)| {
        use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
        use crate::task_execution::intent::OperationAcknowledgement;
        use novarocks_attempt_protocol::{
            capacity::ReservationToken,
            effect::{AttemptEffect, DispatchBatch, EffectBody, EffectClass},
            input::QueuedInputGrantRef,
        };
        use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};

        let key = send_key(&operation);
        let OperationIntent::CancelTask(original) = &operation else {
            unreachable!()
        };
        let protocol =
            novarocks_attempt_protocol::effect::OperationIntent::CancelTask(original.clone());
        let id = AttemptActionId::new(NonZeroU64::new(2).unwrap());
        let effect = AttemptEffectEnvelope::new(AttemptEffect {
            id,
            class: EffectClass::Settlement,
            body: EffectBody::SendBatch {
                batch: DispatchBatch {
                    backend,
                    lane: protocol.lane(),
                    control_progress: true,
                    operations: Box::new([protocol]),
                    queued_bytes: operation.queued_bytes(),
                    queued_at: MonotonicInstant::from_origin(Duration::ZERO),
                    queued_inputs: Box::new([QueuedInputGrantRef::new(
                        NonZeroU64::MIN,
                        key.generation,
                    )]),
                },
                reservation: ReservationToken(NonZeroU64::new(principal.get()).unwrap()),
            },
        });
        receipt
            .record(
                key,
                publisher.canonicalize(OperationAcknowledgement::transport_unknown(
                    key.operation,
                    operation.kind(),
                )),
                None,
                None,
            )
            .unwrap();
        let input = receipt.submit().unwrap();
        let reports = Arc::new(transport::ActorSendReports::new(
            id,
            transport::SendReportRows {
                started: transport::NativeProgressInput::new(start),
                members: Box::new([transport::SendMemberReport::new(receipt, None, None, None)]),
            },
        ));
        let author = result.event_author().unwrap();
        assert!(
            launch
                .bind_actor_result(effect, &author, result, Some(reports))
                .is_ok()
        );
        input
    });
    let encoded = Arc::new(Mutex::new(None));
    let cell = Arc::clone(&encoded);
    let inline = matches!(mode, Mode::InlineEncoded { .. }).then(|| custody::EncodingBinding {
        layout: roles.layout.clone(),
        backend,
        lane: TransportCapacityLane::Control,
        quoted_bytes: plan.protobuf_bytes(),
    });
    let fail_scope =
        matches!(mode, Mode::InlineEncoded { fail_scope: true }).then(|| roles.scope.clone());
    assert!(matches!(
        source.take_encoding_return(),
        Err(custody::CustodyError::OriginalAttachmentUnresolved)
    ));
    launch
        .spawn(&Handle::current(), exit, move |exit, source| {
            transport_factory(
                exit,
                Captures {
                    preparation,
                    mode,
                    encoded: cell,
                    inline: inline.map(|binding| (source.clone(), binding)),
                    fail_scope,
                    _queue: queue_capture,
                    panic: inner.clone(),
                },
                inner,
                reply_input.map(|_| source),
            )
        })
        .unwrap();
    PendingRow {
        source,
        publisher,
        reply_input,
        cleanup,
        legacy_queue,
        encoded,
    }
}
async fn row(roles: &Roles, runtime: &FrontendDataRuntime, wc: &mut Governed, mode: Mode) -> Row {
    let PendingRow {
        mut source,
        publisher,
        cleanup,
        legacy_queue,
        encoded,
        ..
    } = pending_row(roles, runtime, wc, mode, None);
    let completion = poll_fn(|cx| source.poll_shared_completion(cx))
        .await
        .unwrap()
        .expect("actual same row Join");
    let bound = encoded.lock().unwrap().take();
    let (retirement, child) = if matches!(mode, Mode::InlineEncoded { .. }) {
        assert!(bound.is_none());
        let SharedApplyFunding::Encoded(mut original) =
            source.take_apply_funding(&completion).unwrap()
        else {
            panic!("inline encoding returns its exact typed funding");
        };
        assert_eq!(
            original
                .encoding
                .resources()
                .unwrap()
                .quota
                .get(TOTAL_BYTES),
            1
        );
        assert!(source.take_apply_funding(&completion).is_err());
        std::mem::swap(&mut original.queue, &mut original.encoding);
        let (completion, funding) =
            match complete_shared_join(completion, SharedApplyFunding::Encoded(original)) {
                Err(original) => original,
                Ok(_) => panic!("same-scope encoding and queue partitions cannot be interchanged"),
            };
        let SharedApplyFunding::Encoded(mut original) = funding else {
            panic!("refusal retains both originals");
        };
        std::mem::swap(&mut original.queue, &mut original.encoding);
        let retirement = complete_shared_join(completion, SharedApplyFunding::Encoded(original))
            .unwrap_or_else(|_| panic!("same source supplies both original partitions"));
        (retirement, None)
    } else if matches!(mode, Mode::Unused) {
        assert!(bound.is_none());
        let funding = source.take_apply_funding(&completion).unwrap();
        assert!(matches!(funding, SharedApplyFunding::UnusedQueue(_)));
        let retirement = complete_shared_join(completion, funding)
            .unwrap_or_else(|_| panic!("same unused queue partition"));
        (retirement, None)
    } else {
        let queue = legacy_queue.expect("legacy disposal fixture owns this queue");
        let original = bound.expect("disposal fixture holds the unexited child");
        let facts = completion.join_facts();
        let queue_id = queue.id();
        let (completion, funds) = match complete_shared_join_with_funding(
            completion,
            SharedApplyFunding::UnusedQueue(queue),
        ) {
            Err(original) => original,
            Ok(_) => panic!("Taken guard rejects unused funding"),
        };
        assert_eq!(completion.join_facts(), facts);
        let SharedApplyFunding::UnusedQueue(queue) = funds else {
            panic!("same funding returned")
        };
        assert_eq!(queue.id(), queue_id);
        let retirement = complete_shared_join_with_funding(
            completion,
            SharedApplyFunding::Encoded(ReturnedQuota {
                queue,
                encoding: original.quota,
            }),
        )
        .unwrap_or_else(|_| panic!("same encoded proof accepted"));
        (retirement, Some((original.custody, original.exit)))
    };
    Row {
        source,
        retirement,
        child,
        publisher: Some(publisher),
        cleanup,
        completed: Vec::with_capacity(4),
        settlement: None,
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
    shared_row_releases_funds_before_wc_queue_exit(Mode::Unused).await;
}
#[tokio::test(flavor = "multi_thread")]
async fn inline_encoding_returns_its_typed_partition_after_the_original_join() {
    shared_row_releases_funds_before_wc_queue_exit(Mode::InlineEncoded { fail_scope: false }).await;
    shared_row_releases_funds_before_wc_queue_exit(Mode::InlineEncoded { fail_scope: true }).await;
}
async fn shared_row_releases_funds_before_wc_queue_exit(mode: Mode) {
    let roles = Roles::new();
    let mut wc = Governed::new();
    let mut runtime = FrontendDataRuntime::new(Handle::current());
    let original = row(&roles, &runtime, &mut wc, mode).await;
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
    oracles.push(encoded.retirement.take_physical_return().is_none());
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
    let encoding = roles.encoding(principal, backend, 64, WorkKind::Cleanup);
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
    let queue = transport::QueueCustody::new(
        queue,
        source.registration(),
        Arc::from([send_key(&operation)]),
    );
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
                        registration.attach_send(slot, vec![input], encoding),
                        Err(RegistrationFailure::Closed)
                    ));
                    entered.send(()).unwrap();
                    std::future::pending::<()>().await;
                };
                let outcome = GatedWork::new(exit, queue, arbitration, inner)
                    .run(generation(), before_handoff, |_, ()| ready(()))
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
    assert!(source.take_unattached_retirement().unwrap().is_none());
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
        .take_unattached_retirement()
        .unwrap()
        .expect("joined original refusal retains the same partition");
    assert_eq!(error, Some(RegistrationFailure::Closed));
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

#[tokio::test(flavor = "multi_thread")]
async fn withdrawn_send_before_apply_retires_its_original_loan_after_join() {
    use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
    use novarocks_attempt_protocol::{
        effect::{
            AttemptEffect, DispatchBatch, EffectBody, EffectClass,
            OperationIntent as ProtocolIntent,
        },
        input::{ExecutorFact, QueuedInputGrantRef, WithdrawalOutcome},
    };
    use novarocks_control_runtime::exit_work::{GatedNotStarted, GatedOutcome, Withdrawal};
    use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};

    let roles = Roles::new();
    let inbox = custody::tests::WithdrawalInbox::new(&roles.scope);
    let mut wc = transport::GovernedQueuedInputFixture::new();
    let backend = BackendProcessId::new_v7();
    let operations = [cancel(backend), cancel(backend)];
    let keys = [
        send_key(&operations[0]),
        transport::SendKey {
            operation: operations[1].operation_id(),
            generation: novarocks_attempt_protocol::input::SendGeneration::new(
                AttemptActionId::new(NonZeroU64::new(2).unwrap()),
            ),
        },
    ];
    let references = [
        QueuedInputGrantRef::new(NonZeroU64::MIN, keys[0].generation),
        QueuedInputGrantRef::new(NonZeroU64::new(2).unwrap(), keys[1].generation),
    ];
    let mut inputs = transport::GrantedInputs::default();
    inputs.install(references[0], wc.admit(keys[0], 0));
    let queued_bytes = operations.iter().map(OperationIntent::queued_bytes).sum();
    let (parent, queue, principal) = roles.parent_with_items(backend, 2, queued_bytes);
    let result = inbox.reserve(&parent);
    let author = result.event_author().unwrap();
    let protocol = operations.each_ref().map(|operation| {
        let OperationIntent::CancelTask(cancel) = operation else {
            unreachable!()
        };
        ProtocolIntent::CancelTask(cancel.clone())
    });
    let effect_id = AttemptActionId::new(NonZeroU64::new(3).unwrap());
    let effect = AttemptEffectEnvelope::new(AttemptEffect {
        id: effect_id,
        class: EffectClass::Settlement,
        body: EffectBody::SendBatch {
            batch: DispatchBatch {
                backend,
                lane: protocol[0].lane(),
                control_progress: true,
                operations: Box::new(protocol),
                queued_bytes,
                queued_at: MonotonicInstant::from_origin(Duration::ZERO),
                queued_inputs: Box::new(references),
            },
            reservation: novarocks_attempt_protocol::capacity::ReservationToken(
                NonZeroU64::new(principal.get()).unwrap(),
            ),
        },
    });
    let requests = transport::FrozenSendRequests::default();
    let prepared = transport::PreparedSendBatch::resolve(effect.clone(), &requests).unwrap();
    assert_eq!(prepared.operations().len(), 2);
    let withdrawal = effect.handoff().unwrap().clone();
    let panic = PanicSink::new();
    let inner = panic.clone();
    let (mut source, exit, launch) =
        OriginalTransportJoinObligation::prepare(parent, observer(), panic)
            .unwrap_or_else(|error| panic!("{}", error.error));
    assert!(
        launch
            .bind_actor_result(effect, &author, result, None)
            .is_ok()
    );
    let registration = source.registration();
    let refused = match prepared.bind(&mut inputs, queue, registration.clone()) {
        Err(refused) => refused,
        Ok(_) => panic!("missing tail must not consume the available prefix"),
    };
    assert_eq!(refused.error, transport::SendBindingError::MissingGrant);
    assert!(inputs.contains(references[0]));
    inputs.install(references[1], wc.admit(keys[1], 0));
    let (queue, resolved) = refused
        .batch
        .bind(&mut inputs, refused.window, registration.clone())
        .unwrap_or_else(|_| panic!("the same complete original batch must bind"));
    assert!(references.iter().all(|grant| !inputs.contains(*grant)));
    let (keys, loans) = resolved.into_parts();
    assert!(!registration.claim_input(0, keys[0]));
    let arbitration = withdrawal.clone();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    launch
        .spawn(
            &Handle::current(),
            exit,
            move |exit, registration| async move {
                assert!(queue.matches_send(&registration, &keys));
                for (index, loan) in loans.iter().enumerate() {
                    assert!(loan.matches_send(&registration, index, keys[index]));
                }
                let before_handoff = async move {
                    entered.send(()).unwrap();
                    std::future::pending::<()>().await;
                    drop(loans);
                };
                let outcome = GatedWork::new(exit, queue, arbitration, inner)
                    .run(generation(), before_handoff, |_, ()| ready(()))
                    .await
                    .unwrap();
                assert_eq!(
                    outcome,
                    GatedOutcome::NotStarted(GatedNotStarted::Withdrawn)
                );
                assert!(
                    registration
                        .stage_actor_outcome(transport::BodyOutcome::Unused(
                            GatedNotStarted::Withdrawn
                        ))
                        .is_ok()
                );
            },
        )
        .unwrap();
    waiting.await.unwrap();
    assert!(source.take_unattached_retirement().unwrap().is_none());
    assert!(source.take_actor_result().is_none());
    inbox.assert_no_result();
    assert_eq!(withdrawal.withdraw(), Withdrawal::Withdrawn);
    assert!(
        poll_fn(|cx| source.poll_shared_completion(cx))
            .await
            .unwrap()
            .is_none()
    );
    let (error, mut retirement) = source
        .take_unattached_retirement()
        .unwrap()
        .expect("actual Join and original loan return qualify retirement");
    assert!(error.is_none());
    assert!(source.take_actor_result().is_none());
    inbox.assert_no_result();
    assert!(!source.try_retire().unwrap());
    assert_eq!(
        roles
            .ledger
            .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
            .unwrap()
            .unwrap()
            .used[0],
        2
    );
    assert!(!retirement.finish_one().unwrap());
    assert!(source.take_actor_result().is_none());
    inbox.assert_no_result();
    assert!(retirement.finish_one().unwrap());
    let mut result = source
        .take_actor_result()
        .expect("actual queue exit releases original result");
    let input = result
        .withdrawal
        .take()
        .unwrap()
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(inbox.consume(input),
        ExecutorFact::Withdrawn { effect, outcome: WithdrawalOutcome::DefinitelyUnsent }
            if effect == effect_id
    ));
    assert!(source.take_actor_result().is_none());
    result.original.close_after_exit().unwrap();
    assert!(source.try_retire().unwrap());
    assert_eq!(
        roles
            .ledger
            .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
            .unwrap()
            .unwrap()
            .used,
        [0, 0]
    );
    roles.scope.close_external().unwrap();
    roles
        .ledger
        .reap_scope_ready(roles.scope.id(), 128)
        .unwrap();
    inbox.finish();
    roles.finish();
    assert!(wc.finish());
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_reaper_releases_physical_funding_before_waiting_for_reply_aliases() {
    use custody::{TransportRetirement, TransportRetirementStep};
    use novarocks_attempt_protocol::input::ExecutorFact;

    #[derive(Default)]
    struct ReplyWake(AtomicUsize);
    impl Wake for ReplyWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let roles = Roles::new();
    let inbox = custody::tests::WithdrawalInbox::new(&roles.scope);
    let mut wc = Governed::new();
    let mut runtime = FrontendDataRuntime::new(Handle::current());
    let PendingRow {
        source,
        publisher,
        reply_input,
        cleanup,
        legacy_queue,
        encoded,
    } = pending_row(
        &roles,
        &runtime,
        &mut wc,
        Mode::InlineEncoded { fail_scope: false },
        Some(&inbox),
    );
    assert!(legacy_queue.is_none());
    drop(publisher);
    let mut reaper = TransportRetirement::new(source, cleanup, observer());
    let handle = Handle::current();
    let used = || async {
        loop {
            if let Some(capacity) = roles
                .ledger
                .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
                .unwrap()
            {
                break capacity.used;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        assert_eq!(used().await[0], 1);
        while used().await != [0, 0] {
            let step = poll_fn(|cx| reaper.poll_step(&handle, cx))
                .await
                .unwrap_or_else(|_| panic!("original Native cleanup must advance"));
            assert!(matches!(step, TransportRetirementStep::Advanced));
        }
    })
    .await
    .unwrap();
    assert!(encoded.lock().unwrap().is_none());
    let mut returned = reaper
        .take_actor_result()
        .unwrap_or_else(|_| panic!("same report ledger has exited the actual job"))
        .expect("WC queue exit makes the original actor result available");
    assert!(matches!(
        returned.outcome,
        Some(transport::BodyOutcome::ReceiptsRecorded)
    ));
    assert!(returned.close_result_after_exit().unwrap());
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        reaper.poll_step(&handle, &mut context),
        Poll::Ready(Ok(TransportRetirementStep::Advanced))
    ));
    assert!(matches!(
        reaper.poll_step(&handle, &mut context),
        Poll::Ready(Ok(TransportRetirementStep::Scanned))
    ));
    assert!(reaper.poll_step(&handle, &mut context).is_pending());
    let fact = inbox.commit(reply_input.unwrap());
    let ExecutorFact::Operation(observed) = fact else {
        panic!("the committed original acknowledgement");
    };
    let reply = loop {
        match reaper.poll_step(&handle, &mut context) {
            Poll::Ready(Ok(TransportRetirementStep::Report(
                transport::SendReportReturn::Receipt {
                    key,
                    delivery_error: None,
                    receipt,
                    ..
                },
            ))) => {
                assert_eq!(key.generation, observed.generation);
                assert_eq!(key.operation, observed.reply.operation_id());
                assert!(receipt.unpublished.is_none());
                break receipt.original;
            }
            Poll::Ready(Ok(TransportRetirementStep::Scanned)) => {}
            _ => panic!("the committed exact report can retire"),
        }
    };
    assert!(matches!(
        reaper.poll_step(&handle, &mut context),
        Poll::Ready(Ok(TransportRetirementStep::Advanced))
    ));
    inbox.assert_no_result();
    let wake = Arc::new(ReplyWake::default());
    let waker = Waker::from(wake.clone());
    assert!(
        reaper
            .poll_step(&handle, &mut Context::from_waker(&waker))
            .is_pending()
    );
    let before = wake.0.load(Ordering::SeqCst);
    drop(reply);
    assert!(wake.0.load(Ordering::SeqCst) > before);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let step = poll_fn(|cx| reaper.poll_step(&handle, cx))
                .await
                .unwrap_or_else(|_| panic!("original reply cleanup must advance"));
            if matches!(step, TransportRetirementStep::Retired) {
                break;
            }
        }
    })
    .await
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
    roles.scope.close_external().unwrap();
    roles.scope.reap_ready(128).unwrap();
    inbox.finish();
    roles.finish();
    assert!(wc.finish());
}

#[tokio::test]
async fn actor_send_claims_core_guard_and_same_admission_grant_before_withdrawal() {
    run_actor_send(true, None).await;
}

#[tokio::test]
async fn actor_send_delivers_native_receipt_and_waits_for_its_final_alias() {
    run_actor_send(false, None).await;
}

#[tokio::test]
async fn actor_send_failure_tracks_live_native_row_until_original_exit() {
    run_actor_send(
        false,
        Some(novarocks_execution_contract::OperationOutcome::Accepted),
    )
    .await;
}

#[tokio::test]
async fn actor_send_failure_retains_worker_owner_failure_and_unknown_residual() {
    run_actor_send(
        false,
        Some(novarocks_execution_contract::OperationOutcome::OwnerFailed),
    )
    .await;
}

async fn run_actor_send(
    withdraw: bool,
    failure_verdict: Option<novarocks_execution_contract::OperationOutcome>,
) {
    use crate::control_runtime::membership::FrozenMember;
    use crate::control_runtime::{
        actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput},
        host::{
            EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding, HostRegistry,
            PendingHandoff, PreparedEffectJob, SourceProgress,
        },
        metrics::FrontendHostObservations,
        transport_withdrawal::AttemptEffectEnvelope,
    };
    use crate::task_execution::clock::ProcessMonotonicClock;
    use crate::task_execution::host::failure_lifecycle::FailureLifecycle;
    use crate::task_execution::host::failure_publication::{FailureContext, FailurePublications};
    use crate::task_execution::host::failure_supervisor::{
        FailureCleanupRequest, FailureSupervisor,
    };
    use crate::task_execution::{
        bridge::terminal::{RegisteredAttemptTerminal, TerminalOutbox},
        host::failure_bridge::PendingFailureBridge,
    };
    use novarocks_attempt_protocol::{
        capacity::ReservationToken,
        effect::{
            AttemptEffect, DispatchBatch, EffectBody, EffectClass,
            OperationIntent as ProtocolIntent, QueuedInputPayload,
        },
        input::{ExecutorFact, QueuedInputGrantRef, WithdrawalOutcome},
    };
    use novarocks_control_runtime::{
        actor::ActorConfig,
        actor_turn::{ActorHostTurn, ActorRuntime, EffectTicket, ProcessClock},
        causal_input::CausalInputObligation,
        obligation::WorkObligation,
        transaction::RuntimeError,
    };
    use novarocks_control_runtime_core::{identity::Tick, mailbox::InputClass, scope::ScopeError};
    use novarocks_coordination_contract::{
        AttemptActionId, AttemptActivationIdentity, AttemptToLogicalBody, MonotonicInstant,
    };
    use novarocks_execution_contract::{
        AbortCause, AbortParticipation, OperationOutcome, ParticipationRef, RuntimeEndpoint,
    };
    use novarocks_query_application::api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
        NativeActiveConvergenceClosure, NativeActiveConvergenceRequest, NativeAttemptConvergence,
        NativeAttemptTerminal, NativeParticipationConvergence,
    };
    use novarocks_types::FrontendProcessId;

    let roles = Roles::new();
    let fail_core = failure_verdict.is_some();
    let mut peer = if withdraw {
        None
    } else {
        Some(crate::native::task_transport::tests::Loopback::start().await)
    };
    let mut wc = Governed::for_control_origin(peer.as_ref().map(|peer| &peer.control_endpoint));
    let handle = Handle::current();
    let mut runtime = FrontendDataRuntime::new(handle.clone());
    let (parent, payer) = roles
        .scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let backend = BackendProcessId::new_v7();
    let execution =
        QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap();
    let participation = ParticipationRef::new(execution, FrontendProcessId::new_v7(), backend);
    let lifecycle = FailureLifecycle::freeze([participation].into_iter());
    let starts = lifecycle.starts();
    if let Some(peer) = &peer {
        peer.accept_abort(participation);
        match failure_verdict {
            Some(OperationOutcome::Accepted) => peer.accept_abort(participation),
            Some(OperationOutcome::OwnerFailed) => peer.fail_abort_owner(),
            None => {}
            Some(other) => panic!("unsupported fixture failure verdict {other:?}"),
        }
    }
    let abort = AbortParticipation::new(
        TaskOperationId::new_v7(),
        participation,
        AbortCause::QueryFailed,
    );
    let operation = OperationIntent::AbortParticipation(abort);
    let key = send_key(&operation);
    let reference = QueuedInputGrantRef::new(NonZeroU64::MIN, key.generation);
    let protocol = ProtocolIntent::AbortParticipation(abort);
    let admit_id = key.generation.admission_effect();
    let admit = AttemptEffectEnvelope::new(AttemptEffect {
        id: admit_id,
        class: EffectClass::Settlement,
        body: EffectBody::AdmitQueuedInput {
            lane: protocol.lane(),
            operation: protocol.clone(),
            generation: key.generation,
            payload: QueuedInputPayload::AlreadyRetained,
        },
    });
    let queue = roles
        .scope
        .reserve_child_next(
            generation(),
            payer.id(),
            transport::send_queue_resources(
                &roles.layout,
                backend,
                TransportCapacityLane::Control,
                1,
                operation.queued_bytes(),
            )
            .unwrap(),
            WorkKind::Settlement,
        )
        .unwrap();
    let reservation = queue.reservation_id();
    let send_id = AttemptActionId::new(NonZeroU64::new(2).unwrap());
    let send = AttemptEffectEnvelope::new(AttemptEffect {
        id: send_id,
        class: EffectClass::Settlement,
        body: EffectBody::SendBatch {
            batch: DispatchBatch {
                backend,
                lane: protocol.lane(),
                control_progress: true,
                operations: Box::new([protocol]),
                queued_bytes: operation.queued_bytes(),
                queued_at: MonotonicInstant::from_origin(Duration::ZERO),
                queued_inputs: Box::new([reference]),
            },
            reservation: ReservationToken(NonZeroU64::new(reservation.get()).unwrap()),
        },
    });
    struct ScopeOwner {
        scope: ScopeHandle,
        parent: WorkObligation,
        payer: ReservationObligation,
        closing: bool,
        exited: bool,
        finished: bool,
        receipt: Arc<Mutex<Option<crate::task_execution::intent::OperationAcknowledgement>>>,
        release_parent: Arc<AtomicBool>,
    }
    impl EffectSource for ScopeOwner {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            unreachable!()
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: &EffectTicket<FrontendEffect>,
            _: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            unreachable!()
        }
        fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
            unreachable!()
        }
        fn core_consumed(&mut self, _: &[novarocks_control_runtime_core::identity::InputId]) {}
        fn retain_transport_failure(&mut self, original: Box<dyn std::any::Any + Send>) {
            use crate::task_execution::host::failure_supervisor::FailureSupervisorError;
            if let Some((error, panic)) =
                original.downcast_ref::<(FailureSupervisorError, PanicSink)>()
            {
                eprintln!("Failure supervisor test observed {error:?}");
                if let Some(raw) = panic.take() {
                    eprintln!(
                        "Failure supervisor raw payload: {:?} {:?}",
                        raw.downcast_ref::<String>(),
                        raw.downcast_ref::<&str>()
                    );
                }
            }
            std::panic::resume_unwind(original);
        }
        fn transport_event(&mut self, _: AttemptActionId, original: custody::TransportSourceEvent) {
            match original {
                custody::TransportSourceEvent::Report(transport::SendReportReturn::Receipt {
                    key,
                    delivery_error: None,
                    receipt,
                    ..
                }) => {
                    assert_eq!(key.operation, receipt.original.operation_id());
                    assert!(receipt.unpublished.is_none());
                    assert!(
                        self.receipt
                            .lock()
                            .unwrap()
                            .replace(receipt.original)
                            .is_none()
                    );
                }
                original => std::panic::resume_unwind(Box::new(original)),
            }
        }
        fn owner_failed_pending(&mut self, original: PendingHandoff) {
            std::panic::resume_unwind(Box::new(original));
        }
        fn close_external(&mut self) {
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            let mut units = 0;
            if self.closing && budget != 0 && self.release_parent.load(Ordering::Acquire) {
                if !self.exited {
                    self.parent.body_exited().unwrap();
                    self.parent.captures_exited().unwrap();
                    self.exited = true;
                    units += 1;
                }
                units += self.scope.reap_ready(budget - units).unwrap();
                if !self.finished && units < budget {
                    match self.payer.finish() {
                        Ok(()) => {
                            self.finished = true;
                            units += 1;
                        }
                        Err(RuntimeError::Capacity(
                            novarocks_control_runtime_core::capacity::CapacityError::Scope(
                                ScopeError::ChildrenRemain,
                            ),
                        )) => {}
                        Err(error) => panic!("{error:?}"),
                    }
                }
            }
            let retired = self.finished && self.scope.join_ready().unwrap();
            SourceProgress {
                units,
                progressed: units != 0,
                more: units != 0 && !retired,
                retired,
            }
        }
    }
    struct OtherExecutor;
    impl EffectExecutor for OtherExecutor {
        fn handoff(&self, original: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
            Err(original)
        }
    }

    #[derive(Default)]
    struct CleanupState {
        transferred: bool,
        dropped: bool,
    }
    struct CleanupPort(
        Arc<Mutex<CleanupState>>,
        Option<Box<dyn transport::FailureResidualPort>>,
    );
    impl transport::FailureResidualPort for CleanupPort {
        fn finish(
            mut self: Box<Self>,
            original: FailureCleanupRequest,
            attempts: Vec<transport::FailureAbortAttempt>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(async move {
                for attempt in &attempts {
                    if let Err(error) = &attempt.result {
                        eprintln!("Original failure Abort: {error:?}");
                    }
                }
                self.1.take().unwrap().finish(original, attempts).await;
                self.0.lock().unwrap().transferred = true;
            })
        }
    }
    impl Drop for CleanupPort {
        fn drop(&mut self) {
            self.0.lock().unwrap().dropped = true;
        }
    }
    struct ProcessFacts {
        backend: BackendProcessId,
        endpoint: RuntimeEndpoint,
        replacement: BackendProcessId,
        changed: tokio::sync::watch::Sender<u64>,
    }
    impl BackendProcessObservationPort for ProcessFacts {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.changed.subscribe()
        }
        fn observe_process_at_endpoint(
            &self,
            expected: BackendProcessId,
            endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            assert_eq!(expected, self.backend);
            assert_eq!(endpoint, &self.endpoint);
            Ok(if *self.changed.borrow() == 0 {
                BackendProcessObservation::Unobservable
            } else {
                BackendProcessObservation::Replaced {
                    current_process: self.replacement,
                }
            })
        }
    }

    let (failure_work, failure_capacity, failure_report) = roles.cleanup(parent.id()).into_parts();
    let failure_payer = failure_capacity.id();
    let failure_work =
        ReportedWork::try_new(failure_work, failure_capacity, failure_report).unwrap();
    let inbox = custody::tests::WithdrawalInbox::with_failure_work(
        &roles.scope,
        [(admit, None), (send.clone(), Some(queue))],
        [8; 4],
        Some(failure_work.work()),
    );
    let admit_command = inbox.reserve_command(&parent);
    let send_command = inbox.reserve_command(&parent);
    let admission = transport::AdmissionSource::new(
        &parent,
        &payer,
        generation(),
        observer(),
        wc.authority(),
        [
            VecDeque::new(),
            VecDeque::from([inbox.reserve_result(&parent)]),
        ],
    )
    .unwrap_or_else(|_| panic!("original source payer"));
    let send_idle = [
        VecDeque::new(),
        VecDeque::from([inbox.reserve_result(&parent)]),
    ];
    let report_author = roles.cleanup(parent.id());
    let requests = transport::FrozenSendRequests::default();
    let endpoint = peer.as_ref().map_or_else(
        || novarocks_types::NativeEndpoint::from_socket_addr(wc.listener.local_addr().unwrap()),
        |peer| peer.control_endpoint.native_endpoint().clone(),
    );
    let context = transport::SendContext {
        registration: inbox.registration().clone(),
        ledger: roles.ledger.clone(),
        scope: roles.scope.clone(),
        layout: roles.layout.clone(),
        transport: TransportBudget::DEFAULT,
        authority: wc.authority(),
        wire: crate::native::task_transport::wire::AttemptWireFacts {
            native_compatibility_id: novarocks_types::NativeCompatibilityId::new([1; 32]),
        },
        client: Client::for_endpoint(
            endpoint,
            NativeEndpointDomain::BackendControl,
            backend,
            runtime.clone(),
        ),
        apply: runtime.apply_send_port().clone(),
        clock: Arc::new(ProcessMonotonicClock::default()),
        observer: observer(),
    };
    let actor = inbox.registration().actor();
    let contexts = Arc::new(BTreeMap::from([(backend, context)]));
    let actor_runtime = inbox.runtime();
    let host = HostRegistry::new(
        actor_runtime.clone(),
        roles.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let receipt = Arc::new(Mutex::new(None));
    let activation = AttemptActivationIdentity::from_parts(1, execution, 1);
    let publications = Arc::new(FailurePublications::new(
        inbox.registration().clone(),
        activation,
        &roles.scope,
    ));
    let original_endpoint = peer.as_ref().map_or_else(
        || RuntimeEndpoint::from_socket_addr(wc.listener.local_addr().unwrap()),
        |peer| peer.control_endpoint.clone(),
    );
    let (failure_bridge, residual_port) = PendingFailureBridge::new(
        inbox.registration().clone(),
        activation,
        Arc::new([FrozenMember {
            participation,
            endpoint: original_endpoint.clone(),
        }]),
        publications.clone(),
    );
    let processes = Arc::new(ProcessFacts {
        backend,
        endpoint: original_endpoint,
        replacement: BackendProcessId::new_v7(),
        changed: tokio::sync::watch::channel(0).0,
    });
    let cleanup = Arc::new(Mutex::new(CleanupState::default()));
    let release_parent = Arc::new(AtomicBool::new(!fail_core));
    let (abort_ingress, abort_sender, abort_drain) = if fail_core {
        use crate::task_execution::bridge::abort::{AbortSettlementOwner, AttemptAbortIngress};
        let role = actor_runtime
            .reserve_admitted_input_for(inbox.registration(), actor, InputClass::Control, &parent)
            .unwrap()
            .into_standing_input()
            .unwrap();
        let ingress = Arc::new(AttemptAbortIngress::new(
            inbox.registration().clone(),
            activation,
            [(participation, role)],
            tokio::sync::watch::channel(0).0,
            None,
        ));
        let (handoff, drain) = tokio::sync::oneshot::channel();
        (
            Some(ingress.clone()),
            Some((Arc::new(AbortSettlementOwner::new(ingress)), handoff)),
            Some(drain),
        )
    } else {
        (None, None, None)
    };
    let failure_port = transport::NativeFailureCleanup::new(
        contexts.clone(),
        failure_payer,
        handle.clone(),
        Box::new(CleanupPort(cleanup.clone(), Some(residual_port))),
    );
    let failure_port = if let Some((owner, sender)) = abort_sender {
        failure_port.with_abort_drain(owner, sender)
    } else {
        failure_port
    };
    let supervisor = FailureSupervisor::new(
        actor_runtime.clone(),
        inbox.registration().clone(),
        failure_work,
        observer(),
        handle.clone(),
    );
    let exit = host
        .register(HostBinding {
            scope: roles.scope.clone(),
            registration: inbox.registration().clone(),
            kind: FrontendActorKind::Test,
            config: ActorConfig {
                mailbox: [8; 4],
                max_batches: 4,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
            policy: FrontendLanePolicy::attempt(1),
            source: Box::new(transport::AttemptTransportSource::new(
                actor_runtime.clone(),
                inbox.registration().clone(),
                inbox.source(),
                roles.scope.clone(),
                admission,
                [None, None, None],
                transport::AttemptSendSource {
                    requests,
                    contexts,
                    report_references: BTreeMap::new(),
                    report_author,
                    idle: send_idle,
                },
                (
                    FailureContext {
                        lifecycle,
                        publications: publications.clone(),
                    },
                    transport::AttemptFailureBinding {
                        supervisor,
                        port: Box::new(failure_port),
                        abort_drain,
                    },
                ),
                Box::new(ScopeOwner {
                    scope: roles.scope.clone(),
                    parent,
                    payer,
                    closing: false,
                    exited: false,
                    finished: false,
                    receipt: receipt.clone(),
                    release_parent: release_parent.clone(),
                }),
            )),
            executor: Arc::new(transport::AttemptTransportExecutor {
                native: transport::ActorTransportExecutor(handle.clone()),
                other: Arc::new(OtherExecutor),
            }),
            transport: Some(custody::TransportRetirements::new(
                actor_runtime.clone(),
                inbox.registration().clone(),
                handle,
                1,
            )),
            startup: None,
            capacity: None,
            membership: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("same host registration"));
    let mut terminal_owner = RegisteredAttemptTerminal::new(
        TerminalOutbox::new(activation),
        failure_bridge.bind(exit, processes.clone()),
        None,
    );
    let advance_host = || {
        while let Some(turn) = actor_runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };

    let admit_input = admit_command.fill(FrontendInput::Test(1)).unwrap();
    assert!(actor_runtime.poll_one(Tick(1)).unwrap());
    advance_host();
    actor_runtime.retire_input(actor, admit_input).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            drive(&mut wc.driver);
            advance_host();
            if let Some((_, fact)) = inbox.try_commit_next() {
                assert!(matches!(fact, ExecutorFact::QueuedInputGranted { grant, .. } if grant == reference));
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    if withdraw {
        assert_eq!(
            send.handoff().unwrap().withdraw(),
            novarocks_control_runtime::exit_work::Withdrawal::Withdrawn
        );
    }
    let send_input = send_command.fill(FrontendInput::Test(1)).unwrap();
    assert!(actor_runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(
        actor_runtime
            .step_reservations(actor, send_input, None, 1)
            .unwrap()[0]
            .0,
        reservation
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut started = false;
        loop {
            drive(&mut wc.driver);
            advance_host();
            if let Some((_, fact)) = inbox.try_commit_next() {
                match fact {
                    ExecutorFact::Withdrawn {
                        effect,
                        outcome: WithdrawalOutcome::DefinitelyUnsent,
                    } if withdraw => {
                        assert_eq!(effect, send_id);
                        break;
                    }
                    ExecutorFact::SendStarted { effect, .. } if !withdraw => {
                        assert_eq!(effect, send_id);
                        assert!(!started);
                        started = true;
                    }
                    ExecutorFact::Operation(observed) if !withdraw => {
                        assert!(started);
                        assert_eq!(observed.generation, key.generation);
                        assert_eq!(observed.reply.operation_id(), key.operation);
                        assert_eq!(
                            observed.reply.worker_outcome(),
                            Some(novarocks_execution_contract::OperationOutcome::Accepted)
                        );
                        break;
                    }
                    other => panic!("unexpected composed send fact: {other:?}"),
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(starts.was_started(participation), !withdraw);
    drop(starts);
    if !withdraw {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                drive(&mut wc.driver);
                advance_host();
                if receipt.lock().unwrap().is_some() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(peer.as_ref().unwrap().control_requests(), 1);
    }
    assert_eq!(
        roles
            .ledger
            .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
            .unwrap()
            .unwrap()
            .used,
        [0, 0]
    );
    assert!(
        actor_runtime
            .step_reservations(actor, send_input, None, 1)
            .unwrap()
            .is_empty()
    );
    actor_runtime.retire_input(actor, send_input).unwrap();
    if fail_core {
        {
            let mut waiting = Box::pin(terminal_owner.terminal());
            assert!(futures::poll!(waiting.as_mut()).is_pending());
        }
        let failed = actor_runtime
            .reserve_input(actor, actor, InputClass::Lifecycle)
            .unwrap()
            .fill(FrontendInput::Test(u64::MAX))
            .unwrap();
        assert!(actor_runtime.poll_one(Tick(2)).unwrap());
        advance_host();
        let terminal = tokio::time::timeout(Duration::from_secs(1), terminal_owner.terminal())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal_owner.terminal().await.unwrap(), terminal);
        assert!(
            matches!(terminal, NativeAttemptTerminal::Failed(error) if error.class() == novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure)
        );
        let lost = terminal_owner.failure_bridge_mut().original_loss();
        assert!(
            matches!(&lost.body, AttemptToLogicalBody::AttemptLost { unknown_participations, unresolved_actions, .. }
            if &**unknown_participations == &[participation] && &**unresolved_actions == &[send_id])
        );
        actor_runtime.retire_input(actor, failed).unwrap();
    }
    actor_runtime.close_actor_admission(actor).unwrap();
    actor_runtime.close_timer_source(actor).unwrap();
    host.close_registration();
    advance_host();
    if !withdraw {
        assert!(
            !host.is_empty(),
            "the original Native row still has a reply alias"
        );
        let original = receipt.lock().unwrap().take().unwrap();
        assert_eq!(
            original.worker_outcome(),
            Some(novarocks_execution_contract::OperationOutcome::Accepted)
        );
        drop(original);
    }
    let cancellation = fail_core.then(|| wc.root.owner.scope().cancellation().unwrap());
    if fail_core {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !cleanup.lock().unwrap().transferred {
                drive(&mut wc.driver);
                advance_host();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(abort_ingress.unwrap().close_after_exit().unwrap());
        let deadline = if failure_verdict == Some(OperationOutcome::Accepted) {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(5)
        };
        let mut waiting = Box::pin(terminal_owner.failure_bridge_mut().converge(
            NativeActiveConvergenceRequest::new(cancellation.as_ref().unwrap().clone(), deadline),
        ));
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "remote deadline cannot settle original local work"
        );
        drop(waiting);
        assert!(!host.is_empty());
        release_parent.store(true, Ordering::Release);
        actor_runtime
            .notify_host_actor(inbox.registration())
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while !host.is_empty() {
            drive(&mut wc.driver);
            advance_host();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(cleanup.lock().unwrap().dropped);
    if fail_core {
        let cancellation = cancellation.unwrap();
        let (residual, attempts) = terminal_owner.failure_bridge_mut().original_residual();
        let [abort] = attempts else {
            panic!("one frozen failure abort")
        };
        assert_eq!(abort.request.participation(), participation);
        let outcome = abort
            .result
            .as_ref()
            .unwrap_or_else(|error| panic!("failed to deliver original failure Abort: {error:?}"));
        let receipt = outcome.receipt.as_ref().unwrap();
        assert_eq!(receipt.operation, abort.request.envelope().operation_id());
        assert_ne!(receipt.operation, key.operation);
        assert_eq!(
            receipt.worker.as_ref().unwrap().outcome(),
            failure_verdict.unwrap()
        );
        assert_eq!(
            receipt.participation_state,
            (failure_verdict == Some(OperationOutcome::Accepted))
                .then_some(novarocks_execution_contract::ParticipationState::Aborting)
        );
        assert_eq!(peer.as_ref().unwrap().control_requests(), 2);
        assert_eq!(&*residual.unknown_participations, &[participation]);
        residual.publication.unwrap();
        if failure_verdict == Some(OperationOutcome::OwnerFailed) {
            processes.changed.send_replace(1);
        }
        let closure = tokio::time::timeout(
            Duration::from_secs(1),
            terminal_owner
                .failure_bridge_mut()
                .converge(NativeActiveConvergenceRequest::new(
                    cancellation.clone(),
                    Instant::now() + Duration::from_secs(5),
                )),
        )
        .await
        .unwrap()
        .unwrap();
        if failure_verdict == Some(OperationOutcome::Accepted) {
            assert!(
                matches!(closure, NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_participations } if &*exact_participations == &[participation])
            );
            processes.changed.send_replace(1);
            let original = terminal_owner
                .failure_bridge_mut()
                .converge(NativeActiveConvergenceRequest::new(
                    cancellation,
                    Instant::now() + Duration::from_secs(5),
                ))
                .await
                .unwrap();
            assert!(
                matches!(original, NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_participations } if &*exact_participations == &[participation])
            );
        } else {
            assert!(
                matches!(closure, NativeActiveConvergenceClosure::Evidence(NativeAttemptConvergence::Participations(facts)) if &*facts == &[NativeParticipationConvergence::worker_process_replaced(participation)])
            );
        }
    } else {
        assert!(!cleanup.lock().unwrap().transferred);
    }
    assert!(host.is_empty() && actor_runtime.is_empty() && roles.ledger.is_empty());
    let deadline = Instant::now() + Duration::from_secs(3);
    runtime.drain_original_apply_until(deadline).await.unwrap();
    runtime
        .drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    runtime.drain_native_outgoing_until(deadline).await.unwrap();
    if let Some(peer) = peer.as_mut() {
        assert!(peer.settle().await);
    }
    assert!(wc.finish());
}
