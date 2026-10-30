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

//! Original fixed-WC source for actual Native wire fixtures. This fixture
//! proves request/physical/retained lifetime, not complete Native protection.
use super::*;
use crate::native::task_transport::queue_input::queued_header_quote;
use crate::task_execution::intent::TaskOperationQueuePreparation;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub(super) const FIXTURE_CHANGES_PER_REQUEST: usize = 2;

pub(super) struct GovernedWireRole {
    control: WorkloadControl,
    driver: Mutex<JointProgressDriver>,
    _root: novarocks_workload_control::RootWork,
    window: novarocks_workload_control::ResultWindowGrant,
}
impl GovernedWireRole {
    pub(super) fn new(peer: &Loopback) -> Self {
        let count = 8;
        let wait_source =
            NativeTaskOperationSink::proposal_wait_source_geometry(TransportBudget::DEFAULT)
                .unwrap();
        let wait_backing =
            std::alloc::Layout::array::<u8>(wait_source.resident_bytes().unwrap()).unwrap();
        let storage = ControlStorageConfig::new(count, &[]).with_process_source_backing(
            novarocks_workload_control::PollRoleSourceBacking::from_layouts(&[], &[wait_backing])
                .unwrap(),
        );
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
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
        let mut proposal = scope.begin_control_binding().unwrap();
        for endpoint in [&peer.endpoint, &peer.control_endpoint] {
            let origin = novarocks_native_adapter::FrontendNativeTransport::plaintext()
                .selected_network_origin(endpoint.native_endpoint())
                .unwrap()
                .into_owned();
            proposal
                .push_dependency(&novarocks_resource_contract::SharedDependency::Network(
                    origin,
                ))
                .unwrap();
        }
        proposal.end_change().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive_original(&mut driver);
        let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("original control binding must grant"),
        };
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop(ready);
        drive_original(&mut driver);
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        Self {
            control,
            driver: Mutex::new(driver),
            _root: root,
            window,
        }
    }
    pub(super) fn authority(&self) -> NativeAsyncActionAuthority {
        self.window
            .retain_alias()
            .native_action_authority()
            .unwrap()
    }

    pub(super) fn drive(&self) {
        drive_original(&mut self.driver.lock().unwrap())
    }
    pub(super) fn finish(
        self,
    ) -> Result<
        novarocks_workload_control::WorkloadShutdown,
        novarocks_workload_control::WorkloadShutdownFailure,
    > {
        // The caller has joined the original Native owners and loopback.
        // Their real sources kept this same Root/driver alive until here.
        let Self {
            control,
            driver,
            _root: root,
            window,
        } = self;
        let mut driver = driver.into_inner().unwrap();
        drop(window);
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        control.close_admission();
        drive_original(&mut driver);
        control.shutdown()
    }
    pub(super) fn reserve(
        &self,
        sink: &NativeTaskOperationSink,
        intent: &OperationIntent,
    ) -> Box<dyn crate::task_execution::intent::TaskOperationQueuePermit> {
        let mut preparation = TaskOperationQueuePreparation::new();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => return permit,
            TaskOperationQueueAdmission::Rejected(error) => {
                panic!("original paid input refused: {error}")
            }
            _ => {}
        }
        self.drive();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => permit,
            result => panic!("original paid wire fixture input remains unavailable: {result:?}"),
        }
    }
}
fn drive_original(driver: &mut JointProgressDriver) {
    for _ in 0..100000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("finite original wire fixture proof driver failed to reach idle")
}

/// Result delivery releases its position independently of the original
/// control work. Only that original work may finish its paid settlement.
#[tokio::test(flavor = "multi_thread")]
async fn original_settlement_crosses_wire_after_last_result_window_alias_exits() {
    use crate::task_execution::intent::TaskOperationSink;
    use novarocks_workload_control::WorkError;

    let backend = BackendProcessId::new_v7();
    let mut peer = Loopback::start().await;
    let governed = GovernedWireRole::new(&peer);
    let authority = governed.authority();
    let GovernedWireRole {
        control,
        driver,
        _root: root,
        window,
    } = governed;
    let mut driver = driver.into_inner().unwrap();
    let mut runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let sink = NativeTaskOperationSink::new(
        &[peer.descriptor(backend)],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        runtime.clone(),
        authority.clone(),
    )
    .unwrap();
    drop(window);
    assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
    let mut foreground = TaskOperationQueuePreparation::new();
    assert!(matches!(
        sink.try_reserve_queue(
            update_intent(1, backend, 1).queue_request(),
            &mut foreground
        ),
        TaskOperationQueueAdmission::Rejected(WorkError::Released)
    ));
    drop(foreground);

    // The peer's explicit rejection is an independent wire oracle: a local
    // successful admission or synthetic ACK cannot satisfy this assertion.
    peer.peer.expect_apply(ApplyAnswer::Outcomes(vec![
        OperationOutcome::ResourceExhausted,
    ]));
    let intent = cancel_intent(1, backend);
    let mut preparation = TaskOperationQueuePreparation::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let permit = tokio::time::timeout_at(deadline, async {
        loop {
            match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
                TaskOperationQueueAdmission::Admitted(permit) => break permit,
                TaskOperationQueueAdmission::Rejected(error) => {
                    panic!("original settlement input refused: {error}")
                }
                _ => {}
            }
            drive_original(&mut driver);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut batch = DispatchBatch::with_queued_at(
        backend,
        DispatchLane::Lifecycle,
        vec![intent.clone()],
        vec![permit],
        intent.queued_bytes(),
        novarocks_coordination_contract::MonotonicInstant::ORIGIN,
    );
    tokio::time::timeout_at(deadline, async {
        loop {
            batch = match sink.try_submit(batch) {
                TaskOperationSubmit::Accepted => break,
                TaskOperationSubmit::Backpressured(original) => original,
                other => panic!("original settlement send refused: {other:?}"),
            };
            drive_original(&mut driver);
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let received = settled(&acks, 1).await;
    assert_eq!(received[0].operation_id(), intent.operation_id());
    assert_eq!(
        received[0].worker_outcome(),
        Some(OperationOutcome::ResourceExhausted)
    );
    assert_eq!(peer.peer.control_requests(), 1);
    assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
    drop((received, preparation, intent, sink, acks));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    runtime
        .drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    runtime.drain_original_apply_until(deadline).await.unwrap();
    runtime.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.settle().await);
    drop((runtime, peer));
    drive_original(&mut driver);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    drive_original(&mut driver);
    assert_eq!(
        authority
            .begin_queued_input_when_available(
                JointLane::Settlement,
                queued_header_quote(0).unwrap(),
            )
            .err()
            .map(|failure| failure.error),
        Some(WorkError::Released),
        "settlement never revives an exited original Work"
    );
    control.close_admission();
    drive_original(&mut driver);
    assert!(control.shutdown().is_ok());
}

/// Exercise the original paid preparation which RemoteTask withdraws before
/// publication. Physical saturation is real; no operation is sent to the peer.
#[tokio::test(flavor = "multi_thread")]
async fn unpublished_paid_input_withdrawal_recycles_queue_but_keeps_retained_payload() {
    use crate::task_execution::intent::TaskOperationSink;
    use novarocks_resource_contract::ExistingOwnerRetentionLease;
    use novarocks_workload_control::WorkloadShutdownError;

    let backend = BackendProcessId::new_v7();
    let mut peer = Loopback::start().await;
    let governed = GovernedWireRole::new(&peer);
    let mut runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let sink = NativeTaskOperationSink::new(
        &[peer.descriptor(backend)],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        runtime.clone(),
        governed.authority(),
    )
    .unwrap();
    let supervisor = runtime.task_transport_supervisor().clone();
    let physical = supervisor.capacity_sources();
    let blocker = supervisor
        .try_reserve_queue(
            &sink.transport_waiter,
            backend,
            NativeTransportLane::Ordinary,
            physical.target_items,
            1,
        )
        .expect("the original target window is genuinely occupied");
    assert!(matches!(
        supervisor.try_reserve_queue(
            &sink.transport_waiter,
            backend,
            NativeTransportLane::Ordinary,
            1,
            1,
        ),
        Err(NativeTransportBackpressure::Target)
    ));

    // The existing fixture has exactly 128 Custody QueueRecords. Exceed that
    // total without changing any limit: a leaked queued record eventually
    // prevents a later paid preparation, even though no request was published.
    let mut retained_input: Option<(Box<[u8; 16]>, ExistingOwnerRetentionLease)> = None;
    for ordinal in 0..129u32 {
        let intent = update_intent(ordinal + 1, backend, 1);
        let request = intent
            .queue_request()
            .with_native_input(queued_header_quote(std::mem::size_of::<[u8; 16]>()).unwrap());
        let mut original_intent = Some(intent);
        let mut preparation = TaskOperationQueuePreparation::new();
        assert!(matches!(
            sink.try_reserve_queue(request, &mut preparation),
            TaskOperationQueueAdmission::TargetFull
        ));
        governed.drive();
        assert!(matches!(
            sink.try_reserve_queue(request, &mut preparation),
            TaskOperationQueueAdmission::TargetFull
        ));
        assert!(preparation.admission.is_none());
        assert!(preparation.ready.is_none());
        assert!(preparation.failure.is_none());
        let grant = preparation
            .grant
            .as_ref()
            .expect("same paid Q completed before the genuine physical refusal");
        // Construct only after the paid grant. The first immutable input is
        // handed to an independent retained receiver before its queue source
        // is removed; subsequent never-published inputs actually drop here.
        let input = Box::new([ordinal as u8; 16]);
        if ordinal == 0 {
            retained_input = Some((input, grant.retain_payload().unwrap()));
        } else {
            drop(input);
        }
        drop(original_intent.take());
        assert!(original_intent.is_none());
        preparation.withdraw_unpublished();
        assert!(preparation.grant.is_none());
        assert!(preparation.request.is_none());
        assert!(preparation.admission.is_none());
        assert!(preparation.ready.is_none());
        assert!(preparation.failure.is_none());
        governed.drive();
        let retained = retained_input.as_ref().unwrap();
        assert_eq!(retained.0.as_slice(), &[0; 16]);
        assert_eq!(retained.1.capacity_bytes().unwrap(), 16);
    }

    // Re-enter the same actual sink after the original physical holder exits.
    // Its returned permit performs both real physical and paid queue release.
    drop(blocker);
    let final_intent = update_intent(130, backend, 1);
    let final_permit = governed.reserve(&sink, &final_intent);
    drop(final_intent);
    drop(final_permit);
    governed.drive();
    assert!(peer.peer.applied().is_empty());
    assert_eq!(peer.peer.control_requests(), 0);

    drop(sink);
    drop(acks);
    drop(supervisor);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    runtime
        .drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    runtime.drain_original_apply_until(deadline).await.unwrap();
    runtime.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.settle().await);
    drop(runtime);
    drop(peer);

    let GovernedWireRole {
        control,
        driver,
        _root: root,
        window,
    } = governed;
    let mut driver = driver.into_inner().unwrap();
    drop(window);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    drive_original(&mut driver);
    // Completed root and withdrawn queue are insufficient to release the
    // independent original payload. The real unique process owner is returned.
    let failure = control
        .shutdown()
        .expect_err("retained input still owns its paid record");
    assert_eq!(failure.error(), WorkloadShutdownError::NotDrained);
    let (_, control) = failure.into_parts();
    drop(retained_input.take());
    drive_original(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "same original governor truly drains after the final payload exit"
    );
}

/// Each original input remains in its own stable preparation while the
/// responsibility driver is paused; no retired quota creates a boxed wait F.
#[tokio::test(flavor = "multi_thread")]
async fn same_sink_pending_preparations_keep_original_requests_without_quota_wait_futures() {
    use crate::task_execution::intent::TaskOperationSink;
    use futures::FutureExt;
    let backend = BackendProcessId::new_v7();
    let mut peer = Loopback::start().await;
    let governed = GovernedWireRole::new(&peer);
    let mut runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let wake = Arc::new(CountingWake::default());
    let acks = TaskAckIntake::new(wake.clone());
    let sink = NativeTaskOperationSink::new(
        &[peer.descriptor(backend)],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        runtime.clone(),
        governed.authority(),
    )
    .unwrap();
    let mut preparations: [TaskOperationQueuePreparation; 3] =
        std::array::from_fn(|_| TaskOperationQueuePreparation::new());
    let intents = [
        cancel_intent(1, backend),
        cancel_intent(2, backend),
        cancel_intent(3, backend),
    ];
    let original_permit = governed.reserve(&sink, &intents[0]);
    // Installation retires the constructor independently of the original
    // retained input and its actual physical queue lifetime.
    governed.drive();
    let action_input = DispatchBatch::with_queued_at(
        backend,
        DispatchLane::Lifecycle,
        vec![intents[0].clone()],
        vec![original_permit],
        intents[0].queued_bytes(),
        novarocks_coordination_contract::MonotonicInstant::ORIGIN,
    );
    let action_quote = sink.quote_original_batch(&action_input).unwrap();
    let scope = governed._root.owner.scope();
    let first = scope.begin_control_binding().unwrap();
    let second = scope.begin_control_binding().unwrap();
    let unsubmitted = [first, second];
    let body = std::panic::AssertUnwindSafe(async {
        for (intent, preparation) in intents.iter().zip(&mut preparations) {
            assert!(matches!(
                sink.try_reserve_queue(intent.queue_request(), preparation),
                TaskOperationQueueAdmission::TargetFull
            ));
            assert!(
                !preparation
                    .admission
                    .as_ref()
                    .unwrap()
                    .is_waiting_for_proposal_slot()
            );
            assert!(preparation.ready.is_none() && preparation.grant.is_none());
        }
        assert_eq!(sink.proposal_wait.created_wait_futures(), 0);
        assert!(!sink.proposal_wait.has_wait_future());
        // The action also keeps its original request; this is not a physical
        // dispatch or a reason to manufacture a quota-wait Source.
        let mut action = governed
            .authority()
            .action_source(JointLane::Settlement, action_quote)
            .unwrap()
            .begin_when_available()
            .unwrap();
        let waker = Waker::from(Arc::clone(&acks.inner));
        let mut cx = Context::from_waker(&waker);
        assert!(
            action
                .poll_local_offer(&mut cx, &mut |signal, cx| sink.proposal_wait.poll(
                    signal,
                    cx,
                    &acks.inner
                ))
                .is_pending()
        );
        assert_eq!(sink.proposal_wait.created_wait_futures(), 0);
        let before = governed.control.progress_revision();
        let wakes_before = wake.count();
        for (intent, preparation) in intents.iter().zip(&mut preparations) {
            assert!(matches!(
                sink.try_reserve_queue(intent.queue_request(), preparation),
                TaskOperationQueueAdmission::TargetFull
            ));
        }
        assert_eq!(governed.control.progress_revision(), before);
        assert_eq!(wake.count(), wakes_before);
        assert_eq!(sink.proposal_wait.created_wait_futures(), 0);
        drop(action);
        governed.drive();
        // The same service installs these original inputs even while the
        // unrelated unsubmitted builders above remain alive.
        for (intent, preparation) in intents.iter().zip(&mut preparations) {
            let permit = match sink.try_reserve_queue(intent.queue_request(), preparation) {
                TaskOperationQueueAdmission::Admitted(permit) => permit,
                value => {
                    panic!("same original input did not receive its responsibility: {value:?}")
                }
            };
            drop(permit);
            preparation.withdraw_unpublished();
            governed.drive();
        }
        // Teardown still withdraws a genuine original pending request; no
        // physical operation has been submitted or implicitly completed.
        assert!(matches!(
            sink.try_reserve_queue(intents[0].queue_request(), &mut preparations[0]),
            TaskOperationQueueAdmission::TargetFull
        ));
        assert!(preparations[0].admission.is_some());
        assert!(!sink.proposal_wait.has_wait_future());
        assert!(peer.peer.applied().is_empty());
        assert_eq!(peer.peer.control_requests(), 0);
    })
    .catch_unwind()
    .await;
    drop(unsubmitted);
    for preparation in &mut preparations {
        preparation.withdraw_unpublished();
    }
    drop(preparations);
    drop(intents);
    drop(action_input);
    drop(sink); // Original preparations retired before the registration lease.
    drop(acks);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let subscriptions = runtime.drain_original_subscriptions_until(deadline).await;
    let apply = runtime.drain_original_apply_until(deadline).await;
    let outgoing = runtime.drain_native_outgoing_until(deadline).await;
    let peer_exit = peer.settle().await;
    drop(runtime);
    drop(peer);
    let GovernedWireRole {
        control,
        driver,
        _root: root,
        window,
    } = governed;
    let mut driver = driver.into_inner().unwrap();
    drop(window);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    drive_original(&mut driver);
    let shutdown = control.shutdown();
    // Keep original cleanup results until the primary has a stable receiver.
    // The tuple has no diagnostic formatting of arbitrary original causes.
    if body.is_err()
        || subscriptions.is_err()
        || apply.is_err()
        || outgoing.is_err()
        || !peer_exit
        || shutdown.is_err()
    {
        std::panic::panic_any((body, subscriptions, apply, outgoing, peer_exit, shutdown));
    }
}
