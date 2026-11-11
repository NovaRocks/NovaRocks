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
use crate::{
    control_runtime::{
        FrontendControlRuntime, FrontendControlRuntimeConfig, ReplacementQualificationScope,
        ReplacementQualificationScopeError,
    },
    native::{
        data_runtime::FrontendDataRuntime,
        task_transport::{tests::Loopback, wire::AttemptWireFacts},
        transport::Client,
        transport_capacity::TOTAL_ITEMS,
    },
    task_execution::{clock::ProcessMonotonicClock, host::transport::GovernedQueuedInputFixture},
};
use novarocks_attempt_protocol::input::SendGeneration;
use novarocks_control_runtime::{
    completion::ReportSlot, exit_custody::ExitCustody, exit_work::GatedNotStarted,
    reported_work::ReportedWork, transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::ConsumerId,
};
use novarocks_coordination_contract::{AttemptActionId, MonotonicClock};
use novarocks_execution_contract::{
    AdmissionEpochCapability, AdmissionTicketId, LeaseValidFor, ParticipationRef, TaskOperationId,
};
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId,
};
use std::{
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};
use tokio::runtime::Handle;

const GENERATION: Generation = Generation::new(1).unwrap();

struct QualificationFixture {
    layout: NativeTransportCapacity,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    original: ReportedWork,
}
impl QualificationFixture {
    fn new() -> Self {
        // The original Ordinary target item share is one; the process share
        // permits more. A single held queue item blocks only its exact target.
        let budget = TransportBudget::new(
            1,
            4096,
            4096,
            1,
            4096,
            4,
            16384,
            1,
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        Self::with_budget(budget)
    }

    fn with_budget(budget: TransportBudget) -> Self {
        let layout = NativeTransportCapacity::from_transport(budget).unwrap();
        let ledger =
            RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
                .unwrap();
        let scope = ledger.open_scope_next(None, GENERATION).unwrap();
        let original = reported(
            &scope,
            ReservationResources::for_work(ResourceVector::default()),
        );
        Self {
            layout,
            ledger,
            scope,
            original,
        }
    }

    fn occupy(&self, request: AcquireParticipationAdmissionTicket) -> ReportedWork {
        let intent = OperationIntent::AcquireParticipationAdmissionTicket(request);
        let resources = self
            .layout
            .queue_resources(
                intent.backend_process_id(),
                TransportCapacityLane::Ordinary,
                1,
                intent.queued_bytes(),
            )
            .unwrap();
        reported(&self.scope, resources)
    }

    fn wait<'a>(
        &'a self,
        request: AcquireParticipationAdmissionTicket,
        send: SendKey,
        input: &'a OriginalQueuedInput,
    ) -> impl Future<Output = Result<(StepRegistration, WorkKind), ReplacementQueueError>> + 'a
    {
        reserve_replacement_send(
            &self.ledger,
            &self.layout,
            self.original.work(),
            GENERATION,
            request,
            send,
            input,
        )
    }

    fn used(&self) -> u64 {
        self.ledger
            .try_capacity_dimensions([TOTAL_ITEMS])
            .unwrap()
            .unwrap()
            .used[0]
    }

    fn finish(self) {
        self.scope.close_external().unwrap();
        retire_unused(self.original);
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}

fn reported(scope: &ScopeHandle, mut resources: ReservationResources) -> ReportedWork {
    resources.owned_slots.completions = 1;
    resources.owned_slots.captures = 1;
    let (work, mut capacity) = scope
        .reserve_next(
            GENERATION,
            resources,
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        GENERATION,
        1,
        [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
    )
    .unwrap();
    ReportedWork::try_new(work, capacity, report).unwrap()
}

fn retire_unused(original: ReportedWork) {
    let (custody, exit) = ExitCustody::from_admitted(original);
    exit.revoke_unused_and_exit().unwrap();
    assert!(custody.try_retire().unwrap());
}

fn request() -> AcquireParticipationAdmissionTicket {
    let execution =
        QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap();
    let participation = ParticipationRef::new(
        execution,
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    AcquireParticipationAdmissionTicket::new(
        TaskOperationId::new_v7(),
        participation,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([0x71; 32]),
        AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
    )
}

fn key(request: AcquireParticipationAdmissionTicket, generation: u64) -> SendKey {
    SendKey {
        operation: request.envelope().operation_id(),
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(generation).unwrap())),
    }
}

#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl CountWake {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

struct Membership(watch::Sender<u64>);

impl BackendProcessObservationPort for Membership {
    fn subscribe_process_changes(&self) -> watch::Receiver<u64> {
        self.0.subscribe()
    }

    fn observe_process_at_endpoint(
        &self,
        _: BackendProcessId,
        _: &novarocks_execution_contract::RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        Ok(BackendProcessObservation::Current)
    }
}

#[test]
fn rejects_wrong_request_or_send_generation_and_keeps_the_exact_wc_input() {
    let mut governed = GovernedQueuedInputFixture::new();
    let fixture = QualificationFixture::new();
    let operation = request();
    let expected = key(operation, 1);
    let input = governed.admit(expected, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let wrong_request = std::pin::pin!(fixture.wait(request(), expected, &input)).poll(&mut cx);
    let wrong_generation =
        std::pin::pin!(fixture.wait(operation, key(operation, 2), &input)).poll(&mut cx);
    let refused_before_reserving =
        fixture.used() == 0 && fixture.ledger.capacity_waiter_count() == 0;
    let original = std::pin::pin!(fixture.wait(operation, expected, &input)).poll(&mut cx);
    let actual = match original {
        Poll::Ready(Ok((guard, kind))) => {
            let charged = fixture.used() == 1 && guard.scope().same_scope(&fixture.scope);
            drop(guard);
            charged && kind == WorkKind::Business
        }
        _ => false,
    };
    let clean_queue = fixture.used() == 0;
    drop(input);
    fixture.finish();
    let clean_wc = governed.finish();
    assert!(matches!(
        wrong_request,
        Poll::Ready(Err(ReplacementQueueError::WrongOriginalInput))
    ));
    assert!(matches!(
        wrong_generation,
        Poll::Ready(Err(ReplacementQueueError::WrongOriginalInput))
    ));
    assert!(refused_before_reserving && actual && clean_queue && clean_wc);
}

#[test]
fn target_release_wakes_the_current_waiter_and_cancel_unregisters_without_exiting_wc() {
    let mut governed = GovernedQueuedInputFixture::new();
    let fixture = QualificationFixture::new();
    let operation = request();
    let expected = key(operation, 1);
    let input = governed.admit(expected, 0);
    let original_wake = Arc::new(CountWake::default());
    let original_waker = Waker::from(Arc::clone(&original_wake));
    let current_wake = Arc::new(CountWake::default());
    let current_waker = Waker::from(Arc::clone(&current_wake));
    let mut original_cx = Context::from_waker(&original_waker);
    let mut current_cx = Context::from_waker(&current_waker);

    let blocker = fixture.occupy(operation);
    let mut cancelled = Box::pin(fixture.wait(operation, expected, &input));
    let pending_before_cancel = cancelled.as_mut().poll(&mut original_cx).is_pending();
    let registered_before_cancel = fixture.ledger.capacity_waiter_count() == 1;
    drop(cancelled);
    let unregistered = fixture.ledger.capacity_waiter_count() == 0;
    retire_unused(blocker);
    let no_cancelled_wake = original_wake.count() == 0;

    let blocker = fixture.occupy(operation);
    let mut waiting = Box::pin(fixture.wait(operation, expected, &input));
    let pending = waiting.as_mut().poll(&mut original_cx).is_pending()
        && waiting.as_mut().poll(&mut current_cx).is_pending();
    let one_waiter = fixture.ledger.capacity_waiter_count() == 1;
    retire_unused(blocker);
    let actual_wake = original_wake.count() == 0 && current_wake.count() == 1;
    let admitted = match waiting.as_mut().poll(&mut current_cx) {
        Poll::Ready(Ok((guard, kind))) => {
            let charged = fixture.used() == 1 && guard.scope().same_scope(&fixture.scope);
            drop(guard);
            charged && kind == WorkKind::Business
        }
        _ => false,
    };
    drop(waiting);
    let clean_queue = fixture.used() == 0 && fixture.ledger.capacity_waiter_count() == 0;
    drop(input);
    fixture.finish();
    let clean_wc = governed.finish();
    assert!(pending_before_cancel && registered_before_cancel && unregistered && no_cancelled_wake);
    assert!(pending && one_waiter && actual_wake && admitted && clean_queue && clean_wc);
}

#[tokio::test]
async fn cancellation_after_queue_binding_retires_the_same_unstarted_send() {
    use crate::task_execution::host::transport::BodyOutcome;

    let mut peer = Loopback::start().await;
    let mut governed = GovernedQueuedInputFixture::new();
    let fixture = QualificationFixture::with_budget(TransportBudget::DEFAULT);
    let request = request();
    let executor = Handle::current();
    let mut runtime = FrontendDataRuntime::new(executor.clone());
    let authority = governed.authority();
    let clock: Arc<dyn MonotonicClock> = Arc::new(ProcessMonotonicClock::default());
    let client = Client::for_backend(
        peer.descriptor(request.participation().backend_process_id()),
        runtime.clone(),
    );
    let wire = AttemptWireFacts {
        native_compatibility_id: request.native_compatibility_id(),
    };
    let (cancel, mut cancellation) = watch::channel(false);
    let expiry = Instant::now() + Duration::from_secs(5);
    let result = {
        let context = SingleSendContext {
            ledger: &fixture.ledger,
            scope: &fixture.scope,
            layout: &fixture.layout,
            transport: fixture.layout.transport_budget(),
            authority: &authority,
            wire: &wire,
            client: &client,
            apply: runtime.apply_send_port(),
            clock: &clock,
            observer: ConsumerId::new(1).unwrap(),
            executor: &executor,
        };
        let mut send = std::pin::pin!(send_replacement_admission(
            context,
            fixture.original.work(),
            GENERATION,
            request,
            key(request, 1),
            &mut cancellation,
            expiry,
        ));
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            assert!(send.as_mut().poll(&mut cx).is_pending());
            if fixture.used() == 1 {
                // The sole physical queue row now owns its exact WC grant.
                // The single-thread executor has not polled the child yet.
                break;
            }
            governed.progress();
            assert!(Instant::now() < expiry, "original queue binding stalled");
            tokio::task::yield_now().await;
        }
        cancel.send(true).unwrap();
        loop {
            if let Poll::Ready(result) = send.as_mut().poll(&mut cx) {
                break result.unwrap();
            }
            governed.progress();
            assert!(Instant::now() < expiry, "original send did not retire");
            tokio::task::yield_now().await;
        }
    };
    assert_eq!(result.stopped, Some(ReplacementSendStop::Cancelled));
    assert!(matches!(
        result.original.body,
        Some(BodyOutcome::Unused(GatedNotStarted::Withdrawn))
    ));
    assert!(result.original.receipt.is_none());
    assert_eq!(fixture.used(), 0);
    assert_eq!(fixture.ledger.capacity_waiter_count(), 0);
    assert!(peer.applied_requests().is_empty());
    drop((client, authority));
    fixture.finish();
    assert!(governed.finish());
    runtime
        .drain_original_apply_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(runtime.original_apply_joined());
    assert!(peer.settle().await);
}

#[tokio::test]
async fn fresh_send_generations_share_the_same_frozen_acquire_and_retire_apply() {
    let mut peer = Loopback::start().await;
    let request = request();
    let descriptor = peer.descriptor(request.participation().backend_process_id());
    let mut governed = GovernedQueuedInputFixture::with_origin(Some(descriptor.endpoint()));
    let executor = Handle::current();
    let mut runtime = FrontendDataRuntime::new(executor.clone());
    let authority = governed.authority();
    let clock: Arc<dyn MonotonicClock> = Arc::new(ProcessMonotonicClock::default());
    let mut control = FrontendControlRuntime::start(
        FrontendControlRuntimeConfig {
            pollers: NonZeroUsize::new(executor.metrics().num_workers()).unwrap(),
            clock: clock.clone(),
            transport: TransportBudget::DEFAULT,
            workload_scope_limit: NonZeroUsize::new(8).unwrap(),
            history: None,
        },
        &runtime,
        Arc::new(Membership(watch::channel(0).0)),
    )
    .unwrap();
    let attempts = control.attempts();
    let qualification = attempts.open_replacement_qualification().unwrap();
    let parent = qualification.parent();
    let layout = attempts.transport_capacity();
    let client = Client::for_backend(descriptor, runtime.clone());
    let wire = AttemptWireFacts {
        native_compatibility_id: request.native_compatibility_id(),
    };
    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([0x73; 16]).unwrap(),
        request.participation(),
        request.valid_for(),
    );
    let (_cancel, mut cancellation) = watch::channel(false);
    let (finish_progress, finished) = watch::channel(false);
    let progress = executor.spawn(async move {
        while !*finished.borrow() {
            governed.progress();
            tokio::task::yield_now().await;
        }
        governed
    });
    peer.expect_transport_unknown();
    peer.expect_admission_ticket(OperationOutcome::Idempotent, ticket);
    for (generation, verdict) in [(1, None), (2, Some(OperationOutcome::Idempotent))] {
        let context = SingleSendContext {
            ledger: attempts.ledger(),
            scope: parent.scope(),
            layout,
            transport: layout.transport_budget(),
            authority: &authority,
            wire: &wire,
            client: &client,
            apply: runtime.apply_send_port(),
            clock: &clock,
            observer: ConsumerId::new(1).unwrap(),
            executor: &executor,
        };
        let expiry = Instant::now() + Duration::from_secs(5);
        // The independent WC driver only publishes WC progress. The send is
        // polled by its actual WC, ledger and Join wakeups.
        let result = tokio::time::timeout_at(
            expiry + Duration::from_secs(1),
            send_replacement_admission(
                context,
                parent,
                ReplacementQualificationScope::GENERATION,
                request,
                key(request, generation),
                &mut cancellation,
                expiry,
            ),
        )
        .await
        .expect("original Acquire wakes on its actual exit")
        .unwrap();
        assert_eq!(result.stopped, None);
        let receipt = result.original.receipt.expect("exact original reply");
        assert_eq!(receipt.worker_outcome, verdict);
        assert_eq!(receipt.ticket, verdict.map(|_| ticket));
        assert!(receipt.governance.is_none());
        if verdict.is_some() {
            assert!(matches!(
                receipt.dispatch,
                OperationDispatchResult::WorkerReceipt(_)
            ));
        } else {
            assert_eq!(receipt.dispatch, OperationDispatchResult::TransportUnknown);
        }
        assert_eq!(
            attempts
                .ledger()
                .try_capacity_dimensions([TOTAL_ITEMS])
                .unwrap()
                .unwrap()
                .used,
            [0]
        );
    }
    let applied = peer.applied_requests();
    assert_eq!(applied.len(), 2);
    assert_eq!(applied[0], applied[1]);
    drop((client, authority));
    finish_progress.send(true).unwrap();
    assert!(progress.await.unwrap().finish());
    let mut shutdown =
        std::pin::pin!(control.shutdown_until(std::time::Instant::now() + Duration::from_secs(5)));
    let wakes = Arc::new(CountWake::default());
    let waker = Waker::from(wakes.clone());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(matches!(
        attempts.open_replacement_qualification(),
        Err(ReplacementQualificationScopeError::Closed)
    ));
    assert!(!attempts.ledger().is_empty());
    let before = wakes.count();
    attempts
        .finish_replacement_qualification(qualification)
        .unwrap();
    assert!(
        wakes.count() > before,
        "real Scope removal wakes the waiting shutdown"
    );
    shutdown.await.unwrap();
    assert!(attempts.ledger().is_empty());
    runtime
        .drain_original_apply_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(runtime.original_apply_joined());
    assert!(peer.settle().await);
}
