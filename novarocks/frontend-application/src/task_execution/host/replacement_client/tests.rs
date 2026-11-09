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
    native::transport_capacity::TOTAL_ITEMS,
    task_execution::host::transport::GovernedQueuedInputFixture,
};
use novarocks_attempt_protocol::input::SendGeneration;
use novarocks_control_runtime::{
    completion::ReportSlot, exit_custody::ExitCustody, transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::ConsumerId,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{
    AdmissionEpochCapability, LeaseValidFor, ParticipationRef, TaskOperationId,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId,
};
use std::{
    future::Future,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

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
            &self.original,
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
