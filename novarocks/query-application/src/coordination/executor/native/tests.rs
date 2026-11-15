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
use crate::coordination::replacement::{
    ActiveAttemptIsolationOwner, AttemptIsolationActivationFailure, AttemptIsolationReservation,
    QualifiedWorkerAdmission,
};
use novarocks_coordination_contract::{IdSeed, LogicalExecutionActorId, OperationIds};
use novarocks_execution_contract::{ParticipationRef, failure::FailureClass};
use novarocks_types::{AttemptId, BackendProcessId, QueryId, StageId, TaskId};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

fn execution(attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(717, 718), AttemptId::new(attempt).unwrap()).unwrap()
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(19, execution(1), 23)
}
fn root() -> TaskIdentity {
    let process =
        BackendProcessId::try_from_bytes(OperationIds::new(IdSeed::new(72, 10)).issue().to_bytes())
            .unwrap();
    TaskIdentity::new(
        execution(1),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        process,
    )
}
fn identity() -> QualificationIdentity {
    QualificationIdentity {
        actor: LogicalExecutionActorId::from_raw(19),
        failed: execution(1),
        replacement: execution(2),
        generation: 24,
    }
}

struct TestFuture {
    value: Option<u64>,
    poll_panic: bool,
    drop_panic: bool,
    received: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}
impl Future for TestFuture {
    type Output = u64;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u64> {
        assert!(!self.poll_panic, "original poll cause");
        match self.value.take() {
            Some(value) => Poll::Ready(value),
            None => Poll::Pending,
        }
    }
}
impl Drop for TestFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.drop_panic {
            if !self.poll_panic {
                assert!(
                    self.received.load(Ordering::SeqCst),
                    "Ready value reaches stable destination before F Drop"
                );
            }
            panic!("original future destruction cause");
        }
    }
}
fn test_future(
    value: Option<u64>,
    poll_panic: bool,
    drop_panic: bool,
) -> (
    LogicalFuture<'static, u64>,
    Arc<AtomicBool>,
    Arc<AtomicUsize>,
) {
    let received = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(TestFuture {
            value,
            poll_panic,
            drop_panic,
            received: Arc::clone(&received),
            drops: Arc::clone(&drops),
        }),
        received,
        drops,
    )
}
#[tokio::test]
async fn original_ready_output_is_stable_before_future_drop_panics() {
    let (future, received, drops) = test_future(Some(31), false, true);
    let causes = FutureCauses::default();
    let mut stable = None;
    assert_eq!(
        poll_original(future, &causes, None, None, |value| {
            stable = Some(value);
            received.store(true, Ordering::SeqCst);
        })
        .await,
        Ok(())
    );
    assert_eq!(stable, Some(31));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(causes.primary.lock().unwrap().is_none());
    assert!(causes.cleanup.lock().unwrap().is_some());
}
#[tokio::test]
async fn poll_and_future_drop_panics_retain_both_original_causes() {
    let (future, _, drops) = test_future(None, true, true);
    let causes = FutureCauses::default();
    assert_eq!(
        poll_original(future, &causes, None, None, |_| panic!(
            "no invented result"
        ))
        .await,
        Err(WaitFailure::Panicked)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let primary = causes.primary.lock().unwrap();
    let cleanup = causes.cleanup.lock().unwrap();
    assert_eq!(
        primary.as_ref().unwrap().downcast_ref::<&str>(),
        Some(&"original poll cause")
    );
    assert_eq!(
        cleanup.as_ref().unwrap().downcast_ref::<&str>(),
        Some(&"original future destruction cause")
    );
}
#[tokio::test]
async fn expired_preparation_never_polls_and_actually_drops_original_future() {
    let (future, _, drops) = test_future(None, true, false);
    let causes = FutureCauses::default();
    assert_eq!(
        poll_original(
            future,
            &causes,
            None,
            Some(tokio::time::Instant::now()),
            |_| panic!("no result")
        )
        .await,
        Err(WaitFailure::Deadline)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!causes.failed());
}
#[tokio::test]
async fn secondary_drop_panic_does_not_replace_returned_domain_error() {
    struct DomainFuture {
        received: Arc<AtomicBool>,
    }
    impl Future for DomainFuture {
        type Output = Result<(), QueryExecutionError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(protocol("original domain refusal")))
        }
    }
    impl Drop for DomainFuture {
        fn drop(&mut self) {
            assert!(self.received.load(Ordering::SeqCst));
            panic!("secondary domain F drop panic");
        }
    }
    let received = Arc::new(AtomicBool::new(false));
    let causes = FutureCauses::default();
    let mut original = None;
    let future = Box::pin(DomainFuture {
        received: Arc::clone(&received),
    });
    assert_eq!(
        poll_original(future, &causes, None, None, |result| {
            original = Some(result);
            received.store(true, Ordering::SeqCst);
        })
        .await,
        Ok(())
    );
    assert_eq!(
        original.unwrap().unwrap_err().message(),
        "original domain refusal"
    );
    assert!(causes.failed());
}

#[derive(Debug)]
struct FixedClock(MonotonicInstant);
impl MonotonicClock for FixedClock {
    fn now(&self) -> MonotonicInstant {
        self.0
    }
}
#[test]
fn queued_work_uses_only_original_remaining_deadline() {
    let clock = FixedClock(MonotonicInstant::from_origin(Duration::from_secs(99)));
    let before = Instant::now();
    let absolute = absolute_deadline(
        &clock,
        MonotonicInstant::from_origin(Duration::from_secs(100)),
    );
    let after = Instant::now();
    assert!(absolute >= before + Duration::from_secs(1));
    assert!(absolute <= after + Duration::from_secs(1));
}
#[test]
fn already_expired_work_receives_no_fresh_window() {
    let clock = FixedClock(MonotonicInstant::from_origin(Duration::from_secs(101)));
    let absolute = absolute_deadline(
        &clock,
        MonotonicInstant::from_origin(Duration::from_secs(100)),
    );
    assert!(absolute <= Instant::now());
}
fn request() -> ReplacementQualificationRequest {
    let identity = identity();
    ReplacementQualificationRequest::from_logical(
        OperationIds::new(IdSeed::new(91, 100)).issue(),
        ReplacementQualificationIdentity::new(
            identity.actor,
            identity.failed,
            identity.replacement,
            identity.generation,
        ),
        Arc::from([]),
        Arc::from([]),
        MonotonicInstant::from_origin(Duration::from_secs(20)),
        MonotonicInstant::from_origin(Duration::from_secs(25)),
        Instant::now() + Duration::from_secs(2),
        None,
    )
}
#[test]
fn qualification_request_preserves_core_operation_and_absolute_expiry() {
    let operation = OperationIds::new(IdSeed::new(91, 100)).issue();
    let request = request();
    assert_eq!(request.operation_id(), operation);
    assert_eq!(
        request.issued_at(),
        MonotonicInstant::from_origin(Duration::from_secs(20))
    );
    assert_eq!(
        request.conservative_expiry(),
        MonotonicInstant::from_origin(Duration::from_secs(25))
    );
    assert!(request.absolute_expiry() <= Instant::now() + Duration::from_secs(2));
}
#[derive(Debug)]
struct Reservation {
    identity: ReplacementQualificationIdentity,
    reject: bool,
    finishes: Arc<AtomicUsize>,
    abandons: Arc<AtomicUsize>,
}
impl AttemptIsolationReservation for Reservation {
    fn identity(&self) -> ReplacementQualificationIdentity {
        self.identity
    }
    fn topology_revision(&self) -> u64 {
        7
    }
    fn admissions(&self) -> &[QualifiedWorkerAdmission] {
        &[]
    }
    fn validate_binding(
        &self,
        _: &[ParticipationRef],
    ) -> Result<(), ReplacementQualificationFailure> {
        Ok(())
    }
    fn activate(
        self: Box<Self>,
    ) -> Result<Box<dyn ActiveAttemptIsolationOwner>, AttemptIsolationActivationFailure> {
        if self.reject {
            return Err(AttemptIsolationActivationFailure::new(
                ReplacementQualificationFailure::Rejected,
                self,
            ));
        }
        Ok(Box::new(Isolation {
            execution: self.identity.replacement(),
            finishes: Arc::clone(&self.finishes),
            abandons: Arc::clone(&self.abandons),
        }))
    }
    fn abandon(self: Box<Self>) {
        self.abandons.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct Isolation {
    execution: QueryExecutionId,
    finishes: Arc<AtomicUsize>,
    abandons: Arc<AtomicUsize>,
}
impl ActiveAttemptIsolationOwner for Isolation {
    fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    fn finish(self: Box<Self>) {
        self.finishes.fetch_add(1, Ordering::SeqCst);
    }
    fn abandon(self: Box<Self>) {
        self.abandons.fetch_add(1, Ordering::SeqCst);
    }
}
fn reservation(
    reject: bool,
) -> (
    QualifiedReplacementReservation,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let request = request();
    let finishes = Arc::new(AtomicUsize::new(0));
    let abandons = Arc::new(AtomicUsize::new(0));
    let qualified = QualifiedReplacementReservation::try_new(
        &request,
        Box::new(Reservation {
            identity: request.identity(),
            reject,
            finishes: Arc::clone(&finishes),
            abandons: Arc::clone(&abandons),
        }),
    )
    .unwrap();
    (qualified, finishes, abandons)
}
#[test]
fn rejected_isolation_activation_preserves_original_until_explicit_abandon() {
    let (mut original, finishes, abandons) = reservation(true);
    assert_eq!(
        original.activate_retained().err().unwrap(),
        ReplacementQualificationFailure::Rejected
    );
    assert_eq!(abandons.load(Ordering::SeqCst), 0);
    assert_eq!(original.topology_revision(), 7);
    drop(original);
    assert_eq!(abandons.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
}
#[test]
fn active_isolation_finish_and_abandon_have_distinct_original_effects() {
    for finish in [true, false] {
        let (mut original, finishes, abandons) = reservation(false);
        let active = ActiveReplacementResources::replacement(original.activate_retained().unwrap());
        drop(original);
        if finish {
            active.finish();
        } else {
            drop(active);
        }
        assert_eq!(finishes.load(Ordering::SeqCst), usize::from(finish));
        assert_eq!(abandons.load(Ordering::SeqCst), usize::from(!finish));
    }
}
fn ack_failure() -> crate::coordination::RootResultFetchFailure {
    crate::coordination::RootResultFetchFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating("original final ACK refusal"),
        ExecutionFailureSite::Coordinator,
    ))
}
struct DropWitness(Arc<AtomicUsize>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn tail(future: RootAckFuture) -> Arc<OriginalTailSlot> {
    Arc::new(OriginalTailSlot {
        activation: activation(),
        root: root(),
        kind: OriginalTailKind::RootAck,
        state: Mutex::new(OriginalTailState {
            phase: TailPhase::Claimed,
            input: Some(OriginalTailInput::RootAck(future)),
            ack_result: None,
            ack_failure: None,
            consumer_result: None,
            activity: None,
            cleanup_returned: false,
            join_failure: None,
        }),
        causes: FutureCauses::default(),
        original_exit: Weak::new(),
    })
}
#[test]
fn never_polled_tail_keeps_original_future_in_same_slot() {
    let drops = Arc::new(AtomicUsize::new(0));
    let witness = DropWitness(Arc::clone(&drops));
    let slot = tail(Box::pin(async move {
        let _original = witness;
        Err(ack_failure())
    }));
    let obligation = OriginalTailWorkObligation {
        slot: Arc::clone(&slot),
    };
    let future = obligation.into_future().0;
    drop(future);
    assert!(slot.lock().input.is_some());
    assert_eq!(slot.lock().phase, TailPhase::Claimed);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(slot);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn tail_result_return_is_not_actual_join() {
    let slot = tail(Box::pin(async { Err(ack_failure()) }));
    OriginalTailWorkObligation {
        slot: Arc::clone(&slot),
    }
    .into_future()
    .0
    .await;
    assert_eq!(slot.lock().phase, TailPhase::Returned);
    assert!(slot.lock().ack_result.as_ref().unwrap().is_err());
    receive_tail_join(&slot, Ok(())).unwrap();
    assert_eq!(slot.lock().phase, TailPhase::Joined);
    assert!(slot.lock().ack_result.is_none());
    assert!(slot.lock().ack_failure.is_some());
    assert!(
        !slot.failed(),
        "ordinary optional final ACK failure remains diagnostic"
    );
}
#[tokio::test]
async fn duplicate_actual_tail_join_is_rejected() {
    let slot = tail(Box::pin(async { Err(ack_failure()) }));
    OriginalTailWorkObligation {
        slot: Arc::clone(&slot),
    }
    .into_future()
    .0
    .await;
    receive_tail_join(&slot, Ok(())).unwrap();
    assert!(receive_tail_join(&slot, Ok(())).is_err());
}
#[test]
fn successful_join_cannot_invent_never_polled_future_exit() {
    let slot = tail(Box::pin(async { Err(ack_failure()) }));
    assert!(receive_tail_join(&slot, Ok(())).is_err());
    assert!(slot.lock().input.is_some());
}
#[test]
fn failed_join_keeps_unpolled_original_input_and_health() {
    let slot = tail(Box::pin(async { Err(ack_failure()) }));
    receive_tail_join(
        &slot,
        Err(Arc::new(std::io::Error::other(
            "original task panicked before first poll",
        ))),
    )
    .unwrap();
    assert_eq!(slot.lock().phase, TailPhase::Joined);
    assert!(slot.lock().input.is_some());
    assert!(!slot.lock().cleanup_returned);
    assert!(slot.failed());
}
#[tokio::test]
async fn one_tail_join_failure_keeps_pending_peer_and_primary_cause() {
    let failed = tail(Box::pin(async { Err(ack_failure()) }));
    let peer = tail(Box::pin(std::future::pending()));
    OriginalTailWorkObligation {
        slot: Arc::clone(&failed),
    }
    .into_future()
    .0
    .await;
    receive_tail_join(
        &failed,
        Err(Arc::new(std::io::Error::other("original task panic"))),
    )
    .unwrap();
    assert!(failed.failed());
    assert!(failed.lock().ack_failure.is_some());
    assert_eq!(peer.lock().phase, TailPhase::Claimed);
    assert!(peer.lock().input.is_some());
}
#[test]
fn preparation_and_binding_deadlines_keep_exact_reason_and_detail() {
    for detail in [
        "Native replacement preparation exceeded its recovery budget",
        "Native replacement dependency binding exceeded its recovery budget",
    ] {
        let error = wait_error(
            WaitFailure::Deadline,
            "unused cancel",
            detail,
            "unused panic",
        );
        let input = error.execution_failure().unwrap();
        assert_eq!(
            input.fact(),
            QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact()
        );
        assert_eq!(input.fact().class(), FailureClass::Dependency);
        assert_eq!(error.message(), detail);
    }
}
#[test]
fn native_and_consumer_panic_projections_keep_distinct_original_reasons() {
    assert_eq!(
        panic_error("native").execution_failure().unwrap().fact(),
        QueryFailureReason::NativeFuturePanicked.failure_fact()
    );
    assert_eq!(
        consumer_panic().execution_failure().unwrap().fact(),
        QueryFailureReason::InternalConsumerPanicked.failure_fact()
    );
    assert_eq!(
        panic_error("native")
            .execution_failure()
            .unwrap()
            .fact()
            .class(),
        FailureClass::Unexpected
    );
}

fn cancellation_work() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::RootWork,
) {
    use novarocks_workload_control::*;
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    (control, work)
}
fn schedule() -> AttemptSchedule {
    schedule_for(execution(1))
}
fn schedule_for(execution: QueryExecutionId) -> AttemptSchedule {
    let facts = crate::api::ExecutionSchedulingFacts {
        topological_fragment_order: vec![1],
        execution_anchor_fragment_id: 1,
        fragments: vec![crate::api::FragmentSchedulingFacts {
            fragment_id: 1,
            placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            scans: vec![],
        }],
        edges: vec![],
    };
    build_attempt_schedule(
        execution,
        FrontendProcessId::new_v7(),
        &[root().backend_process_id()],
        &facts,
        #[cfg(novarocks_dev_controls)]
        Ok(None),
    )
    .unwrap()
}
#[derive(Debug)]
struct DormantFixture {
    factory_panic: bool,
    future_drop_panic: bool,
    active_drops: Arc<AtomicUsize>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
#[derive(Debug)]
struct ActiveFixture(Arc<AtomicUsize>);
impl Drop for ActiveFixture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl crate::api::ActiveNativeAttemptOwner for ActiveFixture {
    fn run<'a>(
        &'a mut self,
        _: &'a crate::coordination::NativeAttemptDrive,
        _: CancellationView,
    ) -> crate::api::NativeAttemptRunFuture<'a> {
        Box::pin(async { NativeAttemptTerminal::Completed })
    }
    fn converge<'a>(
        &'a mut self,
        _: crate::api::NativeActiveConvergenceRequest,
    ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
        Box::pin(async {
            Ok(crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::all_workers_stopped_and_participations_fenced(
                ),
            ))
        })
    }
}
struct ActivationFuture {
    active: Option<crate::api::ActivatedNativeAttempt>,
    drop_panic: bool,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl Future for ActivationFuture {
    type Output =
        Result<crate::api::ActivatedNativeAttempt, crate::api::NativeAttemptActivationFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(gate) = self.gate.as_mut() {
            if Pin::new(gate).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.gate.take();
        }
        Poll::Ready(Ok(self.active.take().unwrap()))
    }
}
impl Drop for ActivationFuture {
    fn drop(&mut self) {
        assert!(!self.drop_panic, "activation future destructor panic");
    }
}
impl crate::api::DormantNativeAttemptOwner for DormantFixture {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        &[]
    }
    fn poll_bind_qualification(
        &mut self,
        _: &AttemptSchedule,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
        Poll::Ready(Ok(()))
    }
    fn activate<'a>(
        &'a mut self,
        _: &'a AttemptSchedule,
        _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
        _: CancellationView,
    ) -> crate::api::NativeAttemptActivationFuture<'a> {
        assert!(!self.factory_panic, "dormant activation factory panic");
        Box::pin(ActivationFuture {
            active: Some(crate::api::ActivatedNativeAttempt::completion(
                ActiveFixture(Arc::clone(&self.active_drops)),
            )),
            drop_panic: self.future_drop_panic,
            gate: self.gate.take(),
        })
    }
    fn converge<'a>(
        &'a mut self,
        _: CancellationView,
    ) -> crate::api::NativeAttemptConvergenceFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn dormant_factory_panic_stays_in_original_custody_before_passive_fact() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        factory_panic: true,
        future_drop_panic: false,
        active_drops: Arc::clone(&drops),
        gate: None,
    });
    let (owner, original) = custody.activation_parts().unwrap();
    let error = match start_activation(
        owner,
        &schedule,
        None,
        work.owner.scope().cancellation().unwrap(),
        original,
    ) {
        Ok(_) => panic!("factory must fail"),
        Err(error) => error,
    };
    assert!(custody.activation_panic().is_some());
    assert!(custody.activation_parts().is_none());
    assert!(
        custody.convergence_parts().is_some(),
        "original cleanup remains available once"
    );
    assert_eq!(
        error.execution_failure().unwrap().fact(),
        QueryFailureReason::NativeFuturePanicked.failure_fact()
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn successful_native_output_survives_original_activation_future_drop_panic() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        factory_panic: false,
        future_drop_panic: true,
        active_drops: Arc::clone(&drops),
        gate: None,
    });
    let (owner, original) = custody.activation_parts().unwrap();
    let future = start_activation(
        owner,
        &schedule,
        None,
        work.owner.scope().cancellation().unwrap(),
        original,
    )
    .unwrap();
    let causes = FutureCauses::default();
    let mut pending = None;
    assert_eq!(
        poll_original(future, &causes, None, None, |result| {
            pending = Some(PendingNativeActivation::new(result.unwrap()));
        })
        .await,
        Ok(())
    );
    assert!(causes.failed());
    assert!(
        pending.is_some(),
        "same original successful activation is retained before F Drop"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "original active owner has not been lost during unwind"
    );
    drop(pending);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn original_cancellation_is_observed_before_polling_preparation() {
    let (_control, work) = cancellation_work();
    let cancellation = work.owner.scope().cancellation().unwrap();
    work.owner
        .cancellation_requester()
        .request(CancellationReason::DeadlineExceeded)
        .unwrap();
    let (future, _, drops) = test_future(None, true, false);
    let causes = FutureCauses::default();
    assert_eq!(
        poll_original(future, &causes, Some(&cancellation), None, |_| panic!(
            "no invented result"
        ))
        .await,
        Err(WaitFailure::Cancelled(CancellationReason::DeadlineExceeded))
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!causes.failed());
}

#[tokio::test]
async fn reported_cleanup_failure_keeps_typed_cause_before_secondary_future_drop_panic() {
    #[derive(Debug)]
    struct Cause(Arc<AtomicUsize>);
    impl std::fmt::Display for Cause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original typed cleanup cause")
        }
    }
    impl Error for Cause {}
    impl Drop for Cause {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ReportedFuture(Option<crate::api::StatementNativeHealthFailure>);
    impl Future for ReportedFuture {
        type Output = Result<(), crate::api::StatementNativeHealthFailure>;
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(self.0.take().unwrap()))
        }
    }
    impl Drop for ReportedFuture {
        fn drop(&mut self) {
            panic!("secondary cleanup future Drop panic");
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let failure = crate::api::StatementNativeHealthFailure::actual_exit_unproven(
        "original cleanup refusal",
        Arc::new(Cause(Arc::clone(&drops))),
    );
    let reported = Arc::new(CleanupFailure::default());
    let causes = FutureCauses::default();
    assert_eq!(
        poll_original(
            Box::pin(ReportedFuture(Some(failure))),
            &causes,
            None,
            None,
            |result| {
                *reported.reported.lock().unwrap() = Some(result.unwrap_err());
            }
        )
        .await,
        Ok(())
    );
    assert_eq!(
        reported.reported.lock().unwrap().as_ref().unwrap().detail(),
        "original cleanup refusal"
    );
    assert!(causes.failed());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let original_health: Arc<dyn Error + Send + Sync> =
        Arc::clone(&reported) as Arc<dyn Error + Send + Sync>;
    drop(reported);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(original_health);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

fn pending_storage() -> NativeStorage {
    let mut storage = NativeStorage::default();
    storage.prepared.insert(
        execution(1),
        PreparedAttempt {
            schedule: Arc::new(schedule()),
            preparation_deadline: None,
            terminal_cut: None,
            binding_complete: true,
        },
    );
    storage
}
#[tokio::test]
async fn cancellation_cut_survives_pending_activation_and_is_inherited_on_late_return() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let view = work.owner.scope().cancellation().unwrap();
    let (release, gate) = tokio::sync::oneshot::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        factory_panic: false,
        future_drop_panic: false,
        active_drops: Arc::clone(&drops),
        gate: Some(gate),
    });
    let (owner, original) = custody.activation_parts().unwrap();
    let future = start_activation(owner, &schedule, None, view.clone(), original).unwrap();
    let causes = FutureCauses::default();
    let mut pending = None;
    let mut activating = Box::pin(poll_original(future, &causes, None, None, |value| {
        pending = Some(PendingNativeActivation::new(value.unwrap()));
    }));
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(activating.as_mut().poll(&mut context).is_pending());
    work.owner
        .cancellation_requester()
        .request(CancellationReason::DeadlineExceeded)
        .unwrap();
    let mut storage = pending_storage();
    let cut = MonotonicInstant::from_origin(Duration::from_secs(17));
    let deadline = cut.saturating_add(Duration::from_secs(3));
    storage
        .record_terminal_cut(activation(), activation(), cut, deadline)
        .unwrap();
    assert!(
        storage.active.is_empty(),
        "recording the original cut never invents an active owner"
    );
    release.send(()).unwrap();
    assert_eq!(activating.await, Ok(()));
    assert!(pending.is_some());
    assert!(view.reason().is_some());
    // This is the SAME read used while publishing the late active owner. It
    // never reads current time or opens a fresh observation window.
    assert_eq!(
        storage.inherited_terminal_cut(activation()),
        Some((cut, deadline))
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(pending);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn pending_terminal_cut_requires_original_activation_and_never_renews() {
    let mut storage = pending_storage();
    let original = activation();
    let wrong = AttemptActivationIdentity::from_parts(
        original.actor().get(),
        original.execution(),
        original.generation() + 1,
    );
    let cut = MonotonicInstant::from_origin(Duration::from_secs(11));
    let deadline = cut.saturating_add(Duration::from_secs(2));
    assert!(
        storage
            .record_terminal_cut(original, wrong, cut, deadline)
            .is_err()
    );
    assert!(storage.inherited_terminal_cut(original).is_none());
    storage
        .record_terminal_cut(original, original, cut, deadline)
        .unwrap();
    storage
        .record_terminal_cut(
            original,
            original,
            cut.saturating_add(Duration::from_secs(10)),
            deadline.saturating_add(Duration::from_secs(10)),
        )
        .unwrap();
    assert_eq!(
        storage.inherited_terminal_cut(original),
        Some((cut, deadline))
    );
    let shorter = deadline.saturating_add(Duration::from_secs(1));
    // A second later deadline also cannot extend the first accepted bound.
    storage
        .record_terminal_cut(original, original, cut, shorter)
        .unwrap();
    assert_eq!(
        storage.inherited_terminal_cut(original),
        Some((cut, deadline))
    );
}
#[test]
fn successor_pending_cut_matches_original_qualification_generation() {
    let mut storage = pending_storage();
    let identity = identity();
    let initial = activation();
    storage.prepared.insert(
        identity.replacement,
        PreparedAttempt {
            schedule: Arc::new(schedule_for(identity.replacement)),
            preparation_deadline: Some(MonotonicInstant::from_origin(Duration::from_secs(99))),
            terminal_cut: None,
            binding_complete: true,
        },
    );
    let (cancellation, _) = watch::channel(false);
    storage.qualifications.insert(
        identity.replacement,
        QualificationOriginal {
            identity,
            operation: Some(OperationIds::new(IdSeed::new(71, 100)).issue()),
            dispatch_started: false,
            cancellation,
            qualified: None,
            active: None,
            activation: None,
            abandoned: false,
        },
    );
    let successor = AttemptActivationIdentity::from_parts(
        identity.actor.get(),
        identity.replacement,
        identity.generation,
    );
    let stale = AttemptActivationIdentity::from_parts(
        identity.actor.get(),
        identity.replacement,
        identity.generation - 1,
    );
    assert!(
        storage
            .record_terminal_cut(
                initial,
                stale,
                MonotonicInstant::ORIGIN,
                MonotonicInstant::ORIGIN
            )
            .is_err()
    );
    storage
        .record_terminal_cut(
            initial,
            successor,
            MonotonicInstant::ORIGIN,
            MonotonicInstant::from_origin(Duration::from_secs(1)),
        )
        .unwrap();
    assert_eq!(
        storage.inherited_terminal_cut(successor),
        Some((
            MonotonicInstant::ORIGIN,
            MonotonicInstant::from_origin(Duration::from_secs(1))
        ))
    );
    assert!(storage.inherited_terminal_cut(stale).is_none());
}

fn backend_failure() -> novarocks_coordination_contract::ExecutionFailure {
    novarocks_coordination_contract::ExecutionFailure::from_input(ExecutionFailureInput::new(
        QueryFailureReason::BackendLost.failure_fact(),
        SafeDetail::truncating("original backend process lost"),
        ExecutionFailureSite::Backend {
            process: root().backend_process_id(),
        },
    ))
}
#[test]
fn ordinary_backend_terminal_failure_does_not_become_native_run_veto() {
    let failure = backend_failure();
    let original = failure.input().clone();
    match project_run_terminal(
        activation(),
        Some(NativeAttemptTerminal::Failed(failure)),
        false,
    ) {
        NativeFact::Failed { execution, failure } => {
            assert_eq!(execution, activation().execution());
            assert_eq!(failure.original_attempt_failure.as_deref(), Some(&original));
            assert_eq!(failure.error.execution_failure(), Some(&original));
        }
        _ => panic!("ordinary AP terminal must not be projected as NativeRunFailed"),
    }
}
#[tokio::test]
async fn actual_native_run_poll_panic_is_an_independent_veto() {
    let future = Box::pin(std::future::poll_fn(|_| -> Poll<NativeAttemptTerminal> {
        panic!("original native run poll panic")
    }));
    let causes = FutureCauses::default();
    let mut terminal = None;
    let result = poll_original(future, &causes, None, None, |value| terminal = Some(value)).await;
    assert_eq!(result, Err(WaitFailure::Panicked));
    match project_run_terminal(activation(), terminal, result.is_err() || causes.failed()) {
        NativeFact::NativeRunFailed {
            activation: actual,
            failure,
        } => {
            assert_eq!(actual, activation());
            assert_eq!(
                failure.error.execution_failure().unwrap().fact(),
                QueryFailureReason::NativeFuturePanicked.failure_fact()
            );
        }
        _ => panic!("a true original Native panic must veto recovery"),
    }
}
#[test]
fn secondary_native_run_drop_panic_veto_keeps_returned_attempt_failure() {
    let failure = backend_failure();
    let original = failure.input().clone();
    match project_run_terminal(
        activation(),
        Some(NativeAttemptTerminal::Failed(failure)),
        true,
    ) {
        NativeFact::NativeRunFailed { failure, .. } => {
            assert_eq!(failure.original_attempt_failure.as_deref(), Some(&original))
        }
        _ => panic!(
            "independent destructor failure is a veto without replacing the returned original cause"
        ),
    }
}

#[derive(Debug)]
struct BindingFixture {
    released: Arc<AtomicBool>,
    polls: Arc<AtomicUsize>,
    late_settlement: Arc<AtomicUsize>,
    observed: Arc<AtomicUsize>,
}
impl crate::api::DormantNativeAttemptOwner for BindingFixture {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        &[]
    }
    fn poll_bind_qualification(
        &mut self,
        _: &AttemptSchedule,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if !self.released.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        self.observed.store(
            self.late_settlement.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        Poll::Ready(Ok(()))
    }
    fn activate<'a>(
        &'a mut self,
        _: &'a AttemptSchedule,
        _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
        _: CancellationView,
    ) -> crate::api::NativeAttemptActivationFuture<'a> {
        panic!("binding must finish before any replacement activation")
    }
    fn converge<'a>(
        &'a mut self,
        _: CancellationView,
    ) -> crate::api::NativeAttemptConvergenceFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn delayed_dependency_binding_observes_late_settlement_before_prepared_boundary() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let view = work.owner.scope().cancellation().unwrap();
    let released = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let late = Arc::new(AtomicUsize::new(1));
    let observed = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(BindingFixture {
        released: Arc::clone(&released),
        polls: Arc::clone(&polls),
        late_settlement: Arc::clone(&late),
        observed: Arc::clone(&observed),
    });
    let causes = FutureCauses::default();
    let mut binding = Box::pin(bind_original_schedule(
        &mut custody,
        &schedule,
        &view,
        tokio::time::Instant::now() + Duration::from_secs(60),
        &causes,
    ));
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(binding.as_mut().poll(&mut context).is_pending());
    assert_eq!(observed.load(Ordering::SeqCst), 0);
    // A real binding wait can overlap the old attempt's late settlement. Its
    // completion must precede SuccessorPrepared and the core's witness recheck.
    late.store(2, Ordering::SeqCst);
    released.store(true, Ordering::SeqCst);
    assert_eq!(binding.await, Ok(()));
    assert_eq!(observed.load(Ordering::SeqCst), 2);
    assert!(polls.load(Ordering::SeqCst) >= 2);
    assert!(!causes.failed());
}
#[tokio::test]
async fn dependency_binding_expiry_uses_original_preparation_budget_and_never_polls_after_it() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let view = work.owner.scope().cancellation().unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(BindingFixture {
        released: Arc::new(AtomicBool::new(false)),
        polls: Arc::clone(&polls),
        late_settlement: Arc::new(AtomicUsize::new(1)),
        observed: Arc::new(AtomicUsize::new(0)),
    });
    let causes = FutureCauses::default();
    let error = bind_original_schedule(
        &mut custody,
        &schedule,
        &view,
        tokio::time::Instant::now(),
        &causes,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.message(),
        "Native replacement dependency binding exceeded its recovery budget"
    );
    assert_eq!(
        error.execution_failure().unwrap().fact(),
        QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(!causes.failed());
}

fn prepared_qualification_storage() -> NativeStorage {
    let mut storage = pending_storage();
    storage.prepared.insert(
        execution(2),
        PreparedAttempt {
            schedule: Arc::new(schedule_for(execution(2))),
            preparation_deadline: Some(MonotonicInstant::from_origin(Duration::from_secs(50))),
            binding_complete: true,
            terminal_cut: None,
        },
    );
    storage
}
#[test]
fn qualification_cancel_before_first_dispatch_poll_prevents_any_dispatch() {
    let mut storage = prepared_qualification_storage();
    let identity = identity();
    let operation = OperationIds::new(IdSeed::new(81, 50)).issue();
    let retired = storage
        .close_qualification(activation(), identity, Some(operation), false)
        .unwrap();
    assert!(retired.0.is_none() && retired.1.is_none());
    assert!(
        storage
            .begin_qualification_dispatch(activation(), identity, operation)
            .is_err()
    );
    let original = storage.qualifications.get(&identity.replacement).unwrap();
    assert!(!original.dispatch_started);
    assert!(*original.cancellation.borrow());
    assert_eq!(original.operation, Some(operation));
}
#[test]
fn qualification_abandon_before_dispatch_keeps_closed_tombstone_without_minting_operation() {
    let mut storage = prepared_qualification_storage();
    let identity = identity();
    let retired = storage
        .close_qualification(activation(), identity, None, true)
        .unwrap();
    assert!(retired.0.is_none() && retired.1.is_none());
    assert!(
        storage
            .qualifications
            .get(&identity.replacement)
            .unwrap()
            .operation
            .is_none()
    );
    let operation = OperationIds::new(IdSeed::new(82, 50)).issue();
    assert!(
        storage
            .begin_qualification_dispatch(activation(), identity, operation)
            .is_err()
    );
    let original = storage.qualifications.get(&identity.replacement).unwrap();
    assert!(original.abandoned);
    assert!(!original.dispatch_started);
    assert_eq!(original.operation, Some(operation));
    assert!(
        *original.cancellation.borrow(),
        "late Dispatch never reopens the original closed state"
    );
}
#[test]
fn early_qualification_tombstone_rejects_different_operation_and_unknown_prepared_identity() {
    let mut storage = prepared_qualification_storage();
    let identity = identity();
    let mut ids = OperationIds::new(IdSeed::new(83, 50));
    let operation = ids.issue();
    let foreign = ids.issue();
    storage
        .close_qualification(activation(), identity, Some(operation), false)
        .unwrap();
    let error = storage
        .begin_qualification_dispatch(activation(), identity, foreign)
        .err()
        .unwrap();
    assert_eq!(
        error.execution_failure().unwrap().fact(),
        ReplacementQualificationFailure::MismatchedReceipt.failure_fact()
    );
    assert_eq!(
        storage
            .qualifications
            .get(&identity.replacement)
            .unwrap()
            .operation,
        Some(operation)
    );
    let mut empty = NativeStorage::default();
    assert!(
        empty
            .close_qualification(activation(), identity, None, true)
            .is_err()
    );
    assert!(
        empty.qualifications.is_empty(),
        "cleanup cannot invent an unrelated prepared slot"
    );
}
#[test]
fn qualification_dispatch_starts_once_and_late_cancellation_closes_its_same_cell() {
    let mut storage = prepared_qualification_storage();
    let identity = identity();
    let operation = OperationIds::new(IdSeed::new(84, 50)).issue();
    let receiver = storage
        .begin_qualification_dispatch(activation(), identity, operation)
        .unwrap();
    assert!(!*receiver.borrow());
    assert!(
        storage
            .begin_qualification_dispatch(activation(), identity, operation)
            .is_err()
    );
    storage
        .close_qualification(activation(), identity, Some(operation), false)
        .unwrap();
    assert!(*receiver.borrow());
    assert!(
        storage
            .qualifications
            .get(&identity.replacement)
            .unwrap()
            .dispatch_started
    );
}
