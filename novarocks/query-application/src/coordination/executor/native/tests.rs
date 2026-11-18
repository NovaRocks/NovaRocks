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
    observed_activation: Option<Arc<Mutex<Option<AttemptActivationIdentity>>>>,
    factory_panic: bool,
    future_drop_panic: bool,
    activation_calls: Arc<AtomicUsize>,
    received: Option<Arc<AtomicBool>>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
struct ActivationFuture {
    output: Option<crate::api::ActivatedNativeAttempt>,
    received: Option<Arc<AtomicBool>>,
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
        Poll::Ready(Ok(self.output.take().unwrap()))
    }
}
impl Drop for ActivationFuture {
    fn drop(&mut self) {
        if self.output.is_none()
            && let Some(received) = &self.received
        {
            assert!(
                received.load(Ordering::SeqCst),
                "original return reaches its canonical slot before Future Drop"
            );
        }
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
        activation_identity: novarocks_coordination_contract::AttemptActivationIdentity,
        _: &'a AttemptSchedule,
        _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
        _: CancellationView,
    ) -> crate::api::NativeAttemptActivationFuture<'a> {
        if let Some(observed) = &self.observed_activation {
            assert!(
                observed
                    .lock()
                    .unwrap()
                    .replace(activation_identity)
                    .is_none()
            );
        }
        self.activation_calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.factory_panic, "dormant activation factory panic");
        Box::pin(ActivationFuture {
            output: Some(crate::api::ActivatedNativeAttempt::completion(
                activation_identity,
            )),
            received: self.received.clone(),
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
async fn activation_health_reaches_original_custody_before_future_drop_without_a_task_failure() {
    struct OriginalHealth(Arc<AtomicUsize>);
    impl std::fmt::Debug for OriginalHealth {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("activation metadata must not format its opaque health cause")
        }
    }
    impl std::fmt::Display for OriginalHealth {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original source-close health")
        }
    }
    impl Error for OriginalHealth {}
    impl Drop for OriginalHealth {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct HealthFuture {
        original: Option<crate::api::StatementNativeHealthFailure>,
        received: Arc<AtomicBool>,
        drop_panic: bool,
    }
    impl Future for HealthFuture {
        type Output =
            Result<crate::api::ActivatedNativeAttempt, crate::api::NativeAttemptActivationFailure>;
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let failure = crate::api::NativeAttemptActivationFailure::local_exit_unproven(
                self.original.take().unwrap(),
            );
            assert!(failure.failure().is_none());
            let _ = format!("{failure:?}");
            Poll::Ready(Err(failure))
        }
    }
    impl Drop for HealthFuture {
        fn drop(&mut self) {
            assert!(
                self.received.load(Ordering::SeqCst),
                "same reported health was stored before original F Drop"
            );
            if self.drop_panic {
                panic!("secondary original activation Future Drop panic");
            }
        }
    }
    #[derive(Debug)]
    struct HealthDormant {
        original: Option<crate::api::StatementNativeHealthFailure>,
        received: Arc<AtomicBool>,
        factories: Arc<AtomicUsize>,
        bindings: Arc<AtomicUsize>,
        binding: bool,
        convergences: Arc<AtomicUsize>,
        drop_panic: bool,
    }
    impl crate::api::DormantNativeAttemptOwner for HealthDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            &[]
        }
        fn poll_bind_qualification(
            &mut self,
            _: &AttemptSchedule,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            assert!(self.binding, "activation fixture never binds qualification");
            self.bindings.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Err(
                crate::api::NativeAttemptActivationFailure::local_exit_unproven(
                    self.original.take().unwrap(),
                ),
            ))
        }
        fn activate<'a>(
            &'a mut self,
            identity: AttemptActivationIdentity,
            _: &'a AttemptSchedule,
            _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
            _: CancellationView,
        ) -> crate::api::NativeAttemptActivationFuture<'a> {
            assert!(
                !self.binding,
                "qualification fixture never starts activation"
            );
            assert_eq!(identity, activation());
            self.factories.fetch_add(1, Ordering::SeqCst);
            Box::pin(HealthFuture {
                original: self.original.take(),
                received: self.received.clone(),
                drop_panic: self.drop_panic,
            })
        }
        fn converge<'a>(
            &'a mut self,
            _: CancellationView,
        ) -> crate::api::NativeAttemptConvergenceFuture<'a> {
            self.convergences.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }
    for (binding, drop_panic) in [(false, false), (false, true), (true, false)] {
        let (_control, work) = cancellation_work();
        let schedule = schedule();
        let drops = Arc::new(AtomicUsize::new(0));
        let cause = Arc::new(OriginalHealth(drops.clone()));
        let original = crate::api::StatementNativeHealthFailure::actual_exit_unproven(
            "original empty-source close remains unproven",
            cause.clone(),
        );
        let received = Arc::new(AtomicBool::new(false));
        let factories = Arc::new(AtomicUsize::new(0));
        let bindings = Arc::new(AtomicUsize::new(0));
        let convergences = Arc::new(AtomicUsize::new(0));
        let mut custody = crate::api::NativeDormantCustody::test_owner(HealthDormant {
            original: Some(original),
            received: received.clone(),
            factories: factories.clone(),
            bindings: bindings.clone(),
            binding,
            convergences: convergences.clone(),
            drop_panic,
        });
        let causes = FutureCauses::default();
        let mut safe = None;
        if binding {
            safe = Some(
                bind_original_schedule(
                    &mut custody,
                    &schedule,
                    &work.owner.scope().cancellation().unwrap(),
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    &causes,
                )
                .await
                .expect_err("same original binding reports local health"),
            );
        } else {
            let (owner, panic, reported) = custody.activation_parts().unwrap();
            let future = start_activation(
                owner,
                activation(),
                &schedule,
                None,
                work.owner.scope().cancellation().unwrap(),
                panic,
            )
            .unwrap();
            assert_eq!(
                poll_original(future, &causes, None, None, |result| {
                    let failure = result.unwrap_err();
                    safe = Some(failure.into_query_error(reported));
                    received.store(true, Ordering::SeqCst);
                })
                .await,
                Ok(())
            );
        }
        let original = custody
            .convergence_failure()
            .expect("same fixed original Reported slot");
        assert!(std::ptr::eq(
            original.cause().downcast_ref::<OriginalHealth>().unwrap(),
            cause.as_ref()
        ));
        assert_eq!(
            original.detail(),
            "original empty-source close remains unproven"
        );
        assert!(custody.activation_panic().is_none());
        assert_eq!(causes.failed(), drop_panic);
        let safe = safe.unwrap();
        assert_eq!(safe.kind(), QueryExecutionErrorKind::Failed);
        assert!(safe.execution_failure().is_none());
        assert!(report(safe).original_attempt_failure.is_none());
        assert_eq!(factories.load(Ordering::SeqCst), usize::from(!binding));
        assert_eq!(bindings.load(Ordering::SeqCst), usize::from(binding));
        assert!(custody.activation_parts().is_none());
        assert!(custody.convergence_parts().is_none());
        assert!(custody.retire().is_err());
        assert_eq!(convergences.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        // These fixture objects own no actual provider/Scope work. Explicit
        // probe disposition is not an original Native resource-exit receipt.
        drop(custody);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(cause);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn exact_issued_activation_reaches_the_original_dormant_factory() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let observed = Arc::new(Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        observed_activation: Some(observed.clone()),
        factory_panic: false,
        future_drop_panic: false,
        activation_calls: calls.clone(),
        received: None,
        gate: None,
    });
    let (owner, original, _original_health) = custody.activation_parts().unwrap();
    let future = start_activation(
        owner,
        activation(),
        &schedule,
        None,
        work.owner.scope().cancellation().unwrap(),
        original,
    )
    .unwrap();
    let causes = FutureCauses::default();
    let mut output = None;
    assert_eq!(
        poll_original(future, &causes, None, None, |result| output =
            Some(result.unwrap()))
        .await,
        Ok(())
    );
    assert_eq!(*observed.lock().unwrap(), Some(activation()));
    assert_eq!(activation().generation(), 23);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!causes.failed() && custody.activation_panic().is_none());
    let output = output.expect("same original activation output");
    assert_eq!(output.activation(), activation());
    assert!(output.rows().is_none());
    drop(output);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    custody.retire().unwrap();
}
#[tokio::test]
async fn dormant_factory_panic_stays_in_original_custody_before_passive_fact() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        observed_activation: None,
        factory_panic: true,
        future_drop_panic: false,
        activation_calls: Arc::clone(&calls),
        received: None,
        gate: None,
    });
    let (owner, original, _original_health) = custody.activation_parts().unwrap();
    let error = match start_activation(
        owner,
        activation(),
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
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn successful_native_output_survives_original_activation_future_drop_panic() {
    let (_control, work) = cancellation_work();
    let schedule = schedule();
    let calls = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicBool::new(false));
    let mut registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
    let handle = registry.handle();
    let (registration, issued, scripted_exit) =
        crate::coordination::runtime_registry::runtime_tests::install_native_component_runtime(
            &handle,
            execution(1),
            schedule.participations().to_vec(),
        );
    let original_activation =
        AttemptActivationIdentity::from_parts(issued.actor().get(), execution(1), 23);
    let reservation = handle
        .reserve_active_native_owner(&registration, execution(1), schedule.participations())
        .unwrap();
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        observed_activation: None,
        factory_panic: false,
        future_drop_panic: true,
        activation_calls: Arc::clone(&calls),
        received: Some(received.clone()),
        gate: None,
    });
    let (owner, original, _original_health) = custody.activation_parts().unwrap();
    let future = start_activation(
        owner,
        original_activation,
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
            reservation.receive_activated_in(&mut pending, result.unwrap());
            received.store(true, Ordering::SeqCst);
        })
        .await,
        Ok(())
    );
    assert!(causes.failed());
    assert!(
        pending.is_some(),
        "same original successful activation is retained before F Drop"
    );
    assert!(received.load(Ordering::SeqCst));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(handle.active_native_custody_count(), 1);
    reservation
        .stage_activated_in(original_activation, schedule.root(), &mut pending)
        .unwrap();
    assert!(pending.as_ref().unwrap().staged_slot_address().is_some());
    drop(reservation);
    assert_eq!(handle.active_native_custody_count(), 1);
    // No Native/AP Source exits in this scripted registration component.
    // Keep the successful return unresolved until explicit process disposal.
    registry.abandon_for_process_exit();
    drop(pending);
    drop(scripted_exit);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
            activation_started: None,
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
    let calls = Arc::new(AtomicUsize::new(0));
    let mut custody = crate::api::NativeDormantCustody::test_owner(DormantFixture {
        observed_activation: None,
        factory_panic: false,
        future_drop_panic: false,
        activation_calls: Arc::clone(&calls),
        received: None,
        gate: Some(gate),
    });
    let (owner, original, _original_health) = custody.activation_parts().unwrap();
    let future =
        start_activation(owner, activation(), &schedule, None, view.clone(), original).unwrap();
    let causes = FutureCauses::default();
    let mut pending = None;
    let mut activating = Box::pin(poll_original(future, &causes, None, None, |value| {
        pending = Some(value.unwrap());
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
    assert_eq!(pending.as_ref().unwrap().activation(), activation());
    assert!(pending.as_ref().unwrap().rows().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(pending);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
            activation_started: None,
        },
    );
    let (cancellation, _) = watch::channel(false);
    storage.qualifications.insert(
        identity.replacement,
        QualificationOriginal {
            identity,
            operation: Some(OperationIds::new(IdSeed::new(71, 100)).issue()),
            dispatch_started: false,
            cancellation_issued: false,
            activation_in_flight: false,
            resources_retiring: false,
            dispatch_settled: false,
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
fn original_ap_failure_and_successful_native_startup_remain_independent() {
    let failure = backend_failure();
    let original = failure.input().clone();
    let port_failure = FailureReport::from_attempt(&failure);
    assert!(matches!(
        project_start_result(activation(), Some(Ok(NativeAttemptStartupOutcome::Ready)), false),
        NativeFact::RunCompleted { activation: actual } if actual == activation()
    ));
    assert_eq!(
        port_failure.original_attempt_failure.as_deref(),
        Some(&original)
    );
    assert_eq!(port_failure.error.execution_failure(), Some(&original));
}
#[tokio::test]
async fn actual_native_run_poll_panic_is_an_independent_veto() {
    let future = Box::pin(std::future::poll_fn(
        |_| -> Poll<Result<NativeAttemptStartupOutcome, QueryExecutionError>> {
            panic!("original native run poll panic")
        },
    ));
    let causes = FutureCauses::default();
    let mut returned = None;
    let result = poll_original(future, &causes, None, None, |value| returned = Some(value)).await;
    assert_eq!(result, Err(WaitFailure::Panicked));
    match project_start_result(activation(), returned, result.is_err() || causes.failed()) {
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
fn secondary_native_run_drop_panic_veto_keeps_returned_original_error() {
    let failure = backend_failure();
    let original = failure.input().clone();
    match project_start_result(activation(), Some(Err(failure.error().clone())), true) {
        NativeFact::NativeRunFailed { failure, .. } => {
            assert_eq!(failure.original_attempt_failure.as_deref(), Some(&original));
            assert_eq!(failure.error.execution_failure(), Some(&original));
        }
        _ => panic!(
            "independent destructor failure is a veto without replacing the returned original cause"
        ),
    }
}

#[tokio::test]
async fn protocol_closed_startup_callback_keeps_the_separate_ap_backend_loss_recoverable() {
    for backend_first in [false, true] {
        let original = backend_failure();
        let mut ap_failure = backend_first.then(|| FailureReport::from_attempt(&original));
        let causes = FutureCauses::default();
        let mut returned = None;
        let actual_callback = Box::pin(std::future::ready(Ok::<_, QueryExecutionError>(
            NativeAttemptStartupOutcome::ClosedByProtocol,
        )));
        assert_eq!(
            poll_original(actual_callback, &causes, None, None, |value| returned =
                Some(value))
            .await,
            Ok(())
        );
        assert!(!causes.failed());
        assert!(matches!(
            project_start_result(activation(), returned, causes.failed()),
            NativeFact::RunCompleted { activation: actual } if actual == activation()
        ));
        let report = ap_failure.get_or_insert_with(|| FailureReport::from_attempt(&original));
        assert_eq!(
            report.original_attempt_failure.as_deref(),
            Some(original.input())
        );
        assert_eq!(report.error.execution_failure(), Some(original.input()));
        assert_eq!(
            original.input().fact(),
            QueryFailureReason::BackendLost.failure_fact()
        );
        assert_eq!(
            original.input().site(),
            ExecutionFailureSite::Backend {
                process: root().backend_process_id()
            }
        );
        assert_eq!(
            original.class(),
            novarocks_coordination_contract::AttemptFailureClass::RecoverableInfrastructure
        );
        assert!(report.pre_ready_replacement.is_none());
    }
}

#[test]
fn protocol_closed_startup_does_not_downgrade_errors_missing_results_or_supervision_failure() {
    let original = backend_failure();
    for supervision_failed in [false, true] {
        match project_start_result(
            activation(),
            Some(Err(original.error().clone())),
            supervision_failed,
        ) {
            NativeFact::NativeRunFailed {
                activation: actual,
                failure,
            } => {
                assert_eq!(actual, activation());
                assert_eq!(failure.error.execution_failure(), Some(original.input()));
                assert_eq!(
                    failure.original_attempt_failure.as_deref(),
                    Some(original.input())
                );
            }
            _ => panic!("an explicit original startup error remains a Native veto"),
        }
    }
    for (result, supervision_failed, expected) in [
        (
            None,
            false,
            QueryFailureReason::ResultProtocol.failure_fact(),
        ),
        (
            None,
            true,
            QueryFailureReason::NativeFuturePanicked.failure_fact(),
        ),
        (
            Some(Ok(NativeAttemptStartupOutcome::ClosedByProtocol)),
            true,
            QueryFailureReason::NativeFuturePanicked.failure_fact(),
        ),
        (
            Some(Ok(NativeAttemptStartupOutcome::Ready)),
            true,
            QueryFailureReason::NativeFuturePanicked.failure_fact(),
        ),
    ] {
        match project_start_result(activation(), result, supervision_failed) {
            NativeFact::NativeRunFailed {
                activation: actual,
                failure,
            } => {
                assert_eq!(actual, activation());
                assert_eq!(failure.error.execution_failure().unwrap().fact(), expected);
            }
            _ => panic!("a normal callback outcome cannot erase an independent failure"),
        }
    }
}

#[tokio::test]
async fn protocol_closed_startup_keeps_an_independent_original_future_drop_panic() {
    struct ClosedFuture {
        received: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for ClosedFuture {
        type Output = Result<NativeAttemptStartupOutcome, QueryExecutionError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(NativeAttemptStartupOutcome::ClosedByProtocol))
        }
    }
    impl Drop for ClosedFuture {
        fn drop(&mut self) {
            assert!(self.received.load(Ordering::SeqCst));
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("original startup close future Drop panic");
        }
    }
    let received = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let future = Box::pin(ClosedFuture {
        received: received.clone(),
        drops: drops.clone(),
    });
    let causes = FutureCauses::default();
    let mut returned = None;
    let polled = poll_original(future, &causes, None, None, |value| {
        returned = Some(value);
        received.store(true, Ordering::SeqCst);
    })
    .await;
    assert_eq!(polled, Ok(()));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(causes.primary.lock().unwrap().is_none());
    assert_eq!(
        causes
            .cleanup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<&str>(),
        Some(&"original startup close future Drop panic")
    );
    assert!(matches!(
        project_start_result(activation(), returned, polled.is_err() || causes.failed()),
        NativeFact::NativeRunFailed { activation: actual, failure }
            if actual == activation() && failure.error.execution_failure().unwrap().fact() == QueryFailureReason::NativeFuturePanicked.failure_fact()
    ));
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
        _activation_identity: novarocks_coordination_contract::AttemptActivationIdentity,
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
            activation_started: None,
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
    retired.2.send_replace(true);
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
    retired.2.send_replace(true);
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
        .unwrap()
        .2
        .send_replace(true);
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
        .unwrap()
        .2
        .send_replace(true);
    assert!(*receiver.borrow());
    assert!(
        storage
            .qualifications
            .get(&identity.replacement)
            .unwrap()
            .dispatch_started
    );
}

fn drive_paid_native_fixture(
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
) {
    use novarocks_workload_control::joint_admission::JointProgress;
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original paid Native fixture failed to reach idle");
}
fn paid_native_reply(
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    pending: &mut novarocks_workload_control::joint_admission::JointAdmission,
) -> novarocks_workload_control::joint_admission::JointGrantedBatch {
    drive_paid_native_fixture(driver);
    match Pin::new(pending).poll(&mut Context::from_waker(futures::task::noop_waker_ref())) {
        Poll::Ready(Ok(reply)) => reply,
        _ => panic!("the original paid proposal must have a reply"),
    }
}

fn owned_consumer_computation() -> (
    novarocks_workload_control::WorkloadControl,
    novarocks_workload_control::joint_admission::JointProgressDriver,
    novarocks_workload_control::RootWork,
    novarocks_workload_control::WorkOwner,
    novarocks_workload_control::joint_admission::RetainedPayloadGrant,
    crate::admitted_query_context::QueryResultCapacityBinding,
) {
    use crate::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_resource_contract::SharedDependency;
    use novarocks_workload_control::joint_admission::{
        JointLane, JointResource, JointResources, ResponsibilityConfig, RetainedPayloadGrant,
    };
    use novarocks_workload_control::{
        ControlStorageConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };
    let count = 8;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (admission, mut driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    let mut capacity = ResultCapacityConfig::V1;
    capacity.positions[2] = 1;
    capacity.all_objects_bytes[2] = 16;
    control.configure_result_capacity(capacity).unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let mut binding = scope.begin_control_binding().unwrap();
    binding
        .push_dependency(&SharedDependency::LocalHost)
        .unwrap();
    binding.end_change().unwrap();
    let mut pending = binding.submit().unwrap();
    let mut reply = paid_native_reply(&mut driver, &mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    drop((reply, pending));
    let mut builder = admission.begin_paid_proposal(&root.owner).unwrap();
    builder
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO
                .with(JointResource::ClaimRecords, 1)
                .with(JointResource::PayloadBytes, 8),
        )
        .unwrap();
    builder
        .push_dependency(&SharedDependency::LocalHost)
        .unwrap();
    builder.end_change().unwrap();
    let mut pending = builder.submit().unwrap();
    let mut reply = paid_native_reply(&mut driver, &mut pending);
    let mut raw = reply.new_claims.pop();
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    drop((reply, pending));
    let payload = payload.unwrap();
    let mut pending = scope.request_joint_child().unwrap();
    let mut reply = paid_native_reply(&mut driver, &mut pending);
    let mut stage = None;
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&scope, &mut stage)
        .unwrap()
        .notify();
    drop((reply, pending));
    let stage = stage.unwrap();
    let mut builder = payload
        .root_stage_source(&scope, &stage)
        .unwrap()
        .begin(8, JointLane::Foreground)
        .unwrap();
    builder.end_payload().unwrap();
    builder.end_stage().unwrap();
    let mut pending = builder.submit().unwrap();
    drive_paid_native_fixture(&mut driver);
    let mut reply = match Pin::new(&mut pending)
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    {
        Poll::Ready(Ok(reply)) => reply,
        _ => panic!("the original paid Root stage proposal must have a reply"),
    };
    let mut computation = None;
    reply
        .install_in(&scope, &payload, &stage, &mut computation)
        .unwrap()
        .notify();
    drop((reply, pending));
    let mut binding = None;
    QueryResultCapacityBinding::from_owned_internal_computation_in(
        &scope,
        &stage,
        &payload,
        &mut computation,
        &mut binding,
    )
    .unwrap();
    assert!(computation.is_none());
    drive_paid_native_fixture(&mut driver);
    (control, driver, root, stage, payload, binding.unwrap())
}

#[derive(Clone, Copy, Debug)]
enum ConsumerScript {
    FactoryPanic,
    PollPanic,
    DropPanic,
    PollAndDropPanic,
    DomainFailure,
}
impl ConsumerScript {
    fn factory_panics(self) -> bool {
        matches!(self, Self::FactoryPanic)
    }
    fn poll_panics(self) -> bool {
        matches!(self, Self::PollPanic | Self::PollAndDropPanic)
    }
    fn drop_panics(self) -> bool {
        matches!(self, Self::DropPanic | Self::PollAndDropPanic)
    }
}
struct ConsumerOriginalCause {
    label: &'static str,
    drops: Arc<AtomicUsize>,
    original_window: Option<novarocks_workload_control::ResultWindowAlias>,
}
impl Drop for ConsumerOriginalCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct ScriptedOriginalConsumer {
    mode: ConsumerScript,
    window: novarocks_workload_control::ResultWindowAlias,
    factory_calls: Arc<AtomicUsize>,
    future_drops: Arc<AtomicUsize>,
    primary_drops: Arc<AtomicUsize>,
    cleanup_drops: Arc<AtomicUsize>,
    original_domain_error: QueryExecutionError,
}
impl fmt::Debug for ScriptedOriginalConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedOriginalConsumer")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}
struct ScriptedConsumerFuture {
    activation: Option<crate::api::InternalConsumerActivation>,
    mode: ConsumerScript,
    window: Option<novarocks_workload_control::ResultWindowAlias>,
    drops: Arc<AtomicUsize>,
    primary_drops: Arc<AtomicUsize>,
    cleanup_drops: Arc<AtomicUsize>,
    original_domain_error: QueryExecutionError,
}
impl Future for ScriptedConsumerFuture {
    type Output = Result<(), QueryExecutionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.activation.as_ref().unwrap();
        assert_eq!(original.stream.activation(), activation());
        assert_eq!(original.source.activation(), activation());
        assert_eq!(original.source.root(), root());
        if self.mode.poll_panics() {
            let payload = ConsumerOriginalCause {
                label: "original consumer poll cause",
                drops: Arc::clone(&self.primary_drops),
                original_window: self.window.take(),
            };
            std::panic::panic_any(payload);
        }
        Poll::Ready(Err(self.original_domain_error.clone()))
    }
}
impl Drop for ScriptedConsumerFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.mode.drop_panics() {
            std::panic::panic_any(ConsumerOriginalCause {
                label: "original consumer future Drop cause",
                drops: Arc::clone(&self.cleanup_drops),
                original_window: None,
            });
        }
    }
}
impl crate::api::InternalComputationConsumer for ScriptedOriginalConsumer {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        // This script never issues a CPU job. Its real F and activity are
        // exercised separately from FA's future credential/actual Join.
        Poll::Ready(Ok(()))
    }
    fn consume(
        &self,
        activation: crate::api::InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        self.factory_calls.fetch_add(1, Ordering::SeqCst);
        if self.mode.factory_panics() {
            std::panic::panic_any(ConsumerOriginalCause {
                label: "original consumer factory cause",
                drops: Arc::clone(&self.primary_drops),
                original_window: Some(self.window.clone()),
            });
        }
        Box::pin(ScriptedConsumerFuture {
            activation: Some(activation),
            mode: self.mode,
            window: Some(self.window.clone()),
            drops: Arc::clone(&self.future_drops),
            primary_drops: Arc::clone(&self.primary_drops),
            cleanup_drops: Arc::clone(&self.cleanup_drops),
            original_domain_error: self.original_domain_error.clone(),
        })
    }
}

struct ConsumerFacts(tokio::sync::mpsc::UnboundedSender<crate::api::LogicalHostFact>);

async fn original_consumer_tail_for_join_fixture(
    owner: &Arc<super::super::consumer::ConsumerExecutor>,
    source: &Arc<OriginalLogicalExit>,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    activity: crate::admitted_query_context::ResultCapacityActivityLease,
    consumer: Arc<dyn crate::api::InternalComputationConsumer>,
) -> Arc<OriginalTailSlot> {
    let input = owner.bind_internal(activation, root).unwrap();
    let exit = input.supervise_exit().await.unwrap();
    Arc::new(OriginalTailSlot {
        activation,
        root,
        kind: OriginalTailKind::Consumer,
        state: Mutex::new(OriginalTailState {
            phase: TailPhase::Claimed,
            input: Some(OriginalTailInput::Consumer(Box::new(
                OriginalConsumerStart {
                    consumer,
                    activation: input,
                    exit,
                    activity: None,
                },
            ))),
            ack_result: None,
            ack_failure: None,
            consumer_result: None,
            activity: Some(activity),
            cleanup_returned: false,
            join_failure: None,
        }),
        causes: FutureCauses::default(),
        original_exit: Arc::downgrade(source),
    })
}
fn consumer_owner_for_join_fixture() -> Arc<super::super::consumer::ConsumerExecutor> {
    Arc::new(
        super::super::consumer::ConsumerExecutor::new_internal(
            novarocks_logical_execution_protocol::start::RootCarrier {
                kind: novarocks_result_contract::RootOutputKind::CountOnly,
                client_rows: None,
            },
            crate::api::PlanSeal::Version(
                novarocks_physical_plan::PlanVersionId::try_new([37; 16]).unwrap(),
            ),
            std::num::NonZeroUsize::MIN,
            None,
        )
        .unwrap(),
    )
}
#[derive(Debug)]
struct AttemptConsumerJoinCause {
    activation: AttemptActivationIdentity,
    drops: Arc<AtomicUsize>,
}
impl Drop for AttemptConsumerJoinCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct HeldAttemptConsumerForJoin {
    activation: AttemptActivationIdentity,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    drops: Arc<AtomicUsize>,
}
impl crate::api::InternalComputationConsumer for HeldAttemptConsumerForJoin {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        Poll::Ready(Ok(()))
    }
    fn consume(
        &self,
        original: crate::api::InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        let entered = self.entered.lock().unwrap().take().unwrap();
        let gate = self.gate.lock().unwrap().take().unwrap();
        let activation = self.activation;
        let drops = self.drops.clone();
        Box::pin(async move {
            let _original = original;
            entered.send(()).unwrap();
            gate.await.unwrap();
            std::panic::panic_any(AttemptConsumerJoinCause { activation, drops });
        })
    }
}
#[tokio::test]
async fn original_old_and_successor_consumer_causes_stay_separate_through_their_actual_joins() {
    let (control, mut driver, work, stage, payload, binding) = owned_consumer_computation();
    let capacity = work.owner.scope().result_capacity().unwrap();
    let cut = binding.streamed_computation_cut_handle().unwrap();
    let source = OriginalLogicalExit::new();
    let owner = consumer_owner_for_join_fixture();
    let mut originals = Vec::new();
    for offset in 0..2 {
        let identity = AttemptActivationIdentity::from_parts(
            activation().actor().get(),
            execution(offset + 1),
            activation().generation() + offset,
        );
        let task = TaskIdentity::new(
            identity.execution(),
            root().stage_id(),
            root().task_id(),
            root().backend_process_id(),
        );
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, gate) = tokio::sync::oneshot::channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let slot = original_consumer_tail_for_join_fixture(
            &owner,
            &source,
            identity,
            task,
            cut.begin_consumer_activity().unwrap(),
            Arc::new(HeldAttemptConsumerForJoin {
                activation: identity,
                entered: Mutex::new(Some(entered)),
                gate: Mutex::new(Some(gate)),
                drops: drops.clone(),
            }),
        )
        .await;
        let (future, joined) = OriginalTailWorkObligation { slot: slot.clone() }.into_future();
        #[expect(
            clippy::disallowed_methods,
            reason = "This component test retains and joins the original native future or thread to observe its real body and capture exit."
        )]
        let task = tokio::spawn(future);
        entry.await.unwrap();
        originals.push((slot, Some(joined), task, Some(release), drops));
    }
    // This component owns no Native producer; both actual consumer activities
    // still independently retain the same paid computation until their Joins.
    cut.native_body_exited().unwrap();
    drive_paid_native_fixture(&mut driver);
    let mut positions = Vec::new();
    let mut exact_causes = Vec::new();
    for index in 0..2 {
        originals[index].3.take().unwrap().send(()).unwrap();
        let result = (&mut originals[index].2).await;
        let joined = originals[index].1.take().unwrap();
        receive_tail_join(
            &joined.slot,
            result.map_err(|original| Arc::new(original) as OriginalJoinFailure),
        )
        .unwrap();
        drop(joined);
        drive_paid_native_fixture(&mut driver);
        positions.push(capacity.snapshot().held_positions[2]);
        let slot = &originals[index].0;
        exact_causes.push(
            slot.causes
                .primary
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|cause| cause.downcast_ref::<AttemptConsumerJoinCause>())
                .is_some_and(|cause| {
                    cause.activation == slot.activation
                        && Arc::ptr_eq(&cause.drops, &originals[index].4)
                }),
        );
        if index == 0 {
            assert!(originals[1].0.causes.primary.lock().unwrap().is_none());
            assert_eq!(originals[1].0.lock().phase, TailPhase::Running);
        }
    }
    let drops = [originals[0].4.clone(), originals[1].4.clone()];
    let retained = [
        drops[0].load(Ordering::SeqCst),
        drops[1].load(Ordering::SeqCst),
    ];
    drop(originals); // Explicit disposition of the same original raw causes.
    drop((owner, source, cut, binding, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive_paid_native_fixture(&mut driver);
    assert_eq!(positions, [1, 0]);
    assert_eq!(exact_causes, [true, true]);
    assert_eq!(retained, [0, 0]);
    assert_eq!(
        [
            drops[0].load(Ordering::SeqCst),
            drops[1].load(Ordering::SeqCst)
        ],
        [1, 1]
    );
    assert_eq!(control.snapshot().result_windows.held_positions[2], 0);
}

struct OpaqueConsumerReceiptForJoin {
    window: Mutex<Option<novarocks_workload_control::ResultWindowAlias>>,
    cause_drops: Arc<AtomicUsize>,
}
impl fmt::Debug for OpaqueConsumerReceiptForJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpaqueConsumerReceiptForJoin")
            .finish_non_exhaustive()
    }
}
impl Drop for OpaqueConsumerReceiptForJoin {
    fn drop(&mut self) {
        drop(self.window.lock().unwrap().take());
        std::panic::panic_any(AttemptConsumerJoinCause {
            activation: activation(),
            drops: self.cause_drops.clone(),
        });
    }
}
impl crate::api::InternalComputationConsumer for OpaqueConsumerReceiptForJoin {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
        Poll::Ready(Ok(()))
    }
    fn consume(
        &self,
        original: crate::api::InternalConsumerActivation,
    ) -> crate::api::InternalConsumerFuture {
        Box::pin(async move {
            drop(original);
            Ok(())
        })
    }
}
#[tokio::test]
async fn opaque_consumer_receipt_join_cause_survives_actual_p1_release_until_original_health_disposal()
 {
    let (control, mut driver, work, stage, payload, binding) = owned_consumer_computation();
    let capacity = work.owner.scope().result_capacity().unwrap();
    let cut = binding.streamed_computation_cut_handle().unwrap();
    let source = OriginalLogicalExit::new();
    let owner = consumer_owner_for_join_fixture();
    let drops = Arc::new(AtomicUsize::new(0));
    let slot = original_consumer_tail_for_join_fixture(
        &owner,
        &source,
        activation(),
        root(),
        cut.begin_consumer_activity().unwrap(),
        Arc::new(OpaqueConsumerReceiptForJoin {
            window: Mutex::new(Some(binding.try_window_alias().unwrap())),
            cause_drops: drops.clone(),
        }),
    )
    .await;
    let (future, joined) = OriginalTailWorkObligation { slot: slot.clone() }.into_future();
    cut.native_body_exited().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    #[expect(
        clippy::disallowed_methods,
        reason = "This component test retains and joins the original native future or thread to observe its real body and capture exit."
    )]
    let mut original_task = tokio::spawn(async move {
        released.await.expect("same original Join probe release");
        future.await;
    });
    let mut cancelled_wait = Box::pin(std::future::poll_fn(|cx| {
        Pin::new(&mut original_task).poll(cx)
    }));
    assert!(futures::poll!(&mut cancelled_wait).is_pending());
    drop(cancelled_wait); // Cancel only the borrow, never the original task.
    drive_paid_native_fixture(&mut driver);
    assert_eq!(capacity.snapshot().held_positions[2], 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let original_join = (&mut original_task).await;
    // Stabilize the genuine result before disposing the completed handle.
    drop(original_task);
    let actual_panic = original_join.as_ref().is_err_and(|cause| cause.is_panic());
    drive_paid_native_fixture(&mut driver);
    let before_join = capacity.snapshot().held_positions[2];
    receive_tail_join(
        &joined.slot,
        original_join.map_err(|original| Arc::new(original) as OriginalJoinFailure),
    )
    .unwrap();
    drop(joined);
    drive_paid_native_fixture(&mut driver);
    let after_join = capacity.snapshot().held_positions[2];
    let actual_original = slot
        .lock()
        .join_failure
        .as_ref()
        .and_then(|cause| cause.downcast_ref::<tokio::task::JoinError>())
        .is_some_and(|cause| cause.is_panic());
    let health: Arc<dyn Error + Send + Sync> = slot.clone();
    drop(slot);
    let retained = drops.load(Ordering::SeqCst);
    let unhandled = source.has_unhandled_failure();
    let retained_health = Arc::clone(&health);
    drop(health);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the same original health alias still owns the genuine JoinError"
    );
    drop(retained_health); // Explicit process-exit disposition, never a positive close.
    drop((owner, source, cut, binding, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive_paid_native_fixture(&mut driver);
    assert!(actual_panic && actual_original && unhandled);
    assert_eq!((before_join, after_join), (1, 0));
    assert_eq!(retained, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().result_windows.held_positions[2], 0);
}

impl crate::api::LogicalFactSource for ConsumerFacts {
    fn id(&self) -> novarocks_logical_execution_protocol::LogicalSourceId {
        novarocks_logical_execution_protocol::LogicalSourceId::new(std::num::NonZeroU64::MIN)
    }
    fn send(
        &mut self,
        fact: crate::api::LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), crate::api::LogicalSourceClosed>> {
        Box::pin(async move {
            self.0
                .send(fact)
                .map_err(|_| crate::api::LogicalSourceClosed)
        })
    }
}
async fn poll_consumer_script_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}
async fn assert_original_consumer_script(mode: ConsumerScript) {
    use super::super::consumer::ConsumerExecutor;
    use crate::api::LogicalHostFact;
    use novarocks_logical_execution_protocol::{
        LogicalSourceId,
        effect::{ConsumerEffect, InternalResponse},
        input::{ConsumerFact, InternalRequest},
        start::RootCarrier,
    };
    let (control, mut driver, work, stage, payload, binding) = owned_consumer_computation();
    let scope = work.owner.scope();
    let capacity = scope.result_capacity().unwrap();
    let cut = binding.streamed_computation_cut_handle().unwrap();
    let activity = cut.begin_consumer_activity().unwrap();
    let source = OriginalLogicalExit::new();
    let owner = Arc::new(
        ConsumerExecutor::new_internal(
            RootCarrier {
                kind: novarocks_result_contract::RootOutputKind::CountOnly,
                client_rows: None,
            },
            crate::api::PlanSeal::Version(
                novarocks_physical_plan::PlanVersionId::try_new([37; 16]).unwrap(),
            ),
            std::num::NonZeroUsize::MIN,
            None,
        )
        .unwrap(),
    );
    let (facts, mut observed) = tokio::sync::mpsc::unbounded_channel();
    owner
        .attach_source(Box::new(ConsumerFacts(facts)))
        .await
        .unwrap();
    let input = owner.bind_internal(activation(), root()).unwrap();
    let exit = input.supervise_exit().await.unwrap();
    let primary_drops = Arc::new(AtomicUsize::new(0));
    let cleanup_drops = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let domain = protocol("original consumer domain refusal before future destruction");
    let consumer: Arc<dyn crate::api::InternalComputationConsumer> =
        Arc::new(ScriptedOriginalConsumer {
            mode,
            window: binding.try_window_alias().unwrap(),
            factory_calls: Arc::clone(&factory_calls),
            future_drops: Arc::clone(&future_drops),
            primary_drops: Arc::clone(&primary_drops),
            cleanup_drops: Arc::clone(&cleanup_drops),
            original_domain_error: domain.clone(),
        });
    let slot = Arc::new(OriginalTailSlot {
        activation: activation(),
        root: root(),
        kind: OriginalTailKind::Consumer,
        state: Mutex::new(OriginalTailState {
            phase: TailPhase::Claimed,
            input: Some(OriginalTailInput::Consumer(Box::new(
                OriginalConsumerStart {
                    consumer: Arc::clone(&consumer),
                    activation: input,
                    exit,
                    activity: None,
                },
            ))),
            ack_result: None,
            ack_failure: None,
            consumer_result: None,
            activity: Some(activity),
            cleanup_returned: false,
            join_failure: None,
        }),
        causes: FutureCauses::default(),
        original_exit: Arc::downgrade(&source),
    });
    {
        let state = slot.lock();
        let Some(OriginalTailInput::Consumer(input)) = state.input.as_ref() else {
            panic!("same original consumer input expected");
        };
        assert!(Arc::ptr_eq(&input.consumer, &consumer));
    }
    let (future, joined) = OriginalTailWorkObligation {
        slot: Arc::clone(&slot),
    }
    .into_future();
    assert!(Arc::ptr_eq(&joined.slot, &slot));
    let mut observer = Box::pin(owner.observe_internal_source_exit(activation()));
    assert!(
        poll_consumer_script_once(observer.as_mut())
            .await
            .is_pending()
    );
    assert!(observed.try_recv().is_err());
    // Supply only the original producer-exit seam. The consumer's original
    // paid activity still guards the positive computation through its own cut.
    cut.native_body_exited().unwrap();
    drive_paid_native_fixture(&mut driver);
    assert_eq!(capacity.snapshot().held_positions[2], 1);
    #[expect(
        clippy::disallowed_methods,
        reason = "This component test retains and joins the original native future or thread to observe its real body and capture exit."
    )]
    let actual_join = tokio::spawn(future).await;
    let expected = if mode.factory_panics() || mode.poll_panics() {
        consumer_panic()
    } else {
        domain.clone()
    };
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        future_drops.load(Ordering::SeqCst),
        usize::from(!mode.factory_panics())
    );
    {
        let state = slot.lock();
        assert_eq!(state.phase, TailPhase::Returned);
        assert_eq!(state.consumer_result, Some(Err(expected.clone())));
        assert_eq!(state.cleanup_returned, !mode.drop_panics());
        assert!(
            state.activity.is_some(),
            "returning the F is not its actual Join"
        );
        assert!(state.input.is_none());
        assert!(state.join_failure.is_none());
    }
    assert_eq!(capacity.snapshot().held_positions[2], 1);
    assert_eq!(
        source.has_unhandled_failure(),
        !matches!(mode, ConsumerScript::DomainFailure)
    );
    assert!(
        poll_consumer_script_once(observer.as_mut())
            .await
            .is_pending()
    );
    let LogicalHostFact::Internal {
        token,
        activation: original,
        request: InternalRequest::SourceClosed,
    } = observed
        .try_recv()
        .expect("actual source Drop must be observed")
    else {
        panic!("original SourceClosed expected")
    };
    assert_eq!(original, activation());
    assert!(matches!(
        owner
            .execute(ConsumerEffect::InternalReply {
                source: LogicalSourceId::new(std::num::NonZeroU64::MIN),
                token,
                activation: original,
                response: InternalResponse::Applied,
            })
            .await,
        ConsumerFact::Applied
    ));
    assert!(
        poll_consumer_script_once(observer.as_mut())
            .await
            .is_pending()
    );
    let LogicalHostFact::Internal {
        token,
        activation: original,
        request: InternalRequest::ExitObserved { outcome },
    } = observed
        .try_recv()
        .expect("the original reporter must retain the actual F verdict")
    else {
        panic!("original ExitObserved expected")
    };
    assert_eq!(original, activation());
    assert_eq!(outcome, Some(Err(expected.clone())));
    owner
        .execute(ConsumerEffect::InternalReply {
            source: LogicalSourceId::new(std::num::NonZeroU64::MIN),
            token,
            activation: original,
            response: InternalResponse::Applied,
        })
        .await;
    assert_eq!(observer.await, Err(expected));
    let primary = slot.causes.primary.lock().unwrap();
    if mode.factory_panics() || mode.poll_panics() {
        let raw = primary
            .as_ref()
            .unwrap()
            .downcast_ref::<ConsumerOriginalCause>()
            .unwrap();
        assert_eq!(
            raw.label,
            if mode.factory_panics() {
                "original consumer factory cause"
            } else {
                "original consumer poll cause"
            }
        );
        assert!(raw.original_window.is_some());
    } else {
        assert!(primary.is_none());
    }
    drop(primary);
    let cleanup = slot.causes.cleanup.lock().unwrap();
    if mode.drop_panics() {
        assert_eq!(
            cleanup
                .as_ref()
                .unwrap()
                .downcast_ref::<ConsumerOriginalCause>()
                .unwrap()
                .label,
            "original consumer future Drop cause"
        );
    } else {
        assert!(cleanup.is_none());
    }
    drop(cleanup);
    assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
    assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
    // Consume the same actual Tokio Join. This is not a credentialed FA
    // task test, and Join cannot certify an F destructor which did not return.
    receive_tail_join(
        &joined.slot,
        actual_join.map_err(|original| Arc::new(original) as OriginalJoinFailure),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&joined.slot, &slot));
    drop(joined);
    drive_paid_native_fixture(&mut driver);
    assert_eq!(slot.lock().phase, TailPhase::Joined);
    assert_eq!(slot.lock().activity.is_some(), mode.drop_panics());
    assert_eq!(
        capacity.snapshot().held_positions[2],
        usize::from(mode.drop_panics())
    );
    let health: Arc<dyn Error + Send + Sync> = Arc::clone(&slot) as Arc<dyn Error + Send + Sync>;
    drop(slot);
    assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
    assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        capacity.snapshot().held_positions[2],
        usize::from(mode.drop_panics())
    );
    // Explicit test disposition of the same raw health owner, analogous only
    // to process-exit disposal. No success receipt reclaims this allocation.
    drop(health);
    drive_paid_native_fixture(&mut driver);
    assert_eq!(
        primary_drops.load(Ordering::SeqCst),
        usize::from(mode.factory_panics() || mode.poll_panics())
    );
    assert_eq!(
        cleanup_drops.load(Ordering::SeqCst),
        usize::from(mode.drop_panics())
    );
    assert_eq!(capacity.snapshot().held_positions[2], 0);
    drop((owner, consumer, source, cut, binding, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive_paid_native_fixture(&mut driver);
    assert_eq!(control.snapshot().result_windows.held_positions[2], 0);
}
#[tokio::test]
async fn actual_consumer_factory_panic_keeps_original_reporter_cause_and_activity_until_join() {
    assert_original_consumer_script(ConsumerScript::FactoryPanic).await;
}
#[tokio::test]
async fn actual_consumer_poll_panic_releases_original_activity_only_after_returned_cleanup_and_join()
 {
    assert_original_consumer_script(ConsumerScript::PollPanic).await;
}
#[tokio::test]
async fn actual_consumer_future_drop_panic_keeps_domain_verdict_raw_cause_and_original_activity() {
    assert_original_consumer_script(ConsumerScript::DropPanic).await;
}
#[tokio::test]
async fn actual_consumer_poll_and_drop_panic_keep_both_original_causes_and_activity() {
    assert_original_consumer_script(ConsumerScript::PollAndDropPanic).await;
}
#[tokio::test]
async fn actual_consumer_domain_failure_settles_same_reporter_without_process_health() {
    assert_original_consumer_script(ConsumerScript::DomainFailure).await;
}

struct RetiringOriginalPreparation {
    drops: Arc<AtomicUsize>,
    _window: novarocks_workload_control::ResultWindowAlias,
}
impl fmt::Debug for RetiringOriginalPreparation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RetiringOriginalPreparation")
    }
}
impl crate::api::NativeAttemptPreparationPort for RetiringOriginalPreparation {
    fn prepare_in<'a>(
        &'a mut self,
        _: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> crate::api::NativeAttemptPreparationFuture<'a> {
        panic!("retired original preparation must never be polled again")
    }
}
impl Drop for RetiringOriginalPreparation {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn retire_preparation_keeps_real_reply_transport_and_consumer_aliases_until_last_drop() {
    use crate::admitted_query_context::QueryResultCapacityBinding;
    use crate::api::{LogicalNativeOpenRequest, NativeLogicalExecutionSeed};
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, OutputContract,
    };
    use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootReadResponse, RootResultData, RootResultReply,
    };
    use novarocks_result_contract::{RootOutputKind, RootProfileId};
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    let window = capacity
        .try_acquire(&scope, ResultWindowClass::Client)
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let plan = crate::completed_plan_fixture::completed_noop_plan([63; 16]);
    let description = Arc::new(
        FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Maintenance,
            plan.candidate().clone(),
            Vec::new(),
            OutputContract::CompletionOnly,
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap(),
    );
    let seed = NativeLogicalExecutionSeed::issue(
        description.plan_identity(),
        None,
        RetiringOriginalPreparation {
            drops: Arc::clone(&drops),
            _window: window.retain_alias(),
        },
    );
    let (open, accepted) = LogicalNativeOpenRequest::issue(
        execution(1),
        description,
        scope.id(),
        scope.cancellation().unwrap(),
        seed,
    );
    let mut session = accepted.accept(open.bind().unwrap()).unwrap();
    let mut binding =
        Some(QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap());
    session.set_result_capacity_in(&mut binding);
    assert!(binding.is_none());
    let reply = crate::api::RetainedRootReply::try_new(
        RootReadResponse::Reply(RootResultReply {
            root_task: root(),
            profile: RootProfileId::V1,
            kind: RootOutputKind::ClientRows,
            accepted_consumed: 0,
            outcome: RootReadOutcome::Data(
                RootResultData::try_new(
                    RootOutputKind::ClientRows,
                    std::num::NonZeroU64::MIN,
                    bytes::Bytes::from_static(b"0"),
                    None,
                )
                .unwrap(),
            ),
        }),
        window.retain_alias(),
        1,
    )
    .unwrap();
    let transport = reply.retain_physical_guard();
    let consumer_alias = window.retain_alias();
    let consumer_entered = Arc::new(AtomicBool::new(false));
    let entered = Arc::clone(&consumer_entered);
    let mut consumer = Box::pin(async move {
        let _same_original_window = consumer_alias;
        entered.store(true, Ordering::SeqCst);
        std::future::pending::<()>().await
    });
    assert!(
        poll_consumer_script_once(consumer.as_mut())
            .await
            .is_pending()
    );
    assert!(consumer_entered.load(Ordering::SeqCst));
    drop(window);
    let mut registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
    let handle = registry.handle();
    let mut reservation = handle.reserve(execution(1), vec![]).unwrap();
    let registration = reservation.runtime_registration();
    let initial = AttemptActivationIdentity::from_parts(
        reservation.actor_instance().get(),
        execution(1),
        activation().generation(),
    );
    let body = Arc::new(AsyncMutex::new(OriginalNativePreparation {
        session: Some(session),
        ..OriginalNativePreparation::default()
    }));
    let original = NativeExecutor::new(
        None,
        Arc::new(super::UnboundInstalledAttemptPort),
        Arc::clone(&body),
        Weak::new(),
        handle,
        registration,
        initial,
        Arc::new(schedule()),
        FrontendProcessId::try_from_bytes(
            OperationIds::new(IdSeed::new(93, 100)).issue().to_bytes(),
        )
        .unwrap(),
        Arc::new(FixedClock(MonotonicInstant::ORIGIN)),
        scope.cancellation().unwrap(),
    );
    assert_eq!(capacity.snapshot().held_positions[0], 1);
    assert!(matches!(
        original.execute(NativeEffect::RetirePreparation).await,
        NativeFact::Applied
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        original.execute(NativeEffect::RetirePreparation).await,
        NativeFact::Applied
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        body.lock()
            .await
            .session
            .as_ref()
            .unwrap()
            .issue_attempt(execution(2)),
        Err(crate::api::NativeExecutionContractError::AttemptPreparationRetired)
    ));
    assert_eq!(capacity.snapshot().held_positions[0], 1);
    drop(reply);
    assert_eq!(
        capacity.snapshot().held_positions[0],
        1,
        "original transport and consumer aliases stay live"
    );
    drop(transport);
    assert_eq!(
        capacity.snapshot().held_positions[0],
        1,
        "original polled consumer frame is the final alias"
    );
    drop(consumer);
    assert_eq!(
        capacity.snapshot().held_positions[0],
        0,
        "only the final original alias Drop returns the position"
    );
    drop((original, body, reservation));
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    work.business.release();
    work.owner.complete();
    assert_eq!(control.snapshot().result_windows.held_positions[0], 0);
}

#[tokio::test]
async fn cleanup_adopts_actual_completed_activation_without_reissuing_and_keeps_drop_unknown() {
    #[derive(Debug)]
    struct DropDormant {
        inner: DormantFixture,
        panic_drop: bool,
    }
    impl Drop for DropDormant {
        fn drop(&mut self) {
            assert!(
                !self.panic_drop,
                "original dormant cleanup fixture Drop panic"
            );
        }
    }
    impl crate::api::DormantNativeAttemptOwner for DropDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn poll_bind_qualification(
            &mut self,
            schedule: &AttemptSchedule,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }
        fn activate<'a>(
            &'a mut self,
            _activation_identity: novarocks_coordination_contract::AttemptActivationIdentity,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
            cancellation: CancellationView,
        ) -> crate::api::NativeAttemptActivationFuture<'a> {
            self.inner
                .activate(_activation_identity, schedule, admissions, cancellation)
        }
        fn converge<'a>(
            &'a mut self,
            _: CancellationView,
        ) -> crate::api::NativeAttemptConvergenceFuture<'a> {
            panic!("adopted output cleanup must not re-run dormant convergence");
        }
    }
    for dormant_drop_panic in [false, true] {
        let (_control, work) = cancellation_work();
        let scope = work.owner.scope();
        let cancellation = scope.cancellation().unwrap();
        let stage = scope
            .try_acquire(novarocks_workload_control::Stage::Execution)
            .unwrap();
        work.business.release();
        let schedule = Arc::new(schedule());
        let source = OriginalLogicalExit::new();
        let calls = Arc::new(AtomicUsize::new(0));
        source.native.lock().await.dormant =
            crate::api::NativeDormantCustody::test_owner(DropDormant {
                inner: DormantFixture {
                    observed_activation: None,
                    factory_panic: false,
                    future_drop_panic: !dormant_drop_panic,
                    activation_calls: calls.clone(),
                    received: None,
                    gate: None,
                },
                panic_drop: dormant_drop_panic,
            });
        let mut registry = std::mem::ManuallyDrop::new(
            crate::coordination::LogicalExecutionRuntimeRegistry::new(),
        );
        let (registration, issued, scripted_exit) = crate::coordination::runtime_registry::runtime_tests::install_native_component_runtime_for_original(
            &registry.handle(), execution(1), schedule.participations().to_vec(), Some(source.clone()),
        );
        let initial_activation =
            AttemptActivationIdentity::from_parts(issued.actor().get(), execution(1), 23);
        let native = NativeExecutor::new(
            None,
            Arc::new(super::UnboundInstalledAttemptPort),
            source.native.clone(),
            Arc::downgrade(&source),
            registry.handle(),
            registration.clone(),
            initial_activation,
            schedule,
            FrontendProcessId::try_from_bytes(
                OperationIds::new(IdSeed::new(100, 3)).issue().to_bytes(),
            )
            .unwrap(),
            Arc::new(FixedClock(MonotonicInstant::ORIGIN)),
            cancellation.clone(),
        );
        assert!(matches!(
            native
                .execute(NativeEffect::ActivateAttempt {
                    activation: initial_activation,
                    inherited_admissions: Vec::new()
                })
                .await,
            NativeFact::Failed { .. }
        ));
        let address = source
            .native
            .lock()
            .await
            .pending_activated
            .as_ref()
            .unwrap()
            .staged_slot_address()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(registry.active_native_custody_count(), 1);
        let pending_inventory = native.cleanup_inventory().unwrap();
        assert_eq!(pending_inventory.initial_activation, initial_activation);
        assert!(
            pending_inventory.active.is_empty(),
            "a staged pending owner is not an activated-map row"
        );
        assert_eq!(pending_inventory.preparations.len(), 1);
        assert_eq!(
            pending_inventory.preparations[0].activation_started,
            Some(initial_activation)
        );
        assert!(pending_inventory.preparations[0].schedule.is_some());
        assert!(pending_inventory.preparations[0].dormant);
        assert_eq!(
            native.adopt_pending_activation_for_cleanup().await.unwrap(),
            Some(initial_activation)
        );
        assert_eq!(
            initial_activation.generation(),
            23,
            "activation identity is not custody registration generation"
        );
        let adopted_inventory = native.cleanup_inventory().unwrap();
        assert_eq!(adopted_inventory.active.len(), 1);
        assert_eq!(adopted_inventory.active[0].activation, initial_activation);
        assert!(adopted_inventory.active[0].adopted);
        assert!(!adopted_inventory.active[0].resources_exited);
        let active = native
            .lock()
            .active
            .get(&activation_key(initial_activation))
            .unwrap()
            .owner
            .clone();
        assert_eq!(active.lock().await.slot_address(), address);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            native.adopt_pending_activation_for_cleanup().await.unwrap(),
            None
        );
        let dormant = native
            .retire_adopted_dormant_for_cleanup(initial_activation)
            .await;
        assert_eq!(dormant.is_err(), dormant_drop_panic);
        let retired_inventory = native.cleanup_inventory().unwrap();
        assert_eq!(
            retired_inventory.active[0].dormant_retired,
            !dormant_drop_panic
        );
        assert_eq!(
            retired_inventory.preparations[0].dormant,
            dormant_drop_panic
        );
        assert!(!retired_inventory.active[0].resources_exited);
        assert_eq!(
            source.native.lock().await.dormant.drop_unproven(),
            dormant_drop_panic
        );
        let cut = MonotonicInstant::from_origin(Duration::from_secs(1));
        assert!(matches!(
            native
                .execute(NativeEffect::RecordTerminalCut {
                    activation: initial_activation,
                    cut,
                    remote_deadline: cut.saturating_add(Duration::from_secs(2))
                })
                .await,
            NativeFact::Applied
        ));
        assert!(
            native.has_unhandled_failure(),
            "original poll/Drop cause remains present after adoption"
        );
        assert_eq!(registry.active_native_custody_count(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!native.cleanup_inventory().unwrap().active[0].resources_exited);
        assert!(native.has_unhandled_failure());
        assert_eq!(
            source.native.lock().await.dormant.drop_unproven(),
            dormant_drop_panic
        );
        drop(active);
        drop(native);
        assert_eq!(registry.active_native_custody_count(), 1);
        assert!(registry.process_health().is_some());
        // The registry seam supplied no actual FA Source/Native exit. Its
        // unresolved original return is disposed only for this process probe.
        registry.abandon_for_process_exit();
        drop(scripted_exit);
        drop(std::mem::ManuallyDrop::into_inner(registry));
        stage.release();
        work.owner.complete();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

struct ReadOriginalQualificationOnWake {
    storage: Weak<Mutex<NativeStorage>>,
    identity: QualificationIdentity,
    operation: TaskOperationId,
    abandoned: bool,
    originals_owned: bool,
    calls: AtomicUsize,
}
impl std::task::Wake for ReadOriginalQualificationOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let storage = self.storage.upgrade().unwrap();
        let mut storage = storage.try_lock().unwrap_or_else(|_| {
            panic!("original qualification cancellation must notify outside NativeStorage custody");
        });
        let original = storage
            .qualifications
            .get(&self.identity.replacement)
            .unwrap();
        assert_eq!(original.identity, self.identity);
        assert_eq!(original.operation, Some(self.operation));
        assert!(original.cancellation_issued);
        assert!(*original.cancellation.borrow());
        assert_eq!(original.abandoned, self.abandoned);
        assert_eq!(original.qualified.is_some(), self.originals_owned);
        assert_eq!(original.active.is_some(), self.originals_owned);
        assert!(
            storage
                .prepared
                .get(&self.identity.replacement)
                .unwrap()
                .binding_complete
        );
        let error = storage
            .begin_qualification_dispatch(activation(), self.identity, self.operation)
            .err()
            .unwrap();
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            ReplacementQualificationFailure::Rejected.failure_fact()
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn qualification_cancel_is_committed_before_notification_and_original_waker_reads_storage() {
    let storage = Arc::new(Mutex::new(prepared_qualification_storage()));
    let identity = identity();
    let operation = OperationIds::new(IdSeed::new(187, 50)).issue();
    let mut receiver = storage
        .lock()
        .unwrap()
        .ensure_qualification(activation(), identity, Some(operation))
        .unwrap()
        .cancellation
        .subscribe();
    let observer = Arc::new(ReadOriginalQualificationOnWake {
        storage: Arc::downgrade(&storage),
        identity,
        operation,
        abandoned: false,
        originals_owned: false,
        calls: AtomicUsize::new(0),
    });
    let waker = std::task::Waker::from(observer.clone());
    let mut changed = Box::pin(receiver.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let retired = {
        let mut original = storage.lock().unwrap();
        original
            .close_qualification(activation(), identity, Some(operation), false)
            .unwrap()
    };
    assert_eq!(observer.calls.load(Ordering::SeqCst), 0);
    assert!(
        !*retired.2.borrow(),
        "no notification was sent under the custody lock"
    );
    {
        let mut original = storage.lock().unwrap();
        let row = original.qualifications.get(&identity.replacement).unwrap();
        assert!(row.cancellation_issued);
        assert!(!row.dispatch_started);
        assert!(!row.abandoned);
        assert_eq!(row.operation, Some(operation));
        assert!(
            original
                .begin_qualification_dispatch(activation(), identity, operation)
                .is_err(),
            "a first dispatch cannot pass between cancellation promise and notification"
        );
    }
    retired.2.send_replace(true);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(futures::task::noop_waker_ref())),
        Poll::Ready(Ok(()))
    ));
}
#[test]
fn qualification_cancel_keeps_original_resources_and_abandon_transfers_them_before_waking() {
    for abandon in [false, true] {
        let storage = Arc::new(Mutex::new(prepared_qualification_storage()));
        let identity = identity();
        let operation = OperationIds::new(IdSeed::new(188, 50)).issue();
        let (mut qualified, finishes, abandons) = reservation(false);
        let active =
            ActiveReplacementResources::replacement(qualified.activate_retained().unwrap());
        let mut receiver = {
            let mut original = storage.lock().unwrap();
            let row = original
                .ensure_qualification(activation(), identity, Some(operation))
                .unwrap();
            row.qualified = Some(qualified);
            row.active = Some(active);
            row.cancellation.subscribe()
        };
        let observer = Arc::new(ReadOriginalQualificationOnWake {
            storage: Arc::downgrade(&storage),
            identity,
            operation,
            abandoned: abandon,
            originals_owned: !abandon,
            calls: AtomicUsize::new(0),
        });
        let waker = std::task::Waker::from(observer.clone());
        let mut changed = Box::pin(receiver.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let (qualified, active, cancellation) = {
            let mut original = storage.lock().unwrap();
            original
                .close_qualification(activation(), identity, Some(operation), abandon)
                .unwrap()
        };
        assert_eq!(qualified.is_some(), abandon);
        assert_eq!(active.is_some(), abandon);
        assert_eq!(finishes.load(Ordering::SeqCst), 0);
        assert_eq!(
            abandons.load(Ordering::SeqCst),
            0,
            "moving original resources is not their destruction"
        );
        cancellation.send_replace(true);
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            abandons.load(Ordering::SeqCst),
            0,
            "the notification callback cannot drop the original resource transfer"
        );
        drop((qualified, active));
        assert_eq!(abandons.load(Ordering::SeqCst), usize::from(abandon));
        assert!(matches!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref())),
            Poll::Ready(Ok(()))
        ));
        drop(changed);
        drop(receiver);
        drop(storage);
        assert_eq!(abandons.load(Ordering::SeqCst), 1);
        assert_eq!(finishes.load(Ordering::SeqCst), 0);
    }
}

#[path = "tests/read_seal.rs"]
mod read_seal;

// Same production wait used by NativeExecutor::prepare; no Native/AP exit or
// replacement admission is authored by this physical wait component.
#[tokio::test(start_paused = true)]
async fn already_polled_original_preparation_keeps_its_first_deadline_cancel_reason_and_actual_drop()
 {
    #[derive(Clone, Copy, Debug)]
    enum Finish {
        RepeatedDeadline,
        Requested,
        DeadlineReason,
        Released,
    }
    struct OriginalPending {
        release: Arc<AtomicBool>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        received: Arc<AtomicBool>,
    }
    impl Future for OriginalPending {
        type Output = u64;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u64> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.release.load(Ordering::SeqCst) {
                Poll::Ready(41)
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for OriginalPending {
        fn drop(&mut self) {
            if self.release.load(Ordering::SeqCst) {
                assert!(
                    self.received.load(Ordering::SeqCst),
                    "same output is captured before original F Drop"
                );
            }
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OriginalOwners {
        control: Option<novarocks_workload_control::WorkloadControl>,
        work: Option<novarocks_workload_control::RootWork>,
    }
    struct KeepWhole(Option<Box<OriginalOwners>>);
    impl Drop for KeepWhole {
        fn drop(&mut self) {
            if std::thread::panicking() {
                static ORIGINALS: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                    std::sync::OnceLock::new();
                if let Some(original) = self.0.take() {
                    ORIGINALS
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(original);
                }
            }
        }
    }
    for finish in [
        Finish::RepeatedDeadline,
        Finish::Requested,
        Finish::DeadlineReason,
        Finish::Released,
    ] {
        let (control, work) = cancellation_work();
        let mut keep = KeepWhole(Some(Box::new(OriginalOwners {
            control: Some(control),
            work: Some(work),
        })));
        let original = keep.0.as_deref_mut().unwrap();
        let scope = original.work.as_ref().unwrap().owner.scope();
        let cancellation = scope.cancellation().unwrap();
        let first_deadline = tokio::time::Instant::now() + Duration::from_secs(50);
        let drops = Arc::new(AtomicUsize::new(0));
        let causes = FutureCauses::default();
        let mut returns = Vec::new();
        for ordinal in 0..if matches!(finish, Finish::RepeatedDeadline) {
            2
        } else {
            1
        } {
            let release = Arc::new(AtomicBool::new(false));
            let polls = Arc::new(AtomicUsize::new(0));
            let received = Arc::new(AtomicBool::new(false));
            let future = Box::pin(OriginalPending {
                release: release.clone(),
                polls: polls.clone(),
                drops: drops.clone(),
                received: received.clone(),
            });
            let mut actual_wait = Box::pin(poll_original(
                future,
                &causes,
                Some(&cancellation),
                Some(first_deadline),
                |value| {
                    received.store(true, Ordering::SeqCst);
                    returns.push(value);
                },
            ));
            assert!(
                futures::poll!(actual_wait.as_mut()).is_pending(),
                "{finish:?} ordinal {ordinal}"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), ordinal);
            let expected = match finish {
                Finish::RepeatedDeadline if ordinal == 0 => {
                    tokio::time::advance(Duration::from_secs(20)).await;
                    release.store(true, Ordering::SeqCst);
                    Ok(())
                }
                Finish::RepeatedDeadline => {
                    assert_eq!(
                        first_deadline.duration_since(tokio::time::Instant::now()),
                        Duration::from_secs(30)
                    );
                    tokio::time::advance(Duration::from_secs(30)).await;
                    Err(WaitFailure::Deadline)
                }
                Finish::Requested | Finish::DeadlineReason => {
                    let reason = if matches!(finish, Finish::Requested) {
                        CancellationReason::Requested
                    } else {
                        CancellationReason::DeadlineExceeded
                    };
                    original
                        .work
                        .as_ref()
                        .unwrap()
                        .owner
                        .cancellation_requester()
                        .request(reason.clone())
                        .unwrap();
                    original
                        .work
                        .as_ref()
                        .unwrap()
                        .owner
                        .cancellation_requester()
                        .request(CancellationReason::Requested)
                        .unwrap();
                    assert_eq!(cancellation.reason(), Some(reason.clone()));
                    Err(WaitFailure::Cancelled(reason))
                }
                Finish::Released => {
                    tokio::time::advance(Duration::from_secs(20)).await;
                    assert!(tokio::time::Instant::now() < first_deadline);
                    release.store(true, Ordering::SeqCst);
                    Ok(())
                }
            };
            let succeeded = expected.is_ok();
            assert_eq!(actual_wait.await, expected, "{finish:?} ordinal {ordinal}");
            assert_eq!(
                received.load(Ordering::SeqCst),
                succeeded,
                "only an actual Ready output is transferred"
            );
            assert_eq!(drops.load(Ordering::SeqCst), ordinal + 1);
            assert_eq!(received.load(Ordering::SeqCst), succeeded);
            assert_eq!(polls.load(Ordering::SeqCst), if succeeded { 2 } else { 1 });
        }
        assert_eq!(
            returns,
            if matches!(finish, Finish::RepeatedDeadline | Finish::Released) {
                vec![41]
            } else {
                Vec::new()
            }
        );
        assert!(
            !causes.failed(),
            "ordinary expiry/cancel is not raw panic health"
        );
        drop(cancellation);
        let work = original.work.take().unwrap();
        work.business.release();
        while let Some(control) = original.control.as_ref().unwrap().next_control() {
            control.acknowledge();
        }
        work.owner.complete_after_terminal_cancel_settled();
        scope.wait_released().await;
        assert_eq!(
            original
                .control
                .as_ref()
                .unwrap()
                .snapshot()
                .root_responsibilities,
            0
        );
        original.control.as_ref().unwrap().close_admission();
        if let Err(failure) = original.control.take().unwrap().shutdown() {
            let (error, control) = failure.into_parts();
            original.control = Some(control);
            panic!("same original wait component refused actual shutdown: {error}");
        }
    }
}

#[test]
fn missing_rows_runtime_keeps_original_query_6_execution_presentation_without_task_location() {
    use crate::execution_error::ExecutionFailurePresentation;
    // The same pure author is invoked by the public Core's initial/successor
    // activation-result sequences. Presentation remains QA-owned.
    let error = novarocks_logical_execution_protocol::failure::missing_rows_runtime();
    assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
    let input = error.execution_failure().unwrap();
    assert_eq!(
        input.fact(),
        QueryFailureReason::ResultProtocol.failure_fact()
    );
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        input.user_error().phase(),
        novarocks_user_error::ErrorPhase::Execution
    );
    assert_eq!(input.user_error().location(), None);
    assert_eq!(
        error.message(),
        "Native activated a row execution without its root result runtime"
    );
}

// Original passive-return custody tier. Both exact execution ordinals use the
// SAME production activation/receive/stage/retire methods. This component does
// not issue qualification permission or manufacture an AP/Native exit receipt.
#[tokio::test]
async fn initial_and_successor_rows_stay_in_same_original_return_slot_after_dormant_drop_panics() {
    use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
    struct OriginalReadPort {
        drops: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
    }
    impl BoundedRootReadPort for OriginalReadPort {
        fn read(
            &self,
            _: novarocks_execution_contract::root_result::RootResultRead,
            _: novarocks_workload_control::ResultWindowAlias,
        ) -> LogicalFuture<
            'static,
            Result<crate::api::RetainedRootReply, crate::coordination::RootResultFetchFailure>,
        > {
            self.reads.fetch_add(1, Ordering::SeqCst);
            panic!("return-custody test must not issue Root IO");
        }
        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            panic!("return-custody test must not fabricate read close");
        }
    }
    impl Drop for OriginalReadPort {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OriginalDormantDrop {
        ordinal: u64,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for OriginalDormantDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OriginalRowsDormant {
        binding: Option<crate::coordination::RootRelayBinding>,
        calls: Arc<AtomicUsize>,
        owner_drops: Arc<AtomicUsize>,
        raw_drops: Arc<AtomicUsize>,
        ordinal: u64,
    }
    impl std::fmt::Debug for OriginalRowsDormant {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("OriginalRowsDormant")
        }
    }
    impl Drop for OriginalRowsDormant {
        fn drop(&mut self) {
            self.owner_drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OriginalDormantDrop {
                ordinal: self.ordinal,
                drops: self.raw_drops.clone(),
            });
        }
    }
    impl crate::api::DormantNativeAttemptOwner for OriginalRowsDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            &[]
        }
        fn poll_bind_qualification(
            &mut self,
            _: &AttemptSchedule,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            panic!("physical return test never claims qualification");
        }
        fn activate<'a>(
            &'a mut self,
            activation: AttemptActivationIdentity,
            _: &'a AttemptSchedule,
            _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
            _: CancellationView,
        ) -> crate::api::NativeAttemptActivationFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let same_binding = self.binding.take().unwrap();
            Box::pin(async move {
                Ok(crate::api::ActivatedNativeAttempt::relayed_rows(
                    activation,
                    same_binding,
                ))
            })
        }
        fn converge<'a>(
            &'a mut self,
            _: CancellationView,
        ) -> crate::api::NativeAttemptConvergenceFuture<'a> {
            panic!("a dormant destructor panic cannot restart cleanup");
        }
    }
    struct Originals {
        control: Option<novarocks_workload_control::WorkloadControl>,
        work: Option<novarocks_workload_control::RootWork>,
        window: Option<novarocks_workload_control::ResultWindowGrant>,
        registry: Option<crate::coordination::LogicalExecutionRuntimeRegistry>,
        source: Option<Arc<OriginalLogicalExit>>,
        reservation: Option<crate::coordination::NativeActivationReservationObligation>,
        scripted_exit: Option<Box<dyn FnOnce() + Send>>,
        causes: FutureCauses,
    }
    struct KeepWhole(Option<Box<Originals>>);
    impl Drop for KeepWhole {
        fn drop(&mut self) {
            if std::thread::panicking() {
                static ORIGINALS: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                    std::sync::OnceLock::new();
                if let Some(original) = self.0.take() {
                    ORIGINALS
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(original);
                }
            }
        }
    }
    for ordinal in [1, 2] {
        let control = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            novarocks_workload_control::ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Query,
            ))
            .unwrap();
        let scope = work.owner.scope();
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
        let source = OriginalLogicalExit::new();
        let initial_schedule = schedule();
        let (registration, issued, scripted_exit) = crate::coordination::runtime_registry::runtime_tests::install_native_component_runtime_for_original(
            &registry.handle(), execution(1), initial_schedule.participations().to_vec(), Some(source.clone()),
        );
        // One initial Registry registration; the second return has the SAME
        // logical actor/query and its distinct original attempt/generation.
        let original_facts = crate::api::ExecutionSchedulingFacts {
            topological_fragment_order: vec![1],
            execution_anchor_fragment_id: 1,
            fragments: vec![crate::api::FragmentSchedulingFacts {
                fragment_id: 1,
                placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
                scans: vec![],
            }],
            edges: vec![],
        };
        let exact_schedule = build_attempt_schedule(
            execution(ordinal),
            initial_schedule.participations()[0].frontend_process_id(),
            &[initial_schedule.root().backend_process_id()],
            &original_facts,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap();
        assert_eq!(
            exact_schedule.participations()[0].frontend_process_id(),
            initial_schedule.participations()[0].frontend_process_id()
        );
        let expected = AttemptActivationIdentity::from_parts(
            issued.actor().get(),
            execution(ordinal),
            22 + ordinal,
        );
        let reservation = registry
            .handle()
            .reserve_active_native_owner(
                &registration,
                expected.execution(),
                exact_schedule.participations(),
            )
            .unwrap();
        let port_drops = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(OriginalReadPort {
            drops: port_drops.clone(),
            reads: reads.clone(),
        });
        let exact_port = Arc::downgrade(&port);
        let address = Arc::as_ptr(&port) as usize;
        let binding = crate::coordination::RootRelayBinding {
            port,
            frontier: crate::coordination::RootRelayFrontier::new(
                exact_schedule.root(),
                novarocks_result_contract::RootProfileId::V1,
                novarocks_result_contract::RootOutputKind::ClientRows,
                Some(
                    novarocks_result_contract::ClientRowProfile::try_new(
                        novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                        1 << 20,
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
            window: window.retain_alias(),
            max_wait: Duration::from_millis(1),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let owner_drops = Arc::new(AtomicUsize::new(0));
        let raw_drops = Arc::new(AtomicUsize::new(0));
        source.native.lock().await.dormant =
            crate::api::NativeDormantCustody::test_owner(OriginalRowsDormant {
                binding: Some(binding),
                calls: calls.clone(),
                owner_drops: owner_drops.clone(),
                raw_drops: raw_drops.clone(),
                ordinal,
            });
        let mut keep = KeepWhole(Some(Box::new(Originals {
            control: Some(control),
            work: Some(work),
            window: Some(window),
            registry: Some(registry),
            source: Some(source.clone()),
            reservation: Some(reservation),
            scripted_exit: Some(Box::new(scripted_exit)),
            causes: FutureCauses::default(),
        })));
        let original = keep.0.as_deref_mut().unwrap();
        {
            let mut body = source.native.lock().await;
            let OriginalNativePreparation {
                dormant,
                pending_activated,
                ..
            } = &mut *body;
            let (owner, raw_activation, _) = dormant.activation_parts().unwrap();
            let future = start_activation(
                owner,
                expected,
                &exact_schedule,
                None,
                scope.cancellation().unwrap(),
                raw_activation,
            )
            .unwrap();
            assert_eq!(
                poll_original(future, &original.causes, None, None, |result| {
                    original
                        .reservation
                        .as_ref()
                        .unwrap()
                        .receive_activated_in(pending_activated, result.unwrap());
                })
                .await,
                Ok(())
            );
            original
                .reservation
                .as_ref()
                .unwrap()
                .stage_activated_in(expected, exact_schedule.root(), pending_activated)
                .unwrap();
            assert_eq!(
                pending_activated.as_ref().unwrap().staged_execution(),
                Some(expected.execution())
            );
            assert!(
                pending_activated
                    .as_ref()
                    .unwrap()
                    .staged_slot_address()
                    .is_some()
            );
            let raw = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| dormant.retire()))
                .expect_err("actual original dormant destructor panics");
            original.causes.retain(raw, false);
            assert!(dormant.drop_unproven());
            assert!(dormant.activation_parts().is_none());
            assert_eq!(
                dormant.retire(),
                Err(()),
                "the same unproven destructor cannot be retried"
            );
            assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
        }
        drop(original.window.take());
        drop(original.reservation.take());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
        assert_eq!(port_drops.load(Ordering::SeqCst), 0);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            exact_port.upgrade().map(|p| Arc::as_ptr(&p) as usize),
            Some(address)
        );
        assert_eq!(capacity.snapshot().held_positions[0], 1);
        assert_eq!(
            original
                .causes
                .primary
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .downcast_ref::<OriginalDormantDrop>()
                .unwrap()
                .ordinal,
            ordinal
        );
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            original
                .registry
                .as_ref()
                .unwrap()
                .active_native_custody_count(),
            1
        );
        assert!(
            original
                .registry
                .as_mut()
                .unwrap()
                .shutdown_until(Instant::now())
                .await
                .is_err()
        );
        assert_eq!(
            source
                .native
                .lock()
                .await
                .pending_activated
                .as_ref()
                .unwrap()
                .staged_execution(),
            Some(expected.execution())
        );
        assert_eq!(
            port_drops.load(Ordering::SeqCst),
            0,
            "failed shutdown is not the original rows disposition"
        );
        assert_eq!(
            owner_drops.load(Ordering::SeqCst),
            1,
            "unproven original destructor is never retried"
        );
        // Task-free component process disposition only. No scripted Runtime
        // exit closure is called and no Native/AP Scope exit is asserted.
        original
            .registry
            .as_mut()
            .unwrap()
            .abandon_for_process_exit();
        drop(source.native.lock().await.pending_activated.take());
        assert_eq!(
            port_drops.load(Ordering::SeqCst),
            1,
            "same original Source rows are destroyed exactly once"
        );
        assert!(exact_port.upgrade().is_none());
        assert_eq!(capacity.snapshot().held_positions[0], 0);
        assert_eq!(raw_drops.load(Ordering::SeqCst), 0);
        drop(original.causes.primary.lock().unwrap().take());
        assert_eq!(raw_drops.load(Ordering::SeqCst), 1);
        drop(original.scripted_exit.take());
        drop(original.source.take());
        drop(source);
        drop(original.registry.take());
        let work = original.work.take().unwrap();
        work.business.release();
        work.owner.complete();
        scope.wait_released().await;
        original.control.as_ref().unwrap().close_admission();
        if let Err(failure) = original.control.take().unwrap().shutdown() {
            let (error, control) = failure.into_parts();
            original.control = Some(control);
            panic!("original return-custody component refused physical shutdown: {error}");
        }
    }
}

#[path = "tests/qualification_windows.rs"]
mod qualification_windows;
