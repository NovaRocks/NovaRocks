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

//! Actual Session pending preparation and its original dormant cleanup owner.
use super::*;
use crate::api::{
    DormantNativeAttemptOwner, ExecutionSchedulingFacts, FragmentSchedulingFacts,
    LogicalNativeOpenRequest, NativeAttemptActivationFailure, NativeAttemptActivationFuture,
    NativeAttemptConvergenceFuture, NativeAttemptPreparationFuture, NativeAttemptPreparationPort,
    NativeAttemptPreparationWork, NativeAttemptTopologyRequirement, NativeExecutionContractError,
    NativeLogicalExecutionSeed, QueryExecutionKind, StatementNativeHealthFailure,
};
use crate::preparation::{
    ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
    FrozenExecutionDescription, OutputContract,
};
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
use novarocks_types::{AttemptId, BackendProcessId, QueryId};
use novarocks_workload_control::{
    ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupScript {
    Healthy,
    OrdinaryActivationRefusal,
    ReportedFailure,
    PollAndFutureDropPanic,
    ReportedFailureAndFutureDropPanic,
    BindingPollPanic,
    ConvergenceDropPanic,
    OwnerDropPanic,
}
#[derive(Debug, Default)]
struct Counts {
    preparation_factories: AtomicUsize,
    preparation_polls: AtomicUsize,
    preparation_future_drops: AtomicUsize,
    preparation_port_drops: AtomicUsize,
    preparation_raw_drops: AtomicUsize,
    bind_calls: AtomicUsize,
    activate_calls: AtomicUsize,
    activation_future_drops: AtomicUsize,
    convergence_factories: AtomicUsize,
    convergence_polls: AtomicUsize,
    convergence_future_drops: AtomicUsize,
    owner_drops: AtomicUsize,
    cleanup_raw_drops: AtomicUsize,
    poll_raw_drops: AtomicUsize,
    health_drops: AtomicUsize,
}
#[derive(Debug)]
struct RawOriginal {
    execution: QueryExecutionId,
    label: &'static str,
    counts: Arc<Counts>,
}
impl Drop for RawOriginal {
    fn drop(&mut self) {
        if self.label == "original preparation future Drop" {
            self.counts
                .preparation_raw_drops
                .fetch_add(1, Ordering::SeqCst);
        } else if self.label == "original dormant convergence poll"
            || self.label == "original qualification binding poll"
        {
            self.counts.poll_raw_drops.fetch_add(1, Ordering::SeqCst);
        } else {
            self.counts.cleanup_raw_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[derive(Debug)]
struct OriginalHealth(QueryExecutionId, Arc<Counts>);
impl fmt::Display for OriginalHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original dormant health source")
    }
}
impl Error for OriginalHealth {}
impl Drop for OriginalHealth {
    fn drop(&mut self) {
        self.1.health_drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct OriginalDormant {
    execution: QueryExecutionId,
    backends: Vec<BackendProcessId>,
    script: CleanupScript,
    counts: Arc<Counts>,
    health: Arc<OriginalHealth>,
}
impl DormantNativeAttemptOwner for OriginalDormant {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        &self.backends
    }
    fn poll_bind_qualification(
        &mut self,
        _: &AttemptSchedule,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
        self.counts.bind_calls.fetch_add(1, Ordering::SeqCst);
        if self.script == CleanupScript::BindingPollPanic {
            std::panic::panic_any(RawOriginal {
                execution: self.execution,
                label: "original qualification binding poll",
                counts: self.counts.clone(),
            });
        }
        panic!("cleanup must not bind a new qualification");
    }
    fn activate<'a>(
        &'a mut self,
        activation_identity: novarocks_coordination_contract::AttemptActivationIdentity,
        _: &'a AttemptSchedule,
        _: Option<Box<[crate::coordination::ReplacementWorkerAdmissionEvidence]>>,
        _: CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        self.counts.activate_calls.fetch_add(1, Ordering::SeqCst);
        if self.script == CleanupScript::OrdinaryActivationRefusal {
            assert_eq!(activation_identity.execution(), self.execution);
            return Box::pin(activation_refusal::RefusalFuture::new(self.counts.clone()));
        }
        panic!("cleanup must not activate the original dormant owner");
    }
    fn converge<'a>(&'a mut self, _: CancellationView) -> NativeAttemptConvergenceFuture<'a> {
        self.counts
            .convergence_factories
            .fetch_add(1, Ordering::SeqCst);
        Box::pin(OriginalConvergence {
            execution: self.execution,
            script: self.script,
            counts: self.counts.clone(),
            health: self.health.clone(),
            polled: false,
        })
    }
}
impl Drop for OriginalDormant {
    fn drop(&mut self) {
        self.counts.owner_drops.fetch_add(1, Ordering::SeqCst);
        if self.script == CleanupScript::OwnerDropPanic {
            std::panic::panic_any(RawOriginal {
                execution: self.execution,
                label: "original dormant owner Drop",
                counts: self.counts.clone(),
            });
        }
    }
}
struct OriginalConvergence {
    execution: QueryExecutionId,
    script: CleanupScript,
    counts: Arc<Counts>,
    health: Arc<OriginalHealth>,
    polled: bool,
}
impl Future for OriginalConvergence {
    type Output = Result<(), StatementNativeHealthFailure>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.get_mut();
        assert!(
            !original.polled,
            "a completed original convergence future must not be polled twice"
        );
        original.polled = true;
        original
            .counts
            .convergence_polls
            .fetch_add(1, Ordering::SeqCst);
        if original.script == CleanupScript::PollAndFutureDropPanic {
            std::panic::panic_any(RawOriginal {
                execution: original.execution,
                label: "original dormant convergence poll",
                counts: original.counts.clone(),
            });
        }
        Poll::Ready(
            if matches!(
                original.script,
                CleanupScript::ReportedFailure | CleanupScript::ReportedFailureAndFutureDropPanic
            ) {
                Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "original dormant reported failure",
                    original.health.clone(),
                ))
            } else {
                Ok(())
            },
        )
    }
}
impl Drop for OriginalConvergence {
    fn drop(&mut self) {
        self.counts
            .convergence_future_drops
            .fetch_add(1, Ordering::SeqCst);
        if matches!(
            self.script,
            CleanupScript::ConvergenceDropPanic
                | CleanupScript::PollAndFutureDropPanic
                | CleanupScript::ReportedFailureAndFutureDropPanic
        ) && self.polled
        {
            if self.script == CleanupScript::ReportedFailureAndFutureDropPanic {
                assert_eq!(
                    Arc::strong_count(&self.health),
                    4,
                    "the returned original health is retained before its future Drop"
                );
            }
            std::panic::panic_any(RawOriginal {
                execution: self.execution,
                label: "original dormant convergence future Drop",
                counts: self.counts.clone(),
            });
        }
    }
}
#[derive(Debug)]
struct OriginalPreparation {
    owner: Option<Box<dyn DormantNativeAttemptOwner>>,
    scheduling: Option<ExecutionSchedulingFacts>,
    counts: Arc<Counts>,
    panic_after_success: bool,
}
impl NativeAttemptPreparationPort for OriginalPreparation {
    fn prepare_in<'a>(
        &'a mut self,
        work: &'a mut NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        self.counts
            .preparation_factories
            .fetch_add(1, Ordering::SeqCst);
        let execution = work.execution();
        Box::pin(OriginalPreparationFuture {
            work,
            owner: &mut self.owner,
            scheduling: &mut self.scheduling,
            execution,
            counts: self.counts.clone(),
            ready: false,
            panic_after_success: self.panic_after_success,
        })
    }
}
impl Drop for OriginalPreparation {
    fn drop(&mut self) {
        self.counts
            .preparation_port_drops
            .fetch_add(1, Ordering::SeqCst);
    }
}
struct OriginalPreparationFuture<'a> {
    work: &'a mut NativeAttemptPreparationWork,
    owner: &'a mut Option<Box<dyn DormantNativeAttemptOwner>>,
    scheduling: &'a mut Option<ExecutionSchedulingFacts>,
    execution: QueryExecutionId,
    counts: Arc<Counts>,
    ready: bool,
    panic_after_success: bool,
}
impl Future for OriginalPreparationFuture<'_> {
    type Output = Result<(), crate::api::NativeAttemptPreparationError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.get_mut();
        assert!(!original.ready);
        original
            .counts
            .preparation_polls
            .fetch_add(1, Ordering::SeqCst);
        assert_eq!(original.work.execution(), original.execution);
        let result = original
            .work
            .bind_in(original.scheduling, original.owner)
            .map_err(Into::into);
        assert!(
            result.is_ok(),
            "the original sealed preparation must bind before its future Drop"
        );
        original.ready = true;
        Poll::Ready(result)
    }
}
impl Drop for OriginalPreparationFuture<'_> {
    fn drop(&mut self) {
        self.counts
            .preparation_future_drops
            .fetch_add(1, Ordering::SeqCst);
        if self.ready && self.panic_after_success {
            std::panic::panic_any(RawOriginal {
                execution: self.execution,
                label: "original preparation future Drop",
                counts: self.counts.clone(),
            });
        }
    }
}
#[derive(Debug)]
struct Clock;
impl MonotonicClock for Clock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::ORIGIN
    }
}
fn original_execution() -> QueryExecutionId {
    QueryExecutionId::new(QueryId::new(811, 812), AttemptId::new(4).unwrap()).unwrap()
}
fn scheduling() -> ExecutionSchedulingFacts {
    ExecutionSchedulingFacts {
        topological_fragment_order: vec![1],
        execution_anchor_fragment_id: 1,
        fragments: vec![FragmentSchedulingFacts {
            fragment_id: 1,
            placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
            scans: vec![],
        }],
        edges: vec![],
    }
}
struct OriginalFixture {
    native: Option<NativeExecutor>,
    body: Arc<AsyncMutex<OriginalNativePreparation>>,
    registry: Option<crate::coordination::LogicalExecutionRuntimeRegistry>,
    reservation: Option<crate::coordination::runtime_registry::LogicalExecutionRuntimeReservation>,
    root: Option<RootWork>,
    _control: WorkloadControl,
    counts: Arc<Counts>,
    health: Arc<OriginalHealth>,
    execution: QueryExecutionId,
    activation: AttemptActivationIdentity,
}
struct Fixture {
    original: Option<OriginalFixture>,
}
impl Fixture {
    async fn new(script: CleanupScript) -> Self {
        let execution = original_execution();
        let counts = Arc::new(Counts::default());
        let health = Arc::new(OriginalHealth(execution, counts.clone()));
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
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let plan = crate::completed_plan_fixture::completed_noop_plan([171; 16]);
        let description = Arc::new(
            FrozenExecutionDescription::for_completed_plan(
                QueryExecutionKind::Maintenance,
                plan.candidate().clone(),
                vec![],
                OutputContract::CompletionOnly,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                vec![],
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            )
            .unwrap(),
        );
        let backends = vec![BackendProcessId::new_v7()];
        let scheduling = scheduling();
        let frontend_process = FrontendProcessId::new_v7();
        let schedule = Arc::new(
            build_attempt_schedule(
                execution,
                frontend_process,
                &backends,
                &scheduling,
                #[cfg(novarocks_dev_controls)]
                Ok(None),
            )
            .unwrap(),
        );
        let seed = NativeLogicalExecutionSeed::issue(
            description.plan_identity(),
            None,
            OriginalPreparation {
                owner: Some(Box::new(OriginalDormant {
                    execution,
                    backends,
                    script,
                    counts: counts.clone(),
                    health: health.clone(),
                })),
                scheduling: Some(scheduling),
                counts: counts.clone(),
                panic_after_success: true,
            },
        );
        let (open, accepted) = LogicalNativeOpenRequest::issue(
            execution,
            description,
            scope.id(),
            scope.cancellation().unwrap(),
            seed,
        );
        let session = accepted.accept(open.bind().unwrap()).unwrap();
        let registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
        let mut reservation = registry
            .reserve(execution, schedule.participations().to_vec())
            .unwrap();
        let registration = reservation.runtime_registration();
        // This is the one original constructor argument, retained verbatim;
        // cleanup never rebuilds it from a later map key or registry generation.
        let activation = AttemptActivationIdentity::from_parts(
            reservation.actor_instance().get(),
            execution,
            47,
        );
        let body = Arc::new(AsyncMutex::new(OriginalNativePreparation {
            session: Some(session),
            ..OriginalNativePreparation::default()
        }));
        let native = NativeExecutor::new(
            None,
            Arc::new(super::UnboundInstalledAttemptPort),
            body.clone(),
            Weak::new(),
            registry.handle(),
            registration,
            activation,
            schedule,
            frontend_process,
            Arc::new(Clock),
            scope.cancellation().unwrap(),
        );
        let fixture = Self {
            original: Some(OriginalFixture {
                native: Some(native),
                body,
                registry: Some(registry),
                reservation: Some(reservation),
                root: Some(root),
                _control: control,
                counts,
                health,
                execution,
                activation,
            }),
        };
        let causes = fixture.native().causes(execution, 12);
        {
            let mut body = fixture.body.lock().await;
            let session = body.session.as_mut().unwrap();
            session
                .begin_preparation(execution, NativeAttemptTopologyRequirement::LiveSnapshot)
                .unwrap();
            let mut result = None;
            let outcome =
                poll_original(session.prepare_pending(), &causes, None, None, |original| {
                    result = Some(original)
                })
                .await;
            assert!(outcome.is_ok());
            assert!(matches!(result, Some(Ok(()))));
            assert_eq!(
                session.pending_prepared_execution_for_cleanup().unwrap(),
                Some(execution)
            );
        }
        assert_eq!(
            fixture.counts.preparation_factories.load(Ordering::SeqCst),
            1
        );
        assert_eq!(fixture.counts.preparation_polls.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .counts
                .preparation_future_drops
                .load(Ordering::SeqCst),
            1
        );
        fixture.assert_preparation_cause();
        fixture
    }
    fn native(&self) -> &NativeExecutor {
        self.native.as_ref().unwrap()
    }
    fn assert_preparation_cause(&self) {
        let causes = self.native().causes(self.execution, 12);
        let raw = causes.cleanup.lock().unwrap();
        let original = raw.as_ref().unwrap().downcast_ref::<RawOriginal>().unwrap();
        assert_eq!(original.execution, self.execution);
        assert_eq!(original.label, "original preparation future Drop");
        assert_eq!(self.counts.preparation_raw_drops.load(Ordering::SeqCst), 0);
        drop(raw);
        assert!(self.native().has_unhandled_failure());
        assert!(self.native().failure_source().is_some());
    }
    async fn pending_address(&self) -> usize {
        self.body
            .lock()
            .await
            .session
            .as_ref()
            .unwrap()
            .pending_prepared_owner_address_for_cleanup()
            .unwrap()
    }
    fn retain_unknown(mut self) {
        retain_original(self.original.take().unwrap());
    }
    async fn finish(mut self) {
        assert!(matches!(
            self.native().execute(NativeEffect::RetirePreparation).await,
            NativeFact::Applied
        ));
        assert_eq!(self.counts.preparation_port_drops.load(Ordering::SeqCst), 1);
        let original = self.original.as_mut().unwrap();
        drop(original.native.take());
        drop(original.reservation.take());
        if let Err((error, registry)) = original.registry.take().unwrap().shutdown() {
            original.registry = Some(registry);
            panic!("the original uninstalled registry must actually retire: {error:?}");
        }
        let root = original.root.take().unwrap();
        root.business.release();
        root.owner.complete();
    }

    /// Explicit disposal of this task-free component probe. This does not
    /// report a successful Native, AP Scope, remote participation or Work exit.
    async fn dispose_probe(mut self) {
        let OriginalFixture {
            native,
            body,
            registry,
            reservation,
            root,
            _control,
            ..
        } = self.original.take().unwrap();
        assert!(matches!(
            native
                .as_ref()
                .unwrap()
                .execute(NativeEffect::RetirePreparation)
                .await,
            NativeFact::Applied
        ));
        drop(native);
        drop(body);
        drop(reservation);
        assert!(registry.unwrap().shutdown().is_ok());
        let root = root.unwrap();
        root.business.release();
        root.owner.complete();
    }
}
impl Deref for Fixture {
    type Target = OriginalFixture;
    fn deref(&self) -> &Self::Target {
        self.original.as_ref().unwrap()
    }
}
impl DerefMut for Fixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.original.as_mut().unwrap()
    }
}
fn retain_original(original: OriginalFixture) {
    static UNKNOWN: OnceLock<Mutex<Vec<OriginalFixture>>> = OnceLock::new();
    UNKNOWN
        .get_or_init(|| Mutex::new(vec![]))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(original);
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Some(original) = self.original.take()
        {
            retain_original(original);
        }
    }
}

#[tokio::test]
async fn exact_session_cleanup_rejects_foreign_full_execution_before_moving_original_pending_owner()
{
    let f = Fixture::new(CleanupScript::Healthy).await;
    let address = f.pending_address().await;
    let same_attempt_other_query =
        QueryExecutionId::new(QueryId::new(811, 813), f.execution.attempt_id()).unwrap();
    let same_query_other_attempt =
        QueryExecutionId::new(f.execution.query_id(), AttemptId::new(5).unwrap()).unwrap();
    assert_eq!(
        same_attempt_other_query.attempt_id(),
        f.execution.attempt_id()
    );
    for foreign in [same_attempt_other_query, same_query_other_attempt] {
        let mut body = f.body.lock().await;
        let OriginalNativePreparation {
            session, dormant, ..
        } = &mut *body;
        assert!(matches!(
            session
                .as_mut()
                .unwrap()
                .accept_pending_for_cleanup(foreign, dormant),
            Err(NativeExecutionContractError::ForeignAttemptTicket)
        ));
        assert_eq!(
            session
                .as_ref()
                .unwrap()
                .pending_prepared_owner_address_for_cleanup(),
            Some(address)
        );
        assert!(!dormant.has_owner_for_cleanup());
    }
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(f.execution)
            .await
            .unwrap()
    );
    assert_eq!(
        f.body.lock().await.dormant.owner_address_for_cleanup(),
        Some(address)
    );
    f.native()
        .converge_dormant_for_owner_failure(f.execution)
        .await
        .unwrap();
    f.finish().await;
}
#[tokio::test]
async fn native_adoption_rejects_another_full_execution_without_consuming_or_recreating_preparation()
 {
    let f = Fixture::new(CleanupScript::Healthy).await;
    let address = f.pending_address().await;
    let foreign = QueryExecutionId::new(QueryId::new(811, 813), f.execution.attempt_id()).unwrap();
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(foreign)
            .await
            .is_err()
    );
    assert_eq!(f.pending_address().await, address);
    assert_eq!(f.counts.preparation_factories.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.native().cleanup_inventory().unwrap().initial_activation,
        f.activation
    );
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(f.execution)
            .await
            .unwrap()
    );
    f.native()
        .converge_dormant_for_owner_failure(f.execution)
        .await
        .unwrap();
    f.finish().await;
}
#[tokio::test]
async fn pending_success_after_preparation_future_drop_panic_converges_same_owner_once_and_retains_old_cause()
 {
    let f = Fixture::new(CleanupScript::Healthy).await;
    let address = f.pending_address().await;
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(f.execution)
            .await
            .unwrap()
    );
    assert_eq!(
        f.body.lock().await.dormant.owner_address_for_cleanup(),
        Some(address)
    );
    f.native()
        .converge_dormant_for_owner_failure(f.execution)
        .await
        .unwrap();
    f.native()
        .converge_dormant_for_owner_failure(f.execution)
        .await
        .unwrap();
    assert_eq!(f.counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.convergence_polls.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.convergence_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.preparation_factories.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.activate_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.bind_calls.load(Ordering::SeqCst), 0);
    assert!(f.body.lock().await.dormant.owner_retired_for_cleanup());
    assert_eq!(
        f.native().lock().cleaned_dormant_execution,
        Some(f.execution)
    );
    f.assert_preparation_cause();
    assert!(matches!(
        f.native().execute(NativeEffect::RetirePreparation).await,
        NativeFact::Applied
    ));
    {
        let body = f.body.lock().await;
        assert_eq!(
            f.native().qualify_original_body_cleanup(&body),
            Err(crate::coordination::original_exit::OriginalNativeBodyCloseRefusal::BodyUnproven),
            "known output Drop does not prove its original future destructor returned"
        );
    }
    f.finish().await;
}
#[tokio::test]
async fn original_reported_dormant_cleanup_failure_retains_typed_cause_same_box_and_does_not_retry()
{
    let f = Fixture::new(CleanupScript::ReportedFailure).await;
    let address = f.pending_address().await;
    for _ in 0..2 {
        assert!(
            f.native()
                .converge_dormant_for_owner_failure(f.execution)
                .await
                .is_err()
        );
    }
    let body = f.body.lock().await;
    assert_eq!(body.dormant.owner_address_for_cleanup(), Some(address));
    let failure = body.dormant.convergence_failure().unwrap();
    assert_eq!(failure.detail(), "original dormant reported failure");
    let original = failure.cause().downcast_ref::<OriginalHealth>().unwrap();
    assert!(std::ptr::eq(original, f.health.as_ref()));
    assert_eq!(original.0, f.execution);
    assert!(!body.dormant.owner_retired_for_cleanup());
    drop(body);
    assert_eq!(f.counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.owner_drops.load(Ordering::SeqCst), 0);
    assert!(f.native().lock().cleaned_dormant_execution.is_none());
    f.assert_preparation_cause();
    f.retain_unknown();
}
#[tokio::test]
async fn original_convergence_future_drop_panic_retains_raw_cleanup_cause_and_unknown_owner_without_retry()
 {
    let f = Fixture::new(CleanupScript::ConvergenceDropPanic).await;
    let address = f.pending_address().await;
    assert!(
        f.native()
            .converge_dormant_for_owner_failure(f.execution)
            .await
            .is_err()
    );
    let causes = f.native().causes(f.execution, 25);
    let pointer = {
        let raw = causes.cleanup.lock().unwrap();
        let original = raw.as_ref().unwrap().downcast_ref::<RawOriginal>().unwrap();
        assert_eq!(original.execution, f.execution);
        assert_eq!(original.label, "original dormant convergence future Drop");
        original as *const RawOriginal as usize
    };
    assert!(
        f.native()
            .converge_dormant_for_owner_failure(f.execution)
            .await
            .is_err()
    );
    assert_eq!(
        f.body.lock().await.dormant.owner_address_for_cleanup(),
        Some(address)
    );
    assert_eq!(
        causes
            .cleanup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<RawOriginal>()
            .unwrap() as *const RawOriginal as usize,
        pointer
    );
    assert_eq!(f.counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.convergence_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.owner_drops.load(Ordering::SeqCst), 0);
    assert_eq!(f.counts.cleanup_raw_drops.load(Ordering::SeqCst), 0);
    assert!(f.native().lock().cleaned_dormant_execution.is_none());
    f.assert_preparation_cause();
    f.retain_unknown();
}
#[tokio::test]
async fn actual_dormant_owner_drop_panic_stays_unknown_and_is_not_repeated_or_reported_cleaned() {
    let f = Fixture::new(CleanupScript::OwnerDropPanic).await;
    assert!(
        f.native()
            .converge_dormant_for_owner_failure(f.execution)
            .await
            .is_err()
    );
    let causes = f.native().causes(f.execution, 26);
    let pointer = {
        let raw = causes.cleanup.lock().unwrap();
        let original = raw.as_ref().unwrap().downcast_ref::<RawOriginal>().unwrap();
        assert_eq!(original.execution, f.execution);
        assert_eq!(original.label, "original dormant owner Drop");
        original as *const RawOriginal as usize
    };
    assert!(
        f.native()
            .converge_dormant_for_owner_failure(f.execution)
            .await
            .is_err()
    );
    let body = f.body.lock().await;
    assert!(body.dormant.drop_unproven());
    assert!(!body.dormant.owner_retired_for_cleanup());
    drop(body);
    assert_eq!(f.counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(f.counts.cleanup_raw_drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        causes
            .cleanup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<RawOriginal>()
            .unwrap() as *const RawOriginal as usize,
        pointer
    );
    assert!(f.native().lock().cleaned_dormant_execution.is_none());
    f.assert_preparation_cause();
    f.retain_unknown();
}

async fn assert_normal_dormant_dual_failure(script: CleanupScript) {
    let f = Fixture::new(script).await;
    let address = f.pending_address().await;
    let counts = f.counts.clone();
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(f.execution)
            .await
            .unwrap()
    );
    let effect = NativeEffect::ConvergeDormant {
        execution: f.execution,
        remote_deadline: MonotonicInstant::from_origin(std::time::Duration::from_secs(5)),
    };
    let NativeFact::Failed { failure, .. } = f.native().execute(effect.clone()).await else {
        panic!("the same failed original cannot report DormantConverged");
    };
    let health = f.health.clone();
    let causes = f.native().causes(f.execution, 6);
    let poll_address = {
        let primary = causes.primary.lock().unwrap();
        if script == CleanupScript::PollAndFutureDropPanic {
            let raw = primary
                .as_ref()
                .unwrap()
                .downcast_ref::<RawOriginal>()
                .unwrap();
            assert_eq!(raw.execution, f.execution);
            assert_eq!(raw.label, "original dormant convergence poll");
            Some(raw as *const RawOriginal as usize)
        } else {
            assert!(primary.is_none());
            None
        }
    };
    let drop_address = {
        let cleanup = causes.cleanup.lock().unwrap();
        let raw = cleanup
            .as_ref()
            .unwrap()
            .downcast_ref::<RawOriginal>()
            .unwrap();
        assert_eq!(raw.execution, f.execution);
        assert_eq!(raw.label, "original dormant convergence future Drop");
        raw as *const RawOriginal as usize
    };
    {
        let mut body = f.body.lock().await;
        assert_eq!(body.dormant.owner_address_for_cleanup(), Some(address));
        if script == CleanupScript::ReportedFailureAndFutureDropPanic {
            let original = body.dormant.convergence_failure().unwrap();
            assert!(std::ptr::eq(
                original.cause().downcast_ref::<OriginalHealth>().unwrap(),
                health.as_ref()
            ));
            assert_eq!(original.detail(), "original dormant reported failure");
            assert_eq!(failure.error.message(), original.detail());
            assert!(body.dormant.convergence_panic().is_none());
        }
        assert!(
            body.dormant.convergence_parts().is_none(),
            "the failed original convergence must be unavailable before any retry"
        );
        assert!(body.dormant.retire().is_err());
        assert!(!body.dormant.convergence_completed());
    }
    let NativeFact::Failed { failure, .. } = f.native().execute(effect).await else {
        panic!("a repeat cannot erase the original failed convergence");
    };
    if script == CleanupScript::ReportedFailureAndFutureDropPanic {
        assert_eq!(failure.error.message(), "original dormant reported failure");
    }
    assert_eq!(counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.poll_raw_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.cleanup_raw_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.health_drops.load(Ordering::SeqCst), 0);
    assert!(f.native().lock().cleaned_dormant_execution.is_none());
    assert_eq!(
        f.body.lock().await.dormant.owner_address_for_cleanup(),
        Some(address)
    );
    assert_eq!(
        causes.primary.lock().unwrap().as_ref().map(|raw| {
            raw.downcast_ref::<RawOriginal>().unwrap() as *const RawOriginal as usize
        }),
        poll_address
    );
    assert_eq!(
        causes
            .cleanup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<RawOriginal>()
            .unwrap() as *const RawOriginal as usize,
        drop_address
    );
    f.assert_preparation_cause();
    drop(causes);
    f.dispose_probe().await;
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.cleanup_raw_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        counts.poll_raw_drops.load(Ordering::SeqCst),
        usize::from(poll_address.is_some())
    );
    assert_eq!(counts.preparation_raw_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.health_drops.load(Ordering::SeqCst), 0);
    drop(health);
    assert_eq!(counts.health_drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn normal_dormant_poll_and_same_future_drop_failure_keep_both_originals_without_retry() {
    assert_normal_dormant_dual_failure(CleanupScript::PollAndFutureDropPanic).await;
}

#[tokio::test]
async fn normal_dormant_reported_and_same_future_drop_failure_keep_original_typed_cause_without_retry()
 {
    assert_normal_dormant_dual_failure(CleanupScript::ReportedFailureAndFutureDropPanic).await;
}

#[tokio::test]
async fn original_binding_poll_panic_survives_same_owner_normal_convergence_and_refuses_retirement()
{
    let f = Fixture::new(CleanupScript::BindingPollPanic).await;
    let address = f.pending_address().await;
    let counts = f.counts.clone();
    assert!(
        f.native()
            .adopt_pending_preparation_for_cleanup(f.execution)
            .await
            .unwrap()
    );
    let schedule = f.native().lock().prepared[&f.execution].schedule.clone();
    let cancellation = f
        .root
        .as_ref()
        .unwrap()
        .owner
        .scope()
        .cancellation()
        .unwrap();
    let binding_causes = f.native().causes(f.execution, 3);
    let raw_address = {
        let mut body = f.body.lock().await;
        for _ in 0..2 {
            assert!(
                bind_original_schedule(
                    &mut body.dormant,
                    &schedule,
                    &cancellation,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(1),
                    &binding_causes,
                )
                .await
                .is_err()
            );
        }
        let raw = body
            .dormant
            .activation_panic()
            .unwrap()
            .downcast_ref::<RawOriginal>()
            .unwrap();
        assert_eq!(raw.execution, f.execution);
        assert_eq!(raw.label, "original qualification binding poll");
        raw as *const RawOriginal as usize
    };
    assert!(!binding_causes.failed());
    assert_eq!(counts.bind_calls.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        assert!(matches!(
            f.native()
                .execute(NativeEffect::ConvergeDormant {
                    execution: f.execution,
                    remote_deadline: MonotonicInstant::ORIGIN,
                })
                .await,
            NativeFact::Failed { .. }
        ));
    }
    {
        let mut body = f.body.lock().await;
        assert_eq!(body.dormant.owner_address_for_cleanup(), Some(address));
        assert_eq!(
            body.dormant
                .activation_panic()
                .unwrap()
                .downcast_ref::<RawOriginal>()
                .unwrap() as *const RawOriginal as usize,
            raw_address
        );
        assert!(body.dormant.convergence_completed());
        assert!(body.dormant.retire().is_err());
    }
    assert_eq!(counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.activate_calls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 0);
    assert_eq!(counts.poll_raw_drops.load(Ordering::SeqCst), 0);
    drop(binding_causes);
    drop(cancellation);
    f.dispose_probe().await;
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.poll_raw_drops.load(Ordering::SeqCst), 1);
}

mod activation_refusal;
