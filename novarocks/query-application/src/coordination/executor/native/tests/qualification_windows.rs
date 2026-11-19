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
use crate::coordination::replacement::ReplacementQualificationEffectReservation;
use novarocks_workload_control::{RootWork, WorkloadControl};

#[derive(Debug)]
struct Port {
    capacity: Mutex<Option<watch::Sender<u64>>>,
    submission: Arc<Mutex<Option<ReplacementQualificationEffectSubmission>>>,
    calls: AtomicUsize,
    admitted: bool,
}
impl Port {
    fn new(admitted: bool) -> Arc<Self> {
        let (capacity, _) = watch::channel(0);
        Arc::new(Self {
            capacity: Mutex::new(Some(capacity)),
            submission: Arc::new(Mutex::new(None)),
            calls: AtomicUsize::new(0),
            admitted,
        })
    }
}
#[derive(Debug)]
struct Dispatch(Arc<Mutex<Option<ReplacementQualificationEffectSubmission>>>);
impl ReplacementQualificationEffectReservation for Dispatch {
    fn submit(self: Box<Self>, submission: ReplacementQualificationEffectSubmission) {
        let mut original = self.0.lock().unwrap();
        assert!(original.is_none(), "one original dispatch owns the receipt");
        *original = Some(submission);
    }
}
impl ReplacementQualificationEffectPort for Port {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity.lock().unwrap().as_ref().unwrap().subscribe()
    }
    fn try_reserve(
        &self,
        _: &ReplacementQualificationRequest,
    ) -> ReplacementQualificationEffectAdmission {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.admitted {
            ReplacementQualificationEffectAdmission::Admitted(Box::new(Dispatch(Arc::clone(
                &self.submission,
            ))))
        } else {
            ReplacementQualificationEffectAdmission::Backpressured
        }
    }
}

struct Originals {
    control: Option<WorkloadControl>,
    work: Option<RootWork>,
    registry: crate::coordination::LogicalExecutionRuntimeRegistry,
    registration: Option<crate::coordination::runtime_registry::LogicalExecutionRuntimeReservation>,
    native: Option<NativeExecutor>,
    body: Option<Arc<AsyncMutex<OriginalNativePreparation>>>,
    drops: Arc<AtomicUsize>,
    port: Arc<Port>,
    identity: QualificationIdentity,
    operation: TaskOperationId,
}
// An assertion or cancelled borrowed test wait retains the whole original set.
// It cannot turn unknown receipt/resource cleanup into an ordinary stack Drop.
struct Retained(Option<Box<Originals>>);
impl std::ops::Deref for Retained {
    type Target = Originals;
    fn deref(&self) -> &Originals {
        self.0.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for Retained {
    fn deref_mut(&mut self) -> &mut Originals {
        self.0.as_mut().unwrap()
    }
}
impl Drop for Retained {
    fn drop(&mut self) {
        if let Some(originals) = self.0.take() {
            if originals.control.is_none()
                && originals.work.is_none()
                && originals.native.is_none()
                && originals.body.is_none()
                && originals.registration.is_none()
            {
                drop(originals);
            } else {
                let _ = Box::leak(originals);
            }
        }
    }
}
impl Retained {
    fn new(admitted: bool) -> Self {
        use crate::api::{LogicalNativeOpenRequest, NativeLogicalExecutionSeed};
        use crate::preparation::{
            ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
            FrozenExecutionDescription, OutputContract,
        };
        use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let control = WorkloadControl::try_new(
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
        let drops = Arc::new(AtomicUsize::new(0));
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
        let session = accepted.accept(open.bind().unwrap()).unwrap();
        drop(window);
        let registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
        let handle = registry.handle();
        let mut registration = handle.reserve(execution(1), Vec::new()).unwrap();
        let initial = AttemptActivationIdentity::from_parts(
            registration.actor_instance().get(),
            execution(1),
            activation().generation(),
        );
        let identity = QualificationIdentity {
            actor: initial.actor(),
            ..identity()
        };
        let body = Arc::new(AsyncMutex::new(OriginalNativePreparation {
            session: Some(session),
            ..OriginalNativePreparation::default()
        }));
        let port = Port::new(admitted);
        let native = NativeExecutor::new(
            Some(port.clone()),
            Arc::new(super::super::UnboundInstalledAttemptPort),
            Arc::clone(&body),
            Weak::new(),
            handle,
            registration.runtime_registration(),
            initial,
            Arc::new(schedule()),
            FrontendProcessId::try_from_bytes(
                OperationIds::new(IdSeed::new(93, 100)).issue().to_bytes(),
            )
            .unwrap(),
            Arc::new(FixedClock(MonotonicInstant::ORIGIN)),
            scope.cancellation().unwrap(),
        );
        *native.lock() = prepared_qualification_storage();
        Self(Some(Box::new(Originals {
            control: Some(control),
            work: Some(work),
            registry,
            registration: Some(registration),
            native: Some(native),
            body: Some(body),
            drops,
            port,
            identity,
            operation: OperationIds::new(IdSeed::new(81, 50)).issue(),
        })))
    }
    fn effect(&self) -> NativeEffect {
        NativeEffect::DispatchQualification {
            identity: self.identity,
            operation_id: self.operation,
            failed_participations: Arc::from([]),
            replacement_participations: Arc::from([]),
            issued_at: MonotonicInstant::ORIGIN,
            expires_at: MonotonicInstant::from_origin(Duration::from_secs(40)),
        }
    }
    async fn finish(&mut self) {
        let native = self.native.as_ref().unwrap();
        assert!(matches!(
            native
                .execute(NativeEffect::AbandonReservation {
                    identity: self.identity
                })
                .await,
            NativeFact::Applied
        ));
        assert!(matches!(
            native.execute(NativeEffect::RetirePreparation).await,
            NativeFact::Applied
        ));
        drop(self.native.take());
        drop(self.body.take());
        drop(self.registration.take());
        self.registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let work = self.work.take().unwrap();
        let scope = work.owner.scope();
        work.business.release();
        while let Some(control) = self.control.as_ref().unwrap().next_control() {
            control.acknowledge();
        }
        work.owner.complete_after_terminal_cancel_settled();
        scope.wait_released().await;
        assert_eq!(
            self.control
                .as_ref()
                .unwrap()
                .snapshot()
                .result_windows
                .held_positions[0],
            0
        );
        assert_eq!(self.drops.load(Ordering::SeqCst), 1);
        let control = self.control.take().unwrap();
        control.close_admission();
        if let Err(failure) = control.shutdown() {
            let (error, original) = failure.into_parts();
            self.control = Some(original);
            panic!("original qualification fixture shutdown failed: {error:?}");
        }
    }
}

#[tokio::test]
async fn qualification_cancellation_keeps_same_submitted_receiver_until_late_qualified_cleanup() {
    let mut run = Retained::new(true);
    let native = run.native.as_ref().unwrap();
    let mut pending = Box::pin(native.execute(run.effect()));
    let first_pending = poll_consumer_script_once(pending.as_mut())
        .await
        .is_pending();
    let original_request = run
        .port
        .submission
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .request()
        .clone();
    run.work
        .as_ref()
        .unwrap()
        .owner
        .cancellation_requester()
        .request(CancellationReason::Requested)
        .unwrap();
    let cancelled_pending = poll_consumer_script_once(pending.as_mut())
        .await
        .is_pending();
    let (cancelled, unsettled) = {
        let storage = native.lock();
        let row = storage
            .qualifications
            .get(&run.identity.replacement)
            .unwrap();
        (
            row.cancellation_issued && *row.cancellation.borrow(),
            !row.dispatch_settled,
        )
    };
    let submission = run.port.submission.lock().unwrap().take().unwrap();
    let same_request = submission.request().operation_id() == run.operation
        && submission.request().identity() == original_request.identity();
    let submitted_cancel = submission.cancellation_requested();
    let finishes = Arc::new(AtomicUsize::new(0));
    let abandons = Arc::new(AtomicUsize::new(0));
    let qualified = QualifiedReplacementReservation::try_new(
        &original_request,
        Box::new(Reservation {
            identity: original_request.identity(),
            reject: false,
            finishes: Arc::clone(&finishes),
            abandons: Arc::clone(&abandons),
        }),
    )
    .unwrap();
    submission.qualified(qualified).unwrap();
    let result = pending.await;
    let settled = matches!(result, NativeFact::QualificationSettled {
        identity, operation_id, dispatched_at: Some(_), settlement: QualificationSettlement::Qualified(_),
    } if identity == run.identity && operation_id == run.operation);
    let retained = {
        let storage = native.lock();
        let row = storage
            .qualifications
            .get(&run.identity.replacement)
            .unwrap();
        row.dispatch_settled && row.qualified.is_some() && row.cancellation_issued
    };
    let before_cleanup = abandons.load(Ordering::SeqCst);
    run.finish().await;
    assert!(first_pending && cancelled_pending && cancelled && unsettled);
    assert!(same_request && submitted_cancel && settled && retained);
    assert_eq!(before_cleanup, 0);
    assert_eq!(abandons.load(Ordering::SeqCst), 1);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);
    assert_eq!(run.port.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn qualification_closed_capacity_watch_refuses_without_reserving_again() {
    let mut run = Retained::new(false);
    let mut pending = Box::pin(run.native.as_ref().unwrap().execute(run.effect()));
    let backpressured = poll_consumer_script_once(pending.as_mut())
        .await
        .is_pending();
    drop(run.port.capacity.lock().unwrap().take());
    let outcome = pending.await;
    let typed_closed = matches!(outcome, NativeFact::QualificationSettled {
        identity, operation_id, dispatched_at: None, settlement: QualificationSettlement::Rejected(error)
    } if identity == run.identity && operation_id == run.operation
        && error.execution_failure().is_some_and(|failure| failure.fact()
            == ReplacementQualificationFailure::EffectOwnerClosed.failure_fact()));
    let calls = run.port.calls.load(Ordering::SeqCst);
    let unsubmitted = run.port.submission.lock().unwrap().is_none();
    run.finish().await;
    assert!(backpressured && typed_closed && unsubmitted);
    assert_eq!(
        calls, 1,
        "a closed original watch cannot retry its reservation"
    );
}
