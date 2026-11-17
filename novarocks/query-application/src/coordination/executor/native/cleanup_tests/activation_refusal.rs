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

//! Ordinary callback refusal and the SAME actual dormant cleanup component.
//! The registry helper explicitly scripts runtime exit, never actual AP exit.
use super::*;
use std::time::Duration;

pub(super) fn original_input() -> novarocks_coordination_contract::ExecutionFailureInput {
    use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
    // Same original fake producer's closed fact/site/detail as baseline p2:14.
    novarocks_coordination_contract::ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(
                std::num::NonZeroU16::MAX,
                std::num::NonZeroU16::new(1).unwrap(),
            ),
        ),
        novarocks_execution_contract::SafeDetail::truncating("injected Native activation failure"),
        novarocks_coordination_contract::ExecutionFailureSite::Coordinator,
    )
}
pub(super) struct RefusalFuture {
    counts: Arc<Counts>,
    returned: bool,
}
impl RefusalFuture {
    pub(super) fn new(counts: Arc<Counts>) -> Self {
        Self {
            counts,
            returned: false,
        }
    }
}
impl Future for RefusalFuture {
    type Output = Result<crate::api::ActivatedNativeAttempt, NativeAttemptActivationFailure>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.returned,
            "the same completed callback must not be repolled"
        );
        self.returned = true;
        Poll::Ready(Err(NativeAttemptActivationFailure::new(
            crate::api::NativeAttemptPreparationFailure::from_input(original_input()),
        )))
    }
}
impl Drop for RefusalFuture {
    fn drop(&mut self) {
        self.counts
            .activation_future_drops
            .fetch_add(1, Ordering::SeqCst);
    }
}
struct Originals {
    native: Option<NativeExecutor>,
    body: Option<Arc<AsyncMutex<OriginalNativePreparation>>>,
    registry: Option<crate::coordination::LogicalExecutionRuntimeRegistry>,
    runtime_exit: Option<Box<dyn FnOnce() + Send>>,
    root: Option<RootWork>,
    control: Option<WorkloadControl>,
    activation_result: Option<NativeFact>,
    convergence_results: Vec<NativeFact>,
}
struct KeepWhole(Option<Box<Originals>>);
impl Drop for KeepWhole {
    fn drop(&mut self) {
        if std::thread::panicking() {
            static UNKNOWN: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> = OnceLock::new();
            if let Some(original) = self.0.take() {
                UNKNOWN
                    .get_or_init(Default::default)
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(original);
            }
        }
    }
}
#[tokio::test]
async fn ordinary_activation_refusal_converges_same_original_dormant_once_before_exact_registry_retirement()
 {
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
    let mut whole = KeepWhole(Some(Box::new(Originals {
        native: None,
        body: None,
        registry: None,
        runtime_exit: None,
        root: Some(root),
        control: Some(control),
        activation_result: None,
        convergence_results: Vec::new(),
    })));
    let original = whole.0.as_mut().unwrap();
    let execution = original_execution();
    let counts = Arc::new(Counts::default());
    let health = Arc::new(OriginalHealth(execution, counts.clone()));
    let frontend = FrontendProcessId::new_v7();
    let backends = vec![BackendProcessId::new_v7()];
    let frozen = scheduling();
    let schedule = Arc::new(
        build_attempt_schedule(
            execution,
            frontend,
            &backends,
            &frozen,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap(),
    );
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
    let seed = NativeLogicalExecutionSeed::issue(
        description.plan_identity(),
        None,
        OriginalPreparation {
            owner: Some(Box::new(OriginalDormant {
                execution,
                backends,
                script: CleanupScript::OrdinaryActivationRefusal,
                counts: counts.clone(),
                health,
            })),
            scheduling: Some(frozen),
            counts: counts.clone(),
            panic_after_success: false,
        },
    );
    let (open, accepted) = LogicalNativeOpenRequest::issue(
        execution,
        description,
        scope.id(),
        scope.cancellation().unwrap(),
        seed,
    );
    original.body = Some(Arc::new(AsyncMutex::new(OriginalNativePreparation {
        session: Some(accepted.accept(open.bind().unwrap()).unwrap()),
        ..OriginalNativePreparation::default()
    })));
    let body = original.body.as_ref().unwrap();
    let owner_address;
    {
        let mut same = body.lock().await;
        same.session
            .as_mut()
            .unwrap()
            .begin_preparation(execution, NativeAttemptTopologyRequirement::LiveSnapshot)
            .unwrap();
        let mut returned = None;
        let causes = FutureCauses::default();
        let outcome = poll_original(
            same.session.as_mut().unwrap().prepare_pending(),
            &causes,
            None,
            None,
            |value| returned = Some(value),
        )
        .await;
        assert!(matches!(returned, Some(Ok(()))));
        assert_eq!(outcome, Ok(()));
        assert!(
            !causes.failed(),
            "ordinary successful preparation does not inject a Drop panic"
        );
        let OriginalNativePreparation {
            session, dormant, ..
        } = &mut *same;
        let facts = session
            .as_mut()
            .unwrap()
            .accept_pending_in(dormant)
            .unwrap();
        assert_eq!(facts.execution, execution);
        owner_address = dormant.owner_address_for_cleanup().unwrap();
    }
    original.registry = Some(crate::coordination::LogicalExecutionRuntimeRegistry::new());
    let handle = original.registry.as_ref().unwrap().handle();
    let (registration, activation, runtime_exit) =
        crate::coordination::runtime_registry::runtime_tests::install_native_component_runtime(
            &handle,
            execution,
            schedule.participations().to_vec(),
        );
    original.runtime_exit = Some(Box::new(runtime_exit));
    original.native = Some(NativeExecutor::new(
        None,
        Arc::new(super::super::UnboundInstalledAttemptPort),
        body.clone(),
        Weak::new(),
        handle.clone(),
        registration.clone(),
        activation,
        schedule,
        frontend,
        Arc::new(Clock),
        scope.cancellation().unwrap(),
    ));
    let native = original.native.as_ref().unwrap();
    assert!(Arc::ptr_eq(&native.registration, &registration));
    original.activation_result = Some(
        native
            .execute(NativeEffect::ActivateAttempt {
                activation,
                inherited_admissions: vec![],
            })
            .await,
    );
    match original.activation_result.as_ref().unwrap() {
        NativeFact::Failed {
            execution: actual,
            failure,
        } => {
            assert_eq!(*actual, execution);
            assert_eq!(
                failure.error.message(),
                "injected Native activation failure"
            );
            assert_eq!(failure.error.execution_failure(), Some(&original_input()));
            assert_eq!(failure.error.kind(), QueryExecutionErrorKind::Failed);
        }
        _ => panic!("ordinary activation refusal must preserve its original typed author"),
    }
    assert_eq!(counts.preparation_factories.load(Ordering::SeqCst), 1);
    assert_eq!(counts.preparation_polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.bind_calls.load(Ordering::SeqCst), 0);
    assert_eq!(scope.cancellation().unwrap().reason(), None);
    assert_eq!(counts.activate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.activation_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        handle.active_native_custody_count(),
        0,
        "the unused original return reservation must not become an active owner"
    );
    assert!(
        native.lock().active.is_empty(),
        "no Run or installed active owner was created"
    );
    assert_eq!(
        body.lock().await.dormant.owner_address_for_cleanup(),
        Some(owner_address)
    );
    assert!(body.lock().await.pending_activated.is_none());
    for _ in 0..2 {
        original.convergence_results.push(
            native
                .execute(NativeEffect::ConvergeDormant {
                    execution,
                    remote_deadline: MonotonicInstant::from_origin(Duration::from_secs(1)),
                })
                .await,
        );
    }
    assert!(original.convergence_results.iter().all(|result|
        matches!(result, NativeFact::DormantConverged { execution: actual } if *actual == execution)));
    assert_eq!(counts.convergence_factories.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.convergence_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        body.lock().await.dormant.owner_address_for_cleanup(),
        Some(owner_address)
    );
    assert_eq!(
        counts.owner_drops.load(Ordering::SeqCst),
        0,
        "returned convergence is not the actual owner destructor"
    );
    assert!(matches!(
        native.execute(NativeEffect::RetirePreparation).await,
        NativeFact::Applied
    ));
    assert_eq!(counts.preparation_port_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.preparation_future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(counts.preparation_raw_drops.load(Ordering::SeqCst), 0);
    {
        let mut same = body.lock().await;
        assert!(same.dormant.convergence_completed());
        assert_eq!(same.dormant.retire(), Ok(()));
        assert_eq!(same.dormant.retire(), Ok(()));
    }
    assert_eq!(counts.owner_drops.load(Ordering::SeqCst), 1);
    assert!(!native.has_unhandled_failure());
    assert!(native.failure_source().is_none());
    // Actual physical owner cleanup cannot replace the original registry's
    // independent exit receipt, even in this explicit no-AP component.
    assert!(matches!(
        original
            .registry
            .as_mut()
            .unwrap()
            .shutdown_until(std::time::Instant::now())
            .await,
        Err(
            crate::coordination::LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
                remaining_entries: 1
            }
        )
    ));
    original.runtime_exit.take().unwrap()();
    original
        .registry
        .as_mut()
        .unwrap()
        .shutdown_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop(original.native.take());
    drop(original.body.take());
    drop(original.registry.take());
    let root = original.root.take().unwrap();
    root.business.release();
    root.owner.complete();
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
        let (error, same_control) = failure.into_parts();
        original.control = Some(same_control);
        panic!("the same ordinary-refusal component Control did not drain: {error:?}");
    }
}
