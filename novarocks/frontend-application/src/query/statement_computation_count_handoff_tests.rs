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

// Included in the existing helper tests. The original supervisor mints both
// Native exit observers. Ready/local-exit helper preconditions are explicitly
// recorded here, as in the adjacent local-join test; this does not prove SQL
// computation Ready, real DML receiving, or a distributed Native topology.
mod count_handoff_tests {
    use super::*;
    use futures::FutureExt;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_query_application::{
        api::{
            LogicalExecutionNativePort, LogicalNativeOpenRequest, LogicalNativeSession,
            NativeAttemptPreparationFailure, NativeAttemptPreparationFuture,
            NativeAttemptPreparationPort, NativeAttemptPreparationWork, QueryExecutionKind,
            QueryExecutionRequest,
        },
        coordination::{
            AbortQueryContextEffectAdmission, AbortQueryContextEffectPort,
            AbortQueryContextIssueIdentity, LogicalExecutionRowsConfig,
            LogicalExecutionSupervisorConfig,
        },
        execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
        preparation::{
            CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
            FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
        },
        session_error::QueryServiceError,
    };
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
    use novarocks_workload_control::{WorkClass, WorkRequest};
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::{Duration, Instant},
    };

    #[derive(Debug)]
    struct OriginalNativePort;
    impl LogicalExecutionNativePort for OriginalNativePort {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
            LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
        }
    }
    #[derive(Debug, Default)]
    struct PreparationGate {
        entered: AtomicBool,
        returned: AtomicBool,
        frame_dropped: AtomicBool,
        release: tokio::sync::Notify,
    }
    struct ReleaseOnExit(Vec<Arc<PreparationGate>>);
    impl Drop for ReleaseOnExit {
        fn drop(&mut self) {
            for gate in &self.0 {
                gate.release.notify_one();
            }
        }
    }
    struct OriginalPreparationFrame(Arc<PreparationGate>);
    impl Drop for OriginalPreparationFrame {
        fn drop(&mut self) {
            self.0.frame_dropped.store(true, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct HeldOriginalPreparation(Arc<PreparationGate>);
    impl NativeAttemptPreparationPort for HeldOriginalPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            _: &'a mut NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let gate = Arc::clone(&self.0);
            Box::pin(async move {
                let _frame = OriginalPreparationFrame(Arc::clone(&gate));
                gate.entered.store(true, Ordering::SeqCst);
                gate.release.notified().await;
                gate.returned.store(true, Ordering::SeqCst);
                Err(
                    NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
                        QueryFailureReason::PreparationFailed.failure_fact(),
                        novarocks_execution_contract::SafeDetail::truncating(
                            "original held preparation refused after actual release",
                        ),
                        ExecutionFailureSite::Coordinator,
                    ))
                    .into(),
                )
            })
        }
    }
    // The held preparation never binds a backend or starts a context. Any
    // Abort reservation would contradict this fixture's actual execution.
    #[derive(Debug)]
    struct NoContextAborts(tokio::sync::watch::Sender<u64>);
    impl AbortQueryContextEffectPort for NoContextAborts {
        fn subscribe_capacity(&self) -> tokio::sync::watch::Receiver<u64> {
            self.0.subscribe()
        }
        fn try_reserve(
            &self,
            _: AbortQueryContextIssueIdentity,
        ) -> AbortQueryContextEffectAdmission {
            panic!("unbound preparation cannot request a context Abort")
        }
    }
    fn original_request(gate: Arc<PreparationGate>) -> QueryExecutionRequest {
        use novarocks_physical_plan::{
            FragmentBuilder, FragmentId, FragmentSink, PipelineDopDomain, PlanBuilder,
            PlanVersionId, StagePlacementPolicy,
        };
        let mut fragment = FragmentBuilder::new(FragmentId::new(0));
        let root = fragment.reserve_node_id().unwrap();
        fragment
            .add_values(root, Box::default(), Box::default())
            .unwrap();
        let fragment = fragment
            .finish_definition(
                root,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: true,
                },
                StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let mut plan = PlanBuilder::new(PlanVersionId::try_new([199; 16]).unwrap());
        plan.add_fragment(fragment).unwrap();
        let candidate = CompletedPhysicalPlanCandidate::for_program(
            plan.finish().unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let description = FrozenExecutionDescription::for_completed_plan(
            QueryExecutionKind::Maintenance,
            candidate,
            Vec::new(),
            OutputContract::CompletionOnly,
            novarocks_coordination_contract::ExecutionEffect::None,
            novarocks_coordination_contract::RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        let (capacity, _) = tokio::sync::watch::channel(0);
        QueryExecutionRequest::bind_native(
            description,
            Arc::new(NoContextAborts(capacity)),
            None,
            HeldOriginalPreparation(gate),
        )
    }
    async fn wait_entered(gate: &PreparationGate) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !gate.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original supervisor must poll its Native preparation");
    }
    fn helper_for_original(
        scope: WorkScope,
        original: OriginalComputationStart,
    ) -> StatementComputation<OriginalCpuAssembly> {
        StatementComputation {
            admission: StatementStageAdmission::new(scope, 0),
            initial: None,
            assembly: None,
            computation: None,
            output: None,
            original,
            execution: None,
            ready: None,
            exit_receipt: None,
            close_failure: None,
            // Component boundary setup only. The held preparation is not a
            // production Ready witness and supplies no computation capacity.
            ready_confirmed: true,
            local_exit_observed: true,
            native_tail_taken: false,
            busy: false,
            launcher: Arc::new(NoNativeLaunch),
            #[cfg(novarocks_dev_controls)]
            observation: None,
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn count_refusal_occupied_repeat_and_panic_preserve_actual_native_observers() {
        let (mut owner, client) = crate::query_execution::logical_execution_test_profile::process(
            tokio::runtime::Handle::current(),
            Arc::new(OriginalNativePort),
            QueryProcessNamespace::new(0x9c3),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(5),
                ),
            ),
        );
        let mut release_on_exit = ReleaseOnExit(Vec::new());
        let mut helpers = Vec::new();
        let mut businesses = Vec::new();
        let mut replies = Vec::new();
        let mut gates = Vec::new();
        for _ in 0..2 {
            let root = owner
                .control()
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            owner.bind_local_root(&root.owner).await;
            let scope = root.owner.scope();
            let gate = Arc::new(PreparationGate::default());
            release_on_exit.0.push(Arc::clone(&gate));
            let mut input = Some(
                client
                    .prepare_observed_start(original_request(Arc::clone(&gate)), None)
                    .unwrap_or_else(|(error, _, _)| panic!("original preparation: {error}")),
            );
            let mut work = Some(root.owner);
            let mut original = OriginalComputationStart::default();
            client
                .start_observed_in(
                    &mut input,
                    &mut work,
                    &mut original.ready,
                    &mut original.exit,
                )
                .unwrap()
                .finish()
                .unwrap();
            wait_entered(&gate).await;
            replies.push(original.ready.take().unwrap());
            businesses.push(root.business);
            gates.push(gate);
            helpers.push(helper_for_original(scope, original));
        }
        let mut target = None;
        let mut second_target = None;
        let mut no_session_residual = None;
        let checks = AssertUnwindSafe(async {
            let cause = SharedFailureCause::new(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "original count observation refused",
            ));
            let refusal = QueryServiceError::from_failure_cause(cause.clone());
            let result =
                helpers[0].transfer_native_tail_then(&mut target, || Err::<u64, _>(refusal));
            let failure = result
                .expect("domain refusal is not Native health")
                .unwrap_err();
            assert_eq!(failure.failure_cause(), Some(&cause));
            assert_eq!(
                failure.execution_failure().unwrap().fact(),
                cause.failure_fact()
            );
            assert!(helpers[0].original.exit.is_none());
            assert!(
                target
                    .as_mut()
                    .unwrap()
                    .poll_exit(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            assert!(!gates[0].returned.load(Ordering::SeqCst));
            let calls = AtomicUsize::new(0);
            let occupied = helpers[1].transfer_native_tail_then(&mut target, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, QueryServiceError>(0_u64)
            });
            assert!(occupied.is_err());
            assert!(
                helpers[1].original.exit.is_some(),
                "occupied target cannot consume the second original"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = helpers[1].transfer_native_tail_then::<u64, QueryServiceError>(
                    &mut second_target,
                    || panic!("original count observer panicked"),
                );
            }));
            assert!(panic.is_err());
            assert!(helpers[1].original.exit.is_none());
            assert!(
                second_target
                    .as_mut()
                    .unwrap()
                    .poll_exit(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            let repeated = helpers[1].transfer_native_tail_then(&mut second_target, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, QueryServiceError>(0_u64)
            });
            assert!(repeated.is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let mut empty = None;
            let repeated_empty = helpers[1].transfer_native_tail_then(&mut empty, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, QueryServiceError>(0_u64)
            });
            assert!(repeated_empty.is_err());
            assert!(empty.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            no_session_residual = Some(
                crate::query::write_action::tests::residual_without_session(target.take().unwrap()),
            );
            assert!(
                no_session_residual
                    .as_mut()
                    .unwrap()
                    .poll_retirement(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending(),
                "a missing session cannot erase the held Native tail"
            );
            assert!(!gates[0].returned.load(Ordering::SeqCst));
        })
        .catch_unwind()
        .await;
        // Always release and observe the exact original frames before any
        // assertion is rethrown or the fixture's original runtime shuts down.
        for gate in &gates {
            gate.release.notify_one();
        }
        for reply in replies {
            assert!(
                tokio::time::timeout(Duration::from_secs(5), reply)
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        for helper in &mut helpers {
            if let Some(exit) = helper.original.exit.as_mut() {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    std::future::poll_fn(|cx| exit.poll_exit(cx)),
                )
                .await
                .unwrap()
                .unwrap();
            }
        }
        for tail in [&mut target, &mut second_target] {
            if let Some(tail) = tail.as_mut() {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    std::future::poll_fn(|cx| tail.poll_exit(cx)),
                )
                .await
                .unwrap()
                .unwrap();
            }
        }
        if let Some(residual) = no_session_residual.as_mut() {
            tokio::time::timeout(
                Duration::from_secs(5),
                std::future::poll_fn(|cx| residual.poll_retirement(cx)),
            )
            .await
            .unwrap()
            .unwrap();
        }
        assert!(gates.iter().all(|gate| gate.returned.load(Ordering::SeqCst)
            && gate.frame_dropped.load(Ordering::SeqCst)));
        drop((helpers, target, second_target, no_session_residual));
        for business in businesses {
            business.release();
        }
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        if let Err(original) = checks {
            std::panic::resume_unwind(original);
        }
    }
}
