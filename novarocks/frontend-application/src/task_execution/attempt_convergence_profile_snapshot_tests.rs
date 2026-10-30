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

// Included inside diagnostic_exit_tests; this uses the actual QA actor test harness.
mod profile_snapshot_tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};

    use crate::task_execution::profile_diagnostics::{
        ProfileDiagnosticClosure, ProfileDiagnosticMissing, ProfileSplitDiagnostic,
        ProfileTaskDiagnostic,
    };
    use novarocks_coordination_contract::{ExecutionEffect, MonotonicClock, RecoveryMode};
    use novarocks_execution_contract::FinalTaskInfo;
    use novarocks_execution_contract::root_result::RootResultEnd;
    use novarocks_query_application::api::{ComputationObservationBudgets, QueryExecutionKind};
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, LogicalExecutionActorConfig,
    };
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
        FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
    };
    use novarocks_query_application::test_support::LogicalExecutionTestHarness;

    fn internal_count_description() -> FrozenExecutionDescription {
        use novarocks_physical_plan::{
            FragmentBuilder, FragmentId, FragmentSink, OutputPort, PipelineDopDomain, PlanBuilder,
            PlanVersionId, ResultPort,
        };
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        // Diagnostics are carrier-independent. A validated zero-row CountOnly
        // program keeps this test focused on the real actor End/Ready boundary.
        let fragment_id = FragmentId::new(0);
        let mut fragment = FragmentBuilder::new(fragment_id);
        let node = fragment.reserve_node_id().unwrap();
        fragment
            .add_values(node, Box::default(), Box::default())
            .unwrap();
        let fragment = fragment
            .finish_definition(
                node,
                FragmentSink::RootResult(Box::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly,
                ))),
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: true,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let mut plan = PlanBuilder::new(PlanVersionId::try_new([151; 16]).unwrap());
        plan.add_fragment(fragment).unwrap();
        plan.set_result_port(ResultPort {
            scalar_schema: None,
            fragment: fragment_id,
            output: OutputPort {
                node,
                columns: Box::default(),
            },
            fields: Box::default(),
        })
        .unwrap();
        let candidate = CompletedPhysicalPlanCandidate::for_program(
            plan.finish().unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let output =
            OutputContract::from_completed_candidate(QueryExecutionKind::Statistics, &candidate)
                .unwrap();
        FrozenExecutionDescription::for_completed_plan(
            QueryExecutionKind::Statistics,
            candidate,
            Vec::new(),
            output,
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap()
        .into_internal_computation()
        .unwrap()
    }

    struct FirstAvailableThenGated {
        first: Mutex<Option<FinalTaskInfo>>,
        pending: GatedOriginalFinalInfo,
    }

    impl TaskResultTransport for FirstAvailableThenGated {
        fn final_task_info(
            &self,
            task: TaskIdentity,
        ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
            if let Some(info) = self.first.lock().unwrap().take() {
                assert_eq!(info.final_status().identity(), task);
                return Box::pin(async move { Ok(FinalTaskInfoRead::Available(Box::new(info))) });
            }
            self.pending.final_task_info(task)
        }

        fn dynamic_filters(
            &self,
            _: TaskIdentity,
            _: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            panic!("profile diagnostics must not fetch dynamic filters")
        }
    }

    struct OriginalAvailableFinalInfo {
        info: Mutex<std::collections::BTreeMap<TaskIdentity, FinalTaskInfo>>,
        started: Arc<Mutex<Vec<TaskIdentity>>>,
    }

    impl TaskResultTransport for OriginalAvailableFinalInfo {
        fn final_task_info(
            &self,
            task: TaskIdentity,
        ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
            self.started.lock().unwrap().push(task);
            let info = self.info.lock().unwrap().remove(&task);
            Box::pin(async move {
                info.map(|info| FinalTaskInfoRead::Available(Box::new(info)))
                    .ok_or_else(|| "the original final-info fixture was read twice".to_owned())
            })
        }

        fn dynamic_filters(
            &self,
            _: TaskIdentity,
            _: Option<DomainVersion>,
        ) -> Result<DynamicFilterRead, DynamicFilterReadError> {
            panic!("profile diagnostics must not fetch dynamic filters")
        }
    }

    async fn original_ready_profile_snapshot_fixture(final_info_before_release: bool) {
        let (mut round, acks, release, clock, establish_facts) =
            crate::task_execution::tests::manifest_normal_release_ack_with_actor_facts_fixture();
        let root_task = round.root_task();
        let context = *round.execution().graph().contexts().next().unwrap();
        let cut = clock.now();
        let diagnostic_budget = Duration::from_millis(10);
        let convergence_budget = Duration::from_secs(60);
        let control = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            novarocks_workload_control::ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let governed = control
            .try_begin_root(novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Query,
            ))
            .unwrap();
        let cancellation = governed.owner.scope().cancellation().unwrap();
        let stage = governed
            .owner
            .scope()
            .try_acquire(novarocks_workload_control::Stage::Execution)
            .unwrap();
        let (abort, _abort_intake) =
            crate::task_execution::abort_effect::NativeAbortEffectAdapter::bounded(
                NonZeroUsize::new(2).unwrap(),
                Arc::new(crate::task_execution::status_intake::CountingWake::default()),
            );
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            root_task.query_execution_id(),
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            governed.owner,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(
            abort as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install_internal(
            tokio::runtime::Handle::current(),
            config,
            &internal_count_description(),
            NonZeroUsize::new(1).unwrap(),
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            ComputationObservationBudgets::new(diagnostic_budget, convergence_budget),
            root_task.query_execution_id(),
            vec![context],
        )
        .unwrap();
        let activation = logical.activate_initial().await.unwrap();
        crate::task_execution::tests::settle_original_actor_establish_facts(
            &logical,
            establish_facts,
        )
        .await;
        let root_status = round
            .execution()
            .task(root_task.task_id())
            .unwrap()
            .status()
            .unwrap()
            .clone();
        let ready = logical
            .accept_internal_end(
                root_task,
                root_status,
                RootResultEnd {
                    sequence: NonZeroU64::MIN,
                    output_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(ready.activation(), activation);
        assert_eq!(ready.root(), root_task);
        assert_eq!(ready.cut(), cut);
        assert_eq!(
            ready.diagnostic_deadline(),
            cut.saturating_add(diagnostic_budget)
        );
        assert_eq!(
            ready.convergence_deadline(),
            cut.saturating_add(convergence_budget)
        );
        let actor_cut = logical.actor_snapshot().await.unwrap();
        assert_eq!(
            actor_cut.conclusion,
            Some(novarocks_query_application::coordination::LogicalConclusion::ComputationReady),
        );
        // This is the logical recovery cut, not evidence of client delivery.
        assert!(actor_cut.output_visible);

        if final_info_before_release {
            // Each available value comes from this actual round's already
            // accepted exact terminal, not from a hand-built Ready/snapshot.
            let expected = round
                .execution()
                .graph()
                .tasks()
                .map(|task| task.identity())
                .collect::<Vec<_>>();
            let info = expected
                .iter()
                .map(|&identity| {
                    let status = round
                        .execution()
                        .task(identity.task_id())
                        .unwrap()
                        .status()
                        .unwrap()
                        .clone();
                    (
                        identity,
                        FinalTaskInfo::try_new(identity, status, Vec::new(), false).unwrap(),
                    )
                })
                .collect();
            let started = Arc::new(Mutex::new(Vec::new()));
            let transport: Arc<dyn TaskResultTransport> = Arc::new(OriginalAvailableFinalInfo {
                info: Mutex::new(info),
                started: Arc::clone(&started),
            });
            let notify = Arc::new(tokio::sync::Notify::new());
            let mut collector = TaskFinalInfoCollector::new(
                transport,
                true,
                &round,
                Arc::new(NotifyWake::new(Arc::clone(&notify))),
            );
            let observer = collector.enable_profile_observation().unwrap();
            // There is genuinely no split source installed by this fixture.
            // The no-scan Available(empty) fact must not hold up Release.
            collector.set_split_profile_source(None);
            collector.begin_profile_observation(ready).unwrap();
            round
                .execution_mut()
                .retain_profile_diagnostics_until(ready.diagnostic_deadline());
            for _ in 0..=expected.len() {
                std::future::poll_fn(|cx| {
                    collector.poll_profile_observation(&round, clock.now(), false, cx);
                    Poll::Ready(())
                })
                .await;
            }
            let final_info_closed_before_release = collector.final_info_closed();
            let no_early_snapshot = observer.current(ready).unwrap().is_none();
            let original_deadline_before_release = collector.observation_deadline();
            let original_reads = started.lock().unwrap().clone();
            let before_release = round.execution().released_runtime_filter_contributions();
            let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
            let mut owner = original_convergence(&round);
            let remote_deadline = std::time::Instant::now() + convergence_budget;
            let mut wait = Box::pin(owner.drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
                NativeActiveConvergenceRequest::new(cancellation.clone(), remote_deadline),
                &notify,
                Some(&mut collector),
            ));
            let pending_before_release = futures::poll!(wait.as_mut()).is_pending();
            drop(wait);
            clock.advance(diagnostic_budget / 2);
            collector.begin_profile_observation(ready).unwrap();
            std::future::poll_fn(|cx| {
                collector.poll_profile_observation(&round, clock.now(), false, cx);
                Poll::Ready(())
            })
            .await;
            let no_snapshot_after_rebegin = observer.current(ready).unwrap().is_none();
            let original_deadline_after_rebegin = collector.observation_deadline();

            let OperationIntent::ReleaseQueryContext(request) = &release else {
                unreachable!()
            };
            let original_contribution =
                crate::task_execution::tests::fixture_runtime_filter_contribution();
            acks.publish(OperationAcknowledgement::new(
                release.operation_id(),
                OperationKind::ReleaseQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: QueryContextReceipt::new(
                        request.context(),
                        QueryContextState::TerminalRetained,
                    ),
                    outcome: ReleaseOutcome::Released,
                    runtime_filter: Some(original_contribution.clone()),
                },
            ));
            round.turn().unwrap();
            publish_exit(&mut round, context);
            // A test-only watchdog drops only this borrowing wait on failure;
            // the same collector/round/owner remain in this fixture's stack.
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                owner.drive(
                    &mut round,
                    &split_delivery,
                    NativeAttemptConvergenceMode::Normal,
                    NativeActiveConvergenceRequest::new(cancellation, remote_deadline),
                    &notify,
                    Some(&mut collector),
                ),
            )
            .await;
            let snapshot = observer.current(ready).unwrap();
            // Finish the original actor before checking the captured cut facts.
            logical.abandon_running_attempt();
            logical
                .observe_worker_stopped_and_context_fenced(context)
                .await
                .unwrap();
            logical
                .finish_until(std::time::Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            assert!(final_info_closed_before_release);
            assert!(no_early_snapshot);
            assert!(pending_before_release);
            assert!(!before_release.is_complete());
            assert!(before_release.contributions().is_empty());
            assert!(no_snapshot_after_rebegin);
            assert_eq!(
                original_deadline_before_release,
                Some(ready.diagnostic_deadline())
            );
            assert_eq!(
                original_deadline_after_rebegin,
                Some(ready.diagnostic_deadline())
            );
            assert_eq!(original_reads.len(), expected.len());
            assert_eq!(
                original_reads
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>(),
                expected.iter().copied().collect()
            );
            assert_eq!(*started.lock().unwrap(), original_reads);
            assert!(matches!(
                result
                    .expect("test-only watchdog: original Release and local exit")
                    .unwrap(),
                NativeActiveConvergenceClosure::Evidence(_)
            ));
            let snapshot = snapshot.expect("the exact Release permits the final snapshot");
            assert_eq!(snapshot.ready, ready);
            assert_eq!(snapshot.closure, ProfileDiagnosticClosure::FrontierSettled);
            assert!(snapshot.complete());
            assert!(snapshot.runtime_filters.is_complete());
            assert_eq!(
                snapshot.runtime_filters.contributions(),
                &[(context.backend_process_id(), original_contribution)]
            );
            assert!(matches!(
                snapshot.split,
                ProfileSplitDiagnostic::Available(_)
            ));
            assert_eq!(collector.observation_deadline(), None);
            assert!(collector.final_info_closed());
            assert!(collector.is_closed());
            return;
        }

        let first = round.execution().accepted_terminal_tasks()[0];
        let first_status = round
            .execution()
            .task(first.task_id())
            .unwrap()
            .status()
            .unwrap()
            .clone();
        let first_info = FinalTaskInfo::try_new(first, first_status, Vec::new(), false).unwrap();
        let notify = Arc::new(tokio::sync::Notify::new());
        let (release_read, read_gate) = tokio::sync::oneshot::channel();
        let probe = Arc::new(FinalReadProbe::default());
        let transport: Arc<dyn TaskResultTransport> = Arc::new(FirstAvailableThenGated {
            first: Mutex::new(Some(first_info)),
            pending: GatedOriginalFinalInfo {
                gate: Mutex::new(Some(read_gate)),
                probe: Arc::clone(&probe),
            },
        });
        let mut collector = TaskFinalInfoCollector::new(
            transport,
            true,
            &round,
            Arc::new(NotifyWake::new(Arc::clone(&notify))),
        );
        let observer = collector.enable_profile_observation().unwrap();
        collector.begin_profile_observation(ready).unwrap();
        round
            .execution_mut()
            .retain_profile_diagnostics_until(ready.diagnostic_deadline());
        for _ in 0..2 {
            std::future::poll_fn(|cx| {
                collector.poll_profile_observation(&round, clock.now(), false, cx);
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(collector.collected.len(), 1);
        let original_available = Arc::clone(&collector.collected[0]);
        let original_pending = probe.started.lock().unwrap().clone();
        assert_eq!(original_pending.len(), 1);
        assert!(!collector.is_closed());
        assert!(observer.current(ready).unwrap().is_none());

        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let mut owner = original_convergence(&round);
        let remote_deadline = std::time::Instant::now() + convergence_budget;
        // Poll real convergence before the diagnostic deadline, then drop only
        // its borrowing wait. The collector and its original RPC remain owned.
        let mut wait = Box::pin(owner.drive(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Normal,
            NativeActiveConvergenceRequest::new(cancellation.clone(), remote_deadline),
            &notify,
            Some(&mut collector),
        ));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(wait);
        clock.advance(diagnostic_budget - Duration::from_nanos(1));
        collector.begin_profile_observation(ready).unwrap();
        std::future::poll_fn(|cx| {
            collector.poll_profile_observation(&round, clock.now(), false, cx);
            Poll::Ready(())
        })
        .await;
        assert!(observer.current(ready).unwrap().is_none());
        assert_eq!(
            collector.observation_deadline(),
            Some(ready.diagnostic_deadline())
        );
        assert_eq!(owner.remote_deadline, Some(remote_deadline));

        clock.advance(Duration::from_nanos(1));
        let later_remote_deadline = std::time::Instant::now() + Duration::from_secs(300);
        let mut wait = Box::pin(owner.drive(
            &mut round,
            &split_delivery,
            NativeAttemptConvergenceMode::Normal,
            NativeActiveConvergenceRequest::new(cancellation.clone(), later_remote_deadline),
            &notify,
            Some(&mut collector),
        ));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        drop(wait);
        let snapshot = observer
            .current(ready)
            .unwrap()
            .expect("the exact diagnostic cut publishes before RPC exit");
        assert_eq!(snapshot.ready, ready);
        assert_eq!(
            snapshot.fragments.as_ref(),
            round
                .execution()
                .graph()
                .tasks()
                .map(|task| (task.identity(), task.fragment_id()))
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            snapshot.closure,
            ProfileDiagnosticClosure::ObservationDeadline
        );
        assert!(!snapshot.complete());
        assert!(collector.final_info_closed());
        assert!(!snapshot.runtime_filters.is_complete());
        assert!(snapshot.runtime_filters.contributions().is_empty());
        assert!(matches!(
            snapshot.split,
            ProfileSplitDiagnostic::Available(_)
        ));
        let available = snapshot
            .tasks
            .iter()
            .find(|(task, _)| *task == first)
            .unwrap();
        let ProfileTaskDiagnostic::Available(info) = &available.1 else {
            panic!("the original available fact was lost")
        };
        assert!(Arc::ptr_eq(info, &original_available));
        assert!(snapshot.tasks.iter().any(|(task, fact)| {
            *task == original_pending[0]
                && matches!(
                    fact,
                    ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
                )
        }));
        assert!(snapshot.tasks.iter().any(|(_, fact)| matches!(
            fact,
            ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::NotRead)
        )));
        assert!(!collector.is_closed());
        assert!(probe.completed.lock().unwrap().is_empty());
        assert!(probe.dropped.lock().unwrap().is_empty());
        assert_eq!(collector.observation_deadline(), None);
        assert_eq!(owner.remote_deadline, Some(remote_deadline));
        assert!(owner.remote_closure.is_none());

        let OperationIntent::ReleaseQueryContext(request) = &release else {
            unreachable!()
        };
        acks.publish(OperationAcknowledgement::new(
            release.operation_id(),
            OperationKind::ReleaseQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Release {
                receipt: QueryContextReceipt::new(
                    request.context(),
                    QueryContextState::TerminalRetained,
                ),
                outcome: ReleaseOutcome::Released,
                runtime_filter: Some(
                    crate::task_execution::tests::fixture_runtime_filter_contribution(),
                ),
            },
        ));
        round.turn().unwrap();
        publish_exit(&mut round, context);
        assert!(round.attempt_drained());
        assert!(round.pumps_closed());
        assert!(!collector.is_closed());

        let gate = Arc::new(Mutex::new(Some(release_read)));
        let fired = Arc::new(AtomicBool::new(false));
        let timer_saw_ready_snapshot_and_held_rpc = Arc::new(AtomicBool::new(false));
        let (stop, stopped) = std::sync::mpsc::channel();
        let watchdog = {
            let gate = Arc::clone(&gate);
            let fired = Arc::clone(&fired);
            std::thread::spawn(move || {
                if matches!(
                    stopped.recv_timeout(Duration::from_secs(5)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    fired.store(true, Ordering::SeqCst);
                    if let Some(gate) = gate.lock().unwrap().take() {
                        let _ = gate.send(());
                    }
                }
            })
        };
        let sibling = {
            let gate = Arc::clone(&gate);
            let probe = Arc::clone(&probe);
            let saw = Arc::clone(&timer_saw_ready_snapshot_and_held_rpc);
            let original_snapshot = Arc::clone(&snapshot);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let current = observer.current(ready).unwrap().unwrap();
                saw.store(
                    Arc::ptr_eq(&current, &original_snapshot)
                        && probe.completed.lock().unwrap().is_empty()
                        && probe.dropped.lock().unwrap().is_empty(),
                    Ordering::SeqCst,
                );
                if let Some(gate) = gate.lock().unwrap().take() {
                    let _ = gate.send(());
                }
                observer
            })
        };
        let result = owner
            .drive(
                &mut round,
                &split_delivery,
                NativeAttemptConvergenceMode::Normal,
                NativeActiveConvergenceRequest::new(cancellation, later_remote_deadline),
                &notify,
                Some(&mut collector),
            )
            .await;
        let _ = stop.send(());
        watchdog.join().unwrap();
        let observer = sibling.await.unwrap();
        let result = result.unwrap();
        // Collect the actor/registry's real teardown before reporting assertions.
        logical.abandon_running_attempt();
        logical
            .observe_worker_stopped_and_context_fenced(context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!fired.load(Ordering::SeqCst));
        assert!(timer_saw_ready_snapshot_and_held_rpc.load(Ordering::SeqCst));
        assert!(matches!(
            result,
            NativeActiveConvergenceClosure::Evidence(_)
        ));
        assert_eq!(owner.remote_deadline, Some(remote_deadline));
        assert_eq!(*probe.started.lock().unwrap(), original_pending);
        assert_eq!(*probe.completed.lock().unwrap(), original_pending);
        assert_eq!(*probe.dropped.lock().unwrap(), original_pending);
        assert!(collector.is_closed());
        clock.advance(convergence_budget);
        collector.begin_profile_observation(ready).unwrap();
        std::future::poll_fn(|cx| {
            collector.poll_profile_observation(&round, clock.now(), false, cx);
            Poll::Ready(())
        })
        .await;
        assert_eq!(collector.observation_deadline(), None);
        let late = observer.current(ready).unwrap().unwrap();
        assert!(Arc::ptr_eq(&late, &snapshot));
        assert!(
            round
                .execution()
                .released_runtime_filter_contributions()
                .is_complete()
        );
        assert!(!late.runtime_filters.is_complete());
        assert!(late.runtime_filters.contributions().is_empty());
        assert!(matches!(late.split, ProfileSplitDiagnostic::Available(_)));
        assert!(late.tasks.iter().any(|(task, fact)| {
            *task == original_pending[0]
                && matches!(
                    fact,
                    ProfileTaskDiagnostic::Missing(ProfileDiagnosticMissing::ReadPending)
                )
        }));
        assert_eq!(collector.collected.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actual_ready_snapshot_deadline_is_independent_of_remote_wait_and_original_rpc_exit() {
        original_ready_profile_snapshot_fixture(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actual_final_info_cut_waits_for_original_release_without_renewing_ready_deadline() {
        original_ready_profile_snapshot_fixture(true).await;
    }
}
