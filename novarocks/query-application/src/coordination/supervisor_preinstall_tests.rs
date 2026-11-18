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

// Preinstall, allocator and actual supervisor lifecycle cases.
#[test]
fn startup_cancellation_keeps_the_original_frontend_drain_reason() {
    let reason = CancellationReason::FrontendDrainDeadlineExceeded;
    let expected = crate::session_outcome::governed_cancellation_failure_input(reason.clone());
    for error in [
        start_cancellation_error(reason.clone()),
        pre_install_cancellation_error(reason),
    ] {
        assert_eq!(error.execution_failure(), Some(&expected));
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
    }
    assert_eq!(
        start_cancellation_error(CancellationReason::DeadlineExceeded).message(),
        "logical execution start was cancelled before its start verdict"
    );
    assert_eq!(
        pre_install_cancellation_error(CancellationReason::DeadlineExceeded).message(),
        "logical execution cancelled before actor installation"
    );
}

#[test]
fn producer_failures_keep_their_facts_instead_of_claiming_query_cancellation_or_deadline() {
    use novarocks_execution_contract::failure::{FailureClass, FailureEvidence};
    let source = WorkError::CapacityWaitTimeout;
    let error = super::work_error(source.clone());
    let input = error.execution_failure().unwrap();
    assert_eq!(input.fact(), source.failure_fact());
    assert_eq!(input.fact().class(), FailureClass::Resource);
    assert_eq!(input.fact().resource(), None);
    assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);

    let source = WorkError::Cancelled(CancellationReason::Requested);
    let error = super::work_error(source.clone());
    let input = error.execution_failure().unwrap();
    assert_eq!(input.fact(), source.failure_fact());
    assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
    assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
}

#[tokio::test]
async fn accepted_start_returns_on_cancellation_before_supervisor_verdict() {
    let (control, root) = governance().await;
    let mut control = {
        let (mut bank, shutdown, mut registry, client) =
            unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
        let requester = root.owner.cancellation_requester();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        requester
            .request(CancellationReason::DeadlineExceeded)
            .unwrap();
        let error = match tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .expect("accepted start returns after cancellation")
        {
            Ok(_) => panic!("cancellation wins before a start verdict"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded,
            "{}",
            error.message()
        );
        // The response consumer exited, but the SAME accepted input still
        // owns its Work until the real factory is polled and dropped.
        assert_ne!(control.snapshot().root_responsibilities, 0);
        shutdown.send_replace(true);
        bank.stop_ingress();
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        let (_source, completion) = completion.into_parts();
        assert!(matches!(&completion, PollCompletion::Ready(output) if output.value().is_ok()));
        drop(completion);
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        root.business.release();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn uninstalled_failure_settles_its_known_cancel_control() {
    let mut control = {
        let (control, root) = governance().await;
        let scope = root.owner.scope();
        root.owner.cancel(CancellationReason::Requested);

        PendingWorkOwner(Some(root.owner)).complete_uninstalled_failure();
        root.business.release();

        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(
            control.next_control().is_none(),
            "a failure before actor installation has no effect owner left to acknowledge"
        );

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn admitted_capacity_reaches_original_request_and_survives_failed_start() {
    let mut control = {
        use crate::admitted_query_context::QueryResultCapacityBinding;
        use std::future::Future;
        let (control, root) = governance().await;
        let scope = root.owner.scope();
        let capacity = scope.result_capacity().unwrap();
        let window = capacity
            .try_acquire(
                &scope,
                novarocks_workload_control::ResultWindowClass::Internal,
            )
            .unwrap();
        let original =
            QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap();
        let activity = original.begin_result_activity().unwrap();
        let (captured, received) = oneshot::channel();
        let request = completion_request(CaptureAdmittedCapacityPort {
            captured: Some(captured),
        })
        .with_result_capacity(original.clone())
        .unwrap();
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x75),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let result = client.start(request, root.owner).await;
        assert!(result.is_err());
        let issued = received.await.unwrap();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let observed = &issued;
        assert!(
            observed
                .window_alias()
                .shares_capacity_with(&original.window_alias())
        );
        assert!(observed.window_alias().is_for_scope(&scope));
        // An activity started through the original binding remains visible
        // through the actual request. Reconstructing from an alias would fail.
        let mut exited = Box::pin(observed.result_activities_exited());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(exited.as_mut().poll(&mut context).is_pending());
        drop(activity);
        exited.await;
        drop((original, window));
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        drop(issued);
        assert_eq!(capacity.snapshot().held_positions[2], 0);
        root.business.release();
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn foreign_runtime_capacity_is_rejected_before_native_open() {
    let (mut control, mut foreign_control) = {
        use crate::admitted_query_context::QueryResultCapacityBinding;
        let (control, root) = governance().await;
        let (foreign_control, foreign_root) = governance().await;
        let scope = root.owner.scope();
        let foreign_scope = foreign_root.owner.scope();
        assert_eq!(
            scope.id(),
            foreign_scope.id(),
            "runtime identity must also be checked"
        );
        let capacity = foreign_scope.result_capacity().unwrap();
        let window = capacity
            .try_acquire(
                &foreign_scope,
                novarocks_workload_control::ResultWindowClass::Internal,
            )
            .unwrap();
        let original =
            QueryResultCapacityBinding::try_new(&foreign_scope, window.retain_alias()).unwrap();
        let request = completion_request(UnreachablePreparationPort)
            .with_result_capacity(original.clone())
            .unwrap();
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x76),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let Err(error) = client.start(request, root.owner).await else {
            panic!("a foreign runtime binding cannot open Native work");
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        drop((original, window));
        assert_eq!(capacity.snapshot().held_positions[2], 0);
        root.business.release();
        foreign_root.owner.complete();
        foreign_root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        scope.wait_released().await;
        foreign_scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(foreign_control.snapshot().root_responsibilities, 0);

        (control, foreign_control)
    };
    acknowledge_all_control(&control);
    control.finish().await;
    acknowledge_all_control(&foreign_control);
    foreign_control.finish().await;
}

#[tokio::test]
async fn cancelled_rejected_start_settles_uninstalled_control() {
    let mut control = {
        let (control, root) = governance().await;
        let cancellation = root.owner.scope().cancellation().unwrap();
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let error = match rejected_start(root.owner, "supervisor closed").await {
            Err(error) => error,
            Ok(_) => panic!("closed supervisor cannot accept the start"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            error.execution_failure().is_none(),
            "local admission is not a Task failure"
        );
        assert_eq!(
            cancellation.reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        root.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(control.next_control().is_none());

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn an_owner_abandoned_by_an_unwind_reports_the_original_panic() {
    let mut control = {
        let control = governance_empty();
        let (mut supervisor, _client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let mut original_join = supervisor.join.take().unwrap();
        let payload = catch_unwind(AssertUnwindSafe(move || {
            let _abandoned = supervisor;
            panic!("the assertion that actually failed");
        }))
        .expect_err("the closure panics");

        assert_eq!(
            *payload
                .downcast_ref::<&str>()
                .expect("the original panic payload survives the unwind"),
            "the assertion that actually failed"
        );

        let joined = tokio::time::timeout(Duration::from_secs(1), &mut original_join)
            .await
            .expect("the same abandoned root must actually join")
            .unwrap();
        assert!(matches!(joined, OriginalRootOutcome::Completed(Ok(()))));

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[test]
fn native_contract_wrappers_keep_original_fact_and_safe_detail() {
    use crate::api::NativeExecutionContractError;
    for source in [
        NativeExecutionContractError::ForeignAttemptTicket,
        NativeExecutionContractError::AttemptPreparationRetired,
    ] {
        for error in [
            native_open_error(source.into()),
            native_prepare_error(source.into()),
        ] {
            let input = error
                .execution_failure()
                .expect("native contract author must retain its fact");
            assert_eq!(input.fact(), source.failure_fact());
            assert_eq!(input.detail(), &source.safe_detail());
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                input.topology_requirement(),
                crate::api::NativeAttemptTopologyRequirement::LiveSnapshot
            );
            assert_eq!(
                input.user_error().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(input.user_error().location(), None);
        }
    }
}

#[tokio::test]
async fn start_handoff_has_no_second_capacity_and_closed_supervisor_rejects_work() {
    let control = governance_empty();
    let mut control = {
        let seed_drops = Arc::new(AtomicU64::new(0));
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (mut bank, shutdown, mut registry, client) = unpolled_test_driver(
            &control,
            Arc::new(RecordingRejectedOpen {
                opened: Arc::clone(&opened),
            }),
        );
        let first = control.begin_root().await;
        let second = control.begin_root().await;
        // Reserve this real owner before the bank's global closing cut.
        let closed = control.begin_root().await;
        let first_scope = first.owner.scope();
        let second_scope = second.owner.scope();
        let closed_scope = closed.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let closed_cancellation = closed_scope.cancellation().unwrap();
        let first_start = client.start(drop_tracked_request(&seed_drops), first.owner);
        let second_start = client.start(drop_tracked_request(&seed_drops), second.owner);
        assert!(
            control.next_control().is_none(),
            "handoff must not manufacture cancellation work"
        );
        assert_eq!(second_cancellation.reason(), None);
        assert!(
            opened.lock().unwrap().is_empty(),
            "queued ordinary starts do not consume identity or open Native before their factory"
        );
        assert_eq!(
            seed_drops.load(Ordering::SeqCst),
            0,
            "original accepted requests remain in the actual fixed inputs"
        );
        shutdown.send_replace(true);
        bank.stop_ingress();
        // The original factory supplies the old accepted-but-not-started
        // rejection; no manual extraction/completion of an owned Work.
        for _ in 0..2 {
            let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
            let (_source, completion) = completion.into_parts();
            assert!(
                matches!(&completion, PollCompletion::Ready(output) if output.value().is_ok())
            );
            drop(completion);
        }
        let first_error = first_start
            .await
            .err()
            .expect("injected handoff reply must fail");
        let second_error = second_start
            .await
            .err()
            .expect("injected handoff reply must fail");
        assert_eq!(first_error.kind(), QueryExecutionErrorKind::Rejected);
        assert_eq!(second_error.kind(), QueryExecutionErrorKind::Rejected);
        assert_eq!(seed_drops.load(Ordering::SeqCst), 2);
        assert!(
            opened.lock().unwrap().is_empty(),
            "shutdown drain has no Native effect"
        );
        first.business.release();
        second.business.release();
        first_scope.wait_released().await;
        second_scope.wait_released().await;
        let closed_error = client
            .start(drop_tracked_request(&seed_drops), closed.owner)
            .await
            .err()
            .expect("a closed supervisor must reject new work");
        assert_eq!(closed_error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            control.next_control().is_none(),
            "closed admission rejection must not manufacture cancellation work"
        );
        assert_eq!(closed_cancellation.reason(), None);
        assert_eq!(seed_drops.load(Ordering::SeqCst), 3);
        closed.business.release();
        closed_scope.wait_released().await;
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn process_source_health_is_observed_before_the_original_service_exits() {
    #[derive(Debug)]
    struct OriginalCause(Arc<AtomicU64>);
    impl std::fmt::Display for OriginalCause {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original process source failure")
        }
    }
    impl std::error::Error for OriginalCause {}
    impl Drop for OriginalCause {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut control = governance_empty();
    let (mut supervisor, client) = test_supervisor(
        &control,
        Handle::current(),
        Arc::new(BindingNativePort),
        QueryProcessNamespace::new(0x61),
        FrontendProcessId::new_v7(),
        supervisor_config(),
    );
    assert!(supervisor.process_failure().is_none());
    let drops = Arc::new(AtomicU64::new(0));
    let cause = Arc::new(OriginalCause(drops.clone()));
    let original = Arc::downgrade(&cause);
    client.report_process_failure(
        crate::api::StatementNativeHealthFailure::actual_exit_unproven(
            "original background source failed",
            cause,
        ),
    );
    // The production driver has already accepted the same cause into the
    // original health cell. No logical Work or Native identity is minted.
    let observed = supervisor.process_failure().is_some();
    tokio::task::yield_now().await;
    let still_live = !supervisor.service_has_exited();
    let retained = original.upgrade().is_some() && drops.load(Ordering::SeqCst) == 0;
    let roots = control.snapshot().root_responsibilities;
    let shutdown = supervisor
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await;
    supervisor.abandon_for_process_exit();
    drop(client);
    drop(supervisor);
    acknowledge_all_control(&control);
    control.finish().await;
    assert!(observed && still_live && retained);
    assert_eq!(roots, 0);
    assert!(matches!(
        shutdown,
        Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn logical_task_panic_completes_uninstalled_work_and_drains_supervisor() {
    let mut control = {
        let (control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x49),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let scope = root.owner.scope();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        let error = match start.await {
            Ok(_) => panic!("panicking Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_ne!(
            control.snapshot().root_responsibilities,
            0,
            "the real whole health packet still retains its original footer"
        );
        supervisor.abandon_for_process_exit();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn deadline_before_actor_install_settles_its_cancel_control() {
    let mut control = {
        let control = governance_empty();
        let root = control
            .begin_root_request(WorkRequest {
                class: WorkClass::Query,
                deadline: Some((Instant::now() + Duration::from_millis(25)).into()),
            })
            .await;
        let scope = root.owner.scope();
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x4a),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let start = client.start(
            completion_request(OriginalPreparationGate::pending(
                UnreachablePreparationPort,
                Arc::clone(&entered),
            )),
            root.owner,
        );
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("Native open did not begin");
        tokio::time::sleep(Duration::from_millis(30)).await;
        control.expire_deadlines();

        let start = tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .expect("deadline must stop a pre-install Native open");
        let Err(error) = start else {
            panic!("deadline must not return an execution handle");
        };
        assert_eq!(
            error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded,
            "{}",
            error.message()
        );
        let input = error
            .execution_failure()
            .expect("pre-install deadline retains canonical input");
        assert_eq!(
            input.fact(),
            QueryFailureReason::DeadlineExceeded.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            scope.cancellation().unwrap().reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(
            control.next_control().is_none(),
            "the terminal pre-install failure must settle its cancellation control"
        );

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn shutdown_deadline_keeps_the_same_supervisor_join_for_retry() {
    let mut control = {
        let (control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x47),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let scope = root.owner.scope();
        let start = client.start(
            completion_request(OriginalPreparationGate::delayed_cancel(
                UnreachablePreparationPort,
            )),
            root.owner,
        );
        tokio::task::yield_now().await;

        assert_eq!(
            supervisor.shutdown_until(Instant::now()).await,
            Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
        );
        let error = match start.await {
            Ok(_) => panic!("shutdown during Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[test]
fn query_sequence_is_not_reused_by_a_rebuilt_supervisor_allocator() {
    let namespace = QueryProcessNamespace::new(0x4e);
    let first = ProcessQueryIdAllocator::new(namespace)
        .first_execution()
        .unwrap();
    let rebuilt = ProcessQueryIdAllocator::new(namespace)
        .first_execution()
        .unwrap();

    assert_ne!(first, rebuilt);
}

#[test]
fn query_id_issuance_keeps_one_namespace_and_positive_continuous_sequences() {
    let sequence = AtomicU64::new(0);
    let namespace = QueryProcessNamespace::new(0xfedc_ba98_7654_3210);
    let allocator = ProcessQueryIdAllocator::new(namespace);
    let first = allocator.first_execution_from(&sequence).unwrap();
    let second = allocator.first_execution_from(&sequence).unwrap();

    for (execution, expected) in [(first, 1), (second, 2)] {
        let attribution = execution.query_id().process_attribution().unwrap();
        assert_eq!(attribution.namespace(), namespace);
        assert_eq!(attribution.sequence().get(), expected);
        assert_eq!(execution.attempt_id(), AttemptId::new(1).unwrap());
    }
    assert_ne!(first, second);
    assert_eq!(sequence.load(Ordering::Relaxed), 2);
}

#[test]
fn query_id_issuance_preserves_distinct_process_namespaces() {
    let first = ProcessQueryIdAllocator::new(QueryProcessNamespace::new(11))
        .first_execution_from(&AtomicU64::new(0))
        .unwrap();
    let second = ProcessQueryIdAllocator::new(QueryProcessNamespace::new(12))
        .first_execution_from(&AtomicU64::new(0))
        .unwrap();

    assert_eq!(first.query_id().low(), second.query_id().low());
    assert_ne!(first.query_id().high(), second.query_id().high());
    assert_ne!(first, second);
}

#[test]
fn query_id_issuance_exhaustion_never_wraps_or_reuses_the_final_sequence() {
    let final_sequence = i64::MAX as u64;
    let sequence = AtomicU64::new(final_sequence - 1);
    let namespace = QueryProcessNamespace::new(13);
    let allocator = ProcessQueryIdAllocator::new(namespace);
    let final_execution = allocator.first_execution_from(&sequence).unwrap();
    let attribution = final_execution.query_id().process_attribution().unwrap();
    assert_eq!(attribution.namespace(), namespace);
    assert_eq!(attribution.sequence().get(), final_sequence);

    // Both the original and rebuilt allocator use the same checked author.
    // No test changes the process-global issuing counter.
    for issuer in [allocator, ProcessQueryIdAllocator::new(namespace)] {
        let error = issuer.first_execution_from(&sequence).unwrap_err();
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            error.message(),
            "logical execution query id sequence is exhausted"
        );
        assert_eq!(sequence.load(Ordering::Relaxed), final_sequence);
    }
}

#[tokio::test]
async fn reserved_identity_reaches_native_open_without_reallocation() {
    let mut control = {
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(RecordingRejectedOpen {
                opened: Arc::clone(&opened),
            }),
            QueryProcessNamespace::new(0x51),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let reserved = client.reserve_identity().unwrap();
        let expected = reserved.execution();

        let scope = root.owner.scope();
        let result = client
            .start_reserved(
                completion_request(UnreachablePreparationPort),
                root.owner,
                reserved,
            )
            .await;
        assert!(
            matches!(result, Err(ref error) if error.kind() == QueryExecutionErrorKind::Rejected)
        );
        assert_eq!(*opened.lock().unwrap(), vec![expected]);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn foreign_reservation_is_refused_before_native_side_effects() {
    let mut control = {
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let namespace = QueryProcessNamespace::new(0x52);
        let donor = ProcessQueryIdAllocator::new(namespace);
        let reserved = donor.reserve().unwrap();
        let (control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(RecordingRejectedOpen {
                opened: Arc::clone(&opened),
            }),
            namespace,
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let scope = root.owner.scope();
        let result = client
            .start_reserved(
                completion_request(UnreachablePreparationPort),
                root.owner,
                reserved,
            )
            .await;
        assert!(
            matches!(result, Err(ref error) if error.kind() == QueryExecutionErrorKind::InvalidRequest)
        );
        assert!(opened.lock().unwrap().is_empty());
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn registry_shutdown_error_retains_owner_and_run_failure_for_retry() {
    let mut _control = {
        let (_control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &_control,
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let scope = root.owner.scope();
        let error = match client
            .start(completion_request(UnreachablePreparationPort), root.owner)
            .await
        {
            Ok(_) => panic!("panicking Native open must fail the logical task"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        supervisor.inject_registry_shutdown_error_once(
            LogicalExecutionRuntimeRegistryError::JoinNotReady,
        );

        assert_eq!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::Registry(
                LogicalExecutionRuntimeRegistryError::JoinNotReady,
            ))
        );
        assert!(!supervisor.shutdown_complete);

        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert!(supervisor.shutdown_complete);
        assert_ne!(
            _control.snapshot().root_responsibilities,
            0,
            "the real whole health packet still retains its original footer"
        );
        supervisor.abandon_for_process_exit();
        scope.wait_released().await;

        _control
    };
    acknowledge_all_control(&_control);
    _control.finish().await;
}

#[tokio::test]
async fn cancelled_original_prepare_closes_its_same_consumer_before_actor_installation() {
    let (mut control, root) = governance().await;
    let entered = Arc::new(AtomicBool::new(false));
    let consumer = Arc::new(CheckedCountConsumer::default());
    let requester = root.owner.cancellation_requester();
    let request = internal_count_request(HeldBeforeActorPreparation(Arc::clone(&entered)))
        .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
        .unwrap();
    let (mut supervisor, client) = test_supervisor(
        &control,
        Handle::current(),
        Arc::new(BindingNativePort),
        QueryProcessNamespace::new(0xae),
        FrontendProcessId::new_v7(),
        supervisor_config(),
    );
    let started = client.start(request, root.owner);
    wait_preparation_exit_fact(&entered).await.unwrap();
    requester.request(CancellationReason::Requested).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), started)
        .await
        .unwrap();
    assert!(matches!(result, Err(error) if error.kind() == QueryExecutionErrorKind::Cancelled));
    wait_preparation_exit_fact(&consumer.local_closed)
        .await
        .unwrap();
    assert_eq!(consumer.consume_calls.load(Ordering::SeqCst), 0);
        assert!(!consumer.decoded.load(Ordering::SeqCst));
    assert!(consumer.accepted.lock().unwrap().is_none());
    assert!(
        consumer.ready_observation.lock().unwrap().is_none(),
        "a refused original prepare must never activate a consumer"
    );
    assert_eq!(supervisor.registry.active_native_custody_count(), 0);
    root.business.release();
    supervisor
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    drop(client);
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn internal_registration_errors_refuse_before_native_open() {
    let mut control = {
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (control, root) = governance().await;
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(RecordingRejectedOpen {
                opened: Arc::clone(&opened),
            }),
            QueryProcessNamespace::new(0x6f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let consumer: Arc<dyn InternalComputationConsumer> = Arc::new(ObservedCountConsumer(
            Arc::new(CheckedCountConsumer::default()),
        ));
        let wrong_boundary =
            completion_request(UnreachablePreparationPort).with_internal_consumer(consumer);
        assert!(matches!(wrong_boundary, Err(ref error)
        if error.kind() == QueryExecutionErrorKind::InvalidRequest));

        let scope = root.owner.scope();
        let missing_registration = client
            .start(
                internal_count_request(UnreachablePreparationPort),
                root.owner,
            )
            .await;
        assert!(matches!(missing_registration, Err(ref error)
        if error.kind() == QueryExecutionErrorKind::InvalidRequest));
        assert!(opened.lock().unwrap().is_empty());
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;

        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}
