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

// Included inside supervisor::tests to reuse its actual Native admission fixtures.
mod internal_supervisor_tests {
    use super::*;
    use crate::api::{
        ComputationReadyToken, InternalComputationConsumer, InternalConsumerActivation,
        InternalConsumerFuture, InternalResultDelivery,
    };
    use novarocks_coordination_contract::{MonotonicClock, MonotonicInstant};
    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootReadResponse, RootResultEnd, RootResultRead, RootResultReply,
    };
    use novarocks_result_contract::{
        FrozenRootOutput, RootOutputContract, RootOutputKind, RootProfileId,
    };
    use std::pin::Pin;

    const INTERNAL_ROWS: u64 = 7;

    #[derive(Debug)]
    struct FixedComputationClock(MonotonicInstant);

    impl MonotonicClock for FixedComputationClock {
        fn now(&self) -> MonotonicInstant {
            self.0
        }
    }

    #[derive(Debug, Default)]
    struct InternalAttemptSignals {
        // Only the consumer factory-failure fixture issues no Native commands.
        factory_failure_before_native: bool,
        unissued_run_created: AtomicBool,
        unissued_run_dropped: AtomicBool,
        run_entered: AtomicBool,
        run_returned: AtomicBool,
        cleanup_entered: AtomicBool,
        cleanup_completed: AtomicBool,
        active_dropped: AtomicBool,
        final_ack_gate: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        final_ack_entered: AtomicBool,
        native_failure: std::sync::Mutex<Option<ExecutionFailureInput>>,
        replacement_failure: std::sync::Mutex<Option<ExecutionFailureInput>>,
        preparations: AtomicU64,
        convergence_ready: std::sync::Mutex<Option<Option<ComputationReadyToken>>>,
        convergence_deadline: std::sync::Mutex<Option<Instant>>,
        release_run: tokio::sync::Notify,
        release_cleanup: tokio::sync::Notify,
    }

    type ReadyObservation =
        watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>;

    #[derive(Debug, Default)]
    struct CheckedCountConsumer {
        decoded: AtomicBool,
        local_closed: AtomicBool,
        accepted: std::sync::Mutex<Option<ComputationReadyToken>>,
        ready_observation: std::sync::Mutex<Option<ReadyObservation>>,
    }

    #[derive(Debug)]
    struct ObservedCountConsumer(Arc<CheckedCountConsumer>);

    impl InternalComputationConsumer for ObservedCountConsumer {
        // This CountOnly owner never constructs or submits a local CPU job.
        fn begin_local_close(&self) {}

        fn poll_local_closed(
            &self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
            self.0.local_closed.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        }

        fn consume(&self, activation: InternalConsumerActivation) -> InternalConsumerFuture {
            let record = Arc::clone(&self.0);
            *record.ready_observation.lock().unwrap() = Some(activation.ready.clone());
            let (mut stream, source) = activation.into_parts();
            Box::pin(async move {
                assert_eq!(
                    stream.carrier(),
                    crate::api::ResultRowCarrier::relayed(RootOutputKind::CountOnly, None).unwrap()
                );
                let delivery = stream
                    .next()
                    .await?
                    .expect("internal root has its original End");
                let InternalResultDelivery::End(end) = delivery else {
                    panic!("count-only root cannot deliver a data segment")
                };
                assert_eq!(end.end().output_rows, INTERNAL_ROWS);
                let decoded = end.complete().await?;
                assert_eq!(decoded.activation(), source.activation());
                assert_eq!(decoded.root(), source.root());
                record.decoded.store(true, Ordering::SeqCst);
                let accepted = source.accept(decoded).await?;
                *record.accepted.lock().unwrap() = Some(accepted);
                Ok(())
            })
        }
    }

    fn internal_count_request(
        attempts: impl NativeAttemptPreparationPort,
    ) -> QueryExecutionRequest {
        internal_count_request_with_recovery(attempts, RecoveryMode::NoRecovery, None)
    }

    fn internal_count_request_with_recovery(
        attempts: impl NativeAttemptPreparationPort,
        recovery: RecoveryMode,
        replacements: Option<Arc<dyn super::super::super::ReplacementQualificationEffectPort>>,
    ) -> QueryExecutionRequest {
        let plan = crate::completed_plan_fixture::completed_values_plan_blocking(fixture_version());
        let candidate = crate::preparation::CompletedPhysicalPlanCandidate::for_program(
            plan.candidate()
                .plan()
                .as_ref()
                .clone()
                .with_root_output(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::CountOnly,
                ))
                .unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let output = OutputContract::from_completed_candidate(
            crate::api::QueryExecutionKind::Read,
            &candidate,
        )
        .unwrap();
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate,
            Vec::new(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
            recovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap()
        .into_internal_computation()
        .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            replacements,
            attempts,
        )
    }

    struct CountEndPort(Arc<InternalAttemptSignals>);

    impl crate::api::BoundedRootReadPort for CountEndPort {
        fn read(
            &self,
            request: RootResultRead,
            guard: novarocks_workload_control::ResultWindowAlias,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            crate::api::RetainedRootReply,
                            super::super::super::RootResultFetchFailure,
                        >,
                    > + Send,
            >,
        > {
            let signals = Arc::clone(&self.0);
            let final_ack_gate = if request.wanted().is_none() {
                signals.final_ack_gate.lock().unwrap().take()
            } else {
                None
            };
            Box::pin(async move {
                if request.wanted().is_none() {
                    signals.final_ack_entered.store(true, Ordering::SeqCst);
                    if let Some(gate) = final_ack_gate {
                        gate.await.unwrap();
                    }
                }
                assert_eq!(request.kind(), RootOutputKind::CountOnly);
                let outcome = match request.wanted() {
                    Some(sequence) => {
                        assert_eq!(sequence, std::num::NonZeroU64::MIN);
                        RootReadOutcome::End(RootResultEnd {
                            sequence,
                            output_rows: INTERNAL_ROWS,
                        })
                    }
                    None => RootReadOutcome::AckOnly,
                };
                crate::api::RetainedRootReply::try_new(
                    RootReadResponse::Reply(RootResultReply {
                        root_task: request.root_task(),
                        profile: request.profile(),
                        kind: request.kind(),
                        accepted_consumed: request.consumed(),
                        outcome,
                    }),
                    guard,
                    64 * 1024,
                )
                .map_err(|error| fixture_root_reply_failure(request.root_task(), error))
            })
        }

        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct InternalCountPreparation {
        backend: BackendProcessId,
        window: TestWindow,
        signals: Arc<InternalAttemptSignals>,
    }

    impl NativeAttemptPreparationPort for InternalCountPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let ordinal = self.signals.preparations.fetch_add(1, Ordering::SeqCst) + 1;
            let replacement_failure = if ordinal > 1 {
                self.signals.replacement_failure.lock().unwrap().clone()
            } else {
                None
            };
            if let Some(input) = replacement_failure {
                return Box::pin(async move {
                    Err(NativeAttemptPreparationError::Runtime(
                        NativeAttemptPreparationFailure::from_input(input),
                    ))
                });
            }
            let scheduling = no_scan_scheduling(request);
            let owner = InternalCountDormant {
                backend: self.backend,
                window: self.window.clone(),
                signals: Arc::clone(&self.signals),
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move { request.bind_staged(scheduling).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct InternalCountDormant {
        backend: BackendProcessId,
        window: TestWindow,
        signals: Arc<InternalAttemptSignals>,
    }

    impl DormantNativeAttemptOwner for InternalCountDormant {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let contexts = schedule.contexts().to_vec();
            let window = self.window.clone();
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                let (statuses, source) =
                    super::super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                statuses
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let port: Arc<dyn crate::api::BoundedRootReadPort> =
                    if signals.native_failure.lock().unwrap().is_some() {
                        Arc::new(TestRootReadPort {
                            sent: AtomicBool::new(false),
                            payload: None,
                        })
                    } else {
                        Arc::new(CountEndPort(Arc::clone(&signals)))
                    };
                let binding = super::super::super::RootRelayBinding {
                    port,
                    frontier: super::super::super::RootRelayFrontier::new(
                        root,
                        RootProfileId::V1,
                        RootOutputKind::CountOnly,
                        None,
                    )
                    .unwrap(),
                    window: window.0,
                    max_wait: Duration::from_millis(10),
                };
                Ok(ActivatedNativeAttempt::relayed_rows(
                    InternalCountActive {
                        root,
                        contexts,
                        statuses,
                        signals,
                    },
                    binding,
                    source,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    struct InternalCountActive {
        root: novarocks_execution_contract::TaskIdentity,
        contexts: Vec<QueryContextRef>,
        statuses: super::super::super::AcceptedRootStatusSender,
        signals: Arc<InternalAttemptSignals>,
    }

    impl fmt::Debug for InternalCountActive {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("InternalCountActive")
                .field("root", &self.root)
                .finish_non_exhaustive()
        }
    }

    impl Drop for InternalCountActive {
        fn drop(&mut self) {
            self.signals.active_dropped.store(true, Ordering::SeqCst);
        }
    }

    struct UnissuedCountRunFrame(Arc<InternalAttemptSignals>);
    impl Drop for UnissuedCountRunFrame {
        fn drop(&mut self) {
            self.0.unissued_run_dropped.store(true, Ordering::SeqCst);
        }
    }

    impl ActiveNativeAttemptOwner for InternalCountActive {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            if self.signals.factory_failure_before_native {
                // The consumer may fail before this factory is ever called.
                // If called, retain and actually drop its original F; never
                // issue admission/Establish after the actor's terminal cut.
                self.signals
                    .unissued_run_created
                    .store(true, Ordering::SeqCst);
                let frame = UnissuedCountRunFrame(Arc::clone(&self.signals));
                return Box::pin(async move {
                    let _frame = frame;
                    std::future::pending::<NativeAttemptTerminal>().await
                });
            }
            Box::pin(async move {
                let mut admissions = SuccessfulCompletionActiveOwner {
                    contexts: self.contexts.clone(),
                };
                assert!(matches!(
                    admissions.run(drive, cancellation).await,
                    NativeAttemptTerminal::Completed
                ));
                self.signals.run_entered.store(true, Ordering::SeqCst);
                let failure = self.signals.native_failure.lock().unwrap().clone();
                if let Some(input) = failure {
                    self.signals.run_returned.store(true, Ordering::SeqCst);
                    return NativeAttemptTerminal::Failed(
                        NativeAttemptPreparationFailure::from_input(input),
                    );
                }
                let finished = TaskStatus::try_new(
                    self.root,
                    TaskStatusVersion::new(2).unwrap(),
                    TaskState::Finished,
                    None,
                    TaskOutputFacts::new(true),
                )
                .unwrap();
                self.statuses
                    .publish_attempt_observation(
                        finished,
                        super::super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                self.signals.release_run.notified().await;
                self.signals.run_returned.store(true, Ordering::SeqCst);
                NativeAttemptTerminal::Completed
            })
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                {
                    let mut original = self.signals.convergence_ready.lock().unwrap();
                    let ready = request.computation_ready();
                    if let Some(first) = *original {
                        assert_eq!(
                            first, ready,
                            "convergence reentry cannot replace the original Ready cut"
                        );
                    } else {
                        *original = Some(ready);
                    }
                    let mut deadline = self.signals.convergence_deadline.lock().unwrap();
                    if let Some(first) = *deadline {
                        assert!(
                            request.remote_observation_deadline() <= first,
                            "convergence reentry cannot renew its budget"
                        );
                    }
                    *deadline = Some(request.remote_observation_deadline());
                }
                if self.signals.factory_failure_before_native
                    && self.signals.unissued_run_created.load(Ordering::SeqCst)
                {
                    assert!(
                        self.signals.unissued_run_dropped.load(Ordering::SeqCst),
                        "the actual unissued Native run F must exit before cleanup"
                    );
                }
                self.signals.cleanup_entered.store(true, Ordering::SeqCst);
                self.signals.release_cleanup.notified().await;
                self.signals.cleanup_completed.store(true, Ordering::SeqCst);
                let evidence = if self.signals.factory_failure_before_native {
                    NativeAttemptConvergence::contexts(
                        self.contexts
                            .iter()
                            .copied()
                            .map(crate::api::NativeContextConvergence::never_established)
                            .collect::<Box<[_]>>(),
                    )
                } else {
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
                };
                Ok(NativeActiveConvergenceClosure::Evidence(evidence))
            })
        }
    }

    #[derive(Debug)]
    struct HeldBeforeActorPreparation(Arc<AtomicBool>);
    impl NativeAttemptPreparationPort for HeldBeforeActorPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            _: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            self.0.store(true, Ordering::SeqCst);
            Box::pin(std::future::pending())
        }
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
    async fn actual_internal_consumer_returns_ready_before_native_run_and_cleanup() {
        for shutdown_before_ack_exit in [false, true] {
            let mut control = {
                let (control, root) = governance().await;
                let scope = root.owner.scope();
                let window = scope
                    .result_capacity()
                    .unwrap()
                    .try_acquire(
                        &scope,
                        novarocks_workload_control::ResultWindowClass::Internal,
                    )
                    .unwrap();
                let signals = Arc::new(InternalAttemptSignals::default());
                let (release_ack, ack_gate) = tokio::sync::oneshot::channel();
                *signals.final_ack_gate.lock().unwrap() = Some(ack_gate);
                let consumer = Arc::new(CheckedCountConsumer::default());
                let request = internal_count_request(InternalCountPreparation {
                    backend: BackendProcessId::new_v7(),
                    window: TestWindow(window.retain_alias()),
                    signals: Arc::clone(&signals),
                })
                .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
                .unwrap();
                drop(window);
                let cut = MonotonicInstant::from_origin(Duration::from_secs(91));
                let config = supervisor_config();
                let (mut supervisor, client) = test_supervisor_with_clock(
                    &control,
                    Handle::current(),
                    Arc::new(BindingNativePort),
                    QueryProcessNamespace::new(0x6e),
                    FrontendProcessId::new_v7(),
                    config,
                    Arc::new(FixedComputationClock(cut)),
                );
                let mut handle =
                    tokio::time::timeout(Duration::from_secs(5), client.start(request, root.owner))
                        .await
                        .expect("internal readiness must not wait for held Native work")
                        .unwrap();
                let mut actual_exit = handle
                    .take_exit_observer()
                    .expect("same original source observer");
                assert!(
                    futures::poll!(std::future::poll_fn(|cx| actual_exit.poll_exit(cx)))
                        .is_pending(),
                    "Ready is not actor/source actual exit"
                );
                let token = *handle
                    .ready_token()
                    .expect("internal execution returns its accepted cut");
                assert!(handle.take_output().is_none());
                assert_eq!(token.cut(), cut);
                assert_eq!(
                    token.diagnostic_deadline(),
                    cut.saturating_add(config.diagnostic_timeout)
                );
                assert_eq!(
                    token.convergence_deadline(),
                    cut.saturating_add(config.remote_cleanup_timeout)
                );
                assert_eq!(token.end().output_rows, INTERNAL_ROWS);
                assert!(consumer.decoded.load(Ordering::SeqCst));
                wait_preparation_exit_fact(&consumer.local_closed)
                    .await
                    .unwrap();
                assert!(signals.run_entered.load(Ordering::SeqCst));
                assert!(!signals.run_returned.load(Ordering::SeqCst));
                assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
                assert!(!signals.active_dropped.load(Ordering::SeqCst));
                assert_eq!(supervisor.registry.active_native_custody_count(), 1);

                handle.request_cancel().unwrap();
                tokio::task::yield_now().await;
                assert_eq!(handle.ready_token(), Some(&token));
                assert_eq!(
                    consumer
                        .ready_observation
                        .lock()
                        .unwrap()
                        .as_ref()
                        .expect(
                            "the registered consumer retains the actor's actual readiness observer"
                        )
                        .borrow()
                        .as_ref(),
                    Some(&Ok(token))
                );
                assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
                assert!(!signals.active_dropped.load(Ordering::SeqCst));
                signals.release_run.notify_one();
                signals.release_cleanup.notify_one();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !signals.active_dropped.load(Ordering::SeqCst)
                        || !signals.final_ack_entered.load(Ordering::SeqCst)
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(signals.cleanup_completed.load(Ordering::SeqCst));
                assert_eq!(
                    supervisor.registry.active_native_custody_count(),
                    1,
                    "Native closure and logical readiness cannot retire the pending ACK tail"
                );
                assert_eq!(supervisor.registry.root_ack_tail_count(), 1);
                assert_eq!(handle.ready_token(), Some(&token));
                if shutdown_before_ack_exit {
                    assert!(matches!(
                        supervisor.shutdown_until(Instant::now()).await,
                        Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
                    ));
                    assert_eq!(
                        supervisor.registry.root_ack_tail_count(),
                        1,
                        "a bounded shutdown wait retains the same unfinished ACK join"
                    );
                }
                // Cancelling this exact observation future cannot drop its source.
                let mut cancelled_wait =
                    Box::pin(std::future::poll_fn(|cx| actual_exit.poll_exit(cx)));
                assert!(futures::poll!(&mut cancelled_wait).is_pending());
                drop(cancelled_wait);
                release_ack.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while supervisor.registry.active_native_custody_count() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(
                    supervisor.registry.root_ack_tail_count(),
                    0,
                    "both ordinary and shutdown process loops join the original ACK tail"
                );
                // Actor join and Native ACK are necessary but independent.
                // The real role clear now runs the original stable Session's
                // actual Drop author before returning its local receipt.
                let actual_receipt = tokio::time::timeout(
                    Duration::from_secs(2),
                    std::future::poll_fn(|cx| actual_exit.poll_exit(cx)),
                )
                .await
                .expect("same original preparation source actually exits")
                .unwrap();
                assert_eq!(
                    actual_receipt.view().actor_phase(),
                    crate::api::LogicalActorExitPhase::Joined
                );
                assert!(actual_receipt.view().native_preparation_exited());
                assert_eq!(
                    actual_receipt.local_disposition(),
                    crate::api::LocalExecutionDisposition::OriginalPaidOwnerReturn
                );
                assert_eq!(
                    actual_receipt.remote_disposition(),
                    crate::api::RemoteExecutionDisposition::RemoteNotReported
                );
                drop(handle);
                root.business.release();
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
                assert_ne!(
                    control.snapshot().root_responsibilities,
                    0,
                    "the actual observer receipt still holds its original footer"
                );
                drop(actual_receipt);
                drop(actual_exit);
                acknowledge_all_control(&control);
                tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                    .await
                    .unwrap();
                assert_eq!(*consumer.accepted.lock().unwrap(), Some(token));
                assert_eq!(
                    *signals.convergence_ready.lock().unwrap(),
                    Some(Some(token))
                );
                assert!(signals.cleanup_entered.load(Ordering::SeqCst));
                assert!(signals.cleanup_completed.load(Ordering::SeqCst));
                assert!(signals.active_dropped.load(Ordering::SeqCst));
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);

                control
            };
            acknowledge_all_control(&control);
            control.finish().await;
        }
    }

    #[tokio::test]
    async fn no_recovery_native_failure_preserves_original_input_before_cleanup() {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let window = scope
                .result_capacity()
                .unwrap()
                .try_acquire(
                    &scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .unwrap();
            let expected = fixture_execution_input("injected Native failure before internal End");
            let signals = Arc::new(InternalAttemptSignals::default());
            *signals.native_failure.lock().unwrap() = Some(expected.clone());
            let consumer = Arc::new(CheckedCountConsumer::default());
            let request = internal_count_request(InternalCountPreparation {
                backend: BackendProcessId::new_v7(),
                window: TestWindow(window.retain_alias()),
                signals: Arc::clone(&signals),
            })
            .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
            .unwrap();
            drop(window);
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x70),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let result =
                tokio::time::timeout(Duration::from_secs(2), client.start(request, root.owner))
                    .await
                    .expect("the selected Native failure must be delivered while cleanup is held");
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("a failed NoRecovery computation cannot return a ready handle"),
            };
            assert_eq!(error.execution_failure(), Some(&expected));
            assert!(signals.run_returned.load(Ordering::SeqCst));
            assert!(!consumer.decoded.load(Ordering::SeqCst));
            assert!(consumer.accepted.lock().unwrap().is_none());
            assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
            assert!(!signals.active_dropped.load(Ordering::SeqCst));
            assert_eq!(supervisor.registry.active_native_custody_count(), 1);

            signals.release_cleanup.notify_one();
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                .await
                .unwrap();
            assert!(signals.cleanup_completed.load(Ordering::SeqCst));
            assert!(signals.active_dropped.load(Ordering::SeqCst));
            assert_eq!(supervisor.registry.active_native_custody_count(), 0);

            control
        };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn internal_replacement_preparation_failure_keeps_its_actual_source() {
        let mut control = {
            use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};

            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let window = scope
                .result_capacity()
                .unwrap()
                .try_acquire(
                    &scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .unwrap();
            let backend = BackendProcessId::new_v7();
            let original = ExecutionFailureInput::new(
                QueryFailureReason::BackendLost.failure_fact(),
                SafeDetail::truncating("injected first-attempt backend loss"),
                ExecutionFailureSite::Backend { process: backend },
            );
            assert_eq!(
                original.attempt_class(),
                AttemptFailureClass::RecoverableInfrastructure
            );
            let replacement = ExecutionFailureInput::new(
                FailureFact::new(
                    FailureClass::Unsupported,
                    FailureReason::new(
                        std::num::NonZeroU16::MAX,
                        std::num::NonZeroU16::new(2).unwrap(),
                    ),
                ),
                SafeDetail::truncating("injected Unsupported replacement preparation"),
                ExecutionFailureSite::Coordinator,
            );
            let signals = Arc::new(InternalAttemptSignals::default());
            *signals.native_failure.lock().unwrap() = Some(original.clone());
            *signals.replacement_failure.lock().unwrap() = Some(replacement.clone());
            let consumer = Arc::new(CheckedCountConsumer::default());
            let request = internal_count_request_with_recovery(
                InternalCountPreparation {
                    backend,
                    window: TestWindow(window.retain_alias()),
                    signals: Arc::clone(&signals),
                },
                RecoveryMode::RestartAttemptBeforeVisibility,
                Some(Arc::new(ImmediateReplacementPort::default())),
            )
            .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
            .unwrap();
            drop(window);
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x71),
                FrontendProcessId::new_v7(),
                supervisor_config(),
            );
            let result =
                tokio::time::timeout(Duration::from_secs(2), client.start(request, root.owner))
                    .await
                    .expect("replacement failure must be delivered while prior cleanup is held");
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("failed replacement preparation cannot produce internal Ready"),
            };
            assert_eq!(signals.preparations.load(Ordering::SeqCst), 2);
            assert_eq!(error.execution_failure(), Some(&replacement));
            assert_ne!(error.execution_failure(), Some(&original));
            assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
            assert!(!signals.active_dropped.load(Ordering::SeqCst));

            signals.release_cleanup.notify_one();
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await;
            let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) = shutdown
            else {
                panic!("replacement preparation failure must retain its existing shutdown receipt");
            };
            assert_eq!(error.execution_failure(), Some(&replacement));
            acknowledge_all_control(&control);
            tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                .await
                .unwrap();
            assert!(signals.cleanup_completed.load(Ordering::SeqCst));
            assert!(signals.active_dropped.load(Ordering::SeqCst));
            assert!(consumer.accepted.lock().unwrap().is_none());

            control
        };
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

    use super::super::super::{
        LogicalExecutionActorError, ReplacementQualificationEffectAdmission,
        ReplacementQualificationEffectPort, ReplacementQualificationEffectReservation,
        ReplacementQualificationEffectSubmission, ReplacementQualificationFailure,
        ReplacementQualificationRequest,
    };

    #[derive(Debug)]
    struct QualificationReplacementPreparation {
        inner: InternalCountPreparation,
        replacement_backend: BackendProcessId,
        prepared: usize,
    }

    impl NativeAttemptPreparationPort for QualificationReplacementPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            self.prepared += 1;
            if self.prepared > 1 {
                let crate::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) =
                    request.topology_requirement()
                else {
                    panic!("BackendLost replacement must exclude its failed process");
                };
                assert_eq!(excluded, self.inner.backend);
                assert_ne!(self.replacement_backend, excluded);
                self.inner.backend = self.replacement_backend;
            }
            self.inner.prepare_in(request)
        }
    }

    #[derive(Debug)]
    struct RefusingQualificationPort {
        failure: ReplacementQualificationFailure,
        capacity: watch::Sender<u64>,
        submissions: Arc<AtomicU64>,
    }

    #[derive(Debug)]
    struct RefusingQualificationReservation {
        failure: ReplacementQualificationFailure,
        submissions: Arc<AtomicU64>,
    }

    impl ReplacementQualificationEffectReservation for RefusingQualificationReservation {
        fn submit(self: Box<Self>, submission: ReplacementQualificationEffectSubmission) {
            assert!(!submission.request().failed_contexts().is_empty());
            assert!(!submission.request().replacement_contexts().is_empty());
            self.submissions.fetch_add(1, Ordering::SeqCst);
            submission.refused(self.failure);
        }
    }

    impl ReplacementQualificationEffectPort for RefusingQualificationPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _: &ReplacementQualificationRequest,
        ) -> ReplacementQualificationEffectAdmission {
            ReplacementQualificationEffectAdmission::Admitted(Box::new(
                RefusingQualificationReservation {
                    failure: self.failure,
                    submissions: Arc::clone(&self.submissions),
                },
            ))
        }
    }

    #[tokio::test]
    async fn internal_qualification_failure_preserves_actor_fact_before_cleanup() {
        for (namespace, qualification_failure) in [
            (0x72, ReplacementQualificationFailure::Rejected),
            (0x73, ReplacementQualificationFailure::Expired),
        ] {
            let mut control = {
                let (control, root) = governance().await;
                let scope = root.owner.scope();
                let window = scope
                    .result_capacity()
                    .unwrap()
                    .try_acquire(
                        &scope,
                        novarocks_workload_control::ResultWindowClass::Internal,
                    )
                    .unwrap();
                // Freeze both process identities before the first failure. The
                // replacement must never reuse the process excluded by BackendLost.
                let first_backend = BackendProcessId::new_v7();
                let replacement_backend = BackendProcessId::new_v7();
                let original = ExecutionFailureInput::new(
                    QueryFailureReason::BackendLost.failure_fact(),
                    SafeDetail::truncating("injected Native failure before qualification"),
                    ExecutionFailureSite::Backend {
                        process: first_backend,
                    },
                );
                assert_eq!(
                    original.attempt_class(),
                    AttemptFailureClass::RecoverableInfrastructure
                );
                let expected =
                    actor_error(LogicalExecutionActorError::ReplacementQualificationFailed(
                        qualification_failure,
                    ));
                assert_ne!(expected.execution_failure(), Some(&original));
                let signals = Arc::new(InternalAttemptSignals::default());
                *signals.native_failure.lock().unwrap() = Some(original.clone());
                let consumer = Arc::new(CheckedCountConsumer::default());
                let (capacity, _) = watch::channel(1);
                let submissions = Arc::new(AtomicU64::new(0));
                let request = internal_count_request_with_recovery(
                    QualificationReplacementPreparation {
                        inner: InternalCountPreparation {
                            backend: first_backend,
                            window: TestWindow(window.retain_alias()),
                            signals: Arc::clone(&signals),
                        },
                        replacement_backend,
                        prepared: 0,
                    },
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(RefusingQualificationPort {
                        failure: qualification_failure,
                        capacity,
                        submissions: Arc::clone(&submissions),
                    })),
                )
                .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
                .unwrap();
                drop(window);
                let (mut supervisor, client) = test_supervisor(
                    &control,
                    Handle::current(),
                    Arc::new(BindingNativePort),
                    QueryProcessNamespace::new(namespace),
                    FrontendProcessId::new_v7(),
                    supervisor_config(),
                );
                let result =
                    tokio::time::timeout(Duration::from_secs(2), client.start(request, root.owner))
                        .await
                        .expect(
                            "qualification failure must be delivered while prior cleanup is held",
                        );
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("refused qualification cannot produce internal Ready"),
                };
                assert_eq!(signals.preparations.load(Ordering::SeqCst), 2);
                assert_eq!(submissions.load(Ordering::SeqCst), 1);
                assert_eq!(error.execution_failure(), expected.execution_failure());
                assert_ne!(error.execution_failure(), Some(&original));
                assert!(signals.run_returned.load(Ordering::SeqCst));
                assert!(!consumer.decoded.load(Ordering::SeqCst));
                assert!(consumer.accepted.lock().unwrap().is_none());
                assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
                assert!(!signals.active_dropped.load(Ordering::SeqCst));
                assert_eq!(supervisor.registry.active_native_custody_count(), 1);

                signals.release_cleanup.notify_one();
                root.business.release();
                let shutdown = supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(5))
                    .await;
                let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) =
                    shutdown
                else {
                    panic!("qualification failure must retain its existing shutdown receipt");
                };
                assert_eq!(error.execution_failure(), expected.execution_failure());
                acknowledge_all_control(&control);
                tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                    .await
                    .unwrap();
                assert!(signals.cleanup_entered.load(Ordering::SeqCst));
                assert!(signals.cleanup_completed.load(Ordering::SeqCst));
                assert!(signals.active_dropped.load(Ordering::SeqCst));
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);

                control
            };
            acknowledge_all_control(&control);
            control.finish().await;
        }
    }

    #[derive(Clone, Debug)]
    enum RegisteredConsumerOutcome {
        Error(Box<ExecutionFailureInput>),
        Panic,
        PollPanic,
        FactoryPanic,
        DropPanic(Box<ExecutionFailureInput>),
    }

    #[derive(Debug)]
    struct FailingRegisteredConsumer {
        outcome: RegisteredConsumerOutcome,
        end_seen: Arc<AtomicBool>,
        primary_drops: Arc<AtomicU64>,
        cleanup_drops: Arc<AtomicU64>,
        window: TestWindow,
    }

    struct RegisteredConsumerPanic {
        drops: Arc<AtomicU64>,
        _window: Option<TestWindow>,
    }
    impl Drop for RegisteredConsumerPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ConsumerCleanupPanic {
        future: InternalConsumerFuture,
        drops: Arc<AtomicU64>,
    }
    impl Future for ConsumerCleanupPanic {
        type Output = Result<(), QueryExecutionError>;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.get_mut().future.as_mut().poll(cx)
        }
    }
    impl Drop for ConsumerCleanupPanic {
        fn drop(&mut self) {
            std::panic::panic_any(RegisteredConsumerPanic {
                drops: Arc::clone(&self.drops),
                _window: None,
            });
        }
    }

    impl InternalComputationConsumer for FailingRegisteredConsumer {
        // This CountOnly owner never constructs or submits a local CPU job.
        fn begin_local_close(&self) {}

        fn poll_local_closed(
            &self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn consume(&self, activation: InternalConsumerActivation) -> InternalConsumerFuture {
            if matches!(&self.outcome, RegisteredConsumerOutcome::FactoryPanic) {
                std::panic::panic_any(RegisteredConsumerPanic {
                    drops: Arc::clone(&self.primary_drops),
                    _window: Some(self.window.clone()),
                });
            }
            let (mut stream, source) = activation.into_parts();
            let outcome = self.outcome.clone();
            let end_seen = Arc::clone(&self.end_seen);
            let primary_drops = Arc::clone(&self.primary_drops);
            let cleanup_drops = Arc::clone(&self.cleanup_drops);
            let window = self.window.clone();
            let panics = matches!(
                &outcome,
                RegisteredConsumerOutcome::Panic | RegisteredConsumerOutcome::DropPanic(_)
            );
            let future: InternalConsumerFuture = Box::pin(async move {
                // Keep both source and original End owned until the actual
                // future outcome unwinds them. No CPU receipt or Ready is sent.
                let _source = source;
                let delivery = stream
                    .next()
                    .await?
                    .expect("healthy Native root must deliver its original End");
                let InternalResultDelivery::End(end) = delivery else {
                    panic!("count-only root cannot deliver a data segment");
                };
                assert_eq!(end.end().output_rows, INTERNAL_ROWS);
                end_seen.store(true, Ordering::SeqCst);
                match outcome {
                    RegisteredConsumerOutcome::Error(input)
                    | RegisteredConsumerOutcome::DropPanic(input) => {
                        Err(QueryExecutionError::from_execution(*input))
                    }
                    RegisteredConsumerOutcome::FactoryPanic => {
                        unreachable!("factory already exited with its original cause")
                    }
                    RegisteredConsumerOutcome::Panic | RegisteredConsumerOutcome::PollPanic => {
                        std::panic::panic_any(RegisteredConsumerPanic {
                            drops: primary_drops,
                            _window: Some(window),
                        })
                    }
                }
            });
            if panics {
                Box::pin(ConsumerCleanupPanic {
                    future,
                    drops: cleanup_drops,
                })
            } else {
                future
            }
        }
    }

    async fn assert_registered_consumer_failure_before_cleanup(
        outcome: RegisteredConsumerOutcome,
        expected: ExecutionFailureInput,
        namespace: u64,
    ) {
        assert_registered_consumer_failure_with_capacity(outcome, expected, namespace, false).await;
    }
    async fn assert_registered_consumer_failure_with_capacity(
        outcome: RegisteredConsumerOutcome,
        expected: ExecutionFailureInput,
        namespace: u64,
        owned: bool,
    ) {
        let is_panic = !matches!(&outcome, RegisteredConsumerOutcome::Error(_));
        let factory_panic = matches!(&outcome, RegisteredConsumerOutcome::FactoryPanic);
        let cleanup_unproven = matches!(
            &outcome,
            RegisteredConsumerOutcome::Panic | RegisteredConsumerOutcome::DropPanic(_)
        );
        let primary_count = u64::from(matches!(
            &outcome,
            RegisteredConsumerOutcome::Panic
                | RegisteredConsumerOutcome::PollPanic
                | RegisteredConsumerOutcome::FactoryPanic
        ));
        let cleanup_count = u64::from(matches!(
            &outcome,
            RegisteredConsumerOutcome::Panic | RegisteredConsumerOutcome::DropPanic(_)
        ));
        let mut control = {
            let control = governance_with_result_positions(false, None, owned.then_some(1));
            let root = control.begin_root().await;
            let scope = root.owner.scope();
            let mut root_owner = Some(root.owner);
            let (stage, payload, capacity) = if owned {
                let (stage, payload, capacity) =
                    super::client_computation_tests::owned_internal_stage(
                        &control,
                        root_owner.as_ref().unwrap(),
                    )
                    .await;
                (Some(stage), Some(payload), Some(capacity))
            } else {
                (None, None, None)
            };
            let mut stage = stage;
            let original_window = if owned {
                None
            } else {
                Some(
                    scope
                        .result_capacity()
                        .unwrap()
                        .try_acquire(
                            &scope,
                            novarocks_workload_control::ResultWindowClass::Internal,
                        )
                        .unwrap(),
                )
            };
            let window = if let Some(capacity) = capacity.as_ref() {
                capacity.try_window_alias().unwrap()
            } else {
                original_window.as_ref().unwrap().retain_alias()
            };
            let signals = Arc::new(InternalAttemptSignals {
                factory_failure_before_native: factory_panic,
                ..InternalAttemptSignals::default()
            });
            let end_seen = Arc::new(AtomicBool::new(false));
            let panics = is_panic;
            let primary_drops = Arc::new(AtomicU64::new(0));
            let cleanup_drops = Arc::new(AtomicU64::new(0));
            let request = internal_count_request(InternalCountPreparation {
                backend: BackendProcessId::new_v7(),
                window: TestWindow(window.try_clone().unwrap()),
                signals: Arc::clone(&signals),
            })
            .with_internal_consumer(Arc::new(FailingRegisteredConsumer {
                outcome,
                end_seen: Arc::clone(&end_seen),
                primary_drops: Arc::clone(&primary_drops),
                cleanup_drops: Arc::clone(&cleanup_drops),
                window: TestWindow(window.try_clone().unwrap()),
            }))
            .unwrap();
            let request = match capacity {
                Some(capacity) => request.with_result_capacity(capacity).unwrap(),
                None => request,
            };
            drop((window, original_window));
            let (mut supervisor, client) = test_supervisor_with_clock(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(namespace),
                FrontendProcessId::new_v7(),
                supervisor_config(),
                Arc::new(FixedComputationClock(MonotonicInstant::from_origin(
                    Duration::from_secs(92),
                ))),
            );
            let mut source_input = Some(
                client
                    .prepare_observed_start(request, None)
                    .unwrap_or_else(|original| std::panic::panic_any(original)),
            );
            let mut work = Some(stage.take().unwrap_or_else(|| root_owner.take().unwrap()));
            let (mut ready, mut original_exit) = (None, None);
            client
                .start_observed_in(&mut source_input, &mut work, &mut ready, &mut original_exit)
                .unwrap()
                .finish()
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(2), ready.take().unwrap())
                .await
                .expect("actual consumer failure must be delivered while Native cleanup is held");
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("failed registered computation consumer cannot produce Ready"),
            };
            assert_eq!(error.execution_failure(), Some(&expected));
            assert_eq!(end_seen.load(Ordering::SeqCst), !factory_panic);
            assert_eq!(signals.preparations.load(Ordering::SeqCst), 1);
            if factory_panic {
                assert!(
                    !signals.run_entered.load(Ordering::SeqCst),
                    "this factory-failure fixture never issues Native admission"
                );
            } else {
                assert!(signals.run_entered.load(Ordering::SeqCst));
            }
            assert!(!signals.run_returned.load(Ordering::SeqCst));
            assert!(!signals.cleanup_completed.load(Ordering::SeqCst));
            assert!(!signals.active_dropped.load(Ordering::SeqCst));
            assert_eq!(supervisor.registry.active_native_custody_count(), 1);

            signals.release_cleanup.notify_one();
            root.business.release();
            let deadline = Instant::now() + Duration::from_secs(5);
            let exit = tokio::time::timeout_at(deadline.into(), async {
                let mut observer = original_exit.take().expect("the same actual Source");
                let exit = std::future::poll_fn(|cx| observer.poll_exit(cx))
                    .await
                    .expect("the same original Source returns its actual exit receipt");
                assert!(signals.cleanup_entered.load(Ordering::SeqCst));
                assert!(signals.cleanup_completed.load(Ordering::SeqCst));
                if factory_panic && signals.unissued_run_created.load(Ordering::SeqCst) {
                    assert!(signals.unissued_run_dropped.load(Ordering::SeqCst));
                }
                assert!(signals.active_dropped.load(Ordering::SeqCst));
                let view = exit.view();
                assert!(view.native_preparation_exited());
                assert_eq!(
                    view.pending_native_returns(),
                    0,
                    "ACK, Native and consumer actual joins returned once"
                );
                assert!(!view.role_failed());
                assert_eq!(view.internal_consumer_failed(), panics);
                drop(view);
                if owned {
                    assert_eq!(
                        scope.result_capacity().unwrap().snapshot().held_positions[2],
                        usize::from(cleanup_unproven),
                        "only an unproven original cleanup retains the consumer activity"
                    );
                    if !cleanup_unproven {
                        let next_root = control.begin_root().await;
                        let (next, next_payload, next_capacity) =
                            super::client_computation_tests::owned_internal_stage(
                                &control,
                                &next_root.owner,
                            )
                            .await;
                        assert_eq!(
                            scope.result_capacity().unwrap().snapshot().held_positions[2],
                            1,
                            "the real next computation can reuse P before raw health disposal"
                        );
                        drop((next_capacity, next_payload));
                        next.complete();
                        next_root.business.release();
                        next_root.owner.complete();
                    }
                }
                exit
            })
            .await
            .expect("actual Source exit and next Root reuse fit the original close deadline");
            let shutdown = supervisor.shutdown_until(deadline).await;
            drop(payload);
            if let Some(owner) = root_owner {
                owner.complete();
            }
            if panics {
                assert!(matches!(
                    shutdown,
                    Err(LogicalExecutionSupervisorShutdownError::Registry(
                        LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved
                    ))
                ));
                assert_eq!(
                    supervisor.registry.active_native_custody_count(),
                    1,
                    "only canonical health backing remains"
                );
                assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
                assert_eq!(cleanup_drops.load(Ordering::SeqCst), 0);
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                drop(exit);
                supervisor.abandon_for_process_exit();
                assert_eq!(primary_drops.load(Ordering::SeqCst), primary_count);
                assert_eq!(cleanup_drops.load(Ordering::SeqCst), cleanup_count);
            } else {
                shutdown.unwrap();
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);
                drop(exit);
                acknowledge_all_control(&control);
                tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                    .await
                    .unwrap();
            }
            control
        };
        acknowledge_all_control(&control);
        // This fixture covers both ordinary completion and explicit health disposition.
        if is_panic {
            control.finish_for_process_exit().await;
        } else {
            control.finish().await;
        }
    }

    #[tokio::test]
    async fn owned_internal_consumer_factory_panic_releases_activity_but_retains_raw_cause() {
        let expected = ExecutionFailureInput::new(
            QueryFailureReason::InternalConsumerPanicked.failure_fact(),
            SafeDetail::truncating("internal computation consumer panicked"),
            ExecutionFailureSite::Coordinator,
        );
        assert_registered_consumer_failure_with_capacity(
            RegisteredConsumerOutcome::FactoryPanic,
            expected,
            0x793,
            true,
        )
        .await;
    }
    #[tokio::test]
    async fn owned_internal_consumer_poll_panic_releases_activity_after_normal_future_drop() {
        let expected = ExecutionFailureInput::new(
            QueryFailureReason::InternalConsumerPanicked.failure_fact(),
            SafeDetail::truncating("internal computation consumer panicked"),
            ExecutionFailureSite::Coordinator,
        );
        assert_registered_consumer_failure_with_capacity(
            RegisteredConsumerOutcome::PollPanic,
            expected,
            0x795,
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn owned_internal_consumer_drop_panic_keeps_domain_verdict_and_original_activity() {
        let expected =
            fixture_execution_input("original returned domain error before F Drop panic");
        assert_registered_consumer_failure_with_capacity(
            RegisteredConsumerOutcome::DropPanic(Box::new(expected.clone())),
            expected,
            0x794,
            true,
        )
        .await;
    }
    #[tokio::test]
    async fn owned_internal_consumer_domain_error_releases_its_actual_position() {
        let expected = fixture_execution_input("injected owned CountOnly domain failure");
        assert_registered_consumer_failure_with_capacity(
            RegisteredConsumerOutcome::Error(Box::new(expected.clone())),
            expected,
            0x791,
            true,
        )
        .await;
    }
    #[tokio::test]
    async fn owned_internal_consumer_poll_and_drop_panic_retain_original_activity() {
        let expected = ExecutionFailureInput::new(
            QueryFailureReason::InternalConsumerPanicked.failure_fact(),
            SafeDetail::truncating("internal computation consumer panicked"),
            ExecutionFailureSite::Coordinator,
        );
        assert_registered_consumer_failure_with_capacity(
            RegisteredConsumerOutcome::Panic,
            expected,
            0x792,
            true,
        )
        .await;
    }
    #[tokio::test]
    async fn registered_consumer_error_preserves_original_input_before_cleanup() {
        let expected = fixture_execution_input("injected registered computation consumer failure");
        assert_registered_consumer_failure_before_cleanup(
            RegisteredConsumerOutcome::Error(Box::new(expected.clone())),
            expected,
            0x74,
        )
        .await;
    }

    #[tokio::test]
    async fn registered_consumer_panic_keeps_its_own_typed_author_before_cleanup() {
        let expected = ExecutionFailureInput::new(
            QueryFailureReason::InternalConsumerPanicked.failure_fact(),
            SafeDetail::truncating("internal computation consumer panicked"),
            ExecutionFailureSite::Coordinator,
        );
        assert_ne!(
            expected.fact(),
            QueryFailureReason::NativeFuturePanicked.failure_fact()
        );
        assert_registered_consumer_failure_before_cleanup(
            RegisteredConsumerOutcome::Panic,
            expected,
            0x75,
        )
        .await;
    }

    #[derive(Debug, Default)]
    struct HeldInternalActivationSignals {
        entered: AtomicBool,
        returned: AtomicBool,
        dormant_dropped: AtomicBool,
        release: tokio::sync::Notify,
    }

    #[derive(Debug)]
    struct HeldInternalActivationPreparation {
        backend: BackendProcessId,
        window: TestWindow,
        native: Arc<InternalAttemptSignals>,
        activation: Arc<HeldInternalActivationSignals>,
        failure: Option<ExecutionFailureInput>,
    }

    impl NativeAttemptPreparationPort for HeldInternalActivationPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let scheduling = no_scan_scheduling(request);
            let dormant = HeldInternalActivationDormant {
                inner: InternalCountDormant {
                    backend: self.backend,
                    window: self.window.clone(),
                    signals: Arc::clone(&self.native),
                },
                activation: Arc::clone(&self.activation),
                failure: self.failure.clone(),
            };
            request.stage_fixture_owner(dormant);
            Box::pin(async move { request.bind_staged(scheduling).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct HeldInternalActivationDormant {
        inner: InternalCountDormant,
        activation: Arc<HeldInternalActivationSignals>,
        failure: Option<ExecutionFailureInput>,
    }

    impl Drop for HeldInternalActivationDormant {
        fn drop(&mut self) {
            self.activation
                .dormant_dropped
                .store(true, Ordering::SeqCst);
        }
    }

    impl DormantNativeAttemptOwner for HeldInternalActivationDormant {
        fn poll_bind_qualification(
            &mut self,
            schedule: &crate::coordination::AttemptSchedule,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let signals = Arc::clone(&self.activation);
            let failure = self.failure.clone();
            let activation = self.inner.activate(schedule, admissions, cancellation);
            Box::pin(async move {
                signals.entered.store(true, Ordering::SeqCst);
                signals.release.notified().await;
                let result = if let Some(input) = failure {
                    Err(NativeAttemptActivationFailure::new(
                        NativeAttemptPreparationFailure::from_input(input),
                    ))
                } else {
                    activation.await
                };
                signals.returned.store(true, Ordering::SeqCst);
                result
            })
        }

        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            let native = Arc::clone(&self.inner.signals);
            let convergence = self.inner.converge(cancellation);
            Box::pin(async move {
                native.cleanup_entered.store(true, Ordering::SeqCst);
                native.release_cleanup.notified().await;
                convergence.await?;
                native.cleanup_completed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    async fn assert_internal_cancel_survives_held_initial_activation(
        activation_failure: Option<ExecutionFailureInput>,
        namespace: u64,
    ) {
        let mut control =
            {
                let (control, root) = governance().await;
                let scope = root.owner.scope();
                let requester = root.owner.cancellation_requester();
                let window = scope
                    .result_capacity()
                    .unwrap()
                    .try_acquire(
                        &scope,
                        novarocks_workload_control::ResultWindowClass::Internal,
                    )
                    .unwrap();
                let native = Arc::new(InternalAttemptSignals::default());
                let activation = Arc::new(HeldInternalActivationSignals::default());
                let consumer = Arc::new(CheckedCountConsumer::default());
                let request = internal_count_request(HeldInternalActivationPreparation {
                    backend: BackendProcessId::new_v7(),
                    window: TestWindow(window.retain_alias()),
                    native: Arc::clone(&native),
                    activation: Arc::clone(&activation),
                    failure: activation_failure.clone(),
                })
                .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
                .unwrap();
                drop(window);
                let (mut supervisor, client) = test_supervisor_with_clock(
                    &control,
                    Handle::current(),
                    Arc::new(BindingNativePort),
                    QueryProcessNamespace::new(namespace),
                    FrontendProcessId::new_v7(),
                    supervisor_config(),
                    Arc::new(FixedComputationClock(MonotonicInstant::from_origin(
                        Duration::from_secs(93),
                    ))),
                );
                let start = client.start(request, root.owner);
                wait_preparation_exit_fact(&activation.entered)
                    .await
                    .unwrap();
                requester.request(CancellationReason::Requested).unwrap();
                let result = tokio::time::timeout(Duration::from_secs(2), start)
            .await
            .expect("actor cancellation must not wait for the original Native activation future");
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("cancelled internal initialization cannot return Ready"),
                };
                let expected = ExecutionFailureInput::new(
                    QueryFailureReason::Cancelled.failure_fact(),
                    SafeDetail::truncating("logical execution was cancelled before success EOF"),
                    ExecutionFailureSite::Coordinator,
                );
                assert_eq!(error.execution_failure(), Some(&expected));
                if let Some(input) = activation_failure.as_ref() {
                    assert_ne!(error.execution_failure(), Some(input));
                }
                // A delivered terminal verdict cannot abandon the original future or
                // turn an observation timeout into actual Native owner exit.
                wait_preparation_exit_fact(&consumer.local_closed)
                    .await
                    .unwrap();
                assert!(!activation.returned.load(Ordering::SeqCst));
                assert!(!activation.dormant_dropped.load(Ordering::SeqCst));
                assert!(!native.cleanup_entered.load(Ordering::SeqCst));
                assert!(!native.cleanup_completed.load(Ordering::SeqCst));
                assert!(consumer.accepted.lock().unwrap().is_none());
                assert_ne!(control.snapshot().root_responsibilities, 0);

                // This actual Native outcome occurs only after cancellation was
                // accepted and delivered. Its typed Err must remain a later outcome.
                activation.release.notify_one();
                wait_preparation_exit_fact(&activation.returned)
                    .await
                    .unwrap();
                wait_preparation_exit_fact(&native.cleanup_entered)
                    .await
                    .unwrap();
                assert!(!native.cleanup_completed.load(Ordering::SeqCst));
                assert!(!native.run_entered.load(Ordering::SeqCst));
                assert_eq!(error.execution_failure(), Some(&expected));
                native.release_cleanup.notify_one();
                root.business.release();
                supervisor
                    .shutdown_until(Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
                acknowledge_all_control(&control);
                tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                    .await
                    .unwrap();
                assert!(activation.dormant_dropped.load(Ordering::SeqCst));
                assert!(native.cleanup_completed.load(Ordering::SeqCst));
                if activation_failure.is_none() {
                    assert!(native.active_dropped.load(Ordering::SeqCst));
                }
                assert!(consumer.accepted.lock().unwrap().is_none());
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);

                control
            };
        acknowledge_all_control(&control);
        control.finish().await;
    }

    #[tokio::test]
    async fn internal_actor_cancel_returns_before_held_initial_native_activation_exits() {
        assert_internal_cancel_survives_held_initial_activation(None, 0x76).await;
    }

    #[tokio::test]
    async fn internal_initial_activation_error_cannot_replace_accepted_actor_cancel() {
        assert_internal_cancel_survives_held_initial_activation(
            Some(fixture_execution_input(
                "injected Native activation error after accepted cancellation",
            )),
            0x77,
        )
        .await;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum NativeFactoryPanicPhase {
        Activate,
        Run,
    }

    #[derive(Debug)]
    struct FactoryPanickingPreparation {
        inner: HeldInternalActivationPreparation,
        phase: NativeFactoryPanicPhase,
        factory_called: Arc<AtomicBool>,
    }

    impl NativeAttemptPreparationPort for FactoryPanickingPreparation {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            let scheduling = no_scan_scheduling(request);
            let owner = FactoryPanickingDormant {
                inner: HeldInternalActivationDormant {
                    inner: InternalCountDormant {
                        backend: self.inner.backend,
                        window: self.inner.window.clone(),
                        signals: Arc::clone(&self.inner.native),
                    },
                    activation: Arc::clone(&self.inner.activation),
                    failure: None,
                },
                phase: self.phase,
                factory_called: Arc::clone(&self.factory_called),
            };
            request.stage_fixture_owner(owner);
            Box::pin(async move { request.bind_staged(scheduling).map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct FactoryPanickingDormant {
        inner: HeldInternalActivationDormant,
        phase: NativeFactoryPanicPhase,
        factory_called: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for FactoryPanickingDormant {
        fn poll_bind_qualification(
            &mut self,
            schedule: &crate::coordination::AttemptSchedule,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            self.inner.poll_bind_qualification(schedule, cx)
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            if self.phase == NativeFactoryPanicPhase::Activate {
                self.factory_called.store(true, Ordering::SeqCst);
                // Deliberately before returning any future to the caller.
                panic!("injected synchronous Native activation factory panic");
            }
            let activation = self.inner.activate(schedule, admissions, cancellation);
            let factory_called = Arc::clone(&self.factory_called);
            Box::pin(async move {
                let (owner, rows) = activation.await?.into_parts();
                let rows = rows.expect("healthy internal fixture retains its exact root runtime");
                Ok(ActivatedNativeAttempt::relayed_rows(
                    FactoryPanickingActive {
                        inner: owner,
                        factory_called,
                    },
                    rows.binding,
                    rows.statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.inner.converge(cancellation)
        }
    }

    #[derive(Debug)]
    struct FactoryPanickingActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        factory_called: Arc<AtomicBool>,
    }

    impl ActiveNativeAttemptOwner for FactoryPanickingActive {
        fn run<'a>(
            &'a mut self,
            _: &'a NativeAttemptDrive,
            _: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            self.factory_called.store(true, Ordering::SeqCst);
            // Deliberately before returning any future to the caller.
            panic!("injected synchronous Native run factory panic");
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            self.inner.converge(request)
        }
    }

    async fn assert_native_factory_panic_retains_original_owner(
        phase: NativeFactoryPanicPhase,
        namespace: u64,
    ) {
        let mut control = {
            let (control, root) = governance().await;
            let scope = root.owner.scope();
            let window = scope
                .result_capacity()
                .unwrap()
                .try_acquire(
                    &scope,
                    novarocks_workload_control::ResultWindowClass::Internal,
                )
                .unwrap();
            let native = Arc::new(InternalAttemptSignals::default());
            let activation = Arc::new(HeldInternalActivationSignals::default());
            // Run-factory failure needs successful activation, not an activation gate.
            activation.release.notify_one();
            let factory_called = Arc::new(AtomicBool::new(false));
            let consumer = Arc::new(CheckedCountConsumer::default());
            let request = internal_count_request(FactoryPanickingPreparation {
                inner: HeldInternalActivationPreparation {
                    backend: BackendProcessId::new_v7(),
                    window: TestWindow(window.retain_alias()),
                    native: Arc::clone(&native),
                    activation: Arc::clone(&activation),
                    failure: None,
                },
                phase,
                factory_called: Arc::clone(&factory_called),
            })
            .with_internal_consumer(Arc::new(ObservedCountConsumer(Arc::clone(&consumer))))
            .unwrap();
            drop(window);
            let (mut supervisor, client) = test_supervisor_with_clock(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(namespace),
                FrontendProcessId::new_v7(),
                supervisor_config(),
                Arc::new(FixedComputationClock(MonotonicInstant::from_origin(
                    Duration::from_secs(94),
                ))),
            );
            let mut original_exit = None;
            let start = {
                let mut input = match client.prepare_observed_start(request, None) {
                    Ok(input) => Some(input),
                    Err(original) => std::panic::panic_any(original),
                };
                let mut owner = Some(root.owner);
                let mut ready = None;
                client
                    .start_observed_in(&mut input, &mut owner, &mut ready, &mut original_exit)
                    .unwrap()
                    .finish()
                    .unwrap();
                ready.take().unwrap()
            };
            let result = tokio::time::timeout(Duration::from_secs(2), start)
                .await
                .expect("factory panic must become an actor verdict before held cleanup exits");
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("a Native factory panic cannot produce internal Ready"),
            };
            let expected = ExecutionFailureInput::new(
                QueryFailureReason::NativeFuturePanicked.failure_fact(),
                SafeDetail::truncating(match phase {
                    NativeFactoryPanicPhase::Activate => "Native attempt activation panicked",
                    NativeFactoryPanicPhase::Run => "Native row attempt run panicked",
                }),
                ExecutionFailureSite::Coordinator,
            );
            assert_eq!(error.execution_failure(), Some(&expected));
            assert!(factory_called.load(Ordering::SeqCst));
            wait_preparation_exit_fact(&native.cleanup_entered)
                .await
                .unwrap();
            assert!(!native.cleanup_completed.load(Ordering::SeqCst));
            assert!(!native.active_dropped.load(Ordering::SeqCst));
            assert_ne!(control.snapshot().root_responsibilities, 0);
            if phase == NativeFactoryPanicPhase::Activate {
                assert!(!activation.dormant_dropped.load(Ordering::SeqCst));
            } else {
                assert_eq!(supervisor.registry.active_native_custody_count(), 1);
            }
            assert!(consumer.accepted.lock().unwrap().is_none());

            native.release_cleanup.notify_one();
            root.business.release();
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await;
            if phase == NativeFactoryPanicPhase::Activate {
                assert!(
                    matches!(
                        &shutdown,
                        Err(LogicalExecutionSupervisorShutdownError::Registry(
                            LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved
                        ))
                    ),
                    "original activation shutdown: {shutdown:?}"
                );
                let mut exit = original_exit.take().expect("the same original Source");
                wait_for_dormant_failure(&mut exit).await;
                let view = exit.view().unwrap();
                assert!(view.native_dormant_failed());
                assert!(!view.native_preparation_exited());
                drop(view);
                let failure = exit.dormant_failure().unwrap();
                assert_eq!(
                    failure.activation_panic().unwrap().downcast_ref::<&str>(),
                    Some(&"injected synchronous Native activation factory panic")
                );
                assert!(failure.convergence_panic().is_none());
                assert!(failure.reported_failure().is_none());
                drop(failure);
                assert!(futures::poll!(std::future::poll_fn(|cx| exit.poll_exit(cx))).is_pending());
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                assert_eq!(control.snapshot().result_windows.held_positions[2], 1);
                assert!(!activation.dormant_dropped.load(Ordering::SeqCst));
                assert!(native.cleanup_completed.load(Ordering::SeqCst));
                // The root itself really joined; its failed Source remains
                // retained by registry/health and cannot report local exit.
                assert!(supervisor.join.is_none());
                assert!(supervisor.run_outcome.is_some());
                assert!(!supervisor.shutdown_complete);
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);
                drop(exit);
                supervisor.abandon_for_process_exit();
            } else {
                assert!(
                    matches!(
                        &shutdown,
                        Err(LogicalExecutionSupervisorShutdownError::Registry(
                            LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved
                        ))
                    ),
                    "original run factory shutdown: {shutdown:?}"
                );
                let exit = original_exit
                    .take()
                    .expect("same original run factory Source");
                let view = exit.view().unwrap();
                assert_eq!(
                    view.native_run_panic().unwrap().downcast_ref::<&str>(),
                    Some(&"injected synchronous Native run factory panic")
                );
                assert!(view.native_run_cleanup_panic().is_none());
                assert!(view.native_preparation_exited());
                assert!(!view.role_failed());
                drop(view);
                assert!(activation.dormant_dropped.load(Ordering::SeqCst));
                assert!(native.cleanup_completed.load(Ordering::SeqCst));
                assert!(native.active_dropped.load(Ordering::SeqCst));
                assert_eq!(supervisor.registry.active_native_custody_count(), 0);
                assert!(futures::poll!(Box::pin(scope.wait_released())).is_pending());
                drop(exit);
                supervisor.abandon_for_process_exit();
            }

            control
        };
        control.finish_for_process_exit().await;
    }

    #[tokio::test]
    async fn synchronous_native_activation_factory_panic_retains_dormant_owner() {
        assert_native_factory_panic_retains_original_owner(NativeFactoryPanicPhase::Activate, 0x78)
            .await;
    }

    #[tokio::test]
    async fn synchronous_native_run_factory_panic_retains_active_owner() {
        assert_native_factory_panic_retains_original_owner(NativeFactoryPanicPhase::Run, 0x79)
            .await;
    }
}
