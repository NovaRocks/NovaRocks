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

// Included in the original supervisor fixture; no extra runtime or authority.
mod client_computation_tests {
    use super::*;
    use crate::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::joint_admission::{
        InternalComputationGrant, JointLane, JointResource, JointResources, NativeAsyncActionQuote,
        RetainedPayloadGrant,
    };
    use novarocks_workload_control::{WorkError, WorkOwner, WorkScope};
    use std::num::NonZeroU64;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    #[derive(Debug, Default)]
    struct Signals {
        sealed: AtomicBool,
        local_run_exited: AtomicBool,
        cleanup_entered: AtomicBool,
        cleanup_exited: AtomicBool,
        release_cleanup: tokio::sync::Notify,
    }
    struct SealPort {
        sender: std::sync::Mutex<Option<super::super::super::AcceptedRootStatusSender>>,
        signals: Arc<Signals>,
    }
    impl fmt::Debug for SealPort {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("ClientCutSealPort")
        }
    }
    impl super::super::super::AcceptedRootControlPort for SealPort {
        fn enqueue_root_control(
            &self,
            request: super::super::super::AcceptedRootControlRequest,
        ) -> Result<(), super::super::super::AcceptedRootControlRequest> {
            if request.terminal_control_root().is_some() {
                request.accept_terminal_control().unwrap();
            } else {
                request
                    .accept(self.sender.lock().unwrap().take().unwrap(), Vec::new())
                    .unwrap();
                self.signals.sealed.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }
    struct EndPort {
        scalar_schema: Option<novarocks_result_contract::ScalarSchema>,
    }
    impl crate::api::BoundedRootReadPort for EndPort {
        fn read(
            &self,
            request: novarocks_execution_contract::root_result::RootResultRead,
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
            let scalar_schema = self.scalar_schema.clone();
            Box::pin(async move {
                use novarocks_execution_contract::root_result::*;
                let outcome = match request.wanted() {
                    Some(sequence) if scalar_schema.is_some() => {
                        use novarocks_result_contract::{BorrowedScalarLeaf, ScalarLeafCursor};
                        assert_eq!(sequence, NonZeroU64::MIN);
                        let cursor = ScalarLeafCursor::try_new(
                            scalar_schema.as_ref().unwrap(),
                            BorrowedScalarLeaf::NoRows,
                        )
                        .unwrap();
                        let mut body = vec![0; cursor.encoded_len()];
                        cursor.copy_range(0, &mut body).unwrap();
                        RootReadOutcome::Data(
                            RootResultData::try_new(
                                request.kind(),
                                sequence,
                                bytes::Bytes::from(body),
                                Some(RootResultEnd {
                                    sequence: NonZeroU64::new(2).unwrap(),
                                    output_rows: cursor.rows(),
                                }),
                            )
                            .unwrap(),
                        )
                    }
                    Some(sequence) => RootReadOutcome::End(RootResultEnd {
                        sequence,
                        output_rows: 0,
                    }),
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
                .map_err(|e| fixture_root_reply_failure(request.root_task(), e))
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
    struct Preparation {
        backend: BackendProcessId,
        window: TestWindow,
        signals: Arc<Signals>,
        scalar_schema: Option<novarocks_result_contract::ScalarSchema>,
        count_only: bool,
    }
    impl NativeAttemptPreparationPort for Preparation {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            request.stage_fixture_owner(Dormant {
                backend: self.backend,
                window: self.window.clone(),
                signals: Arc::clone(&self.signals),
                scalar_schema: self.scalar_schema.clone(),
                count_only: self.count_only,
            });
            Box::pin(async move {
                let scheduling = no_scan_scheduling(request);
                request.bind_staged(scheduling).map_err(Into::into)
            })
        }
    }
    #[derive(Debug)]
    struct Dormant {
        backend: BackendProcessId,
        window: TestWindow,
        signals: Arc<Signals>,
        scalar_schema: Option<novarocks_result_contract::ScalarSchema>,
        count_only: bool,
    }
    impl DormantNativeAttemptOwner for Dormant {
        fn poll_bind_qualification(
            &mut self,
            _: &AttemptSchedule,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
            Poll::Ready(Ok(()))
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
            let scalar_schema = self.scalar_schema.clone();
            let count_only = self.count_only;
            Box::pin(async move {
                let control = Arc::new(SealPort {
                    sender: std::sync::Mutex::new(None),
                    signals: Arc::clone(&signals),
                });
                let (sender, statuses) =
                    super::super::super::accepted_root_status_projection_with_control_port(
                        root,
                        control.clone(),
                    );
                sender
                    .publish_attempt_observation(
                        TaskStatus::try_new(
                            root,
                            TaskStatusVersion::new(1).unwrap(),
                            TaskState::Finished,
                            None,
                            TaskOutputFacts::new(true),
                        )
                        .unwrap(),
                        super::super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                *control.sender.lock().unwrap() = Some(sender);
                let mut binding = test_root_binding(root, window.0.try_clone().unwrap(), None);
                if count_only {
                    binding.frontier = super::super::super::RootRelayFrontier::new(
                        root,
                        novarocks_result_contract::RootProfileId::V1,
                        novarocks_result_contract::RootOutputKind::CountOnly,
                        None,
                    )
                    .unwrap();
                } else if scalar_schema.is_some() {
                    binding.frontier = super::super::super::RootRelayFrontier::new(
                        root,
                        novarocks_result_contract::RootProfileId::V1,
                        novarocks_result_contract::RootOutputKind::InternalFacts(
                            novarocks_result_contract::InternalResultDomain::ScalarValueV1,
                        ),
                        None,
                    )
                    .unwrap();
                }
                binding.port = Arc::new(EndPort { scalar_schema });
                Ok(ActivatedNativeAttempt::relayed_rows(
                    Active {
                        contexts,
                        signals,
                        _control: control,
                        _native_tail_window: window,
                    },
                    binding,
                    statuses,
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
    struct RunFrame(Arc<Signals>);
    impl Drop for RunFrame {
        fn drop(&mut self) {
            self.0.local_run_exited.store(true, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct Active {
        contexts: Vec<QueryContextRef>,
        signals: Arc<Signals>,
        _control: Arc<SealPort>,
        _native_tail_window: TestWindow,
    }
    impl ActiveNativeAttemptOwner for Active {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            // The original borrowed run F must actually drop before the cut is consumed.
            let frame = RunFrame(Arc::clone(&self.signals));
            Box::pin(async move {
                let _frame = frame;
                let mut admissions = SuccessfulCompletionActiveOwner {
                    contexts: self.contexts.clone(),
                };
                assert!(matches!(
                    admissions.run(drive, cancellation).await,
                    NativeAttemptTerminal::Completed
                ));
                // The actor now has its original admission/Establish facts.
                // This Native run still waits for the actual Client cut.
                std::future::pending().await
            })
        }
        fn converge<'a>(
            &'a mut self,
            _: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                assert!(self.signals.local_run_exited.load(Ordering::SeqCst));
                self.signals.cleanup_entered.store(true, Ordering::SeqCst);
                self.signals.release_cleanup.notified().await;
                self.signals.cleanup_exited.store(true, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }
    async fn child(root: &WorkScope) -> WorkOwner {
        let mut request = root.request_joint_child().unwrap();
        let mut ready = tokio::time::timeout(Duration::from_secs(2), &mut request)
            .await
            .unwrap()
            .unwrap();
        let mut child = None;
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(root, &mut child)
            .unwrap()
            .notify();
        child.unwrap()
    }
    async fn stage(
        payload: &RetainedPayloadGrant,
        root: &WorkScope,
        owner: &WorkOwner,
        window: Option<&novarocks_workload_control::ResultWindowAlias>,
    ) -> InternalComputationGrant {
        let source = payload.root_stage_source(root, owner).unwrap();
        let source = match window {
            Some(window) => source.for_client_window(window).unwrap(),
            None => source,
        };
        let mut builder = source
            .begin(8 * 1024 * 1024, JointLane::Foreground)
            .unwrap();
        builder.end_payload().unwrap();
        builder.end_stage().unwrap();
        let mut request = builder.submit().unwrap();
        let mut ready = tokio::time::timeout(Duration::from_secs(2), &mut request)
            .await
            .unwrap()
            .unwrap();
        let mut compute = None;
        ready
            .install_in(root, payload, owner, &mut compute)
            .unwrap()
            .notify();
        compute.unwrap()
    }
    pub(super) async fn owned_internal_stage<F: Future>(
        control: &TestGovernance<F>,
        root: &WorkOwner,
    ) -> (WorkOwner, RetainedPayloadGrant, QueryResultCapacityBinding) {
        let scope = root.scope();
        let mut builder = control.admission.begin_paid_proposal(root).unwrap();
        builder
            .begin_new(
                JointLane::Custody,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(JointResource::PayloadBytes, 8 * 1024 * 1024),
            )
            .unwrap();
        builder
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        builder.end_change().unwrap();
        let mut pending = builder.submit().unwrap();
        let mut ready = tokio::time::timeout(Duration::from_secs(2), &mut pending)
            .await
            .unwrap()
            .unwrap();
        let mut raw = ready.new_claims.pop();
        let mut payload = None;
        RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
            .unwrap()
            .notify();
        drop((ready, pending));
        let payload = payload.unwrap();
        let child = child(&scope).await;
        let mut compute = Some(stage(&payload, &scope, &child, None).await);
        let mut capacity = None;
        QueryResultCapacityBinding::from_owned_internal_computation_in(
            &scope,
            &child,
            &payload,
            &mut compute,
            &mut capacity,
        )
        .unwrap();
        assert!(compute.is_none());
        (child, payload, capacity.unwrap())
    }
    #[derive(Debug, Default)]
    struct CountConsumerSignals {
        accepted: AtomicBool,
        frame_dropped: AtomicBool,
        release: tokio::sync::Notify,
    }
    struct CountConsumerFrame(Arc<CountConsumerSignals>);
    impl Drop for CountConsumerFrame {
        fn drop(&mut self) {
            self.0.frame_dropped.store(true, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct HeldCountConsumer {
        signals: Arc<CountConsumerSignals>,
        domain_error: bool,
    }
    impl crate::api::InternalComputationConsumer for HeldCountConsumer {
        fn begin_local_close(&self) {}
        fn poll_local_closed(
            &self,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
            // This CountOnly consumer submits no local CPU work.
            Poll::Ready(Ok(()))
        }
        fn consume(
            &self,
            activation: crate::api::InternalConsumerActivation,
        ) -> crate::api::InternalConsumerFuture {
            let signals = Arc::clone(&self.signals);
            let domain_error = self.domain_error;
            Box::pin(async move {
                let _frame = CountConsumerFrame(Arc::clone(&signals));
                let (mut stream, source) = activation.into_parts();
                assert_eq!(
                    stream.carrier(),
                    crate::api::ResultRowCarrier::relayed(
                        novarocks_result_contract::RootOutputKind::CountOnly,
                        None
                    )
                    .unwrap()
                );
                let crate::api::InternalResultDelivery::End(end) =
                    stream.next().await?.expect("actual CountOnly End")
                else {
                    panic!("CountOnly cannot deliver a data segment")
                };
                let decoded = end.complete().await?;
                let activation = source.activation();
                let ready = source.accept(decoded).await?;
                assert_eq!(ready.activation(), activation);
                signals.accepted.store(true, Ordering::SeqCst);
                signals.release.notified().await;
                if domain_error {
                    Err(QueryExecutionError::from_execution(
                        fixture_execution_input(
                            "ordinary fixture domain error after accepted Ready",
                        ),
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }
    async fn request(
        capacity: QueryResultCapacityBinding,
        mut attempts: Preparation,
        internal: bool,
        count: Option<Arc<dyn crate::api::InternalComputationConsumer>>,
    ) -> QueryExecutionRequest {
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        let plan = if internal {
            crate::completed_plan_fixture::completed_literal_query(
                fixture_version(),
                "SELECT 1 AS a",
            )
            .await
        } else {
            crate::completed_plan_fixture::completed_values_plan_blocking(fixture_version())
        };
        let root_output = if count.is_some() {
            FrozenRootOutput::CountOnly
        } else if internal {
            FrozenRootOutput::ScalarValue(
                plan.candidate()
                    .original_public_result_port()
                    .and_then(|port| port.scalar_schema.clone())
                    .expect("the actual one-column SQL result has its frozen scalar contract"),
            )
        } else {
            let render = novarocks_sql::compiler::client_render_schema(
                plan.candidate().plan(),
                novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                    timezone_offset_seconds: 0,
                },
            )
            .unwrap();
            FrozenRootOutput::ClientRows(render)
        };
        let candidate = crate::preparation::CompletedPhysicalPlanCandidate::for_program(
            plan.candidate()
                .plan()
                .as_ref()
                .clone()
                .with_root_output(RootOutputContract::new(RootProfileId::V1, root_output))
                .unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let output = OutputContract::from_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate.plan(),
        )
        .unwrap();
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate,
            Vec::new(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        attempts.scalar_schema = description.scalar_schema().cloned();
        attempts.count_only = count.is_some();
        let description = if count.is_some() {
            description.into_internal_computation().unwrap()
        } else {
            description
        };
        let request = QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            attempts,
        )
        .with_result_capacity(capacity)
        .unwrap();
        match count {
            Some(consumer) => request.with_internal_consumer(consumer).unwrap(),
            None => request,
        }
    }
    enum ClientObservationFailure {
        Check(&'static str),
        Execution {
            phase: &'static str,
            error: QueryExecutionError,
        },
        Admission {
            phase: &'static str,
            error: WorkError,
        },
    }
    impl fmt::Debug for ClientObservationFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Check(message) => f.debug_tuple("Check").field(message).finish(),
                Self::Execution { phase, error } => f
                    .debug_struct("Execution")
                    .field("phase", phase)
                    .field("error", error)
                    .finish(),
                Self::Admission { phase, error } => f
                    .debug_struct("Admission")
                    .field("phase", phase)
                    .field("error", error)
                    .finish(),
            }
        }
    }
    impl From<&'static str> for ClientObservationFailure {
        fn from(message: &'static str) -> Self {
            Self::Check(message)
        }
    }
    async fn success_cut(hold_activity: bool) {
        streamed_success_cut(hold_activity, false).await;
    }
    async fn streamed_success_cut(hold_activity: bool, internal: bool) {
        streamed_success_cut_with_count_consumer(hold_activity, internal, None).await;
    }
    async fn streamed_success_cut_with_count_consumer(
        hold_activity: bool,
        internal: bool,
        count_error: Option<bool>,
    ) {
        let count_signals = count_error.map(|_| Arc::new(CountConsumerSignals::default()));
        let mut control = governance_with_result_positions(!internal, None, internal.then_some(1));
        let (observations, cleanup, shutdown) = {
            let root = control.begin_root().await;
            let scope = root.owner.scope();
            let mut builder = control.admission.begin_paid_proposal(&root.owner).unwrap();
            builder
                .begin_new(
                    JointLane::Custody,
                    JointResources::ZERO
                        .with(JointResource::ClaimRecords, 1)
                        .with(JointResource::PayloadBytes, 8 * 1024 * 1024),
                )
                .unwrap();
            builder
                .push_dependency(&SharedDependency::LocalHost)
                .unwrap();
            builder.end_change().unwrap();
            let mut pending = builder.submit().unwrap();
            let mut ready = tokio::time::timeout(Duration::from_secs(2), &mut pending)
                .await
                .unwrap()
                .unwrap();
            let mut raw = ready.new_claims.pop();
            let mut payload = None;
            RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
                .unwrap()
                .notify();
            drop(ready);
            drop(pending);
            let payload = payload.unwrap();
            let first = child(&scope).await;
            let window = if internal {
                None
            } else {
                Some(
                    scope
                        .result_capacity()
                        .unwrap()
                        .try_acquire(
                            &scope,
                            novarocks_workload_control::ResultWindowClass::Client,
                        )
                        .unwrap(),
                )
            };
            let stage_window = window.as_ref().map(|window| window.retain_alias());
            let mut compute = Some(stage(&payload, &scope, &first, stage_window.as_ref()).await);
            let activity_alias = (internal && hold_activity)
                .then(|| compute.as_ref().unwrap().retain_alias().unwrap());
            let mut capacity = None;
            if internal {
                QueryResultCapacityBinding::from_owned_internal_computation_in(
                    &scope,
                    &first,
                    &payload,
                    &mut compute,
                    &mut capacity,
                )
                .unwrap();
            } else {
                QueryResultCapacityBinding::from_owned_client_computation_in(
                    &scope,
                    &first,
                    stage_window.as_ref().unwrap(),
                    &payload,
                    &mut compute,
                    &mut capacity,
                )
                .unwrap();
            }
            assert!(
                compute.is_none(),
                "the same positive grant moved into original custody"
            );
            let capacity = capacity.unwrap();
            assert_eq!(
                scope.result_capacity().unwrap().snapshot().held_positions,
                if internal { [0, 0, 1, 0] } else { [1, 0, 0, 0] },
                "each streamed computation uses its actual original position class"
            );
            let original_tail = capacity.try_clone().unwrap();
            let old_manifest = capacity
                .try_window_alias()
                .unwrap()
                .internal_manifest_binding()
                .unwrap()
                .unwrap()
                .clone();
            let old_authority = capacity
                .try_window_alias()
                .unwrap()
                .native_action_authority()
                .unwrap();
            let quote = NativeAsyncActionQuote::for_original_native_transport(
                NonZeroU64::MIN,
                NonZeroU64::MIN,
                NonZeroU64::MIN,
                NonZeroU64::MIN,
                NonZeroU64::MIN,
                NonZeroU64::MIN,
            );
            let mut activity = hold_activity.then(|| {
                if internal {
                    capacity
                        .begin_internal_result_activity(activity_alias.unwrap())
                        .unwrap()
                } else {
                    capacity.begin_result_activity().unwrap()
                }
            });
            let signals = Arc::new(Signals::default());
            let attempts = Preparation {
                backend: BackendProcessId::new_v7(),
                window: TestWindow(capacity.try_window_alias().unwrap()),
                signals: Arc::clone(&signals),
                scalar_schema: None,
                count_only: false,
            };
            let (mut supervisor, client) = test_supervisor(
                &control,
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x78),
                FrontendProcessId::new_v7(),
                supervisor_config().with_remote_cleanup_timeout(Duration::from_secs(10)),
            );
            let second = child(&scope).await;
            let mut second_compute = None;
            let observations: Result<(), ClientObservationFailure> = async {
                let consumer = count_signals.as_ref().map(|signals| {
                    Arc::new(HeldCountConsumer {
                        signals: Arc::clone(signals),
                        domain_error: count_error.unwrap(),
                    }) as Arc<dyn crate::api::InternalComputationConsumer>
                });
                let request = request(capacity, attempts, internal, consumer).await;
                let scalar_schema = request.description().scalar_schema().cloned();
                let mut execution =
                    tokio::time::timeout(Duration::from_secs(2), client.start(request, first))
                        .await
                        .map_err(|_| "Client start timeout")?
                        .map_err(|error| ClientObservationFailure::Execution {
                            phase: "Client start",
                            error,
                        })?;
                let mut exit = execution
                    .take_exit_observer()
                    .ok_or("original exit observer missing")?;
                let rows = if let Some(count) = count_signals.as_ref() {
                    if execution.ready_token().is_none() || execution.take_output().is_some() {
                        return Err("CountOnly must return its real computation Ready".into());
                    }
                    wait_preparation_exit_fact(&count.accepted).await?;
                    None
                } else {
                    let Some(crate::api::ExecutionOutput::Rows(mut rows)) = execution.take_output()
                    else {
                        return Err("Rows output missing".into());
                    };
                    rows.begin_schema()
                        .ok_or("Client schema delivery missing")?
                        .complete();
                    if internal {
                        use novarocks_result_contract::{BorrowedScalarLeaf, BorrowedScalarRecord};
                        let Some(crate::api::ResultDelivery::Segment(segment)) =
                            tokio::time::timeout(Duration::from_secs(2), rows.next())
                                .await
                                .map_err(|_| "actual Internal scalar record timeout")?
                                .map_err(|error| ClientObservationFailure::Execution {
                                    phase: "Internal scalar record",
                                    error,
                                })?
                        else {
                            return Err(
                                "ScalarValue requires its explicit original NoRows record".into()
                            );
                        };
                        let frozen = scalar_schema
                            .as_ref()
                            .expect("the actual frozen Internal scalar schema");
                        let record = BorrowedScalarRecord::try_decode(frozen, segment.body())
                            .map_err(
                                |_| "the actual scalar record must match its frozen contract",
                            )?;
                        if record.rows() != 0 || segment.rows() != record.rows() {
                            return Err(
                                "the original scalar NoRows record must retain zero rows".into()
                            );
                        }
                        if !matches!(
                            BorrowedScalarLeaf::decode(frozen, segment.body()),
                            Ok(BorrowedScalarLeaf::NoRows)
                        ) {
                            return Err("the original scalar record must represent NoRows".into());
                        }
                        segment.complete();
                    }
                    let delivery = tokio::time::timeout(Duration::from_secs(2), rows.next())
                        .await
                        .map_err(|_| "actual Client EOF timeout")?
                        .map_err(|error| ClientObservationFailure::Execution {
                            phase: "Client EOF",
                            error,
                        })?;
                    let Some(crate::api::ResultDelivery::End(end)) = delivery else {
                        return Err("empty root must deliver its actual success EOF".into());
                    };
                    if end.root_output_rows() != Some(0) {
                        return Err("empty root EOF must retain its actual zero row count".into());
                    }
                    end.complete();

                    wait_preparation_exit_fact(&signals.sealed).await?;
                    Some(rows)
                };
                let source = payload.root_stage_source(&scope, &second).unwrap();
                let source = match stage_window.as_ref() {
                    Some(window) => source.for_client_window(window).unwrap(),
                    None => source,
                };
                let mut builder = source
                    .begin(8 * 1024 * 1024, JointLane::Foreground)
                    .unwrap();
                builder.end_payload().unwrap();
                builder.end_stage().unwrap();
                let mut next = builder.submit().unwrap();
                if hold_activity || count_signals.is_some() {
                    // The actual computation position remains busy. The SAME
                    // request waits fairly and must not receive a quota error.
                    if tokio::time::timeout(Duration::from_millis(20), &mut next)
                        .await
                        .is_ok()
                    {
                        return Err(
                            "held activity released the actual Client position early".into()
                        );
                    }
                    if signals.cleanup_entered.load(Ordering::SeqCst) {
                        return Err("cleanup passed held activity".into());
                    }
                }
                if let Some(count) = count_signals.as_ref() {
                    assert!(
                        !count.frame_dropped.load(Ordering::SeqCst),
                        "Ready cannot retire a live consumer F"
                    );
                    count.release.notify_one();
                    wait_preparation_exit_fact(&count.frame_dropped).await?;
                }
                drop(activity.take());
                // The real local producer/activity cut retires its position;
                // held Native cleanup still has not exited. Reuse the SAME
                // pending request, not a replacement admission or authority.
                wait_preparation_exit_fact(&signals.cleanup_entered).await?;
                let mut ready = tokio::time::timeout(Duration::from_secs(2), &mut next)
                    .await
                    .map_err(|_| "actual second P=1 stage did not fit before Native cleanup")?
                    .map_err(|failure| ClientObservationFailure::Admission {
                        phase: "second P=1 stage after actual local exit",
                        error: failure.error,
                    })?;
                ready
                    .install_in(&scope, &payload, &second, &mut second_compute)
                    .unwrap()
                    .notify();
                drop(ready);
                drop(next);
                let expected = if internal { [0, 0, 1, 0] } else { [1, 0, 0, 0] };
                if scope.result_capacity().unwrap().snapshot().held_positions != expected {
                    return Err("streamed stage consumed a foreign position class".into());
                }
                if signals.cleanup_exited.load(Ordering::SeqCst) {
                    return Err("Native cleanup was not held".into());
                }
                if exit
                    .poll_exit(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
                {
                    return Err("Native exit was fabricated at Client EOF".into());
                }
                if old_manifest.try_begin().is_ok() {
                    return Err("old manifest reopened P".into());
                }
                if old_authority
                    .action_source(JointLane::Foreground, quote)
                    .is_ok()
                {
                    return Err("old FG authority reopened after P exit".into());
                }
                if old_authority
                    .action_source(JointLane::Settlement, quote)
                    .is_err()
                {
                    return Err("actual tail settlement lost its original authority".into());
                }
                original_tail
                    .try_window_alias()
                    .unwrap()
                    .check_backing_total(8 * 1024 * 1024)
                    .unwrap();
                drop(rows);
                drop(execution);
                drop(exit);
                Ok(())
            }
            .await;
            eprintln!(
                "streamed internal={internal}, hold_activity={hold_activity}, observations={observations:?}, actual_exit_snapshot: sealed={}, local_run_exited={}, cleanup_entered={}, cleanup_exited={}",
                signals.sealed.load(Ordering::SeqCst),
                signals.local_run_exited.load(Ordering::SeqCst),
                signals.cleanup_entered.load(Ordering::SeqCst),
                signals.cleanup_exited.load(Ordering::SeqCst),
            );
            if let Some(count) = count_signals.as_ref() {
                count.release.notify_one();
            }
            drop(activity.take());
            signals.release_cleanup.notify_one();
            let cleanup = wait_preparation_exit_fact(&signals.cleanup_exited).await;
            let shutdown = supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(2))
                .await;
            drop((
                second_compute,
                old_authority,
                old_manifest,
                original_tail,
                stage_window,
                window,
                payload,
            ));
            second.complete();
            root.business.release();
            root.owner.complete();
            (observations, cleanup, shutdown)
        };
        control.finish().await;
        assert!(cleanup.is_ok(), "{cleanup:?}");
        assert!(shutdown.is_ok(), "{shutdown:?}");
        assert!(observations.is_ok(), "{observations:?}");
    }
    #[tokio::test]
    async fn client_success_releases_actual_p1_before_original_native_cleanup() {
        success_cut(false).await;
    }
    #[tokio::test]
    async fn client_success_seal_waits_actual_activity_exit_before_p1_reuse() {
        success_cut(true).await;
    }
    #[tokio::test]
    async fn internal_read_success_releases_actual_p1_before_original_native_cleanup() {
        streamed_success_cut(false, true).await;
    }
    #[tokio::test]
    async fn internal_read_success_cut_waits_actual_activity_before_p1_reuse() {
        streamed_success_cut(true, true).await;
    }
    #[tokio::test]
    async fn count_only_ready_retains_p1_until_actual_consumer_and_pump_exit() {
        streamed_success_cut_with_count_consumer(false, true, Some(false)).await;
    }
    #[tokio::test]
    async fn count_only_late_domain_error_releases_p1_after_actual_consumer_exit() {
        streamed_success_cut_with_count_consumer(false, true, Some(true)).await;
    }
}
