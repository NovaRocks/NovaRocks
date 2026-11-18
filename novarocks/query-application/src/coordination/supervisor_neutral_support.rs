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

// Included in supervisor::tests; same original paid bank, final Native API only.
#[derive(Clone, Copy, Debug)]
enum FixtureFailureReason {
    InjectedExecution,
}
impl FailureProjection for FixtureFailureReason {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
        match self {
            Self::InjectedExecution => FailureFact::new(
                FailureClass::Unexpected,
                FailureReason::new(
                    std::num::NonZeroU16::MAX,
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
        }
    }
}
fn fixture_execution_input(message: &'static str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        FixtureFailureReason::InjectedExecution.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    )
}
fn fixture_cancellation_input(
    reason: CancellationReason,
    message: &'static str,
) -> ExecutionFailureInput {
    let reason = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => {
            QueryFailureReason::DeadlineExceeded
        }
        _ => QueryFailureReason::Cancelled,
    };
    ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    )
}

fn supervisor_config() -> LogicalExecutionSupervisorConfig {
    LogicalExecutionSupervisorConfig::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        LogicalExecutionRowsConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU32::new(2).unwrap(),
            Duration::from_secs(30),
        ),
    )
}

fn fixture_version() -> [u8; 16] {
    static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_VERSION.fetch_add(1, Ordering::Relaxed);
    let mut version = [72; 16];
    version[8..].copy_from_slice(&sequence.to_be_bytes());
    version
}

fn completion_request(attempts: impl NativeAttemptPreparationPort) -> QueryExecutionRequest {
    let plan = crate::completed_plan_fixture::completed_noop_plan(fixture_version());
    let description = FrozenExecutionDescription::for_completed_plan(
        crate::api::QueryExecutionKind::Maintenance,
        plan.candidate().clone(),
        Vec::new(),
        OutputContract::CompletionOnly,
        novarocks_coordination_contract::ExecutionEffect::None,
        RecoveryMode::NoRecovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap();
    QueryExecutionRequest::bind_native(description, None, attempts)
}

fn acknowledge_all_control(control: &WorkloadControl) {
    while let Some(permit) = control.next_control() {
        permit.acknowledge();
    }
}

async fn wait_preparation_exit_fact(flag: &AtomicBool) -> Result<(), &'static str> {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !flag.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "fixture fact was not observed within its bound")
}

#[derive(Debug)]
struct UnreachablePreparationPort;

impl NativeAttemptPreparationPort for UnreachablePreparationPort {
    fn prepare_in<'a>(
        &'a mut self,
        _request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        Box::pin(async { panic!("unreachable Native attempt preparation") })
    }
}

#[derive(Debug)]
struct DropTrackedPreparationPort {
    drops: Arc<AtomicU64>,
}

impl Drop for DropTrackedPreparationPort {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl NativeAttemptPreparationPort for DropTrackedPreparationPort {
    fn prepare_in<'a>(
        &'a mut self,
        _request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        Box::pin(async { panic!("rejected Native seed must never prepare an attempt") })
    }
}

fn drop_tracked_request(drops: &Arc<AtomicU64>) -> QueryExecutionRequest {
    completion_request(DropTrackedPreparationPort {
        drops: Arc::clone(drops),
    })
}

struct CaptureAdmittedCapacityPort {
    captured:
        Option<oneshot::Sender<crate::admitted_query_context::QueryResultCapacityBinding>>,
}
impl fmt::Debug for CaptureAdmittedCapacityPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CaptureAdmittedCapacityPort")
    }
}
impl NativeAttemptPreparationPort for CaptureAdmittedCapacityPort {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        self.captured
            .take()
            .unwrap()
            .send(request.result_capacity().unwrap().clone())
            .unwrap_or_else(|_| panic!("original admitted binding receiver exited"));
        Box::pin(async {
            Err(
                NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                    "observed original preparation request",
                ))
                .into(),
            )
        })
    }
}

#[derive(Debug)]
    struct BindingNativePort;

    impl LogicalExecutionNativePort for BindingNativePort {
        fn bind_in(
            &self,
            source: &mut Option<LogicalNativeOpenRequest>,
            target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            LogicalNativeOpenRequest::bind_in(source, target).map_err(Into::into)
        }
    }

#[derive(Debug)]
    struct PanickingOpenNativePort;

    impl LogicalExecutionNativePort for PanickingOpenNativePort {
        fn bind_in(
            &self,
            _source: &mut Option<LogicalNativeOpenRequest>,
            _target: &mut Option<LogicalNativeSession>,
        ) -> Result<(), LogicalNativeOpenError> {
            panic!("injected Native bind panic")
        }
    }

#[derive(Debug)]
struct RecordingRejectedOpen {
    opened: Arc<std::sync::Mutex<Vec<QueryExecutionId>>>,
}

impl LogicalExecutionNativePort for RecordingRejectedOpen {
    fn bind_in(
        &self,
        source: &mut Option<LogicalNativeOpenRequest>,
        _target: &mut Option<LogicalNativeSession>,
    ) -> Result<(), LogicalNativeOpenError> {
        self.opened
            .lock()
            .unwrap()
            .push(source.as_ref().unwrap().initial_execution());
        Err(crate::api::LogicalNativeOpenFailure::rejected(
            "injected Native bind rejection after identity observation",
        )
        .into())
    }
}

#[derive(Debug)]
enum OriginalPreparationGateKind {
    Pending(Arc<tokio::sync::Notify>),
    DelayedCancel,
}
#[derive(Debug)]
struct OriginalPreparationGate {
    inner: Box<dyn NativeAttemptPreparationPort>,
    gate: OriginalPreparationGateKind,
}
impl OriginalPreparationGate {
    fn pending(
        inner: impl NativeAttemptPreparationPort,
        entered: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            gate: OriginalPreparationGateKind::Pending(entered),
        }
    }
    fn delayed_cancel(inner: impl NativeAttemptPreparationPort) -> Self {
        Self {
            inner: Box::new(inner),
            gate: OriginalPreparationGateKind::DelayedCancel,
        }
    }
}
impl NativeAttemptPreparationPort for OriginalPreparationGate {
    fn prepare_in<'a>(
        &'a mut self,
        request: &'a mut crate::api::NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        // Retain the original preparation input until this same gate exits.
        let _original = &self.inner;
        match &self.gate {
            OriginalPreparationGateKind::Pending(entered) => {
                entered.notify_one();
                Box::pin(std::future::pending())
            }
            OriginalPreparationGateKind::DelayedCancel => {
                let cancellation = request.cancellation();
                Box::pin(async move {
                    let reason = cancellation.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Err(
                        NativeAttemptPreparationFailure::from_input(fixture_cancellation_input(
                            reason,
                            "server shutdown interrupted Native preparation",
                        ))
                        .into(),
                    )
                })
            }
        }
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

fn internal_count_request(
    attempts: impl NativeAttemptPreparationPort,
) -> QueryExecutionRequest {
    internal_count_request_with_recovery(attempts, RecoveryMode::NoRecovery, None)
}

fn internal_count_request_with_recovery(
    attempts: impl NativeAttemptPreparationPort,
    recovery: RecoveryMode,
    replacements: Option<Arc<dyn crate::coordination::ReplacementQualificationEffectPort>>,
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
        replacements,
        attempts,
    )
}

// Preinstall-only observation. No consumer future, decoder, Root read or Ready
// authority exists unless the original installation actually invokes consume.
#[derive(Debug, Default)]
struct CheckedCountConsumer {
    decoded: AtomicBool,
    local_closed: AtomicBool,
    accepted: std::sync::Mutex<Option<ComputationReadyToken>>,
    ready_observation: std::sync::Mutex<Option<()>>,
    consume_calls: AtomicU64,
}
#[derive(Debug)]
struct ObservedCountConsumer(Arc<CheckedCountConsumer>);
impl InternalComputationConsumer for ObservedCountConsumer {
    fn begin_local_close(&self) {}
    fn poll_local_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.0.local_closed.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
    fn consume(&self, _: InternalConsumerActivation) -> InternalConsumerFuture {
        self.0.consume_calls.fetch_add(1, Ordering::SeqCst);
        *self.0.ready_observation.lock().unwrap() = Some(());
        panic!("preinstall refusal must not activate the original consumer")
    }
}
