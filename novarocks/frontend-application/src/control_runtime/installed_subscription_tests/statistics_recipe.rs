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

//! Canonical statistics DATA through the actual QA/LP/AP installation path.
//! The finite peer authors no statistics query, M5 proof or Worker execution.

#[path = "statistics_recipe/publication.rs"]
mod publication;
#[path = "statistics_recipe/publication_outcomes.rs"]
mod publication_outcomes;

use super::drive_until_with_original_clock;
use crate::fixtures::native_recipe::{
    ActualLogicalCase, OriginalRecipeContext, OriginalRecipeOperands,
    PreparedOriginalRecipeProcess, install_original_recipe, prepare_original_recipe_process,
};
use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
use crate::query_execution::{
    internal_consumer::{
        decoded::{
            DecodedConsumerRuntime, ReadyStatisticsComputation, StatisticsComputationOutput,
            statistics_computation_consumer,
        },
        domain_cpu::DomainCpuCustody,
        test_fixture::internal_statistics_encoded_description,
    },
    internal_result_cpu::InternalResultCpuOwner,
    native_execution_adapter::source_cohort::NativeSourceCohort,
    statistics::StatisticsCollectionProgram,
};
use arrow::{
    array::{ArrayRef, BinaryArray, Int32Builder, ListBuilder, MapBuilder, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding, api::ExecutionOutputObligation,
    cancellation::QueryCancellationView, coordination::LogicalExecutionSupervisor,
};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableHandle, StatisticsArtifactIdentity,
    StatisticsRequiredAggregation, StatisticsScanColumn,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    ResultCapacityConfig, ResultCapacityHandle, WorkloadControl, joint_admission as joint,
};
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex, OnceLock},
    task::Poll,
    time::{Duration, Instant},
};

pub(crate) fn original_program() -> StatisticsCollectionProgram {
    let identity = StatisticsArtifactIdentity::try_new(vec![1], "test/blob").unwrap();
    let required = StatisticsRequiredAggregation::try_new(
        StatisticsScanColumn::try_new(0, "v", FunctionValueType::new(DataType::Int64, false))
            .unwrap(),
        "$test_stat",
        identity,
    )
    .unwrap();
    let program = StatisticsCollectionProgram::try_new(
        ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("statistics_component").unwrap(),
            Bytes::from_static(b"table"),
        )
        .unwrap(),
        Some(7),
        vec![required],
    )
    .unwrap();
    assert_eq!(program.required_aggregations().len(), 1);
    program
}

/// The ordinary Backend statistics encoder is the only record author. The
/// fixture uses the same four-column relation as statistics.rs's decoder tests.
pub(crate) fn original_statistics_body() -> Vec<u8> {
    use novarocks_native_adapter::root_statistics_codec::{
        StatisticsArtifactEncoder, StatisticsCodecStatus, StatisticsCodecTotals,
    };
    let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    fields.values().append_value(1);
    fields.append(true);
    let fields = Arc::new(fields.finish()) as ArrayRef;
    let types = Arc::new(StringArray::from(vec!["test/blob"])) as ArrayRef;
    let bodies = Arc::new(BinaryArray::from_iter_values([
        b"original artifact".as_slice()
    ])) as ArrayRef;
    let mut properties = MapBuilder::new(
        Some(arrow::array::MapFieldNames {
            entry: "entries".to_owned(),
            key: "key".to_owned(),
            value: "value".to_owned(),
        }),
        arrow::array::StringBuilder::new(),
        arrow::array::StringBuilder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    properties.append(true).unwrap();
    let properties = Arc::new(properties.finish()) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("input_fields", fields.data_type().clone(), false),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("body", DataType::Binary, true),
        Field::new("properties", properties.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![fields, types, bodies, properties]).unwrap();
    let mut encoder =
        StatisticsArtifactEncoder::try_new(batch, StatisticsCodecTotals::default()).unwrap();
    let mut body = Vec::new();
    let mut output = vec![0_u8; 9];
    loop {
        let turn = encoder.step(&mut output).unwrap();
        body.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == StatisticsCodecStatus::InputComplete {
            break;
        }
    }
    body
}

pub(crate) fn drive_typed(driver: &mut joint::JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), joint::JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite Joint driver did not reach idle");
}

pub(crate) fn typed_statistics_batch(
    root: &novarocks_workload_control::RootWork,
    payload: &joint::RetainedPayloadGrant,
    stage: &novarocks_workload_control::WorkOwner,
    driver: &mut joint::JointProgressDriver,
) -> joint::InternalComputationGrant {
    let mut proposal = payload
        .root_stage_source(&root.owner.scope(), stage)
        .unwrap()
        .begin(0, joint::JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut admission = proposal.submit().unwrap();
    drive_typed(driver);
    let mut ready = match std::pin::Pin::new(&mut admission)
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("the original cap-one compute position must be available"),
    };
    let mut computation = None;
    ready
        .install_in(&root.owner.scope(), payload, stage, &mut computation)
        .unwrap()
        .notify();
    drop(ready);
    drop(admission);
    drive_typed(driver);
    computation.unwrap()
}

pub(crate) fn typed_statistics_child(
    root: &novarocks_workload_control::RootWork,
    driver: &mut joint::JointProgressDriver,
) -> novarocks_workload_control::WorkOwner {
    let scope = root.owner.scope();
    let mut pending = scope.request_joint_child().unwrap();
    drive_typed(driver);
    let mut ready = match std::pin::Pin::new(&mut pending)
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original paid child reply must complete"),
    };
    let mut target = None;
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&scope, &mut target)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive_typed(driver);
    target.unwrap()
}

// The actual original Root/result owner and closed CPU factory. Source
// identity grants no bytes; real collector admission uses the Result budget.
pub(crate) struct TypedStatisticsCustody {
    pub(crate) root: novarocks_workload_control::RootWork,
    pub(crate) payload: joint::RetainedPayloadGrant,
    pub(crate) computation: Option<joint::InternalComputationGrant>,
    pub(crate) retired: novarocks_workload_control::WorkRetirementObserver,
    pub(crate) driver: joint::JointProgressDriver,
    pub(crate) progress: novarocks_workload_control::DeadlineExpiryHandle,
}

pub(crate) fn admit_statistics_originals(
    control: &novarocks_workload_control::WorkloadControl,
    driver: &mut joint::JointProgressDriver,
) -> (
    novarocks_workload_control::RootWork,
    joint::RetainedPayloadGrant,
    novarocks_workload_control::WorkOwner,
    joint::InternalComputationGrant,
    novarocks_workload_control::WorkRetirementObserver,
) {
    use novarocks_workload_control::{WorkClass, WorkRequest};
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    // The real initial source installs LocalHost and the original payload pin
    // together. No raw New claim or test-only paid-incarnation shortcut.
    let domains = scope.prepare_root_domains().unwrap();
    let source = domains
        .initial_payload_source(&scope, joint::JointLane::Custody)
        .unwrap();
    let mut proposal = match source
        .try_begin(
            NonZeroU64::new(std::mem::size_of::<TypedStatisticsCustody>() as u64).unwrap(),
            NonZeroU64::new(original_statistics_body().len() as u64).unwrap(),
        )
        .unwrap()
    {
        joint::JointTryBegin::Started(proposal) => proposal,
        joint::JointTryBegin::Full(_) => {
            panic!("fresh original Root source cannot have an outstanding constructor")
        }
    };
    proposal
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    let mut initial = proposal.submit().unwrap();
    drive_typed(driver);
    let mut payload = None;
    match initial.poll_install_in(
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
        &scope,
        &mut payload,
    ) {
        Poll::Ready(Ok(notice)) => notice.notify(),
        _ => panic!("the original initial Root source must install"),
    }
    drop(initial);
    drive_typed(driver);
    let payload = payload.unwrap();
    let stage = typed_statistics_child(&root, driver);
    let retired = stage.retirement_observer().unwrap();
    let computation = typed_statistics_batch(&root, &payload, &stage, driver);
    (root, payload, stage, computation, retired)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatisticsRecipeCase {
    Success,
    TypedRootSuccess,
    PublicationWhileCleanupPending,
    PublicationOutcome(publication_outcomes::PublicationOutcomeCase),
    MissingArtifact,
    ProtocolFailureFirst,
    OriginalFailureStreamAfterEnd,
}
impl StatisticsRecipeCase {
    fn is_success(self) -> bool {
        matches!(
            self,
            Self::Success
                | Self::TypedRootSuccess
                | Self::PublicationWhileCleanupPending
                | Self::PublicationOutcome(_)
        )
    }
    fn is_task_failure(self) -> bool {
        matches!(
            self,
            Self::ProtocolFailureFirst | Self::OriginalFailureStreamAfterEnd
        )
    }
}

fn original_failure() -> novarocks_execution_contract::TaskFailure {
    novarocks_execution_contract::TaskFailure::new_in_phase(
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Data,
            novarocks_execution_contract::failure::FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::MIN,
            ),
        ),
        novarocks_execution_contract::SafeDetail::new("original statistics Task failure").unwrap(),
        novarocks_execution_contract::TaskFailurePhase::Execution,
    )
}

// This guard retains the complete unresolved test owner on assertion unwind.
// It performs no asynchronous close and publishes no exit/release receipt.
struct RetainedStatisticsRun(Option<Box<ActualStatisticsRun>>);
impl Drop for RetainedStatisticsRun {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNRESOLVED: OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                OnceLock::new();
            UNRESOLVED
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
struct ActualStatisticsRun {
    fixture: Option<crate::fixtures::FrontendFixture>,
    logical: Option<ActualLogicalCase>,
    process: Arc<crate::control_runtime::FrontendAttemptRuntime>,
    clock: Arc<crate::task_execution::clock::ManualClock>,
    typed: Option<TypedStatisticsCustody>,
    final_driver: Option<joint::JointProgressDriver>,
    workload: Option<WorkloadControl>,
    supervisor: Option<LogicalExecutionSupervisor>,
    capacity: ResultCapacityHandle,
    cohort: Option<NativeSourceCohort>,
    output: Option<StatisticsComputationOutput>,
    custody: Option<Arc<DomainCpuCustody>>,
    binding: Option<QueryResultCapacityBinding>,
    receiving: Option<ExecutionOutputObligation>,
    ready: Option<ReadyStatisticsComputation>,
    artifact_body: Option<Bytes>,
    publication: Option<
        crate::statistics::attempt_executor::publication_fixture::OriginalStatisticsPublication,
    >,
    publication_future:
        Option<crate::statistics::attempt_executor::publication_fixture::PublicationFuture>,
    publication_cleanup_gate: Option<Arc<crate::fixtures::native_recipe::ReadCloseGate>>,
    session_publication_close_future: Option<crate::statistics::attempt_executor::publication_fixture::session_close::SessionPublicationCloseFuture>,
    installed: Option<Arc<crate::task_execution::host::install::InstalledAttemptHost>>,
    cpu_owner: InternalResultCpuOwner,
    data: FrontendDataRuntime,
    peer: Option<Loopback>,
    history: novarocks_control_runtime::history::HistoryCapture,
    plan: novarocks_query_application::api::PlanSeal,
}
impl ActualStatisticsRun {
    async fn complete(&mut self, case: StatisticsRecipeCase) {
        // Release the same original callback on assertion unwind while the
        // complete unresolved owner remains in RetainedStatisticsRun.
        let _cleanup_release = if matches!(
            case,
            StatisticsRecipeCase::PublicationWhileCleanupPending
                | StatisticsRecipeCase::PublicationOutcome(_)
        ) {
            Some(publication::hold_cleanup(self))
        } else {
            None
        };
        let wait = Duration::from_secs(5);
        let process = self.process.clone();
        self.logical
            .as_ref()
            .unwrap()
            .probe
            .graph_capture_requested
            .store(true, std::sync::atomic::Ordering::Release);
        // The original fixture author runs once inside the Native activation,
        // after the real execution identity exists. Only a Weak is observed.
        self.logical
            .as_ref()
            .unwrap()
            .probe
            .credentials_requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.installed = Some(
            self.fixture
                .as_mut()
                .unwrap()
                .activate_attempt(
                    self.logical.as_ref().unwrap(),
                    &mut self.typed.as_mut().unwrap().driver,
                    false,
                )
                .await,
        );
        let logical = self.logical.as_ref().unwrap();
        let root_task = logical.probe.root.lock().unwrap().unwrap();
        let activation = logical.probe.activation.lock().unwrap().unwrap();
        {
            let observed = logical.probe.graph_capture.lock().unwrap();
            let graph = observed
                .as_ref()
                .expect("same original installed protocol graph");
            assert_eq!(graph.plan, self.plan);
            assert_eq!(graph.root, root_task);
        }
        let kind = novarocks_result_contract::RootOutputKind::InternalFacts(
            novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
        );
        let peer = self.peer.as_ref().unwrap();
        let end = if case == StatisticsRecipeCase::MissingArtifact {
            peer.enable_single_root_lifecycle(root_task, kind);
            novarocks_execution_contract::root_result::RootResultEnd {
                sequence: NonZeroU64::MIN,
                output_rows: 0,
            }
        } else {
            peer.enable_single_root_encoded_result(
                root_task,
                kind,
                Bytes::from(crate::fixtures::native_recipe::original_statistics_body()),
                1,
            )
            .unwrap();
            novarocks_execution_contract::root_result::RootResultEnd {
                sequence: NonZeroU64::new(2).unwrap(),
                output_rows: 1,
            }
        };
        peer.defer_single_root_terminal().unwrap();
        // This fixture observes the exact final ACK on success. Production
        // Release may independently finish before that optional read.
        let _release_guard = case
            .is_success()
            .then(|| peer.hold_single_root_release_for_final_ack().unwrap());
        logical.probe.release_activation();

        // Observe the SAME checked End receipt after real EOF CPU work and
        // End.complete. Neither a peer offer nor final ACK substitutes for it.
        let output = self.output.as_ref().unwrap();
        let checked_end = drive_until_with_original_clock(
            &process,
            &mut self.typed.as_mut().unwrap().driver,
            async {
                while output.decoded_end_for_fixture() != Some(end) {
                    tokio::task::yield_now().await;
                }
            },
            Instant::now() + wait,
            None,
        )
        .await;
        if checked_end.is_none() {
            let lp = logical.binding.bound().unwrap().registration.actor();
            let ap = self.installed.as_ref().unwrap().fixture_sources.0;
            let protocol = logical
                .probe
                .protocol
                .lock()
                .unwrap()
                .as_ref()
                .map(|original| {
                    original
                        .snapshot(activation, root_task, self.plan)
                        .map(|value| value.map(|value| value.tasks))
                });
            eprintln!(
                "original statistics End pending: plan={:?}, activation={:?}, root={:?}, end={:?}, checked={:?}, stream_failure={:?}, protocol={:?}, peer_progress={:?}, cpu_health_retained={}, capacity={:?}, lp={:?}, ap={:?}, lp_pending={:?}, ap_pending={:?}, native={}",
                self.plan,
                activation,
                root_task,
                end,
                output.decoded_end_for_fixture(),
                output.stream_failure_for_fixture(),
                protocol,
                peer.single_root_result_progress(),
                self.custody.as_ref().unwrap().health_failure().is_some(),
                self.capacity.snapshot().held_positions,
                process.runtime.snapshot(lp),
                process.runtime.snapshot(ap),
                process.runtime.pending_batches(lp, None, 16),
                process.runtime.pending_batches(ap, None, 16),
                logical.binding.original_failure_for_fixture(),
            );
        }
        assert!(
            checked_end.is_some(),
            "actual checked statistics End did not arrive"
        );
        assert!(
            futures::poll!(
                self.logical
                    .as_mut()
                    .unwrap()
                    .ready
                    .as_mut()
                    .unwrap()
                    .as_mut()
            )
            .is_pending(),
            "End alone cannot publish Ready before the original full-Task proof"
        );
        assert_eq!(
            self.capacity.snapshot().held_positions,
            [0, 0, 1, 0],
            "the same computation remains held for the second original proof receipt"
        );
        let expected_failure = case.is_task_failure().then(|| {
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                root_task,
                novarocks_execution_contract::TerminationDetail::Failed(original_failure()),
            );
            novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(
                &selected,
            )
            .unwrap()
        });
        if case == StatisticsRecipeCase::OriginalFailureStreamAfterEnd {
            let error = novarocks_query_application::api::QueryExecutionError::from_execution(
                expected_failure.as_ref().unwrap().clone(),
            );
            let original = self
                .logical
                .as_ref()
                .unwrap()
                .binding
                .bound()
                .unwrap()
                .original_consumer_for_fixture(activation)
                .unwrap()
                .upgrade()
                .expect("same live bound Internal consumer");
            // Component injection into the actual consumer only. This is NOT
            // an LP-emitted effect, AP acceptance, current-ticket or exit claim.
            let applied = original.execute(
                novarocks_logical_execution_protocol::effect::ConsumerEffect::PublishAttemptFailure {
                    activation, error: error.clone(),
                },
            ).await;
            assert!(matches!(
                applied,
                novarocks_logical_execution_protocol::input::ConsumerFact::Applied
            ));
            drop(original);
            // The original paid consumer future already runs on Tokio. Keep
            // manual CR queues unpolled until that exact stream branch was
            // observed; no private state, input or queue entry is changed.
            tokio::time::timeout(wait, async {
                while self
                    .output
                    .as_ref()
                    .unwrap()
                    .stream_failure_for_fixture()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the original failure stream was not observed");
            assert_eq!(
                self.output.as_ref().unwrap().stream_failure_for_fixture(),
                Some(error)
            );
        }
        // ProtocolFailureFirst obtains its exact failure only via the actual
        // covered status RPC. The stream case now resumes the same actual AP
        // path as well; no additional Native/actor failure is submitted.
        self.peer
            .as_ref()
            .unwrap()
            .publish_single_root_terminal(case.is_task_failure().then(original_failure))
            .unwrap();
        let result = drive_until_with_original_clock(
            &process,
            &mut self.typed.as_mut().unwrap().driver,
            self.logical
                .as_mut()
                .unwrap()
                .ready
                .as_mut()
                .unwrap()
                .as_mut(),
            Instant::now() + wait,
            None,
        )
        .await
        .expect("actual statistics result did not arrive");
        if case.is_success() {
            self.receiving = Some(result.unwrap());
            let original = self.receiving.as_mut().unwrap();
            let token = *original
                .ready_token()
                .expect("same actual computation Ready");
            assert_eq!(token.end(), end);
            if let Some(exit) = original.take_exit_observer() {
                assert!(self.logical.as_ref().unwrap().exit.is_none());
                self.logical.as_mut().unwrap().exit = Some(exit);
            }
            self.ready = Some(self.output.as_mut().unwrap().take_ready(token).unwrap());
            assert!(
                self.output.as_mut().unwrap().take_ready(token).is_err(),
                "the same complete Ready payload is consumed once"
            );
            let ready = self.ready.as_ref().unwrap();
            assert_eq!(ready.artifacts.len(), 1);
            assert_eq!(ready.artifacts[0].body().as_ref(), b"original artifact");
            let expected = self
                .logical
                .as_ref()
                .unwrap()
                .probe
                .credentials
                .lock()
                .unwrap()
                .clone()
                .unwrap();
            assert!(
                std::ptr::eq(
                    Arc::as_ptr(ready._credentials.as_ref().unwrap()),
                    expected.as_ptr()
                ),
                "Ready retains the exact original attempt credential allocation"
            );
            self.artifact_body = Some(ready.artifacts[0].body().clone());
            assert!(ready._binding.is_root_backed_internal());
            assert!(
                ready
                    ._binding
                    .window_alias()
                    .is_for_scope(&self.typed.as_ref().unwrap().root.owner.scope())
            );
            assert!(
                ready
                    ._binding
                    .window_alias()
                    .internal_manifest_binding()
                    .unwrap()
                    .is_none()
            );
            if case == StatisticsRecipeCase::Success {
                drop(self.ready.take());
            }
        } else {
            let error = match result {
                Err(error) => error,
                Ok(original) => {
                    self.receiving = Some(original);
                    panic!("the original statistics refusal cannot publish Ready");
                }
            };
            if let Some(expected) = &expected_failure {
                assert_eq!(
                    error.execution_failure(),
                    Some(expected),
                    "retain the original Task fact/detail/site/phase, never a domain or retirement replacement"
                );
            }
            assert!(self.ready.is_none() && self.receiving.is_none());
        }

        if case == StatisticsRecipeCase::PublicationWhileCleanupPending {
            publication::publish_while_cleanup_pending(self).await;
        }

        if let StatisticsRecipeCase::PublicationOutcome(case) = case {
            publication_outcomes::publish_outcome(self, case).await;
        }

        // Business result, original CPU close and both actual source exits are
        // independent. A typed domain/Task error is not process-health failure.
        drive_until_with_original_clock(
            &process,
            &mut self.typed.as_mut().unwrap().driver,
            self.custody.as_ref().unwrap().join_closed(),
            Instant::now() + wait,
            None,
        )
        .await
        .expect("actual statistics CPU custody did not close")
        .unwrap();
        assert_eq!(self.capacity.snapshot().held_positions, [0; 4]);
        drop(self.receiving.take());
        // This is the SAME five-second physical-exit budget. A borrowed
        // first-stop wait must not renew that budget or advance time before
        // the original AP's real control/RPC work actually starts.
        let exit_until = Instant::now() + wait;
        if !case.is_success() {
            let ap = self.installed.as_ref().unwrap().fixture_sources.0;
            let ap_exit = &self.installed.as_ref().unwrap().fixture_exit;
            let peer = self.peer.as_ref().unwrap();
            let stopped_or_exited = drive_until_with_original_clock(
                &process,
                &mut self.typed.as_mut().unwrap().driver,
                async {
                    loop {
                        if *ap_exit.observe_actual_exit().borrow()
                            || (peer.single_root_abort_observed()
                                && process
                                    .runtime
                                    .snapshot(ap)
                                    .is_ok_and(|original| !original.failed))
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                },
                exit_until,
                None,
            )
            .await;
            assert!(
                stopped_or_exited.is_some(),
                "original statistics cleanup did not issue Abort through its healthy actual AP before clock advancement: abort={}, ap={:?}, next={:?}, native={}",
                peer.single_root_abort_observed(),
                process.runtime.snapshot(ap),
                process.runtime.next_deadline(),
                self.logical
                    .as_ref()
                    .unwrap()
                    .binding
                    .original_failure_for_fixture()
            );
        }
        let exit = self.logical.as_mut().unwrap().exit.as_mut().unwrap();
        let receipt = drive_until_with_original_clock(
            &process,
            &mut self.typed.as_mut().unwrap().driver,
            std::future::poll_fn(|cx| exit.poll_exit(cx)),
            exit_until,
            if case.is_success() { None } else { Some(self.clock.as_ref()) },
        )
        .await
        .unwrap_or_else(|| {
            let lp = self.logical.as_ref().unwrap().binding.bound().unwrap().registration.actor();
            let ap = self.installed.as_ref().unwrap().fixture_sources.0;
            let inputs = process.runtime.input_inventory(ap, None, 32).unwrap();
            let standings = process.runtime.standing_input_inventory(ap, None, 512).unwrap();
            eprintln!(
                "original statistics exit custody: ap_inputs={inputs:?}, ap_batches={:?}, lp_inputs={:?}, unresolved_work={:?}",
                process.runtime.committed_batches(ap, None, 32),
                process.runtime.input_inventory(lp, None, 32),
                process.ledger.unresolved_work(),
            );
            for input in inputs {
                eprintln!(
                    "original statistics input retirement: input={:?}, role={:?}, preflight={:?}, header_authors={:?}, effects={:?}",
                    input,
                    standings.iter().find(|row| Some(row.standing_input_id) == input.standing_input_id),
                    process.runtime.input_retirement_status(ap, input.input),
                    process.runtime.input_header_authors(ap, input.input, 32),
                    process.runtime.effect_inventory(ap, input.input, None, 32),
                );
            }
            panic!(
                "actual statistics QA exit did not arrive: now={:?}, next={:?}, lp={:?}, ap={:?}, abort={}, ap_exit={}, native={}",
                process.clock.now(), process.runtime.next_deadline(),
                process.runtime.snapshot(lp), process.runtime.snapshot(ap),
                self.peer.as_ref().unwrap().single_root_abort_observed(),
                *self.installed.as_ref().unwrap().fixture_exit.observe_actual_exit().borrow(),
                self.logical.as_ref().unwrap().binding.original_failure_for_fixture(),
            );
        })
        .unwrap();
        assert!(matches!(
            receipt.view().runtime_actor_result(),
            Some(novarocks_query_application::api::logical_runtime::LogicalExit::Completed)
        ));
        assert!(
            *self
                .installed
                .as_ref()
                .unwrap()
                .fixture_exit
                .observe_actual_exit()
                .borrow(),
            "the original business result does not replace AP HostExit"
        );
        assert!(self.peer.as_ref().unwrap().single_root_result_progress().0);
        if case.is_success() {
            assert_eq!(
                self.peer.as_ref().unwrap().single_root_result_progress(),
                (true, 2)
            );
        }
        assert!(
            !self
                .peer
                .as_ref()
                .unwrap()
                .subscription_requests()
                .is_empty()
        );
        if case.is_success() {
            assert!(
                self.peer.as_ref().unwrap().single_root_release_observed(),
                "normal convergence must send the original Release RPC"
            );
        } else {
            assert!(
                self.peer.as_ref().unwrap().single_root_abort_observed(),
                "refused statistics must reach the original peer through Abort cleanup"
            );
        }
        drop(receipt);
        drop(self.installed.take());
        drop(self.logical.take());
        drop(self.output.take());
        drop(self.custody.take());
        drop(self.binding.take());
        drop(self.cohort.take());
        // The role receipt can be visible before its last backing reference
        // exits. Observe the exact stage reset under the same exit deadline.
        let typed = self.typed.as_mut().unwrap();
        tokio::time::timeout_at(exit_until.into(), async {
            loop {
                let progress = typed.driver.step();
                if typed.retired.is_reclaimed().unwrap() {
                    break;
                }
                if let joint::JointProgress::Idle(revision) = progress {
                    typed.progress.wait_state_change(revision).await;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original stage retires independently of the retained domain output");
        if case.is_success() {
            assert_eq!(
                self.artifact_body.as_ref().unwrap().as_ref(),
                b"original artifact"
            );
        }
        if case == StatisticsRecipeCase::TypedRootSuccess {
            let typed = self.typed.as_mut().unwrap();
            let next = typed_statistics_child(&typed.root, &mut typed.driver);
            let next_retired = next.retirement_observer().unwrap();
            let computation =
                typed_statistics_batch(&typed.root, &typed.payload, &next, &mut typed.driver);
            assert_eq!(self.capacity.snapshot().held_positions, [0, 0, 1, 0]);
            drop(computation);
            drive_typed(&mut typed.driver);
            assert_eq!(self.capacity.snapshot().held_positions, [0; 4]);
            next.complete();
            drive_typed(&mut typed.driver);
            assert!(next_retired.is_reclaimed().unwrap());
            assert_eq!(
                self.ready.as_ref().unwrap().artifacts[0].body().as_ref(),
                b"original artifact"
            );
        } else {
            assert!(self.ready.is_none());
        }
        drop(self.ready.take());
        drop(self.publication.take());
        drop(self.artifact_body.take());
        self.cpu_owner
            .shutdown_until(Instant::now() + wait)
            .await
            .unwrap();
        drive_until_with_original_clock(
            &process,
            &mut self.typed.as_mut().unwrap().driver,
            self.supervisor
                .as_mut()
                .unwrap()
                .shutdown_until(Instant::now() + wait),
            Instant::now() + wait,
            None,
        )
        .await
        .expect("actual statistics Supervisor did not close")
        .unwrap();
        drop(self.supervisor.take());
        process.host.close_registration();
        self.fixture.as_mut().unwrap().finish().await;
        drop(self.fixture.take());
        let TypedStatisticsCustody {
            root,
            payload,
            driver,
            progress,
            ..
        } = self.typed.take().unwrap();
        self.final_driver = Some(driver);
        drop(payload);
        let scope = root.owner.scope();
        root.owner.complete();
        root.business.release();
        crate::fixtures::native_recipe::drive_typed(self.final_driver.as_mut().unwrap());
        tokio::time::timeout(wait, async {
            let released = scope.wait_released();
            tokio::pin!(released);
            loop {
                match self.final_driver.as_mut().unwrap().step() {
                    joint::JointProgress::More => tokio::select! {
                        result = released.as_mut() => break result,
                        _ = tokio::task::yield_now() => {}
                    },
                    joint::JointProgress::Idle(revision) => tokio::select! {
                        result = released.as_mut() => break result,
                        _ = progress.wait_state_change(revision) => {}
                    },
                }
            }
        })
        .await
        .unwrap();
        drop(scope);
        self.workload.as_ref().unwrap().close_admission();
        crate::fixtures::native_recipe::drive_typed(self.final_driver.as_mut().unwrap());
        if let Err(failure) = self.workload.take().unwrap().shutdown() {
            let (error, owner) = failure.into_parts();
            self.workload = Some(owner);
            panic!("the same statistics Control did not drain: {error:?}");
        }
        self.data
            .drain_original_apply_until(Instant::now() + wait)
            .await
            .unwrap();
        self.data
            .drain_native_outgoing_until(Instant::now() + wait)
            .await
            .unwrap();
        assert!(self.peer.take().unwrap().settle().await);
        assert!(
            process.host.is_empty()
                && process.runtime.is_empty()
                && process.ledger.is_empty()
                && process.members.is_empty()
        );
        crate::fixtures::history::CapturedHistory::drain(&self.history, 65_536)
            .check_completed_runtime(&process.runtime, &process.ledger)
            .unwrap();
    }
}

pub(crate) async fn run_actual_statistics_case(case: StatisticsRecipeCase) {
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let peer = Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(32_768);
    let mut result = ResultCapacityConfig::V1;
    result.positions[2] = 1;
    result.all_objects_bytes[2] = 1024 * 1024;
    let PreparedOriginalRecipeProcess {
        mut fixture,
        process,
        clock,
        mut joint,
        workload,
        supervisor,
        client,
        capacity,
        handle,
        descriptor,
    } = prepare_original_recipe_process(
        &data,
        descriptor,
        history.clone(),
        result,
        Some(&mut cpu_owner),
    );
    let (root, payload, stage, computation, retired) =
        crate::fixtures::native_recipe::admit_statistics_originals(&workload, &mut joint);
    let mut typed = crate::fixtures::native_recipe::TypedStatisticsCustody {
        root,
        payload,
        computation: Some(computation),
        retired,
        driver: joint,
        progress: workload.deadline_expiry_handle(),
    };
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &typed.root.owner.scope(),
        &stage.scope(),
        &typed.payload,
    )
    .unwrap();
    let carrier = crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
        novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
        novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
    );
    let compile = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let (description, plan, recipe_root) =
        internal_statistics_encoded_description(&carrier, &compile).unwrap();
    let program = crate::fixtures::native_recipe::original_program();
    let (consumer, offers, output) = statistics_computation_consumer(
        &description,
        plan,
        &program,
        DecodedConsumerRuntime {
            computation: typed.computation.take(),
            binding: binding.clone(),
            cpu: cpu_owner.runtime(),
            runtime: data.clone(),
            cancellation: QueryCancellationView::governed(
                stage.scope().cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
    let (logical, cohort, output) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut fixture,
            joint: &mut typed.driver,
            process: &process,
            data: &data,
            clock: &clock,
            handle,
            descriptor,
            client,
        },
        OriginalRecipeOperands {
            owner: stage,
            binding: binding.clone(),
            description,
            plan,
            root: recipe_root,
            consumer,
            offers,
            output,
        },
    );
    let mut original = RetainedStatisticsRun(Some(Box::new(ActualStatisticsRun {
        fixture: Some(fixture),
        logical: Some(logical),
        process,
        clock,
        typed: Some(typed),
        final_driver: None,
        workload: Some(workload),
        supervisor: Some(supervisor),
        capacity,
        cohort: Some(cohort),
        output: Some(output),
        custody: Some(custody),
        binding: Some(binding),
        receiving: None,
        ready: None,
        artifact_body: None,
        publication: None,
        publication_future: None,
        publication_cleanup_gate: None,
        session_publication_close_future: None,
        installed: None,
        cpu_owner,
        data,
        peer: Some(peer),
        history,
        plan,
    })));
    original.0.as_mut().unwrap().complete(case).await;
    // Only the independently checked complete path drops the whole owner.
    drop(original.0.take());
}
