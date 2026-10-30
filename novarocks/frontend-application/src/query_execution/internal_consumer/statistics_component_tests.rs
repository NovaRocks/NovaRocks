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

//! Fixed statistics consumers over the actual original QA/TaskRound fixture.

use super::*;
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::{
    internal_consumer::{
        decoded::{DecodedConsumerRuntime, statistics_computation_consumer},
        test_fixture::internal_statistics_description,
    },
    internal_result_cpu::InternalResultCpuOwner,
    statistics::StatisticsCollectionProgram,
};
use arrow::{
    array::{ArrayRef, BinaryArray, Int32Builder, ListBuilder, MapBuilder, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use novarocks_execution_contract::{
    ResultPacketSequence, SafeDetail, TaskFailure, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason},
    root_result::{RootReadOutcome, RootReadResponse, RootResultData, RootResultReply},
};
use novarocks_query_application::coordination::LogicalConclusion;
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{QueryExecutionError, RetainedRootReply},
    cancellation::QueryCancellationView,
    execution_error::ExecutionFailureInput,
};
use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableHandle, StatisticsArtifactIdentity,
    StatisticsRequiredAggregation, StatisticsScanColumn,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_workload_control::{ResultWindowClass, joint_admission as joint};
use std::{future::Future, num::NonZeroU16};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Statistics component watchdog: {stage}"))
}

fn original_program() -> StatisticsCollectionProgram {
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
fn original_statistics_body() -> Vec<u8> {
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

#[derive(Clone, Copy)]
enum Case {
    Success,
    TypedRootSuccess,
    MissingArtifact,
    ProtocolFailureFirst,
    OriginalFailureStreamAfterEnd,
}

fn original_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::new(u16::MAX).unwrap(), NonZeroU16::MIN),
        ),
        SafeDetail::new("original statistics Task failure").unwrap(),
        TaskFailurePhase::Execution,
    )
}

// The actual original Root/result owner and closed CPU factory. Source
// identity grants no bytes; real collector admission uses the Result budget.
struct TypedStatisticsCustody {
    root: novarocks_workload_control::RootWork,
    payload: joint::RetainedPayloadGrant,
    computation: Option<joint::InternalComputationGrant>,
    retired: novarocks_workload_control::WorkRetirementObserver,
    driver: joint::JointProgressDriver,
    progress: novarocks_workload_control::DeadlineExpiryHandle,
}

// The same original driver progresses alongside the consumer future. More
// yields after a finite turn; Idle waits on its original revision notification.
// This introduces no second task, polling timer or duplicate driver authority.
async fn watchdog_joint<T>(
    typed: Option<&mut TypedStatisticsCustody>,
    stage: &'static str,
    future: impl Future<Output = T>,
) -> T {
    let Some(typed) = typed else {
        return watchdog(stage, future).await;
    };
    watchdog(stage, async {
        tokio::pin!(future);
        loop {
            match typed.driver.step() {
                joint::JointProgress::More => {
                    tokio::select! {
                        result = future.as_mut() => return result,
                        _ = tokio::task::yield_now() => {}
                    }
                }
                joint::JointProgress::Idle(revision) => {
                    tokio::select! {
                        result = future.as_mut() => return result,
                        _ = typed.progress.wait_state_change(revision) => {}
                    }
                }
            }
        }
    })
    .await
}

fn drive_typed(driver: &mut joint::JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), joint::JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite Joint driver did not reach idle");
}

fn typed_statistics_batch(
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

fn typed_statistics_custody(
    cpu_owner: &mut InternalResultCpuOwner,
) -> (
    TypedStatisticsCustody,
    (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::WorkOwner,
        novarocks_workload_control::ResultCapacityHandle,
    ),
) {
    use novarocks_workload_control::{
        ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig, WorkClass, WorkRequest,
        WorkloadConfig, WorkloadControl,
    };
    let count = 8;
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let mut result = ResultCapacityConfig::V1;
    result.positions[2] = 1;
    result.all_objects_bytes[2] = 1024 * 1024;
    assert!(
        original_statistics_body().len() < 16 * 1024,
        "the actual component body fits its explicit small payload budget"
    );
    let capacity = control.configure_result_capacity(result).unwrap();
    let (_, mut driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    cpu_owner.bind_for_test_before_ready(&mut control).unwrap();
    control.mark_ready().unwrap();
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
    drive_typed(&mut driver);
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
    drive_typed(&mut driver);
    let payload = payload.unwrap();
    let stage = typed_statistics_child(&root, &mut driver);
    let retired = stage.retirement_observer().unwrap();
    let computation = typed_statistics_batch(&root, &payload, &stage, &mut driver);
    let progress = control.deadline_expiry_handle();
    (
        TypedStatisticsCustody {
            root,
            payload,
            computation: Some(computation),
            retired,
            driver,
            progress,
        },
        (control, stage, capacity),
    )
}

async fn run_case(case: Case) {
    let (description, plan) = internal_statistics_description();
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let (mut typed, original_work) = if matches!(case, Case::Success | Case::TypedRootSuccess) {
        let (typed, original) = typed_statistics_custody(&mut cpu_owner);
        (Some(typed), Some(original))
    } else {
        (None, None)
    };
    let cpu_runtime = cpu_owner.runtime();
    let (mut attempt, resources, activation) =
        watchdog_joint(typed.as_mut(), "original admission and Establish", async {
            if let Some(original) = original_work {
                let execution =
                    QueryExecutionId::new(QueryId::new(0x92, 0x31), AttemptId::new(1).unwrap())
                        .unwrap();
                OriginalAttempt::new_bound_with_work(
                    execution,
                    Arc::new(UnavailableFinalInfo),
                    description.clone(),
                    plan,
                    true,
                    None,
                    WriteProtocolRoles::None,
                    Some(original),
                )
                .await
            } else {
                OriginalAttempt::new_with_description(
                    1,
                    Arc::new(UnavailableFinalInfo),
                    description.clone(),
                    plan,
                    true,
                )
                .await
            }
        })
        .await;
    if let Some(typed) = &typed {
        assert!(typed.computation.is_some());
        assert_eq!(
            attempt.capacity.snapshot().held_positions,
            [0, 0, 1, 0],
            "the original typed RootStage already owns its real Internal computation position"
        );
    } else {
        assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
    }
    let binding = if let Some(typed) = &typed {
        // Actor, stage compute, cancellation and binding all name this exact
        // direct child; the positive payload stays with the outer Root owner.
        QueryResultCapacityBinding::from_internal_payload(
            &typed.root.owner.scope(),
            &attempt.scope,
            &typed.payload,
        )
        .unwrap()
    } else {
        let window = attempt
            .capacity
            .try_acquire(&attempt.scope, ResultWindowClass::Internal)
            .unwrap();
        let binding =
            QueryResultCapacityBinding::try_new(&attempt.scope, window.retain_alias()).unwrap();
        drop(window);
        binding
    };
    let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let program = original_program();
    let (consumer, offers, mut output) = statistics_computation_consumer(
        &description,
        plan,
        &program,
        DecodedConsumerRuntime {
            computation: typed.as_mut().and_then(|typed| typed.computation.take()),
            binding: binding.clone(),
            cpu: cpu_runtime,
            runtime,
            cancellation: QueryCancellationView::governed(
                attempt.scope.cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    if let Some(typed) = &typed {
        assert!(typed.computation.is_none());
        assert_eq!(
            attempt.capacity.snapshot().held_positions,
            [0, 0, 1, 0],
            "moving the SAME computation into stable CPU custody neither frees nor mints a position"
        );
    }
    let mut producer = offers.reserve(attempt.root.query_execution_id()).unwrap();
    producer
        .stage(
            attempt.root.query_execution_id(),
            attempt.root,
            plan,
            resources,
        )
        .unwrap();
    attempt.bind_original_drive(&mut producer);
    let with_body = !matches!(case, Case::MissingArtifact);
    let segment = with_body.then(|| {
        let body = original_statistics_body();
        let backing = body.len() as u64;
        Arc::new(
            RetainedRootReply::try_new(
                RootReadResponse::Reply(RootResultReply {
                    root_task: attempt.root,
                    profile: RootProfileId::V1,
                    kind: RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
                    accepted_consumed: 0,
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            RootOutputKind::InternalFacts(
                                InternalResultDomain::StatisticsArtifactV1,
                            ),
                            NonZeroU64::MIN,
                            Bytes::from(body),
                            None,
                        )
                        .unwrap(),
                    ),
                }),
                binding.window_alias(),
                backing,
            )
            .unwrap(),
        )
    });
    let status = attempt
        .round
        .execution()
        .task(attempt.root.task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    let end = RootResultEnd {
        sequence: NonZeroU64::new(if with_body { 2 } else { 1 }).unwrap(),
        output_rows: u64::from(with_body),
    };
    let mut artifact_body = None;
    let mut retained_ready = None;
    let mut native_failure = None;
    {
        let root = attempt.root;
        let logical = &attempt.logical;
        let consume = logical.run_registered_internal_consumer(Arc::clone(&consumer), activation);
        let deliver = async {
            if let Some(segment) = segment {
                logical
                    .deliver_original_internal_segment(ResultPacketSequence::new(0), segment, 1)
                    .await
                    .unwrap();
            }
            logical
                .deliver_original_internal_end(root, status, end)
                .await
        };
        tokio::pin!(consume, deliver);
        watchdog_joint(typed.as_mut(), "End CPU receipt precedes upstream terminal facts", async {
        tokio::select! {
            biased;
            result = consume.as_mut() => panic!("statistics completed before its original nonterminal upstream: {result:?}"),
            result = deliver.as_mut() => result.unwrap(),
        }
    }).await;
        assert!(
            logical.original_internal_outcome().borrow().is_none(),
            "End is not the Ready proof"
        );
        if typed.is_some() {
            assert_eq!(
                attempt.capacity.snapshot().held_positions,
                [0, 0, 1, 0],
                "End completion keeps the SAME computation for the second original proof receipt"
            );
        }
        std::future::poll_fn(|cx| {
            assert!(
                consume.as_mut().poll(cx).is_pending(),
                "must wait for the real upstream facts"
            );
            Poll::Ready(())
        })
        .await;

        attempt
            .pending_upstream
            .as_ref()
            .unwrap()
            .begin_original_convergence(
                &mut attempt.round,
                Some(&novarocks_query_application::api::NativeAttemptTerminal::Completed),
            );
        let expected_failure = if matches!(
            case,
            Case::ProtocolFailureFirst | Case::OriginalFailureStreamAfterEnd
        ) {
            let selected = attempt
                .pending_upstream
                .as_ref()
                .unwrap()
                .publish_original_failure(&mut attempt.round, original_failure());
            let input = ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert!(logical.original_internal_outcome().borrow().is_none());
            if matches!(case, Case::OriginalFailureStreamAfterEnd) {
                // This is the actual production failure-freeze path after End;
                // consumer retirement must wake its failure stream with this input.
                watchdog(
                    "real pump failure retirement after End",
                    logical.freeze_original_internal_failure(QueryExecutionError::from_execution(
                        input.clone(),
                    )),
                )
                .await
                .unwrap();
            }
            Some(input)
        } else {
            None
        };
        if expected_failure.is_none() {
            let tail = attempt.pending_upstream.take().unwrap();
            attempt.release = tail.complete(&mut attempt.round, &attempt.acks);
        }
        let result = watchdog_joint(
            typed.as_mut(),
            "second CPU receipt or exact original Task failure",
            consume.as_mut(),
        )
        .await;

        match case {
            Case::Success | Case::TypedRootSuccess => {
                result.unwrap();
                let token = logical
                    .original_internal_outcome()
                    .borrow()
                    .clone()
                    .unwrap()
                    .unwrap();
                let ready = output.take_ready(token).unwrap();
                assert_eq!(ready.artifacts.len(), 1);
                assert_eq!(ready.artifacts[0].body().as_ref(), b"original artifact");
                assert!(Arc::ptr_eq(
                    ready._credentials.as_ref().unwrap(),
                    &attempt.native_credentials
                ));
                artifact_body = Some(ready.artifacts[0].body().clone());
                assert!(output.take_ready(token).is_err());
                if matches!(case, Case::TypedRootSuccess) {
                    retained_ready = Some(ready);
                } else {
                    drop(ready);
                }
            }
            Case::MissingArtifact => {
                assert!(
                    result.is_err(),
                    "nonempty original program must fail its second membership check"
                );
                let outcome = logical.original_internal_outcome();
                assert!(
                    matches!(*outcome.borrow(), Some(Err(_))),
                    "missing artifact cannot publish Ready"
                );
            }
            Case::ProtocolFailureFirst | Case::OriginalFailureStreamAfterEnd => {
                let error = result.expect_err("the original Task failure vetoes readiness");
                assert_eq!(
                    error.execution_failure(),
                    expected_failure.as_ref(),
                    "never replace the original Task site/phase/fact with a generic proof or retirement error"
                );
                assert!(!matches!(
                    *logical.original_internal_outcome().borrow(),
                    Some(Ok(_))
                ));
                if matches!(case, Case::OriginalFailureStreamAfterEnd) {
                    assert_eq!(logical.actor_snapshot().await.unwrap().conclusion, None);
                    // The freeze retired this consumer, so its failure/exit
                    // cannot replace the original Native terminal decision.
                    native_failure = Some(error);
                } else {
                    assert_eq!(
                        logical.actor_snapshot().await.unwrap().conclusion,
                        Some(LogicalConclusion::Failed),
                    );
                }
            }
        }
    }
    if let Some(error) = native_failure {
        // Both consumer and delivery futures have relinquished their borrow.
        // Move the exact original permit into the same terminal actor command
        // as the production Native pump; the actor owns Abort issuance.
        assert_eq!(
            watchdog(
                "original Native failure chooses the NoRecovery conclusion",
                attempt.logical.fail_original_running_attempt(error.clone()),
            )
            .await
            .unwrap(),
            LogicalConclusion::Failed,
        );
        let outcome = attempt.logical.original_internal_outcome();
        assert_eq!(outcome.borrow().as_ref(), Some(&Err(error)));
    }
    if let Some(tail) = attempt.pending_upstream.take() {
        attempt.release = tail.complete(&mut attempt.round, &attempt.acks);
    }
    watchdog_joint(
        typed.as_mut(),
        "stable CPU custody actual close",
        custody.join_closed(),
    )
    .await
    .unwrap();
    if let Some(typed) = &mut typed {
        drive_typed(&mut typed.driver);
        assert_eq!(
            attempt.capacity.snapshot().held_positions,
            [0; 4],
            "actual CPU custody closure releases compute while original payload, binding and output remain live"
        );
        assert_eq!(
            artifact_body.as_ref().unwrap().as_ref(),
            b"original artifact"
        );
        if matches!(case, Case::TypedRootSuccess) {
            assert!(retained_ready.is_some());
        }
    }
    drop(custody);
    drop(output);
    drop(consumer);
    drop(binding);
    drop(producer);
    drop(offers);
    if let Some(mut typed) = typed {
        let capacity = attempt.capacity.clone();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        // Join the original actor that owns the actual stage WorkOwner. Root
        // and Ready payload remain stable outside this consuming future.
        let process = watchdog_joint(
            Some(&mut typed),
            "all original Task/Native/QA owners drain",
            attempt.finish_retaining_process(),
        )
        .await;
        drive_typed(&mut typed.driver);
        assert!(
            typed.retired.is_reclaimed().unwrap(),
            "Ready payload must not keep old stage control or compute alive"
        );
        // Both success paths keep the separately cloned original artifact
        // alive through actual stage reclamation. Only TypedRootSuccess keeps
        // the complete Ready envelope and asks for a subsequent real stage.
        assert_eq!(
            artifact_body.as_ref().unwrap().as_ref(),
            b"original artifact"
        );
        if matches!(case, Case::TypedRootSuccess) {
            let ready = retained_ready
                .as_ref()
                .expect("original Ready output remains live");
            assert_eq!(ready.artifacts[0].body().as_ref(), b"original artifact");
            assert!(ready._binding.is_root_backed_internal());
            assert!(
                ready
                    ._binding
                    .window_alias()
                    .is_for_scope(&typed.root.owner.scope())
            );
            assert!(
                ready
                    ._binding
                    .window_alias()
                    .internal_manifest_binding()
                    .unwrap()
                    .is_none(),
                "publication keeps the original payload without old P1/manifest authority"
            );
            // Same hard cap is still one: a second real closed batch can receive
            // only if the original actual CPU/forwarder/activity position exited.
            let next = typed_statistics_child(&typed.root, &mut typed.driver);
            let next_retired = next.retirement_observer().unwrap();
            let computation =
                typed_statistics_batch(&typed.root, &typed.payload, &next, &mut typed.driver);
            assert_eq!(
                capacity.snapshot().held_positions,
                [0, 0, 1, 0],
                "the next actual stage acquires the sole compute position while original Ready payload remains live"
            );
            drop(computation);
            drive_typed(&mut typed.driver);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            next.complete();
            drive_typed(&mut typed.driver);
            assert!(next_retired.is_reclaimed().unwrap());
        } else {
            assert!(
                retained_ready.is_none(),
                "the ordinary success path already dropped its Ready envelope"
            );
        }
        drop(retained_ready);
        drop(artifact_body);
        drop(typed.payload);
        typed.root.owner.complete();
        typed.root.business.release();
        drive_typed(&mut typed.driver);
        drop(capacity);
        drop(process);
    } else {
        if artifact_body.is_some() {
            assert_eq!(
                attempt.capacity.snapshot().held_positions[2],
                1,
                "the separately cloned original artifact body retains the window"
            );
        }
        drop(artifact_body);
        assert_eq!(attempt.capacity.snapshot().held_positions, [0; 4]);
        watchdog("all original Task/Native/QA owners drain", attempt.finish()).await;
    }
    watchdog(
        "actual finite CPU executor exits",
        cpu_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_end_precedes_all_success_then_second_receipt_reaches_real_ready() {
    run_case(Case::Success).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_nonempty_program_missing_artifact_refuses_second_receipt_and_ready() {
    run_case(Case::MissingArtifact).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_te_failure_before_qa_watch_preserves_exact_task_cause() {
    run_case(Case::ProtocolFailureFirst).await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_statistics_original_failure_stream_wakes_after_end_without_waiting_for_ready() {
    run_case(Case::OriginalFailureStreamAfterEnd).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_root_statistics_real_cpu_ready_releases_stage_while_ready_payload_remains_live() {
    run_case(Case::TypedRootSuccess).await;
}

// A bound fixed-inventory Root creates its actual child through joint admission.
fn typed_statistics_child(
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

#[derive(Debug)]
struct StatisticsCpuFixtureWake;
impl crate::task_execution::status_intake::StatusIntakeWake for StatisticsCpuFixtureWake {
    fn wake(&self) {}
}
struct OriginalStatisticsCpuCause(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for OriginalStatisticsCpuCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
// A finite typed phase Source, not an arbitrary Fn callback. The fixture uses
// the same positive factory/offer/receipt and explicitly limits its Provider
// substitution to this component; it proves no Native/Provider IO profile.
struct OriginalStatisticsCpuPhase {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    epoch: novarocks_spi::connector::ProviderBindingEpoch,
    calls: std::sync::atomic::AtomicUsize,
    panic: bool,
    cause_drops: Arc<std::sync::atomic::AtomicUsize>,
    source_drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for OriginalStatisticsCpuPhase {
    fn drop(&mut self) {
        self.source_drops
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl novarocks_spi::connector::StatisticsSchemaCpuSource for OriginalStatisticsCpuPhase {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.epoch
    }
    fn run_schema(&self) {
        assert_eq!(
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            0
        );
        if self.panic {
            std::panic::panic_any(OriginalStatisticsCpuCause(self.cause_drops.clone()));
        }
    }
}

// Copying the lease's public identity cannot qualify another concrete factory.
struct ForeignStatisticsCpuPhase(Arc<OriginalStatisticsCpuPhase>);
impl novarocks_spi::connector::StatisticsSchemaCpuSource for ForeignStatisticsCpuPhase {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.0.descriptor
    }
    fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
        self.0.epoch
    }
    fn run_schema(&self) {
        panic!("foreign factory must never be dispatched");
    }
}

async fn original_statistics_scoped_cpu_case(panic: bool) {
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorProviderId, ProviderBindingEpoch, StatisticsCpuInput,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let (mut typed, (control, stage, capacity)) = typed_statistics_custody(&mut owner);
    let scope = stage.scope();
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &typed.root.owner.scope(),
        &scope,
        &typed.payload,
    )
    .unwrap();
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("test").unwrap(),
        instance_id: ConnectorInstanceId::parse("statistics_cpu_component").unwrap(),
    };
    let epoch = ProviderBindingEpoch::new();
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let source_drops = Arc::new(AtomicUsize::new(0));
    let phase = Arc::new(OriginalStatisticsCpuPhase {
        descriptor: descriptor.clone(),
        epoch,
        calls: AtomicUsize::new(0),
        panic,
        cause_drops: cause_drops.clone(),
        source_drops: source_drops.clone(),
    });
    let (port, fixture) = owner
        .runtime()
        .statistics_cpu_fixture(
            &scope,
            binding.try_window_alias().unwrap(),
            descriptor,
            epoch,
            std::any::TypeId::of::<OriginalStatisticsCpuPhase>(),
            QueryCancellationView::governed(scope.cancellation().unwrap(), None),
            tokio::runtime::Handle::current(),
            Arc::new(StatisticsCpuFixtureWake),
        )
        .unwrap();
    let counterfeit = Arc::new(ForeignStatisticsCpuPhase(phase.clone()));
    let mut counterfeit_input = Some(StatisticsCpuInput::Schema(counterfeit.clone()));
    assert!(port.submit_in(&mut counterfeit_input).is_err());
    assert!(
        matches!(counterfeit_input.as_ref(),Some(StatisticsCpuInput::Schema(s))
        if std::ptr::addr_eq(s.as_ref(), counterfeit.as_ref()))
    );
    assert_eq!(phase.calls.load(Ordering::SeqCst), 0);
    drop(counterfeit_input);
    drop(counterfeit);
    // A real generation refusal leaves the exact input at its original caller.
    let foreign = Arc::new(OriginalStatisticsCpuPhase {
        descriptor: phase.descriptor.clone(),
        epoch: ProviderBindingEpoch::new(),
        calls: AtomicUsize::new(0),
        panic: false,
        cause_drops: cause_drops.clone(),
        source_drops: Arc::new(AtomicUsize::new(0)),
    });
    let mut refused = Some(StatisticsCpuInput::Schema(foreign.clone()));
    assert!(port.submit_in(&mut refused).is_err());
    assert!(
        matches!(refused.as_ref(),Some(StatisticsCpuInput::Schema(s))
        if std::ptr::addr_eq(s.as_ref(), foreign.as_ref()))
    );
    assert_eq!(foreign.calls.load(Ordering::SeqCst), 0);
    drop(refused);
    drop(foreign);
    let mut input = Some(StatisticsCpuInput::Schema(phase.clone()));
    port.submit_in(&mut input).unwrap();
    assert!(input.is_none());
    let outcome = watchdog_joint(
        Some(&mut typed),
        "same scoped Statistics physical receipt",
        std::future::poll_fn(|cx| port.poll_finished(cx)),
    )
    .await;
    assert_eq!(phase.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
    if panic {
        assert!(outcome.is_err());
        let observed =
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "fixture received original Statistics CPU health",
                port.local_failure().unwrap(),
            );
        assert!(fixture.worker_failure_is::<OriginalStatisticsCpuCause>());
        // Cancelling a borrowed close observation cannot replace the original
        // job, cause or window. A later observer joins that same actual source.
        let mut close = Box::pin(std::future::poll_fn(|cx| port.poll_closed(cx)));
        let _ = close
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
        drop(close);
        assert!(
            watchdog_joint(
                Some(&mut typed),
                "same joined Statistics failed close",
                std::future::poll_fn(|cx| port.poll_closed(cx))
            )
            .await
            .is_err()
        );
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        drop(phase);
        assert_eq!(source_drops.load(Ordering::SeqCst), 0);
        assert!(!typed.retired.is_reclaimed().unwrap());
        // Test-only explicit disposal after actual join and same-health intake.
        // This does not set finished or issue a normal exit receipt.
        let disposition = fixture.take_joined_for_disposition(&observed).unwrap();
        drop(disposition);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        drop(observed);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    } else {
        outcome.unwrap();
        assert!(port.local_failure().is_none());
        assert!(
            watchdog_joint(
                Some(&mut typed),
                "empty healthy Statistics source close",
                std::future::poll_fn(|cx| port.poll_closed(cx))
            )
            .await
            .is_ok()
        );
        drop(phase);
        assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    }
    drop(fixture);
    drop(port);
    drop(binding);
    drop(scope);
    drop(typed.computation.take());
    stage.complete();
    drive_typed(&mut typed.driver);
    assert!(typed.retired.is_reclaimed().unwrap());
    drop(typed.payload);
    typed.root.owner.complete();
    typed.root.business.release();
    drive_typed(&mut typed.driver);
    drop(capacity);
    control.close_admission();
    assert!(
        control.shutdown().is_ok(),
        "same original WC must actually drain"
    );
    watchdog(
        "same original scoped CPU owner exits",
        owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn original_statistics_scoped_cpu_positive_receipt_and_refused_input_preservation() {
    original_statistics_scoped_cpu_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_statistics_scoped_cpu_panic_keeps_original_source_cause_and_window_until_disposition()
 {
    original_statistics_scoped_cpu_case(true).await;
}
