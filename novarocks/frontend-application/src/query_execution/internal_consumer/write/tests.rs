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

//! Observes confirmation, publication and actual exit through the original Internal Write consumer.

use super::*;
use crate::{
    native::data_runtime::FrontendDataRuntime,
    query_execution::{
        internal_consumer::test_fixture::offer_original_credentials,
        internal_result_cpu::InternalResultCpuOwner,
        write_session::tests::{
            commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
        },
    },
};
use bytes::Bytes;
use novarocks_execution_contract::{
    SafeDetail, TaskFailure, TaskFailurePhase,
    failure::{FailureClass, FailureFact, FailureReason},
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{LogicalExecutionNativePort, LogicalNativeOpenRequest},
    coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
    execution_error::ExecutionFailureInput,
};
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use std::{
    future::Future,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    task::Poll,
    time::{Duration, Instant},
};

async fn watchdog<T>(stage: &'static str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap_or_else(|_| panic!("Write component watchdog: {stage}"))
}
#[derive(Debug)]
struct ReservationOnly;
impl LogicalExecutionNativePort for ReservationOnly {
    fn bind_in(
        &self,
        _source: &mut Option<LogicalNativeOpenRequest>,
        _target: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
    ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
        panic!("reserving an identity cannot bind Native work")
    }
}
fn original_failure() -> TaskFailure {
    TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(NonZeroU16::new(u16::MAX).unwrap(), NonZeroU16::MIN),
        ),
        SafeDetail::new("original fixed Write Task failure").unwrap(),
        TaskFailurePhase::Execution,
    )
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionEvent {
    RootRows,
    ArrivalRefusal,
    RootEnd,
    WriterSuccess,
    SummarySuccess,
    SummaryFailure,
    TaskFailure,
    Cancelled,
    DeadlineExpired,
}

fn causal_orders(events: &[CompletionEvent]) -> Vec<Vec<CompletionEvent>> {
    fn permutations(events: &[CompletionEvent]) -> Vec<Vec<CompletionEvent>> {
        if events.is_empty() {
            return vec![Vec::new()];
        }
        let mut orders = Vec::new();
        for index in 0..events.len() {
            let mut remaining = events.to_vec();
            let event = remaining.remove(index);
            for mut suffix in permutations(&remaining) {
                suffix.insert(0, event);
                orders.push(suffix);
            }
        }
        orders
    }
    permutations(events)
        .into_iter()
        .filter(|order| {
            order
                .iter()
                .position(|event| *event == CompletionEvent::RootRows)
                .unwrap()
                < order
                    .iter()
                    .position(|event| *event == CompletionEvent::RootEnd)
                    .unwrap()
        })
        .collect()
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ControlCut {
    Arrival,
    AfterReady,
}

async fn run_arrival_order(order: &[CompletionEvent], publish: bool, cut: ControlCut) {
    run_arrival_order_with_summary_source(order, publish, cut, false).await;
}

struct TypedWriteCustody {
    root: novarocks_workload_control::RootWork,
    payload: novarocks_workload_control::joint_admission::RetainedPayloadGrant,
    computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    retired: novarocks_workload_control::WorkRetirementObserver,
    driver: novarocks_workload_control::joint_admission::JointProgressDriver,
    progress: novarocks_workload_control::DeadlineExpiryHandle,
}

// Whole unresolved originals stay together on assertion unwind. No asynchronous
// close, fabricated exit, Scope completion or fragment mem::forget is performed.
struct RetainedWriteRun(Option<Box<ActualWriteRun>>);
impl Drop for RetainedWriteRun {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNRESOLVED: std::sync::OnceLock<
                std::sync::Mutex<Vec<Box<dyn std::any::Any + Send>>>,
            > = std::sync::OnceLock::new();
            UNRESOLVED
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
// A passive per-original-consumer test latch, never a CPU/Scope authority.
#[derive(Default)]
pub(super) struct OriginalPreUnionGate {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
impl OriginalPreUnionGate {
    pub(super) async fn wait(&self) {
        self.entered
            .store(true, std::sync::atomic::Ordering::Release);
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
    fn entered(&self) -> bool {
        self.entered.load(std::sync::atomic::Ordering::Acquire)
    }
    fn release(&self) {
        if !self
            .released
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.changed.notify_waiters();
        }
    }
}
struct OriginalPreUnionRelease(Arc<OriginalPreUnionGate>);
impl Drop for OriginalPreUnionRelease {
    fn drop(&mut self) {
        // Unwind opens only this passive latch. Original future/input/Work
        // stay in the existing whole-run custody; no cleanup is fabricated.
        self.0.release();
    }
}
struct ActualWriteRun {
    fixture: Option<crate::fixtures::FrontendFixture>,
    logical: Option<crate::fixtures::native_recipe::ActualLogicalCase>,
    process: Arc<crate::control_runtime::FrontendAttemptRuntime>,
    clock: Arc<crate::task_execution::clock::ManualClock>,
    typed: Option<TypedWriteCustody>,
    final_driver: Option<novarocks_workload_control::joint_admission::JointProgressDriver>,
    workload: Option<novarocks_workload_control::WorkloadControl>,
    supervisor: Option<novarocks_query_application::coordination::LogicalExecutionSupervisor>,
    capacity: novarocks_workload_control::ResultCapacityHandle,
    cohort:
        Option<crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort>,
    output: Option<WriteComputationOutput>,
    custody: Option<Arc<DomainCpuCustody>>,
    binding: Option<QueryResultCapacityBinding>,
    result: Option<
        Result<
            novarocks_query_application::api::ExecutionOutputObligation,
            novarocks_query_application::api::QueryExecutionError,
        >,
    >,
    receiving: Option<novarocks_query_application::api::ExecutionOutputObligation>,
    ready: Option<ReadyWriteComputation>,
    token: Option<novarocks_query_application::api::ComputationReadyToken>,
    installed: Option<Arc<crate::task_execution::host::install::InstalledAttemptHost>>,
    cpu_owner: InternalResultCpuOwner,
    data: FrontendDataRuntime,
    peer: Option<crate::native::task_transport::tests::Loopback>,
    history: novarocks_control_runtime::history::HistoryCapture,
    session_fixture: Option<crate::query_execution::write_session::tests::Fixture>,
    session: Option<Arc<ConnectorWriteSession>>,
    credentials: Option<Arc<InitialComputationCredentials>>,
    native_storage: Option<Arc<AttemptCredentialStorage>>,
    credential_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    publication_context: Option<novarocks_spi::connector::ConnectorRequestContext>,
    publication_stop: novarocks_spi::connector::ConnectorStopOwner,
    requester: Option<novarocks_workload_control::WorkCancellationRequester>,
    stage_scope: novarocks_workload_control::WorkScope,
    plan: novarocks_query_application::api::PlanSeal,
}

enum OriginalWriteReceipt {
    Receiving,
    Body,
    End(novarocks_execution_contract::root_result::RootResultEnd),
    Started(Vec<TaskIdentity>),
    Protocol(Vec<TaskIdentity>),
    PreUnion,
    Result,
}
impl ActualWriteRun {
    async fn round(&mut self) {
        crate::fixtures::native_recipe::drive_typed(&mut self.typed.as_mut().unwrap().driver);
        self.fixture
            .as_mut()
            .unwrap()
            .progress(
                crate::fixtures::ProgressSelection::All,
                crate::fixtures::ProgressBudget::default(),
            )
            .await;
        if self.result.is_none() {
            let ready = self.logical.as_mut().unwrap().ready.as_mut().unwrap();
            if let Poll::Ready(result) = futures::poll!(ready.as_mut()) {
                self.result = Some(result);
            }
        }
        tokio::task::yield_now().await;
    }
    fn has_receipt(&self, receipt: &OriginalWriteReceipt) -> bool {
        let logical = self.logical.as_ref().unwrap();
        let output = self.output.as_ref().unwrap();
        match receipt {
            OriginalWriteReceipt::Receiving => {
                let activation = logical.probe.activation.lock().unwrap().unwrap();
                let root = logical.probe.root.lock().unwrap().unwrap();
                let held = output.holder.receiving.lock().unwrap();
                matches!(&*held, Receiving::Reserved(binding)
                    if binding.activation == activation && binding.root == root && binding.plan == self.plan)
            }
            OriginalWriteReceipt::Body => output.holder.body_applied.get().is_some(),
            OriginalWriteReceipt::End(end) => output.holder.decoded_end.get() == Some(end),
            OriginalWriteReceipt::Started(tasks) | OriginalWriteReceipt::Protocol(tasks) => {
                let activation = logical.probe.activation.lock().unwrap().unwrap();
                let root = logical.probe.root.lock().unwrap().unwrap();
                let observed = logical.probe.protocol.lock().unwrap().clone().unwrap();
                let accepted = observed
                    .snapshot(activation, root, self.plan)
                    .unwrap()
                    .is_some_and(|snapshot| {
                        tasks.iter().all(|task| {
                            snapshot.tasks.iter().any(|(identity, status)| {
                                if identity != task {
                                    return false;
                                }
                                status.as_ref().is_some_and(|status| {
                                    if matches!(receipt, OriginalWriteReceipt::Started(_)) {
                                        matches!(
                                            status.state(),
                                            novarocks_execution_contract::TaskState::Running
                                                | novarocks_execution_contract::TaskState::Finished
                                        )
                                    } else {
                                        status.state()
                                            == novarocks_execution_contract::TaskState::Finished
                                    }
                                })
                            })
                        })
                    });
                // The SAME consumer's real Ready also proves its frozen writer
                // gate completed if the publisher has already actually exited.
                accepted || matches!(&self.result, Some(Ok(_)))
            }
            OriginalWriteReceipt::PreUnion => output
                .holder
                .pre_union_gate
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|gate| gate.entered()),
            OriginalWriteReceipt::Result => self.result.is_some(),
        }
    }
    async fn wait_receipt(&mut self, receipt: OriginalWriteReceipt) {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if self.has_receipt(&receipt) || matches!(&self.result, Some(Err(_))) {
                return;
            }
            assert!(
                Instant::now() < until,
                "same original write receipt did not arrive"
            );
            self.round().await;
        }
    }
    async fn complete(
        &mut self,
        order: &[CompletionEvent],
        publish: bool,
        cut: ControlCut,
        independent_summary_source: bool,
    ) {
        let pre_union_gate = (cut == ControlCut::Arrival
            && order.iter().any(|event| {
                matches!(
                    event,
                    CompletionEvent::Cancelled | CompletionEvent::DeadlineExpired
                )
            }))
        .then(|| Arc::new(OriginalPreUnionGate::default()));
        let _pre_union_release = pre_union_gate
            .as_ref()
            .map(|gate| OriginalPreUnionRelease(gate.clone()));
        if let Some(gate) = &pre_union_gate {
            assert!(
                self.output
                    .as_ref()
                    .unwrap()
                    .holder
                    .pre_union_gate
                    .lock()
                    .unwrap()
                    .replace(gate.clone())
                    .is_none(),
                "one original pre-union test cut"
            );
        }
        self.logical
            .as_ref()
            .unwrap()
            .probe
            .graph_capture_requested
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
        let activation = logical.probe.activation.lock().unwrap().unwrap();
        let root = logical.probe.root.lock().unwrap().unwrap();
        let graph = logical.probe.graph_capture.lock().unwrap().take().unwrap();
        assert_eq!(graph.tasks.len(), 3);
        assert_eq!(graph.root, root);
        assert_eq!(graph.plan, self.plan);
        assert_eq!(graph.edges.len(), 2);
        assert!(
            graph.edges.iter().all(|edge| edge.delivery()
                == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain),
            "the SAME write component's required inputs survive early Root completion"
        );
        let task_for = |fragment| {
            let tasks = graph
                .tasks
                .iter()
                .filter(|task| task.fragment_id() == fragment)
                .map(|task| task.identity())
                .collect::<Vec<_>>();
            assert_eq!(tasks.len(), 1);
            tasks[0]
        };
        let leaf = task_for(2);
        let writer = task_for(3);
        assert_ne!(leaf, writer);
        assert_ne!(root, leaf);
        assert_ne!(root, writer);
        let all = graph
            .tasks
            .iter()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        let peer = self.peer.as_ref().unwrap();
        peer.enable_single_root_encoded_result(
            root,
            novarocks_result_contract::RootOutputKind::InternalFacts(
                novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
            ),
            Bytes::from(
                crate::query_execution::write_result::tests::component_write_body(
                    commit_fragment_bytes(),
                ),
            ),
            2,
        )
        .unwrap();
        peer.enable_three_task_roster(all.clone().try_into().unwrap())
            .unwrap();
        peer.defer_three_task_terminals().unwrap();
        peer.defer_encoded_root_result().unwrap();
        logical.probe.release_activation();
        self.wait_receipt(OriginalWriteReceipt::Receiving).await;
        self.wait_receipt(OriginalWriteReceipt::Started(all.clone()))
            .await;
        if self.result.is_none() {
            // A protocol snapshot cannot stand in for actual Create RPCs.
            let created = self.peer.as_ref().unwrap().applied_requests().into_iter()
                .flat_map(|batch| batch.operations)
                .filter(|wire| matches!(&wire.operation,
                    Some(novarocks_proto_models::novarocks::task_operation::Operation::CreateTask(_))))
                .map(|wire| {
                    let novarocks_task_codec::operation::DecodedOperation::CreateTask(original) =
                        novarocks_task_codec::operation::decode_operation(&wire,
                            novarocks_proto_codec::FieldPath::root("same_original_write_create"))
                            .expect("the same accepted Create decodes") else {
                                unreachable!("filtered original Create");
                            };
                    original.request().identity()
                }).collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                created,
                all.into_iter().collect(),
                "all three SAME manifest Tasks reached the actual peer Create path"
            );
        }
        assert!(
            self.result.is_none(),
            "the same registered consumer waits before original input: original_error={:?}, unexpected_ready={}, peer_progress={:?}, p1={:?}, native={}, protocol_progress={:?}, peer_operations={:?}",
            self.result
                .as_ref()
                .and_then(|original| original.as_ref().err()),
            matches!(&self.result, Some(Ok(_))),
            self.peer.as_ref().unwrap().single_root_result_progress(),
            self.capacity.snapshot().held_positions,
            self.logical
                .as_ref()
                .unwrap()
                .binding
                .original_failure_for_fixture(),
            self.logical
                .as_ref()
                .unwrap()
                .probe
                .protocol
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .snapshot(activation, root, self.plan)
                .map(|snapshot| snapshot.map(|snapshot| snapshot.tasks)),
            self.peer
                .as_ref()
                .unwrap()
                .applied_requests()
                .into_iter()
                .flat_map(|batch| batch.operations)
                .filter(|wire| matches!(
                    &wire.operation,
                    Some(
                        novarocks_proto_models::novarocks::task_operation::Operation::CreateTask(_)
                    ) | Some(
                        novarocks_proto_models::novarocks::task_operation::Operation::UpdateTask(_)
                    ) | Some(
                        novarocks_proto_models::novarocks::task_operation::Operation::CancelTask(_)
                    )
                ))
                .map(|wire| {
                    use novarocks_task_codec::operation::{
                        DecodedOperation as D, decode_operation,
                    };
                    decode_operation(
                        &wire,
                        novarocks_proto_codec::FieldPath::root("original_write_progress"),
                    )
                    .map(|original| match original {
                        D::CreateTask(original) => ("Create", original.request().identity()),
                        D::UpdateTask(original) => ("Update", original.request().identity()),
                        D::CancelTask(original) => ("Cancel", original.identity()),
                        _ => unreachable!("filtered original operation kind"),
                    })
                    .map_err(|error| error.to_string())
                })
                .collect::<Vec<_>>(),
        );
        assert_eq!(self.capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let end = novarocks_execution_contract::root_result::RootResultEnd {
            sequence: NonZeroU64::new(2).unwrap(),
            output_rows: 2,
        };
        let execution = activation.execution();
        assert_eq!(self.credentials.as_ref().unwrap().execution(), execution);
        let control = order.iter().copied().find(|event| {
            matches!(
                event,
                CompletionEvent::Cancelled | CompletionEvent::DeadlineExpired
            )
        });
        let mut expected = None;
        let mut end_received = false;
        let mut writers_succeeded = false;
        let mut summary_succeeded = !independent_summary_source;
        let mut control_requested = false;
        let mut accepted_before_control = None;
        for event in order {
            match event {
                CompletionEvent::RootRows => {
                    self.peer.as_ref().unwrap().release_encoded_root_data();
                    self.wait_receipt(OriginalWriteReceipt::Body).await;
                    let recorded = self
                        .session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap();
                    assert_eq!(recorded.fragment_arrivals, 1);
                    assert_eq!(recorded.write_confirmations, 0);
                }
                CompletionEvent::ArrivalRefusal => {
                    self.peer.as_ref().unwrap().release_encoded_root_data();
                    self.wait_receipt(OriginalWriteReceipt::Result).await;
                    expected = Some(
                        super::super::domain_state::InternalDomainFailure::from(arrival_refusal())
                            .execution_input()
                            .unwrap(),
                    );
                    let recorded = self
                        .session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap();
                    assert!(recorded.arrival_refusal.is_none());
                    assert_eq!(recorded.fragment_arrivals, 0);
                }
                CompletionEvent::RootEnd => {
                    self.peer.as_ref().unwrap().release_encoded_root_end();
                    self.wait_receipt(OriginalWriteReceipt::End(end)).await;
                    end_received =
                        self.output.as_ref().unwrap().holder.decoded_end.get() == Some(&end);
                }
                CompletionEvent::WriterSuccess => {
                    let tasks = if independent_summary_source {
                        vec![writer]
                    } else {
                        vec![leaf, writer]
                    };
                    for task in &tasks {
                        self.peer
                            .as_ref()
                            .unwrap()
                            .publish_three_task_terminal(*task, None)
                            .unwrap();
                    }
                    self.wait_receipt(OriginalWriteReceipt::Protocol(tasks))
                        .await;
                    writers_succeeded = true;
                }
                CompletionEvent::SummarySuccess => {
                    assert!(independent_summary_source);
                    self.peer
                        .as_ref()
                        .unwrap()
                        .publish_three_task_terminal(leaf, None)
                        .unwrap();
                    self.wait_receipt(OriginalWriteReceipt::Protocol(vec![leaf]))
                        .await;
                    summary_succeeded = true;
                }
                CompletionEvent::SummaryFailure | CompletionEvent::TaskFailure => {
                    if *event == CompletionEvent::SummaryFailure {
                        assert!(independent_summary_source);
                    }
                    let original = original_failure();
                    let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                        leaf,
                        novarocks_execution_contract::TerminationDetail::Failed(original.clone()),
                    );
                    expected = Some(ExecutionFailureInput::from_selected_task(&selected).unwrap());
                    self.peer
                        .as_ref()
                        .unwrap()
                        .publish_three_task_terminal(leaf, Some(original))
                        .unwrap();
                    self.wait_receipt(OriginalWriteReceipt::Result).await;
                }
                CompletionEvent::Cancelled | CompletionEvent::DeadlineExpired => {
                    if cut == ControlCut::Arrival
                        && end_received
                        && writers_succeeded
                        && summary_succeeded
                    {
                        self.wait_receipt(OriginalWriteReceipt::PreUnion).await;
                        assert!(
                            pre_union_gate.as_ref().unwrap().entered(),
                            "all original proofs reached the real pre-union continuation"
                        );
                        assert!(self.result.is_none() && self.token.is_none());
                        assert_eq!(
                            self.session
                                .as_ref()
                                .unwrap()
                                .accumulated_row_count()
                                .unwrap(),
                            0
                        );
                        assert_eq!(self.capacity.snapshot().held_positions, [0, 0, 1, 0]);
                    }
                    if cut == ControlCut::AfterReady {
                        assert!(end_received && writers_succeeded && summary_succeeded);
                        self.wait_receipt(OriginalWriteReceipt::Result).await;
                        let Some(Ok(original)) = &self.result else {
                            panic!("the real Ready cut precedes this control");
                        };
                        let token = *original.ready_token().unwrap();
                        assert_eq!(token.end(), end);
                        assert_eq!(token.root(), root);
                        assert_eq!(token.activation(), activation);
                        assert_eq!(
                            self.session
                                .as_ref()
                                .unwrap()
                                .accumulated_row_count()
                                .unwrap(),
                            3
                        );
                        accepted_before_control = Some(token);
                    }
                    let reason = if *event == CompletionEvent::Cancelled {
                        novarocks_workload_control::CancellationReason::Requested
                    } else {
                        novarocks_workload_control::CancellationReason::DeadlineExceeded
                    };
                    match self.requester.as_ref().unwrap().request_with_outcome(reason.clone()).unwrap() {
                        novarocks_workload_control::WorkCancellationRequestOutcome::Requested => {
                            assert_eq!(self.stage_scope.cancellation().unwrap().reason(), Some(reason.clone()));
                        }
                        novarocks_workload_control::WorkCancellationRequestOutcome::AlreadyRequested(original) => assert_eq!(original, reason),
                        novarocks_workload_control::WorkCancellationRequestOutcome::SuccessSealed => {
                            assert!(matches!(&self.result, Some(Ok(_))), "accepted Ready remains stable");
                        }
                    }
                    if *event == CompletionEvent::Cancelled {
                        self.publication_stop.request_stop();
                    }
                    if let Some(gate) = &pre_union_gate {
                        // DeadlineExpired must reach this SAME governed Scope,
                        // not merely the independent publication Context.
                        assert_eq!(
                            self.stage_scope.cancellation().unwrap().reason(),
                            Some(reason)
                        );
                        gate.release();
                    }
                    control_requested = true;
                    self.wait_receipt(OriginalWriteReceipt::Result).await;
                }
            }
            self.round().await;
            if matches!(&self.result, Some(Err(_))) {
                break;
            }
            if expected.is_none()
                && !control_requested
                && (!end_received || !writers_succeeded || !summary_succeeded)
            {
                assert!(
                    self.result.is_none(),
                    "Ready needs original Root and every declared writer/source: {order:?}"
                );
                assert_eq!(
                    self.session
                        .as_ref()
                        .unwrap()
                        .accumulated_row_count()
                        .unwrap(),
                    0,
                    "the second original union waits for all exact protocol gates"
                );
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .finish,
                    0
                );
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .abort,
                    0
                );
                assert_eq!(
                    self.capacity.snapshot().held_positions,
                    [0, 0, 1, 0],
                    "the SAME original cap-one computation waits for all proofs"
                );
            }
        }
        self.wait_receipt(OriginalWriteReceipt::Result).await;
        let result = self.result.take().unwrap();
        if result.is_err() {
            let recorded = self
                .session_fixture
                .as_ref()
                .unwrap()
                .recorded
                .lock()
                .unwrap();
            assert_eq!(recorded.write_confirmations, 0);
            assert_eq!(recorded.finish, 0);
        }
        match (expected, result) {
            (Some(expected), Err(error)) => {
                assert_eq!(error.execution_failure(), Some(&expected));
                assert_eq!(
                    self.session
                        .as_ref()
                        .unwrap()
                        .accumulated_row_count()
                        .unwrap(),
                    0
                );
                assert!(self.token.is_none() && self.ready.is_none());
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .finish,
                    0
                );
            }
            (Some(_), Ok(original)) => {
                self.receiving = Some(original);
                panic!("original failure cannot publish Ready");
            }
            (None, Err(error)) => {
                let kind = match control.expect("only original control stops this case") {
                    CompletionEvent::Cancelled => {
                        novarocks_query_application::api::QueryExecutionErrorKind::Cancelled
                    }
                    CompletionEvent::DeadlineExpired => {
                        novarocks_query_application::api::QueryExecutionErrorKind::DeadlineExceeded
                    }
                    _ => unreachable!(),
                };
                assert_eq!(error.kind(), kind, "original control result for {order:?}");
                assert!(self.token.is_none() && self.ready.is_none());
                assert_eq!(
                    self.session
                        .as_ref()
                        .unwrap()
                        .accumulated_row_count()
                        .unwrap(),
                    0
                );
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .finish,
                    0
                );
            }
            (None, Ok(mut original)) => {
                let token = *original.ready_token().unwrap();
                assert_eq!(token.end(), end);
                assert_eq!(token.root(), root);
                assert_eq!(token.activation(), activation);
                if let Some(accepted) = accepted_before_control {
                    assert_eq!(token, accepted);
                }
                if let Some(exit) = original.take_exit_observer() {
                    assert!(self.logical.as_ref().unwrap().exit.is_none());
                    self.logical.as_mut().unwrap().exit = Some(exit);
                }
                self.receiving = Some(original);
                let ready = self.output.as_mut().unwrap().take_ready(token).unwrap();
                assert_eq!(ready.accepted.row_count(), 3);
                assert!(!ready.accepted.is_empty());
                assert!(ready.accepted.belongs_to(self.session.as_ref().unwrap()));
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .write_confirmations,
                    1
                );
                assert!(Arc::ptr_eq(&ready.session, self.session.as_ref().unwrap()));
                assert!(Arc::ptr_eq(
                    &ready._credentials,
                    self.native_storage.as_ref().unwrap()
                ));
                assert_eq!(
                    self.session
                        .as_ref()
                        .unwrap()
                        .accumulated_row_count()
                        .unwrap(),
                    3
                );
                assert!(self.output.as_mut().unwrap().take_ready(token).is_err());
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .finish,
                    0,
                    "Ready is original union custody, not Provider publication"
                );
                assert_eq!(
                    self.session_fixture
                        .as_ref()
                        .unwrap()
                        .recorded
                        .lock()
                        .unwrap()
                        .abort,
                    0
                );
                self.token = Some(token);
                self.ready = Some(ready);
            }
        }
        let session = self.session.as_ref().unwrap();
        if let Some(control) = control {
            let mut call = None;
            let mut refusal = None;
            session
                .prepare_finish_or_abort_call_in(
                    self.publication_context.take().unwrap(),
                    &mut call,
                    &mut refusal,
                )
                .unwrap();
            let expected_kind = if control == CompletionEvent::Cancelled {
                novarocks_spi::connector::ConnectorErrorKind::Cancelled
            } else {
                novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded
            };
            assert_eq!(refusal.as_ref().unwrap().kind(), expected_kind);
            if let Some(original) = accepted_before_control {
                assert_eq!(self.token, Some(original));
                assert_eq!(session.accumulated_row_count().unwrap(), 3);
                let ready = self.ready.as_ref().unwrap();
                assert_eq!(ready.accepted.row_count(), 3);
                assert!(ready.accepted.belongs_to(session));
                assert!(Arc::ptr_eq(&ready.session, session));
                assert!(Arc::ptr_eq(
                    &ready._credentials,
                    self.native_storage.as_ref().unwrap()
                ));
            }
            let mut call = call.unwrap();
            watchdog(
                "same original abort completion",
                std::future::poll_fn(|cx| call.poll_completed(cx)),
            )
            .await;
            let mut outcome = None;
            call.take_abort_outcome_in(&mut outcome).unwrap();
            assert!(matches!(
                outcome,
                Some(Ok(
                    novarocks_spi::connector::ConnectorWriteAbortOutcome::KnownUncommitted { .. }
                ))
            ));
            let recorded = self
                .session_fixture
                .as_ref()
                .unwrap()
                .recorded
                .lock()
                .unwrap();
            assert_eq!(recorded.finish, 0);
            assert_eq!(recorded.abort, 1);
            if let Some(original) = accepted_before_control {
                assert_eq!(self.token, Some(original));
                assert_eq!(session.accumulated_row_count().unwrap(), 3);
                assert_eq!(recorded.abort_production,
                    Some(novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven));
                assert_eq!(recorded.abort_received_fragments, None);
                assert_eq!(self.ready.as_ref().unwrap().accepted.row_count(), 3);
            }
        } else if publish
            && !order.iter().any(|event| {
                matches!(
                    event,
                    CompletionEvent::TaskFailure
                        | CompletionEvent::SummaryFailure
                        | CompletionEvent::ArrivalRefusal
                )
            })
        {
            let mut call = session
                .prepare_finish_call(self.publication_context.take().unwrap())
                .unwrap();
            watchdog(
                "same original Ready union publication",
                std::future::poll_fn(|cx| call.poll_completed(cx)),
            )
            .await;
            let mut outcome = None;
            call.take_outcome_in(&mut outcome).unwrap();
            assert!(matches!(outcome, Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }))));
            assert_eq!(
                self.session_fixture
                    .as_ref()
                    .unwrap()
                    .recorded
                    .lock()
                    .unwrap()
                    .finish,
                1
            );
        }
        drop(self.ready.take());
        self.cleanup().await;
    }
    async fn cleanup(&mut self) {
        let wait = Duration::from_secs(5);
        // Actual outstanding Root RPCs must return or be canceled/dropped by
        // their original Source. Opening a gate is not a DATA/End/exit receipt.
        self.peer.as_ref().unwrap().release_encoded_root_data();
        self.peer.as_ref().unwrap().release_encoded_root_end();
        let cpu_until = Instant::now() + wait;
        {
            let custody = self.custody.as_ref().unwrap().clone();
            let mut closed = Box::pin(custody.join_closed());
            loop {
                if let Poll::Ready(result) = futures::poll!(closed.as_mut()) {
                    result.unwrap();
                    break;
                }
                assert!(
                    Instant::now() < cpu_until,
                    "same original write CPU did not exit"
                );
                self.drive_retirement_round().await;
            }
        }
        assert_eq!(self.capacity.snapshot().held_positions, [0; 4]);
        drop(self.receiving.take());
        // Do not poll a completed Ready future again during actual retirement.
        drop(self.logical.as_mut().unwrap().ready.take());
        let until = Instant::now() + wait;
        let receipt = loop {
            let exited = futures::poll!(std::future::poll_fn(|cx| self
                .logical
                .as_mut()
                .unwrap()
                .exit
                .as_mut()
                .unwrap()
                .poll_exit(cx)));
            if let Poll::Ready(receipt) = exited {
                break receipt.unwrap();
            }
            if Instant::now() >= until {
                let lp = self
                    .logical
                    .as_ref()
                    .unwrap()
                    .binding
                    .bound()
                    .unwrap()
                    .registration
                    .actor();
                let ap = self.installed.as_ref().unwrap().fixture_sources.0;
                let inputs = self.process.actor_runtime().input_inventory(ap, None, 32);
                let standings = self
                    .process
                    .actor_runtime()
                    .standing_input_inventory(ap, None, 512)
                    .map(|rows| {
                        rows.into_iter()
                            .filter(|role| role.in_flight.is_some())
                            .collect::<Vec<_>>()
                    });
                eprintln!(
                    "original write exit custody: ap_inputs={inputs:?}, ap_roles={standings:?}, ap_batches={:?}, lp_inputs={:?}, unresolved_work={:?}",
                    self.process.actor_runtime().committed_batches(ap, None, 32),
                    self.process.actor_runtime().input_inventory(lp, None, 32),
                    self.process.ledger().unresolved_work()
                );
                if let Ok(inputs) = inputs {
                    for input in inputs {
                        eprintln!(
                            "original write input retirement: input={input:?}, preflight={:?}, header_authors={:?}, effects={:?}",
                            self.process
                                .actor_runtime()
                                .input_retirement_status(ap, input.input),
                            self.process
                                .actor_runtime()
                                .input_header_authors(ap, input.input, 32),
                            self.process.actor_runtime().effect_inventory(
                                ap,
                                input.input,
                                None,
                                32
                            )
                        );
                    }
                }
                panic!(
                    "original write QA exit did not arrive: now={:?}, next={:?}, lp={:?}, ap={:?}, abort={}, release={}, ap_exit={}, native={}",
                    novarocks_coordination_contract::MonotonicClock::now(self.clock.as_ref()),
                    self.process.actor_runtime().next_deadline(),
                    self.process.actor_runtime().snapshot(lp),
                    self.process.actor_runtime().snapshot(ap),
                    self.peer.as_ref().unwrap().single_root_abort_observed(),
                    self.peer.as_ref().unwrap().single_root_release_observed(),
                    *self
                        .installed
                        .as_ref()
                        .unwrap()
                        .fixture_exit
                        .observe_actual_exit()
                        .borrow(),
                    self.logical
                        .as_ref()
                        .unwrap()
                        .binding
                        .original_failure_for_fixture()
                );
            }
            let actual = self.drive_retirement_round().await;
            let ap = self.installed.as_ref().unwrap().fixture_sources.0;
            // Arrival/CPU/Ready paths retain their original clock. Only this
            // failed computation's actual cleanup may select an indexed timer,
            // after the SAME healthy AP has issued its real Abort RPC.
            if self.token.is_none()
                && Instant::now() < until
                && actual.actors == 0
                && actual.hosts == 0
                && self.peer.as_ref().unwrap().single_root_abort_observed()
                && self
                    .process
                    .actor_runtime()
                    .snapshot(ap)
                    .is_ok_and(|original| !original.failed)
                && let Some(next) = self.process.actor_runtime().next_deadline()
            {
                let now = novarocks_coordination_contract::MonotonicClock::now(self.clock.as_ref())
                    .since_origin();
                let deadline = Duration::from_nanos(next.0);
                if deadline > now {
                    self.clock.advance(deadline - now);
                }
            }
        };
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
                .borrow()
        );
        if self.token.is_some() {
            assert_eq!(
                self.peer.as_ref().unwrap().single_root_result_progress(),
                (true, 2)
            );
        }
        if self.token.is_some() {
            assert!(
                self.peer.as_ref().unwrap().single_root_release_observed(),
                "accepted Ready keeps its original normal Release obligation"
            );
        } else {
            assert!(
                self.peer.as_ref().unwrap().single_root_abort_observed(),
                "failed computation reaches its original Abort cleanup"
            );
        }
        drop(receipt);
        self.credentials
            .as_ref()
            .unwrap()
            .seal_for_terminal()
            .unwrap();
        drop(self.installed.take());
        drop(self.logical.take());
        drop(self.output.take());
        drop(self.custody.take());
        drop(self.binding.take());
        drop(self.cohort.take());
        drop(self.requester.take());
        drop(self.session.take());
        drop(self.session_fixture.take());
        drop(self.credential_context.take());
        drop(self.native_storage.take());
        drop(self.credentials.take());
        crate::fixtures::native_recipe::drive_typed(&mut self.typed.as_mut().unwrap().driver);
        assert!(self.typed.as_ref().unwrap().retired.is_reclaimed().unwrap());
        self.cpu_owner
            .shutdown_until(Instant::now() + wait)
            .await
            .unwrap();
        let mut supervisor = self.supervisor.take().unwrap();
        {
            let mut shutdown = Box::pin(supervisor.shutdown_until(Instant::now() + wait));
            loop {
                if let Poll::Ready(result) = futures::poll!(shutdown.as_mut()) {
                    result.unwrap();
                    break;
                }
                self.drive_retirement_round().await;
            }
        }
        drop(supervisor);
        self.process.close_original_host_registration_for_fixture();
        self.fixture.as_mut().unwrap().finish().await;
        drop(self.fixture.take());
        let TypedWriteCustody {
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
                    novarocks_workload_control::joint_admission::JointProgress::More => {
                        tokio::select! {
                            result = released.as_mut() => break result,
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                    novarocks_workload_control::joint_admission::JointProgress::Idle(revision) => {
                        tokio::select! {
                            result = released.as_mut() => break result,
                            _ = progress.wait_state_change(revision) => {}
                        }
                    }
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
            panic!("same original write Control did not drain: {error:?}");
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
        assert!(self.process.original_registries_empty_for_fixture());
        crate::fixtures::history::CapturedHistory::drain(&self.history, 65_536)
            .check_completed_runtime(self.process.actor_runtime(), self.process.ledger())
            .unwrap();
    }
    async fn drive_retirement_round(&mut self) -> crate::fixtures::ProgressObservation {
        crate::fixtures::native_recipe::drive_typed(&mut self.typed.as_mut().unwrap().driver);
        let actual = self
            .fixture
            .as_mut()
            .unwrap()
            .progress(
                crate::fixtures::ProgressSelection::All,
                crate::fixtures::ProgressBudget::default(),
            )
            .await;
        tokio::task::yield_now().await;
        actual
    }
}

fn arrival_refusal() -> novarocks_spi::connector::ConnectorError {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_workload_control::WorkError::Capacity("original Provider arrival refusal"),
    )
}

async fn run_arrival_order_with_summary_source(
    order: &[CompletionEvent],
    publish: bool,
    cut: ControlCut,
    independent_summary_source: bool,
) {
    use crate::fixtures::native_recipe::{
        OriginalRecipeContext, OriginalRecipeOperands, PreparedOriginalRecipeProcess,
        RecipeWriteProtocolRoles, install_original_recipe_with_initial,
        prepare_original_recipe_process_with_budgets,
    };
    let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
    let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let peer = crate::native::task_transport::tests::Loopback::start().await;
    let descriptor = peer.bounded_root_descriptor(novarocks_types::BackendProcessId::new_v7());
    let history = novarocks_control_runtime::history::HistoryCapture::new(65_536);
    let mut result_config = novarocks_workload_control::ResultCapacityConfig::V1;
    result_config.positions[2] = 1;
    result_config.all_objects_bytes[2] = 1024 * 1024;
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
    } = prepare_original_recipe_process_with_budgets(
        &data,
        descriptor,
        history.clone(),
        result_config,
        Some(&mut cpu_owner),
        Duration::from_secs(1),
        Duration::from_secs(5),
    );
    let body =
        crate::query_execution::write_result::tests::component_write_body(commit_fragment_bytes());
    let (root, payload, stage, computation, retired) =
        admit_write_originals(&workload, &mut joint, body.len());
    let stage_scope = stage.scope();
    let requester = stage.cancellation_requester();
    let mut typed = TypedWriteCustody {
        root,
        payload,
        computation: Some(computation),
        retired,
        driver: joint,
        progress: workload.deadline_expiry_handle(),
    };
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &typed.root.owner.scope(),
        &stage_scope,
        &typed.payload,
    )
    .unwrap();
    let initial = InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
    let credentials = initial.credentials();
    let context = initial
        .connector_request_context(request_context(), None)
        .unwrap();
    offer_original_credentials(context.vended_credential_lease_sink().unwrap().as_ref());
    let source = credentials.take_source(credentials.execution()).unwrap();
    let leases = source.test_into_credential_leases().unwrap();
    assert!(!leases.leases().is_empty());
    let native_storage = leases.into_storage();
    credentials.verify_native_storage(&native_storage).unwrap();
    assert!(credentials.take_source(credentials.execution()).is_err());
    let session_fixture = fixture_with_outcome(1, 1, known_committed());
    if order.contains(&CompletionEvent::ArrivalRefusal) {
        session_fixture.recorded.lock().unwrap().arrival_refusal = Some(arrival_refusal());
    }
    let session = Arc::clone(&session_fixture.session);
    credentials.bind_write_production_session(&session).unwrap();
    session
        .retain_computation_credentials(&credentials)
        .unwrap();
    let publication_stop = novarocks_spi::connector::ConnectorStopOwner::new();
    let deadline = if order.contains(&CompletionEvent::DeadlineExpired) {
        Instant::now()
    } else {
        Instant::now() + Duration::from_secs(60)
    };
    let publication_context = novarocks_spi::connector::ConnectorRequestContext::try_new(
        deadline,
        publication_stop.view(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap();
    let carrier = crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
        novarocks_plan_codec::physical_package_v2::PACKAGE_ADMISSION_FIXTURE,
    );
    let compile = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let (description, plan, root_recipe) = crate::query_execution::internal_consumer::test_fixture::internal_write_encoded_description(
        &carrier, &compile).unwrap();
    let root_recipe = root_recipe
        .with_three_task_protocol_chain_with_delivery(
            plan,
            [2, 3],
            novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain,
        )
        .with_write_protocol_roles(if independent_summary_source {
            RecipeWriteProtocolRoles::IndependentSummarySource
        } else {
            RecipeWriteProtocolRoles::UpstreamWriters
        });
    let targets = [novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap()];
    let seed = WriteComputationSeed {
        contract: RootWriteDecodeContract::try_new(
            &targets,
            session.targets(),
            novarocks_result_contract::PerKind::empty(),
        )
        .unwrap(),
        decode: session.computation_decode(&targets).unwrap(),
        session: session.clone(),
        initial: credentials.clone(),
        production_execution: None,
    };
    let (consumer, offers, output) = write_computation_consumer(
        &description,
        plan,
        seed,
        DecodedConsumerRuntime {
            computation: typed.computation.take(),
            binding: binding.clone(),
            cpu: cpu_owner.runtime(),
            runtime: data.clone(),
            cancellation: QueryCancellationView::governed(
                stage_scope.cancellation().unwrap(),
                None,
            ),
        },
    )
    .unwrap();
    let custody = output.cpu_custody();
    let (logical, cohort, output) = install_original_recipe_with_initial(
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
            root: root_recipe,
            consumer,
            offers,
            output,
        },
        initial,
        native_storage.clone(),
    );
    let mut original = RetainedWriteRun(Some(Box::new(ActualWriteRun {
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
        result: None,
        receiving: None,
        ready: None,
        token: None,
        installed: None,
        cpu_owner,
        data,
        peer: Some(peer),
        history,
        session_fixture: Some(session_fixture),
        session: Some(session),
        credentials: Some(credentials),
        native_storage: Some(native_storage),
        credential_context: Some(context),
        publication_context: Some(publication_context),
        publication_stop,
        requester: Some(requester),
        stage_scope,
        plan,
    })));
    original
        .0
        .as_mut()
        .unwrap()
        .complete(order, publish, cut, independent_summary_source)
        .await;
    drop(original.0.take());
}

fn admit_write_originals(
    control: &novarocks_workload_control::WorkloadControl,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    body_bytes: usize,
) -> (
    novarocks_workload_control::RootWork,
    novarocks_workload_control::joint_admission::RetainedPayloadGrant,
    novarocks_workload_control::WorkOwner,
    novarocks_workload_control::joint_admission::InternalComputationGrant,
    novarocks_workload_control::WorkRetirementObserver,
) {
    use novarocks_workload_control::{WorkClass, WorkRequest, joint_admission as joint};
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let domains = scope.prepare_root_domains().unwrap();
    let source = domains
        .initial_payload_source(&scope, joint::JointLane::Custody)
        .unwrap();
    let mut proposal = match source
        .try_begin(
            NonZeroU64::new(std::mem::size_of::<TypedWriteCustody>() as u64).unwrap(),
            NonZeroU64::new(u64::try_from(body_bytes).unwrap()).unwrap(),
        )
        .unwrap()
    {
        joint::JointTryBegin::Started(proposal) => proposal,
        joint::JointTryBegin::Full(_) => {
            panic!("fresh original write Root has no outstanding constructor")
        }
    };
    proposal
        .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
        .unwrap();
    let mut initial = proposal.submit().unwrap();
    crate::fixtures::native_recipe::drive_typed(driver);
    let mut payload = None;
    match initial.poll_install_in(
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
        &scope,
        &mut payload,
    ) {
        Poll::Ready(Ok(notice)) => notice.notify(),
        _ => panic!("same original write Root source must install"),
    }
    drop(initial);
    crate::fixtures::native_recipe::drive_typed(driver);
    let payload = payload.unwrap();
    let mut pending = scope.request_joint_child().unwrap();
    crate::fixtures::native_recipe::drive_typed(driver);
    let mut ready = match std::pin::Pin::new(&mut pending)
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("same paid write child reply must complete"),
    };
    let mut stage = None;
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&scope, &mut stage)
        .unwrap()
        .notify();
    drop((ready, pending));
    crate::fixtures::native_recipe::drive_typed(driver);
    let stage = stage.unwrap();
    let retired = stage.retirement_observer().unwrap();
    let mut proposal = payload
        .root_stage_source(&scope, &stage)
        .unwrap()
        .begin(0, joint::JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut admission = proposal.submit().unwrap();
    crate::fixtures::native_recipe::drive_typed(driver);
    let mut ready = match std::pin::Pin::new(&mut admission)
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("same cap-one write CPU position must be available"),
    };
    let mut computation = None;
    ready
        .install_in(&scope, &payload, &stage, &mut computation)
        .unwrap()
        .notify();
    drop((ready, admission));
    crate::fixtures::native_recipe::drive_typed(driver);
    (root, payload, stage, computation.unwrap(), retired)
}

async fn run_case(fail_writer: bool) {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            if fail_writer {
                CompletionEvent::TaskFailure
            } else {
                CompletionEvent::WriterSuccess
            },
        ],
        false,
        ControlCut::Arrival,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_write_eof_then_original_writer_proof_then_second_cpu_union_reaches_ready() {
    run_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn fixed_write_original_te_task_failure_after_end_keeps_exact_cause_and_no_union() {
    run_case(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn independent_summary_terminal_and_original_writer_accept_every_causal_arrival_order() {
    let orders = causal_orders(&[
        CompletionEvent::RootRows,
        CompletionEvent::RootEnd,
        CompletionEvent::WriterSuccess,
        CompletionEvent::SummarySuccess,
    ]);
    assert_eq!(orders.len(), 12);
    for order in orders {
        run_arrival_order_with_summary_source(&order, true, ControlCut::Arrival, true).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn summary_source_failure_after_eof_and_writer_success_keeps_original_cause_and_no_union() {
    run_arrival_order_with_summary_source(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::SummaryFailure,
        ],
        false,
        ControlCut::Arrival,
        true,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_control_after_eof_and_writer_success_vetoes_before_summary_session_union() {
    for control in [CompletionEvent::Cancelled, CompletionEvent::DeadlineExpired] {
        run_arrival_order_with_summary_source(
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                control,
            ],
            true,
            ControlCut::Arrival,
            true,
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exact_root_rows_eof_and_all_success_accept_every_causal_arrival_order() {
    let orders = causal_orders(&[
        CompletionEvent::RootRows,
        CompletionEvent::RootEnd,
        CompletionEvent::WriterSuccess,
    ]);
    assert_eq!(orders.len(), 3);
    for order in orders {
        run_arrival_order(&order, true, ControlCut::Arrival).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task_failure_cancel_and_deadline_veto_every_causal_arrival_order() {
    let cases: &[(&[CompletionEvent], usize)] = &[
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::TaskFailure,
            ],
            3,
        ),
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                CompletionEvent::Cancelled,
            ],
            12,
        ),
        (
            &[
                CompletionEvent::RootRows,
                CompletionEvent::RootEnd,
                CompletionEvent::WriterSuccess,
                CompletionEvent::DeadlineExpired,
            ],
            12,
        ),
    ];
    for (events, count) in cases {
        let orders = causal_orders(events);
        assert_eq!(orders.len(), *count);
        for order in orders {
            run_arrival_order(&order, true, ControlCut::Arrival).await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_ready_then_cancel_vetoes_only_the_original_publication_claim() {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::Cancelled,
        ],
        true,
        ControlCut::AfterReady,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_ready_then_expired_context_vetoes_only_the_original_publication_claim() {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::WriterSuccess,
            CompletionEvent::DeadlineExpired,
        ],
        true,
        ControlCut::AfterReady,
    )
    .await;
}

#[tokio::test]
async fn distinct_stage_credentials_keep_one_write_union_through_terminal_context_exit() {
    use crate::query_execution::write_session::tests::{commit_unknown, fragment_bytes};
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose,
        CatalogProperties, ConnectorStorageResolver, CredentialConsumerRole, StorageAccessRequest,
        StorageCredentialScopePrefix, VendedS3CredentialLeaseContribution,
        VendedS3CredentialLeaseEntry,
    };
    for unknown in [false, true] {
        let (mut identity_owner, client) =
            crate::query_execution::logical_execution_test_profile::process(
                tokio::runtime::Handle::current(),
                Arc::new(ReservationOnly),
                QueryProcessNamespace::new(0x9b8),
                FrontendProcessId::new_v7(),
                LogicalExecutionSupervisorConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroUsize::MIN,
                    LogicalExecutionRowsConfig::new(
                        NonZeroUsize::MIN,
                        NonZeroU32::MIN,
                        Duration::from_secs(1),
                    ),
                ),
            );
        let fixture = fixture_with_outcome(
            1,
            16,
            if unknown {
                commit_unknown()
            } else {
                known_committed()
            },
        );
        let session = Arc::clone(&fixture.session);
        let properties = CatalogProperties::new(
            session.catalog_properties().handle().clone(),
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let request = StorageAccessRequest::try_new(
            properties.handle().clone(),
            "s3://warehouse/db/t/metadata/v2.metadata.json",
        )
        .unwrap();
        let target = novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0).unwrap();
        let mut originals = Vec::new();
        let mut selected_lease = None;
        for stage in 0..2 {
            let initial =
                InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
            let credentials = initial.credentials();
            let context = initial
                .connector_request_context(request_context(), None)
                .unwrap();
            context
                .vended_credential_lease_sink()
                .unwrap()
                .offer_vended_s3_credential_lease(
                    &properties,
                    VendedS3CredentialLeaseContribution::try_new(
                        vec![
                            VendedS3CredentialLeaseEntry::try_new(
                                StorageCredentialScopePrefix::try_from_normalized(
                                    "s3://warehouse/db/t",
                                )
                                .unwrap(),
                                u64::MAX,
                                SecretValue::new("stage-access"),
                                SecretValue::new("stage-secret"),
                                SecretValue::new("stage-token"),
                            )
                            .unwrap(),
                        ],
                        None,
                    )
                    .unwrap(),
                )
                .unwrap();
            credentials.bind_write_production_session(&session).unwrap();
            session
                .retain_computation_credentials(&credentials)
                .unwrap();
            session
                .retain_computation_credentials(&credentials)
                .unwrap();
            // The original first-party relation cannot be overwritten by a
            // different Session before or after typed registration.
            let foreign = fixture_with_outcome(1, 16, known_committed());
            assert!(
                credentials
                    .bind_write_production_session(&foreign.session)
                    .is_err()
            );
            assert!(
                foreign
                    .session
                    .retain_computation_credentials(&credentials)
                    .is_err()
            );
            assert!(
                foreign
                    .session
                    .following_terminal_context(request_context())
                    .unwrap()
                    .storage_resolver()
                    .is_none()
            );
            assert!(
                credentials
                    .write_production_session()
                    .unwrap()
                    .ptr_eq(&Arc::downgrade(&session))
            );
            let terminal = credentials.terminal_storage_resolver();
            selected_lease = Some(terminal.resolve_vended_s3(&request).unwrap().lease_id());
            originals.push(Arc::downgrade(&terminal));
            let source = credentials.take_source(credentials.execution()).unwrap();
            let storage = source.test_into_credential_leases().unwrap().into_storage();
            credentials.verify_native_storage(&storage).unwrap();
            // Real Session decode/accept/union, with unique canonical fragments.
            // This finite relation fixture does not claim Native execution.
            let decoded = session
                .computation_decode(&[target])
                .unwrap()
                .interpret(
                    crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(
                        3 + stage,
                        vec![(
                            target,
                            fragment_bytes(&format!(
                                "s3://warehouse/db/t/data/stage-{stage}.parquet"
                            )),
                        )],
                    ),
                )
                .unwrap();
            session.receiving_port().accept(decoded).unwrap();
            drop((storage, terminal, credentials, initial, context));
            assert!(
                originals
                    .iter()
                    .all(|original| original.upgrade().is_some())
            );
        }
        assert_eq!(session.accumulated_row_count().unwrap(), 7);
        let following = session
            .following_terminal_context(request_context())
            .unwrap();
        assert_eq!(
            following
                .storage_resolver()
                .unwrap()
                .resolve_vended_s3(&request)
                .unwrap()
                .lease_id(),
            selected_lease.unwrap()
        );
        let completion = session.publish_for_test(request_context()).unwrap();
        assert_eq!(matches!(&completion, novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }), unknown);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 7);
        assert_eq!(
            fixture.recorded.lock().unwrap().terminal_storage,
            Some(Ok("s3://warehouse/db/t".into()))
        );
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_some())
        );
        drop((completion, session, fixture));
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_some()),
            "the following Context is the actual last router holder"
        );
        assert_eq!(
            following
                .storage_resolver()
                .unwrap()
                .resolve_vended_s3(&request)
                .unwrap()
                .lease_id(),
            selected_lease.unwrap()
        );
        drop(following);
        assert!(
            originals
                .iter()
                .all(|original| original.upgrade().is_none())
        );
        watchdog(
            "two-reservation QA owner actual exit",
            identity_owner.shutdown_until(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .unwrap();
    }
}

mod receipt_failures;
mod receipt_order;
