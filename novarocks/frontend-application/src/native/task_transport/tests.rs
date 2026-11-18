// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Wire-level coverage of the task carrier.
//!
//! Every test drives the real client against a loopback Tonic peer built
//! from the frontend's own generated stub, so batch framing, receipt
//! attribution, status streaming, and the deployment JWT interceptor are
//! exercised rather than mocked.

#[test]
fn original_method_run_progress_quotes_the_full_supported_count_and_rejects_mixed_runs() {
    use novarocks_workload_control::joint_admission::JointResource;
    let budget = TransportBudget::DEFAULT;
    let backend = BackendProcessId::new_v7();
    let operations: Vec<_> = (0..budget.max_batch_items())
        .map(|task| cancel_intent(task as u32 + 1, backend))
        .collect();
    let quote =
        NativeTaskOperationSink::progress_for_original_method_run(&operations, budget).unwrap();
    let foreground = quote.foreground().unwrap();
    let settlement = quote.settlement().unwrap();
    assert_eq!(foreground, settlement);
    assert_eq!(foreground.get(JointResource::AsyncIo), 1);
    assert_eq!(foreground.get(JointResource::PhysicalQueueRecords), 1);
    assert_eq!(
        foreground.get(JointResource::PhysicalQueueBytes),
        joint_action::original_run_encoding(&operations, budget)
            .unwrap()
            .protobuf_bytes() as u64
    );
    assert_eq!(
        quote.custody().get(JointResource::ReplyRecords),
        operations.len() as u64
    );
    assert_eq!(quote.custody().get(JointResource::QueueRecords), 0);
    assert_eq!(
        quote.custody().get(JointResource::PayloadBytes),
        (budget.message_limits().control_response_max_bytes
            + novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_response(
                operations.len(),
                budget.message_limits().control_response_max_bytes,
            )
            .unwrap()
            .peak_bytes()) as u64
    );
    let mut over = operations.clone();
    over.push(cancel_intent(99, backend));
    assert!(NativeTaskOperationSink::progress_for_original_method_run(&over, budget).is_err());
    assert!(NativeTaskOperationSink::progress_for_original_method_run(&[], budget).is_err());
    let same_control_method = [
        cancel_intent(1, backend),
        abort_intent(participation(backend)),
    ];
    assert_eq!(
        same_control_method[0].native_run_key(),
        same_control_method[1].native_run_key()
    );
    assert!(
        NativeTaskOperationSink::progress_for_original_method_run(&same_control_method, budget)
            .is_ok()
    );
    let mixed = [cancel_intent(1, backend), update_intent(2, backend, 1)];
    assert_ne!(mixed[0].native_run_key(), mixed[1].native_run_key());
    assert!(NativeTaskOperationSink::progress_for_original_method_run(&mixed, budget).is_err());
}

#[test]
fn original_small_method_runs_invoice_the_actual_codec_without_a_composition_envelope() {
    use novarocks_workload_control::joint_admission::JointResource;
    let budget = TransportBudget::DEFAULT;
    let backend = BackendProcessId::new_v7();
    let runs = [
        vec![
            cancel_intent(1, backend),
            abort_intent(participation(backend)),
        ],
        vec![renew_intent(backend)],
        vec![update_intent(1, backend, 128)],
        (1..=budget.max_batch_items())
            .map(|id| cancel_intent(id as u32, backend))
            .collect(),
    ];
    for operations in runs {
        let plan = joint_action::original_run_encoding(&operations, budget).unwrap();
        let encoded = operations
            .iter()
            .map(|item| {
                (
                    is_small_control(item),
                    encode_operation(item, &test_attempt_facts()).unwrap(),
                )
            })
            .collect();
        let batches = encode_method_batches(encoded, budget).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(plan.retained_bytes(), batches[0].encoded_len() + 5);
        let actual =
            NativeTaskOperationSink::progress_for_original_method_run(&operations, budget).unwrap();
        let payload = actual
            .foreground()
            .unwrap()
            .get(JointResource::PayloadBytes);
        assert_eq!(
            payload,
            plan.retained_bytes() as u64
                + joint_action::clone_input_backing(&operations).unwrap() as u64
        );
        assert!(
            payload < 64 * 1024,
            "small full runs must not reserve the maximum 96 MiB wire envelope"
        );
        assert_eq!(
            actual.custody().get(JointResource::ReplyRecords),
            operations.len() as u64
        );
        assert_eq!(
            actual
                .foreground()
                .unwrap()
                .get(JointResource::PhysicalQueueRecords),
            1
        );
        assert_eq!(
            actual
                .foreground()
                .unwrap()
                .get(JointResource::PhysicalQueueBytes),
            plan.protobuf_bytes() as u64
        );
        assert_eq!(plan.retained_bytes(), plan.protobuf_bytes() + 5);
    }
}

#[test]
fn original_dispatch_batch_drives_the_input_gate_before_dto_construction() {
    use super::action_input::{NativeApplyInputPlan, known_input_layouts_for_composition};
    use std::{alloc::Layout, num::NonZeroUsize};
    let backend = BackendProcessId::new_v7();
    let batch = released_batch(
        backend,
        vec![cancel_intent(1, backend), cancel_intent(2, backend)],
    );
    let plan = NativeApplyInputPlan::from_operations(batch.operations(), TransportBudget::DEFAULT)
        .unwrap();
    assert_eq!(plan.items(), batch.operations().len());
    let layouts = known_input_layouts_for_composition(
        NonZeroUsize::new(plan.items()).unwrap(),
        TransportBudget::DEFAULT,
    )
    .unwrap();
    fn frame<F: Future<Output = ()>>(
        _: fn(ApplySend, Vec<EncodedMethodBatch>, tokio::time::Instant) -> F,
    ) -> Layout {
        Layout::new::<F>()
    }
    assert_eq!(layouts.original.future, frame(apply_operations));
    let too_many = vec![cancel_intent(1, backend); TransportBudget::DEFAULT.max_batch_items() + 1];
    assert!(NativeApplyInputPlan::from_operations(&too_many, TransportBudget::DEFAULT).is_err());
    assert!(NativeApplyInputPlan::from_operations(&[], TransportBudget::DEFAULT).is_err());
    let error = encode_method_batches(
        vec![
            (false, proto::TaskOperation::default());
            TransportBudget::DEFAULT.max_batch_items() + 1
        ],
        TransportBudget::DEFAULT,
    );
    assert!(error.is_err());
}

use std::collections::VecDeque;

use novarocks_execution::task_execution::{
    AbortCause, AbortParticipation, AdmissionTicketId, CancelReason, CancelTask, CreateTaskReceipt,
    CredentialDomainId, CredentialEpoch, CredentialUpdate, DomainVersion, EstablishParticipation,
    FetchTaskDynamicFilters, LeaseSequence, LeaseValidFor, ParticipationConvergenceReceipt,
    ParticipationConvergenceState, ParticipationConvergenceVersion, ParticipationReceipt,
    ParticipationState, QuiesceParticipationReceipt, RenewParticipationLease,
    TaskConvergenceCursor, TaskConvergenceReceipt, TaskConvergenceVersion, TaskDomainUpdate,
    TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion, UpdateTask,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{catalog, filter};
use novarocks_task_codec::domain::{WireContent, WireCredential};
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredObservationBookmark, decode_covered_subscribe_task_status,
    encode_covered_status_event, encode_operation_outcome, encode_participation_ack,
};
use novarocks_task_codec::operation::{
    ESTABLISH_CATALOG_DOMAIN_TAG, ESTABLISH_FILTER_DOMAIN_TAG, ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
};
use novarocks_types::identity::{FrontendProcessId, QueryExecutionId, StageId, TaskId};
use novarocks_types::{AttemptId, QueryId};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::native::transport_supervisor::NativeTransportSupervisor;
use crate::wake::CountingWake;
use novarocks_native_adapter::generated::nova_rocks_grpc_server::{
    NovaRocksGrpc, NovaRocksGrpcServer,
};

use super::*;

// -----------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------

/// The original Native test role and SAME Workload authority. Cleanup joins
/// original Apply/outgoing tasks before settling its real Root.
struct NativeRoleFixture {
    runtime: FrontendDataRuntime,
    workload: novarocks_workload_control::WorkloadControl,
    root: Option<novarocks_workload_control::RootWork>,
}

impl NativeRoleFixture {
    fn new() -> Self {
        use novarocks_workload_control::{WorkClass, WorkRequest, WorkloadConfig, WorkloadControl};
        let config = WorkloadConfig::default();
        config.validate().unwrap();
        let workload = WorkloadControl::try_new_counted(config).unwrap().owner;
        workload.mark_ready().unwrap();
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        Self {
            runtime: FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            workload,
            root: Some(root),
        }
    }

    async fn settle(mut self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        self.runtime
            .drain_original_apply_until(deadline)
            .await
            .unwrap();
        self.runtime
            .drain_native_outgoing_until(deadline)
            .await
            .unwrap();
        let root = self.root.take().unwrap();
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        self.workload.close_admission();
        assert!(self.workload.shutdown().is_ok());
    }
}

fn execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(0x7a5c, 0x0006),
        AttemptId::new(1).expect("attempt one is nonzero"),
    )
    .expect("a nonzero query id")
}

fn identity(task: u32, backend: BackendProcessId) -> TaskIdentity {
    TaskIdentity::new(
        execution_id(),
        StageId::new(1).expect("a nonzero stage id"),
        TaskId::new(task).expect("a nonzero task id"),
        backend,
    )
}

fn status_at(identity: TaskIdentity, version: TaskStatusVersion) -> TaskStatus {
    TaskStatus::try_new(
        identity,
        version,
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .expect("a running task status")
}

fn participation(backend: BackendProcessId) -> ParticipationRef {
    ParticipationRef::new(execution_id(), FrontendProcessId::new_v7(), backend)
}

fn cancel_intent(task: u32, backend: BackendProcessId) -> OperationIntent {
    OperationIntent::CancelTask(CancelTask::new(
        TaskOperationId::new_v7(),
        identity(task, backend),
        CancelReason::UpstreamNoLongerNeeded,
    ))
}

fn abort_intent(participation: ParticipationRef) -> OperationIntent {
    OperationIntent::AbortParticipation(AbortParticipation::new(
        TaskOperationId::new_v7(),
        participation,
        AbortCause::QueryFailed,
    ))
}

fn update_intent(task: u32, backend: BackendProcessId, version: u64) -> OperationIntent {
    OperationIntent::UpdateTask(Arc::new(
        UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity(task, backend),
            vec![TaskDomainUpdate::TaskDynamicFilter {
                version: DomainVersion::new(version).expect("a positive domain version"),
                payload: Arc::new(WireContent::new(
                    b"novarocks.test.task_dynamic_filter.v1",
                    filter::RuntimeFilterEnvelope::default(),
                )),
            }],
        )
        .expect("one domain is an update"),
    ))
}

fn renew_intent(backend: BackendProcessId) -> OperationIntent {
    OperationIntent::UpdateParticipation(Arc::new(UpdateParticipation::RenewLease(
        RenewParticipationLease::new(
            TaskOperationId::new_v7(),
            participation(backend),
            LeaseSequence::INITIAL.next().expect("no overflow"),
            LeaseValidFor::new(Duration::from_secs(5)).expect("a legal lease duration"),
        ),
    )))
}

/// Assemble exact wire-test metadata. Actual submission replaces the placeholder
/// permits with the original governed queue grants; this proves no dispatch policy.
fn released_batch(backend: BackendProcessId, intents: Vec<OperationIntent>) -> DispatchBatch {
    let lane = intents.first().expect("one original operation").lane();
    assert!(
        intents
            .iter()
            .all(|intent| intent.backend_process_id() == backend && intent.lane() == lane)
    );
    let queued_bytes = intents
        .iter()
        .try_fold(0usize, |bytes, intent| {
            bytes.checked_add(intent.queued_bytes())
        })
        .expect("original batch bytes fit");
    DispatchBatch::test_fixture(backend, lane, intents, queued_bytes)
}

/// A seam double for operations that need no retained typed content.
/// The attempt-level wire facts every test sink carries.
fn test_attempt_facts() -> AttemptWireFacts {
    AttemptWireFacts {
        native_compatibility_id: novarocks_types::NativeCompatibilityId::new([0x71; 32]),
    }
}

#[test]
fn an_establish_encodes_the_query_options_owned_by_its_intent() {
    let backend = BackendProcessId::new_v7();
    let expected = proto::QueryOptions {
        query_mem_limit: 4096,
        pipeline_dop: 3,
        ..proto::QueryOptions::default()
    };
    let request = UpdateParticipation::Establish(EstablishParticipation::new(
        TaskOperationId::new_v7(),
        participation(backend),
        AdmissionTicketId::try_from_bytes([0x54; 16]).expect("the ticket is nonzero"),
        Arc::new(WireContent::new(
            ESTABLISH_CATALOG_DOMAIN_TAG,
            catalog::CatalogSet::default(),
        )),
        Arc::new(WireContent::new(
            ESTABLISH_FILTER_DOMAIN_TAG,
            proto::RuntimeFilterContribution::default(),
        )),
        Arc::new(WireContent::new(
            ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
            expected,
        )),
        CredentialUpdate::new(
            CredentialDomainId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(
                WireCredential::decode(&[], FieldPath::root("credential"))
                    .expect("an empty credential table is legal"),
            ),
        ),
        LeaseValidFor::new(Duration::from_secs(30)).expect("a legal lease"),
    ));

    let encoded = encode_participation_operation(&request, &test_attempt_facts())
        .expect("the establish is codec-owned");
    let UpdateParticipation::Establish(original) = &request else {
        unreachable!()
    };
    assert_eq!(
        novarocks_task_codec::operation::wire_size::establish(original).unwrap(),
        prost::Message::encoded_len(&encoded)
    );
    let Some(proto::task_operation::Operation::UpdateParticipation(update)) = encoded.operation
    else {
        panic!("an establish encodes as UpdateParticipation");
    };
    let Some(proto::update_participation_request::Command::Establish(establish)) = update.command
    else {
        panic!("the update carries an establish");
    };
    assert_eq!(establish.query_options, Some(expected));
}

// -----------------------------------------------------------------------
// The loopback peer
// -----------------------------------------------------------------------

/// How the peer answers one `ApplyTaskOperations`.
#[derive(Clone, Debug)]
enum ApplyAnswer {
    /// One receipt per request item, in request order.
    Outcomes(Vec<OperationOutcome>),
    /// The same receipts, reversed.
    Reversed(Vec<OperationOutcome>),
    /// The same receipts with the last one missing.
    Truncated(Vec<OperationOutcome>),
    /// One receipt per request item, each with its own body.
    WithAck(Vec<(OperationOutcome, Option<proto::task_operation_verdict::Ack>)>),
    Reject(tonic::Code),
    RejectWithMessage(tonic::Code, &'static str),
}

/// How the peer answers one `SubscribeTaskStatus`.
enum SubscribeAnswer {
    /// Deliver these events, then close the stream.
    EventsThenClose(Vec<proto::TaskStatusStreamEvent>),
    /// Deliver these events and keep the stream open.
    EventsThenHold(Vec<proto::TaskStatusStreamEvent>),
    Reject(tonic::Code),
    StallOpening,
}

/// One fixed healthy external peer script for the installed-host test. These
/// are wire facts authored by this peer, never FE local-work exit reports.
/// Only the fixed external test peer waits here. A release is persistent;
/// registration precedes checking it, and canceled RPCs drop their waiter.
struct SingleRootIoGate {
    released: std::sync::atomic::AtomicBool,
    waiters: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

struct SingleRootIoWaiter<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for SingleRootIoWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl SingleRootIoGate {
    fn new() -> Self {
        Self {
            released: std::sync::atomic::AtomicBool::new(false),
            waiters: std::sync::atomic::AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }

    async fn wait(&self) {
        use std::sync::atomic::Ordering;
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let _waiter = SingleRootIoWaiter(&self.waiters);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn waiting(&self) -> bool {
        self.waiters.load(std::sync::atomic::Ordering::Acquire) != 0
    }

    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::Release);
        self.changed.notify_waiters();
    }
}

struct SingleRootIoGates {
    first_subscription_selected: bool,
    first_subscription: Arc<SingleRootIoGate>,
    create: Arc<SingleRootIoGate>,
}

/// The default peer remains the original empty root. EncodedData is one
/// caller-authored canonical segment; this peer never encodes domain values or
/// derives a row count from an artifact, query, Task status or payload length.
enum SingleRootResult {
    EndOnly,
    EncodedData {
        data: novarocks_execution_contract::root_result::RootResultData,
        output_rows: u64,
    },
}

enum SingleRootTerminal {
    Finished,
    Failed(Box<novarocks_execution_contract::TaskFailure>),
}
struct SingleRootFinalInfo {
    operators: Vec<novarocks_execution_contract::OperatorStatistics>,
    truncated: bool,
    requested: bool,
}

struct SingleRootLifecycle {
    exchange_updates:
        std::collections::BTreeMap<TaskIdentity, three_task_recipe::ScriptedExchangeUpdates>,
    other_tasks: std::collections::BTreeMap<TaskIdentity, three_task_recipe::OtherTaskSlot>,
    final_info: std::collections::BTreeMap<TaskIdentity, SingleRootFinalInfo>,
    io_gates: Option<SingleRootIoGates>,
    root_data_gate: Option<Arc<SingleRootIoGate>>,
    root_end_gate: Option<Arc<SingleRootIoGate>>,
    root: TaskIdentity,
    kind: novarocks_result_contract::RootOutputKind,
    result: SingleRootResult,
    terminal: Option<SingleRootTerminal>,
    failing_revision: Option<u64>,
    participation: Option<ParticipationRef>,
    ticket_issued: bool,
    revision: u64,
    running_revision: Option<u64>,
    status_revision: Option<u64>,
    task_revision: Option<u64>,
    quiesce_revision: Option<u64>,
    participation_revision: Option<u64>,
    root_end_offered: bool,
    root_offered_through: u64,
    root_consumed: u64,
    established: bool,
    created: bool,
    quiesce: Option<QuiesceParticipationReceipt>,
    released: bool,
    abort_cause: Option<AbortCause>,
    close_finished_tasks_on_abort: bool,
}

impl SingleRootLifecycle {
    fn bind_participation(&mut self, participation: ParticipationRef) -> Result<(), Status> {
        if self.root.verify_participation(participation).is_err()
            || self
                .participation
                .is_some_and(|original| original != participation)
        {
            return Err(Status::failed_precondition(
                "single-root peer participation mismatch",
            ));
        }
        self.participation = Some(participation);
        Ok(())
    }

    fn next_revision(&mut self) -> u64 {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("the declared finite peer facts fit");
        self.revision
    }

    fn quiesce_receipt(&self) -> &QuiesceParticipationReceipt {
        self.quiesce
            .as_ref()
            .expect("the first exact fence receipt is retained")
    }

    fn require_participation(&self, participation: ParticipationRef) -> Result<(), Status> {
        if self.participation != Some(participation)
            || self.root.verify_participation(participation).is_err()
        {
            return Err(Status::failed_precondition(
                "single-root peer participation mismatch",
            ));
        }
        Ok(())
    }

    fn running_status(&self) -> TaskStatus {
        TaskStatus::try_new(
            self.root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .expect("the fixed peer started its one installed root")
    }

    fn finished_status(&self) -> TaskStatus {
        TaskStatus::try_new(
            self.root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("the fixed peer finished its one installed root")
    }

    fn current_status(&self) -> TaskStatus {
        match &self.terminal {
            None => self.running_status(),
            Some(SingleRootTerminal::Finished) => self.finished_status(),
            Some(SingleRootTerminal::Failed(original)) => TaskStatus::try_new(
                self.root,
                TaskStatusVersion::new(4).unwrap(),
                TaskState::Failed,
                Some(novarocks_execution_contract::TerminationDetail::Failed(
                    original.as_ref().clone(),
                )),
                TaskOutputFacts::default(),
            )
            .expect("the original fixed Task failure retains its exact phase and fact"),
        }
    }
    fn assign_terminal_revisions(&mut self) {
        if matches!(self.terminal, Some(SingleRootTerminal::Failed(_))) {
            self.failing_revision = Some(self.next_revision());
        }
        self.status_revision = Some(self.next_revision());
        self.task_revision = Some(self.next_revision());
    }
    fn terminal_frames(&self) -> Vec<proto::TaskStatusStreamEvent> {
        let Some(terminal) = &self.terminal else {
            return Vec::new();
        };
        let mut frames = Vec::new();
        if let SingleRootTerminal::Failed(original) = terminal {
            let failing = TaskStatus::try_new(
                self.root,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Failing,
                Some(novarocks_execution_contract::TerminationDetail::Failed(
                    original.as_ref().clone(),
                )),
                TaskOutputFacts::default(),
            )
            .expect("the same original cause is present on the legal Failing prefix");
            frames.push(self.frame(
                CoveredStatusStreamFact::Status(failing),
                self.failing_revision,
            ));
        }
        frames.push(self.frame(
            CoveredStatusStreamFact::Status(self.current_status()),
            self.status_revision,
        ));
        frames.push(self.frame(
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                self.root,
                TaskConvergenceVersion::FIRST,
            )),
            self.task_revision,
        ));
        frames
    }
    fn completion_frames(&self) -> Vec<proto::TaskStatusStreamEvent> {
        let mut frames = vec![self.frame(
            CoveredStatusStreamFact::Status(self.running_status()),
            self.running_revision,
        )];
        frames.extend(self.terminal_frames());
        frames
    }

    fn frame(
        &self,
        fact: CoveredStatusStreamFact,
        source_revision: Option<u64>,
    ) -> proto::TaskStatusStreamEvent {
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact,
            source_revision,
        })
        .expect("fixed single-root peer fact encodes")
    }
}

#[derive(Default)]
struct PeerState {
    original_root_socket: Option<std::net::SocketAddr>,
    single_root: Option<SingleRootLifecycle>,
    participation_only: Option<participation_recipe::ParticipationOnly>,
    applied: Vec<proto::ApplyTaskOperationsRequest>,
    single_root_abort_refusals: Vec<(TaskOperationId, tonic::Code)>,
    control_requests: usize,
    heartbeat_descriptor: Option<BackendProcessDescriptor>,
    heartbeats: Vec<proto::HeartbeatRequest>,
    apply_answers: VecDeque<ApplyAnswer>,
    subscribed: Vec<proto::SubscribeTaskStatusRequest>,
    subscribe_answers: VecDeque<SubscribeAnswer>,
    held: Vec<tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>>,
}

#[derive(Clone)]
struct TaskWirePeer {
    state: Arc<Mutex<PeerState>>,
    domain: NativeEndpointDomain,
}

impl Default for TaskWirePeer {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(PeerState::default())),
            domain: NativeEndpointDomain::BackendData,
        }
    }
}

impl TaskWirePeer {
    async fn apply_operations(
        &self,
        request: Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        let request = request.into_inner();
        if self
            .state
            .lock()
            .expect("peer state")
            .participation_only
            .is_some()
        {
            return self.apply_participation_only(request);
        }
        let (scripted, create_gate) = {
            let state = self.state.lock().expect("peer state");
            let script = state.single_root.as_ref();
            let create = request.operations.iter().any(|operation| {
                matches!(
                    operation.operation.as_ref(),
                    Some(proto::task_operation::Operation::CreateTask(_))
                )
            });
            let gate = create
                .then(|| {
                    script
                        .and_then(|script| script.io_gates.as_ref())
                        .map(|gates| Arc::clone(&gates.create))
                })
                .flatten();
            (script.is_some(), gate)
        };
        if let Some(gate) = create_gate {
            // The actual Create RPC remains pending. No model mutation or ACK
            // happens before the fixture releases this original I/O future.
            gate.wait().await;
        }
        if scripted {
            return self.apply_single_root(request);
        }
        let answer = {
            let mut state = self.state.lock().expect("peer state");
            state.applied.push(request.clone());
            state
                .apply_answers
                .pop_front()
                .unwrap_or(ApplyAnswer::Outcomes(vec![
                    OperationOutcome::Accepted;
                    request.operations.len()
                ]))
        };
        let (outcomes, reversed, truncated) = match answer {
            ApplyAnswer::Outcomes(outcomes) => (
                outcomes
                    .into_iter()
                    .map(|outcome| (outcome, None))
                    .collect(),
                false,
                false,
            ),
            ApplyAnswer::Reversed(outcomes) => (
                outcomes
                    .into_iter()
                    .map(|outcome| (outcome, None))
                    .collect(),
                true,
                false,
            ),
            ApplyAnswer::Truncated(outcomes) => (
                outcomes
                    .into_iter()
                    .map(|outcome| (outcome, None))
                    .collect(),
                false,
                true,
            ),
            ApplyAnswer::WithAck(items) => (items, false, false),
            ApplyAnswer::Reject(code) => {
                return Err(Status::new(code, "task wire test peer rejection"));
            }
            ApplyAnswer::RejectWithMessage(code, message) => {
                return Err(Status::new(code, message));
            }
        };
        let mut receipts = request
            .operations
            .iter()
            .zip(outcomes)
            .map(|(operation, (outcome, ack))| proto::TaskOperationReceipt {
                operation_id: operation
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.operation_id.clone()),
                result: Some(proto::task_operation_receipt::Result::Verdict(
                    proto::TaskOperationVerdict {
                        outcome: encode_operation_outcome(outcome),
                        safe_detail: String::new(),
                        safe_field_path: None,
                        ack,
                    },
                )),
            })
            .collect::<Vec<_>>();
        if reversed {
            receipts.reverse();
        }
        if truncated {
            receipts.pop();
        }
        Ok(Response::new(proto::ApplyTaskOperationsResponse {
            receipts,
        }))
    }

    /// Fixed single-root branch only; the existing generic wire tests keep
    /// their original explicit answer queues. Unknown commands never succeed.
    fn apply_single_root(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        use novarocks_execution_contract::{
            LeaseReceipt, ParticipationAdmissionTicketReceipt, ReleaseOutcome,
        };
        use novarocks_task_codec::operation::{
            DecodedOperation as D, DecodedUpdateParticipation as U,
        };
        let decoded = request
            .operations
            .iter()
            .map(|operation| {
                codec::decode_operation(operation, FieldPath::root("single_root_peer"))
                    .map_err(|_| Status::invalid_argument("single-root operation did not decode"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut state = self.state.lock().expect("peer state");
        state.applied.push(request.clone());
        let PeerState {
            single_root,
            held,
            single_root_abort_refusals,
            ..
        } = &mut *state;
        let script = single_root
            .as_mut()
            .expect("enabled fixed single-root script");
        let mut receipts = Vec::with_capacity(request.operations.len());
        for (wire, operation) in request.operations.iter().zip(decoded) {
            let mut finish = false;
            let mut fence = false;
            let mut release = false;
            let mut outcome = OperationOutcome::Accepted;
            let ack = match operation {
                D::AcquireParticipationAdmissionTicket(acquire) => {
                    let participation = acquire.participation();
                    script.bind_participation(participation)?;
                    script.ticket_issued = true;
                    Some(
                        proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(
                            codec::encode_participation_admission_ticket_ack(
                                ParticipationAdmissionTicketReceipt::new(
                                    AdmissionTicketId::try_from_bytes([0x57; 16]).unwrap(),
                                    participation,
                                    acquire.valid_for(),
                                ),
                            ),
                        ),
                    )
                }
                D::UpdateParticipation(U::Establish(establish)) => {
                    script.require_participation(establish.participation())?;
                    if !script.ticket_issued
                        || establish.admission_ticket_id()
                            != AdmissionTicketId::try_from_bytes([0x57; 16]).unwrap()
                        || script.released
                        || script.abort_cause.is_some()
                    {
                        return Err(Status::failed_precondition(
                            "single-root establish is not admitted",
                        ));
                    }
                    script.established = true;
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(
                                establish.participation(),
                                ParticipationState::Active,
                            )
                            .with_lease(LeaseReceipt::new(
                                LeaseSequence::INITIAL,
                                establish.initial_lease_valid_for(),
                                establish.initial_lease_valid_for().get(),
                            )),
                            None,
                        )
                        .expect("active participation and its actual initial lease encode"),
                    ))
                }
                D::UpdateParticipation(U::RenewLease {
                    participation,
                    sequence,
                    valid_for,
                    ..
                }) => {
                    script.require_participation(participation)?;
                    if !script.established || script.released {
                        return Err(Status::failed_precondition(
                            "single-root renew has no active owner",
                        ));
                    }
                    let state = if script.abort_cause.is_some() {
                        ParticipationState::Aborting
                    } else if script.quiesce.is_some() {
                        ParticipationState::Quiescing
                    } else {
                        ParticipationState::Active
                    };
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(participation, state).with_lease(
                                LeaseReceipt::new(sequence, valid_for, valid_for.get()),
                            ),
                            script.abort_cause,
                        )
                        .expect("original participation state and exact renewed lease encode"),
                    ))
                }
                D::CreateTask(create) => {
                    script.require_participation(create.request().participation())?;
                    if !script.declares_task(create.request().identity())
                        || !script.established
                        || script.quiesce.is_some()
                        || script.released
                        || script.abort_cause.is_some()
                    {
                        return Err(Status::failed_precondition(
                            "single-root Create is not admitted",
                        ));
                    }
                    let task = create.request().identity();
                    script.bind_exchange_descriptor(create.request().descriptor())?;
                    if task != script.root {
                        let (status, frames) = script.create_other_task(task)?;
                        for frame in frames {
                            Self::queue_single_root_frame(held, frame)?;
                        }
                        Some(Self::create_ack(task, status))
                    } else {
                        let status = if script.created {
                            script.current_status()
                        } else {
                            TaskStatus::created(script.root).with_installed()
                        };
                        finish = !script.created;
                        if finish {
                            // Each actual modeled publication owns its revision.
                            // Running is published even when a later catch-up
                            // retains only the latest Finished snapshot.
                            script.running_revision = Some(script.next_revision());
                            if script.terminal.is_some() {
                                script.assign_terminal_revisions();
                            }
                        }
                        script.created = true;
                        Some(Self::create_ack(script.root, status))
                    }
                }
                D::UpdateTask(update) => {
                    let (receipt, applied) = script.update_exchange_domains(update.request())?;
                    outcome = if applied {
                        OperationOutcome::Accepted
                    } else {
                        OperationOutcome::Idempotent
                    };
                    Some(proto::task_operation_verdict::Ack::UpdateTask(
                        codec::encode_update_task_ack(&receipt).ok_or_else(|| {
                            Status::internal("the original finite exchange receipt did not encode")
                        })?,
                    ))
                }
                D::QuiesceParticipation(quiesce) => {
                    script.require_participation(quiesce.participation())?;
                    fence = script.quiesce.is_none();
                    if fence {
                        // Freeze the actual first decision, including its
                        // state and accepted membership. Later close state
                        // cannot rewrite an ACK or catch-up snapshot.
                        script.quiesce = Some(QuiesceParticipationReceipt::new(
                            quiesce.participation(),
                            1, // The production first immutable QuiesceDecision fence is 1.
                            script.created_tasks(),
                            if script.abort_cause.is_some() {
                                ParticipationState::Aborting
                            } else {
                                ParticipationState::Quiescing
                            },
                        ));
                        script.quiesce_revision = Some(script.next_revision());
                    } else {
                        outcome = OperationOutcome::Idempotent;
                    }
                    Some(proto::task_operation_verdict::Ack::QuiesceParticipation(
                        codec::encode_quiesce_ack(script.quiesce_receipt())
                            .expect("the exact single-root admission fence encodes"),
                    ))
                }
                D::ReleaseParticipation(original) => {
                    script.require_participation(original.participation())?;
                    if script.quiesce.is_none() && script.abort_cause.is_none() {
                        return Err(Status::failed_precondition(
                            "single-root release precedes its fence",
                        ));
                    }
                    release = !script.released;
                    script.released = true;
                    if release {
                        script.participation_revision = Some(script.next_revision());
                    }
                    let mut receipt = codec::encode_release_ack(
                        original.participation(),
                        ReleaseOutcome::Released,
                        ParticipationState::TerminalRetained,
                        None,
                    )
                    .expect("the original single-root release encodes");
                    receipt.termination_cause =
                        script.abort_cause.map(codec::encode_abort_cause_field);
                    Some(proto::task_operation_verdict::Ack::ReleaseParticipation(
                        receipt,
                    ))
                }
                D::AbortParticipation(abort) => {
                    if let Err(error) = script.require_participation(abort.participation()) {
                        // Preserve the actual refusal code of this same RPC;
                        // this is not a Worker acknowledgement or close fact.
                        single_root_abort_refusals
                            .push((abort.envelope().operation_id(), error.code()));
                        return Err(error);
                    }
                    if !script.released {
                        script.abort_cause = Some(abort.cause());
                    } else {
                        outcome = OperationOutcome::Idempotent;
                    }
                    Some(proto::task_operation_verdict::Ack::Participation(
                        encode_participation_ack(
                            &ParticipationReceipt::new(
                                abort.participation(),
                                if script.released
                                    || (script.close_finished_tasks_on_abort
                                        && script.created
                                        && script.created_tasks().len()
                                            == script.other_tasks.len() + 1
                                        && script
                                            .created_tasks()
                                            .into_iter()
                                            .all(|task| script.static_stopped(task)))
                                {
                                    // This original Abort supplies the fence. The same peer
                                    // already authored actual_stopped for EVERY declared Task.
                                    // No FE local exit or Release is claimed by this receipt.
                                    ParticipationState::TerminalRetained
                                } else {
                                    ParticipationState::Aborting
                                },
                            ),
                            script.abort_cause,
                        )
                        .expect("the original abort cause and closed state encode"),
                    ))
                }
                D::CancelTask(cancel) => {
                    if !script.declares_task(cancel.identity()) {
                        return Err(Status::failed_precondition(
                            "single-root cancel identity mismatch",
                        ));
                    }
                    // Preserve the declared Task's original terminal status
                    // when cancellation observes its completed script.
                    outcome = if (cancel.identity() == script.root
                        && script.created
                        && script.terminal.is_some())
                        || script.other_task_terminal(cancel.identity())
                    {
                        OperationOutcome::Idempotent
                    } else if script.other_task_created(cancel.identity()) {
                        let frames =
                            script.cancel_other_task(cancel.identity(), cancel.reason())?;
                        for frame in frames {
                            Self::queue_single_root_frame(held, frame)?;
                        }
                        OperationOutcome::Accepted
                    } else {
                        OperationOutcome::NotReady
                    };
                    None
                }
                _ => {
                    return Err(Status::failed_precondition(
                        "operation is outside the fixed single-root script",
                    ));
                }
            };
            if finish {
                for frame in script.completion_frames() {
                    Self::queue_single_root_frame(held, frame)?;
                }
            }
            if fence {
                let frame = script.frame(
                    CoveredStatusStreamFact::Quiesce(script.quiesce_receipt().clone()),
                    script.quiesce_revision,
                );
                Self::queue_single_root_frame(held, frame)?;
            }
            if release {
                let frame = script.frame(
                    CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                        script.participation.expect("release has an exact owner"),
                    )),
                    script.participation_revision,
                );
                Self::queue_single_root_frame(held, frame)?;
                // Real server EOF after the queued original strong fact. This
                // is not an FE SubscriptionExited or actual-exit input.
                held.clear();
            }
            receipts.push(proto::TaskOperationReceipt {
                operation_id: wire
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.operation_id.clone()),
                result: Some(proto::task_operation_receipt::Result::Verdict(
                    proto::TaskOperationVerdict {
                        outcome: encode_operation_outcome(outcome),
                        safe_detail: String::new(),
                        safe_field_path: None,
                        ack,
                    },
                )),
            });
        }
        Ok(Response::new(proto::ApplyTaskOperationsResponse {
            receipts,
        }))
    }

    fn queue_single_root_frame(
        held: &[tokio::sync::mpsc::Sender<Result<proto::TaskStatusStreamEvent, Status>>],
        frame: proto::TaskStatusStreamEvent,
    ) -> Result<(), Status> {
        // A fixed script has at most one open stream and at most eight declared frames.
        // Enqueue under the original PeerState gate so Create cannot overtake
        // its subscription's catch-up snapshot. No extra sender job exists.
        if let Some(sender) = held.last().filter(|sender| !sender.is_closed()) {
            sender
                .try_send(Ok(frame))
                .map_err(|_| Status::resource_exhausted("single-root stream did not drain"))?;
        }
        Ok(())
    }

    fn subscribe_single_root(
        &self,
        request: proto::SubscribeTaskStatusRequest,
        close_after_initial: bool,
    ) -> Result<Response<StatusStream>, Status> {
        let three_tasks = self
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .is_some_and(|script| !script.other_tasks.is_empty());
        if three_tasks {
            return self.subscribe_three_task_root(request, close_after_initial);
        }
        let decoded = decode_covered_subscribe_task_status(
            &request,
            FieldPath::root("single_root_subscription"),
        )
        .map_err(|_| Status::invalid_argument("single-root subscription did not decode"))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let mut state = self.state.lock().expect("peer state");
        state.subscribed.push(request);
        let PeerState {
            single_root, held, ..
        } = &mut *state;
        let script = single_root
            .as_mut()
            .expect("enabled fixed single-root script");
        script.bind_participation(decoded.participation)?;
        if decoded
            .required_identities
            .iter()
            .any(|task| *task != script.root)
            || decoded
                .status_cursors
                .iter()
                .any(|cursor| cursor.identity() != script.root)
            || decoded
                .task_convergence_cursors
                .iter()
                .any(|cursor| cursor.identity() != script.root)
            || held.iter().any(|sender| !sender.is_closed())
        {
            return Err(Status::failed_precondition(
                "single-root subscription has a foreign task or live predecessor",
            ));
        }
        let status_version = decoded
            .status_cursors
            .iter()
            .find_map(|cursor| cursor.current_version());
        let task_version = decoded
            .task_convergence_cursors
            .iter()
            .find_map(|cursor| cursor.current_version());
        let participation_version = decoded
            .participation_cursor
            .and_then(|cursor| cursor.current_version());
        if status_version
            .is_some_and(|version| !script.created || version > script.current_status().version())
            || task_version.is_some_and(|version| {
                script.task_revision.is_none() || version > TaskConvergenceVersion::FIRST
            })
            || participation_version.is_some_and(|version| {
                !script.released || version > ParticipationConvergenceVersion::FIRST
            })
            || decoded
                .quiesce_cursor
                .and_then(|cursor| cursor.fence_version)
                .is_some_and(|version| script.quiesce.is_none() || version.get() > 1)
        {
            return Err(Status::failed_precondition(
                "single-root subscription cursor is ahead of its retained fact",
            ));
        }
        held.clear();
        // Initial facts carry no live source revision. The exact original
        // catch-up cut below separates snapshot replay from later live frames.
        if script.created {
            let status = if status_version == Some(script.current_status().version()) {
                CoveredStatusStreamFact::StatusUnchanged(script.root)
            } else {
                CoveredStatusStreamFact::Status(script.current_status())
            };
            sender
                .try_send(Ok(script.frame(status, None)))
                .expect("fixed snapshot fits");
            if script.task_revision.is_some() {
                let task = if task_version == Some(TaskConvergenceVersion::FIRST) {
                    CoveredStatusStreamFact::TaskConvergenceUnchanged(script.root)
                } else {
                    CoveredStatusStreamFact::TaskConvergence(
                        TaskConvergenceReceipt::actual_stopped(
                            script.root,
                            TaskConvergenceVersion::FIRST,
                        ),
                    )
                };
                sender
                    .try_send(Ok(script.frame(task, None)))
                    .expect("fixed snapshot fits");
            }
        } else if !decoded.required_identities.is_empty() || !decoded.status_cursors.is_empty() {
            sender
                .try_send(Ok(
                    script.frame(CoveredStatusStreamFact::Unknown(script.root), None)
                ))
                .expect("fixed absence snapshot fits");
        }
        if script.quiesce.is_some() {
            sender
                .try_send(Ok(script.frame(
                    CoveredStatusStreamFact::Quiesce(script.quiesce_receipt().clone()),
                    None,
                )))
                .expect("fixed fence snapshot fits");
        }
        if script.released && participation_version != Some(ParticipationConvergenceVersion::FIRST)
        {
            sender
                .try_send(Ok(script.frame(
                    CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                        decoded.participation,
                    )),
                    None,
                )))
                .expect("fixed release snapshot fits");
        }
        sender
            .try_send(Ok(script.frame(
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: decoded.generation.get(),
                    initial_cut: script.revision,
                }),
                None,
            )))
            .expect("fixed catch-up cut fits the original channel");
        if !script.released && !close_after_initial {
            held.push(sender);
        }
        // In the selected first-response mode, dropping this actual sender
        // yields real server EOF after the queued typed catch-up frames.
        drop(state);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    fn create_ack(task: TaskIdentity, status: TaskStatus) -> proto::task_operation_verdict::Ack {
        proto::task_operation_verdict::Ack::CreateTask(
            codec::encode_create_task_ack(&CreateTaskReceipt::new(task, Vec::new(), status))
                .expect("the exact Create acknowledgement encodes"),
        )
    }

    fn rejected(rpc: &str) -> Status {
        Status::failed_precondition(format!("task wire test peer rejects {rpc}"))
    }

    fn expect_apply(&self, answer: ApplyAnswer) {
        self.state
            .lock()
            .expect("peer state")
            .apply_answers
            .push_back(answer);
    }

    fn expect_subscribe(&self, answer: SubscribeAnswer) {
        self.state
            .lock()
            .expect("peer state")
            .subscribe_answers
            .push_back(answer);
    }

    fn applied(&self) -> Vec<proto::ApplyTaskOperationsRequest> {
        self.state.lock().expect("peer state").applied.clone()
    }

    fn control_requests(&self) -> usize {
        self.state.lock().expect("peer state").control_requests
    }

    fn subscribed(&self) -> Vec<proto::SubscribeTaskStatusRequest> {
        self.state.lock().expect("peer state").subscribed.clone()
    }

    fn open_held_subscriptions(&self) -> usize {
        self.state
            .lock()
            .expect("peer state")
            .held
            .iter()
            .filter(|sender| !sender.is_closed())
            .count()
    }

    fn close_held_subscriptions(&self) {
        self.state.lock().expect("peer state").held.clear();
    }

    async fn send_held_event(&self, event: proto::TaskStatusStreamEvent) {
        let sender = self
            .state
            .lock()
            .expect("peer state")
            .held
            .last()
            .expect("one held subscription")
            .clone();
        sender.send(Ok(event)).await.expect("held stream is open");
    }
}

type StatusStream = ReceiverStream<Result<proto::TaskStatusStreamEvent, Status>>;

#[tonic::async_trait]
impl NovaRocksGrpc for TaskWirePeer {
    type ExchangeStream = tokio_stream::Empty<Result<proto::ExchangeResponse, tonic::Status>>;

    async fn exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange is retired; use DeliverExchangeUnit",
        ))
    }

    type SubscribeExchangeGrantsStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeGrantBatch, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn deliver_exchange_unit(
        &self,
        _request: tonic::Request<proto::DeliverExchangeUnitRequest>,
    ) -> Result<tonic::Response<proto::DeliverExchangeUnitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange unit delivery is not installed",
        ))
    }

    async fn register_exchange_demand(
        &self,
        _request: tonic::Request<proto::RegisterExchangeDemandRequest>,
    ) -> Result<tonic::Response<proto::RegisterExchangeDemandResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange demand registration is not installed",
        ))
    }

    async fn subscribe_exchange_grants(
        &self,
        _request: tonic::Request<proto::SubscribeExchangeGrantsRequest>,
    ) -> Result<tonic::Response<Self::SubscribeExchangeGrantsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange grant subscription is not installed",
        ))
    }

    type SubscribeTaskStatusStream = StatusStream;

    async fn apply_task_operations(
        &self,
        request: Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        if !NativeRpcMethod::ApplyTaskOperations.is_allowed_at(self.domain) {
            return Err(Self::rejected("wrong endpoint domain"));
        }
        self.apply_operations(request).await
    }

    async fn apply_task_control_operations(
        &self,
        request: Request<proto::ApplyTaskControlOperationsRequest>,
    ) -> Result<Response<proto::ApplyTaskOperationsResponse>, Status> {
        if !NativeRpcMethod::ApplyTaskControlOperations.is_allowed_at(self.domain) {
            return Err(Self::rejected("wrong endpoint domain"));
        }
        self.state.lock().expect("peer state").control_requests += 1;
        let operations = request
            .into_inner()
            .operations
            .into_iter()
            .map(|operation| {
                let body = match operation.control.expect("control body") {
                    proto::task_control_operation::Control::CloseExchangeDestinations(close) => {
                        proto::task_operation::Operation::UpdateTask(proto::UpdateTaskRequest {
                            identity: close.identity,
                            domains: close
                                .destinations
                                .into_iter()
                                .map(|destination| proto::TaskDomainUpdate {
                                    domain: Some(
                                        proto::task_domain_update::Domain::CloseExchangeDestination(
                                            destination,
                                        ),
                                    ),
                                })
                                .collect(),
                        })
                    }
                    proto::task_control_operation::Control::RenewLease(renew) => {
                        proto::task_operation::Operation::UpdateParticipation(
                            proto::UpdateParticipationRequest {
                                command: Some(
                                    proto::update_participation_request::Command::RenewLease(renew),
                                ),
                            },
                        )
                    }
                    proto::task_control_operation::Control::CancelTask(cancel) => {
                        proto::task_operation::Operation::CancelTask(cancel)
                    }
                    proto::task_control_operation::Control::AbortParticipation(abort) => {
                        proto::task_operation::Operation::AbortParticipation(abort)
                    }
                    proto::task_control_operation::Control::QuiesceParticipation(quiesce) => {
                        proto::task_operation::Operation::QuiesceParticipation(quiesce)
                    }
                    proto::task_control_operation::Control::ReleaseParticipation(release) => {
                        proto::task_operation::Operation::ReleaseParticipation(release)
                    }
                };
                proto::TaskOperation {
                    envelope: operation.envelope,
                    operation: Some(body),
                }
            })
            .collect();
        self.apply_operations(Request::new(proto::ApplyTaskOperationsRequest {
            operations,
        }))
        .await
    }

    async fn subscribe_task_status(
        &self,
        request: Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<Response<Self::SubscribeTaskStatusStream>, Status> {
        if !NativeRpcMethod::SubscribeTaskStatus.is_allowed_at(self.domain) {
            return Err(Self::rejected("wrong endpoint domain"));
        }
        if self
            .state
            .lock()
            .expect("peer state")
            .participation_only
            .is_some()
        {
            return self.subscribe_participation_only(request.into_inner());
        }
        let (scripted, first_response) = {
            let mut state = self.state.lock().expect("peer state");
            let script = state.single_root.as_mut();
            let configured = script.is_some();
            let first = script
                .and_then(|script| script.io_gates.as_mut())
                .filter(|gates| !gates.first_subscription_selected)
                .map(|gates| {
                    gates.first_subscription_selected = true;
                    Arc::clone(&gates.first_subscription)
                });
            (configured, first)
        };
        let first_eof = first_response.is_some();
        if let Some(gate) = first_response {
            // Actual request arrival is observable, but response headers and
            // the original catch-up stream are held until explicit release.
            gate.wait().await;
        }
        if scripted {
            return self.subscribe_single_root(request.into_inner(), first_eof);
        }
        let answer = {
            let mut state = self.state.lock().expect("peer state");
            state.subscribed.push(request.into_inner());
            state
                .subscribe_answers
                .pop_front()
                .unwrap_or(SubscribeAnswer::EventsThenHold(Vec::new()))
        };
        let (events, hold) = match answer {
            SubscribeAnswer::StallOpening => return std::future::pending().await,
            SubscribeAnswer::EventsThenClose(events) => (events, false),
            SubscribeAnswer::EventsThenHold(events) => (events, true),
            SubscribeAnswer::Reject(code) => {
                return Err(Status::new(code, "task wire test peer rejection"));
            }
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        for event in events {
            sender.send(Ok(event)).await.expect("test stream capacity");
        }
        if hold {
            // Retaining the sender keeps the stream open, which is how a
            // healthy subscription looks between events.
            self.state.lock().expect("peer state").held.push(sender);
        }
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        _request: Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<Response<proto::FetchTaskDynamicFiltersResponse>, Status> {
        Err(Self::rejected("FetchTaskDynamicFilters"))
    }

    async fn get_final_task_info(
        &self,
        request: Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<Response<proto::GetFinalTaskInfoResponse>, Status> {
        let mut state = self.state.lock().expect("peer state");
        if let Some(original) = state.participation_only.as_mut() {
            return original.final_info(request.into_inner());
        }
        let Some(script) = state
            .single_root
            .as_mut()
            .filter(|s| !s.final_info.is_empty())
        else {
            // Existing peers remain explicitly unsupported unless the caller
            // supplied bounded original diagnostic payloads before Create.
            return Err(Self::rejected("GetFinalTaskInfo"));
        };
        let request = novarocks_task_codec::operation::decode_get_final_task_info(
            &request.into_inner(),
            TaskOperationId::new_v7(),
            FieldPath::root("get_final_task_info"),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let task = request.identity();
        // This is the SAME status already authored by this peer's original
        // Create/covered subscription. Root End never manufactures Finished.
        let status = script.static_status(task).ok_or_else(|| {
            Status::failed_precondition("final info requires the original created Task")
        })?;
        let original = script.final_info.get_mut(&task).ok_or_else(|| {
            Status::failed_precondition("final info payload is not declared for this Task")
        })?;
        if original.requested {
            return Err(Status::failed_precondition(
                "the original final-info read was already consumed",
            ));
        }
        let info = novarocks_execution_contract::FinalTaskInfo::try_new(
            task,
            status,
            original.operators.clone(),
            original.truncated,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        original.requested = true;
        Ok(Response::new(proto::GetFinalTaskInfoResponse {
            result: Some(proto::get_final_task_info_response::Result::Info(
                novarocks_task_codec::status::encode_final_task_info(&info),
            )),
        }))
    }

    async fn fetch_task_result(
        &self,
        _request: Request<proto::FetchTaskResultRequest>,
    ) -> Result<Response<proto::FetchResultResponse>, Status> {
        Err(Self::rejected("FetchTaskResult"))
    }

    async fn fetch_root_result(
        &self,
        request: Request<proto::FetchRootResultRequest>,
    ) -> Result<Response<proto::FetchRootResultResponse>, Status> {
        use novarocks_execution_contract::root_result::{
            RootReadDeferral, RootReadDeferred, RootReadOutcome, RootReadResponse, RootResultEnd,
            RootResultReply,
        };
        // Unconfigured generic peers retain their original method rejection,
        // including malformed requests. Only the explicit script parses V1.
        let scripted = { self.state.lock().expect("peer state").single_root.is_some() };
        if !scripted {
            return Err(Self::rejected("FetchRootResult"));
        }
        if !NativeRpcMethod::FetchRootResult.is_allowed_at(self.domain) {
            return Err(Self::rejected("wrong root endpoint domain"));
        }
        let original_root_socket = request.remote_addr();
        let wire = request.into_inner();
        if prost::Message::encoded_len(&wire)
            > novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES
        {
            return Err(Status::resource_exhausted(
                "fixed root request exceeds its V1 envelope",
            ));
        }
        let read = novarocks_task_codec::root_result::decode_read(
            &wire,
            FieldPath::root("single_root_read"),
        )
        .map_err(|_| Status::invalid_argument("single-root read did not decode"))?;
        let result_gate = {
            let state = self.state.lock().expect("peer state");
            let script = state
                .single_root
                .as_ref()
                .ok_or_else(|| Self::rejected("root is not installed"))?;
            if read.root_task() != script.root
                || read.kind() != script.kind
                || read.profile() != novarocks_result_contract::RootProfileId::V1
            {
                return Err(Status::failed_precondition(
                    "root read differs from its explicit fixture declaration",
                ));
            }
            if read.consumed() > script.root_offered_through {
                return Err(Status::invalid_argument(
                    "root ACK exceeds the actual offered sequence",
                ));
            }
            match read.wanted().map(|value| value.get()) {
                Some(1) if script.created && !script.released && script.root_consumed < 1 => {
                    script.root_data_gate.clone()
                }
                Some(2) if script.created && !script.released && script.root_consumed < 2 => {
                    script.root_end_gate.clone()
                }
                _ => None,
            }
        };
        if let Some(gate) = result_gate {
            {
                let mut state = self.state.lock().expect("peer state");
                if state.original_root_socket.is_none() {
                    state.original_root_socket = original_root_socket;
                }
            }
            // Hold this SAME real RPC future, outside the peer state lock.
            // No DATA/End/ACK fact is offered by releasing the test gate.
            gate.wait().await;
        }
        let response = {
            let mut state = self.state.lock().expect("peer state");
            let script = state
                .single_root
                .as_mut()
                .ok_or_else(|| Self::rejected("root is not installed"))?;
            if read.root_task() != script.root
                || read.kind() != script.kind
                || read.profile() != novarocks_result_contract::RootProfileId::V1
            {
                return Err(Status::failed_precondition(
                    "root read differs from its explicit fixture declaration",
                ));
            }
            if !script.created {
                RootReadResponse::Deferred(RootReadDeferred::for_request(
                    &read,
                    RootReadDeferral::Preparing,
                ))
            } else {
                if read.consumed() > script.root_offered_through {
                    return Err(Status::invalid_argument(
                        "root ACK exceeds the actual offered sequence",
                    ));
                }
                if !script.released {
                    script.root_consumed = script.root_consumed.max(read.consumed());
                }
                let outcome = if script.released {
                    RootReadOutcome::AwaitTerminalControl
                } else if read.wanted().is_none() {
                    RootReadOutcome::AckOnly
                } else if read.wanted().unwrap().get() <= script.root_consumed {
                    RootReadOutcome::Retired
                } else {
                    let wanted = read.wanted().unwrap().get();
                    match (&script.result, wanted) {
                        (SingleRootResult::EndOnly, 1) => {
                            script.root_offered_through = 1;
                            script.root_end_offered = true;
                            RootReadOutcome::End(RootResultEnd {
                                sequence: std::num::NonZeroU64::MIN,
                                output_rows: 0,
                            })
                        }
                        (SingleRootResult::EncodedData { data, .. }, 1) => {
                            script.root_offered_through = script.root_offered_through.max(1);
                            RootReadOutcome::Data(data.clone())
                        }
                        (SingleRootResult::EncodedData { output_rows, .. }, 2)
                            if script.root_offered_through >= 1 =>
                        {
                            script.root_offered_through = 2;
                            script.root_end_offered = true;
                            RootReadOutcome::End(RootResultEnd {
                                sequence: std::num::NonZeroU64::new(2).unwrap(),
                                output_rows: *output_rows,
                            })
                        }
                        (SingleRootResult::EndOnly, _) => {
                            return Err(Status::invalid_argument(
                                "fixed empty root has no later sequence",
                            ));
                        }
                        (SingleRootResult::EncodedData { .. }, _) => {
                            return Err(Status::invalid_argument(
                                "fixed encoded root read skips or exceeds its declared sequence",
                            ));
                        }
                    }
                };
                RootReadResponse::Reply(RootResultReply {
                    root_task: script.root,
                    profile: read.profile(),
                    kind: script.kind,
                    accepted_consumed: script.root_consumed,
                    outcome,
                })
            }
        };
        let encoded = novarocks_task_codec::root_result::encode_response(&response)
            .map_err(|_| Status::internal("the fixed root reply did not encode"))?;
        Ok(Response::new(encoded))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        _request: Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<Response<filter::RuntimeFilterEnvelopeResponse>, Status> {
        Err(Self::rejected("TransmitRuntimeFilterEnvelope"))
    }

    async fn fetch_result(
        &self,
        _request: Request<proto::FetchResultRequest>,
    ) -> Result<Response<proto::FetchResultResponse>, Status> {
        Err(Self::rejected("FetchResult"))
    }

    async fn prune_catalogs(
        &self,
        _request: Request<catalog::PruneCatalogsRequest>,
    ) -> Result<Response<catalog::PruneCatalogsResponse>, Status> {
        Err(Self::rejected("PruneCatalogs"))
    }

    async fn announce_backend(
        &self,
        _request: Request<proto::AnnounceBackendRequest>,
    ) -> Result<Response<proto::AnnounceBackendResponse>, Status> {
        Err(Self::rejected("AnnounceBackend"))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        if !NativeRpcMethod::Heartbeat.is_allowed_at(self.domain) {
            return Err(Self::rejected("wrong endpoint domain"));
        }
        let descriptor = {
            let mut state = self.state.lock().expect("peer state");
            state.heartbeats.push(request.into_inner());
            state
                .heartbeat_descriptor
                .clone()
                .ok_or_else(|| Self::rejected("missing heartbeat fixture"))?
        };
        Ok(Response::new(proto::HeartbeatResponse {
            num_cores: 2,
            descriptor: Some(
                novarocks_proto_codec::membership::BackendProcessDescriptor::from_contract(
                    descriptor,
                )
                .as_proto()
                .clone(),
            ),
            reported_state: proto::BackendReportedState::Running as i32,
            admission_epoch_capability: Some(
                novarocks_task_codec::identity::encode_admission_epoch_capability(
                    novarocks_execution_contract::AdmissionEpochCapability::try_from_bytes(
                        [0x61; 16],
                    )
                    .unwrap(),
                ),
            ),
        }))
    }
}

#[tokio::test]
async fn generic_peer_rejects_root_before_parsing_an_unconfigured_request() {
    let data = TaskWirePeer::default();
    let mut control = data.clone();
    control.domain = NativeEndpointDomain::BackendControl;
    let data_error = data
        .fetch_root_result(Request::new(proto::FetchRootResultRequest::default()))
        .await
        .unwrap_err();
    let control_error = control
        .fetch_root_result(Request::new(proto::FetchRootResultRequest::default()))
        .await
        .unwrap_err();
    let idle = {
        let state = data.state.lock().expect("peer state");
        state.single_root.is_none()
            && state.applied.is_empty()
            && state.subscribed.is_empty()
            && state.held.is_empty()
    };
    // No server or stream was started; release the original peer objects
    // before checking the saved protocol refusal facts.
    drop((data, control));
    assert!(idle);
    for error in [data_error, control_error] {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            error.message(),
            "task wire test peer rejects FetchRootResult"
        );
    }
}

/// Two independent sockets for one shared process fixture.
pub(crate) struct Loopback {
    root_connections: Option<original_root_connection::OriginalRootConnections>,
    peer: TaskWirePeer,
    endpoint: RuntimeEndpoint,
    pub(in crate::native) control_endpoint: RuntimeEndpoint,
    shutdown: [Option<tokio::sync::oneshot::Sender<()>>; 2],
    served: [tokio::task::JoinHandle<()>; 2],
}

impl Loopback {
    pub(crate) fn expect_covered_opening_blackhole(&self) {
        self.peer.expect_subscribe(SubscribeAnswer::StallOpening);
    }
}
impl Loopback {
    async fn serve(
        peer: TaskWirePeer,
    ) -> (
        RuntimeEndpoint,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the loopback task peer");
        Self::serve_listener(peer, listener).await
    }

    async fn serve_listener(
        peer: TaskWirePeer,
        listener: tokio::net::TcpListener,
    ) -> (
        RuntimeEndpoint,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let address = listener.local_addr().expect("loopback address");
        let incoming = futures::stream::unfold(listener, |listener| async {
            let item = listener.accept().await.map(|(stream, _)| stream);
            Some((item, listener))
        });
        let (shutdown, stop) = tokio::sync::oneshot::channel();
        let served = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    NovaRocksGrpcServer::new(peer)
                        .max_decoding_message_size(128 * 1024 * 1024)
                        .max_encoding_message_size(128 * 1024 * 1024),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stop.await;
                })
                .await
                .expect("serve the loopback task peer");
        });
        (
            RuntimeEndpoint::new("127.0.0.1", i32::from(address.port()))
                .expect("valid loopback endpoint"),
            shutdown,
            served,
        )
    }

    pub(crate) async fn start() -> Self {
        let peer = TaskWirePeer::default();
        let (endpoint, data_shutdown, data_served) = Self::serve(peer.clone()).await;
        let mut control_peer = peer.clone();
        control_peer.domain = NativeEndpointDomain::BackendControl;
        let (control_endpoint, control_shutdown, control_served) = Self::serve(control_peer).await;
        assert_ne!(endpoint, control_endpoint);
        Self {
            root_connections: None,
            peer,
            endpoint,
            control_endpoint,
            shutdown: [Some(data_shutdown), Some(control_shutdown)],
            served: [data_served, control_served],
        }
    }

    /// Same first-party codec/service, with physical access to this peer's
    /// accepted BackendData sockets, where FetchRootResult actually runs.
    /// BackendControl stays separate; default start() stays unchanged.
    pub(crate) async fn start_with_original_root_connection_control() -> Self {
        let peer = TaskWirePeer::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (endpoint, data_shutdown, data_served) =
            original_root_connection::serve(peer.clone(), listener, connections.clone()).await;
        let mut control_peer = peer.clone();
        control_peer.domain = NativeEndpointDomain::BackendControl;
        let (control_endpoint, control_shutdown, control_served) = Self::serve(control_peer).await;
        assert_ne!(endpoint, control_endpoint);
        Self {
            root_connections: Some(connections),
            peer,
            endpoint,
            control_endpoint,
            shutdown: [Some(data_shutdown), Some(control_shutdown)],
            served: [data_served, control_served],
        }
    }

    /// Configure once, after the original manifest supplies its exact root.
    pub(crate) fn enable_single_root_lifecycle(
        &self,
        root: TaskIdentity,
        kind: novarocks_result_contract::RootOutputKind,
    ) {
        self.enable_single_root_result(root, kind, SingleRootResult::EndOnly);
    }

    /// Configure one original canonical DATA segment followed by End. The
    /// caller supplies its frozen kind and exact Root output_rows; this peer
    /// neither constructs a domain artifact nor interprets those bytes.
    /// Invalid/oversized DATA refuses before mutating the original peer.
    pub(crate) fn enable_single_root_encoded_result(
        &self,
        root: TaskIdentity,
        kind: novarocks_result_contract::RootOutputKind,
        canonical_body: bytes::Bytes,
        output_rows: u64,
    ) -> Result<(), novarocks_execution_contract::root_result::RootResultError> {
        let data = novarocks_execution_contract::root_result::RootResultData::try_new(
            kind,
            std::num::NonZeroU64::MIN,
            canonical_body,
            None,
        )?;
        self.enable_single_root_result(
            root,
            kind,
            SingleRootResult::EncodedData { data, output_rows },
        );
        Ok(())
    }

    fn enable_single_root_result(
        &self,
        root: TaskIdentity,
        kind: novarocks_result_contract::RootOutputKind,
        result: SingleRootResult,
    ) {
        let mut state = self.peer.state.lock().expect("peer state");
        assert!(
            state.single_root.is_none()
                && state.participation_only.is_none()
                && state.applied.is_empty()
                && state.subscribed.is_empty()
                && state.apply_answers.is_empty()
                && state.subscribe_answers.is_empty()
                && state.held.is_empty(),
            "configure the fixed peer before the original runtime starts"
        );
        state.single_root = Some(SingleRootLifecycle {
            exchange_updates: std::collections::BTreeMap::new(),
            other_tasks: std::collections::BTreeMap::new(),
            final_info: std::collections::BTreeMap::new(),
            root_data_gate: None,
            root_end_gate: None,
            io_gates: None,
            root,
            kind,
            result,
            terminal: Some(SingleRootTerminal::Finished),
            failing_revision: None,
            participation: None,
            ticket_issued: false,
            revision: 0,
            running_revision: None,
            status_revision: None,
            task_revision: None,
            quiesce_revision: None,
            participation_revision: None,
            root_end_offered: false,
            root_offered_through: 0,
            root_consumed: 0,
            established: false,
            created: false,
            quiesce: None,
            released: false,
            abort_cause: None,
            close_finished_tasks_on_abort: false,
        });
    }

    /// Supply only the bounded diagnostic payload. The terminal status comes
    /// from this SAME peer's actual original Task lifecycle when the RPC runs.
    /// Configuration refuses before moving a payload for a foreign/started Task.
    pub(crate) fn enable_original_final_task_info(
        &self,
        task: TaskIdentity,
        operators: Vec<novarocks_execution_contract::OperatorStatistics>,
        truncated: bool,
    ) -> Result<(), Status> {
        if operators.len() > novarocks_execution_contract::status::FINAL_TASK_INFO_MAX_OPERATORS {
            return Err(Status::resource_exhausted(
                "original final-info payload exceeds its bound",
            ));
        }
        let mut state = self.peer.state.lock().expect("peer state");
        if let Some(original) = state.participation_only.as_mut() {
            return original.declare_final_info(task, operators, truncated);
        }
        let script = state.single_root.as_mut().ok_or_else(|| {
            Status::failed_precondition("declare the original root before its diagnostic payload")
        })?;
        if !script.declares_task(task)
            || script.static_status(task).is_some()
            || script.final_info.contains_key(&task)
        {
            return Err(Status::failed_precondition(
                "original final-info payload is foreign or already fixed",
            ));
        }
        script.final_info.insert(
            task,
            SingleRootFinalInfo {
                operators,
                truncated,
                requested: false,
            },
        );
        Ok(())
    }

    /// Actual successful codec/RPC reads, without exposing any Source or owner.
    pub(crate) fn original_final_task_info_reads(&self) -> Vec<TaskIdentity> {
        let state = self.peer.state.lock().expect("peer state");
        if let Some(original) = state.participation_only.as_ref() {
            return original.final_info_reads();
        }
        state.single_root.as_ref().map_or_else(Vec::new, |script| {
            script
                .final_info
                .iter()
                .filter_map(|(task, info)| info.requested.then_some(*task))
                .collect()
        })
    }

    /// Positive Worker Abort closure only at the actual RPC, after every
    /// original roster member has its positive actual_stopped fact. Freezing
    /// this option emits no receipt, Release, process fact or FE local exit.
    pub(crate) fn close_original_stopped_roster_on_abort(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("declare original Root first"))?;
        if script.created || script.close_finished_tasks_on_abort {
            return Err(Status::failed_precondition(
                "original Abort closure mode freezes before Create",
            ));
        }
        script.close_finished_tasks_on_abort = true;
        Ok(())
    }

    /// Configure before Create. This withholds only terminal publications,
    /// not any RPC, Root DATA/End, AP turn or subscription's running facts.
    pub(crate) fn defer_single_root_terminal(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state
            .single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if script.created {
            return Err(Status::failed_precondition(
                "defer precedes original Create",
            ));
        }
        script.terminal = None;
        Ok(())
    }
    /// Original typed status frames travel through the same covered RPC stream.
    /// Failure has the legal Running2 -> Failing3 -> Failed4 prefix.
    pub(crate) fn publish_single_root_terminal(
        &self,
        original: Option<novarocks_execution_contract::TaskFailure>,
    ) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let PeerState {
            single_root, held, ..
        } = &mut *state;
        let script = single_root
            .as_mut()
            .ok_or_else(|| Status::failed_precondition("root is not declared"))?;
        if !script.created
            || script.terminal.is_some()
            || script.quiesce.is_some()
            || script.released
            || script.abort_cause.is_some()
        {
            return Err(Status::failed_precondition(
                "original deferred terminal is not live",
            ));
        }
        script.terminal = Some(match original {
            Some(original) => SingleRootTerminal::Failed(Box::new(original)),
            None => SingleRootTerminal::Finished,
        });
        script.assign_terminal_revisions();
        for frame in script.terminal_frames() {
            TaskWirePeer::queue_single_root_frame(held, frame)?;
        }
        Ok(())
    }

    /// Hold this SAME first Root RPC before offering any DATA or End. Used
    /// for real connection-loss observation, independently of Task terminal.
    pub(crate) fn defer_single_root_first_reply(&self) -> Result<(), Status> {
        let mut state = self.peer.state.lock().expect("peer state");
        let script = state.single_root.as_mut().ok_or_else(|| {
            Status::failed_precondition("declare original Root before its RPC gate")
        })?;
        if script.created || script.root_data_gate.is_some() {
            return Err(Status::failed_precondition(
                "first Root reply gate precedes Create",
            ));
        }
        script.root_data_gate = Some(Arc::new(SingleRootIoGate::new()));
        Ok(())
    }
    pub(crate) fn single_root_first_reply_waiting(&self) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.root_data_gate.as_ref())
            .is_some_and(|gate| gate.waiting())
    }

    /// Two explicit test I/O gates. Configure before the original runtime.
    pub(crate) fn enable_single_root_first_subscription_eof_before_create(
        &self,
        root: TaskIdentity,
        kind: novarocks_result_contract::RootOutputKind,
    ) {
        self.enable_single_root_lifecycle(root, kind);
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_mut()
            .expect("the exact single-root script was configured")
            .io_gates = Some(SingleRootIoGates {
            first_subscription_selected: false,
            first_subscription: Arc::new(SingleRootIoGate::new()),
            create: Arc::new(SingleRootIoGate::new()),
        });
    }

    pub(crate) fn single_root_first_subscription_waiting(&self) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .and_then(|script| script.io_gates.as_ref())
            .is_some_and(|gates| gates.first_subscription.waiting())
    }

    pub(crate) fn release_single_root_first_subscription(&self) {
        let gate = {
            let state = self.peer.state.lock().expect("peer state");
            Arc::clone(
                &state
                    .single_root
                    .as_ref()
                    .and_then(|script| script.io_gates.as_ref())
                    .expect("release only the configured fixed test gate")
                    .first_subscription,
            )
        };
        gate.release();
    }

    pub(crate) fn release_single_root_create(&self) {
        let gate = {
            let state = self.peer.state.lock().expect("peer state");
            Arc::clone(
                &state
                    .single_root
                    .as_ref()
                    .and_then(|script| script.io_gates.as_ref())
                    .expect("release only the configured fixed test gate")
                    .create,
            )
        };
        gate.release();
    }

    pub(crate) fn single_root_result_progress(&self) -> (bool, u64) {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .map_or((false, 0), |original| {
                (original.root_end_offered, original.root_consumed)
            })
    }

    pub(crate) fn single_root_release_observed(&self) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .is_some_and(|script| script.released)
    }

    pub(crate) fn single_root_abort_observed(&self) -> bool {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root
            .as_ref()
            .is_some_and(|script| script.abort_cause.is_some())
    }

    pub(crate) fn bounded_root_descriptor(
        &self,
        process: BackendProcessId,
    ) -> BackendProcessDescriptor {
        self.descriptor(process)
            .with_bounded_root_support(
                novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
                    self.control_endpoint.clone(),
                    novarocks_result_contract::RootProfileId::V1,
                ),
            )
            .expect("advertise V1 only at this peer's actual independent control socket")
    }

    pub(crate) fn descriptor(&self, process: BackendProcessId) -> BackendProcessDescriptor {
        test_descriptor(
            process,
            self.endpoint.clone(),
            self.control_endpoint.clone(),
        )
    }

    pub(crate) fn subscription_requests(&self) -> Vec<proto::SubscribeTaskStatusRequest> {
        self.peer.subscribed()
    }

    pub(crate) fn close_held_subscriptions(&self) {
        self.peer.close_held_subscriptions();
    }

    /// First-party test-peer scripting only: these are the same original
    /// covered response channel and captured physical request inventory.
    pub(crate) fn expect_covered_frames_then_hold(
        &self,
        frames: Vec<proto::TaskStatusStreamEvent>,
    ) {
        self.peer
            .expect_subscribe(SubscribeAnswer::EventsThenHold(frames));
    }
    pub(crate) fn expect_covered_rejection(&self, code: tonic::Code) {
        self.peer.expect_subscribe(SubscribeAnswer::Reject(code));
    }

    pub(crate) fn expect_covered_frames_then_close(
        &self,
        frames: Vec<proto::TaskStatusStreamEvent>,
    ) {
        self.peer
            .expect_subscribe(SubscribeAnswer::EventsThenClose(frames));
    }

    pub(crate) fn open_covered_subscriptions(&self) -> usize {
        self.peer.open_held_subscriptions()
    }
    pub(crate) async fn publish_held_covered_event(&self, event: proto::TaskStatusStreamEvent) {
        self.peer.send_held_event(event).await;
    }

    pub(crate) fn applied_requests(&self) -> Vec<proto::ApplyTaskOperationsRequest> {
        self.peer.applied()
    }
    pub(crate) fn single_root_abort_refusals(&self) -> Vec<(TaskOperationId, tonic::Code)> {
        self.peer
            .state
            .lock()
            .expect("peer state")
            .single_root_abort_refusals
            .clone()
    }

    pub(crate) fn expect_transport_unknown(&self) {
        self.peer
            .expect_apply(ApplyAnswer::Reject(tonic::Code::Unavailable));
    }

    pub(crate) fn expect_admission_ticket(
        &self,
        outcome: OperationOutcome,
        receipt: novarocks_execution_contract::ParticipationAdmissionTicketReceipt,
    ) {
        self.peer.expect_apply(ApplyAnswer::WithAck(vec![(
            outcome,
            Some(
                proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(
                    codec::encode_participation_admission_ticket_ack(receipt),
                ),
            ),
        )]));
    }

    pub(crate) fn control_requests(&self) -> usize {
        self.peer.control_requests()
    }

    pub(in crate::native) fn accept_create(&self, task: TaskIdentity) {
        self.peer.expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(TaskWirePeer::create_ack(task, TaskStatus::created(task))),
        )]));
    }

    pub(in crate::native) fn accept_abort(&self, participation: ParticipationRef) {
        self.peer.expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(proto::task_operation_verdict::Ack::Participation(
                encode_participation_ack(
                    &ParticipationReceipt::new(participation, ParticipationState::Aborting),
                    Some(AbortCause::QueryFailed),
                )
                .unwrap(),
            )),
        )]));
    }

    pub(in crate::native) fn fail_abort_owner(&self) {
        self.peer
            .expect_apply(ApplyAnswer::Outcomes(vec![OperationOutcome::OwnerFailed]));
    }

    /// Shut down only the accepted TCP socket of the SAME held Root RPC.
    /// Actual Tonic ConnectedInfo identifies it. Other Control connections and
    /// this original server remain live; settle() consumes its SAME final Join.
    pub(crate) fn disconnect_original_root_connection(&self) {
        let remote = self
            .peer
            .state
            .lock()
            .expect("peer state")
            .original_root_socket
            .expect("the original held Root RPC recorded its actual connection");
        let live = self
            .root_connections
            .as_ref()
            .expect("controlled original Root peer")
            .lock()
            .unwrap()
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .filter(|connection| connection.remote == remote)
            .collect::<Vec<_>>();
        assert_eq!(live.len(), 1, "one SAME actual accepted Root socket");
        assert!(
            live[0]
                .shutdown_original()
                .expect("shut down that actual original socket once")
        );
        // No typed failure/Ready/exit is emitted. Native independently observes IO.
    }

    pub(crate) async fn settle(&mut self) -> bool {
        for shutdown in &mut self.shutdown {
            if let Some(shutdown) = shutdown.take() {
                let _ = shutdown.send(());
            }
        }
        let mut joined = true;
        for original in &mut self.served {
            original.abort();
            let result = original.await;
            joined &= result.is_ok() || result.is_err_and(|error| error.is_cancelled());
        }
        joined
    }
}

fn test_descriptor(
    process: BackendProcessId,
    data: RuntimeEndpoint,
    control: RuntimeEndpoint,
) -> BackendProcessDescriptor {
    BackendProcessDescriptor::try_new(
        process,
        data,
        control,
        "task-wire-test",
        "task-wire-test",
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        4096,
    )
    .expect("independent fixture endpoints")
}

impl Drop for Loopback {
    fn drop(&mut self) {
        for shutdown in &mut self.shutdown {
            if let Some(shutdown) = shutdown.take() {
                let _ = shutdown.send(());
            }
        }
        for served in &self.served {
            served.abort();
        }
    }
}

struct SinkFixture {
    loopback: Loopback,
    sink: NativeTaskOperationSink,
    acks: TaskAckIntake,
    // Only this original retains joins; the sink receives a projection.
    runtime: FrontendDataRuntime,
    governed: governed_wire::GovernedWireRole,
}

mod governed_wire;
mod message_limits;
mod root_frontier;

fn sink_fixture(loopback: Loopback, backend: BackendProcessId) -> SinkFixture {
    sink_fixture_with_budget(loopback, backend, TransportBudget::DEFAULT)
}

fn sink_fixture_with_budget(
    loopback: Loopback,
    backend: BackendProcessId,
    budget: TransportBudget,
) -> SinkFixture {
    let runtime =
        FrontendDataRuntime::new_with_task_budget(tokio::runtime::Handle::current(), budget);
    let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let governed = governed_wire::GovernedWireRole::new(&loopback);
    let sink = NativeTaskOperationSink::new(
        &[loopback.descriptor(backend)],
        budget,
        test_attempt_facts(),
        acks.handle(),
        runtime.clone(),
        governed.authority(),
    )
    .expect("one frozen backend target");
    SinkFixture {
        loopback,
        sink,
        acks,
        runtime,
        governed,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_one_input_capability_rejects_the_original_before_any_rpc() {
    use crate::task_execution::intent::TaskOperationQueuePermit;
    use novarocks_workload_control::WorkError;

    #[derive(Debug)]
    struct HiddenInput(Box<dyn TaskOperationQueuePermit>);
    impl TaskOperationQueuePermit for HiddenInput {
        fn mark_in_flight(&mut self) {
            self.0.mark_in_flight()
        }
        // Deliberately no typed capability. The actual paid original
        // remains owned by this wrapper through physical retirement.
    }

    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intents = (1..=3)
        .map(|id| cancel_intent(id, backend))
        .collect::<Vec<_>>();
    assert!(intents.len() > governed_wire::FIXTURE_CHANGES_PER_REQUEST);
    let permits = intents
        .iter()
        .enumerate()
        .map(|(index, intent)| {
            let original = fixture.governed.reserve(&fixture.sink, intent);
            if index == 1 {
                Box::new(HiddenInput(original)) as Box<dyn TaskOperationQueuePermit>
            } else {
                original
            }
        })
        .collect();
    let mut batch = released_batch(backend, intents);
    batch.replace_fixture_permits(permits);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    tokio::time::timeout_at(deadline, async {
        loop {
            batch = match fixture.sink.try_submit(batch) {
                TaskOperationSubmit::Accepted => break,
                TaskOperationSubmit::Backpressured(original) => original,
                other => panic!("original source refused before the actual gate: {other:?}"),
            };
            fixture.governed.drive();
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the same finite original source must reach its dispatch gate");

    let received = tokio::time::timeout_at(deadline, async {
        loop {
            if fixture.acks.queued() == 3 {
                break fixture.acks.drain();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all original operations retain their local typed refusal");
    let clone = received[0].clone();
    for ack in &received {
        assert_eq!(
            ack.dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
        assert_eq!(ack.worker_outcome(), None);
        assert_eq!(ack.governance_failure(), Some(&WorkError::Conflict));
        assert_eq!(
            crate::task_execution::error::dispatch_rejection(ack, ack.kind()),
            crate::task_execution::error::TaskExecutionError::Workload(WorkError::Conflict)
        );
    }
    assert!(std::ptr::eq(
        received[0].governance_failure().unwrap(),
        clone.governance_failure().unwrap()
    ));
    assert!(received[0].shares_original_canonical_with(&clone));
    drop(received);
    tokio::time::timeout_at(deadline, async {
        loop {
            if fixture
                .sink
                .data_runtime
                .task_transport_supervisor()
                .snapshot()
                .retained_items
                == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actual original F and physical input cleanup must exit");
    assert_eq!(clone.governance_failure(), Some(&WorkError::Conflict));
    assert!(fixture.loopback.peer.applied().is_empty());
    assert_eq!(fixture.loopback.peer.control_requests(), 0);
    drop(clone);

    let SinkFixture {
        mut loopback,
        sink,
        acks,
        mut runtime,
        governed,
    } = fixture;
    drop((sink, acks));
    governed.drive();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    governed.drive();
    let apply = runtime.drain_original_apply_until(deadline).await;
    governed.drive();
    let outgoing = runtime.drain_native_outgoing_until(deadline).await;
    let peer_exit = loopback.settle().await;
    drop((runtime, loopback));
    let shutdown = governed.finish();
    if apply.is_err() || outgoing.is_err() || !peer_exit || shutdown.is_err() {
        std::panic::panic_any((apply, outgoing, peer_exit, shutdown));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn actual_dispatch_freezes_more_inputs_than_the_original_change_body() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intents = (1..=3)
        .map(|id| cancel_intent(id, backend))
        .collect::<Vec<_>>();
    assert!(intents.len() > governed_wire::FIXTURE_CHANGES_PER_REQUEST);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Outcomes(vec![
        OperationOutcome::ResourceExhausted; 3
    ]));
    submit_batch(&fixture, released_batch(backend, intents));
    let received = settled(&fixture.acks, 3).await;
    for ack in &received {
        assert_eq!(
            ack.worker_outcome(),
            Some(OperationOutcome::ResourceExhausted)
        );
        assert!(ack.governance_failure().is_none());
    }
    assert_eq!(fixture.loopback.peer.control_requests(), 1);
    drop(received);
    let SinkFixture {
        mut loopback,
        sink,
        acks,
        mut runtime,
        governed,
    } = fixture;
    drop((sink, acks));
    governed.drive();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    governed.drive();
    let apply = runtime.drain_original_apply_until(deadline).await;
    governed.drive();
    let outgoing = runtime.drain_native_outgoing_until(deadline).await;
    let peer_exit = loopback.settle().await;
    drop((runtime, loopback));
    let shutdown = governed.finish();
    if apply.is_err() || outgoing.is_err() || !peer_exit || shutdown.is_err() {
        std::panic::panic_any((apply, outgoing, peer_exit, shutdown));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn canonical_reply_alias_survives_original_send_and_physical_exit() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intent = cancel_intent(1, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Outcomes(vec![
            OperationOutcome::ResourceExhausted,
        ]));
    submit_batch(&fixture, released_batch(backend, vec![intent]));
    let mut acks = settled(&fixture.acks, 1).await;
    let original = acks.pop().unwrap();
    let retained = original.clone();
    assert!(
        original.shares_original_canonical_with(&retained),
        "both values borrow the same original canonical receipt"
    );
    drop(original);
    // The real original task and cleanup execute on the actual fixture
    // runtime; reply aliases are independent of process queue occupancy.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .sink
                .data_runtime
                .task_transport_supervisor()
                .snapshot()
                .retained_items
                == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual original queue/encoding exit");
    assert_eq!(
        retained.worker_outcome(),
        Some(OperationOutcome::ResourceExhausted)
    );
    assert_eq!(fixture.loopback.peer.control_requests(), 1);
    drop(retained);
}

#[tokio::test(flavor = "multi_thread")]
async fn frozen_descriptor_routes_ordinary_and_control_runs_to_independent_sockets() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    assert_ne!(fixture.loopback.endpoint, fixture.loopback.control_endpoint);
    let operations = [update_intent(1, backend, 1), cancel_intent(1, backend)]
        .iter()
        .map(|intent| {
            (
                is_small_control(intent),
                encode_operation(intent, &test_attempt_facts()).unwrap(),
            )
        })
        .collect();
    let runs = encode_method_batches(operations, TransportBudget::DEFAULT).unwrap();
    assert_eq!(runs.len(), 2);
    for run in runs {
        send_operations(
            &fixture.sink.targets[&backend].client,
            run.request,
            tokio::time::Instant::now() + Duration::from_secs(5),
            &fixture.acks.handle(),
            &[],
        )
        .await
        .expect("the correct socket accepts its method");
    }
    assert_eq!(fixture.loopback.peer.applied().len(), 2);
    assert_eq!(fixture.loopback.peer.control_requests(), 1);

    // Deliberately bypass the frozen client selector to prove each actual
    // peer rejects the other socket's method, before consuming its grammar.
    let mut data = super::super::transport::Client::for_endpoint(
        fixture.loopback.endpoint.native_endpoint().clone(),
        NativeEndpointDomain::BackendData,
        backend,
        fixture.sink.data_runtime.clone(),
    )
    .grpc_with_channel_error(NativeRpcMethod::ApplyTaskOperations)
    .await
    .unwrap();
    let error = data
        .apply_task_control_operations(proto::ApplyTaskControlOperationsRequest {
            operations: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    let mut control = super::super::transport::Client::for_endpoint(
        fixture.loopback.control_endpoint.native_endpoint().clone(),
        NativeEndpointDomain::BackendControl,
        backend,
        fixture.sink.data_runtime.clone(),
    )
    .grpc_with_channel_error(NativeRpcMethod::ApplyTaskControlOperations)
    .await
    .unwrap();
    let error = control
        .apply_task_operations(proto::ApplyTaskOperationsRequest {
            operations: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(fixture.loopback.peer.applied().len(), 2);
    assert_eq!(fixture.loopback.peer.control_requests(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_adapter_uses_the_frozen_control_socket_and_preserves_the_whole_descriptor() {
    use novarocks_query_application::api::HeartbeatOutcome;
    let backend = BackendProcessId::new_v7();
    let loopback = Loopback::start().await;
    let descriptor = loopback.descriptor(backend);
    loopback.peer.state.lock().unwrap().heartbeat_descriptor = Some(descriptor.clone());
    let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let control = loopback.control_endpoint.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        super::super::transport::heartbeat(&runtime, backend, control, Duration::from_secs(5))
    })
    .await
    .unwrap();
    let HeartbeatOutcome::Ok {
        descriptor: observed,
        ..
    } = outcome
    else {
        panic!("control heartbeat must complete");
    };
    assert_eq!(observed, descriptor);
    {
        let state = loopback.peer.state.lock().unwrap();
        assert_eq!(state.heartbeats.len(), 1);
        assert_eq!(
            state.heartbeats[0]
                .expected_process_id
                .as_ref()
                .unwrap()
                .value,
            backend.to_bytes()
        );
    }
    let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let mut data = Client::for_endpoint(
        loopback.endpoint.native_endpoint().clone(),
        NativeEndpointDomain::BackendData,
        backend,
        runtime,
    )
    .grpc_with_channel_error(NativeRpcMethod::ApplyTaskOperations)
    .await
    .unwrap();
    assert_eq!(
        data.heartbeat(proto::HeartbeatRequest {
            expected_process_id: None
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(loopback.peer.state.lock().unwrap().heartbeats.len(), 1);
}

/// Waits for the transport to settle `count` operations.
async fn settled(acks: &TaskAckIntake, count: usize) -> Vec<OperationAcknowledgement> {
    for _ in 0..600 {
        if acks.queued() >= count {
            return acks.drain();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the transport settled {} of {count} operations",
        acks.queued()
    );
}

fn submit_batch(fixture: &SinkFixture, mut batch: DispatchBatch) {
    let permits = batch
        .operations()
        .iter()
        .map(|intent| fixture.governed.reserve(&fixture.sink, intent))
        .collect();
    batch.replace_fixture_permits(permits);
    let batch = match fixture.sink.try_submit(batch) {
        TaskOperationSubmit::Backpressured(batch) => batch,
        result => {
            assert!(matches!(result, TaskOperationSubmit::Accepted));
            return;
        }
    };
    fixture.governed.drive();
    assert!(matches!(
        fixture.sink.try_submit(batch),
        TaskOperationSubmit::Accepted
    ));
}

// -----------------------------------------------------------------------
// The operation sink
// -----------------------------------------------------------------------

#[test]
fn task_cancel_uses_the_process_control_reserve() {
    let backend = BackendProcessId::new_v7();
    let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
    assert_eq!(supervisor_lane(&batch), NativeTransportLane::Control);
}

#[test]
fn mixed_operation_shapes_keep_order_across_method_batches() {
    let backend = BackendProcessId::new_v7();
    let intents = [
        update_intent(1, backend, 1),
        renew_intent(backend),
        cancel_intent(1, backend),
        update_intent(1, backend, 2),
    ];
    let expected_ids = intents
        .iter()
        .map(OperationIntent::operation_id)
        .collect::<Vec<_>>();
    let operations = intents
        .iter()
        .map(|intent| {
            (
                is_small_control(intent),
                encode_operation(intent, &test_attempt_facts()).expect("encodable intent"),
            )
        })
        .collect();
    let batches = encode_method_batches(operations, TransportBudget::DEFAULT)
        .expect("mixed batch fits transport budget");
    assert_eq!(
        batches.iter().map(|batch| batch.items).collect::<Vec<_>>(),
        vec![1, 1, 1, 1]
    );
    let actual_ids = batches
        .iter()
        .flat_map(|batch| match &batch.request {
            EncodedMethodRequest::Ordinary(request) => request
                .operations
                .iter()
                .map(|operation| operation.envelope.as_ref().unwrap().operation_id.clone())
                .collect::<Vec<_>>(),
            EncodedMethodRequest::Control(request) => request
                .operations
                .iter()
                .map(|operation| operation.envelope.as_ref().unwrap().operation_id.clone())
                .collect::<Vec<_>>(),
        })
        .collect::<Vec<_>>();
    let expected_wire_ids = expected_ids
        .into_iter()
        .map(|id| {
            Some(proto::TaskOperationId {
                value: id.to_bytes().to_vec(),
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, expected_wire_ids);
    assert!(matches!(
        batches[0].request,
        EncodedMethodRequest::Ordinary(_)
    ));
    assert!(matches!(
        batches[1].request,
        EncodedMethodRequest::Control(_)
    ));
    assert!(matches!(
        batches[2].request,
        EncodedMethodRequest::Control(_)
    ));
    assert!(matches!(
        batches[3].request,
        EncodedMethodRequest::Ordinary(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_later_control_run_does_not_restart_its_expired_submit_deadline() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intents = [update_intent(1, backend, 1), cancel_intent(1, backend)];
    let operations = intents
        .iter()
        .enumerate()
        .map(|(index, intent)| {
            let mut operation =
                encode_operation(intent, &test_attempt_facts()).expect("encodable intent");
            operation
                .envelope
                .as_mut()
                .expect("envelope")
                .max_wait_millis = if index == 0 { 300_000 } else { 100 };
            (is_small_control(intent), operation)
        })
        .collect();
    let batches =
        encode_method_batches(operations, TransportBudget::DEFAULT).expect("two method runs");
    assert_eq!(batches.len(), 2);
    // Model time already consumed by an earlier run without sleeping or
    // depending on scheduler timing. The ordinary item still has time;
    // the later control item had only 100 ms from the same submit origin.
    let submitted_at = tokio::time::Instant::now() - Duration::from_secs(10);
    let client = &fixture.sink.targets[&backend].client;
    let mut batches = batches.into_iter();
    let first = batches.next().expect("ordinary run");
    let first_expiry = first.expires_at(submitted_at);
    send_operations(
        client,
        first.request,
        first_expiry,
        &fixture.acks.handle(),
        &[],
    )
    .await
    .expect("ordinary run still has time");
    let second = batches.next().expect("control run");
    assert!(matches!(&second.request, EncodedMethodRequest::Control(_)));
    let second_expiry = second.expires_at(submitted_at);
    assert_eq!(
        send_operations(
            client,
            second.request,
            second_expiry,
            &fixture.acks.handle(),
            &[]
        )
        .await,
        Err(OperationDispatchResult::TransportUnknown),
        "expired control run must not reach its RPC"
    );
    assert_eq!(fixture.loopback.peer.control_requests(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn adjacent_control_items_with_different_waits_keep_each_submit_deadline() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intents = [
        update_intent(1, backend, 1),
        cancel_intent(1, backend),
        renew_intent(backend),
    ];
    let operations = intents
        .iter()
        .enumerate()
        .map(|(index, intent)| {
            let mut operation = encode_operation(intent, &test_attempt_facts()).unwrap();
            operation.envelope.as_mut().unwrap().max_wait_millis =
                if index == 1 { 100 } else { 300_000 };
            (is_small_control(intent), operation)
        })
        .collect();
    let batches = encode_method_batches(operations, TransportBudget::DEFAULT).unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.items).collect::<Vec<_>>(),
        [1, 1, 1]
    );
    let submitted_at = tokio::time::Instant::now() - Duration::from_secs(10);
    let client = &fixture.sink.targets[&backend].client;
    let mut batches = batches.into_iter();
    let first = batches.next().unwrap();
    let deadline = first.expires_at(submitted_at);
    send_operations(client, first.request, deadline, &fixture.acks.handle(), &[])
        .await
        .unwrap();
    let short = batches.next().unwrap();
    let deadline = short.expires_at(submitted_at);
    assert_eq!(
        send_operations(client, short.request, deadline, &fixture.acks.handle(), &[]).await,
        Err(OperationDispatchResult::TransportUnknown)
    );
    assert_eq!(fixture.loopback.peer.control_requests(), 0);
    let long = batches.next().unwrap();
    let deadline = long.expires_at(submitted_at);
    send_operations(client, long.request, deadline, &fixture.acks.handle(), &[])
        .await
        .unwrap();
    assert_eq!(fixture.loopback.peer.control_requests(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn send_started_is_published_only_when_the_exact_rpc_future_is_polled() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intent = update_intent(1, backend, 1);
    let operation_id = intent.operation_id();
    let participation = participation(backend);
    let operation = encode_operation(&intent, &test_attempt_facts()).expect("encodable");
    let batch = encode_method_batches(vec![(false, operation)], TransportBudget::DEFAULT)
        .expect("one legal request")
        .pop()
        .expect("one method batch");
    let client = &fixture.sink.targets[&backend].client;
    let intake = fixture.acks.handle();
    let establishes = [(operation_id, participation)];
    let send = send_operations(
        client,
        batch.request,
        tokio::time::Instant::now() + Duration::from_secs(5),
        &intake,
        &establishes,
    );
    assert!(
        fixture.acks.drain_events().is_empty(),
        "future construction is not send start"
    );
    send.await.expect("the loopback RPC answers");
    let events = fixture.acks.drain_events();
    assert!(matches!(
        events.as_slice(),
        [TaskOperationIntakeEvent::EstablishSendStarted {
            operation_id: observed,
            participation: observed_participation,
        }] if *observed == operation_id && *observed_participation == participation
    ));

    let expired = encode_method_batches(
        vec![(
            false,
            encode_operation(&intent, &test_attempt_facts()).expect("encodable"),
        )],
        TransportBudget::DEFAULT,
    )
    .expect("one legal request")
    .pop()
    .expect("one method batch");
    assert_eq!(
        send_operations(
            client,
            expired.request,
            tokio::time::Instant::now() - Duration::from_secs(1),
            &intake,
            &establishes,
        )
        .await,
        Err(OperationDispatchResult::TransportUnknown)
    );
    assert!(
        fixture.acks.drain_events().is_empty(),
        "an expired preflight cannot report send start"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn submitting_after_original_role_drain_rejects_the_original_batch_without_a_send() {
    let backend = BackendProcessId::new_v7();
    let mut fixture = sink_fixture(Loopback::start().await, backend);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("build the original role runtime");
    let mut original_role = FrontendDataRuntime::new(runtime.handle().clone());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let original_drain = original_role.drain_original_apply_until(deadline).await;
    let original_joined = original_role.original_apply_joined();
    // The actual original task owner is drained while its runtime is alive.
    tokio::task::block_in_place(|| runtime.shutdown_timeout(Duration::from_secs(2)));
    let mut sink = NativeTaskOperationSink::new(
        &[fixture.loopback.descriptor(backend)],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        fixture.acks.handle(),
        original_role,
        fixture.governed.authority(),
    )
    .expect("one frozen backend target after original role drain");
    // Keep the actual wire client on the live test runtime. Only the
    // original role has closed. Rejection must precede any wire task.
    sink.targets
        .get_mut(&backend)
        .expect("frozen target")
        .client = fixture.sink.targets[&backend].client.clone();
    let supervisor = sink.data_runtime.task_transport_supervisor();
    let before = supervisor.snapshot();
    let mut batch = released_batch(
        backend,
        vec![cancel_intent(1, backend), cancel_intent(2, backend)],
    );
    let expected_ids = batch
        .operations()
        .iter()
        .map(OperationIntent::operation_id)
        .collect::<Vec<_>>();
    assert_eq!(expected_ids.len(), 2);
    let permits = batch
        .operations()
        .iter()
        .map(|intent| fixture.governed.reserve(&sink, intent))
        .collect();
    batch.replace_fixture_permits(permits);
    assert_eq!(
        supervisor.snapshot().retained_items,
        before.retained_items + 2
    );
    assert!(
        supervisor.snapshot().retained_queued_and_encoded_bytes
            > before.retained_queued_and_encoded_bytes
    );
    assert!(fixture.acks.drain_events().is_empty());

    // A closed original role rejects BEFORE constructing F. The caller
    // keeps the exact batch for the existing definitely-unsent rollback.
    let (rejected, returned_ids, reason, held_returned_items) = match sink.try_submit(batch) {
        TaskOperationSubmit::Rejected { batch, reason } => {
            let ids = batch
                .operations()
                .iter()
                .map(OperationIntent::operation_id)
                .collect::<Vec<_>>();
            let held = supervisor.snapshot().retained_items;
            drop(batch);
            (true, ids, reason, held)
        }
        TaskOperationSubmit::Backpressured(batch) => {
            drop(batch);
            (false, Vec::new(), String::new(), 0)
        }
        TaskOperationSubmit::Accepted => (false, Vec::new(), String::new(), 0),
        TaskOperationSubmit::AdmissionFailed { batch, error } => {
            drop(batch);
            panic!("original source must admit before testing closed role: {error}")
        }
    };
    let acknowledgements = fixture.acks.drain_events();
    let returned = supervisor.snapshot();
    let applied = fixture.loopback.peer.applied();
    let control_requests = fixture.loopback.peer.control_requests();
    // Join the actual fixture servers before checking the collected result.
    // Forced server cancellation is test cleanup, not role drain evidence.
    for served in &fixture.loopback.served {
        served.abort();
    }
    let mut server_cleanup = true;
    for served in &mut fixture.loopback.served {
        match tokio::time::timeout_at(deadline.into(), served).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            _ => server_cleanup = false,
        }
    }

    assert!(original_drain.is_ok(), "{original_drain:?}");
    assert!(original_joined);
    assert!(rejected);
    assert!(reason.contains("closed"));
    assert_eq!(returned_ids, expected_ids);
    assert_eq!(held_returned_items, before.retained_items + 2);
    assert!(
        acknowledgements.is_empty(),
        "rejection cannot manufacture an ACK"
    );
    assert_eq!(
        returned, before,
        "caller returned its original reservations"
    );
    assert!(applied.is_empty());
    assert_eq!(control_requests, 0);
    assert!(server_cleanup);
}

#[test]
fn dropping_an_accepted_send_settles_only_its_remaining_items_as_unknown() {
    let intake = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let backend = BackendProcessId::new_v7();
    let intents = [update_intent(1, backend, 1), cancel_intent(2, backend)];
    let sent = intents
        .iter()
        .map(|intent| SentOperation {
            operation_id: intent.operation_id(),
            kind: intent.kind(),
            establish_participation: None,
            lease_renewal: is_lease_renewal(intent),
            address: AckAddress::of(intent),
        })
        .collect::<Vec<_>>();
    let ids = sent
        .iter()
        .map(|item| item.operation_id)
        .collect::<Vec<_>>();
    let mut receipts = AcceptedOperationReceipts::new(intake.handle(), sent);
    receipts.publish_next(OperationAcknowledgement::transport_unknown(
        ids[0],
        intents[0].kind(),
    ));

    drop(receipts);

    let acknowledgements = intake.drain();
    assert_eq!(acknowledgements.len(), 2);
    assert_eq!(
        acknowledgements
            .iter()
            .map(OperationAcknowledgement::operation_id)
            .collect::<Vec<_>>(),
        ids,
        "drop settlement preserves request order and never republishes a settled item"
    );
    assert!(acknowledgements.iter().all(|ack| matches!(
        ack.dispatch_result(),
        OperationDispatchResult::TransportUnknown
    )));
}

#[test]
fn task_cancel_leaves_update_backlog_first_and_reaches_control_capacity() {
    let backend = BackendProcessId::new_v7();
    let transport = TransportBudget::new(
        2,
        1024,
        novarocks_task_codec::TransportMessageLimits::DEFAULT,
        4,
        256 * 1024 * 1024,
        4,
        512 * 1024 * 1024,
        1,
        2,
        Duration::from_secs(1),
    )
    .expect("one ordinary and one control batch fit");
    // Queue selection and the saturated Update permit are checked against
    // the actual pure dispatcher in AP's matching named test. This component
    // checks the same Cancel metadata at the original Native capacity seam.
    let cancel = released_batch(backend, vec![cancel_intent(1, backend)]);
    assert_eq!(cancel.operations().len(), 1);
    assert_eq!(cancel.operations()[0].kind(), OperationKind::CancelTask);
    assert_eq!(cancel.lane(), DispatchLane::Control);
    assert_eq!(supervisor_lane(&cancel), NativeTransportLane::Control);

    let supervisor = NativeTransportSupervisor::from_transport(transport)
        .expect("the process windows were validated");
    let intake = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let waiter = supervisor
        .register_waiter(Arc::new(intake.handle()))
        .expect("register the attempt");
    let mut ordinary_queue = supervisor
        .try_reserve_queue(
            &waiter,
            backend,
            NativeTransportLane::Ordinary,
            transport.max_batch_items(),
            crate::task_execution::intent::NativeRequestWindows::for_transport(transport)
                .unwrap()
                .empty_queue_capacity(false),
        )
        .expect("fill the ordinary queue window exactly");
    let ordinary_encoding = supervisor
        .try_reserve_encoding(&waiter, backend, NativeTransportLane::Ordinary)
        .expect("ordinary head can always reserve its encoding window");
    ordinary_queue.mark_in_flight();
    let mut control_queue = supervisor
        .try_reserve_queue(
            &waiter,
            backend,
            supervisor_lane(&cancel),
            cancel.operations().len(),
            cancel.queued_bytes(),
        )
        .expect("the cancellation directly consumes the reserved control queue");
    let mut control_encoding = supervisor
        .try_reserve_encoding(&waiter, backend, supervisor_lane(&cancel))
        .expect("the cancellation can reserve encoding beside ordinary I/O");
    control_encoding.shrink_to(1);
    control_queue.mark_in_flight();
    drop(control_encoding);
    drop(control_queue);
    drop(ordinary_encoding);
    drop(ordinary_queue);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partial_batch_failure_is_settled_per_item_in_request_order() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(
        backend,
        vec![
            cancel_intent(1, backend),
            cancel_intent(2, backend),
            cancel_intent(3, backend),
        ],
    );
    let expected = batch
        .operations()
        .iter()
        .map(OperationIntent::operation_id)
        .collect::<Vec<_>>();
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Outcomes(vec![
            OperationOutcome::Accepted,
            OperationOutcome::TerminalRejected,
            OperationOutcome::Idempotent,
        ]));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 3).await;

    assert_eq!(
        acks.iter()
            .map(OperationAcknowledgement::operation_id)
            .collect::<Vec<_>>(),
        expected,
        "receipts are handed back in request order"
    );
    assert_eq!(
        acks.iter()
            .map(OperationAcknowledgement::worker_outcome)
            .collect::<Vec<_>>(),
        vec![
            Some(OperationOutcome::Accepted),
            Some(OperationOutcome::TerminalRejected),
            Some(OperationOutcome::Idempotent),
        ],
        "one item's failure leaves every other item exactly as its own receipt reports"
    );
    assert_eq!(
        fixture.loopback.peer.applied().len(),
        1,
        "one batch, one RPC"
    );
    assert_eq!(fixture.loopback.peer.control_requests(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn reordered_receipts_are_refused() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(
        backend,
        vec![cancel_intent(1, backend), cancel_intent(2, backend)],
    );
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Reversed(vec![
            OperationOutcome::Accepted,
            OperationOutcome::TerminalRejected,
        ]));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 2).await;

    for ack in &acks {
        assert_eq!(
            ack.dispatch_result(),
            OperationDispatchResult::NonWorkerRejected
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_receipt_batch_is_refused() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(
        backend,
        vec![cancel_intent(1, backend), cancel_intent(2, backend)],
    );
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Truncated(vec![
            OperationOutcome::Accepted,
            OperationOutcome::Accepted,
        ]));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 2).await;

    assert!(
        acks.iter()
            .all(|ack| ack.dispatch_result() == OperationDispatchResult::NonWorkerRejected),
        "a response with fewer receipts than items cannot be attributed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_reply_preflight_failure_preserves_remote_effect_without_worker_verdict_or_replay() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(proto::task_operation_verdict::Ack::CreateTask(
                proto::CreateTaskAck {
                    accepted_domains: vec![
                        proto::TaskDomainReceipt::default();
                        novarocks_task_codec::domain::MAX_DOMAIN_UPDATES + 1
                    ],
                    ..Default::default()
                },
            )),
        )]));
    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 1).await;
    assert_eq!(
        fixture.loopback.peer.applied().len(),
        1,
        "the remote effect was dispatched once before its reply was refused locally"
    );
    assert_eq!(
        acks[0].dispatch_result(),
        OperationDispatchResult::NonWorkerRejected
    );
    assert_eq!(
        acks[0].worker_outcome(),
        None,
        "a local decoder never mints a Worker verdict"
    );
    assert_eq!(
        crate::task_execution::operation::frontend_action(acks[0].dispatch_result()),
        crate::task_execution::operation::FrontendAction::FailAttempt
    );
    assert_eq!(*acks[0].payload(), AckPayload::None);
}

struct GoAwayWireProof {
    stream_id: u32,
    last_stream_id: u32,
    reason: u32,
    body: Vec<u8>,
}

// This is a real HTTP/2 wire peer, not a manufactured h2::Error. The
// request is received but never handed to a Task application before the
// real GOAWAY declares last_stream_id=0. The client library owns parsing
// that frame and constructing its original tonic Status/source chain.
async fn receive_then_goaway(
    socket: &mut tokio::net::TcpStream,
    reason: h2::Reason,
) -> anyhow::Result<GoAwayWireProof> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut preface = [0; 24];
    socket.read_exact(&mut preface).await?;
    anyhow::ensure!(
        &preface == b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
        "actual client preface differs"
    );
    socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
    let mut stream_id = None;
    let mut body = Vec::new();
    loop {
        let mut header = [0; 9];
        socket.read_exact(&mut header).await?;
        let len =
            (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
        // Empty peer SETTINGS leave the protocol's default frame size.
        anyhow::ensure!(len <= 16_384, "client exceeded default HTTP/2 frame size");
        let id = u32::from_be_bytes(header[5..9].try_into()?) & 0x7fff_ffff;
        let mut payload = vec![0; len];
        socket.read_exact(&mut payload).await?;
        match header[3] {
            4 => {
                anyhow::ensure!(id == 0, "SETTINGS addressed an application stream");
                if header[4] & 1 == 0 {
                    socket.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).await?;
                }
            }
            6 => {
                anyhow::ensure!(id == 0 && len == 8, "invalid client PING");
                if header[4] & 1 == 0 {
                    socket.write_all(&[0, 0, 8, 6, 1, 0, 0, 0, 0]).await?;
                    socket.write_all(&payload).await?;
                }
            }
            1 => {
                anyhow::ensure!(id > 0 && id & 1 == 1, "invalid client request stream");
                anyhow::ensure!(
                    stream_id.is_none_or(|original| original == id),
                    "a second request preceded the controlled GOAWAY"
                );
                stream_id = Some(id);
                anyhow::ensure!(
                    header[4] & 1 == 0,
                    "unary request ended without its real message"
                );
            }
            0 => {
                anyhow::ensure!(
                    stream_id == Some(id),
                    "DATA differs from original request stream"
                );
                let bytes = if header[4] & 8 != 0 {
                    let padding = usize::from(
                        *payload
                            .first()
                            .ok_or_else(|| anyhow::anyhow!("missing DATA padding length"))?,
                    );
                    anyhow::ensure!(padding < len, "invalid DATA padding");
                    &payload[1..len - padding]
                } else {
                    payload.as_slice()
                };
                body.extend_from_slice(bytes);
                anyhow::ensure!(
                    body.len()
                        <= novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
                            + 5,
                    "test request exceeded its original codec bound"
                );
                if header[4] & 1 != 0 {
                    break;
                }
            }
            2 | 8 | 9 => {}
            kind => anyhow::bail!("unexpected client frame before GOAWAY: {kind}"),
        }
    }
    anyhow::ensure!(
        body.len() >= 5 && body[0] == 0,
        "missing uncompressed unary message"
    );
    anyhow::ensure!(
        u32::from_be_bytes(body[1..5].try_into()?) as usize == body.len() - 5,
        "unary message length differs"
    );
    let _request = <proto::ApplyTaskOperationsRequest as prost::Message>::decode(&body[5..])?;
    let last_stream_id = 0u32;
    let mut frame = vec![0, 0, 8, 7, 0, 0, 0, 0, 0];
    frame.extend_from_slice(&last_stream_id.to_be_bytes());
    frame.extend_from_slice(&u32::from(reason).to_be_bytes());
    socket.write_all(&frame).await?;
    socket.flush().await?;
    Ok(GoAwayWireProof {
        stream_id: stream_id.ok_or_else(|| anyhow::anyhow!("no original request stream"))?,
        last_stream_id,
        reason: u32::from(reason),
        body,
    })
}

async fn goaway_loopback(
    reason: h2::Reason,
) -> anyhow::Result<(Loopback, tokio::sync::oneshot::Receiver<GoAwayWireProof>)> {
    let peer = TaskWirePeer::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint =
        RuntimeEndpoint::new("127.0.0.1", i32::from(address.port())).map_err(anyhow::Error::msg)?;
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let (observed, proof) = tokio::sync::oneshot::channel();
    let healthy = peer.clone();
    let served = tokio::spawn(async move {
        let (mut original_socket, _) = listener
            .accept()
            .await
            .expect("accept original Native connection");
        let original = receive_then_goaway(&mut original_socket, reason)
            .await
            .expect("send real controlled GOAWAY");
        let _ = observed.send(original);
        // A transparent Hyper reconnect, or an allowed exact replay, uses
        // this same endpoint and the existing real Tonic receipt author.
        let incoming = futures::stream::unfold(listener, |listener| async {
            let accepted = listener.accept().await.map(|(socket, _)| socket);
            Some((accepted, listener))
        });
        tonic::transport::Server::builder()
            .add_service(NovaRocksGrpcServer::new(healthy))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .expect("serve subsequent original Native connection");
        // Keep the first socket alive until the original owner stops; an
        // injected EOF must not replace the real GOAWAY source evidence.
        drop(original_socket);
    });
    let mut control_peer = peer.clone();
    control_peer.domain = NativeEndpointDomain::BackendControl;
    let (control_endpoint, control_shutdown, control_served) = Loopback::serve(control_peer).await;
    Ok((
        Loopback {
            root_connections: None,
            peer,
            endpoint,
            control_endpoint,
            shutdown: [Some(shutdown), Some(control_shutdown)],
            served: [served, control_served],
        },
        proof,
    ))
}

async fn real_goaway_conformance_case(reason: h2::Reason) -> anyhow::Result<()> {
    let backend = BackendProcessId::new_v7();
    // Dynamic-filter updates select ApplyTaskOperations/Submission. The
    // old cancellation fixture would exercise the separate Control lane.
    let intent = update_intent(1, backend, 1);
    let wire = proto::ApplyTaskOperationsRequest {
        operations: vec![
            encode_operation(&intent, &test_attempt_facts()).map_err(anyhow::Error::msg)?,
        ],
    };
    let encoded = prost::Message::encode_to_vec(&wire);
    let mut expected = vec![0];
    expected.extend_from_slice(&u32::try_from(encoded.len())?.to_be_bytes());
    expected.extend_from_slice(&encoded);
    let (loopback, proof) = goaway_loopback(reason).await?;
    let mut fixture = sink_fixture(loopback, backend);
    let client = &fixture.sink.targets[&backend].client;
    let operation = tokio::time::timeout(Duration::from_secs(5), async {
        let method = NativeRpcMethod::ApplyTaskOperations;
        let (mut grpc, acquired) = client.operation_test_grpc_with_channel_identity(method).await.map_err(|error| anyhow::anyhow!("{error}"))?;
        let response = fixture_apply_request(&mut grpc, Request::new(wire.clone()), method, wire.operations.len(), client.method_message_limits(method).1).await;
        let proof = proof.await?;
        anyhow::ensure!(proof.stream_id > proof.last_stream_id && proof.last_stream_id == 0, "actual stream did not exceed actual GOAWAY last id");
        anyhow::ensure!(proof.reason == u32::from(reason) && proof.body == expected, "actual frame or original request bytes differ");
        let mut receipt = None;
        let observation = match response {
            Ok(reply) => {
                receipt = Some(reply.into_inner());
                (None, None, None)
            }
            Err(status) => {
                let code = status.code();
                let observed = task_operation_h2_source(&status);
                let classified = classify_apply_status(&status);
                eprintln!("REAL_GOAWAY_CONFORMANCE reason={} stream={} last={} code={:?} h2_source={:?} classified={:?}", proof.reason, proof.stream_id, proof.last_stream_id, code, observed, classified);
                drop(status);
                if reason == h2::Reason::NO_ERROR && classified == OperationDispatchResult::TransportUnknown {
                    client.invalidate_channel_if_current(method, &acquired);
                    let (mut grpc, _) = client.operation_test_grpc_with_channel_identity(method).await.map_err(|error| anyhow::anyhow!("{error}"))?;
                    receipt = Some(fixture_apply_request(&mut grpc, Request::new(wire.clone()), method, wire.operations.len(), client.method_message_limits(method).1).await?.into_inner());
                }
                (Some(code), observed, Some(classified))
            }
        };
        if let Some(reply) = receipt {
            let headers = decode_receipt_batch(&reply, &[intent.operation_id()], FieldPath::root("real_goaway_receipt"))?;
            anyhow::ensure!(headers.len() == 1 && headers[0].outcome() == OperationOutcome::Accepted, "original wire receipt did not accept the same operation");
            let applied = fixture.loopback.peer.applied();
            anyhow::ensure!(applied.len() == 1 && applied[0] == wire, "reconnect/replay changed the immutable request");
            eprintln!("REAL_GOAWAY_CONFORMANCE actual_wire_receipt=true transparent_recovery={}", observation.0.is_none());
        }
        Ok::<_, anyhow::Error>(observation)
    }).await;
    let joined = fixture.loopback.settle().await;
    anyhow::ensure!(joined, "original GOAWAY peer handles did not join");
    let (code, observed, classified) = operation
        .map_err(|_| anyhow::anyhow!("real GOAWAY conformance five-second watchdog"))??;
    match code {
        Some(tonic::Code::Internal) => {
            let observed = observed.ok_or_else(|| {
                anyhow::anyhow!(
                    "actual Internal status did not retain typed H2; no typed classifier proof"
                )
            })?;
            anyhow::ensure!(
                observed.reason == Some(u32::from(reason))
                    && observed.is_remote
                    && observed.is_go_away
                    && !observed.is_reset,
                "actual Internal source differs from controlled remote GOAWAY"
            );
            assert_eq!(
                classified,
                Some(if reason == h2::Reason::NO_ERROR {
                    OperationDispatchResult::TransportUnknown
                } else {
                    OperationDispatchResult::NonWorkerRejected
                }),
                "classify the real typed transport source, never its message"
            );
        }
        Some(code) => {
            eprintln!(
                "REAL_GOAWAY_CONFORMANCE counterevidence=no_Internal actual_code={code:?} h2_source={observed:?} classified={classified:?}"
            );
            anyhow::ensure!(
                classified == Some(OperationDispatchResult::TransportUnknown),
                "non-Internal outcome must retain its existing classifier contract"
            );
        }
        None => {
            eprintln!(
                "REAL_GOAWAY_CONFORMANCE counterevidence=transparent_success no_Internal_or_typed_failure"
            );
            anyhow::ensure!(
                reason == h2::Reason::NO_ERROR,
                "protocol error transparently recovered without exposing an error"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn real_network_no_error_goaway_observes_actual_status_and_retry_contract()
-> anyhow::Result<()> {
    real_goaway_conformance_case(h2::Reason::NO_ERROR).await
}

#[tokio::test(flavor = "multi_thread")]
async fn real_network_protocol_error_goaway_keeps_typed_internal_rejected() -> anyhow::Result<()> {
    real_goaway_conformance_case(h2::Reason::PROTOCOL_ERROR).await
}

#[tokio::test(flavor = "multi_thread")]
async fn real_network_remote_internal_same_literal_has_no_h2_source_and_is_rejected()
-> anyhow::Result<()> {
    let backend = BackendProcessId::new_v7();
    let mut fixture = sink_fixture(Loopback::start().await, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::RejectWithMessage(
            tonic::Code::Internal,
            "h2 protocol error: http2 error",
        ));
    let request = proto::ApplyTaskOperationsRequest {
        operations: vec![
            encode_operation(&update_intent(1, backend, 1), &test_attempt_facts())
                .map_err(anyhow::Error::msg)?,
        ],
    };
    let operation = tokio::time::timeout(Duration::from_secs(5), async {
        let client = &fixture.sink.targets[&backend].client;
        let (mut grpc, _) = client
            .grpc_with_channel_identity(NativeRpcMethod::ApplyTaskOperations)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let result = grpc.apply_task_operations(Request::new(request)).await;
        match result {
            Err(status) => Ok::<_, anyhow::Error>((
                status.code(),
                status.message().to_owned(),
                task_operation_h2_source(&status),
                classify_apply_status(&status),
            )),
            Ok(_) => anyhow::bail!("remote Internal unexpectedly returned a receipt"),
        }
    })
    .await;
    let joined = fixture.loopback.settle().await;
    anyhow::ensure!(joined, "original remote Internal peer handles did not join");
    let (code, message, source, classified) =
        operation.map_err(|_| anyhow::anyhow!("remote Internal five-second watchdog"))??;
    assert_eq!(code, tonic::Code::Internal);
    assert_eq!(message, "h2 protocol error: http2 error");
    assert!(
        source.is_none(),
        "remote wire status has no local typed H2 cause"
    );
    assert_eq!(classified, OperationDispatchResult::NonWorkerRejected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_transport_outcome_is_retryable_and_resends_the_identical_request() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let intent = cancel_intent(1, backend);
    let batch = released_batch(backend, vec![intent.clone()]);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Reject(tonic::Code::Unavailable));

    submit_batch(&fixture, released_batch(backend, vec![intent]));
    let first = settled(&fixture.acks, 1).await;
    assert_eq!(
        first[0].dispatch_result(),
        OperationDispatchResult::TransportUnknown
    );

    // The owner replays the same immutable intent, so the same bytes go
    // back out under the same operation id.
    submit_batch(&fixture, batch);
    let second = settled(&fixture.acks, 1).await;
    assert_eq!(second[0].worker_outcome(), Some(OperationOutcome::Accepted));
    assert_eq!(second[0].operation_id(), first[0].operation_id());

    let applied = fixture.loopback.peer.applied();
    assert_eq!(applied.len(), 2);
    assert_eq!(
        applied[0], applied[1],
        "a retry resends the identical request rather than a new one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_rejection_is_not_retried() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Reject(tonic::Code::InvalidArgument));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 1).await;

    assert_eq!(
        acks[0].dispatch_result(),
        OperationDispatchResult::NonWorkerRejected
    );
    assert_eq!(acks[0].worker_outcome(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_process_mismatch_fails_the_attempt_without_sending() {
    let frozen = BackendProcessId::new_v7();
    let replacement = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, frozen);
    let batch = released_batch(replacement, vec![cancel_intent(1, replacement)]);

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 1).await;

    assert_eq!(acks[0].worker_outcome(), None);
    assert_eq!(
        acks[0].dispatch_result(),
        OperationDispatchResult::NonWorkerRejected
    );
    let original = acks[0]
        .local_failure()
        .expect("original local identity fact");
    assert_eq!(
        original.fact().reason(),
        novarocks_query_application::execution_error::TASK_PROTOCOL_IDENTITY_MISMATCH_REASON
    );
    assert_eq!(
        crate::task_execution::error::dispatch_rejection(&acks[0], acks[0].kind())
            .preparation_input(),
        *original
    );
    assert_eq!(acks[0].clone().local_failure(), Some(original));
    assert!(
        fixture.loopback.peer.applied().is_empty(),
        "a replaced process is never retried elsewhere"
    );

    // The same outcome arriving through an actual validated response
    // retains its Worker branch; local refusal does not widen that branch.
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::Outcomes(vec![
            OperationOutcome::IdentityMismatch,
        ]));
    submit_batch(
        &fixture,
        released_batch(frozen, vec![cancel_intent(2, frozen)]),
    );
    let remote = settled(&fixture.acks, 1).await;
    assert_eq!(
        remote[0].worker_outcome(),
        Some(OperationOutcome::IdentityMismatch)
    );
    assert!(remote[0].local_failure().is_none());
    assert_eq!(fixture.loopback.peer.applied().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_observation_read_is_refused_before_the_wire() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let supervisor = fixture.sink.data_runtime.task_transport_supervisor();
    let before = supervisor.snapshot();
    let intent = OperationIntent::FetchTaskDynamicFilters(FetchTaskDynamicFilters::new(
        TaskOperationId::new_v7(),
        identity(1, backend),
        None,
    ));
    let mut preparation = crate::task_execution::intent::TaskOperationQueuePreparation::new();
    assert!(matches!(
        fixture
            .sink
            .try_reserve_queue(intent.queue_request(), &mut preparation),
        TaskOperationQueueAdmission::Rejected(novarocks_workload_control::WorkError::Conflict)
    ));
    assert_eq!(
        supervisor.snapshot(),
        before,
        "a non-operation has no batch encoding and cannot acquire a queue reservation"
    );
    assert!(fixture.acks.drain_events().is_empty());
    assert!(fixture.loopback.peer.applied().is_empty());
    assert_eq!(fixture.loopback.peer.control_requests(), 0);
    drop(preparation);
    message_limits::close(fixture).await;
}

#[test]
fn the_transport_status_classification_is_exact() {
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
    ] {
        let result = classify_apply_status(&Status::new(code, "unknown outcome"));
        assert_eq!(result, OperationDispatchResult::TransportUnknown);
    }
    assert_eq!(
        classify_apply_status(&Status::new(tonic::Code::ResourceExhausted, "full")),
        OperationDispatchResult::IngressRejected(
            crate::task_execution::operation::IngressRejection::UnclassifiedCapacity
        )
    );
    let mut waiting = Status::resource_exhausted("ignored detail");
    waiting.metadata_mut().insert(
        "x-novarocks-ingress-rejection",
        tonic::metadata::MetadataValue::from_static("waiting_capacity"),
    );
    assert_eq!(
        classify_apply_status(&waiting),
        OperationDispatchResult::IngressRejected(
            crate::task_execution::operation::IngressRejection::WaitingCapacity
        )
    );
    let mut body = Status::resource_exhausted("ignored detail");
    body.metadata_mut().insert(
        "x-novarocks-ingress-rejection",
        tonic::metadata::MetadataValue::from_static("body_limit"),
    );
    assert_eq!(
        classify_apply_status(&body),
        OperationDispatchResult::IngressRejected(
            crate::task_execution::operation::IngressRejection::BodyLimit
        )
    );
    for code in [
        tonic::Code::InvalidArgument,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::FailedPrecondition,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unimplemented,
        tonic::Code::Internal,
        tonic::Code::DataLoss,
        tonic::Code::Unauthenticated,
    ] {
        let result = classify_apply_status(&Status::new(code, "settled"));
        assert!(
            matches!(result, OperationDispatchResult::NonWorkerRejected),
            "{code:?} must not be resent"
        );
    }
    assert!(matches!(
        classify_channel_error(&ChannelAcquisitionError::fatal("bad material")),
        OperationDispatchResult::NonWorkerRejected
    ));
    assert!(matches!(
        classify_channel_error(&ChannelAcquisitionError::retryable_network("dial failed")),
        OperationDispatchResult::TransportUnknown
    ));
}

// -----------------------------------------------------------------------
// The status subscription
// -----------------------------------------------------------------------

fn convergence_receipt(participation: ParticipationRef) -> ParticipationConvergenceReceipt {
    ParticipationConvergenceReceipt::new(
        participation,
        ParticipationConvergenceVersion::FIRST,
        ParticipationConvergenceState::WorkerStoppedAndParticipationFenced,
    )
}

#[test]
fn covered_unchanged_requires_the_exact_requested_identity() {
    use super::wire::{CoveredRequestedVersions, validate_covered_event_participation};
    let backend = BackendProcessId::new_v7();
    let participation = participation(backend);
    let requested = identity(1, backend);
    let other = identity(2, backend);
    let request = DecodedCoveredSubscription {
        participation,
        generation: NonZeroU64::new(3).unwrap(),
        status_cursors: vec![TaskStatusCursor::at(requested, TaskStatusVersion::FIRST)],
        task_convergence_cursors: vec![TaskConvergenceCursor::at(
            requested,
            TaskConvergenceVersion::FIRST,
        )],
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: vec![requested, other],
    };
    for fact in [
        CoveredStatusStreamFact::StatusUnchanged(other),
        CoveredStatusStreamFact::TaskConvergenceUnchanged(other),
    ] {
        let event = CoveredStatusStreamEvent {
            fact,
            source_revision: None,
        };
        assert!(
            validate_covered_event_participation(
                &request,
                &CoveredRequestedVersions::from_request(&request),
                &event
            )
            .is_err()
        );
    }
}

fn covered_validation_request(
    participation: ParticipationRef,
    task: TaskIdentity,
) -> DecodedCoveredSubscription {
    DecodedCoveredSubscription {
        participation,
        generation: NonZeroU64::new(3).unwrap(),
        status_cursors: vec![TaskStatusCursor::at(task, TaskStatusVersion::FIRST)],
        task_convergence_cursors: vec![TaskConvergenceCursor::at(
            task,
            TaskConvergenceVersion::FIRST,
        )],
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: vec![task],
    }
}

#[test]
fn covered_identity_violations_retain_their_typed_failure_for_every_carrier() {
    use super::wire::{
        CoveredEventValidationError, CoveredRequestedVersions, validate_covered_event_participation,
    };
    let backend = BackendProcessId::new_v7();
    let exact_participation = participation(backend);
    let exact_task = identity(1, backend);
    let request = covered_validation_request(exact_participation, exact_task);
    let requested_versions = CoveredRequestedVersions::from_request(&request);
    let other_query =
        QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(1).unwrap()).unwrap();
    for (query, process, expected) in [
        (
            exact_participation.query_execution_id(),
            BackendProcessId::new_v7(),
            CoveredEventValidationError::ProcessMismatch,
        ),
        (
            other_query,
            backend,
            CoveredEventValidationError::QueryMismatch,
        ),
    ] {
        let foreign_task =
            TaskIdentity::new(query, exact_task.stage_id(), exact_task.task_id(), process);
        let foreign_participation =
            ParticipationRef::new(query, exact_participation.frontend_process_id(), process);
        let facts = [
            CoveredStatusStreamFact::Status(status_at(foreign_task, TaskStatusVersion::FIRST)),
            CoveredStatusStreamFact::Gone(foreign_task),
            CoveredStatusStreamFact::Unknown(foreign_task),
            CoveredStatusStreamFact::StatusUnchanged(foreign_task),
            CoveredStatusStreamFact::TaskConvergenceUnchanged(foreign_task),
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                foreign_task,
                TaskConvergenceVersion::FIRST,
            )),
            CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(
                foreign_participation,
            )),
            CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                foreign_participation,
                1,
                vec![exact_task],
                ParticipationState::Quiescing,
            )),
            // A correct Participation cannot launder a foreign accepted member.
            CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
                exact_participation,
                1,
                vec![foreign_task],
                ParticipationState::Quiescing,
            )),
        ];
        for fact in facts {
            let event = CoveredStatusStreamEvent {
                fact,
                source_revision: None,
            };
            let error = validate_covered_event_participation(&request, &requested_versions, &event)
                .expect_err("foreign identity must be rejected");
            assert_eq!(error, expected, "{event:?}");
        }
    }
    let foreign_frontend = ParticipationRef::new(
        exact_participation.query_execution_id(),
        FrontendProcessId::new_v7(),
        backend,
    );
    for fact in [
        CoveredStatusStreamFact::ParticipationConvergence(convergence_receipt(foreign_frontend)),
        CoveredStatusStreamFact::Quiesce(QuiesceParticipationReceipt::new(
            foreign_frontend,
            1,
            vec![exact_task],
            ParticipationState::Quiescing,
        )),
    ] {
        assert_eq!(
            validate_covered_event_participation(
                &request,
                &requested_versions,
                &CoveredStatusStreamEvent {
                    fact,
                    source_revision: None
                }
            )
            .unwrap_err(),
            CoveredEventValidationError::QueryMismatch
        );
    }
}

#[test]
fn covered_version_and_generation_violations_remain_protocol_rejections() {
    use super::wire::{
        CoveredEventValidationError, CoveredRequestedVersions, validate_covered_event_participation,
    };
    let backend = BackendProcessId::new_v7();
    let request = covered_validation_request(participation(backend), identity(1, backend));
    let versions = CoveredRequestedVersions::from_request(&request);
    for fact in [
        CoveredStatusStreamFact::StatusUnchanged(identity(2, backend)),
        CoveredStatusStreamFact::TaskConvergenceUnchanged(identity(2, backend)),
        CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
            generation: 4,
            initial_cut: 1,
        }),
        CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
            generation: 4,
            sequence: 1,
            covered_prefix: 0,
            source_cut: 1,
        }),
    ] {
        let event = CoveredStatusStreamEvent {
            fact,
            source_revision: None,
        };
        assert!(
            matches!(
                validate_covered_event_participation(&request, &versions, &event).unwrap_err(),
                CoveredEventValidationError::Protocol(_)
            ),
            "{event:?}"
        );
    }
    for fact in [
        CoveredStatusStreamFact::StatusUnchanged(identity(1, backend)),
        CoveredStatusStreamFact::TaskConvergenceUnchanged(identity(1, backend)),
        CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
            generation: 3,
            initial_cut: 1,
        }),
        CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
            generation: 3,
            sequence: 1,
            covered_prefix: 0,
            source_cut: 1,
        }),
    ] {
        let event = CoveredStatusStreamEvent {
            fact,
            source_revision: None,
        };
        validate_covered_event_participation(&request, &versions, &event)
            .expect("exact version and generation remain valid");
    }
}

#[tokio::test]
async fn constructor_refusals_keep_their_actual_producer_facts() {
    use novarocks_coordination_contract::AttemptFailureClass;
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    use novarocks_query_application::coordination::ReplacementQualificationFailure;
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    let fixture = NativeRoleFixture::new();
    let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let descriptor = test_descriptor(
        BackendProcessId::new_v7(),
        RuntimeEndpoint::new("127.0.0.1", 9000).unwrap(),
        RuntimeEndpoint::new("control.test.invalid", 19061).unwrap(),
    );
    let source_loopback = Loopback::start().await;
    let governed = governed_wire::GovernedWireRole::new(&source_loopback);
    let error = NativeTaskOperationSink::new(
        &[],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        fixture.runtime.clone(),
        governed.authority(),
    )
    .unwrap_err();
    assert_eq!(error, NativeTaskTransportCreationError::NoBackends);
    assert_eq!(error.failure_fact().reason().code(), 102);
    let error = NativeTaskOperationSink::new(
        &[descriptor.clone(), descriptor.clone()],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        fixture.runtime.clone(),
        governed.authority(),
    )
    .unwrap_err();
    assert_eq!(
        error,
        NativeTaskTransportCreationError::DuplicateBackend {
            process: descriptor.process_id(),
        }
    );
    assert_eq!(error.failure_fact().reason().code(), 103);
    let other_budget = TransportBudget::new(
        2,
        1024,
        novarocks_task_codec::TransportMessageLimits::DEFAULT,
        4,
        256 * 1024 * 1024,
        4,
        512 * 1024 * 1024,
        1,
        2,
        Duration::from_secs(1),
    )
    .unwrap();
    let error = NativeTaskOperationSink::new(
        &[descriptor],
        other_budget,
        test_attempt_facts(),
        acks.handle(),
        fixture.runtime.clone(),
        governed.authority(),
    )
    .unwrap_err();
    assert_eq!(error, NativeTaskTransportCreationError::BudgetMismatch);
    assert_eq!(error.failure_fact().reason().code(), 101);
    let original = error.failure_fact();
    let refusal = ReplacementQualificationFailure::DomainRefused(original);
    let input = ExecutionFailureInput::new(
        refusal.failure_fact(),
        refusal.safe_detail(),
        ExecutionFailureSite::Coordinator,
    );
    assert_eq!(input.fact(), original);
    assert_eq!(input.fact().class(), FailureClass::Invariant);
    assert_eq!(
        input.detail().as_str(),
        "owning service refused replacement qualification"
    );
    assert_eq!(
        input.attempt_class(),
        AttemptFailureClass::ContractViolation
    );
    // Real original Apply/outgoing owners settle before this test returns.
    fixture.settle().await;
}

#[test]
fn every_task_transport_collector_registers() {
    let registry = Registry::new();
    register_metric_collectors(&registry).expect("a fresh registry accepts every collector");
    observe_dispatch_lane(DispatchLane::Create, 3, 1);
    let names = registry
        .gather()
        .into_iter()
        .map(|family| family.get_name().to_owned())
        .collect::<Vec<_>>();
    assert!(names.contains(&"novarocks_task_dispatch_lane_depth".to_owned()));
}

// -----------------------------------------------------------------------
// Acknowledgement bodies
// -----------------------------------------------------------------------

#[test]
fn an_admission_request_and_receipt_keep_the_exact_participation_validity_and_compatibility() {
    let backend = BackendProcessId::new_v7();
    let participation = participation(backend);
    let valid_for = LeaseValidFor::new(Duration::from_secs(10)).expect("a legal validity");
    let compatibility = novarocks_types::NativeCompatibilityId::new([0x71; 32]);
    let admission_epoch_capability =
        novarocks_execution::task_execution::AdmissionEpochCapability::try_from_bytes([0x61; 16])
            .expect("nonzero epoch");
    let request = AcquireParticipationAdmissionTicket::new(
        TaskOperationId::new_v7(),
        participation,
        valid_for,
        compatibility,
        admission_epoch_capability,
    );
    let encoded = encode_operation(
        &OperationIntent::AcquireParticipationAdmissionTicket(request),
        &test_attempt_facts(),
    )
    .expect("the admission request is encodable");
    assert_eq!(
        codec::wire_size::acquire(request).unwrap(),
        prost::Message::encoded_len(&encoded)
    );
    let Some(proto::task_operation::Operation::AcquireParticipationAdmissionTicket(encoded)) =
        encoded.operation
    else {
        panic!("admission uses its own batch operation");
    };
    assert_eq!(encoded.valid_for_millis, 10_000);
    assert_eq!(
        encoded
            .native_compatibility_id
            .expect("compatibility is required")
            .value,
        compatibility.as_bytes()
    );

    let ticket = AdmissionTicketId::try_from_bytes([0x57; 16]).expect("the ticket is nonzero");
    let receipt = proto::TaskOperationReceipt{operation_id:None,result:Some(proto::task_operation_receipt::Result::Verdict(proto::TaskOperationVerdict{outcome:0,safe_detail:String::new(),safe_field_path:None,ack:Some(
            proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(
                codec::encode_participation_admission_ticket_ack(
                    novarocks_execution::task_execution::ParticipationAdmissionTicketReceipt::new(
                        ticket,
                        participation,
                        valid_for,
                    ),
                ),
            ),
        )}))};
    let payload = decode_ack(
        OperationKind::AcquireParticipationAdmissionTicket,
        AckAddress::Admission(request),
        &receipt,
    )
    .expect("the exact receipt decodes");
    assert!(matches!(
        payload,
        AckPayload::AdmissionTicket(receipt) if receipt.ticket_id() == ticket
    ));
}

/// A create acknowledgement is the frontend's only proof that a task was
/// installed, and its status snapshot closes the window between creating a
/// task and observing it. These read the real wire bodies through the real
/// codec.
#[test]
fn native_domain_failure_reaches_the_original_ack_without_proving_task_acceptance() {
    use novarocks_execution_contract::failure::*;
    use novarocks_execution_contract::operation::{
        DomainFailureOutcome, OperationDomainFailure, OperationResult,
    };
    let fact = FailureFact::new(
        FailureClass::Representation,
        FailureReason::new(
            std::num::NonZeroU16::new(u16::MAX).unwrap(),
            std::num::NonZeroU16::new(65001).unwrap(),
        ),
    )
    .with_row_position(FailureRowPosition {
        selected_ordinal: 0,
        batch_row: 19,
    });
    let failure = OperationDomainFailure::new(
        fact,
        novarocks_execution_contract::SafeDetail::new("producer representation boundary").unwrap(),
    );
    let operation_id = TaskOperationId::new_v7();
    let wire = codec::encode_receipt(
        operation_id,
        OperationResult::DomainFailure {
            outcome: DomainFailureOutcome::InvalidStateOrRequest,
            failure: failure.clone(),
        },
    );
    let header = codec::decode_receipt_header(&wire, FieldPath::root("receipt")).unwrap();
    let item = SentOperation {
        operation_id,
        kind: OperationKind::CreateTask,
        establish_participation: None,
        lease_renewal: false,
        address: AckAddress::None,
    };
    let ack = acknowledgement(&item, &header, &wire);
    assert!(!ack.is_applied());
    assert!(matches!(ack.payload(), AckPayload::None));
    assert_eq!(
        ack.worker_outcome(),
        Some(OperationOutcome::InvalidStateOrRequest)
    );
    let OperationResult::DomainFailure {
        failure: carried, ..
    } = ack.worker_header().unwrap()
    else {
        panic!("domain branch")
    };
    assert_eq!(carried, failure);
}

#[test]
fn an_applied_create_acknowledgement_carries_the_task_status() {
    let backend = BackendProcessId::new_v7();
    let mine = identity(1, backend);
    let status = TaskStatus::created(mine);
    let ack = proto::task_operation_verdict::Ack::CreateTask(
        codec::encode_create_task_ack(&CreateTaskReceipt::new(mine, Vec::new(), status.clone()))
            .expect("an applied create encodes"),
    );
    let receipt = proto::TaskOperationReceipt {
        operation_id: None,
        result: Some(proto::task_operation_receipt::Result::Verdict(
            proto::TaskOperationVerdict {
                outcome: 0,
                safe_detail: String::new(),
                safe_field_path: None,
                ack: Some(ack.clone()),
            },
        )),
    };

    let payload = decode_ack(OperationKind::CreateTask, AckAddress::Task(mine), &receipt)
        .expect("its own acknowledgement decodes");
    let AckPayload::Create(decoded) = payload else {
        panic!("a create acknowledgement decodes as a create payload");
    };
    assert_eq!(decoded.identity(), mine);
    assert_eq!(decoded.current_status().state(), status.state());

    // The same body addressed elsewhere is not this request's proof.
    let theirs = identity(2, backend);
    assert!(
        decode_ack(
            OperationKind::CreateTask,
            AckAddress::Task(theirs),
            &receipt
        )
        .is_err(),
        "an acknowledgement for another task must not settle this one"
    );

    // An applied create with no body at all is a protocol violation, not
    // a create that installed nothing.
    let empty = {
        let mut value = receipt.clone();
        let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = &mut value.result
        else {
            panic!("fixture requires a protocol verdict")
        };
        verdict.ack = None;
        value
    };
    assert!(
        decode_ack(OperationKind::CreateTask, AckAddress::Task(mine), &empty).is_err(),
        "an applied create must carry its acknowledgement"
    );

    // A body of the wrong kind is refused rather than read as empty.
    assert!(
        decode_ack(OperationKind::UpdateTask, AckAddress::Task(mine), &receipt).is_err(),
        "a create body must not settle an update"
    );
}

#[tokio::test]
async fn an_applied_operation_whose_body_cannot_be_read_settles_as_a_refusal() {
    // The whole point of reading the body is that an applied operation the
    // frontend cannot interpret must not settle as applied-with-nothing.
    // A lease renewal consumes its participation body, so a create body here is
    // a producer bug the frontend must refuse rather than absorb.
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(backend, vec![renew_intent(backend)]);
    let task = identity(1, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(proto::task_operation_verdict::Ack::CreateTask(
                codec::encode_create_task_ack(&CreateTaskReceipt::new(
                    task,
                    Vec::new(),
                    TaskStatus::created(task),
                ))
                .expect("an applied create encodes"),
            )),
        )]));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 1).await;
    assert_eq!(
        acks[0].dispatch_result(),
        OperationDispatchResult::NonWorkerRejected
    );
    assert_eq!(*acks[0].payload(), AckPayload::None);
}

#[test]
fn owner_failure_never_consumes_a_success_or_closure_acknowledgement() {
    for kind in [
        OperationKind::AcquireParticipationAdmissionTicket,
        OperationKind::CreateTask,
        OperationKind::UpdateTask,
        OperationKind::UpdateParticipation,
        OperationKind::QuiesceParticipation,
        OperationKind::AbortParticipation,
        OperationKind::ReleaseParticipation,
    ] {
        assert!(!consumes_ack_body_for_outcome(
            kind,
            OperationOutcome::OwnerFailed
        ));
    }
    assert_eq!(outcome_name(OperationOutcome::OwnerFailed), "owner_failed");
}

#[test]
fn owner_failed_subscription_rejection_is_fatal_but_network_loss_can_resubscribe() {
    assert!(subscription_rejection_is_fatal(
        &tonic::Status::failed_precondition("the participation protocol owner failed",)
    ));
    assert!(!subscription_rejection_is_fatal(
        &tonic::Status::unavailable("connection closed",)
    ));
}

#[tokio::test]
async fn owner_failed_abort_keeps_its_exact_verdict_without_a_closure_body() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::OwnerFailed,
            None,
        )]));
    submit_batch(
        &fixture,
        released_batch(backend, vec![abort_intent(participation(backend))]),
    );
    let acks = settled(&fixture.acks, 1).await;
    assert_eq!(
        acks[0].worker_outcome(),
        Some(OperationOutcome::OwnerFailed)
    );
    assert_eq!(*acks[0].payload(), AckPayload::None);
    assert_eq!(
        crate::task_execution::operation::frontend_action(acks[0].dispatch_result()),
        crate::task_execution::operation::FrontendAction::FailAttempt,
    );
}

#[tokio::test]
async fn every_legal_abort_closing_outcome_keeps_its_participation_receipt() {
    for (outcome, state) in [
        (OperationOutcome::Accepted, ParticipationState::Aborting),
        (OperationOutcome::Idempotent, ParticipationState::Aborting),
        (
            OperationOutcome::ParticipationTerminalReceipt,
            ParticipationState::Releasing,
        ),
        (
            OperationOutcome::LeaseExpired,
            ParticipationState::TerminalRetained,
        ),
        (OperationOutcome::Gone, ParticipationState::Gone),
    ] {
        let backend = BackendProcessId::new_v7();
        let participation = participation(backend);
        let fixture = sink_fixture(Loopback::start().await, backend);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                outcome,
                Some(proto::task_operation_verdict::Ack::Participation(
                    encode_participation_ack(
                        &ParticipationReceipt::new(participation, state),
                        Some(AbortCause::QueryFailed),
                    )
                    .expect("a closing participation receipt encodes"),
                )),
            )]));
        submit_batch(
            &fixture,
            released_batch(backend, vec![abort_intent(participation)]),
        );
        let acks = settled(&fixture.acks, 1).await;
        assert_eq!(acks[0].worker_outcome(), Some(outcome));
        assert!(matches!(
            acks[0].payload(),
            AckPayload::Participation(receipt)
                if receipt.participation() == participation && receipt.state() == state
        ));
    }
}

#[tokio::test]
async fn a_legal_abort_closing_outcome_without_its_body_fails_closed() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::ParticipationTerminalReceipt,
            None,
        )]));
    submit_batch(
        &fixture,
        released_batch(backend, vec![abort_intent(participation(backend))]),
    );
    let acks = settled(&fixture.acks, 1).await;
    assert_eq!(
        acks[0].dispatch_result(),
        OperationDispatchResult::NonWorkerRejected
    );
    assert_eq!(*acks[0].payload(), AckPayload::None);
}

/// A cancel's status body is on the wire but has exactly one consumer, and
/// it is not this path. This pins that as a decision rather than an
/// oversight: an unread body must not turn an applied cancel into a
/// refusal.
#[tokio::test]
async fn a_cancel_settles_on_its_outcome_and_ignores_its_status_body() {
    let backend = BackendProcessId::new_v7();
    let fixture = sink_fixture(Loopback::start().await, backend);
    let batch = released_batch(backend, vec![cancel_intent(1, backend)]);
    let task = identity(1, backend);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(proto::task_operation_verdict::Ack::CancelTask(
                novarocks_task_codec::status::encode_task_status(&TaskStatus::created(task)),
            )),
        )]));

    submit_batch(&fixture, batch);
    let acks = settled(&fixture.acks, 1).await;
    assert_eq!(acks[0].worker_outcome(), Some(OperationOutcome::Accepted));
    assert_eq!(*acks[0].payload(), AckPayload::None);
}

#[path = "tests/original_root_connection.rs"]
mod original_root_connection;
#[path = "tests/three_task_recipe.rs"]
mod three_task_recipe;

#[path = "tests/participation_recipe.rs"]
mod participation_recipe;
