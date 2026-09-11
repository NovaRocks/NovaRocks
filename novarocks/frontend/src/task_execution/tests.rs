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

//! Deterministic coverage of the frontend task-protocol owners.
//!
//! Every test drives a fake operation sink and a manual clock. Nothing here
//! sleeps, polls, or reads a wall clock, so a failure is always a real
//! ordering or bound violation rather than a timing artefact.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use novarocks_execution::exec::fragment::program::{FragmentContractVersion, FragmentSinkKind};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AbortCause, AdmissionEpochCapability, AdmissionTicketId, CancelReason, CodecOwnedContent,
    ConfidentialContent, ContentFingerprint, CreateTaskReceipt, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, DomainVersion, DynamicFilterAdvertisement, EdgeOpenVersion, ExchangeEdgeId,
    LeaseReceipt, LeaseSequence, LeaseValidFor, OperationKind, OperationOutcome,
    PhysicalFragmentPlan, PlanNodeId, PlanNodeSplitReceipt, QueryContextAdmissionTicketReceipt,
    QueryContextReceipt, QueryContextRef, QueryContextState, ReleaseOutcome, SplitAssignmentIntent,
    SplitOffer, SplitSequence, SplitWatermark, TaskDomainReceipt, TaskDomainUpdate, TaskIdentity,
    TaskOperationId, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion, TerminationDetail,
    UpdateQueryContext, UpdateTaskReceipt,
};
use novarocks_query_application::coordination::{
    DispatchBudget, DispatchLane, MonotonicInstant, RenewSchedule, StageState,
};
use novarocks_sql::plan_read::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind, PartitionKind,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};
use novarocks_types::{AttemptId, NativeCompatibilityId, QueryId};

use super::blocking_io::{ConnectorBlockingIoBudget, ConnectorBlockingIoSupervisor};
use super::clock::{ManualClock, TaskProtocolClock};
use super::context_owner::{
    ContextEstablishFacts, ContextEstablishSource, QueryContextOwner, ReleaseSettlement,
};
use super::dispatch::OperationDispatcher;
use super::error::{CapacityBound, TaskExecutionError};
use super::execution::{AbortSubmission, QueryTaskExecution};
use super::graph::{
    FragmentPlanFacts, FragmentPlanSource, TaskGraph, TaskGraphInputs, build_task_graph,
};
use super::intent::{
    AckPayload, DispatchBatch, OPERATION_FIXED_BYTES, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationQueuePermit, TaskOperationQueueRequest,
    TaskOperationSink, TaskOperationSubmit, test_queue_permit,
};
use super::remote_task::{RemoteTaskState, UpdateAdmission};
use super::split_domain::{assignment_targets, delivery_action};
use super::status_intake::{
    CountingWake, StatusEvent, StatusIntake, StatusIntakeAdmission, StatusIntakeWake,
};
use crate::query_execution::FragmentInstancePlacement;
use crate::query_execution::artifact::fragment_instance_id_for_contract_test;
use crate::query_execution::schedule::SchedulingPlan;
use crate::query_execution::split_assignment::SplitAssignmentDriverError;

const LEAF_FRAGMENT: FragmentId = 1;
const MIDDLE_FRAGMENT: FragmentId = 2;
const ROOT_FRAGMENT: FragmentId = 3;
const LEAF_TO_MIDDLE_NODE: i32 = 20;
const MIDDLE_TO_ROOT_NODE: i32 = 30;
const LEAF_TO_ROOT_NODE: i32 = 31;
const SCAN_NODE: i32 = 5;

fn test_connector_blocking_io() -> ConnectorBlockingIoSupervisor {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(8)
            .enable_all()
            .build()
            .expect("test Connector blocking-I/O runtime")
    });
    ConnectorBlockingIoSupervisor::new(
        runtime.handle().clone(),
        ConnectorBlockingIoBudget::default(),
    )
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// Codec-owned content whose only observable facts are its fingerprint and
/// its size, which is exactly what the neutral contract exposes.
#[derive(Debug)]
struct FakeContent {
    fingerprint: ContentFingerprint,
    encoded_len: usize,
}

impl FakeContent {
    fn new(tag: u8, encoded_len: usize) -> Arc<Self> {
        Arc::new(Self {
            fingerprint: ContentFingerprint::from_bytes([tag; 16]),
            encoded_len,
        })
    }
}

impl CodecOwnedContent for FakeContent {
    fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }

    fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

#[derive(Debug)]
struct FakePlan {
    content: FakeContent,
}

impl FakePlan {
    fn new(tag: u8, encoded_len: usize) -> Arc<Self> {
        Arc::new(Self {
            content: FakeContent {
                fingerprint: ContentFingerprint::from_bytes([tag; 16]),
                encoded_len,
            },
        })
    }
}

impl CodecOwnedContent for FakePlan {
    fn fingerprint(&self) -> ContentFingerprint {
        self.content.fingerprint()
    }

    fn encoded_len(&self) -> usize {
        self.content.encoded_len()
    }
}

impl PhysicalFragmentPlan for FakePlan {
    fn contract_version(&self) -> FragmentContractVersion {
        FragmentContractVersion::new(1)
    }

    fn sink_kind(&self) -> FragmentSinkKind {
        FragmentSinkKind::DataStream
    }
}

struct FakeSecret;

impl ConfidentialContent for FakeSecret {
    fn encoded_len(&self) -> usize {
        32
    }

    fn matches(&self, _other: &dyn ConfidentialContent) -> bool {
        true
    }
}

#[derive(Debug)]
struct FakePlans {
    plan_bytes: usize,
}

impl FragmentPlanSource for FakePlans {
    fn plan_for(
        &self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError> {
        // Distinct per instance, like the real encoding: two instances of one
        // fragment do not share a plan handle.
        let seed = (fragment_id as u8).wrapping_mul(16) ^ (instance_index as u8);
        Ok(FragmentPlanFacts {
            plan: FakePlan::new(seed, self.plan_bytes),
            pipeline_dop: NonZeroUsize::new(2).expect("two is nonzero"),
        })
    }
}

#[derive(Debug)]
struct FakeEstablish;

impl ContextEstablishSource for FakeEstablish {
    fn facts_for(
        &self,
        _context: QueryContextRef,
    ) -> Result<ContextEstablishFacts, TaskExecutionError> {
        Ok(ContextEstablishFacts {
            catalog_binding: FakeContent::new(0xa1, 64),
            initial_runtime_filter: FakeContent::new(0xa2, 32),
            query_options: FakeContent::new(0xa3, 48),
            initial_credential: CredentialUpdate::new(
                CredentialLeaseId::new(7),
                CredentialEpoch::FIRST,
                Arc::new(FakeSecret),
            ),
        })
    }
}

fn grant_admission(owner: &mut QueryContextOwner, now: MonotonicInstant) {
    let intent = owner
        .admission_intent(now)
        .expect("an admission request can be built")
        .expect("the admission request is released");
    let OperationIntent::AcquireQueryContextAdmissionTicket(request) = intent else {
        unreachable!("admission releases its exact request");
    };
    owner
        .on_admission_ack(
            &OperationAcknowledgement::worker_receipt(
                request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
                OperationOutcome::Accepted,
                AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                    AdmissionTicketId::try_from_bytes([0x54; 16])
                        .expect("the test ticket is nonzero"),
                    request.context(),
                    request.valid_for(),
                )),
            ),
            now,
        )
        .expect("the admission request settles");
}

/// Records what the dispatcher released, and nothing else.
#[derive(Debug, Default)]
struct RecordingSink {
    batches: Mutex<Vec<(DispatchLane, Vec<OperationIntent>)>>,
}

impl RecordingSink {
    fn take(&self) -> Vec<(DispatchLane, Vec<OperationIntent>)> {
        std::mem::take(&mut *self.batches.lock().expect("recording sink"))
    }
}

impl TaskOperationSink for RecordingSink {
    fn try_reserve_queue(
        &self,
        _request: TaskOperationQueueRequest,
    ) -> TaskOperationQueueAdmission {
        TaskOperationQueueAdmission::Admitted(test_queue_permit())
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        let lane = batch.lane();
        let operations = batch.into_operations();
        self.batches
            .lock()
            .expect("recording sink")
            .push((lane, operations));
        TaskOperationSubmit::Accepted
    }
}

#[derive(Debug, Default)]
struct BackpressureOnceSink {
    attempts: AtomicUsize,
    refused: Mutex<Vec<TaskOperationId>>,
    accepted: Mutex<Vec<Vec<TaskOperationId>>>,
}

#[derive(Debug, Default)]
struct QueueBackpressureOnceSink {
    reserve_attempts: AtomicUsize,
    submitted_operations: AtomicUsize,
}

#[derive(Debug)]
struct SwitchableQueueSink {
    queue_open: std::sync::atomic::AtomicBool,
    submit_open: std::sync::atomic::AtomicBool,
    batches: Mutex<Vec<(DispatchLane, Vec<OperationIntent>)>>,
    live_queue_permits: Arc<AtomicUsize>,
}

impl Default for SwitchableQueueSink {
    fn default() -> Self {
        Self {
            queue_open: std::sync::atomic::AtomicBool::new(true),
            submit_open: std::sync::atomic::AtomicBool::new(true),
            batches: Mutex::new(Vec::new()),
            live_queue_permits: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Debug)]
struct SwitchableQueuePermit {
    live: Arc<AtomicUsize>,
}

impl Drop for SwitchableQueuePermit {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl TaskOperationQueuePermit for SwitchableQueuePermit {
    fn mark_in_flight(&mut self) {}
}

impl SwitchableQueueSink {
    fn set_queue_open(&self, open: bool) {
        self.queue_open.store(open, Ordering::SeqCst);
    }

    fn set_submit_open(&self, open: bool) {
        self.submit_open.store(open, Ordering::SeqCst);
    }

    fn take(&self) -> Vec<(DispatchLane, Vec<OperationIntent>)> {
        std::mem::take(&mut *self.batches.lock().expect("switchable sink"))
    }

    fn live_queue_permits(&self) -> usize {
        self.live_queue_permits.load(Ordering::SeqCst)
    }
}

impl TaskOperationSink for SwitchableQueueSink {
    fn try_reserve_queue(
        &self,
        _request: TaskOperationQueueRequest,
    ) -> TaskOperationQueueAdmission {
        if self.queue_open.load(Ordering::SeqCst) {
            self.live_queue_permits.fetch_add(1, Ordering::SeqCst);
            TaskOperationQueueAdmission::Admitted(Box::new(SwitchableQueuePermit {
                live: Arc::clone(&self.live_queue_permits),
            }))
        } else {
            TaskOperationQueueAdmission::Backpressured
        }
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        if !self.submit_open.load(Ordering::SeqCst) {
            return TaskOperationSubmit::Backpressured(batch);
        }
        let lane = batch.lane();
        let operations = batch.into_operations();
        self.batches
            .lock()
            .expect("switchable sink")
            .push((lane, operations));
        TaskOperationSubmit::Accepted
    }
}

impl TaskOperationSink for QueueBackpressureOnceSink {
    fn try_reserve_queue(
        &self,
        _request: TaskOperationQueueRequest,
    ) -> TaskOperationQueueAdmission {
        if self.reserve_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            TaskOperationQueueAdmission::Backpressured
        } else {
            TaskOperationQueueAdmission::Admitted(test_queue_permit())
        }
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        self.submitted_operations
            .fetch_add(batch.operations().len(), Ordering::SeqCst);
        TaskOperationSubmit::Accepted
    }
}

impl TaskOperationSink for BackpressureOnceSink {
    fn try_reserve_queue(
        &self,
        _request: TaskOperationQueueRequest,
    ) -> TaskOperationQueueAdmission {
        TaskOperationQueueAdmission::Admitted(test_queue_permit())
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        let ids = batch
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>();
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            *self.refused.lock().expect("refused batch") = ids;
            TaskOperationSubmit::Backpressured(batch)
        } else {
            self.accepted.lock().expect("accepted batches").push(ids);
            TaskOperationSubmit::Accepted
        }
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// One available runtime-filter contribution, shaped as a backend's release
/// acknowledgement carries it.
fn fixture_runtime_filter_contribution()
-> novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry {
    use novarocks_proto_models::novarocks as wire;
    novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry::parse(
        wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                wire::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                    wire::QueryTerminalProfileContributionV1 {
                        version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                        channels: vec![wire::QueryTerminalRuntimeFilterChannelV1 {
                            channel_binding_id: 1,
                            channel_id: 7,
                            install_state:
                                wire::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                                    as i32,
                            terminal_state:
                                wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                                    as i32,
                            latest_published_logical_version: Some(3),
                            published_count: 1,
                            completed_count: 1,
                            unavailable_count: 0,
                            cancelled_count: 0,
                        }],
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .expect("the fixture contribution satisfies the terminal contract")
}

fn execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(0x1234, 0x5678),
        AttemptId::new(1).expect("attempt one is nonzero"),
    )
    .expect("a nonzero query id")
}

fn endpoint(index: usize) -> RuntimeEndpoint {
    RuntimeEndpoint::new("127.0.0.1", 9000 + index as i32).expect("a valid endpoint")
}

fn backends(count: usize) -> BTreeMap<usize, BackendProcessId> {
    (0..count)
        .map(|index| (index, BackendProcessId::new_v7()))
        .collect()
}

fn admission_epoch() -> AdmissionEpochCapability {
    AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("nonzero epoch")
}

/// One fragment's placements, taking the kernel key from the same derivation
/// the production schedule uses.
fn placements(
    fragment_id: FragmentId,
    backend_indexes: &[usize],
    scan_nodes: &[i32],
) -> Vec<FragmentInstancePlacement> {
    backend_indexes
        .iter()
        .enumerate()
        .map(|(instance_index, &backend_idx)| FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id: fragment_instance_id_for_contract_test(
                execution_id().query_id(),
                fragment_id,
                instance_index,
            ),
            backend_idx,
            endpoint: endpoint(backend_idx),
            scan_ranges: scan_nodes
                .iter()
                .map(|&node_id| (node_id, Vec::new()))
                .collect(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        })
        .collect()
}

fn stream_edge(source: FragmentId, target: FragmentId, node_id: i32) -> FragmentEdge {
    FragmentEdge {
        source_fragment_id: source,
        target_fragment_id: target,
        target_exchange_node_id: node_id,
        output_partition: DataPartition {
            kind: PartitionKind::Hash,
            exprs: Vec::new(),
        },
        stream_kind: FragmentStreamKind::Partitioned,
        edge_kind: FragmentEdgeKind::Stream,
        output_slot_ids: Vec::new(),
    }
}

/// A three-stage chain: leaves feed a middle stage which feeds the root.
fn chain_schedule(leaf_backends: &[usize], middle_backends: &[usize]) -> SchedulingPlan {
    let mut by_fragment = BTreeMap::new();
    by_fragment.insert(
        LEAF_FRAGMENT,
        placements(LEAF_FRAGMENT, leaf_backends, &[SCAN_NODE]),
    );
    by_fragment.insert(
        MIDDLE_FRAGMENT,
        placements(MIDDLE_FRAGMENT, middle_backends, &[]),
    );
    by_fragment.insert(ROOT_FRAGMENT, placements(ROOT_FRAGMENT, &[0], &[]));
    let root = by_fragment[&ROOT_FRAGMENT][0].clone();
    SchedulingPlan {
        root_fragment_id: ROOT_FRAGMENT,
        by_fragment,
        root_finst_id: root.finst_id,
        root_backend_idx: root.backend_idx,
    }
}

fn chain_edges() -> Vec<FragmentEdge> {
    vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
    ]
}

/// The chain plus a second consumer of the leaf stage's output, so every leaf
/// task is a producer on two exchange edges.
///
/// This is the shape a CTE consumed twice schedules, and it is what any plan
/// that reuses one fragment's output through a second exchange node produces.
fn multicast_edges() -> Vec<FragmentEdge> {
    vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
    ]
}

#[test]
fn every_instance_of_one_fragment_gets_its_own_plan_handle() {
    // The encoded plan carries this instance's own parameters, and the backend
    // refuses a descriptor whose plan names a different instance. One handle
    // shared across a fragment's instances would therefore be rejected for
    // every instance but one -- which on the 1FE+3BE baseline is every
    // non-root fragment.
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[0, 1]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");

    let mut fingerprints = BTreeMap::<TaskId, ContentFingerprint>::new();
    for task in graph.tasks() {
        let descriptor = graph
            .descriptor(task.task_id())
            .expect("a built graph owns every descriptor");
        fingerprints.insert(task.task_id(), descriptor.plan().fingerprint());
    }
    assert_eq!(fingerprints.len(), 6, "three leaves, two middles, one root");

    let distinct: BTreeSet<ContentFingerprint> = fingerprints.values().copied().collect();
    assert_eq!(
        distinct.len(),
        fingerprints.len(),
        "two instances of one fragment must not share a plan handle"
    );
}

fn build_graph(
    schedule: &SchedulingPlan,
    edges: &[FragmentEdge],
    processes: &BTreeMap<usize, BackendProcessId>,
    plan_bytes: usize,
) -> Result<TaskGraph, TaskExecutionError> {
    build_task_graph(
        TaskGraphInputs::from_schedule(
            execution_id(),
            FrontendProcessId::new_v7(),
            schedule,
            edges,
            processes,
            TransportBudget::DEFAULT,
        ),
        &FakePlans { plan_bytes },
    )
}

struct Harness {
    execution: QueryTaskExecution,
    sink: Arc<RecordingSink>,
    clock: Arc<ManualClock>,
    wake: Arc<CountingWake>,
    establish: FakeEstablish,
    statuses: BTreeMap<TaskId, u64>,
}

impl Harness {
    fn new(leaf_backends: &[usize], middle_backends: &[usize], plan_bytes: usize) -> Self {
        let processes = backends(
            leaf_backends
                .iter()
                .chain(middle_backends)
                .copied()
                .max()
                .unwrap_or_default()
                + 1,
        );
        let schedule = chain_schedule(leaf_backends, middle_backends);
        let graph = build_graph(&schedule, &chain_edges(), &processes, plan_bytes)
            .expect("the chain schedule is a legal task graph");
        Self::from_graph(graph)
    }

    fn from_graph(graph: TaskGraph) -> Self {
        let sink = Arc::new(RecordingSink::default());
        let clock = Arc::new(ManualClock::new());
        let wake = Arc::new(CountingWake::default());
        let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
        let admission_epochs = graph
            .contexts()
            .map(|context| (context.backend_process_id(), admission_epoch()))
            .collect::<BTreeMap<_, _>>();
        let execution = QueryTaskExecution::new(
            graph,
            DispatchBudget::DEFAULT,
            TransportBudget::DEFAULT,
            NativeCompatibilityId::new([0x41; 32]),
            &admission_epochs,
            Arc::clone(&clock) as Arc<dyn TaskProtocolClock>,
            Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
            intake,
        )
        .expect("the graph composes into a task execution");
        Self {
            execution,
            sink,
            clock,
            wake,
            establish: FakeEstablish,
            statuses: BTreeMap::new(),
        }
    }

    fn pump(&mut self) -> Vec<(DispatchLane, Vec<OperationIntent>)> {
        let mut visible = Vec::new();
        loop {
            let batches = self.pump_once();
            let mut acquired = false;
            for (lane, operations) in batches {
                let mut remaining = Vec::new();
                for intent in operations {
                    if let OperationIntent::AcquireQueryContextAdmissionTicket(request) = intent {
                        acquired = true;
                        let ticket_id = AdmissionTicketId::try_from_bytes([0x54; 16])
                            .expect("the test ticket is nonzero");
                        self.execution
                            .acknowledge(&OperationAcknowledgement::worker_receipt(
                                request.envelope().operation_id(),
                                OperationKind::AcquireQueryContextAdmissionTicket,
                                OperationOutcome::Accepted,
                                AckPayload::AdmissionTicket(
                                    QueryContextAdmissionTicketReceipt::new(
                                        ticket_id,
                                        request.context(),
                                        request.valid_for(),
                                    ),
                                ),
                            ))
                            .expect("an admission acknowledgement settles");
                    } else {
                        remaining.push(intent);
                    }
                }
                if !remaining.is_empty() {
                    visible.push((lane, remaining));
                }
            }
            if !acquired {
                return visible;
            }
        }
    }

    fn pump_once(&mut self) -> Vec<(DispatchLane, Vec<OperationIntent>)> {
        self.execution
            .pump(&self.establish)
            .expect("pumping produces intents");
        self.sink.take()
    }

    fn released(&mut self) -> Vec<OperationIntent> {
        self.pump()
            .into_iter()
            .flat_map(|(_, operations)| operations)
            .collect()
    }

    /// Answers one context operation with a lease of `effective`.
    ///
    /// The establish and the creates of an attempt are released in the same
    /// rotation on purpose: a create may legitimately reach a backend before
    /// its establish and wait on the creation gate there.
    fn context_ack(&mut self, intent: &OperationIntent, effective: Duration) {
        let OperationIntent::UpdateQueryContext(request) = intent else {
            unreachable!("an update-context intent carries its request");
        };
        let sequence = self
            .execution
            .owner(request.context())
            .expect("the context has an owner")
            .lease_sequence();
        let sequence = if request.may_create() {
            LeaseSequence::INITIAL
        } else {
            sequence.next().expect("the lease sequence advances")
        };
        let receipt = QueryContextReceipt::new(request.context(), QueryContextState::Active)
            .with_lease(LeaseReceipt::new(
                sequence,
                LeaseValidFor::new(if request.may_create() {
                    Duration::from_secs(30)
                } else {
                    Duration::from_secs(5)
                })
                .expect("a legal request"),
                effective,
            ));
        self.execution
            .acknowledge(&OperationAcknowledgement::new(
                intent.operation_id(),
                OperationKind::UpdateQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Context(receipt),
            ))
            .expect("a context acknowledgement settles");
    }

    /// Answers every establish one pump released, and returns everything else
    /// that pump released.
    fn establish_all(&mut self, effective: Duration) -> Vec<OperationIntent> {
        let released = self.released();
        let mut rest = Vec::new();
        for intent in released {
            if matches!(intent.kind(), OperationKind::UpdateQueryContext) {
                self.context_ack(&intent, effective);
            } else {
                rest.push(intent);
            }
        }
        rest
    }

    fn create_ack(
        &mut self,
        intent: &OperationIntent,
        outcome: OperationOutcome,
    ) -> Result<(), TaskExecutionError> {
        let OperationIntent::CreateTask(request) = intent else {
            unreachable!("a create intent carries its request");
        };
        let payload = if matches!(
            outcome,
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        ) {
            AckPayload::Create(CreateTaskReceipt::new(
                request.identity(),
                Vec::new(),
                TaskStatus::created(request.identity()),
            ))
        } else {
            AckPayload::None
        };
        self.statuses.insert(request.identity().task_id(), 1);
        self.execution.acknowledge(&OperationAcknowledgement::new(
            intent.operation_id(),
            OperationKind::CreateTask,
            outcome,
            payload,
        ))
    }

    fn update_ack(
        &mut self,
        intent: &OperationIntent,
        outcome: OperationOutcome,
    ) -> Result<(), TaskExecutionError> {
        let OperationIntent::UpdateTask(request) = intent else {
            unreachable!("an update intent carries its request");
        };
        let payload = if matches!(
            outcome,
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        ) {
            AckPayload::Update(UpdateTaskReceipt::new(
                request.identity(),
                request.domains().iter().map(task_domain_receipt).collect(),
            ))
        } else {
            AckPayload::None
        };
        self.execution.acknowledge(&OperationAcknowledgement::new(
            intent.operation_id(),
            OperationKind::UpdateTask,
            outcome,
            payload,
        ))
    }

    fn cancel_ack(&mut self, intent: &OperationIntent) {
        self.execution
            .acknowledge(&OperationAcknowledgement::new(
                intent.operation_id(),
                OperationKind::CancelTask,
                OperationOutcome::Accepted,
                AckPayload::None,
            ))
            .expect("a cancel acknowledgement settles");
    }

    fn transport_unknown_ack(
        &mut self,
        intent: &OperationIntent,
    ) -> Result<(), TaskExecutionError> {
        self.execution
            .acknowledge(&OperationAcknowledgement::transport_unknown(
                intent.operation_id(),
                intent.kind(),
            ))
    }

    /// Answers everything one pump released, and reports what it answered.
    fn settle_round(&mut self, effective: Duration) -> Vec<OperationIntent> {
        let released = self.released();
        for intent in &released {
            match intent.kind() {
                OperationKind::CreateTask => {
                    self.create_ack(intent, OperationOutcome::Accepted)
                        .expect("a create acknowledgement settles");
                }
                OperationKind::UpdateTask => {
                    self.update_ack(intent, OperationOutcome::Accepted)
                        .expect("an update acknowledgement settles");
                }
                OperationKind::UpdateQueryContext => self.context_ack(intent, effective),
                OperationKind::CancelTask => self.cancel_ack(intent),
                kind => unreachable!("this fixture does not release {kind}"),
            }
        }
        released
    }

    /// Drives the attempt until nothing more is released.
    fn settle_until_quiet(&mut self, effective: Duration) -> Vec<OperationIntent> {
        let mut all = Vec::new();
        loop {
            let released = self.settle_round(effective);
            if released.is_empty() {
                return all;
            }
            all.extend(released);
        }
    }

    fn identity(&self, task_id: TaskId) -> TaskIdentity {
        self.execution
            .graph()
            .task(task_id)
            .expect("the task is in the graph")
            .identity()
    }

    /// Publishes one status snapshot through the intake and applies it.
    fn publish(
        &mut self,
        task_id: TaskId,
        state: TaskState,
        termination: Option<TerminationDetail>,
        output_complete: bool,
    ) {
        self.publish_with_filters(task_id, state, termination, output_complete, None);
    }

    /// Advances this task's status version counter without publishing, the way
    /// a subscription's catch-up leaves a gap: it replays only the latest
    /// version per cursor, so every version in between is never delivered.
    fn skip_status_versions(&mut self, task_id: TaskId, count: u64) {
        *self.statuses.entry(task_id).or_insert(1) += count;
    }

    /// Publishes one status snapshot, optionally advertising a filter version.
    fn publish_with_filters(
        &mut self,
        task_id: TaskId,
        state: TaskState,
        termination: Option<TerminationDetail>,
        output_complete: bool,
        filters: Option<DynamicFilterAdvertisement>,
    ) {
        let identity = self.identity(task_id);
        let version = self.statuses.entry(task_id).or_insert(1);
        *version += 1;
        let mut status = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(*version).expect("a nonzero version"),
            state,
            termination,
            if output_complete {
                TaskOutputFacts::new(true)
            } else {
                TaskOutputFacts::default()
            },
        )
        .expect("a legal status snapshot");
        if let Some(filters) = filters {
            status = status.with_dynamic_filters(filters);
        }
        assert_eq!(
            self.execution
                .intake()
                .handle()
                .publish(StatusEvent::Published(status)),
            StatusIntakeAdmission::Enqueued
        );
        self.execution
            .apply_status(64)
            .expect("applying intake succeeds");
    }

    fn stage_tasks(&self, stage_id: u32) -> Vec<TaskId> {
        self.execution
            .graph()
            .stage(StageId::new(stage_id).expect("a nonzero stage id"))
            .expect("the stage is in the graph")
            .tasks()
            .to_vec()
    }

    fn stage_state(&self, stage_id: u32) -> StageState {
        self.execution
            .stage(StageId::new(stage_id).expect("a nonzero stage id"))
            .expect("the stage is owned")
            .state()
    }
}

fn task_domain_receipt(update: &TaskDomainUpdate) -> TaskDomainReceipt {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = match intent.offer() {
                SplitOffer::Batch { last, no_more, .. } => {
                    SplitWatermark::empty().apply_batch(last, no_more)
                }
                SplitOffer::Seal => SplitWatermark::empty().apply_no_more(),
            };
            TaskDomainReceipt::SplitAssignment {
                nodes: vec![PlanNodeSplitReceipt::new(intent.node(), watermark)],
                progression: novarocks_execution::task_execution::DomainProgression::Apply,
            }
        }
        TaskDomainUpdate::TaskDynamicFilter { version, .. } => {
            TaskDomainReceipt::TaskDynamicFilter {
                accepted_version: Some(*version),
                progression: novarocks_execution::task_execution::DomainProgression::Apply,
            }
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
            TaskDomainReceipt::OpenExchangeEdges {
                accepted_version: *version,
                opened: edges.clone(),
                progression: novarocks_execution::task_execution::DomainProgression::Apply,
            }
        }
    }
}

fn split_update(node: i32, sequence: u64, no_more: bool) -> TaskDomainUpdate {
    let sequence = SplitSequence::new(sequence).expect("a nonzero split sequence");
    TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
        PlanNodeId::new(node).expect("a nonnegative plan node"),
        SplitOffer::batch(sequence, sequence, no_more).expect("a contiguous split batch"),
        FakeContent::new(0xb1, 128),
    ))
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

#[test]
fn stage_and_task_ids_follow_the_static_schedule_and_reuse_its_kernel_keys() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1, 2]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("a legal graph");

    // One stage per fragment, in the schedule's own ascending fragment order.
    let stages = graph
        .stages()
        .map(|stage| (stage.stage_id().get(), stage.fragment_id()))
        .collect::<Vec<_>>();
    assert_eq!(
        stages,
        vec![(1, LEAF_FRAGMENT), (2, MIDDLE_FRAGMENT), (3, ROOT_FRAGMENT)]
    );

    // Task ids come from one counter over the attempt and are never reused.
    let ids = graph
        .tasks()
        .map(|task| task.task_id().get())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    // Every kernel key is the schedule's own frozen value, not a second
    // derivation.
    for task in graph.tasks() {
        let expected = fragment_instance_id_for_contract_test(
            execution_id().query_id(),
            task.fragment_id(),
            task.instance_index(),
        );
        assert_eq!(task.fragment_instance_id(), expected);
        let descriptor = graph
            .descriptor(task.task_id())
            .expect("the graph still owns its descriptors");
        assert_eq!(descriptor.fragment_instance_id(), expected);
    }
}

#[test]
fn every_destination_and_source_carries_both_addresses() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1, 2]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("a legal graph");

    let key_of = |identity: TaskIdentity| {
        graph
            .task(identity.task_id())
            .expect("the destination is a graph task")
            .fragment_instance_id()
    };

    for task in graph.tasks() {
        let descriptor = graph
            .descriptor(task.task_id())
            .expect("the graph still owns its descriptors");
        for edge in descriptor.topology().outbound() {
            assert!(!edge.destinations().is_empty());
            for destination in edge.destinations() {
                assert_eq!(
                    destination.fragment_instance_id(),
                    key_of(destination.task())
                );
                assert_eq!(
                    destination.destination_node_id(),
                    edge.destination_node_id()
                );
            }
        }
        for inbound in descriptor.topology().inbound() {
            for source in inbound.sources() {
                assert_eq!(source.fragment_instance_id(), key_of(source.task()));
            }
            // The producer's sender count and the consumer's expectation are
            // derived once per exchange node, so they cannot disagree.
            let expected = inbound.expected_sender_count();
            for producer in inbound.sources() {
                let producer_descriptor = graph
                    .descriptor(producer.task().task_id())
                    .expect("the producer is a graph task");
                let edge = producer_descriptor
                    .topology()
                    .outbound()
                    .iter()
                    .find(|edge| edge.destination_node_id() == inbound.node_id())
                    .expect("the producer has an edge into this node");
                assert!(
                    edge.destinations()
                        .iter()
                        .all(|destination| destination.sender_count() == expected)
                );
            }
        }
    }
}

#[test]
fn a_repeated_kernel_key_is_refused() {
    let processes = backends(2);
    let mut schedule = chain_schedule(&[0, 1], &[1]);
    let leaf = schedule
        .by_fragment
        .get_mut(&LEAF_FRAGMENT)
        .expect("the leaf fragment is placed");
    leaf[1].finst_id = leaf[0].finst_id;

    let error = build_graph(&schedule, &chain_edges(), &processes, 512)
        .expect_err("two tasks may not share one kernel key");
    assert!(matches!(
        error,
        TaskExecutionError::KernelKeyCollision { .. }
    ));
}

#[test]
fn a_scheduled_backend_without_a_frozen_process_identity_is_refused() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let error = build_graph(&schedule, &chain_edges(), &processes, 512)
        .expect_err("a placement needs a frozen backend process identity");
    assert!(matches!(
        error,
        TaskExecutionError::UnknownBackend { backend_idx: 1 }
    ));
}

// ---------------------------------------------------------------------------
// Create, edge open, and the update queue
// ---------------------------------------------------------------------------

#[test]
fn an_edge_stays_closed_until_its_producer_is_created() {
    let mut harness = Harness::new(&[0, 1], &[1], 512);
    let creates = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .collect::<Vec<_>>();
    assert_eq!(creates.len(), 4);

    let middle = harness.stage_tasks(2)[0];
    let leaves = harness.stage_tasks(1);
    let middle_identity = harness.identity(middle);

    // Acknowledge only the destination of the leaf-to-middle edge. Its
    // producers are still creating.
    let destination_create = creates
        .iter()
        .find(|intent| match intent {
            OperationIntent::CreateTask(request) => request.identity() == middle_identity,
            _ => false,
        })
        .expect("the middle task has a create")
        .clone();
    harness
        .create_ack(&destination_create, OperationOutcome::Accepted)
        .expect("the destination create settles");

    for &leaf in &leaves {
        let task = harness.execution.task(leaf).expect("the leaf is owned");
        assert_eq!(task.state(), RemoteTaskState::Creating);
        assert_eq!(
            task.pending_updates(),
            1,
            "the edge-open decision is recorded while the producer is still creating"
        );
    }

    // The decision exists, but nothing may leave the frontend for a producer
    // whose own create has not been acknowledged.
    let released = harness.released();
    assert!(
        released
            .iter()
            .all(|intent| !matches!(intent.kind(), OperationKind::UpdateTask)),
        "an edge open may not precede its producer's own acknowledgement"
    );

    // Acknowledge one producer; only that producer's edge open goes out.
    let leaf_identity = harness.identity(leaves[0]);
    let producer_create = creates
        .iter()
        .find(|intent| match intent {
            OperationIntent::CreateTask(request) => request.identity() == leaf_identity,
            _ => false,
        })
        .expect("the leaf task has a create")
        .clone();
    harness
        .create_ack(&producer_create, OperationOutcome::Accepted)
        .expect("the producer create settles");

    let updates = harness
        .released()
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::UpdateTask))
        .collect::<Vec<_>>();
    assert_eq!(updates.len(), 1);
    let OperationIntent::UpdateTask(request) = &updates[0] else {
        unreachable!("an update intent carries its request");
    };
    assert_eq!(request.identity(), leaf_identity);
    assert!(matches!(
        request.domains().first(),
        Some(TaskDomainUpdate::OpenExchangeEdges { .. })
    ));
}

#[test]
fn a_producer_of_two_edges_opens_each_edge_at_its_own_version() {
    // The defect this catches: every edge-open decision was minted at version
    // one, while one version may only ever name one exact edge set. A producer
    // that feeds two exchange nodes has its edges decided separately, so its
    // second decision replayed version one with a different edge set --
    // `SameTokenDifferentContent`, refused where it is produced. The
    // consequence was that every multi-cast query failed the moment its second
    // edge was decided: the whole `cte` suite, and any plan that consumes one
    // fragment's output twice.
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[0, 1, 2]);
    let graph = build_graph(&schedule, &multicast_edges(), &processes, 512)
        .expect("a multi-cast schedule is a legal task graph");
    let mut harness = Harness::from_graph(graph);
    let leaves = harness.stage_tasks(1);

    let mut opens = BTreeMap::<TaskId, Vec<(EdgeOpenVersion, Vec<ExchangeEdgeId>)>>::new();
    loop {
        let released = harness.released();
        if released.is_empty() {
            break;
        }
        for intent in &released {
            match intent.kind() {
                OperationKind::UpdateQueryContext => {
                    harness.context_ack(intent, Duration::from_secs(10));
                }
                OperationKind::CreateTask => harness
                    .create_ack(intent, OperationOutcome::Accepted)
                    .expect("a create settles and its edge-open decisions are admitted"),
                OperationKind::UpdateTask => {
                    let OperationIntent::UpdateTask(request) = intent else {
                        unreachable!("an update intent carries its request");
                    };
                    if let Some(TaskDomainUpdate::OpenExchangeEdges { version, edges }) =
                        request.domains().first()
                    {
                        opens
                            .entry(request.identity().task_id())
                            .or_default()
                            .push((*version, edges.clone()));
                    }
                    harness
                        .update_ack(intent, OperationOutcome::Accepted)
                        .expect("an edge open settles");
                }
                kind => unreachable!("this fixture does not release {kind}"),
            }
        }
    }

    for &leaf in &leaves {
        let decisions = opens
            .get(&leaf)
            .expect("every leaf task produces on both of its edges");
        assert_eq!(
            decisions.len(),
            2,
            "a leaf feeding two exchange nodes opens two edges"
        );
        let versions: BTreeSet<EdgeOpenVersion> =
            decisions.iter().map(|(version, _)| *version).collect();
        assert_eq!(
            versions.len(),
            2,
            "one version may not name two different edge sets"
        );
        let edges: BTreeSet<ExchangeEdgeId> = decisions
            .iter()
            .flat_map(|(_, edges)| edges.iter().copied())
            .collect();
        assert_eq!(
            edges.len(),
            2,
            "each decision names exactly the edge it opened"
        );
    }

    for task in harness.execution.graph().tasks().collect::<Vec<_>>() {
        assert_eq!(
            harness
                .execution
                .task(task.task_id())
                .expect("the task is owned")
                .state(),
            RemoteTaskState::Created,
            "a multi-cast attempt must reach a fully created schedule"
        );
    }
}

#[test]
fn no_update_is_released_while_a_create_outcome_is_unknown() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let creates = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .collect::<Vec<_>>();
    let leaf = harness.stage_tasks(1)[0];
    let leaf_create = creates
        .iter()
        .find(|intent| match intent {
            OperationIntent::CreateTask(request) => request.identity() == harness.identity(leaf),
            _ => false,
        })
        .expect("the leaf has a create")
        .clone();

    // A split assignment arrives while the create's outcome is unknown.
    harness
        .transport_unknown_ack(&leaf_create)
        .expect("an unknown outcome settles without failing");
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("a split assignment is recorded");

    let released = harness.released();
    assert!(
        released
            .iter()
            .any(|intent| matches!(intent.kind(), OperationKind::CreateTask)),
        "the create is retried"
    );
    assert!(
        released
            .iter()
            .all(|intent| !matches!(intent.kind(), OperationKind::UpdateTask)),
        "no update may go out concurrently with an unknown-outcome create"
    );
    assert_eq!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .pending_updates(),
        1
    );
}

#[test]
fn an_unknown_create_outcome_retries_the_identical_request() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let first = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .expect("a create was released");
    harness
        .transport_unknown_ack(&first)
        .expect("an unknown outcome settles without failing");

    let second = harness
        .released()
        .into_iter()
        .find(|intent| intent.operation_id() == first.operation_id())
        .expect("the identical request is released again");
    let (OperationIntent::CreateTask(first), OperationIntent::CreateTask(second)) =
        (&first, &second)
    else {
        unreachable!("both intents are creates");
    };
    assert!(
        Arc::ptr_eq(first, second),
        "a retry must resend the identical immutable request, not a rebuilt one"
    );
    assert_eq!(first.descriptor(), second.descriptor());
}

#[test]
fn a_domain_regression_is_refused_where_it_is_produced() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let leaf = harness.stage_tasks(1)[0];
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("the first split batch is recorded");
    // An offer that adds nothing is settled, not refused. ADR-0123 keeps only
    // the watermark, so an exact replay and a sequence reused for different
    // content are indistinguishable here by construction -- and the replay is
    // the recovery path for an unknown outcome, so refusing it would make this
    // owner reject its own retry.
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("a re-offer of an applied range adds nothing and is settled");

    // What stays verifiable is a producer that skips ahead: the watermark
    // knows the next legal sequence, so a gap is a real regression and is
    // refused where it is produced.
    let error = harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 5, false))
        .expect_err("a sequence past the next expected one is not a progression");
    assert!(matches!(
        error,
        TaskExecutionError::DomainRegression {
            domain: "split_assignment",
            ..
        }
    ));
}

#[test]
fn a_split_for_a_plan_node_the_descriptor_does_not_own_is_refused() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let middle = harness.stage_tasks(2)[0];
    let error = harness
        .execution
        .enqueue_task_update(middle, split_update(SCAN_NODE, 1, false))
        .expect_err("a task with no scan node accepts no split");
    assert!(matches!(
        error,
        TaskExecutionError::DomainRegression {
            domain: "split_assignment",
            ..
        }
    ));
}

#[test]
fn a_terminal_task_discards_never_sent_updates_and_converges_what_is_in_flight() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaf = harness.stage_tasks(1)[0];
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("the first split batch is recorded");
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 2, true))
        .expect("the second split batch is recorded");

    let in_flight = harness
        .released()
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::UpdateTask))
        .expect("one update is released");

    harness.publish(leaf, TaskState::Running, None, false);
    harness.publish(
        leaf,
        TaskState::Failing,
        Some(TerminationDetail::Failed(
            novarocks_execution::task_execution::TaskFailure::new(
                novarocks_execution::task_execution::TaskFailureCategory::Execution,
                novarocks_execution::task_execution::SafeDetail::truncating("scan failed"),
            ),
        )),
        false,
    );
    harness.publish(
        leaf,
        TaskState::Failed,
        Some(TerminationDetail::Failed(
            novarocks_execution::task_execution::TaskFailure::new(
                novarocks_execution::task_execution::TaskFailureCategory::Execution,
                novarocks_execution::task_execution::SafeDetail::truncating("scan failed"),
            ),
        )),
        false,
    );

    let task = harness.execution.task(leaf).expect("the leaf is owned");
    assert_eq!(task.state(), RemoteTaskState::Terminal);
    assert_eq!(task.pending_updates(), 0);
    assert_eq!(task.discarded_updates(), 1);

    // The already released request still converges on its own receipt.
    harness
        .update_ack(&in_flight, OperationOutcome::Accepted)
        .expect("a released update still settles after the task went terminal");
}

#[test]
fn a_terminal_rejected_update_waits_for_the_task_status_authority() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaf = harness.stage_tasks(1)[0];
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("the first split batch is recorded");
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 2, true))
        .expect("the second split batch is recorded");
    let in_flight = harness
        .released()
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::UpdateTask))
        .expect("one update is released");

    harness
        .update_ack(&in_flight, OperationOutcome::TerminalRejected)
        .expect("a terminal rejection stops delivery without failing the attempt");
    let task = harness.execution.task(leaf).expect("the leaf is owned");
    assert_eq!(task.state(), RemoteTaskState::Created);
    assert!(!task.is_terminal(), "only a task status proves termination");
    assert_eq!(task.pending_updates(), 0);
    assert_eq!(task.discarded_updates(), 1);
    assert_eq!(task.converged_after_terminal(), 1);
    let context = task.context();
    assert_eq!(
        harness.execution.take_status_reconciliations(),
        [context].into_iter().collect(),
        "StopSendingAndReconcile requests an immediate status replay"
    );

    assert_eq!(
        harness
            .execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 3, true))
            .expect("a late split is classified"),
        UpdateAdmission::DiscardedTerminal
    );
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| !matches!(intent.kind(), OperationKind::UpdateTask)),
        "the owner must not send after the backend says the task terminated"
    );

    harness.publish(leaf, TaskState::Running, None, false);
    harness.publish(leaf, TaskState::Flushing, None, false);
    harness.publish(leaf, TaskState::Finished, None, true);
    let task = harness.execution.task(leaf).expect("the leaf is owned");
    assert_eq!(task.state(), RemoteTaskState::Terminal);
    assert_eq!(
        task.terminal_report().expect("terminal facts").state,
        TaskState::Finished
    );
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

#[test]
fn renewal_is_scheduled_from_the_effective_duration_and_the_local_send_time() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let establishes = harness.released();
    let establish = establishes
        .iter()
        .find(|intent| matches!(intent.kind(), OperationKind::UpdateQueryContext))
        .expect("an establish was released")
        .clone();
    let OperationIntent::UpdateQueryContext(request) = &establish else {
        unreachable!("an update-context intent carries its request");
    };
    let context = request.context();

    // The response arrives one second after the request was handed over.
    harness.clock.advance(Duration::from_secs(1));
    let effective = Duration::from_secs(9);
    harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            establish.operation_id(),
            OperationKind::UpdateQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Context(
                QueryContextReceipt::new(context, QueryContextState::Active).with_lease(
                    LeaseReceipt::new(
                        LeaseSequence::INITIAL,
                        LeaseValidFor::new(Duration::from_secs(30)).expect("a legal request"),
                        effective,
                    ),
                ),
            ),
        ))
        .expect("the establish settles");

    let schedule = harness
        .execution
        .owner(context)
        .expect("the context has an owner")
        .renew_schedule()
        .expect("an accepted lease installs a schedule");
    assert_eq!(
        schedule,
        RenewSchedule::after(MonotonicInstant::ORIGIN, effective),
        "the schedule comes from the send time and the effective duration"
    );
    assert_ne!(
        schedule,
        RenewSchedule::after(MonotonicInstant::ORIGIN, Duration::from_secs(30)),
        "the requested duration must not be used"
    );
    assert_ne!(
        schedule,
        RenewSchedule::after(
            MonotonicInstant::from_origin(Duration::from_secs(1)),
            effective
        ),
        "the acknowledgement's arrival time must not be used"
    );
}

#[test]
fn a_renewal_is_not_blocked_by_an_unsettled_domain_update() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(9));

    let leaf = harness.stage_tasks(1)[0];
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("a split assignment is recorded");
    let update = harness
        .released()
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::UpdateTask))
        .expect("one update is released");
    harness
        .transport_unknown_ack(&update)
        .expect("an unknown outcome settles without failing");

    // The renewal falls due while that update's outcome is still unknown.
    harness.clock.advance(Duration::from_secs(4));
    let released = harness.released();
    let renewals = released
        .iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::UpdateQueryContext))
        .count();
    assert_eq!(
        renewals, 1,
        "liveness never waits on a domain update whose outcome is unknown"
    );
    assert!(
        released
            .iter()
            .any(|intent| intent.operation_id() == update.operation_id()),
        "the unknown-outcome update is still retried as the identical request"
    );
}

#[test]
fn a_release_waits_for_closure_and_drain_and_a_not_ready_keeps_renewing() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let mut owner = QueryContextOwner::new(
        context,
        2,
        NativeCompatibilityId::new([0x41; 32]),
        admission_epoch(),
    );
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "a release may not precede the establish acknowledgement"
    );

    let facts = FakeEstablish
        .facts_for(context)
        .expect("the fake source has facts");
    grant_admission(&mut owner, MonotonicInstant::ORIGIN);
    let intent = owner
        .establish_intent(facts, MonotonicInstant::ORIGIN)
        .expect("an establish is produced")
        .expect("the establish intent exists");
    owner
        .on_context_ack(&OperationAcknowledgement::new(
            intent.operation_id(),
            OperationKind::UpdateQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Context(
                QueryContextReceipt::new(context, QueryContextState::Active).with_lease(
                    LeaseReceipt::new(
                        LeaseSequence::INITIAL,
                        LeaseValidFor::new(Duration::from_secs(30)).expect("a legal request"),
                        Duration::from_secs(9),
                    ),
                ),
            ),
        ))
        .expect("the establish settles");

    owner.note_create_acknowledged();
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "creates are not closed while one is outstanding"
    );
    owner.note_create_acknowledged();
    assert!(owner.creates_closed());
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "closure alone is not drain"
    );

    owner.note_task_drained();
    owner.note_task_drained();
    owner.note_output_released();
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "one output responsibility is still open"
    );
    owner.note_output_released();
    let release = owner
        .release_intent(MonotonicInstant::ORIGIN)
        .expect("closure and drain release the context");

    let not_ready = OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(context, QueryContextState::Active),
            outcome: ReleaseOutcome::NotReady,
            // A backend that answers NOT_READY has sealed nothing: it is
            // still draining, so there is no terminal observation to carry.
            runtime_filter: Some(fixture_runtime_filter_contribution()),
        },
    );
    assert_eq!(
        owner
            .on_release_ack(&not_ready, MonotonicInstant::ORIGIN)
            .expect("a not-ready answer settles"),
        ReleaseSettlement::NotReadyKeepRenewing
    );
    assert!(
        owner.runtime_filter_contribution().is_none(),
        "a backend that is still draining has sealed nothing, so a NOT_READY \
         answer must not leave a terminal observation behind"
    );
    assert!(
        owner.must_keep_renewing(),
        "a not-ready release keeps the lease alive"
    );
    assert!(
        owner
            .release_intent(MonotonicInstant::from_origin(Duration::from_millis(49)))
            .is_none(),
        "the identical release observes its retry backoff"
    );

    // Backend-local in-flight work can settle without advancing a frontend
    // progress fact. The timer still releases the exact same request.
    let retry = owner
        .release_intent(MonotonicInstant::from_origin(Duration::from_millis(50)))
        .expect("the retry timer re-releases the identical request");
    assert_eq!(retry.operation_id(), release.operation_id());

    assert_eq!(
        owner
            .on_release_ack(
                &not_ready,
                MonotonicInstant::from_origin(Duration::from_millis(50)),
            )
            .expect("a second not-ready answer settles"),
        ReleaseSettlement::NotReadyKeepRenewing
    );
    assert!(
        owner
            .release_intent(MonotonicInstant::from_origin(Duration::from_millis(149)))
            .is_none(),
        "a repeated not-ready answer increases the retry backoff"
    );
    let backed_off_retry = owner
        .release_intent(MonotonicInstant::from_origin(Duration::from_millis(150)))
        .expect("the increased retry backoff eventually expires");
    assert_eq!(backed_off_retry.operation_id(), release.operation_id());

    owner
        .on_release_ack(
            &not_ready,
            MonotonicInstant::from_origin(Duration::from_millis(150)),
        )
        .expect("a third not-ready answer settles");
    owner.note_output_released();
    let progress_retry = owner
        .release_intent(MonotonicInstant::from_origin(Duration::from_millis(150)))
        .expect("frontend progress bypasses the outstanding retry timer");
    assert_eq!(progress_retry.operation_id(), release.operation_id());

    owner
        .on_release_ack(
            &OperationAcknowledgement::new(
                progress_retry.operation_id(),
                OperationKind::ReleaseQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: QueryContextReceipt::new(context, QueryContextState::TerminalRetained),
                    outcome: ReleaseOutcome::Released,
                    runtime_filter: Some(fixture_runtime_filter_contribution()),
                },
            ),
            MonotonicInstant::from_origin(Duration::from_millis(50)),
        )
        .expect("a released answer settles");
    assert!(!owner.must_keep_renewing());
    assert!(owner.is_released());
    // The defect this catches: a settle that reads the release outcome and
    // drops the rest of the payload. The acknowledgement is the only message
    // that carries this backend's terminal runtime-filter observation, so a
    // settle that discards it makes every downstream convergence fact absent
    // while every part of the carrier stays individually correct.
    assert!(
        owner
            .runtime_filter_contribution()
            .expect("a released answer carries the backend's observation")
            .available()
            .is_some(),
        "the retained contribution must be the available one the backend sent"
    );
}

#[test]
fn an_admission_transport_unknown_replays_exactly_and_only_the_next_turn_establishes() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let compatibility = NativeCompatibilityId::new([0x42; 32]);
    let mut owner = QueryContextOwner::new(context, 0, compatibility, admission_epoch());
    let first = owner
        .admission_intent(MonotonicInstant::ORIGIN)
        .expect("the request is legal")
        .expect("admission is required");
    let OperationIntent::AcquireQueryContextAdmissionTicket(first_request) = first else {
        unreachable!("admission has its own operation kind");
    };
    assert_eq!(first_request.valid_for().get(), Duration::from_secs(10));
    assert_eq!(first_request.native_compatibility_id(), compatibility);

    owner
        .on_admission_ack(
            &OperationAcknowledgement::transport_unknown(
                first_request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
            ),
            MonotonicInstant::from_origin(Duration::from_millis(1)),
        )
        .expect("transport unknown preserves the request");
    let second = owner
        .admission_intent(MonotonicInstant::from_origin(Duration::from_millis(2)))
        .expect("the exact retry is legal")
        .expect("the exact retry is released");
    let OperationIntent::AcquireQueryContextAdmissionTicket(second_request) = second else {
        unreachable!("admission has its own operation kind");
    };
    assert_eq!(second_request.envelope(), first_request.envelope());
    assert_eq!(second_request.context(), first_request.context());
    assert_eq!(second_request.valid_for(), first_request.valid_for());
    assert_eq!(
        second_request.native_compatibility_id(),
        first_request.native_compatibility_id()
    );

    let ticket_id =
        AdmissionTicketId::try_from_bytes([0x55; 16]).expect("the test ticket is nonzero");
    owner
        .on_admission_ack(
            &OperationAcknowledgement::worker_receipt(
                second_request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
                OperationOutcome::Accepted,
                AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                    ticket_id,
                    context,
                    second_request.valid_for(),
                )),
            ),
            MonotonicInstant::from_origin(Duration::from_millis(3)),
        )
        .expect("the exact grant settles");
    assert_eq!(owner.state(), QueryContextState::Absent);

    let establish = owner
        .establish_intent(
            FakeEstablish.facts_for(context).expect("frozen facts"),
            MonotonicInstant::from_origin(Duration::from_millis(4)),
        )
        .expect("the grant is current")
        .expect("the next owner turn starts establish");
    let OperationIntent::UpdateQueryContext(request) = establish else {
        unreachable!("the granted owner releases an establish");
    };
    let UpdateQueryContext::Establish(request) = request.as_ref() else {
        unreachable!("the first context update is establish");
    };
    assert_eq!(request.admission_ticket_id(), ticket_id);
}

#[test]
fn a_round_never_emits_establish_before_each_context_grant() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let first = harness
        .pump_once()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    assert!(first.iter().any(|intent| matches!(
        intent,
        OperationIntent::AcquireQueryContextAdmissionTicket(_)
    )));
    assert!(first.iter().all(|intent| !matches!(
        intent,
        OperationIntent::UpdateQueryContext(request)
            if matches!(request.as_ref(), UpdateQueryContext::Establish(_))
    )));

    for intent in first {
        let OperationIntent::AcquireQueryContextAdmissionTicket(request) = intent else {
            continue;
        };
        harness
            .execution
            .acknowledge(&OperationAcknowledgement::worker_receipt(
                request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
                OperationOutcome::Accepted,
                AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                    AdmissionTicketId::try_from_bytes([0x56; 16]).expect("the ticket is nonzero"),
                    request.context(),
                    request.valid_for(),
                )),
            ))
            .expect("the context grant settles");
    }
    assert!(
        harness.sink.take().is_empty(),
        "a receipt does not send work"
    );
    let second = harness
        .pump_once()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    assert!(second.iter().any(|intent| matches!(
        intent,
        OperationIntent::UpdateQueryContext(request)
            if matches!(request.as_ref(), UpdateQueryContext::Establish(_))
    )));
}

#[test]
fn a_release_is_withheld_until_every_expected_create_is_acknowledged() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let creates = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .collect::<Vec<_>>();
    assert_eq!(creates.len(), 3);
    for intent in creates.iter().take(2) {
        harness
            .create_ack(intent, OperationOutcome::Accepted)
            .expect("a create acknowledgement settles");
    }
    let released = harness.released();
    assert!(
        released
            .iter()
            .all(|intent| !matches!(intent.kind(), OperationKind::ReleaseQueryContext)),
        "a release may not be sent while a legal create can still follow"
    );
}

// ---------------------------------------------------------------------------
// Stage state and cancellation
// ---------------------------------------------------------------------------

#[test]
fn one_finished_task_does_not_make_its_stage_flush_or_cancel_its_children() {
    let mut harness = Harness::new(&[0, 1], &[0, 1], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let middle = harness.stage_tasks(2);
    assert_eq!(middle.len(), 2);
    harness.publish(middle[0], TaskState::Running, None, false);
    harness.publish(middle[1], TaskState::Running, None, false);
    harness.publish(middle[0], TaskState::Flushing, None, false);
    harness.publish(middle[0], TaskState::Finished, None, true);

    assert_eq!(
        harness.stage_state(2),
        StageState::Running,
        "a stage holding a running task is not flushing"
    );
    let released = harness.released();
    assert!(
        released
            .iter()
            .all(|intent| !matches!(intent.kind(), OperationKind::CancelTask)),
        "a stage that is still running may not stand its children down"
    );
    for &leaf in &harness.stage_tasks(1) {
        assert!(
            !harness
                .execution
                .task(leaf)
                .expect("the leaf is owned")
                .cancel_requested()
        );
    }

    // Once the whole stage stops consuming, its children stand down once, one
    // layer at a time.
    harness.publish(middle[1], TaskState::Flushing, None, false);
    assert_eq!(harness.stage_state(2), StageState::Flushing);
    let cancels = harness
        .released()
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CancelTask))
        .collect::<Vec<_>>();
    assert_eq!(
        cancels.len(),
        2,
        "exactly the child stage's tasks stand down"
    );
    for intent in &cancels {
        let OperationIntent::CancelTask(request) = intent else {
            unreachable!("a cancel intent carries its request");
        };
        assert_eq!(request.reason(), CancelReason::UpstreamNoLongerNeeded);
        assert_eq!(request.identity().stage_id().get(), 1);
    }

    // A second pass adds nothing: the layer was already propagated.
    harness
        .execution
        .apply_status(8)
        .expect("applying intake succeeds");
    let repeat = harness
        .released()
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CancelTask))
        .count();
    assert_eq!(repeat, 0);
}

#[test]
fn the_client_visible_read_completes_without_waiting_for_upstream_cancellation() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let root = harness.execution.graph().root_task();
    let leaf = harness.stage_tasks(1)[0];
    harness.publish(root, TaskState::Running, None, false);
    harness.publish(leaf, TaskState::Running, None, false);
    harness.publish(root, TaskState::Finished, None, true);

    // A backend's claim that its output responsibility is complete is not this
    // frontend's evidence that it received the end of the stream. Completing
    // here would tell a client the answer is whole before the last packet was
    // read.
    assert!(
        !harness.execution.client_visible_completion(),
        "a root FINISHED without an observed end of stream is not a completion"
    );

    harness
        .execution
        .consume_root_result_packet(harness.identity(root), 0, true)
        .expect("the root's own end of stream is accepted");

    assert!(
        harness.execution.client_visible_completion(),
        "the read completes once the root finished and its stream ended"
    );
    assert!(
        !harness.execution.attempt_drained(),
        "internal draining is still outstanding"
    );

    harness.publish(
        leaf,
        TaskState::Canceling,
        Some(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )),
        false,
    );
    assert!(
        harness.execution.client_visible_completion(),
        "an upstream still standing down never retracts a linearized completion"
    );
    assert!(harness.execution.failure_cause().is_none());
}

#[test]
fn a_task_failure_latches_once_and_withholds_client_completion() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaf = harness.stage_tasks(1)[0];
    let failure = TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
        novarocks_execution::task_execution::TaskFailureCategory::Execution,
        novarocks_execution::task_execution::SafeDetail::truncating("first cause"),
    ));
    harness.publish(leaf, TaskState::Running, None, false);
    harness.publish(leaf, TaskState::Failing, Some(failure.clone()), false);
    harness.publish(leaf, TaskState::Failed, Some(failure.clone()), false);

    assert_eq!(harness.execution.failure_cause(), Some(&failure));
    assert_eq!(harness.stage_state(1), StageState::Failed);
    assert!(!harness.execution.client_visible_completion());
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

#[test]
fn a_create_burst_never_starves_a_lifecycle_operation() {
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 512);

    let batches = harness.pump();
    assert!(
        batches
            .iter()
            .any(|(lane, _)| matches!(lane, DispatchLane::Lifecycle)),
        "the establish leaves in the same rotation as a 65-task create burst"
    );
    assert!(
        batches
            .iter()
            .any(|(lane, _)| matches!(lane, DispatchLane::Create))
    );
}

#[test]
fn process_backpressure_restores_the_exact_batch_without_in_flight_or_spin() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("legal graph");
    let admission_epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(BackpressureOnceSink::default());
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let mut execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::DEFAULT,
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        Arc::new(ManualClock::new()),
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        intake,
    )
    .expect("compose execution");

    let first = execution.pump(&FakeEstablish).expect("first pump");
    assert_eq!(first.operations, 0);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(execution.dispatcher().in_flight_operations(), 0);
    assert!(execution.dispatcher().queued_items() > 0);

    let second = execution.pump(&FakeEstablish).expect("capacity wake turn");
    assert!(second.operations > 0);
    let refused = sink.refused.lock().expect("refused batch").clone();
    let accepted = sink.accepted.lock().expect("accepted batches");
    assert!(
        accepted.iter().any(|ids| ids == &refused),
        "the refused batch must later be accepted with the same operation ids and order"
    );
    assert!(execution.dispatcher().in_flight_operations() > 0);
}

#[test]
fn process_queue_backpressure_rolls_back_every_unadmitted_owner_transition() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("legal graph");
    let stage_ids = graph
        .stages()
        .map(|stage| stage.stage_id())
        .collect::<Vec<_>>();
    let admission_epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(QueueBackpressureOnceSink::default());
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let mut execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::DEFAULT,
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        Arc::new(ManualClock::new()),
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        intake,
    )
    .expect("compose execution");

    let first = execution
        .pump(&FakeEstablish)
        .expect("queue backpressure is a retryable local admission result");
    assert_eq!(first.operations, 0);
    assert_eq!(execution.dispatcher().queued_items(), 0);
    assert_eq!(execution.dispatcher().in_flight_operations(), 0);
    for stage_id in stage_ids {
        assert!(
            execution
                .stage(stage_id)
                .expect("the stage is owned")
                .tasks()
                .all(|(_, task)| !task.has_released_operation()),
            "no task may retain create/update/cancel state before process admission"
        );
    }

    let second = execution
        .pump(&FakeEstablish)
        .expect("the capacity-change turn can release the same owner work");
    assert!(second.operations > 0);
    assert!(sink.submitted_operations.load(Ordering::SeqCst) > 0);
}

#[test]
fn task_update_process_rejection_precedes_remote_task_retention() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("legal graph");
    let leaf = graph
        .tasks()
        .find(|task| task.stage_id() == StageId::new(1).expect("stage one"))
        .map(|task| task.task_id())
        .expect("the graph has one leaf task");
    let admission_epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(QueueBackpressureOnceSink::default());
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let mut execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::DEFAULT,
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        Arc::new(ManualClock::new()),
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        intake,
    )
    .expect("compose execution");

    let error = execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect_err("the first process reservation is refused");
    assert!(matches!(
        error,
        TaskExecutionError::Capacity(CapacityBound::ProcessTransportQueue { .. })
    ));
    assert_eq!(
        execution
            .task(leaf)
            .expect("the task is owned")
            .pending_updates(),
        0,
        "a rejected process reservation must leave no per-attempt payload"
    );
    assert_eq!(
        execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
            .expect("the unchanged domain update can be admitted later"),
        UpdateAdmission::Queued
    );
}

#[test]
fn an_abort_jumps_the_queue_without_preempting_a_released_operation() {
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 512);
    let released_before = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .map(|intent| intent.operation_id())
        .collect::<Vec<_>>();
    assert!(!released_before.is_empty());

    let context = *harness
        .execution
        .graph()
        .contexts()
        .next()
        .expect("the attempt has a context");
    let submission = harness
        .execution
        .abort_context(context, AbortCause::QueryFailed)
        .expect("an abort is admitted");
    assert_eq!(submission, AbortSubmission::Accepted);
    let batches = harness.sink.take();
    let [(lane, operations)] = batches.as_slice() else {
        panic!("the accepted abort must reach exactly one transport batch");
    };
    assert_eq!(*lane, DispatchLane::Lifecycle);
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].kind(), OperationKind::AbortQueryContext);

    // Nothing already released was withdrawn: it is still tracked in flight.
    assert!(
        harness.execution.dispatcher().in_flight_operations() > released_before.len(),
        "an abort adds an operation rather than revoking released ones"
    );
}

#[test]
fn a_bounded_fan_out_stays_inside_every_lane_and_queue_bound() {
    for tasks in [10_usize, 100, 1000] {
        let leaves = (0..tasks).map(|index| index % 4).collect::<Vec<_>>();
        let mut harness = Harness::new(&leaves, &[0, 1, 2, 3], 4096);
        let budget = DispatchBudget::DEFAULT;
        let transport = TransportBudget::DEFAULT;
        let backend_count = harness.execution.dispatcher().backends().count();

        let mut rounds = 0_usize;
        let mut create_rounds = 0_usize;
        let mut created = 0_usize;
        loop {
            rounds += 1;
            assert!(
                rounds <= tasks + 32,
                "the fan-out must not become a per-task serial chain"
            );
            let released = harness.released();
            if released
                .iter()
                .any(|intent| matches!(intent.kind(), OperationKind::CreateTask))
            {
                create_rounds += 1;
            }
            assert_bounds(harness.execution.dispatcher(), budget, transport);
            assert!(
                harness.execution.dispatcher().in_flight_operations()
                    <= budget.total_permits() * backend_count,
                "in-flight operations are bounded by the per-backend permits"
            );
            if released.is_empty() {
                break;
            }
            for intent in released {
                match intent.kind() {
                    OperationKind::CreateTask => {
                        created += 1;
                        harness
                            .create_ack(&intent, OperationOutcome::Accepted)
                            .expect("a create acknowledgement settles");
                    }
                    OperationKind::UpdateQueryContext => {
                        harness.context_ack(&intent, Duration::from_secs(30));
                    }
                    OperationKind::UpdateTask => {
                        harness
                            .update_ack(&intent, OperationOutcome::Accepted)
                            .expect("an update acknowledgement settles");
                    }
                    kind => unreachable!("no {kind} is produced by this fan-out"),
                }
                assert_bounds(harness.execution.dispatcher(), budget, transport);
            }
        }
        assert_eq!(created, tasks + 5, "every task of the attempt was created");
        // Concurrency is real and bounded at the same time: the busiest
        // backend needs one round per full set of create permits, not one
        // round per task.
        let busiest = tasks.div_ceil(4) + 1;
        let expected = busiest.div_ceil(budget.create_permits());
        assert!(
            create_rounds <= expected + 1,
            "{tasks} tasks needed {create_rounds} create rounds, expected at most {expected}"
        );
        assert!(
            busiest <= budget.create_permits() || create_rounds > 1,
            "a fan-out larger than the create permits must be released over several rounds"
        );
    }
}

fn assert_bounds(
    dispatcher: &OperationDispatcher,
    budget: DispatchBudget,
    transport: TransportBudget,
) {
    assert!(dispatcher.queued_items() <= transport.max_backend_queued_operations());
    assert!(dispatcher.queued_bytes() <= transport.max_backend_queued_bytes());
    for backend in dispatcher.backends() {
        for lane in [
            DispatchLane::Create,
            DispatchLane::Update,
            DispatchLane::Lifecycle,
        ] {
            assert!(
                dispatcher.lane_in_flight(backend, lane) <= budget.permits_for(lane),
                "lane {lane:?} exceeded its permits"
            );
            assert!(
                dispatcher.lane_queued(backend, lane)
                    <= transport.max_query_backend_queued_operations()
            );
        }
    }
}

#[test]
fn an_establish_accounts_for_its_query_options_payload() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let mut owner = QueryContextOwner::new(
        context,
        0,
        NativeCompatibilityId::new([0x41; 32]),
        admission_epoch(),
    );
    let facts = FakeEstablish
        .facts_for(context)
        .expect("the fake source has facts");
    grant_admission(&mut owner, MonotonicInstant::ORIGIN);
    let intent = owner
        .establish_intent(facts, MonotonicInstant::ORIGIN)
        .expect("an establish is produced")
        .expect("the establish intent exists");

    assert_eq!(
        intent.queued_bytes(),
        OPERATION_FIXED_BYTES + 64 + 32 + 48 + 32,
        "the bounded queue must account for every establish payload"
    );
}

#[test]
fn one_operation_cannot_exceed_the_queue_side_carrier_bound() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let mut owner = QueryContextOwner::new(
        context,
        0,
        NativeCompatibilityId::new([0x42; 32]),
        admission_epoch(),
    );
    grant_admission(&mut owner, MonotonicInstant::ORIGIN);
    let intent = owner
        .establish_intent(
            FakeEstablish
                .facts_for(context)
                .expect("the fake source has facts"),
            MonotonicInstant::ORIGIN,
        )
        .expect("an establish is produced")
        .expect("the establish intent exists");
    let transport = TransportBudget::new(1, 255, 1, 1, 255, 2, 1020, 1, 2, Duration::from_secs(1))
        .expect("the process has one ordinary and one control retained window");
    let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, transport);
    let actual = intent.queued_bytes();
    let error = dispatcher
        .enqueue(intent, MonotonicInstant::ORIGIN)
        .expect_err("an oversized queue-side carrier must fail before it can become head-of-line");
    assert_eq!(
        error,
        TaskExecutionError::Capacity(CapacityBound::OperationBytes { limit: 255, actual })
    );
}

#[test]
fn a_queued_byte_bound_fails_closed_instead_of_being_exceeded() {
    // Each descriptor carries a payload at the descriptor bound, so a
    // moderate fan-out reaches the queued-byte bound rather than exceeding it.
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 16 * 1024 * 1024);
    let error = harness
        .execution
        .pump(&FakeEstablish)
        .expect_err("the queued-byte bound must be reached");
    assert!(matches!(
        error,
        TaskExecutionError::Capacity(CapacityBound::BackendBytes { .. })
            | TaskExecutionError::Capacity(CapacityBound::QueryBackendBytes { .. })
    ));
    assert_eq!(
        CapacityBound::BackendBytes { limit: 1 }.as_operation_outcome(),
        OperationOutcome::ResourceExhausted
    );
}

#[test]
fn an_operation_that_outlives_queue_residence_fails_typed_and_rolls_back_its_owner() {
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 512);
    let released = harness.pump();
    assert!(!released.is_empty());

    // Nothing is acknowledged, so the remaining creates stay queued past the
    // residence bound.
    harness
        .clock
        .advance(TransportBudget::DEFAULT.frontend_queue_residence());
    let error = harness
        .execution
        .pump(&FakeEstablish)
        .expect_err("queue expiry is a typed local failure, not a silent drop");
    let TaskExecutionError::QueueResidenceExpired { kind, waited, .. } = error else {
        panic!("expected a queue-expiry receipt, got {error:?}");
    };
    assert_eq!(kind, OperationKind::CreateTask);
    assert!(waited >= TransportBudget::DEFAULT.frontend_queue_residence());
    harness
        .execution
        .pump(&FakeEstablish)
        .expect("every expired owner marker was rolled back before failure returned");
    assert!(harness.execution.dispatcher().queued_items() > 0);
}

// ---------------------------------------------------------------------------
// Status intake
// ---------------------------------------------------------------------------

#[test]
fn a_status_callback_only_enqueues_and_wakes_one_serial_runner() {
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(2, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let identity = TaskIdentity::new(
        execution_id(),
        StageId::new(1).expect("a nonzero stage id"),
        TaskId::new(1).expect("a nonzero task id"),
        BackendProcessId::new_v7(),
    );
    let handle = intake.handle();

    assert_eq!(
        handle.publish(StatusEvent::Published(TaskStatus::created(identity))),
        StatusIntakeAdmission::Enqueued
    );
    assert_eq!(intake.queued(), 1);
    assert_eq!(wake.count(), 1);

    let runner = intake.try_enter().expect("the runner slot is free");
    assert!(
        intake.try_enter().is_none(),
        "intake is applied by one runner at a time"
    );
    drop(runner);
    assert!(intake.try_enter().is_some());
}

#[test]
fn intake_overflow_is_reported_as_observation_loss_rather_than_a_silent_gap() {
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(1, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let identity = TaskIdentity::new(
        execution_id(),
        StageId::new(1).expect("a nonzero stage id"),
        TaskId::new(1).expect("a nonzero task id"),
        BackendProcessId::new_v7(),
    );
    let handle = intake.handle();
    handle.publish(StatusEvent::Published(TaskStatus::created(identity)));
    assert_eq!(
        handle.publish(StatusEvent::Published(TaskStatus::created(identity))),
        StatusIntakeAdmission::Overflowed
    );

    let mut runner = intake.try_enter().expect("the runner slot is free");
    let (observation_loss, _) = runner.drain_statuses(2);
    assert!(
        observation_loss,
        "overflow tells the runner to resubscribe with its cursors"
    );
    assert!(!runner.drain_statuses(2).0);
}

#[test]
fn intake_is_applied_by_the_serial_runner_not_the_callback() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaf = harness.stage_tasks(1)[0];
    let identity = harness.identity(leaf);
    let status = TaskStatus::try_new(
        identity,
        TaskStatusVersion::new(2).expect("a nonzero version"),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .expect("a legal status snapshot");

    harness
        .execution
        .intake()
        .handle()
        .publish(StatusEvent::Published(status));
    assert_eq!(harness.wake.count(), 1);
    assert_eq!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .task_state(),
        TaskState::Planned,
        "publishing alone must not advance a task"
    );

    let report = harness
        .execution
        .apply_status(8)
        .expect("applying intake succeeds");
    assert_eq!(report.accepted, 1);
    assert_eq!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .task_state(),
        TaskState::Running
    );
}

// ---------------------------------------------------------------------------
// Split-assignment adapter
// ---------------------------------------------------------------------------

#[test]
fn the_split_adapter_addresses_graph_tasks_and_reuses_the_driver_retry_rule() {
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("a legal graph");

    let node = PlanNodeId::new(SCAN_NODE).expect("a nonnegative plan node");
    let targets = assignment_targets(&graph, node);
    assert_eq!(targets.len(), 2);
    for target in &targets {
        let task = graph
            .tasks()
            .find(|task| task.fragment_instance_id() == target.fragment_instance_id)
            .expect("every target addresses a graph task");
        assert_eq!(task.backend_idx(), target.backend_idx);
        assert_eq!(task.fragment_id(), LEAF_FRAGMENT);
    }
    assert!(
        assignment_targets(&graph, PlanNodeId::new(SCAN_NODE + 1).expect("a plan node")).is_empty()
    );

    // Only a genuinely unknown transport outcome is replayable, and that
    // verdict comes from the delivery owner rather than being restated here.
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::Transport {
            target: targets[0].clone(),
            detail: "acknowledgement lost".to_owned(),
        }),
        novarocks_query_application::coordination::FrontendAction::RetryExactRequest
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::Rejected {
            target: targets[0].clone(),
            reason: "watermark".to_owned(),
            detail: "gap".to_owned(),
        }),
        novarocks_query_application::coordination::FrontendAction::FailAttempt
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::NoAdmittedTask { plan_node_id: 9 }),
        novarocks_query_application::coordination::FrontendAction::FailAttempt
    );
}

// ---------------------------------------------------------------------------
// Settlement bookkeeping
// ---------------------------------------------------------------------------

#[test]
fn an_acknowledgement_for_an_unsent_operation_is_refused() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let error = harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            novarocks_execution::task_execution::TaskOperationId::new_v7(),
            OperationKind::CreateTask,
            OperationOutcome::Accepted,
            AckPayload::None,
        ))
        .expect_err("an unknown operation id is refused");
    assert!(matches!(error, TaskExecutionError::UnknownOperation));
}

#[test]
fn an_accepted_create_without_its_receipt_is_refused() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let create = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .expect("a create was released");
    let error = harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            create.operation_id(),
            OperationKind::CreateTask,
            OperationOutcome::Accepted,
            AckPayload::None,
        ))
        .expect_err("an accepted create must carry its receipt");
    assert!(matches!(
        error,
        TaskExecutionError::MissingReceipt(OperationKind::CreateTask)
    ));
}

/// The runner records which contexts it started subscriptions for.
#[derive(Debug, Default)]
struct RecordingSubscriptions {
    ensured: Mutex<Vec<(QueryContextRef, usize)>>,
    resubscribed: Mutex<Vec<(QueryContextRef, usize)>>,
    /// What a settled subscription reports, so a test can put a backend out
    /// of observation without a transport.
    settled: Mutex<Option<crate::native::task_transport::SubscriptionState>>,
}

impl crate::task_execution::round::StatusSubscriptions for RecordingSubscriptions {
    fn ensure(
        &self,
        context: QueryContextRef,
        cursors: Vec<novarocks_execution::task_execution::TaskStatusCursor>,
    ) -> Result<(), String> {
        self.ensured
            .lock()
            .expect("subscription ledger")
            .push((context, cursors.len()));
        Ok(())
    }

    fn resubscribe(
        &self,
        context: QueryContextRef,
        cursors: Vec<novarocks_execution::task_execution::TaskStatusCursor>,
    ) -> Result<(), String> {
        self.resubscribed
            .lock()
            .expect("resubscription ledger")
            .push((context, cursors.len()));
        Ok(())
    }

    fn settled_fatally(
        &self,
        _context: QueryContextRef,
    ) -> Option<crate::native::task_transport::SubscriptionState> {
        // The fake owes the trait's contract, not just its shape: a state
        // resubscribing can still repair is not something to report.
        self.settled
            .lock()
            .expect("settled subscription")
            .filter(|state| state.is_fatal())
    }
}

/// The defect this catches: a create acknowledgement adopted the receipt's
/// snapshot outright instead of classifying it. The create response and the
/// status stream are independent transports, so the stream can deliver
/// version 2 (RUNNING) before the create response is settled -- and adopting
/// the receipt's version 1 (PLANNED) then regressed a running task, which
/// fails the whole attempt with an illegal transition. Observed in a real
/// 1FE+3BE run as "task state RUNNING may not become PLANNED".
#[test]
fn a_create_receipt_snapshot_older_than_the_observed_one_is_ignored_not_adopted() {
    let mut harness = Harness::new(&[0], &[0], 64);
    let creates = harness
        .establish_all(Duration::from_secs(30))
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .collect::<Vec<_>>();
    assert!(!creates.is_empty(), "the attempt owes creates");
    let OperationIntent::CreateTask(request) = &creates[0] else {
        unreachable!("a create intent carries its request");
    };
    let task_id = request.identity().task_id();

    // The backend already started this task and published RUNNING while its
    // create response was still in flight.
    harness.publish_with_filters(task_id, TaskState::Running, None, false, None);
    let observed = harness
        .execution
        .task(task_id)
        .expect("the task is owned")
        .status()
        .expect("a published status")
        .clone();
    assert_eq!(observed.state(), TaskState::Running);
    assert_eq!(observed.version(), TaskStatusVersion::new(2).expect("v2"));

    // Settling the create must not regress that.
    harness
        .create_ack(&creates[0], OperationOutcome::Accepted)
        .expect("a create receipt carrying a stale snapshot still settles");
    let held = harness
        .execution
        .task(task_id)
        .expect("the task is owned")
        .status()
        .expect("a published status");
    assert_eq!(held.state(), TaskState::Running);
    assert_eq!(held.version(), TaskStatusVersion::new(2).expect("v2"));
    assert_eq!(
        harness
            .execution
            .task(task_id)
            .expect("the task is owned")
            .state(),
        RemoteTaskState::Created,
        "the create is still acknowledged"
    );
}

/// Builds the acknowledgement a released establish settles with.
///
/// A fresh attempt's establish creates its context, so its lease sequence is
/// the initial one and nothing has to be read back out of the execution.
fn establish_ack(intent: &OperationIntent) -> OperationAcknowledgement {
    let OperationIntent::UpdateQueryContext(request) = intent else {
        unreachable!("an update-context intent carries its request");
    };
    assert!(
        request.may_create(),
        "a fresh attempt's establish creates its context"
    );
    let receipt = QueryContextReceipt::new(request.context(), QueryContextState::Active)
        .with_lease(LeaseReceipt::new(
            LeaseSequence::INITIAL,
            LeaseValidFor::new(Duration::from_secs(30)).expect("a legal request"),
            Duration::from_secs(30),
        ));
    OperationAcknowledgement::new(
        intent.operation_id(),
        OperationKind::UpdateQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Context(receipt),
    )
}

/// Builds the exact Worker acknowledgement for one released admission request.
fn admission_ack(intent: &OperationIntent) -> OperationAcknowledgement {
    let OperationIntent::AcquireQueryContextAdmissionTicket(request) = intent else {
        unreachable!("an admission intent carries its exact request");
    };
    OperationAcknowledgement::worker_receipt(
        intent.operation_id(),
        OperationKind::AcquireQueryContextAdmissionTicket,
        OperationOutcome::Accepted,
        AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([0x54; 16]).expect("the test ticket is nonzero"),
            request.context(),
            request.valid_for(),
        )),
    )
}

/// Every admission intent one sink recorded.
fn released_admissions(sink: &RecordingSink) -> Vec<OperationIntent> {
    sink.take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .filter(|intent| {
            matches!(
                intent.kind(),
                OperationKind::AcquireQueryContextAdmissionTicket
            )
        })
        .collect()
}

/// Every establish intent one sink recorded.
fn released_establishes(sink: &RecordingSink) -> Vec<OperationIntent> {
    sink.take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .filter(|intent| matches!(intent.kind(), OperationKind::UpdateQueryContext))
        .collect()
}

fn accepted_create_ack(intent: &OperationIntent) -> OperationAcknowledgement {
    let OperationIntent::CreateTask(request) = intent else {
        unreachable!("a create intent carries its request");
    };
    OperationAcknowledgement::new(
        intent.operation_id(),
        OperationKind::CreateTask,
        OperationOutcome::Accepted,
        AckPayload::Create(CreateTaskReceipt::new(
            request.identity(),
            Vec::new(),
            TaskStatus::created(request.identity()),
        )),
    )
}

/// The defect this catches: the runner subscribed to task status for every
/// context on the same turn that merely *released* the establish, so the
/// subscription and the establish raced to the backend as two independent
/// RPCs. A subscription that won named a query context the backend did not
/// hold yet, the backend answered FailedPrecondition, and the frontend
/// classifies that as fatal -- so that context was never observed again for
/// the life of the attempt. Losing the root's context hangs the query to its
/// deadline; losing any other burns the whole drain budget.
#[test]
fn a_context_is_subscribed_only_after_its_own_establish_is_acknowledged() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let contexts = graph.contexts().count();
    assert!(contexts > 1, "this test needs more than one context");
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();

    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();

    let first = round.turn().expect("a turn on a fresh attempt");
    assert!(
        first.operations > 0,
        "the first turn owes an admission request to every context"
    );
    assert!(
        subscriptions.ensured.lock().expect("ledger").is_empty(),
        "a released establish is not an acknowledged one, so no context may be subscribed yet"
    );

    let admissions = released_admissions(&sink);
    assert_eq!(admissions.len(), contexts);
    for intent in &admissions {
        acks.publish(admission_ack(intent));
    }
    round
        .turn()
        .expect("admission receipts release every establish");

    // Answer exactly one establish. Only that context becomes subscribable;
    // the other is still a context its backend does not hold.
    let establishes = released_establishes(&sink);
    assert_eq!(establishes.len(), contexts);
    let OperationIntent::UpdateQueryContext(first_request) = &establishes[0] else {
        unreachable!("an establish carries its request");
    };
    let established_context = first_request.context();
    acks.publish(establish_ack(&establishes[0]));

    round.turn().expect("a turn that settles one establish");
    assert_eq!(
        subscriptions
            .ensured
            .lock()
            .expect("ledger")
            .iter()
            .map(|(context, _)| *context)
            .collect::<Vec<_>>(),
        vec![established_context],
        "exactly the acknowledged context is subscribed"
    );

    // Answer the rest, and every context becomes subscribable.
    for intent in &establishes[1..] {
        acks.publish(establish_ack(intent));
    }
    round.turn().expect("a turn that settles the rest");
    let ensured = subscriptions.ensured.lock().expect("ledger").clone();
    assert_eq!(
        ensured.len(),
        1 + contexts,
        "the acknowledged context keeps being ensured and the rest join it"
    );
    let mut subscribed: Vec<_> = ensured.iter().map(|(context, _)| *context).collect();
    subscribed.sort_unstable();
    subscribed.dedup();
    assert_eq!(subscribed.len(), contexts);
}

/// The defect this catches: `SubscriptionState::is_fatal` was computed and
/// never consumed, so nothing in the attempt noticed a backend it could no
/// longer observe. A lease renewal that cannot reach a dead process
/// classifies as a retryable transport unknown and is retried, an exchange
/// peer fails only if it happens to have one, and a root task on a surviving
/// backend simply blocks. Measured on 1FE+3BE: killing one of three backends
/// left a distributed SELECT hanging for the full 30 s step budget whenever
/// the killed process did not host the root task; with this it fails in
/// under three seconds and names the backend.
#[test]
fn a_backend_whose_subscription_settled_fatally_fails_the_attempt_by_name() {
    use crate::native::task_transport::{SubscriptionState, TaskAckIntake};
    use crate::task_execution::round::TaskRound;

    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();

    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();

    round
        .turn()
        .expect("a turn that releases every admission request");
    for intent in released_admissions(&sink) {
        acks.publish(admission_ack(&intent));
    }
    round
        .turn()
        .expect("admission receipts release every establish");
    for intent in released_establishes(&sink) {
        acks.publish(establish_ack(&intent));
    }
    round
        .turn()
        .expect("a turn that settles every establish and subscribes");

    // A state resubscribing can still repair is not evidence of anything.
    *subscriptions.settled.lock().expect("settled") = Some(SubscriptionState::Resubscribing);
    round
        .turn()
        .expect("a recoverable subscription decides nothing");

    // One that has settled is.
    *subscriptions.settled.lock().expect("settled") = Some(SubscriptionState::BudgetExhausted);
    let error = round
        .turn()
        .expect_err("a backend out of observation fails the attempt");
    let TaskExecutionError::ParticipantUnobservable { backend, state } = error else {
        panic!("expected an unobservable participant, got {error:?}");
    };
    assert_eq!(state, "budget_exhausted");
    assert!(
        processes.values().any(|process| *process == backend),
        "the failure has to name a backend of this attempt, got {backend}"
    );
}

#[test]
fn a_turn_starts_one_subscription_per_context_and_reports_what_moved() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    // The runner owns no policy: a turn moves the state machine and reports
    // what moved. What it does own is the order -- acknowledgements settle
    // before the pump, so a permit freed this turn is usable this turn rather
    // than a turn later -- and starting exactly one subscription per
    // established context.
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let contexts = graph.contexts().count();
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();

    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    // This attempt has no filter channel and no rotatable credential, so its
    // per-turn owner set is empty -- but it still has to say so. The runner
    // refuses to turn until the set is declared, which is what keeps the
    // production installation from being forgotten again.
    round.seal_pumps();

    let first = round.turn().expect("a turn on a fresh attempt");
    assert!(
        first.operations > 0,
        "the first turn owes an admission request to every context"
    );
    assert!(!first.is_idle());

    for intent in released_admissions(&sink) {
        acks.publish(admission_ack(&intent));
    }
    round
        .turn()
        .expect("admission receipts release every establish");
    for intent in released_establishes(&sink) {
        acks.publish(establish_ack(&intent));
    }
    let settled = round.turn().expect("a turn that settles every establish");
    assert_eq!(settled.acknowledgements, contexts);
    let ensured = subscriptions.ensured.lock().expect("ledger").len();
    assert_eq!(
        ensured, contexts,
        "every established context needs its one subscription"
    );

    // `ensure` is idempotent, so a second turn does not start a second
    // subscription for a context that already has one -- but the runner still
    // calls it, because a context that lost its transport has to be restarted
    // by exactly this path.
    let second = round.turn().expect("a second turn");
    assert!(second.acknowledgements == 0, "nothing answered yet");
    assert_eq!(
        subscriptions.ensured.lock().expect("ledger").len(),
        ensured + contexts
    );

    // An attempt that has only sent its establishes reports nothing finished.
    // These are the verdicts the caller reads instead of inferring completion
    // from the loop having run, which is how a query reports success it cannot
    // substantiate.
    assert!(!round.client_visible_completion());
    assert!(!round.attempt_drained());
    assert!(round.failure_cause().is_none());
    assert_eq!(round.root_task(), round.execution().graph().root_identity());

    // The root's stream has not started, so a packet claiming to be its second
    // is a gap rather than something to absorb.
    assert!(round.consume_root_result_packet(1, false).is_err());
    let _ = round.execution_mut();
}

#[test]
fn a_turn_replaces_every_established_subscription_after_local_observation_loss() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let contexts = graph.contexts().count();
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let status_handle = harness.execution.intake().handle();
    let identity = harness.identity(harness.stage_tasks(1)[0]);
    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();
    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();

    round
        .turn()
        .expect("a turn releases context admission requests");
    for intent in released_admissions(&sink) {
        acks.publish(admission_ack(&intent));
    }
    round
        .turn()
        .expect("admission receipts release context establishes");
    for intent in released_establishes(&sink) {
        acks.publish(establish_ack(&intent));
    }
    round.turn().expect("the contexts become established");

    for _ in 0..64 {
        assert_eq!(
            status_handle.publish(StatusEvent::Published(TaskStatus::created(identity))),
            StatusIntakeAdmission::Enqueued
        );
    }
    assert_eq!(
        status_handle.publish(StatusEvent::Published(TaskStatus::created(identity))),
        StatusIntakeAdmission::Overflowed
    );

    let report = round
        .turn()
        .expect("observation loss replaces the subscriptions");
    assert_eq!(report.resubscriptions, contexts);
    let resubscribed = subscriptions
        .resubscribed
        .lock()
        .expect("resubscription ledger");
    assert_eq!(resubscribed.len(), contexts);
    assert!(
        resubscribed
            .iter()
            .all(|(_, cursor_count)| *cursor_count > 0),
        "every replacement replays the runner's task cursors"
    );
}

#[test]
fn the_two_start_gates_are_observations_of_acknowledgements_not_of_sending() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    // The coordinator closes its pre-ready retry window on these two, so each
    // has to mean "the backend answered", not "the frontend asked". A round
    // that has only released its establishes is exactly the state in which a
    // replaced backend may still be discovered, and closing the window there
    // would strand the attempt on a process that is gone.
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let mut harness = Harness::from_graph(graph);

    // Nothing acknowledged yet.
    let released = harness.released();
    assert!(
        released
            .iter()
            .any(|intent| matches!(intent.kind(), OperationKind::UpdateQueryContext)),
        "a fresh attempt owes an establish"
    );
    let subscriptions = Arc::new(RecordingSubscriptions::default())
        as Arc<dyn crate::task_execution::round::StatusSubscriptions>;
    {
        let probe = TaskRound::new(
            harness.execution,
            TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>),
            Box::new(FakeEstablish),
            Arc::clone(&subscriptions),
        );
        assert!(
            !probe.contexts_established(),
            "a released establish is not an acknowledged one"
        );
        assert!(!probe.tasks_created());
        harness.execution = probe.into_execution();
    }

    // Answer every establish, then every create.
    for intent in &released {
        if matches!(intent.kind(), OperationKind::UpdateQueryContext) {
            harness.context_ack(intent, Duration::from_secs(30));
        }
    }
    let creates = released
        .iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .cloned()
        .collect::<Vec<_>>();
    assert!(!creates.is_empty(), "the attempt owes creates as well");
    {
        let probe = TaskRound::new(
            harness.execution,
            TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>),
            Box::new(FakeEstablish),
            Arc::clone(&subscriptions),
        );
        assert!(
            probe.contexts_established(),
            "every context answered its establish"
        );
        assert!(
            !probe.tasks_created(),
            "no create has been acknowledged yet"
        );
        harness.execution = probe.into_execution();
    }

    for intent in &creates {
        harness
            .create_ack(intent, OperationOutcome::Accepted)
            .expect("a create acknowledgement settles");
    }
    let probe = TaskRound::new(
        harness.execution,
        TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>),
        Box::new(FakeEstablish),
        subscriptions,
    );
    assert!(probe.contexts_established());
    assert!(
        probe.tasks_created(),
        "every task answered its create, so the attempt finished starting"
    );
}

#[test]
fn a_status_several_versions_ahead_is_adopted_rather_than_called_an_illegal_jump() {
    // A subscription's catch-up replays only the latest version per cursor, so
    // an observation loss and its resubscription hand this owner a version
    // several ahead of the one it holds. The states in between existed and
    // were passed through; they were simply not seen.
    //
    // Judging that jump by the adjacent-transition table refuses it, and the
    // refusal fails the whole query -- so a dropped status stream, which is a
    // recoverable observation problem by construction, became a lost query.
    // Measured with the task-status-subscription-drop fault: "task execution
    // did not advance: task state PLANNED may not become FINISHED".
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let mut harness = Harness::from_graph(graph);
    let leaf = harness.stage_tasks(1)[0];
    let identity = harness.identity(leaf);

    let status_at = |version: u64, state: TaskState| {
        TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(version).expect("a nonzero version"),
            state,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("a legal status snapshot")
    };
    let publish = |harness: &Harness, status| {
        harness
            .execution
            .intake()
            .handle()
            .publish(StatusEvent::Published(status));
    };

    // Held at PLANNED first, so there is something for the jump to be judged
    // against: with no held status the transition table is never consulted and
    // this case would pass either way.
    publish(&harness, status_at(1, TaskState::Planned));
    let held = harness
        .execution
        .apply_status(8)
        .expect("the first snapshot is adopted");
    assert_eq!(held.accepted, 1);
    assert_eq!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .task_state(),
        TaskState::Planned
    );

    // Version five, not two: every version between was published while nothing
    // was listening. PLANNED to FINISHED is illegal between neighbours and
    // unremarkable across a gap.
    publish(&harness, status_at(5, TaskState::Finished));
    let report = harness
        .execution
        .apply_status(8)
        .expect("a version several ahead is a gap, not an illegal transition");
    assert_eq!(
        report.accepted, 1,
        "the skipped-ahead status was accepted, not refused"
    );
    assert_eq!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .task_state(),
        TaskState::Finished
    );
}

#[test]
fn a_terminal_marker_after_a_task_took_its_splits_is_admitted() {
    // The sender gives every task of a plan node the terminal marker, so a
    // task whose splits all arrived in an earlier batch receives a final
    // message that re-offers its range and adds the seal. Judging it by the
    // range alone calls it a regression, the frontend refuses to send it, and
    // that task's scan waits forever for a terminal it was already told about.
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let mut harness = Harness::from_graph(graph);
    let leaf = harness.stage_tasks(1)[0];

    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("a first batch is admitted");

    // The same range again, now carrying the seal.
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, true))
        .expect("the terminal marker for an already-accepted range is admitted");

    // The relaxation is wider than the seal, and the evidence forced it: a
    // seal re-offered after it was applied showed up on a real cluster as
    // `offered=1..=1 no_more=true` refused as non-monotonic. Anything that
    // adds nothing is settled, sealed or not.
    harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect("a re-offered range that adds nothing is settled");
}

// ---------------------------------------------------------------------------
// The per-turn owner seam, and the two loops that hang on it
// ---------------------------------------------------------------------------

/// One feedback channel whose only authorized publisher is `process`.
fn feedback_declaration(
    process: BackendProcessId,
) -> crate::runtime_filter::install_encoder::FrontendRuntimeFilterFeedbackDeclaration {
    use crate::runtime_filter::install_encoder::{
        FrontendRuntimeFilterFeedbackChannel, FrontendRuntimeFilterFeedbackDeclaration,
        FrontendRuntimeFilterFeedbackPublisherOwner, FrontendRuntimeFilterFeedbackPublisherSlot,
        FrontendRuntimeFilterFeedbackScanBinding, FrontendRuntimeFilterFeedbackWaitEligibility,
    };

    FrontendRuntimeFilterFeedbackDeclaration::new([FrontendRuntimeFilterFeedbackChannel {
        channel_id: 7,
        contract_digest: [9; 32],
        max_encoded_domain_bytes: 64 * 1024,
        publishers: vec![FrontendRuntimeFilterFeedbackPublisherSlot {
            participant_id: 5,
            backend_process_id: process,
            owner: FrontendRuntimeFilterFeedbackPublisherOwner::Aggregator,
        }],
        scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
            fragment_id: LEAF_FRAGMENT,
            plan_node_id: SCAN_NODE,
            binding_id: 7,
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }],
        wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
    }])
    .expect("a legal declaration")
}

/// The canonical encoding of a one-value membership domain.
fn exact_domain(value: i64) -> Vec<u8> {
    use novarocks_execution::runtime_filter::contribution::{MembershipValues, ValueDomainDelta};
    use novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomain;

    RuntimeFilterFeedbackDomain::Exact(ValueDomainDelta::new(
        MembershipValues::int64([value]),
        false,
    ))
    .encode(64 * 1024)
    .expect("a canonical domain")
}

/// The envelope a carrier task retains for one channel's terminal domain.
fn feedback_envelope(
    channel_id: u32,
    digest: [u8; 32],
    payload: Vec<u8>,
) -> crate::runtime_filter::feedback::TaskRuntimeFilterFeedback {
    use novarocks_proto_models::filter;

    let execution = execution_id();
    let envelope = filter::RuntimeFilterEnvelope {
        kind: filter::RuntimeFilterEnvelopeKind::DegradedLogical as i32,
        query_id: Some(novarocks_proto_models::common::UniqueId {
            hi: execution.query_id().high(),
            lo: execution.query_id().low(),
        }),
        channel_id,
        deployment_epoch: execution.attempt_id().get(),
        route_identity: None,
        schema_digest: digest.to_vec(),
        payload,
        producer_open: None,
    };
    crate::runtime_filter::feedback::TaskRuntimeFilterFeedback::parse(&envelope)
        .expect("a legal feedback envelope")
}

/// A scripted dynamic filter transport that records every read it answered.
#[derive(Default)]
struct RecordingFilterReads {
    answers: Mutex<
        BTreeMap<
            TaskId,
            std::collections::VecDeque<
                Result<
                    crate::native::fragment_transport::DynamicFilterRead,
                    crate::native::fragment_transport::DynamicFilterReadError,
                >,
            >,
        >,
    >,
    requested: Mutex<Vec<(TaskId, Option<DomainVersion>)>>,
}

impl RecordingFilterReads {
    fn script(
        &self,
        task_id: TaskId,
        answer: Result<
            crate::native::fragment_transport::DynamicFilterRead,
            crate::native::fragment_transport::DynamicFilterReadError,
        >,
    ) {
        self.answers
            .lock()
            .expect("scripted answers")
            .entry(task_id)
            .or_default()
            .push_back(answer);
    }

    fn requested(&self) -> Vec<(TaskId, Option<DomainVersion>)> {
        self.requested.lock().expect("requested reads").clone()
    }
}

impl crate::task_execution::feedback_pump::TaskDynamicFilterReads for RecordingFilterReads {
    fn dynamic_filters(
        &self,
        identity: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    ) -> Result<
        crate::native::fragment_transport::DynamicFilterRead,
        crate::native::fragment_transport::DynamicFilterReadError,
    > {
        self.requested
            .lock()
            .expect("requested reads")
            .push((identity.task_id(), acknowledged));
        self.answers
            .lock()
            .expect("scripted answers")
            .get_mut(&identity.task_id())
            .and_then(std::collections::VecDeque::pop_front)
            .unwrap_or_else(|| {
                Ok(crate::native::fragment_transport::DynamicFilterRead::new(
                    None,
                    Vec::new(),
                ))
            })
    }
}

fn round_of(harness: Harness) -> (TaskRoundForTest, Arc<CountingWake>) {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let wake = Arc::clone(&harness.wake);
    let round = TaskRound::new(
        harness.execution,
        TaskAckIntake::new(Arc::clone(&wake) as Arc<dyn StatusIntakeWake>),
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default())
            as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    )
    .with_connector_blocking_io(test_connector_blocking_io());
    (round, wake)
}

async fn actor_abort_round_at_first_dispatch() -> (
    crate::task_execution::round::TaskRound,
    Arc<SwitchableQueueSink>,
    crate::native::task_transport::TaskAckIntakeHandle,
    novarocks_query_application::coordination::LogicalExecutionActorOwner,
    novarocks_query_application::coordination::LogicalExecutionActor,
    QueryContextRef,
    OperationIntent,
    Arc<ManualClock>,
) {
    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionTicketId, LeaseValidFor,
        QueryContextAdmissionTicketReceipt,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, AdmissionIssueSettlement, ExecutionEffect,
        LogicalExecutionActorConfig, spawn_logical_execution_actor,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::abort_effect::NativeAbortEffectAdapter;
    use crate::task_execution::round::TaskRound;

    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let exact_context = *graph.contexts().next().expect("one attempt context");
    let admission_epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(SwitchableQueueSink::default());
    let wake = Arc::new(CountingWake::default());
    let clock = Arc::new(ManualClock::new());
    let status = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::new(16, 12, 1, 4).expect("nonzero dispatch budget"),
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        Arc::clone(&clock) as Arc<dyn TaskProtocolClock>,
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        status,
    )
    .expect("compose execution");
    let acks = TaskAckIntake::new(Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let ack_handle = acks.handle();
    let (adapter, abort_intake) = NativeAbortEffectAdapter::bounded(
        NonZeroUsize::new(2).unwrap(),
        Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
    );
    let mut round = TaskRound::new(
        execution,
        acks,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    )
    .with_abort_effect_intake(abort_intake);
    round.seal_pumps();
    round.turn().expect("startup releases admission");
    let admission = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .find(|intent| {
            matches!(
                intent.kind(),
                OperationKind::AcquireQueryContextAdmissionTicket
            )
        })
        .expect("startup releases admission");

    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 30,
            control_bytes: 1 << 20,
            per_scope_bytes: 1 << 28,
        },
    )
    .expect("valid workload control");
    control.mark_ready().expect("workload control ready");
    let governed = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .expect("query work admitted");
    let execution_stage = governed
        .owner
        .scope()
        .try_acquire(Stage::Execution)
        .expect("execution stage admitted");
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        execution_id(),
        ExecutionEffect::None,
        NonZeroUsize::new(2).unwrap(),
        vec![exact_context],
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        governed.owner,
        execution_stage,
    )
    .expect("valid actor configuration")
    .with_abort_query_context_effect_port(
        Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
        NonZeroUsize::new(3).unwrap(),
    );
    let (owner, initial) =
        spawn_logical_execution_actor(&tokio::runtime::Handle::current(), config)
            .expect("spawn actor");
    let actor = owner.actor().clone();
    let running = actor.activate(initial.ready()).await.expect("activate");
    let actor_admission = AcquireQueryContextAdmissionTicket::new(
        TaskOperationId::new_v7(),
        exact_context,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([7; 32]),
        AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
    );
    let activation = running.identity();
    let pending = running
        .begin_admission_issue(actor_admission)
        .await
        .expect("begin actor admission");
    drop(running);
    actor
        .settle_late_admission_issue(
            activation,
            pending,
            AdmissionIssueSettlement::applied(
                pending.operation_id(),
                OperationOutcome::Accepted,
                QueryContextAdmissionTicketReceipt::new(
                    AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                    exact_context,
                    LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                ),
            )
            .unwrap(),
        )
        .await
        .expect("settle actor admission");
    for _ in 0..100 {
        if round.queued_abort_effects() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    ack_handle.publish(admission_ack(&admission));
    round
        .turn()
        .expect("admission ACK frees lifecycle capacity");
    let first_abort = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .find(|intent| matches!(intent.kind(), OperationKind::AbortQueryContext))
        .expect("first actor Abort reaches transport");
    (
        round,
        sink,
        ack_handle,
        owner,
        actor,
        exact_context,
        first_abort,
        clock,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_abort_waits_for_real_lifecycle_capacity_and_replays_exactly() {
    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionTicketId, LeaseValidFor,
        QueryContextAdmissionTicketReceipt,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, AdmissionIssueSettlement, ContextClosureState,
        ExecutionEffect, LogicalExecutionActorConfig, spawn_logical_execution_actor,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::abort_effect::NativeAbortEffectAdapter;
    use crate::task_execution::round::TaskRound;

    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let exact_context = *graph.contexts().next().expect("one attempt context");
    let admission_epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(SwitchableQueueSink::default());
    let wake = Arc::new(CountingWake::default());
    let status = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::new(16, 12, 1, 4).expect("nonzero dispatch budget"),
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        Arc::new(ManualClock::new()),
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        status,
    )
    .expect("compose execution");
    let acks = TaskAckIntake::new(Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let ack_handle = acks.handle();
    let (adapter, abort_intake) = NativeAbortEffectAdapter::bounded(
        NonZeroUsize::new(2).unwrap(),
        Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
    );
    let mut round = TaskRound::new(
        execution,
        acks,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    )
    .with_abort_effect_intake(abort_intake);
    round.seal_pumps();

    // Occupy the only real lifecycle permit with TaskRound's admission. An
    // adapter slot exists independently and must not be mistaken for it.
    round.turn().expect("startup releases one admission");
    let mut startup = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    let admission = startup
        .iter()
        .find(|intent| {
            matches!(
                intent.kind(),
                OperationKind::AcquireQueryContextAdmissionTicket
            )
        })
        .cloned()
        .expect("startup releases admission");

    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 30,
            control_bytes: 1 << 20,
            per_scope_bytes: 1 << 28,
        },
    )
    .expect("valid workload control");
    control.mark_ready().expect("workload control ready");
    let governed = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .expect("query work admitted");
    let execution_stage = governed
        .owner
        .scope()
        .try_acquire(Stage::Execution)
        .expect("execution stage admitted");
    let config = LogicalExecutionActorConfig::single_attempt_completion(
        execution_id(),
        ExecutionEffect::None,
        NonZeroUsize::new(2).unwrap(),
        vec![exact_context],
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        governed.owner,
        execution_stage,
    )
    .expect("valid actor configuration")
    .with_abort_query_context_effect_port(
        Arc::clone(&adapter) as Arc<dyn AbortQueryContextEffectPort>,
        NonZeroUsize::new(2).unwrap(),
    );
    let (owner, initial) =
        spawn_logical_execution_actor(&tokio::runtime::Handle::current(), config)
            .expect("spawn actor");
    let actor = owner.actor().clone();
    let running = actor.activate(initial.ready()).await.expect("activate");
    let actor_admission = AcquireQueryContextAdmissionTicket::new(
        TaskOperationId::new_v7(),
        exact_context,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([7; 32]),
        AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
    );
    let activation = running.identity();
    let pending = running
        .begin_admission_issue(actor_admission)
        .await
        .expect("begin actor admission");
    drop(running);
    actor
        .settle_late_admission_issue(
            activation,
            pending,
            AdmissionIssueSettlement::applied(
                pending.operation_id(),
                OperationOutcome::Accepted,
                QueryContextAdmissionTicketReceipt::new(
                    AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                    exact_context,
                    LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                ),
            )
            .unwrap(),
        )
        .await
        .expect("settle actor admission");
    for _ in 0..100 {
        if round.queued_abort_effects() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(round.queued_abort_effects(), 1, "actor publishes one Abort");

    let blocked = round
        .turn()
        .expect("full lifecycle lane leaves the adapter untouched");
    assert_eq!(blocked.abort_effects, 0);
    assert_eq!(round.queued_abort_effects(), 1);

    sink.set_queue_open(false);
    ack_handle.publish(admission_ack(&admission));
    let process_blocked = round
        .turn()
        .expect("process backpressure leaves the adapter untouched");
    assert_eq!(process_blocked.abort_effects, 0);
    assert_eq!(round.queued_abort_effects(), 1);

    sink.set_queue_open(true);
    let admitted = round
        .turn()
        .expect("the freed lifecycle permit admits the actor Abort");
    assert_eq!(admitted.abort_effects, 1);
    assert_eq!(round.queued_abort_effects(), 0);
    startup = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect();
    let first_abort = startup
        .iter()
        .find(|intent| matches!(intent.kind(), OperationKind::AbortQueryContext))
        .cloned()
        .expect("priority lifecycle dispatch releases the actor Abort");
    assert_eq!(startup[0].operation_id(), first_abort.operation_id());
    let operation_id = first_abort.operation_id();

    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    round
        .turn()
        .expect("unknown outcome is published to the actor");

    let mut replay = None;
    for _ in 0..100 {
        for intent in sink
            .take()
            .into_iter()
            .flat_map(|(_, operations)| operations)
        {
            match intent.kind() {
                OperationKind::AbortQueryContext => replay = Some(intent),
                OperationKind::UpdateQueryContext => {
                    ack_handle.publish(establish_ack(&intent));
                }
                _ => {}
            }
        }
        if replay.is_some() {
            break;
        }
        round.turn().expect("bounded retry turn");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let replay = replay.expect("the actor replays after unknown outcome");
    assert_eq!(replay.operation_id(), operation_id);
    let OperationIntent::AbortQueryContext(first_request) = first_abort else {
        unreachable!()
    };
    let OperationIntent::AbortQueryContext(replay_request) = replay else {
        unreachable!()
    };
    assert_eq!(
        replay_request.envelope().operation_id(),
        first_request.envelope().operation_id()
    );
    assert_eq!(replay_request.context(), first_request.context());
    assert_eq!(replay_request.cause(), first_request.cause());

    // A definitive receipt from the original send races with replay gen2
    // already in flight. It may settle the same exact logical operation; the
    // later gen2 transport callback must then hit the closing tombstone rather
    // than fail as UnknownOperation.
    ack_handle.publish(OperationAcknowledgement::worker_receipt(
        operation_id,
        OperationKind::AbortQueryContext,
        OperationOutcome::ContextTerminalReceipt,
        AckPayload::Context(QueryContextReceipt::new(
            exact_context,
            QueryContextState::TerminalRetained,
        )),
    ));
    let closed = round
        .turn()
        .expect("the original receipt closes the in-flight exact replay");
    assert_eq!(closed.acknowledgements, 1);
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    let tombstoned = round
        .turn()
        .expect("replay gen2's later callback is absorbed by the exact closing tombstone");
    assert_eq!(tombstoned.acknowledgements, 1);
    for _ in 0..100 {
        if actor
            .stand_down_snapshot(exact_context)
            .await
            .expect("stand-down snapshot")
            .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        actor
            .stand_down_snapshot(exact_context)
            .await
            .unwrap()
            .unwrap()
            .closure(),
        ContextClosureState::TerminalRetained
    );
    assert_eq!(
        round.queued_abort_effects(),
        0,
        "the late receipt settles before another replay enters the adapter"
    );
    assert!(
        sink.take()
            .into_iter()
            .flat_map(|(_, operations)| operations)
            .all(|intent| intent.kind() != OperationKind::AbortQueryContext),
        "the settled actor must not leave another Abort in process dispatch"
    );
    let supervisor = owner.into_residual_stand_down_supervisor();
    drop(actor);
    supervisor
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    supervisor.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_late_receipt_consumes_an_adapter_queued_replay() {
    use novarocks_query_application::coordination::ContextClosureState;

    let (mut round, sink, ack_handle, owner, actor, exact_context, first_abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    let operation_id = first_abort.operation_id();
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    round.turn().expect("first Abort becomes transport-unknown");
    for _ in 0..100 {
        if round.queued_abort_effects() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(round.queued_abort_effects(), 1, "replay waits in adapter");

    ack_handle.publish(OperationAcknowledgement::worker_receipt(
        operation_id,
        OperationKind::AbortQueryContext,
        OperationOutcome::ContextTerminalReceipt,
        AckPayload::Context(QueryContextReceipt::new(
            exact_context,
            QueryContextState::TerminalRetained,
        )),
    ));
    let settled = round
        .turn()
        .expect("late receipt settles and consumes the adapter replay");
    assert_eq!(settled.acknowledgements, 1);
    assert_eq!(settled.abort_effects, 1);
    assert_eq!(round.queued_abort_effects(), 0);
    assert!(
        sink.take()
            .into_iter()
            .flat_map(|(_, operations)| operations)
            .all(|intent| intent.kind() != OperationKind::AbortQueryContext),
        "a replay closed in the adapter must never reach process transport"
    );
    for _ in 0..100 {
        if actor
            .stand_down_snapshot(exact_context)
            .await
            .unwrap()
            .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let supervisor = owner.into_residual_stand_down_supervisor();
    drop(actor);
    supervisor
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    supervisor.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_late_receipt_cancels_a_dispatcher_queued_replay() {
    use novarocks_query_application::coordination::ContextClosureState;

    let (mut round, sink, ack_handle, owner, actor, exact_context, first_abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    let operation_id = first_abort.operation_id();
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    round.turn().expect("first Abort becomes transport-unknown");
    for _ in 0..100 {
        if round.queued_abort_effects() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(round.queued_abort_effects(), 1, "actor authorizes replay");

    // Release the real lifecycle lane, then make process submission refuse
    // the replay after TaskRound has moved it out of the adapter.
    for intent in sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
    {
        if intent.kind() == OperationKind::UpdateQueryContext {
            ack_handle.publish(establish_ack(&intent));
        }
    }
    sink.set_submit_open(false);
    let queued = round
        .turn()
        .expect("process backpressure restores replay to dispatcher");
    assert_eq!(queued.abort_effects, 1);
    assert_eq!(round.queued_abort_effects(), 0);
    assert_eq!(
        sink.live_queue_permits(),
        1,
        "dispatcher-queued replay retains its process reservation"
    );

    ack_handle.publish(OperationAcknowledgement::worker_receipt(
        operation_id,
        OperationKind::AbortQueryContext,
        OperationOutcome::ContextTerminalReceipt,
        AckPayload::Context(QueryContextReceipt::new(
            exact_context,
            QueryContextState::TerminalRetained,
        )),
    ));
    let settled = round
        .turn()
        .expect("late receipt cancels the definitely-unsent queued replay");
    assert_eq!(settled.acknowledgements, 1);
    assert_eq!(
        sink.live_queue_permits(),
        0,
        "cancelling the queued carrier returns process capacity"
    );

    sink.set_submit_open(true);
    round
        .turn()
        .expect("a later turn has no closed replay to send");
    assert!(
        sink.take()
            .into_iter()
            .flat_map(|(_, operations)| operations)
            .all(|intent| intent.kind() != OperationKind::AbortQueryContext),
        "the cancelled dispatcher replay must never reach process transport"
    );
    for _ in 0..100 {
        if actor
            .stand_down_snapshot(exact_context)
            .await
            .unwrap()
            .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    let supervisor = owner.into_residual_stand_down_supervisor();
    drop(actor);
    supervisor
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    supervisor.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_actor_abort_expiry_returns_definitely_unsent_for_bounded_replay() {
    use novarocks_query_application::coordination::{
        AbortQueryContextIssueState, ContextClosureState,
    };

    let (mut round, sink, ack_handle, owner, actor, exact_context, first_abort, clock) =
        actor_abort_round_at_first_dispatch().await;
    let operation_id = first_abort.operation_id();
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    round.turn().expect("first Abort becomes transport-unknown");
    for _ in 0..100 {
        if round.queued_abort_effects() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(round.queued_abort_effects(), 1, "actor authorizes replay");

    for intent in sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
    {
        if intent.kind() == OperationKind::UpdateQueryContext {
            ack_handle.publish(establish_ack(&intent));
        }
    }
    sink.set_submit_open(false);
    let queued = round
        .turn()
        .expect("process backpressure retains replay in dispatcher");
    assert_eq!(queued.abort_effects, 1);
    assert_eq!(round.queued_abort_effects(), 0);
    clock.advance(TransportBudget::DEFAULT.frontend_queue_residence());
    let error = round
        .turn()
        .expect_err("expired actor Abort is a typed local failure");
    assert!(matches!(
        error,
        TaskExecutionError::QueueResidenceExpired {
            kind: OperationKind::AbortQueryContext,
            ..
        }
    ));
    for _ in 0..100 {
        if round.queued_abort_effects() == 1
            && actor
                .stand_down_snapshot(exact_context)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.closure() == ContextClosureState::AbortPending
                        && snapshot.issue_state()
                            == Some(AbortQueryContextIssueState::TransportOwned)
                })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        round.queued_abort_effects(),
        1,
        "definitely-unsent returns the exact Abort to bounded actor replay"
    );
    let supervisor = owner.into_residual_stand_down_supervisor();
    drop(actor);
    supervisor
        .observe_worker_process_replaced(exact_context)
        .await
        .unwrap();
    supervisor.join().await.unwrap();
}

#[test]
fn closed_actor_abort_intake_fails_the_active_round() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::abort_effect::NativeAbortEffectAdapter;
    use crate::task_execution::round::TaskRound;

    let harness = Harness::new(&[0], &[0], 64);
    let wake = Arc::clone(&harness.wake);
    let (adapter, abort_intake) = NativeAbortEffectAdapter::bounded(
        NonZeroUsize::MIN,
        Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
    );
    drop(adapter);
    let mut round = TaskRound::new(
        harness.execution,
        TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>),
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    )
    .with_abort_effect_intake(abort_intake);
    round.seal_pumps();

    let error = round
        .turn()
        .expect_err("a disconnected effect owner cannot be treated as no work");
    assert!(error.to_string().contains("Abort effect intake closed"));
}

type TaskRoundForTest = crate::task_execution::round::TaskRound;

fn round_waiting_on_creates(
    harness: Harness,
) -> (
    TaskRoundForTest,
    crate::task_execution::status_intake::StatusIntakeHandle,
    crate::native::task_transport::TaskAckIntakeHandle,
    Vec<OperationIntent>,
) {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let expected_creates = harness.execution.graph().tasks().count();
    let sink = Arc::clone(&harness.sink);
    let status = harness.execution.intake().handle();
    let intake = TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();
    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default())
            as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();

    let mut creates = Vec::new();
    for _ in 0..8 {
        if creates.len() == expected_creates {
            break;
        }
        round
            .turn()
            .expect("a bounded startup turn releases task work");
        let released = sink
            .take()
            .into_iter()
            .flat_map(|(_, operations)| operations)
            .collect::<Vec<_>>();
        for intent in released {
            match intent.kind() {
                OperationKind::AcquireQueryContextAdmissionTicket => {
                    acks.publish(admission_ack(&intent));
                }
                OperationKind::UpdateQueryContext => acks.publish(establish_ack(&intent)),
                OperationKind::CreateTask => creates.push(intent),
                kind => panic!("attempt startup unexpectedly released {kind}"),
            }
        }
    }
    assert_eq!(
        creates.len(),
        expected_creates,
        "every scheduled task must release its exact create"
    );
    (round, status, acks, creates)
}

#[test]
fn result_pump_gate_remembers_create_ack_after_root_skips_created() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let _projection = round
        .take_root_status_source()
        .expect("one result owner takes the root projection");
    assert!(round.take_root_status_source().is_none());

    let root = round.root_task();
    let finished = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(2).expect("v2"),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .expect("a valid finished status");
    assert_eq!(
        status.publish(StatusEvent::Published(finished)),
        StatusIntakeAdmission::Enqueued
    );
    round
        .turn()
        .expect("a terminal root status may race ahead of create ACK");
    assert!(!round.result_pump_ready());

    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round
        .turn()
        .expect("the exact create ACK completes the historical gate");
    assert!(round.result_pump_ready());
    assert_eq!(
        round
            .execution()
            .task(root.task_id())
            .expect("the root remains owned")
            .state(),
        RemoteTaskState::Terminal,
        "the gate must not require the transient Created lifecycle state"
    );
}

#[test]
fn root_pending_failure_refines_at_the_same_status_version() {
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let _projection = round
        .take_root_status_source()
        .expect("the result owner takes one projection");
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().expect("all create ACKs settle");
    assert!(round.result_pump_ready());

    let root = round.root_task();
    let root_derived = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Aborted,
        Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
        TaskOutputFacts::new(false),
    )
    .expect("a valid derived terminal");
    assert_eq!(
        status.publish(StatusEvent::Published(root_derived)),
        StatusIntakeAdmission::Enqueued
    );
    round
        .turn()
        .expect("the root's derived failure is published as pending");
    assert!(
        round
            .failure_cause()
            .is_some_and(TerminationDetail::is_derived)
    );

    let origin = round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .find(|identity| *identity != root)
        .expect("the chain has a non-root task");
    let authoritative =
        TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
            novarocks_execution::task_execution::TaskFailureCategory::Execution,
            novarocks_execution::task_execution::SafeDetail::new("originating failure")
                .expect("safe detail"),
        ));
    let origin_failed = TaskStatus::try_new(
        origin,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Failed,
        Some(authoritative.clone()),
        TaskOutputFacts::new(false),
    )
    .expect("a valid originating failure");
    assert_eq!(
        status.publish(StatusEvent::Published(origin_failed)),
        StatusIntakeAdmission::Enqueued
    );
    round
        .turn()
        .expect("the same root version refines to the authoritative attempt cause");
    assert_eq!(round.failure_cause(), Some(&authoritative));
}

#[test]
fn finished_root_projection_is_refined_by_a_later_attempt_failure() {
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let _projection = round
        .take_root_status_source()
        .expect("the result owner takes one projection");
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().expect("all create ACKs settle");

    let root = round.root_task();
    let non_roots = round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .filter(|identity| *identity != root)
        .collect::<Vec<_>>();
    let derived_origin = non_roots[0];
    let authoritative_origin = *non_roots
        .get(1)
        .expect("the chain has another upstream task");
    let finished = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .expect("a valid finished root");
    status.publish(StatusEvent::Published(finished));
    let derived = TaskStatus::try_new(
        derived_origin,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Aborted,
        Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
        TaskOutputFacts::new(false),
    )
    .expect("a valid derived upstream terminal");
    status.publish(StatusEvent::Published(derived));
    round
        .turn()
        .expect("the Finished root is atomically published with a pending attempt failure");
    assert!(
        round
            .failure_cause()
            .is_some_and(TerminationDetail::is_derived)
    );
    assert!(
        !round.root_success_sealed(),
        "an upstream derived failure must freeze success until its cause is authoritative"
    );

    let authoritative =
        TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
            novarocks_execution::task_execution::TaskFailureCategory::Execution,
            novarocks_execution::task_execution::SafeDetail::new("late upstream failure")
                .expect("safe detail"),
        ));
    let origin_failed = TaskStatus::try_new(
        authoritative_origin,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Failed,
        Some(authoritative.clone()),
        TaskOutputFacts::new(false),
    )
    .expect("a valid originating failure");
    status.publish(StatusEvent::Published(origin_failed));
    round
        .turn()
        .expect("the same Finished root snapshot accepts attempt-failure refinement");
    assert_eq!(round.failure_cause(), Some(&authoritative));
}

#[test]
fn a_runner_refuses_to_turn_until_its_per_turn_owners_are_declared() {
    // The defect this catches, and it is the reason the check exists at all:
    // two fully built, fully unit-tested loops -- dynamic filter feedback and
    // credential rotation -- were never handed to a runner, and every query
    // ran clean. A missing pump is now a query failure on the first turn
    // rather than a loop that silently never runs.
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, _wake) = round_of(harness);

    let error = round
        .turn()
        .expect_err("a runner with undeclared owners must not turn");
    assert!(
        error
            .to_string()
            .contains("per-turn owners were never declared"),
        "{error}"
    );

    // Declaring zero is legal and explicit: an attempt with no filter channel
    // and no rotatable credential really has nothing to drive.
    round.seal_pumps();
    round.turn().expect("a declared attempt turns");
}

#[test]
fn installing_the_attempt_pumps_supplies_both_feedback_loops() {
    use crate::coordinator::task_round::{AttemptPumps, install_attempt_pumps};
    use crate::native::task_transport::installed_attempt_pumps;
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::feedback_pump::DynamicFilterFeedbackPump;

    // The defect this catches: an owner that is constructed and then dropped.
    // Both loops were exactly that, and no test of either could see it,
    // because each component was individually correct. This asserts the
    // *supply*: the one production installation path hands both to a runner.
    let harness = Harness::new(&[0], &[0], 64);
    let publisher = harness
        .identity(TaskId::new(1).expect("nonzero"))
        .backend_process_id();
    let (mut round, _wake) = round_of(harness);

    let declaration = feedback_declaration(publisher);
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), declaration.clone()).expect("state"),
    );
    let (storage, credential) = refreshable_credential_storage();

    let before_filters = installed_attempt_pumps(DynamicFilterFeedbackPump::PUMP_NAME);
    let before_credential = installed_attempt_pumps(CredentialRotationPump::PUMP_NAME);

    let rotation = install_attempt_pumps(
        &mut round,
        AttemptPumps {
            execution_id: execution_id(),
            feedback_state: Arc::clone(&feedback),
            declared_feedback_channels: declaration.channels().len(),
            reads: Arc::new(RecordingFilterReads::default())
                as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
            initial_credential: &credential,
            credential_storage: Some(Arc::clone(&storage)),
        },
    );

    assert_eq!(
        round.installed_pumps(),
        2,
        "a declared filter channel and a rotatable credential are two per-turn owners"
    );
    assert!(
        rotation.is_some(),
        "the caller must get the rotation owner back so it can stop it at completion"
    );
    assert_eq!(
        installed_attempt_pumps(DynamicFilterFeedbackPump::PUMP_NAME),
        before_filters + 1
    );
    assert_eq!(
        installed_attempt_pumps(CredentialRotationPump::PUMP_NAME),
        before_credential + 1
    );
    // And the runner will turn, which is what `seal_pumps` inside the
    // installation is for.
    round.turn().expect("an installed attempt turns");
}

#[test]
fn an_attempt_with_no_channel_and_no_vended_credential_installs_no_owner() {
    use crate::coordinator::task_round::{AttemptPumps, install_attempt_pumps};
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;

    // The other half of the supply rule: "declared zero" must stay reachable,
    // or every query without runtime filters would fail its first turn.
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, _wake) = round_of(harness);
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), Default::default()).expect("state"),
    );
    let credential = CredentialUpdate::new(
        CredentialLeaseId::new(1),
        CredentialEpoch::FIRST,
        Arc::new(FakeSecret) as Arc<dyn ConfidentialContent>,
    );

    let rotation = install_attempt_pumps(
        &mut round,
        AttemptPumps {
            execution_id: execution_id(),
            feedback_state: feedback,
            declared_feedback_channels: 0,
            reads: Arc::new(RecordingFilterReads::default())
                as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
            initial_credential: &credential,
            credential_storage: None,
        },
    );

    assert_eq!(round.installed_pumps(), 0);
    assert!(rotation.is_none());
    round.turn().expect("an attempt with no owners still turns");
}

/// One attempt credential table holding a single refreshable vended lease.
///
/// The refresher is scripted: it advances the provider epoch and pushes the
/// expiry out, which is exactly what the table's install rules require.
fn refreshable_credential_storage() -> (
    Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>,
    CredentialUpdate,
) {
    let (storage, credential, _) = refreshable_credential_storage_with_refresher(0);
    (storage, credential)
}

fn refreshable_credential_storage_with_refresher(
    remaining_ms: u64,
) -> (
    Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>,
    CredentialUpdate,
    Arc<ScriptedRefresher>,
) {
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::lifecycle::{
        CredentialLeaseSecretEnvelope, encode_credential_lease_descriptor,
        encode_credential_lease_secret_envelope,
    };
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor, CredentialLeaseProvider,
        StorageAccessDomainId, StorageCredentialScopePrefix,
    };
    use novarocks_task_codec::domain::WireCredential;

    let owner = novarocks_spi::connector::CatalogHandle::new(
        ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog id"),
        CatalogVersion::from_bytes([7; 32]),
    );
    let not_after = now_unix_ms() + remaining_ms;
    let descriptor = CredentialLeaseDescriptor::try_new(
        novarocks_spi::connector::CredentialLeaseId::try_from_bytes([3; 16]).expect("lease id"),
        1,
        owner,
        CredentialLeaseProvider::S3,
        vec![StorageCredentialScopePrefix::try_from_normalized("s3://bucket/a").expect("prefix")],
        not_after,
        true,
        StorageAccessDomainId::from_bytes([8; 32]),
    )
    .expect("a legal descriptor");
    let envelope = CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
        descriptor.lease_id(),
        1,
        "access-key".to_owned(),
        "secret".to_owned(),
        "session-token".to_owned(),
        not_after,
    )
    .expect("a legal envelope");
    let refresher = Arc::new(ScriptedRefresher::default());
    let lease = crate::query_execution::lifecycle_plan::QueryCredentialLease::try_new(
        descriptor.clone(),
        envelope.clone(),
        Some(Arc::clone(&refresher)
            as Arc<
                dyn crate::query_execution::lifecycle_plan::QueryCredentialLeaseRefresher,
            >),
    )
    .expect("a legal lease");
    let storage =
        crate::query_execution::lifecycle_plan::QueryCredentialLeases::try_new(vec![lease])
            .expect("a legal table")
            .into_attempt_storage_resolver()
            .expect("a non-empty table");

    let material = Arc::new(
        WireCredential::decode(
            &[encode_credential_lease_descriptor(&descriptor)],
            &[encode_credential_lease_secret_envelope(&envelope)],
            FieldPath::root("initial_credential"),
        )
        .expect("a legal rotation"),
    );
    let credential = CredentialUpdate::new(
        CredentialLeaseId::new(1),
        CredentialEpoch::FIRST,
        material as Arc<dyn ConfidentialContent>,
    );
    (storage, credential, refresher)
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("representable")
}

/// A credential provider that advances the epoch, or refuses on demand.
#[derive(Default)]
struct ScriptedRefresher {
    calls: std::sync::atomic::AtomicUsize,
    fail: std::sync::atomic::AtomicBool,
}

impl crate::query_execution::lifecycle_plan::QueryCredentialLeaseRefresher for ScriptedRefresher {
    fn refresh(
        &self,
        current: &novarocks_spi::connector::CredentialLeaseDescriptor,
    ) -> Result<crate::query_execution::lifecycle_plan::QueryCredentialLeaseRefresh, String> {
        use novarocks_proto_codec::lifecycle::CredentialLeaseSecretEnvelope;
        use novarocks_spi::connector::CredentialLeaseDescriptor;

        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("scripted provider failure".to_owned());
        }
        let epoch = current.epoch() + 1;
        let not_after = current.not_after_unix_ms() + 600_000;
        let descriptor = CredentialLeaseDescriptor::try_new(
            current.lease_id(),
            epoch,
            current.owner().clone(),
            current.provider(),
            current.prefixes().to_vec(),
            not_after,
            true,
            current.storage_access_domain_id(),
        )
        .map_err(|error| error.to_string())?;
        let envelope = CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
            current.lease_id(),
            epoch,
            "access-key".to_owned(),
            "rotated-secret".to_owned(),
            "session-token".to_owned(),
            not_after,
        )
        .map_err(|error| error.to_string())?;
        crate::query_execution::lifecycle_plan::QueryCredentialLeaseRefresh::try_new(
            descriptor, envelope,
        )
        .map_err(|error| error.message().to_owned())
    }
}

#[test]
fn the_feedback_reader_ingests_each_task_version_once_and_keeps_a_cursor_per_task() {
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::feedback_pump::DynamicFilterFeedbackPump;
    use crate::task_execution::round::TurnPump;

    // The consumer side of the loop. Two facts are load-bearing and neither is
    // visible from a test of the feedback state alone: the version is fetched
    // exactly once per advertisement, and the cursor is per task -- one task's
    // version must never suppress another's fetch, because versions are minted
    // per task and two tasks can both sit at version one with different
    // payloads.
    // Both tasks are placed on one backend, so both speak for the same
    // declared publisher: what is being pinned here is the per-task cursor,
    // not the per-process authorization, which has its own test.
    let mut harness = Harness::new(&[0], &[0], 64);
    let first = TaskId::new(1).expect("nonzero");
    let second = TaskId::new(2).expect("nonzero");
    let publisher = harness.identity(first).backend_process_id();
    assert_eq!(
        harness.identity(second).backend_process_id(),
        publisher,
        "this fixture places both tasks on one backend"
    );
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), feedback_declaration(publisher))
            .expect("state"),
    );
    let reads = Arc::new(RecordingFilterReads::default());
    let domain = exact_domain(41);
    reads.script(
        first,
        Ok(crate::native::fragment_transport::DynamicFilterRead::new(
            Some(DomainVersion::new(1).expect("nonzero")),
            vec![feedback_envelope(7, [9; 32], domain.clone())],
        )),
    );
    let mut pump = DynamicFilterFeedbackPump::new(
        Arc::clone(&feedback),
        Arc::clone(&reads) as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
        1,
    )
    .expect("a declared channel means there is something to read");

    // Nothing advertised yet: no read is made at all.
    assert_eq!(pump.drive(&mut harness.execution).expect("a clean turn"), 0);
    assert!(reads.requested().is_empty());

    harness.publish_with_filters(
        first,
        TaskState::Running,
        None,
        false,
        Some(DynamicFilterAdvertisement::new(
            DomainVersion::new(1).expect("nonzero"),
            1,
        )),
    );
    assert_eq!(pump.drive(&mut harness.execution).expect("a clean turn"), 1);
    assert_eq!(pump.ingested(), 1);
    assert_eq!(reads.requested(), vec![(first, None)]);
    assert!(
        !feedback.is_initial_wait_blocked(SCAN_NODE, [7]),
        "an admitted exact domain releases the split source's initial wait"
    );

    // The same advertisement is not fetched again.
    assert_eq!(pump.drive(&mut harness.execution).expect("a clean turn"), 0);
    assert_eq!(reads.requested().len(), 1);

    // A second task at the same version is its own cursor. A shared cursor
    // would silently drop this one. It republishes the identical domain, which
    // admission treats as idempotent rather than as a conflicting winner.
    reads.script(
        second,
        Ok(crate::native::fragment_transport::DynamicFilterRead::new(
            Some(DomainVersion::new(1).expect("nonzero")),
            vec![feedback_envelope(7, [9; 32], domain.clone())],
        )),
    );
    harness.publish_with_filters(
        second,
        TaskState::Running,
        None,
        false,
        Some(DynamicFilterAdvertisement::new(
            DomainVersion::new(1).expect("nonzero"),
            1,
        )),
    );
    assert_eq!(pump.drive(&mut harness.execution).expect("a clean turn"), 1);
    assert_eq!(
        reads.requested(),
        vec![(first, None), (second, None)],
        "the second task must be read even though the first already settled version one"
    );
}

#[test]
fn a_refused_dynamic_filter_read_fails_the_attempt_and_a_lost_one_is_retried() {
    use crate::native::fragment_transport::DynamicFilterReadError;
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::feedback_pump::DynamicFilterFeedbackPump;
    use crate::task_execution::round::TurnPump;

    // The two failures are different facts. A read that did not complete costs
    // pruning and is retried; a read the backend *refused* says this frontend
    // and that backend disagree about the task, and swallowing it would hide a
    // real protocol disagreement behind a silent loss of pruning.
    let mut harness = Harness::new(&[0], &[0], 64);
    let task = TaskId::new(1).expect("nonzero");
    let publisher = harness.identity(task).backend_process_id();
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), feedback_declaration(publisher))
            .expect("state"),
    );
    let reads = Arc::new(RecordingFilterReads::default());
    reads.script(
        task,
        Err(DynamicFilterReadError::Unavailable("no channel".to_owned())),
    );
    reads.script(
        task,
        Err(DynamicFilterReadError::Refused("not observable".to_owned())),
    );
    let mut pump = DynamicFilterFeedbackPump::new(
        feedback,
        Arc::clone(&reads) as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
        1,
    )
    .expect("a declared channel");

    harness.publish_with_filters(
        task,
        TaskState::Running,
        None,
        false,
        Some(DynamicFilterAdvertisement::new(
            DomainVersion::new(1).expect("nonzero"),
            1,
        )),
    );

    assert_eq!(
        pump.drive(&mut harness.execution)
            .expect("a lost read is not a query failure"),
        0
    );
    let error = pump
        .drive(&mut harness.execution)
        .expect_err("a refused read is a disagreement, not a lost optimization");
    assert!(error.to_string().contains("was refused"), "{error}");
    assert_eq!(
        reads.requested().len(),
        2,
        "the cursor stays put after a lost read, so the next turn asks again"
    );
}

#[test]
fn an_advertised_version_that_is_no_longer_retained_stops_being_asked_for() {
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::feedback_pump::DynamicFilterFeedbackPump;
    use crate::task_execution::round::TurnPump;

    // A carrier task that went terminal keeps no payload, and the read is
    // settled with version zero. Leaving the cursor behind would re-ask every
    // turn for a payload that is gone, which is a busy loop for the whole
    // remaining life of the query.
    let mut harness = Harness::new(&[0], &[0], 64);
    let task = TaskId::new(1).expect("nonzero");
    let publisher = harness.identity(task).backend_process_id();
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), feedback_declaration(publisher))
            .expect("state"),
    );
    let reads = Arc::new(RecordingFilterReads::default());
    let mut pump = DynamicFilterFeedbackPump::new(
        feedback,
        Arc::clone(&reads) as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
        1,
    )
    .expect("a declared channel");

    harness.publish_with_filters(
        task,
        TaskState::Running,
        None,
        false,
        Some(DynamicFilterAdvertisement::new(
            DomainVersion::new(4).expect("nonzero"),
            1,
        )),
    );
    // The default scripted answer is a settled empty read.
    assert_eq!(pump.drive(&mut harness.execution).expect("a turn"), 0);
    assert_eq!(pump.drive(&mut harness.execution).expect("a turn"), 0);
    assert_eq!(
        reads.requested().len(),
        1,
        "an unretained version is asked for once, not on every turn"
    );
}

/// Answers one released credential advance with the epoch it carried.
fn credential_ack(
    execution: &mut QueryTaskExecution,
    observer: &dyn crate::task_execution::round::AcknowledgementObserver,
    intent: &OperationIntent,
    outcome: OperationOutcome,
) {
    use novarocks_execution::task_execution::domain::DomainProgression;
    use novarocks_execution::task_execution::operation::{
        QueryContextDomainReceipt, QueryContextDomainUpdate, UpdateQueryContext,
    };

    let OperationIntent::UpdateQueryContext(request) = intent else {
        unreachable!("a credential advance is a context update");
    };
    let UpdateQueryContext::AdvanceDomain(advance) = request.as_ref() else {
        unreachable!("a rotation is an advance");
    };
    let QueryContextDomainUpdate::Credential(update) = advance.domain() else {
        unreachable!("a rotation carries the credential domain");
    };
    let payload = if matches!(outcome, OperationOutcome::Accepted) {
        AckPayload::Context(
            QueryContextReceipt::new(advance.context(), QueryContextState::Active).with_domains(
                vec![QueryContextDomainReceipt::Credential {
                    lease_id: update.lease_id(),
                    accepted_epoch: update.epoch(),
                    progression: DomainProgression::Apply,
                }],
            ),
        )
    } else {
        AckPayload::None
    };
    let ack = OperationAcknowledgement::new(
        intent.operation_id(),
        OperationKind::UpdateQueryContext,
        outcome,
        payload,
    );
    observer
        .observe_acknowledgement(&ack)
        .expect("the rotation owner settles its own advance");
    execution
        .acknowledge(&ack)
        .expect("the state machine releases the permit");
}

/// Reports that transport lost the outcome of one credential advance.
fn credential_unknown_ack(
    execution: &mut QueryTaskExecution,
    observer: &dyn crate::task_execution::round::AcknowledgementObserver,
    intent: &OperationIntent,
) {
    let ack = OperationAcknowledgement::transport_unknown(
        intent.operation_id(),
        OperationKind::UpdateQueryContext,
    );
    observer
        .observe_acknowledgement(&ack)
        .expect("the rotation owner retains an unknown advance");
    execution
        .acknowledge(&ack)
        .expect("the state machine releases the unknown transport slot");
}

fn credential_material(intent: &OperationIntent) -> &Arc<dyn ConfidentialContent> {
    use novarocks_execution::task_execution::operation::{
        QueryContextDomainUpdate, UpdateQueryContext,
    };

    let OperationIntent::UpdateQueryContext(request) = intent else {
        panic!("a credential advance is a context update")
    };
    let UpdateQueryContext::AdvanceDomain(advance) = request.as_ref() else {
        panic!("a rotation is an advance")
    };
    let QueryContextDomainUpdate::Credential(update) = advance.domain() else {
        panic!("a rotation carries the credential domain")
    };
    update.material()
}

fn is_credential_advance(intent: &OperationIntent) -> bool {
    use novarocks_execution::task_execution::operation::{
        QueryContextDomainUpdate, UpdateQueryContext,
    };

    matches!(
        intent,
        OperationIntent::UpdateQueryContext(request)
            if matches!(
                request.as_ref(),
                UpdateQueryContext::AdvanceDomain(advance)
                    if matches!(advance.domain(), QueryContextDomainUpdate::Credential(_))
            )
    )
}

/// Turns until the rotation the provider produced reaches the wire.
///
/// The provider call runs off the turn on purpose -- the same thread owns the
/// result loop -- so a test has to turn until it lands rather than assume one
/// turn is enough.
fn drive_until_rotation_released(
    pump: &mut Arc<crate::task_execution::credential_pump::CredentialRotationPump>,
    harness: &mut Harness,
) -> Vec<OperationIntent> {
    use crate::task_execution::round::TurnPump;

    for _ in 0..600 {
        pump.drive(&mut harness.execution)
            .expect("a clean rotation turn");
        let advances = harness
            .released()
            .into_iter()
            .filter(is_credential_advance)
            .collect::<Vec<_>>();
        if !advances.is_empty() {
            return advances;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("the rotation never reached the wire");
}

#[test]
fn a_credential_rotation_reaches_every_context_and_is_reported_once() {
    use crate::task_execution::credential::CredentialRefreshOwner;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::round::AcknowledgementObserver;

    // The defect this catches: `CredentialRefreshOwner` was complete and had
    // nine tests, and nothing drove it. A long query on a vended-credential
    // deployment therefore kept reading with material the provider had stopped
    // honouring, and failed somewhere inside a connector instead. This asserts
    // the loop runs end to end: the provider is called, the attempt's own
    // table takes the new epoch, and every context is sent the rotation.
    let mut harness = Harness::new(&[0, 1], &[0], 64);
    let contexts = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(contexts.len(), 2, "two backends, two query contexts");

    let (storage, credential, refresher) = refreshable_credential_storage_with_refresher(60_000);
    let clock = Arc::clone(&harness.clock);
    let pump = CredentialRotationPump::new(
        execution_id(),
        CredentialRefreshOwner::from_establish(&credential, contexts.iter().copied()),
        Arc::clone(&storage),
        clock.clone() as Arc<dyn TaskProtocolClock>,
        test_connector_blocking_io(),
    )
    .expect("a refreshable lease means there is something to rotate");
    let mut driver = Arc::clone(&pump);

    // Before the soft delay nothing is asked of the provider: rotating on the
    // first turn would burn a provider call on every query.
    use crate::task_execution::round::TurnPump;
    assert_eq!(
        driver.drive(&mut harness.execution).expect("a clean turn"),
        0
    );
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    // Past the soft delay the provider is called exactly once, off the turn.
    clock.advance(Duration::from_secs(120));
    let advances = drive_until_rotation_released(&mut driver, &mut harness);
    assert_eq!(
        refresher.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the provider is called once, not once per turn"
    );
    assert_eq!(
        advances.len(),
        contexts.len(),
        "every participating context is sent the rotation"
    );
    assert_eq!(pump.rotations_applied(), 0, "nothing has accepted it yet");

    // Lose one acknowledgement and prove the complete immutable request is
    // replayed. A fresh operation id at the same epoch is not a retry: it is a
    // second claim on one domain transition.
    credential_unknown_ack(
        &mut harness.execution,
        pump.as_ref() as &dyn AcknowledgementObserver,
        &advances[0],
    );
    driver
        .drive(&mut harness.execution)
        .expect("an unknown advance is retried");
    let retry = harness
        .released()
        .into_iter()
        .filter(is_credential_advance)
        .collect::<Vec<_>>();
    assert_eq!(retry.len(), 1, "only the unknown advance is replayed");
    assert_eq!(
        retry[0].operation_id(),
        advances[0].operation_id(),
        "the retry keeps the original operation identity"
    );
    assert!(
        Arc::ptr_eq(
            credential_material(&retry[0]),
            credential_material(&advances[0])
        ),
        "the retry keeps the exact credential material"
    );

    credential_ack(
        &mut harness.execution,
        pump.as_ref() as &dyn AcknowledgementObserver,
        &retry[0],
        OperationOutcome::Accepted,
    );
    for intent in &advances[1..] {
        credential_ack(
            &mut harness.execution,
            pump.as_ref() as &dyn AcknowledgementObserver,
            intent,
            OperationOutcome::Accepted,
        );
    }
    assert_eq!(
        pump.rotations_applied(),
        1,
        "one rotation, reported once every context has accepted it"
    );

    // And the attempt's own table advanced with them: the frontend reads
    // object storage through this table, so a table that lagged the backends
    // would leave this process using the epoch it just replaced.
    assert_eq!(
        storage
            .refreshable()
            .first()
            .map(|(_, not_after)| *not_after > now_unix_ms() + 60_000),
        Some(true)
    );
}

#[test]
fn a_rotation_that_cannot_be_accepted_before_its_hard_deadline_fails_the_attempt() {
    use crate::task_execution::credential::CredentialRefreshOwner;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::round::TurnPump;

    // The old supervisor aborted the query when a rotation failed, and so does
    // this. Degrading instead would leave every backend on material the
    // provider has stopped honouring, and the query would fail later, deeper,
    // and with an error about access rather than about a lease.
    let mut harness = Harness::new(&[0], &[0], 64);
    let contexts = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let (storage, credential, refresher) = refreshable_credential_storage_with_refresher(60_000);
    let clock = Arc::clone(&harness.clock);
    let pump = CredentialRotationPump::new(
        execution_id(),
        CredentialRefreshOwner::from_establish(&credential, contexts.iter().copied()),
        storage,
        clock.clone() as Arc<dyn TaskProtocolClock>,
        test_connector_blocking_io(),
    )
    .expect("a refreshable lease");
    let mut driver = Arc::clone(&pump);

    // One turn before the soft delay, so the schedule is taken from the
    // lifetime the lease had when this attempt first saw it.
    driver
        .drive(&mut harness.execution)
        .expect("a clean turn before the soft delay");
    clock.advance(Duration::from_secs(120));
    let advances = drive_until_rotation_released(&mut driver, &mut harness);
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!advances.is_empty());
    // The advance was released and nobody answered. Past the point the minted
    // epoch stops being usable, this is a query failure.
    clock.advance(Duration::from_secs(600));
    let error = driver
        .drive(&mut harness.execution)
        .expect_err("an unaccepted rotation past its hard deadline fails the attempt");
    assert!(
        error.to_string().contains("stopped being usable"),
        "{error}"
    );
}

#[test]
fn a_provider_success_settled_at_the_hard_deadline_is_not_installed() {
    use crate::task_execution::credential::CredentialRefreshOwner;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::round::TurnPump;

    let mut harness = Harness::new(&[0], &[0], 64);
    let contexts = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let (storage, credential, refresher) = refreshable_credential_storage_with_refresher(60_000);
    let storage_lease_id = storage.refreshable()[0].0;
    let before = storage
        .refresh_source(storage_lease_id)
        .expect("the lease is refreshable")
        .0;
    let clock = Arc::clone(&harness.clock);
    let pump = CredentialRotationPump::new(
        execution_id(),
        CredentialRefreshOwner::from_establish(&credential, contexts),
        Arc::clone(&storage),
        clock.clone() as Arc<dyn TaskProtocolClock>,
        test_connector_blocking_io(),
    )
    .expect("a refreshable lease");
    let mut driver = Arc::clone(&pump);

    driver
        .drive(&mut harness.execution)
        .expect("the first turn schedules the soft deadline");
    clock.advance(Duration::from_secs(120));
    driver
        .drive(&mut harness.execution)
        .expect("the due turn starts the provider call");
    for _ in 0..600 {
        if pump.stage_provider_outcome_for_test() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        pump.stage_provider_outcome_for_test(),
        "the successful provider outcome must be present before time advances"
    );
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    // Settlement, not the instant the blocking provider returned, owns the
    // mutation. Once that turn reaches the hard deadline the successful value
    // is stale and must be discarded before it can alter either authority.
    let hard_deadline = pump
        .provider_hard_deadline_for_test()
        .expect("the finished provider call retains its hard deadline");
    clock.set(hard_deadline.since_origin());
    let error = driver
        .drive(&mut harness.execution)
        .expect_err("a late provider success fails closed");
    assert!(error.to_string().contains("answered after"), "{error}");

    let after = storage
        .refresh_source(storage_lease_id)
        .expect("the original lease remains installed")
        .0;
    assert_eq!(after.epoch(), before.epoch(), "storage epoch must not move");
    assert_eq!(
        after.not_after_unix_ms(),
        before.not_after_unix_ms(),
        "storage material must not be replaced"
    );
    assert_eq!(
        pump.minted_epoch(),
        CredentialEpoch::FIRST,
        "the credential domain epoch must not be minted"
    );
}

#[test]
fn a_wiped_rotation_owner_stops_driving_the_credential_domain() {
    use crate::task_execution::credential::CredentialRefreshOwner;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::round::TurnPump;

    // The runner keeps turning through the drain, after the client's answer is
    // already linearized. A rotation started there would be judged against a
    // hard deadline for material no task still reads, which would turn a
    // completed query into a failure.
    let mut harness = Harness::new(&[0], &[0], 64);
    let contexts = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let (storage, credential, refresher) = refreshable_credential_storage_with_refresher(60_000);
    let clock = Arc::clone(&harness.clock);
    let pump = CredentialRotationPump::new(
        execution_id(),
        CredentialRefreshOwner::from_establish(&credential, contexts.iter().copied()),
        storage,
        clock.clone() as Arc<dyn TaskProtocolClock>,
        test_connector_blocking_io(),
    )
    .expect("a refreshable lease");
    let mut driver = Arc::clone(&pump);

    pump.wipe();
    clock.advance(Duration::from_secs(3600));
    assert_eq!(
        driver.drive(&mut harness.execution).expect("a clean turn"),
        0
    );
    assert_eq!(
        refresher.calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a wiped owner asks the provider for nothing"
    );
}

#[test]
fn a_context_that_is_already_over_is_retired_rather_than_failing_the_rotation() {
    use crate::task_execution::credential::CredentialRefreshOwner;
    use crate::task_execution::credential_pump::CredentialRotationPump;
    use crate::task_execution::round::{AcknowledgementObserver, TurnPump};

    // A released context has no task left that could read through the
    // credential and can never acknowledge anything again. Treating its
    // refusal as a rotation failure would fail a healthy query; leaving it a
    // participant would stall every later rotation behind it and then fail the
    // attempt on that rotation's hard deadline.
    let mut harness = Harness::new(&[0, 1], &[0], 64);
    let contexts = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let (storage, credential, _) = refreshable_credential_storage_with_refresher(60_000);
    let clock = Arc::clone(&harness.clock);
    let pump = CredentialRotationPump::new(
        execution_id(),
        CredentialRefreshOwner::from_establish(&credential, contexts.iter().copied()),
        storage,
        clock.clone() as Arc<dyn TaskProtocolClock>,
        test_connector_blocking_io(),
    )
    .expect("a refreshable lease");
    let mut driver = Arc::clone(&pump);

    driver.drive(&mut harness.execution).expect("a clean turn");
    clock.advance(Duration::from_secs(120));
    let advances = drive_until_rotation_released(&mut driver, &mut harness);
    assert_eq!(advances.len(), 2);

    // One context accepts; the other is already gone.
    credential_ack(
        &mut harness.execution,
        pump.as_ref() as &dyn AcknowledgementObserver,
        &advances[0],
        OperationOutcome::Accepted,
    );
    credential_ack(
        &mut harness.execution,
        pump.as_ref() as &dyn AcknowledgementObserver,
        &advances[1],
        OperationOutcome::Gone,
    );
    assert_eq!(
        pump.rotations_applied(),
        1,
        "a retired context does not hold the rotation open"
    );

    // And the next hard deadline cannot fail the attempt on its behalf.
    clock.advance(Duration::from_secs(3600));
    driver
        .drive(&mut harness.execution)
        .expect("a retired context leaves nothing outstanding");
}
