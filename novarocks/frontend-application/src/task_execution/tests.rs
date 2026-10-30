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
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::native::fragment_encoder::frozen::tests as frozen_tests;
use crate::native::fragment_encoder::frozen::{FragmentArtifact, StaticSinkTarget};
use crate::query_execution::artifact::{
    FragmentId, NativePlacementAssignment, TaskManifestBinding,
};
use crate::query_execution::attempt_plan_facts::AttemptPartitionKind;
use crate::task_execution::completion_facts::StageState;
use crate::task_execution::lease::RenewSchedule;
use crate::task_protocol_config::{DispatchBudget, DispatchLane};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AbortCause, AdmissionEpochCapability, AdmissionTicketId, CancelReason, CodecOwnedContent,
    ConfidentialContent, ContentFingerprint, CreateTaskReceipt, CredentialEpoch, CredentialLeaseId,
    CredentialUpdate, DomainVersion, DynamicFilterAdvertisement, EdgeOpenVersion, ExchangeEdgeId,
    LeaseReceipt, LeaseSequence, LeaseValidFor, OperationKind, OperationOutcome, PlanNodeId,
    PlanNodeSplitReceipt, QueryContextAdmissionTicketReceipt, QueryContextReceipt, QueryContextRef,
    QueryContextState, QuiesceQueryContextReceipt, ReleaseOutcome, SplitAssignmentIntent,
    SplitOffer, SplitSequence, SplitWatermark, TaskConvergenceReceipt, TaskConvergenceVersion,
    TaskDomainReceipt, TaskDomainUpdate, TaskIdentity, TaskOperationId, TaskOutputFacts, TaskState,
    TaskStatus, TaskStatusVersion, TerminationDetail, UpdateTaskReceipt,
};
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_proto_models::novarocks as wire;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::descriptor::encode_task_descriptor;
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredObservationBookmark, CoveredStatusStreamEvent,
    CoveredStatusStreamFact,
};
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};
use novarocks_types::{AttemptId, NativeCompatibilityId, QueryId};
use prost::Message;

use super::blocking_io::ConnectorBlockingIoSupervisor;
use super::clock::ManualClock;
use super::context_owner::{
    ContextEstablishFacts, ContextEstablishSource, QueryContextOwner, ReleaseSettlement,
};
use super::creation::tests as creation_tests;
use super::dispatch::OperationDispatcher;
use super::error::{CapacityBound, TaskExecutionError};
use super::execution::QueryTaskExecution;
use super::graph::{
    FragmentPlanFacts, FragmentPlanSource, TaskGraph, TaskNode, build_task_graph_from_manifest,
};
use super::intent::{
    AckPayload, DispatchBatch, OPERATION_FIXED_BYTES, OperationAcknowledgement, OperationIntent,
    TaskOperationQueueAdmission, TaskOperationQueuePermit, TaskOperationQueueRequest,
    TaskOperationSink, TaskOperationSubmit, test_queue_permit,
};
use super::remote_task::{RemoteTaskState, UpdateAdmission};
use super::split_domain::{assignment_targets, delivery_action};
use super::stage::EdgeOpenTracker;
use super::status_intake::{
    CountingWake, ObservationFrame, ObservationIntakeEntry, StatusEvent, StatusIntake,
    StatusIntakeAdmission, StatusIntakeWake,
};
use crate::query_execution::artifact::fragment_instance_id_for_contract_test;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use crate::query_execution::split_assignment::SplitAssignmentDriverError;
use novarocks_coordination_contract::MonotonicClock;

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
    ConnectorBlockingIoSupervisor::new(runtime.handle().clone())
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

struct FakeSecret;

impl ConfidentialContent for FakeSecret {
    fn encoded_len(&self) -> usize {
        32
    }

    fn matches(&self, _other: &dyn ConfidentialContent) -> bool {
        true
    }
}

/// A checked Result or single-Stream package. Multi-edge control tests keep
/// their neutral topology separate and use a real Result package here; they
/// do not claim admission of a multicast or router executable package.
fn frozen_fragment(
    fragment_id: FragmentId,
    targets: &[StaticSinkTarget],
    padding: usize,
) -> Arc<FragmentArtifact> {
    let target = match targets {
        [target] => Some((
            target.target_fragment_id(),
            target.target_exchange_node_id(),
            target.delivery(),
        )),
        _ => None,
    };
    frozen_tests::frozen_package(
        fragment_id,
        i32::try_from(fragment_id).expect("small fragment") * 100,
        [9; 16],
        PipelineDopDomain {
            min: 1,
            max: 4,
            requires_power_of_two: false,
        },
        target,
        padding,
    )
}

/// The task graph's own neutral targets, in its declared branch order.
fn edge_sink(
    fragment_id: FragmentId,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
) -> Vec<StaticSinkTarget> {
    edges
        .iter()
        .filter(|edge| edge.source_fragment_id == fragment_id)
        .map(|edge| {
            StaticSinkTarget::for_test(
                edge.target_fragment_id,
                edge.target_exchange_node_id,
                edge.delivery,
            )
        })
        .collect()
}

fn multicast_sink(targets: [(FragmentId, i32); 2]) -> Vec<StaticSinkTarget> {
    targets
        .into_iter()
        .map(|(fragment, node)| {
            StaticSinkTarget::for_test(
                fragment,
                node,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            )
        })
        .collect()
}

fn router_sink(targets: [(FragmentId, i32); 2]) -> Vec<StaticSinkTarget> {
    multicast_sink(targets)
}

/// Creation facts for every placement of one schedule, handed out the way
/// the production submission index hands them out: one frozen fragment per
/// fragment, shared by all of its instances, and each instance's own
/// assignment.
struct FakePlans<'a> {
    schedule: &'a SchedulingPlan,
    edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    padding: usize,
    leaf_sink: Option<Vec<StaticSinkTarget>>,
    frozen: BTreeMap<FragmentId, Arc<FragmentArtifact>>,
}

impl<'a> FakePlans<'a> {
    fn new(
        schedule: &'a SchedulingPlan,
        edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        padding: usize,
    ) -> Self {
        Self {
            schedule,
            edges,
            padding,
            leaf_sink: None,
            frozen: BTreeMap::new(),
        }
    }

    /// The same source, with the leaf fragment declaring `sink` instead of
    /// the one its edges imply.
    fn with_leaf_sink(mut self, sink: Vec<StaticSinkTarget>) -> Self {
        self.leaf_sink = Some(sink);
        self
    }
}

impl FragmentPlanSource for FakePlans<'_> {
    fn control_topology_targets(&self, fragment_id: FragmentId) -> Option<Vec<StaticSinkTarget>> {
        Some(match (&self.leaf_sink, fragment_id) {
            (Some(targets), LEAF_FRAGMENT) => targets.clone(),
            _ => edge_sink(fragment_id, self.edges),
        })
    }

    fn take_plan(
        &mut self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError> {
        let placement = self
            .schedule
            .by_fragment
            .get(&fragment_id)
            .and_then(|placements| placements.get(instance_index))
            .ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} has no instance {instance_index}"
                ))
            })?;
        let sink = match (&self.leaf_sink, fragment_id) {
            (Some(sink), LEAF_FRAGMENT) => sink.clone(),
            _ => edge_sink(fragment_id, self.edges),
        };
        let padding = self.padding;
        let fragment = Arc::clone(
            self.frozen
                .entry(fragment_id)
                .or_insert_with(|| frozen_fragment(fragment_id, &sink, padding)),
        );
        Ok(FragmentPlanFacts {
            fragment,
            assignment: NativePlacementAssignment::new(
                u32::try_from(instance_index).expect("a test instance ordinal fits"),
                NonZeroUsize::new(2).expect("two is nonzero"),
                placement.scan_ranges.clone(),
            ),
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

/// How a gated reservation is refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueRefusal {
    /// That target's window is full.
    Target,
    /// The process-wide window is full.
    Process,
}

/// Records what the dispatcher released, and nothing else.
///
/// Every reservation is admitted unless a test gated its lane and target, and
/// every reservation asked for is counted, so a test can see which requests
/// an admission pass reached.
#[derive(Debug, Default)]
pub(crate) struct RecordingSink {
    batches: Mutex<Vec<(DispatchLane, Vec<OperationIntent>)>>,
    refusals: Mutex<BTreeMap<(DispatchLane, BackendProcessId), QueueRefusal>>,
    reservations: Mutex<Vec<(DispatchLane, BackendProcessId)>>,
    backpressured_lanes: Mutex<BTreeSet<DispatchLane>>,
}

impl RecordingSink {
    pub(crate) fn take(&self) -> Vec<(DispatchLane, Vec<OperationIntent>)> {
        std::mem::take(&mut *self.batches.lock().expect("recording sink"))
    }

    fn refuse(&self, lane: DispatchLane, backend: BackendProcessId, refusal: QueueRefusal) {
        self.refusals
            .lock()
            .expect("recording sink refusals")
            .insert((lane, backend), refusal);
    }

    fn admit(&self, lane: DispatchLane, backend: BackendProcessId) {
        self.refusals
            .lock()
            .expect("recording sink refusals")
            .remove(&(lane, backend));
    }

    /// How many reservations this sink was asked for on `lane`.
    fn reservations(&self, lane: DispatchLane) -> usize {
        self.reservations
            .lock()
            .expect("recording sink reservations")
            .iter()
            .filter(|(asked, _)| *asked == lane)
            .count()
    }
}

impl TaskOperationSink for RecordingSink {
    fn try_reserve_queue(
        &self,
        request: TaskOperationQueueRequest,
        _preparation: &mut super::intent::TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        let key = (request.lane(), request.backend_process_id());
        self.reservations
            .lock()
            .expect("recording sink reservations")
            .push(key);
        match self
            .refusals
            .lock()
            .expect("recording sink refusals")
            .get(&key)
        {
            Some(QueueRefusal::Target) => TaskOperationQueueAdmission::TargetFull,
            Some(QueueRefusal::Process) => TaskOperationQueueAdmission::ProcessFull,
            None => TaskOperationQueueAdmission::Admitted(test_queue_permit()),
        }
    }

    fn try_submit(&self, batch: DispatchBatch) -> TaskOperationSubmit {
        let lane = batch.lane();
        if self.backpressured_lanes.lock().unwrap().contains(&lane) {
            return TaskOperationSubmit::Backpressured(batch);
        }
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
    reserve_attempts: AtomicUsize,
    ordinary_update_reserve_attempts: AtomicUsize,
}

impl Default for SwitchableQueueSink {
    fn default() -> Self {
        Self {
            queue_open: std::sync::atomic::AtomicBool::new(true),
            submit_open: std::sync::atomic::AtomicBool::new(true),
            batches: Mutex::new(Vec::new()),
            live_queue_permits: Arc::new(AtomicUsize::new(0)),
            reserve_attempts: AtomicUsize::new(0),
            ordinary_update_reserve_attempts: AtomicUsize::new(0),
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

    fn reserve_attempts(&self) -> usize {
        self.reserve_attempts.load(Ordering::SeqCst)
    }

    fn ordinary_update_reserve_attempts(&self) -> usize {
        self.ordinary_update_reserve_attempts.load(Ordering::SeqCst)
    }
}

impl TaskOperationSink for SwitchableQueueSink {
    fn try_reserve_queue(
        &self,
        request: TaskOperationQueueRequest,
        _preparation: &mut crate::task_execution::intent::TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        self.reserve_attempts.fetch_add(1, Ordering::SeqCst);
        if request.lane() == DispatchLane::Update && !request.requires_control_progress() {
            self.ordinary_update_reserve_attempts
                .fetch_add(1, Ordering::SeqCst);
        }
        if self.queue_open.load(Ordering::SeqCst) {
            self.live_queue_permits.fetch_add(1, Ordering::SeqCst);
            TaskOperationQueueAdmission::Admitted(Box::new(SwitchableQueuePermit {
                live: Arc::clone(&self.live_queue_permits),
            }))
        } else {
            TaskOperationQueueAdmission::ProcessFull
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
        _preparation: &mut crate::task_execution::intent::TaskOperationQueuePreparation,
    ) -> TaskOperationQueueAdmission {
        if self.reserve_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            TaskOperationQueueAdmission::ProcessFull
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
        _preparation: &mut crate::task_execution::intent::TaskOperationQueuePreparation,
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
pub(crate) fn fixture_runtime_filter_contribution()
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
        })
        .collect()
}

fn stream_edge(
    source: FragmentId,
    target: FragmentId,
    node_id: i32,
) -> crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
    crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
        source_fragment_id: source,
        target_fragment_id: target,
        target_exchange_node_id: node_id,
        partition_kind: AttemptPartitionKind::Hash,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
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

fn chain_edges() -> Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts> {
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
fn multicast_edges() -> Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts> {
    vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
    ]
}

/// One task's protocol descriptor, as its creation seed derives it.
fn descriptor_of(
    graph: &TaskGraph,
    task_id: TaskId,
) -> novarocks_execution::task_execution::TaskDescriptor {
    graph
        .seed(task_id)
        .expect("a built graph owns every creation seed")
        .descriptor()
        .expect("a built graph derives every descriptor")
}

/// Freezes every task's creation metadata exactly as a first send would, and
/// decodes it back: what the assertions read is what goes on the wire.
fn frozen_metadata(graph: TaskGraph) -> BTreeMap<TaskId, wire::CreationMetadata> {
    let (_graph, seeds) = graph.into_seeds();
    seeds
        .into_iter()
        .map(|(task_id, seed)| {
            let priced = seed.lengths().expect("a seed prices its creation");
            let parts = seed.freeze(priced).expect("a seed freezes at its price");
            let metadata = wire::CreationMetadata::decode(parts.metadata().bytes().clone())
                .expect("frozen creation metadata decodes");
            (task_id, metadata)
        })
        .collect()
}

fn sink_edge_ids(metadata: &wire::CreationMetadata) -> Vec<u32> {
    metadata
        .assignment
        .as_ref()
        .expect("every creation carries its assignment")
        .sink_edge_ids
        .clone()
}

#[test]
fn every_instance_of_one_fragment_shares_its_frozen_plan_and_keeps_its_own_assignment() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[0, 1]);
    let freezes = frozen_tests::freezes_on_this_thread();
    let metadata_freezes = creation_tests::metadata_freezes_on_this_thread();
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    assert_eq!(
        frozen_tests::freezes_on_this_thread() - freezes,
        3,
        "each of the three fragments is frozen once, however many instances it has"
    );
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread(),
        metadata_freezes,
        "building the graph freezes no creation metadata"
    );

    let tasks = graph
        .tasks()
        .map(|task| (task.task_id(), task.fragment_id(), task.instance_index()))
        .collect::<Vec<_>>();
    assert_eq!(tasks.len(), 6, "three leaves, two middles, one root");
    for &(left, left_fragment, _) in &tasks {
        for &(right, right_fragment, _) in &tasks {
            let shared = Arc::ptr_eq(
                graph.seed(left).expect("a seed").fragment(),
                graph.seed(right).expect("a seed").fragment(),
            );
            assert_eq!(
                shared,
                left_fragment == right_fragment,
                "instances share exactly their own fragment's frozen plan"
            );
        }
    }

    let metadata = frozen_metadata(graph);
    for (task_id, _, instance_index) in tasks {
        let assignment = metadata[&task_id]
            .assignment
            .as_ref()
            .expect("every creation carries its assignment");
        assert_eq!(
            assignment.instance_ordinal,
            u32::try_from(instance_index).expect("a small ordinal"),
            "each instance keeps its own ordinal"
        );
    }
}

/// However many instances a fragment has, its static plan is frozen once and
/// held by reference: one backing serves every task, and it is never copied.
#[test]
fn a_fragment_is_frozen_once_however_many_instances_it_has() {
    for leaves in [1_usize, 3, 16, 64] {
        let processes = backends(3);
        let leaf_backends = (0..leaves).map(|index| index % 3).collect::<Vec<_>>();
        let schedule = chain_schedule(&leaf_backends, &[0, 1]);
        let freezes = frozen_tests::freezes_on_this_thread();
        let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
        assert_eq!(
            frozen_tests::freezes_on_this_thread() - freezes,
            3,
            "{leaves} leaf instances still freeze exactly the three fragments"
        );
        let leaf_tasks = graph
            .tasks()
            .filter(|task| task.fragment_id() == LEAF_FRAGMENT)
            .map(TaskNode::task_id)
            .collect::<Vec<_>>();
        assert_eq!(leaf_tasks.len(), leaves);
        let shared = graph.seed(leaf_tasks[0]).expect("a leaf seed").fragment();
        for task in &leaf_tasks {
            let fragment = graph.seed(*task).expect("a leaf seed").fragment();
            assert!(Arc::ptr_eq(fragment, shared));
            assert!(fragment.content().shares_backing_with(shared.content()));
        }
        assert_eq!(
            Arc::strong_count(shared),
            leaves,
            "the static plan is held once per leaf task and by nothing else"
        );
    }
}

#[test]
fn multicast_sink_edges_bind_actual_graph_ids_and_endpoint_changes_only_metadata() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1]);
    let edges = multicast_edges();
    // Intentionally reverse the graph edge order to prove the binding follows
    // static sink positions, not edge order.
    let leaf_targets = [
        (ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
        (MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
    ];
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(multicast_sink(leaf_targets)),
    )
    .expect("multicast graph");
    let leaves = graph
        .tasks()
        .filter(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .collect::<Vec<_>>();
    assert_eq!(leaves.len(), 2);
    let leaf_fragment = Arc::clone(graph.seed(leaves[0]).expect("leaf seed").fragment());
    assert!(
        Arc::ptr_eq(
            &leaf_fragment,
            graph.seed(leaves[1]).expect("leaf seed").fragment()
        ),
        "placement cannot alter the frozen fragment"
    );
    let first_descriptor = descriptor_of(&graph, leaves[0]);
    let expected = [LEAF_TO_ROOT_NODE, LEAF_TO_MIDDLE_NODE]
        .into_iter()
        .map(|node_id| {
            first_descriptor
                .topology()
                .outbound()
                .iter()
                .find(|edge| edge.destination_node_id().get() == node_id)
                .expect("scheduled edge")
                .edge_id()
                .get()
        })
        .collect::<Vec<_>>();
    let metadata = frozen_metadata(graph);
    assert_eq!(sink_edge_ids(&metadata[&leaves[0]]), expected);
    assert_eq!(sink_edge_ids(&metadata[&leaves[1]]), expected);

    let mut moved = schedule.clone();
    for placement in moved.by_fragment.values_mut().flatten() {
        if placement.backend_idx == 1 {
            placement.endpoint = RuntimeEndpoint::new("127.0.0.9", 19099).expect("moved endpoint");
        }
    }
    let moved_graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &moved,
        &edges,
        &processes,
        &mut FakePlans::new(&moved, &edges, 64).with_leaf_sink(multicast_sink(leaf_targets)),
    )
    .expect("moved graph");
    assert_eq!(
        leaf_fragment.content().bytes(),
        moved_graph
            .seed(leaves[0])
            .expect("moved leaf seed")
            .fragment()
            .content()
            .bytes(),
        "endpoint is absent from the frozen fragment"
    );
    assert_ne!(
        encode_task_descriptor(&first_descriptor).encode_to_vec(),
        encode_task_descriptor(&descriptor_of(&moved_graph, leaves[0])).encode_to_vec(),
        "the task-local topology carries the changed endpoint"
    );
}

#[test]
fn router_route_positions_bind_their_actual_exchange_edge_ids() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    let edges = multicast_edges();
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(router_sink([
            (ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
            (MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        ])),
    )
    .expect("router graph");
    let leaf = graph
        .tasks()
        .find(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .expect("leaf task");
    let descriptor = descriptor_of(&graph, leaf);
    let expected = [LEAF_TO_ROOT_NODE, LEAF_TO_MIDDLE_NODE]
        .into_iter()
        .map(|node_id| {
            descriptor
                .topology()
                .outbound()
                .iter()
                .find(|edge| edge.destination_node_id().get() == node_id)
                .expect("router edge")
                .edge_id()
                .get()
        })
        .collect::<Vec<_>>();
    assert_eq!(sink_edge_ids(&frozen_metadata(graph)[&leaf]), expected);
}

fn same_node_targets_bind_distinct_edges(router: bool) {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    // Exchange node IDs are local to their target fragment. Both consumers
    // can legally name node zero, including a CTE multicast or MERGE router.
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, 0),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, 0),
    ];
    let targets = [(ROOT_FRAGMENT, 0), (MIDDLE_FRAGMENT, 0)];
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(if router {
            router_sink(targets)
        } else {
            multicast_sink(targets)
        }),
    )
    .expect("same node IDs in different fragments have distinct edges");
    let leaf = graph
        .tasks()
        .find(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .expect("leaf task");
    let descriptor = descriptor_of(&graph, leaf);
    let target_stages = [ROOT_FRAGMENT, MIDDLE_FRAGMENT].map(|fragment| {
        graph
            .tasks()
            .find(|task| task.fragment_id() == fragment)
            .expect("target stage")
            .stage_id()
    });
    let expected = target_stages.map(|stage| {
        descriptor
            .topology()
            .outbound()
            .iter()
            .find(|edge| {
                edge.destination_node_id().get() == 0
                    && edge
                        .destinations()
                        .iter()
                        .all(|destination| destination.task().stage_id() == stage)
            })
            .expect("exact target fragment and node")
            .edge_id()
            .get()
    });
    assert_ne!(expected[0], expected[1]);
    assert_eq!(sink_edge_ids(&frozen_metadata(graph)[&leaf]), expected);
}

#[test]
fn cte_multicast_same_node_ids_bind_by_target_fragment() {
    same_node_targets_bind_distinct_edges(false);
}

#[test]
fn merge_router_same_node_ids_bind_by_target_fragment() {
    same_node_targets_bind_distinct_edges(true);
}

#[test]
fn static_sink_unknown_target_fragment_fails_closed() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, 0),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, 0),
    ];
    let error = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64)
            .with_leaf_sink(multicast_sink([(ROOT_FRAGMENT, 0), (99, 0)])),
    )
    .expect_err("a static sink cannot guess the target from a reused node ID");
    assert!(
        error
            .to_string()
            .contains("static sink targets absent fragment 99")
    );
}

#[test]
fn a_static_plan_over_the_descriptor_bound_is_refused_before_any_task_exists() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let error = build_graph(
        &schedule,
        &chain_edges(),
        &processes,
        TransportBudget::DEFAULT.max_descriptor_encoded_bytes(),
    )
    .expect_err("a static plan larger than one descriptor can never be created");
    assert!(
        matches!(
            error,
            TaskExecutionError::Capacity(CapacityBound::DescriptorBytes { limit, .. })
                if limit == TransportBudget::DEFAULT.max_descriptor_encoded_bytes()
        ),
        "an inherent size limit is a deterministic error, not backpressure: {error:?}"
    );
}

fn protocol_frozen_fragments(
    schedule: &SchedulingPlan,
    plans: &mut FakePlans<'_>,
) -> Result<BTreeMap<FragmentId, Arc<FragmentArtifact>>, TaskExecutionError> {
    let mut fragments = BTreeMap::new();
    for &fragment in schedule.by_fragment.keys() {
        fragments.insert(fragment, plans.take_plan(fragment, 0)?.fragment);
    }
    Ok(fragments)
}

fn build_protocol_graph(
    execution: QueryExecutionId,
    frontend: FrontendProcessId,
    schedule: &SchedulingPlan,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    processes: &BTreeMap<usize, BackendProcessId>,
    plans: &mut FakePlans<'_>,
) -> Result<TaskGraph, TaskExecutionError> {
    let fragments = protocol_frozen_fragments(schedule, plans)?;
    let manifest = TaskManifestBinding::protocol_fixture_for_test(
        execution, frontend, schedule, edges, processes, fragments,
    )
    .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
    let graph = build_task_graph_from_manifest(&manifest, plans, TransportBudget::DEFAULT);
    // The graph now owns only its original per-task fragment aliases. The
    // temporary fixture template must really retire before callers count them.
    drop(manifest);
    graph
}

fn build_graph(
    schedule: &SchedulingPlan,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    processes: &BTreeMap<usize, BackendProcessId>,
    plan_bytes: usize,
) -> Result<TaskGraph, TaskExecutionError> {
    build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        schedule,
        edges,
        processes,
        &mut FakePlans::new(schedule, edges, plan_bytes),
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
        Self::from_graph_with_preparing_positions(graph, 4096)
    }

    fn from_graph_with_preparing_positions(graph: TaskGraph, preparing_positions: usize) -> Self {
        let sink = Arc::new(RecordingSink::default());
        Self::from_graph_with_original_transport(
            graph,
            preparing_positions,
            Arc::clone(&sink),
            Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        )
    }

    fn from_graph_with_original_transport(
        graph: TaskGraph,
        preparing_positions: usize,
        sink: Arc<RecordingSink>,
        transport: Arc<dyn TaskOperationSink>,
    ) -> Self {
        let clock = Arc::new(ManualClock::new());
        let wake = Arc::new(CountingWake::default());
        let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
        let admission_epochs = graph
            .contexts()
            .map(|context| (context.backend_process_id(), admission_epoch()))
            .collect::<BTreeMap<_, _>>();
        let execution = QueryTaskExecution::new(
            graph,
            DispatchBudget::new(
                preparing_positions.min(DispatchBudget::DEFAULT.create_permits()),
                DispatchBudget::DEFAULT.update_permits(),
                DispatchBudget::DEFAULT.lifecycle_permits(),
                DispatchBudget::DEFAULT.control_permits(),
            )
            .unwrap(),
            TransportBudget::DEFAULT,
            NativeCompatibilityId::new([0x41; 32]),
            &admission_epochs,
            &admission_epochs
                .keys()
                .map(|&process| (process, preparing_positions))
                .collect(),
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            transport,
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
            let mut establish_started = false;
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
                        if let OperationIntent::EstablishQueryContext(request) = &intent {
                            // RecordingSink models a transport that began
                            // polling the exact RPC before its ACK arrives.
                            self.execution
                                .establish_send_started(
                                    request.context(),
                                    request.envelope().operation_id(),
                                )
                                .expect("the released Establish started");
                            establish_started = true;
                        }
                        remaining.push(intent);
                    }
                }
                if !remaining.is_empty() {
                    visible.push((lane, remaining));
                }
            }
            if !acquired && !establish_started {
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
        let (context, may_create) = match intent {
            OperationIntent::EstablishQueryContext(request) => (request.context(), true),
            OperationIntent::UpdateQueryContext(request) => {
                (request.context(), request.may_create())
            }
            _ => unreachable!("a context intent carries its request"),
        };
        let sequence = self
            .execution
            .owner(context)
            .expect("the context has an owner")
            .lease_sequence();
        let sequence = if may_create {
            LeaseSequence::INITIAL
        } else {
            sequence.next().expect("the lease sequence advances")
        };
        let receipt = QueryContextReceipt::new(context, QueryContextState::Active).with_lease(
            LeaseReceipt::new(
                sequence,
                LeaseValidFor::new(if may_create {
                    Duration::from_secs(30)
                } else {
                    Duration::from_secs(5)
                })
                .expect("a legal request"),
                effective,
            ),
        );
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
            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                request.identity(),
                Vec::new(),
                TaskStatus::created(request.identity()).with_installed(),
            )))
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

    /// Every backend this attempt holds a context on.
    fn context_backends(&self) -> Vec<BackendProcessId> {
        self.execution
            .graph()
            .contexts()
            .map(|context| context.backend_process_id())
            .collect()
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
                .publish(StatusEvent::Published(Box::new(status))),
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
        TaskDomainUpdate::CloseExchangeDestination { version, .. } => {
            TaskDomainReceipt::CloseExchangeDestination {
                accepted_version: *version,
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
        let descriptor = descriptor_of(&graph, task.task_id());
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
        let descriptor = descriptor_of(&graph, task.task_id());
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
            // derived once per exchange node, so they cannot disagree; each
            // producer counts at its own position in that union.
            let expected = inbound.expected_sender_count();
            let mut ordinals = BTreeSet::new();
            for producer in inbound.sources() {
                let producer_descriptor = descriptor_of(&graph, producer.task().task_id());
                let edge = producer_descriptor
                    .topology()
                    .outbound()
                    .iter()
                    .find(|edge| edge.destination_node_id() == inbound.node_id())
                    .expect("the producer has an edge into this node");
                assert_eq!(edge.sender_count(), expected);
                assert!(
                    ordinals.insert(edge.sender_ordinal()),
                    "two producers of one node may not count at one position"
                );
            }
        }
    }
}

#[test]
fn a_repeated_kernel_key_is_refused() {
    let mut schedule = chain_schedule(&[0, 1], &[1]);
    let leaf = schedule
        .by_fragment
        .get_mut(&LEAF_FRAGMENT)
        .expect("the leaf fragment is placed");
    leaf[1].finst_id = leaf[0].finst_id;

    let error =
        TaskManifestBinding::protocol_collision_for_test(leaf[0].finst_id, leaf[1].finst_id)
            .expect_err("two tasks may not share one kernel key");
    assert_eq!(
        error.kind(),
        crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
    );
    assert!(
        error
            .message()
            .contains("fragment-instance identity collision")
    );
    assert!(error.message().contains(&leaf[0].finst_id.to_string()));
}

#[test]
fn a_scheduled_backend_without_a_frozen_process_identity_is_refused() {
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let edges = chain_edges();
    let fragments =
        protocol_frozen_fragments(&schedule, &mut FakePlans::new(&schedule, &edges, 512))
            .expect("original fragments");
    let error = TaskManifestBinding::protocol_missing_backend_for_test(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        fragments,
        1,
    )
    .expect_err("a placement needs a frozen backend process identity");
    assert_eq!(
        error.kind(),
        crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
    );
    assert!(
        error
            .message()
            .contains("names backend process absent from the frozen endpoint snapshot")
    );
    assert!(error.message().contains(&processes[&1].to_string()));
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
fn an_exact_normal_close_resolves_only_its_frozen_destination() {
    let processes = backends(3);
    let graph = build_graph(
        &chain_schedule(&[0, 1], &[0, 1]),
        &chain_edges(),
        &processes,
        512,
    )
    .expect("the chain is a legal task graph");
    let edge = graph
        .edges()
        .find(|edge| edge.destinations().len() == 2)
        .expect("the middle exchange has two destinations");
    let edge_id = edge.edge_id();
    let [closed, needed] = edge.destinations() else {
        panic!("the middle exchange has exactly two destinations");
    };
    let mut tracker = EdgeOpenTracker::from_graph(&graph);

    // A can remain ClosingUnknown; its exact no-more-input authorization
    // removes only its send need. B still needs its own Create evidence.
    let resolution = tracker.note_normally_closed(*closed);
    assert_eq!(resolution.closed().len(), 1);
    assert_eq!(resolution.closed()[0].edge_id(), edge_id);
    assert_eq!(resolution.closed()[0].destination(), *closed);
    assert!(resolution.opened().is_empty());
    assert!(tracker.note_created(*closed).is_empty());
    let decided = tracker.note_created(*needed);
    assert_eq!(decided.len(), 1);
    assert_eq!(decided[0].edge_id(), edge_id);
    assert_eq!(decided[0].normally_closed(), &[*closed]);
    assert!(tracker.note_created(*closed).is_empty());
    assert_eq!(tracker.note_normally_closed(*closed).closed().len(), 0);
    assert!(tracker.note_created(*needed).is_empty());
}

#[test]
fn normal_close_and_ready_order_do_not_close_a_multicast_sibling() {
    let processes = backends(3);
    let graph = build_graph(
        &chain_schedule(&[0, 1], &[0, 1]),
        &multicast_edges(),
        &processes,
        512,
    )
    .expect("the multicast chain is a legal task graph");
    let mixed = graph
        .edges()
        .find(|edge| edge.destinations().len() == 2)
        .expect("one edge has two middle destinations");
    let [needed, closed] = mixed.destinations() else {
        panic!("the middle exchange has exactly two destinations");
    };
    let sibling = graph
        .edges()
        .find(|edge| edge.edge_id() != mixed.edge_id() && edge.producers() == mixed.producers())
        .expect("the leaf stage also feeds a sibling edge");
    let mut tracker = EdgeOpenTracker::from_graph(&graph);

    assert!(tracker.note_created(*needed).is_empty());
    let resolution = tracker.note_normally_closed(*closed);
    assert_eq!(resolution.closed().len(), 1);
    assert_eq!(resolution.closed()[0].edge_id(), mixed.edge_id());
    assert_eq!(resolution.opened().len(), 1);
    assert_eq!(resolution.opened()[0].edge_id(), mixed.edge_id());
    assert_eq!(resolution.opened()[0].normally_closed(), &[*closed]);

    let sibling_decisions = sibling
        .destinations()
        .iter()
        .flat_map(|destination| tracker.note_created(*destination))
        .collect::<Vec<_>>();
    let sibling_decisions = sibling_decisions
        .iter()
        .filter(|decision| decision.edge_id() == sibling.edge_id())
        .collect::<Vec<_>>();
    assert_eq!(sibling_decisions.len(), 1);
    assert!(sibling_decisions[0].normally_closed().is_empty());
}

#[test]
fn normal_close_after_edge_open_still_emits_an_exact_close_effect() {
    let processes = backends(3);
    let graph = build_graph(&chain_schedule(&[0], &[1]), &chain_edges(), &processes, 512)
        .expect("the chain is a legal task graph");
    let edge = graph
        .edges()
        .find(|edge| edge.destinations().len() == 1)
        .expect("the chain has a one-destination edge");
    let destination = edge.destinations()[0];
    let mut tracker = EdgeOpenTracker::from_graph(&graph);

    let opened = tracker.note_created(destination);
    assert!(
        opened
            .iter()
            .any(|decision| decision.edge_id() == edge.edge_id())
    );
    let closed = tracker.note_normally_closed(destination);
    assert!(closed.opened().is_empty());
    assert!(closed.closed().iter().any(|effect| {
        effect.edge_id() == edge.edge_id() && effect.destination() == destination
    }));
    assert!(
        tracker
            .note_normally_closed(destination)
            .closed()
            .is_empty()
    );
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
        first.same_original(second),
        "a retry must resend the identical immutable request, not a rebuilt one"
    );
    assert!(
        first.same_parts(second),
        "a retry resends the carriers frozen for the first send"
    );
}

#[test]
fn create_not_ready_waits_for_exact_establish_ack_before_replay() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let released = harness.released();
    let establish = released
        .iter()
        .find(|intent| matches!(intent.kind(), OperationKind::UpdateQueryContext))
        .expect("the context establish was released");
    let create = released
        .iter()
        .find(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .expect("a create was pipelined");
    harness
        .create_ack(create, OperationOutcome::NotReady)
        .expect("pre-admission NotReady does not fail the attempt");
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| intent.operation_id() != create.operation_id()),
        "NotReady must not resend the large body before Establish settles"
    );
    harness.context_ack(establish, Duration::from_secs(10));
    let replay = harness
        .released()
        .into_iter()
        .find(|intent| intent.operation_id() == create.operation_id())
        .expect("the exact Establish ACK releases the retained Create");
    let (OperationIntent::CreateTask(first), OperationIntent::CreateTask(replayed)) =
        (create, &replay)
    else {
        unreachable!("both intents are creates");
    };
    assert!(first.same_original(replayed));
}

#[test]
fn first_create_waits_for_the_exact_establish_send_start() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let admission = harness
        .pump_once()
        .into_iter()
        .flat_map(|(_, intents)| intents)
        .find_map(|intent| match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => Some(request),
            _ => None,
        })
        .expect("admission is released first");
    harness
        .execution
        .acknowledge(&OperationAcknowledgement::worker_receipt(
            admission.envelope().operation_id(),
            OperationKind::AcquireQueryContextAdmissionTicket,
            OperationOutcome::Accepted,
            AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([0x54; 16]).expect("nonzero ticket"),
                admission.context(),
                admission.valid_for(),
            )),
        ))
        .expect("admission settles");

    let released = harness
        .pump_once()
        .into_iter()
        .flat_map(|(_, intents)| intents)
        .collect::<Vec<_>>();
    let establish = released
        .iter()
        .find_map(|intent| match intent {
            OperationIntent::EstablishQueryContext(request) => Some(request),
            _ => None,
        })
        .expect("Establish is submitted");
    assert!(
        released
            .iter()
            .all(|intent| intent.kind() != OperationKind::CreateTask),
        "submit acceptance alone cannot release Create"
    );
    assert!(
        harness
            .pump_once()
            .into_iter()
            .flat_map(|(_, intents)| intents)
            .all(|intent| intent.kind() != OperationKind::CreateTask),
        "a queued Establish is not a send-start fact"
    );
    assert_eq!(
        harness
            .execution
            .owner(admission.context())
            .unwrap()
            .create_refusal_observation_horizon(),
        None,
        "a queued parent grants no replacement observation source"
    );
    assert!(
        harness
            .execution
            .establish_send_started(admission.context(), TaskOperationId::new_v7())
            .is_err(),
        "a different Establish cannot open the first Create wave"
    );
    assert_eq!(
        harness
            .execution
            .owner(admission.context())
            .unwrap()
            .create_refusal_observation_horizon(),
        None,
        "a foreign send cannot mint the original observation source"
    );
    harness
        .execution
        .establish_send_started(admission.context(), establish.envelope().operation_id())
        .expect("the exact Establish has started its RPC");
    assert_eq!(
        harness
            .execution
            .owner(admission.context())
            .unwrap()
            .create_refusal_observation_horizon(),
        Some(
            super::context_owner::CreateRefusalObservationHorizon::IssuedEstablish(
                MonotonicInstant::from_origin(Duration::from_secs(10)),
            )
        ),
        "only the exact original send carries its existing ticket horizon"
    );
    assert!(
        harness
            .pump_once()
            .into_iter()
            .flat_map(|(_, intents)| intents)
            .any(|intent| intent.kind() == OperationKind::CreateTask),
        "the exact send-start opens the first Create wave"
    );
}

#[test]
fn normal_quiesce_without_any_create_fences_every_task_before_release() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let context = *harness
        .execution
        .graph()
        .contexts()
        .next()
        .expect("one context");
    harness.execution.begin_normal_drain();
    let first = harness.released();
    assert!(
        first
            .iter()
            .all(|intent| intent.kind() != OperationKind::CreateTask)
    );
    let quiesce = first
        .iter()
        .find(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .expect("normal drain fences even an absent context");
    harness
        .transport_unknown_ack(quiesce)
        .expect("an unknown fence remains replayable");
    let replay = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .expect("the exact fence is replayed");
    assert_eq!(quiesce.operation_id(), replay.operation_id());
    harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            replay.operation_id(),
            OperationKind::QuiesceQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                context,
                1,
                Vec::new(),
                QueryContextState::Quiescing,
            )),
        ))
        .expect("the complete empty membership settles");
    let identities = harness
        .execution
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    for identity in identities {
        let task = harness.execution.task(identity.task_id()).unwrap();
        assert!(task.fenced_out());
        assert!(task.status().is_none(), "fence must not invent TaskStatus");
    }
    assert!(
        harness
            .released()
            .iter()
            .any(|intent| intent.kind() == OperationKind::ReleaseQueryContext),
        "a complete fence allows Release after local nonexistent tasks settle"
    );
}

fn covered_frame(
    context: QueryContextRef,
    generation: u64,
    fact: CoveredStatusStreamFact,
) -> ObservationIntakeEntry {
    ObservationIntakeEntry::Frame(Box::new(ObservationFrame::Covered {
        context,
        generation,
        event: CoveredStatusStreamEvent {
            fact,
            source_revision: None,
        },
    }))
}

#[test]
fn covered_quiesce_receipt_preceding_lost_ack_still_allows_release() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let context = *harness.execution.graph().contexts().next().unwrap();
    harness.execution.begin_normal_drain();
    let quiesce = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .expect("normal drain sends the Context fence");
    let receipt =
        QuiesceQueryContextReceipt::new(context, 1, Vec::new(), QueryContextState::Quiescing);
    harness
        .execution
        .apply_covered_entries(vec![covered_frame(
            context,
            1,
            CoveredStatusStreamFact::Quiesce(receipt.clone()),
        )])
        .expect("the observed complete fence settles membership");
    assert!(
        harness
            .execution
            .owner(context)
            .unwrap()
            .quiesce_acknowledged()
    );
    assert!(
        harness
            .released()
            .iter()
            .any(|intent| intent.kind() == OperationKind::ReleaseQueryContext),
        "an observed complete Quiesce receipt permits Release without the lost ACK"
    );
    harness
        .transport_unknown_ack(&quiesce)
        .expect("late transport unknown cannot retract the positive fence");
}

#[test]
fn covered_initial_unknown_does_not_retract_later_accepted_ack() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let create = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::CreateTask)
        .expect("a frozen task is released");
    let OperationIntent::CreateTask(request) = &create else {
        unreachable!()
    };
    let identity = request.identity();
    let context = harness
        .execution
        .graph()
        .task(identity.task_id())
        .unwrap()
        .context();
    harness
        .create_ack(&create, OperationOutcome::Accepted)
        .expect("the ACK may reach the serial owner before the initial source cut");
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(context, 1, CoveredStatusStreamFact::Unknown(identity)),
            covered_frame(
                context,
                1,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 1,
                    initial_cut: 2,
                }),
            ),
        ])
        .expect("an older negative source cut cannot erase a later accepted ACK");
    assert!(
        harness
            .execution
            .task(identity.task_id())
            .unwrap()
            .create_ownership_proven()
    );
    assert!(harness.execution.covered_observation_ready());
}

#[test]
fn covered_gap_needs_positive_evidence_and_new_complete_catch_up() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let identity = harness.execution.graph().root_identity();
    let context = harness
        .execution
        .graph()
        .task(identity.task_id())
        .unwrap()
        .context();
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(context, 1, CoveredStatusStreamFact::Gone(identity)),
            covered_frame(
                context,
                1,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 1,
                    initial_cut: 2,
                }),
            ),
        ])
        .expect("Gone without the retained terminal fact registers a local gap");
    assert!(!harness.execution.covered_observation_ready());
    assert!(!harness.execution.covered_recovery_expired());
    harness.clock.advance(Duration::from_secs(29));
    assert!(!harness.execution.covered_recovery_expired());
    assert_eq!(
        harness.execution.take_covered_reconciliations(),
        [context].into()
    );

    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(
                context,
                2,
                CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                    identity,
                    TaskConvergenceVersion::FIRST,
                )),
            ),
            covered_frame(
                context,
                2,
                CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 2,
                    sequence: 1,
                    covered_prefix: 0,
                    source_cut: 3,
                }),
            ),
            covered_frame(
                context,
                2,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 2,
                    initial_cut: 3,
                }),
            ),
        ])
        .expect("actual stopped is positive but cannot replace missing status");
    assert!(!harness.execution.covered_observation_ready());
    assert!(harness.execution.task_actual_stopped(identity));
    harness.clock.advance(Duration::from_secs(1));
    assert!(
        harness.execution.covered_recovery_expired(),
        "a newer generation and bookmark cannot renew recovery"
    );
    let request = harness
        .execution
        .covered_subscription_request(context, NonZeroU64::new(3).unwrap())
        .expect("only applied entity facts enter the next request");
    assert_eq!(request.generation.get(), 3);
    assert!(
        request
            .task_convergence_cursors
            .iter()
            .any(|cursor| cursor.identity() == identity
                && cursor.current_version() == Some(TaskConvergenceVersion::FIRST))
    );

    let terminal = TaskStatus::try_new(
        identity,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Canceled,
        Some(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )),
        TaskOutputFacts::default(),
    )
    .unwrap();
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(context, 3, CoveredStatusStreamFact::Status(terminal)),
            covered_frame(
                context,
                3,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 3,
                    initial_cut: 4,
                }),
            ),
        ])
        .expect("a complete newer catch-up restores the missing terminal fact");
    assert!(harness.execution.covered_observation_ready());
    assert!(!harness.execution.covered_recovery_expired());
}

#[test]
fn quiesce_membership_preserves_lost_create_ack_cleanup() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let creates = harness
        .released()
        .into_iter()
        .filter(|intent| intent.kind() == OperationKind::CreateTask)
        .collect::<Vec<_>>();
    let OperationIntent::CreateTask(accepted) = &creates[0] else {
        unreachable!()
    };
    let context = *harness
        .execution
        .graph()
        .contexts()
        .next()
        .expect("one context");
    harness.execution.begin_normal_drain();
    let quiesce = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .expect("normal drain fences the Context");
    harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            quiesce.operation_id(),
            OperationKind::QuiesceQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                context,
                1,
                vec![accepted.identity()],
                QueryContextState::Quiescing,
            )),
        ))
        .expect("the cumulative membership replaces a missing Create ACK");
    let task = harness
        .execution
        .task(accepted.identity().task_id())
        .unwrap();
    assert!(task.create_ownership_proven());
    assert!(task.status().is_none());
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| intent.kind() != OperationKind::ReleaseQueryContext),
        "an accepted member still owes actual terminal observation"
    );
    harness.publish(
        accepted.identity().task_id(),
        TaskState::Canceled,
        Some(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )),
        false,
    );
    assert!(
        harness
            .released()
            .iter()
            .any(|intent| intent.kind() == OperationKind::ReleaseQueryContext),
        "terminal status closes the member's remaining cleanup responsibility"
    );
}

#[test]
fn late_accepted_create_cannot_cross_an_excluding_quiesce_fence() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let create = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::CreateTask)
        .expect("one Create was in flight");
    let OperationIntent::CreateTask(request) = &create else {
        unreachable!()
    };
    let context = *harness
        .execution
        .graph()
        .contexts()
        .next()
        .expect("one context");
    harness.execution.begin_normal_drain();
    let quiesce = harness
        .released()
        .into_iter()
        .find(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .expect("normal drain fences the Context");
    harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            quiesce.operation_id(),
            OperationKind::QuiesceQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                context,
                1,
                Vec::new(),
                QueryContextState::Quiescing,
            )),
        ))
        .expect("the in-flight Create lost the fence race");
    assert!(
        harness
            .execution
            .task(request.identity().task_id())
            .unwrap()
            .fenced_out()
    );
    assert!(
        matches!(
            harness.create_ack(&create, OperationOutcome::Accepted),
            Err(TaskExecutionError::DomainReceipt(_))
        ),
        "a contradictory late Accepted must fail closed"
    );
}

#[test]
fn preparation_busy_uses_bounded_backoff_and_exact_replay() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let create = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .find(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .expect("a create was released");
    harness
        .create_ack(&create, OperationOutcome::PreparationBusy)
        .expect("short preparation pressure keeps the create pending");
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| intent.operation_id() != create.operation_id()),
        "the immediate next pump cannot spin on a busy response"
    );
    harness.clock.advance(Duration::from_millis(10));
    let replay = harness
        .released()
        .into_iter()
        .find(|intent| intent.operation_id() == create.operation_id())
        .expect("the first bounded retry delay elapsed");
    let (OperationIntent::CreateTask(first), OperationIntent::CreateTask(replayed)) =
        (&create, &replay)
    else {
        unreachable!("both intents are creates");
    };
    assert!(first.same_original(replayed));
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
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Data,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
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
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Data,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
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
    let OperationIntent::EstablishQueryContext(request) = &establish else {
        unreachable!("an establish intent carries its exact request");
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

    owner.note_create_owned();
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "creates are not closed while one is outstanding"
    );
    owner.note_create_owned();
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
    assert!(
        owner.release_intent(MonotonicInstant::ORIGIN).is_none(),
        "normal Release still requires the exact Quiesce fence"
    );
    let quiesce = owner
        .quiesce_intent()
        .expect("normal closure mints a fence");
    owner
        .on_quiesce_ack(&OperationAcknowledgement::new(
            quiesce.operation_id(),
            OperationKind::QuiesceQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                context,
                1,
                Vec::new(),
                QueryContextState::Quiescing,
            )),
        ))
        .expect("the fence settles");
    let release = owner
        .release_intent(MonotonicInstant::ORIGIN)
        .expect("closure and drain release the context");

    let not_ready = OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(context, QueryContextState::Quiescing),
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
    let OperationIntent::EstablishQueryContext(request) = establish else {
        unreachable!("the granted owner releases an establish");
    };
    assert_eq!(request.admission_ticket_id(), ticket_id);
}

#[test]
fn an_unconsumed_expired_admission_is_reissued_after_the_worker_ticket_must_be_gone() {
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
    let first = owner
        .admission_intent(MonotonicInstant::ORIGIN)
        .expect("the request is legal")
        .expect("admission is required");
    let first_operation = first.operation_id();
    owner
        .on_admission_ack(
            &admission_ack(&first),
            MonotonicInstant::from_origin(Duration::from_secs(1)),
        )
        .expect("the admission grant settles");

    assert!(
        owner
            .establish_intent(
                FakeEstablish.facts_for(context).expect("frozen facts"),
                MonotonicInstant::from_origin(Duration::from_secs(10)),
            )
            .expect("expiry schedules a replacement grant")
            .is_none()
    );
    assert_eq!(owner.state(), QueryContextState::Absent);
    assert!(!owner.is_released());
    assert!(
        owner
            .admission_intent(MonotonicInstant::from_origin(Duration::from_millis(10_999)))
            .expect("the wait remains valid")
            .is_none(),
        "the Worker may still hold the old ticket"
    );
    let replacement = owner
        .admission_intent(MonotonicInstant::from_origin(Duration::from_secs(11)))
        .expect("the reissue is legal")
        .expect("a fresh grant is requested after the safe bound");
    assert_ne!(replacement.operation_id(), first_operation);
}

#[test]
fn a_grant_received_after_its_conservative_expiry_waits_before_reissue() {
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
    let first = owner
        .admission_intent(MonotonicInstant::ORIGIN)
        .expect("the request is legal")
        .expect("admission is required");
    let first_operation = first.operation_id();
    owner
        .on_admission_ack(
            &admission_ack(&first),
            MonotonicInstant::from_origin(Duration::from_secs(11)),
        )
        .expect("a late definitive grant keeps the attempt authorized");

    assert_eq!(owner.state(), QueryContextState::Absent);
    assert!(!owner.is_released());
    assert!(
        owner
            .admission_intent(MonotonicInstant::from_origin(Duration::from_millis(20_999)))
            .expect("the wait remains valid")
            .is_none()
    );
    let replacement = owner
        .admission_intent(MonotonicInstant::from_origin(Duration::from_secs(21)))
        .expect("the reissue is legal")
        .expect("a fresh grant is requested after the late receipt's safe bound");
    assert_ne!(replacement.operation_id(), first_operation);
}

#[test]
fn a_late_establish_receipt_closes_a_queued_exact_replay() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph =
        build_graph(&schedule, &chain_edges(), &processes, 16).expect("the test graph is valid");
    let sink = Arc::new(SwitchableQueueSink::default());
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
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
        clock as Arc<dyn MonotonicClock>,
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        intake,
    )
    .expect("the graph composes into one execution");
    let acks = TaskAckIntake::new(Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let ack_handle = acks.handle();
    let mut round = TaskRound::new(
        execution,
        acks,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    );
    round.seal_pumps();

    round.turn().expect("initial dispatch");
    let admission = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .find(|intent| {
            matches!(
                intent,
                OperationIntent::AcquireQueryContextAdmissionTicket(_)
            )
        })
        .expect("the context admission was sent");
    let OperationIntent::AcquireQueryContextAdmissionTicket(request) = admission else {
        unreachable!()
    };
    let ticket = QueryContextAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([0x55; 16]).unwrap(),
        request.context(),
        request.valid_for(),
    );
    ack_handle.publish(OperationAcknowledgement::worker_receipt(
        request.envelope().operation_id(),
        OperationKind::AcquireQueryContextAdmissionTicket,
        OperationOutcome::Accepted,
        AckPayload::AdmissionTicket(ticket),
    ));

    round
        .turn()
        .expect("admission settles and Establish dispatches");
    let establish = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .find(|intent| matches!(intent, OperationIntent::EstablishQueryContext(_)))
        .expect("the establish was sent");
    sink.set_submit_open(false);
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
    ));
    round
        .turn()
        .expect("the exact replay remains queued under backpressure");
    let OperationIntent::EstablishQueryContext(establish_request) = &establish else {
        unreachable!()
    };
    let receipt = OperationAcknowledgement::worker_receipt(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Context(
            QueryContextReceipt::new(establish_request.context(), QueryContextState::Active)
                .with_lease(LeaseReceipt::new(
                    LeaseSequence::INITIAL,
                    LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
                    Duration::from_secs(10),
                )),
        ),
    );
    ack_handle.publish(receipt);
    round
        .turn()
        .expect("the older definitive receipt closes the queued replay");
    assert!(round.contexts_established());
    assert!(
        sink.take().is_empty(),
        "the definitely-unsent replay must not cross process transport"
    );
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
    assert!(
        first
            .iter()
            .all(|intent| !matches!(intent, OperationIntent::EstablishQueryContext(_)))
    );

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
    assert!(
        second
            .iter()
            .any(|intent| matches!(intent, OperationIntent::EstablishQueryContext(_)))
    );
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
// Creation admission and payload lifetime
// ---------------------------------------------------------------------------

fn creates_of(released: Vec<OperationIntent>) -> Vec<OperationIntent> {
    released
        .into_iter()
        .filter(|intent| matches!(intent.kind(), OperationKind::CreateTask))
        .collect()
}

fn create_identity(intent: &OperationIntent) -> TaskIdentity {
    let OperationIntent::CreateTask(request) = intent else {
        unreachable!("a create intent carries its request");
    };
    request.identity()
}

#[test]
fn a_full_process_ends_the_pass_so_no_later_create_is_priced_or_reserved() {
    let mut harness = Harness::new(&[0, 1, 2], &[0], 512);
    let backends = harness.context_backends();
    for &backend in &backends {
        harness
            .sink
            .refuse(DispatchLane::Create, backend, QueueRefusal::Process);
    }
    let pricings = creation_tests::pricings_on_this_thread();
    let freezes = creation_tests::metadata_freezes_on_this_thread();

    assert!(
        creates_of(harness.establish_all(Duration::from_secs(10))).is_empty(),
        "no create crosses a full process"
    );
    for _ in 0..4 {
        let asked = harness.sink.reservations(DispatchLane::Create);
        assert!(creates_of(harness.released()).is_empty());
        assert_eq!(
            harness.sink.reservations(DispatchLane::Create) - asked,
            1,
            "a full process ends the pass at its first create"
        );
    }
    assert_eq!(
        creation_tests::pricings_on_this_thread() - pricings,
        1,
        "only the head create was ever priced, once, however many passes it waited"
    );
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread() - freezes,
        0,
        "no waiting create is frozen"
    );

    // Capacity returns: every create goes out, each priced and frozen once.
    for &backend in &backends {
        harness.sink.admit(DispatchLane::Create, backend);
    }
    assert_eq!(creates_of(harness.released()).len(), 5);
    assert_eq!(creation_tests::pricings_on_this_thread() - pricings, 5);
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread() - freezes,
        5
    );
}

#[test]
fn a_full_target_holds_back_only_its_own_creates_and_then_releases_them_in_order() {
    // Leaves alternate over three backends; backend zero also hosts the
    // middle and the root, so it owes four creates.
    let mut harness = Harness::new(&[0, 1, 2, 0, 1, 2], &[0], 512);
    let leaves = harness.stage_tasks(1);
    let full = harness.identity(leaves[0]).backend_process_id();
    harness
        .sink
        .refuse(DispatchLane::Create, full, QueueRefusal::Target);
    let pricings = creation_tests::pricings_on_this_thread();
    let freezes = creation_tests::metadata_freezes_on_this_thread();

    let mut early = creates_of(harness.establish_all(Duration::from_secs(10)));
    for _ in 0..3 {
        early.extend(creates_of(harness.released()));
    }
    assert!(
        early
            .iter()
            .all(|intent| intent.backend_process_id() != full),
        "nothing crosses a full target"
    );
    assert_eq!(early.len(), 4, "every create on the other targets went out");
    assert_eq!(
        creation_tests::pricings_on_this_thread() - pricings,
        5,
        "the other targets' creates and the full target's head, each priced once"
    );
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread() - freezes,
        4,
        "only admitted creates are frozen"
    );

    harness.sink.admit(DispatchLane::Create, full);
    let late = creates_of(harness.released())
        .iter()
        .map(|intent| create_identity(intent).task_id())
        .collect::<Vec<_>>();
    let expected = [
        leaves[0],
        leaves[3],
        harness.stage_tasks(2)[0],
        harness.execution.graph().root_task(),
    ];
    assert_eq!(late, expected, "a target's own order is never crossed");
    assert_eq!(creation_tests::pricings_on_this_thread() - pricings, 8);
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread() - freezes,
        8
    );
}

#[test]
fn a_create_payload_lives_until_its_exact_acknowledgement_and_never_after() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let leaf = harness.stage_tasks(1)[0];
    let leaf_identity = harness.identity(leaf);
    let first = creates_of(harness.establish_all(Duration::from_secs(10)))
        .into_iter()
        .find(|intent| create_identity(intent) == leaf_identity)
        .expect("the leaf has a create");
    let OperationIntent::CreateTask(frozen) = &first else {
        unreachable!("a create intent carries its request");
    };
    let frozen = frozen.clone();
    let operation = first.operation_id();
    let task = harness.execution.task(leaf).expect("the leaf is owned");
    assert!(task.create_frozen() && task.holds_create_payload());

    // An unknown outcome keeps the payload: the replay is the same request.
    harness
        .transport_unknown_ack(&first)
        .expect("an unknown outcome settles without failing");
    drop(first);
    assert!(
        harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .holds_create_payload()
    );
    let replay = creates_of(harness.released())
        .into_iter()
        .find(|intent| intent.operation_id() == operation)
        .expect("the exact request is released again");
    let OperationIntent::CreateTask(replayed) = &replay else {
        unreachable!("a create intent carries its request");
    };
    assert!(replayed.same_original(&frozen));

    // The exact acknowledgement settles the create and lets go of it.
    harness
        .create_ack(&replay, OperationOutcome::Accepted)
        .expect("the replayed create settles");
    drop(replay);
    assert!(
        !harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .holds_create_payload()
    );
    assert_eq!(
        frozen.original_strong_count(),
        1,
        "after the exact acknowledgement nothing but this test holds the frozen create"
    );

    // A late duplicate acknowledgement names nothing and brings nothing back.
    let late = harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            operation,
            OperationKind::CreateTask,
            OperationOutcome::Accepted,
            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                leaf_identity,
                Vec::new(),
                TaskStatus::created(leaf_identity),
            ))),
        ));
    assert!(matches!(late, Err(TaskExecutionError::UnknownOperation)));
    assert!(
        !harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .holds_create_payload()
    );
    assert!(
        creates_of(harness.released())
            .iter()
            .all(|intent| create_identity(intent) != leaf_identity),
        "a settled create is never sent again"
    );
}

#[test]
fn a_create_that_fails_closed_drops_its_payload_and_a_late_answer_cannot_revive_it() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let leaf = harness.stage_tasks(1)[0];
    let leaf_identity = harness.identity(leaf);
    let create = creates_of(harness.establish_all(Duration::from_secs(10)))
        .into_iter()
        .find(|intent| create_identity(intent) == leaf_identity)
        .expect("the leaf has a create");
    let OperationIntent::CreateTask(frozen) = &create else {
        unreachable!("a create intent carries its request");
    };
    let frozen = frozen.clone();
    let operation = create.operation_id();

    // A refusal is terminal for the task: dispatch closes first, and the
    // payload goes with it.
    let _ = harness.create_ack(&create, OperationOutcome::InvalidStateOrRequest);
    drop(create);
    let task = harness.execution.task(leaf).expect("the leaf is owned");
    assert_eq!(task.state(), RemoteTaskState::Terminal);
    assert!(!task.holds_create_payload());
    assert_eq!(frozen.original_strong_count(), 1);

    let late = harness
        .execution
        .acknowledge(&OperationAcknowledgement::new(
            operation,
            OperationKind::CreateTask,
            OperationOutcome::Accepted,
            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                leaf_identity,
                Vec::new(),
                TaskStatus::created(leaf_identity),
            ))),
        ));
    assert!(matches!(late, Err(TaskExecutionError::UnknownOperation)));
    assert!(
        !harness
            .execution
            .task(leaf)
            .expect("the leaf is owned")
            .holds_create_payload()
    );
}

#[test]
fn a_stage_release_skipped_behind_a_full_target_stays_owed_until_every_cancel_goes_out() {
    let mut harness = Harness::new(&[0, 1], &[0, 1], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let leaves = harness.stage_tasks(1);
    let middle = harness.stage_tasks(2);
    let full = harness.identity(leaves[0]).backend_process_id();
    harness
        .sink
        .refuse(DispatchLane::Control, full, QueueRefusal::Target);

    for &task in &middle {
        harness.publish(task, TaskState::Running, None, false);
    }
    for &task in &middle {
        harness.publish(task, TaskState::Flushing, None, false);
    }
    assert_eq!(harness.stage_state(2), StageState::Flushing);
    let cancelled = |released: Vec<OperationIntent>| {
        released
            .into_iter()
            .filter_map(|intent| match intent {
                OperationIntent::CancelTask(request) => Some(request.identity().task_id()),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        cancelled(harness.released()),
        vec![leaves[1]],
        "the reachable target's cancel goes out"
    );
    assert!(
        !harness
            .execution
            .task(leaves[0])
            .expect("the leaf is owned")
            .cancel_requested(),
        "a cancel skipped behind a full target is rolled back, not half-sent"
    );

    // The release is still owed, so the next turn asks for it again.
    harness.sink.admit(DispatchLane::Control, full);
    harness
        .execution
        .apply_status(8)
        .expect("applying intake succeeds");
    assert_eq!(cancelled(harness.released()), vec![leaves[0]]);
    harness
        .execution
        .apply_status(8)
        .expect("applying intake succeeds");
    assert!(
        cancelled(harness.released()).is_empty(),
        "a release that sent every cancel is complete"
    );
}

#[test]
fn a_union_node_counts_every_producer_of_every_edge_that_feeds_it() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[1, 2]);
    // The leaves feed the middle and, with the middle, one root node.
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
    ];
    let graph = build_graph(&schedule, &edges, &processes, 64).expect("a union graph");
    let root = descriptor_of(&graph, graph.root_task());
    let inbound = root
        .topology()
        .inbound()
        .iter()
        .find(|inbound| inbound.node_id().get() == MIDDLE_TO_ROOT_NODE)
        .expect("the union node");
    assert_eq!(
        inbound.sources().len(),
        5,
        "three leaves and two middles feed the union node"
    );
    let mut ordinals = BTreeSet::new();
    for source in inbound.sources() {
        let producer = descriptor_of(&graph, source.task().task_id());
        let edge = producer
            .topology()
            .outbound()
            .iter()
            .find(|edge| edge.destination_node_id() == inbound.node_id())
            .expect("the producer has an edge into the union node");
        assert_eq!(edge.sender_count(), inbound.expected_sender_count());
        assert!(ordinals.insert(edge.sender_ordinal()));
    }
    assert_eq!(
        ordinals,
        (0..5).collect::<BTreeSet<_>>(),
        "every producer of the union counts at its own position"
    );
}

// ---------------------------------------------------------------------------
// Stage state and cancellation
// ---------------------------------------------------------------------------

// Demand release is only a read-side optimization. These witnesses drive the
// original QueryTaskExecution/TaskRound owners and inspect their actual wire
// intents, including work that was not yet Created when drain was requested.
fn delivery_chain_harness(
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
) -> Harness {
    let schedule = chain_schedule(&[0], &[1]);
    let mut edges = chain_edges();
    for edge in &mut edges {
        edge.delivery = delivery;
    }
    Harness::from_graph(
        build_graph(&schedule, &edges, &backends(2), 512)
            .expect("the delivery contract forms the original chain"),
    )
}

/// The original required-input chain with a real Finished root status and
/// nonterminal strict upstream Tasks. Callers install their original source
/// pump before requesting normal convergence.
pub(crate) fn strict_split_failure_convergence_round_fixture() -> (
    TaskRoundForTest,
    crate::task_execution::status_intake::StatusIntakeHandle,
    crate::native::task_transport::TaskAckIntakeHandle,
) {
    let (mut round, status, acks, creates) = round_waiting_on_creates(delivery_chain_harness(
        novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain,
    ));
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    for (version, state, output) in [
        (2, TaskState::Running, false),
        (3, TaskState::Flushing, false),
        (4, TaskState::Finished, true),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(output),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    (round, status, acks)
}

fn assert_no_strict_demand_controls(intents: &[OperationIntent]) {
    assert!(
        intents.iter().all(|intent| match intent {
            OperationIntent::CancelTask(_) => false,
            OperationIntent::UpdateTask(update) => update.domains().iter().all(|domain| {
                !matches!(domain, TaskDomainUpdate::CloseExchangeDestination { .. })
            }),
            _ => true,
        }),
        "strict work cannot acquire a demand Cancel or destination Close: {intents:?}"
    );
}

#[test]
fn strict_producer_and_consumer_keep_unsent_create_when_demand_is_released() {
    use novarocks_execution_contract::ExchangeDeliveryRequirement::{DemandDriven, MustDrain};
    for delivery in [DemandDriven, MustDrain] {
        for stage in [1, 2] {
            let mut harness = delivery_chain_harness(delivery);
            let task = harness.stage_tasks(stage)[0];
            let identity = harness.identity(task);
            harness
                .execution
                .stand_down_task_normally(identity)
                .unwrap();
            let released = harness.settle_until_quiet(Duration::from_secs(10));
            let created = released.iter().any(|intent| {
                matches!(intent, OperationIntent::CreateTask(create) if create.identity() == identity)
            });
            assert_eq!(
                created,
                delivery == MustDrain,
                "{delivery:?}, stage {stage}"
            );
            assert_eq!(
                harness.execution.task(task).unwrap().normally_stood_down(),
                delivery == DemandDriven
            );
            if delivery == MustDrain {
                assert_no_strict_demand_controls(&released);
                assert!(harness.execution.task(task).unwrap().installed());
            }
        }
    }
}

#[test]
fn strict_consumer_unknown_create_survives_demand_release_and_late_ack_opens_input() {
    use novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
    let mut harness = delivery_chain_harness(MustDrain);
    let leaf = harness.identity(harness.stage_tasks(1)[0]);
    let middle_task = harness.stage_tasks(2)[0];
    let middle = harness.identity(middle_task);
    let creates = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .filter(|intent| intent.kind() == OperationKind::CreateTask)
        .collect::<Vec<_>>();
    let late = creates
        .iter()
        .find(|create| create_identity(create) == middle)
        .unwrap()
        .clone();
    for create in &creates {
        if create_identity(create) == middle {
            harness.transport_unknown_ack(create).unwrap();
        } else {
            harness
                .create_ack(create, OperationOutcome::Accepted)
                .unwrap();
        }
    }
    harness.execution.stand_down_task_normally(middle).unwrap();
    let before_ack = harness.released();
    assert_no_strict_demand_controls(&before_ack);
    assert!(
        !harness
            .execution
            .task(middle_task)
            .unwrap()
            .normally_stood_down()
    );
    assert!(before_ack.iter().all(|intent| !matches!(intent, OperationIntent::UpdateTask(update)
        if update.identity() == leaf && update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. })))), "unknown strict destination still needs its real Installed fact");
    harness
        .create_ack(&late, OperationOutcome::Accepted)
        .unwrap();
    let after_ack = harness.released();
    assert_no_strict_demand_controls(&after_ack);
    assert!(after_ack.iter().any(|intent| matches!(intent, OperationIntent::UpdateTask(update)
        if update.identity() == leaf && update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. })))), "late strict Installed resolves the original opening barrier");
}

#[test]
fn strict_normal_drain_request_keeps_create_split_and_late_edge_open_until_terminals() {
    use novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
    let mut harness = delivery_chain_harness(MustDrain);
    let leaf = harness.stage_tasks(1)[0];
    let middle = harness.stage_tasks(2)[0];
    let root = harness.stage_tasks(3)[0];
    let leaf_identity = harness.identity(leaf);
    let middle_identity = harness.identity(middle);
    harness.sink.refuse(
        DispatchLane::Create,
        leaf_identity.backend_process_id(),
        QueueRefusal::Target,
    );
    let first = harness.establish_all(Duration::from_secs(10));
    let creates = first
        .into_iter()
        .filter(|intent| intent.kind() == OperationKind::CreateTask)
        .collect::<Vec<_>>();
    // The leaf and root share backend zero: both may be transport-blocked.
    // Keep every already-released create, including the middle's late ACK.
    assert!(
        creates
            .iter()
            .any(|intent| create_identity(intent) == middle_identity)
    );
    assert!(
        !creates
            .iter()
            .any(|intent| create_identity(intent) == leaf_identity)
    );
    harness.execution.begin_normal_drain();
    assert!(!harness.execution.normal_drain_started());
    assert!(matches!(
        harness
            .execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, true))
            .unwrap(),
        UpdateAdmission::Preparing | UpdateAdmission::Queued
    ));
    harness
        .sink
        .admit(DispatchLane::Create, leaf_identity.backend_process_id());
    let late = harness.released();
    assert_no_strict_demand_controls(&late);
    assert!(late.iter().any(|intent| matches!(intent, OperationIntent::CreateTask(create) if create.identity() == leaf_identity)), "the unsent strict producer still gets Created");
    let mut all_creates = creates;
    all_creates.extend(
        late.into_iter()
            .filter(|intent| intent.kind() == OperationKind::CreateTask),
    );
    for create in &all_creates {
        harness
            .create_ack(create, OperationOutcome::Accepted)
            .unwrap();
    }
    let continued = harness.settle_until_quiet(Duration::from_secs(10));
    assert_no_strict_demand_controls(&continued);
    assert!(continued.iter().any(|intent| matches!(intent, OperationIntent::UpdateTask(update)
        if update.identity() == leaf_identity && update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::SplitAssignment(_))))), "the original split remains deliverable after requesting drain");
    assert!(continued.iter().any(|intent| matches!(intent, OperationIntent::UpdateTask(update)
        if update.identity() == leaf_identity && update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. })))), "late strict Installed opens its producer edge");
    assert!(!harness.execution.normal_drain_started());
    for task in [root, middle] {
        for (state, complete) in [
            (TaskState::Running, false),
            (TaskState::Flushing, false),
            (TaskState::Finished, true),
        ] {
            harness.publish(task, state, None, complete);
        }
    }
    let still_waiting = harness.released();
    assert_no_strict_demand_controls(&still_waiting);
    assert!(
        !harness.execution.normal_drain_started(),
        "one live strict endpoint still owes actual Task completion"
    );
    assert!(
        still_waiting
            .iter()
            .all(|intent| !matches!(intent, OperationIntent::QuiesceQueryContext(_)))
    );
    for (state, complete) in [
        (TaskState::Running, false),
        (TaskState::Flushing, false),
        (TaskState::Finished, true),
    ] {
        harness.publish(leaf, state, None, complete);
    }
    let closing = harness.released();
    assert!(harness.execution.normal_drain_started());
    assert!(
        closing
            .iter()
            .any(|intent| matches!(intent, OperationIntent::QuiesceQueryContext(_))),
        "ordinary Context closure begins only after all strict endpoints terminate"
    );
}

#[test]
fn task_round_root_seal_keeps_strict_late_installation_and_pumps_alive_until_real_tail() {
    use crate::task_execution::round::{PumpCloseUnresolved, TurnPump};
    use novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain;
    struct ProbePump {
        drives: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        closed: bool,
    }
    impl TurnPump for ProbePump {
        fn name(&self) -> &'static str {
            "strict-drain-regression"
        }
        fn drive(&mut self, _: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
            assert!(!self.closed);
            self.drives.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }
        fn begin_close(&mut self) {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.closed = true;
        }
        fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
            Ok(0)
        }
        fn is_closed(&self) -> bool {
            self.closed
        }
        fn take_close_unresolved(&mut self) -> Option<PumpCloseUnresolved> {
            None
        }
    }
    fn installed_ack(intent: &OperationIntent) -> OperationAcknowledgement {
        let OperationIntent::CreateTask(create) = intent else {
            unreachable!();
        };
        OperationAcknowledgement::new(
            intent.operation_id(),
            OperationKind::CreateTask,
            OperationOutcome::Accepted,
            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                create.identity(),
                Vec::new(),
                TaskStatus::created(create.identity()).with_installed(),
            ))),
        )
    }
    let harness = delivery_chain_harness(MustDrain);
    let sink = harness.sink.clone();
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let root = round.root_task();
    let source = round.take_root_status_source().unwrap();
    let drives = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    round.add_pump(Box::new(ProbePump {
        drives: drives.clone(),
        closes: closes.clone(),
        closed: false,
    }));
    let root_create = creates
        .iter()
        .find(|create| create_identity(create) == root)
        .unwrap();
    acks.publish(installed_ack(root_create));
    round.turn().unwrap();
    let mut seal = source.begin_success_seal_request().unwrap();
    // The original ordered intake applies this control before later statuses.
    round.turn().unwrap();
    assert!(matches!(
        seal.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    for (version, state, complete) in [
        (2, TaskState::Running, false),
        (3, TaskState::Flushing, false),
        (4, TaskState::Finished, true),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(complete),
            )
            .unwrap()
            .with_installed(),
        )));
        round.turn().unwrap();
    }
    assert_eq!(seal.try_recv().unwrap(), Ok(()));
    assert!(round.accepted_root_success_sealed());
    assert!(!round.execution().normal_drain_started());
    assert!(!round.attempt_drained());
    assert_eq!(
        closes.load(Ordering::SeqCst),
        0,
        "root result demand cannot close strict input owners"
    );
    let after_seal_drives = drives.load(Ordering::SeqCst);
    let mut released = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    for create in creates
        .iter()
        .filter(|create| create_identity(create) != root)
    {
        acks.publish(installed_ack(create));
        round.turn().unwrap();
        released.extend(
            sink.take()
                .into_iter()
                .flat_map(|(_, operations)| operations),
        );
    }
    assert_no_strict_demand_controls(&released);
    assert!(released.iter().any(|intent| matches!(intent, OperationIntent::UpdateTask(update)
        if update.domains().iter().any(|domain| matches!(domain, TaskDomainUpdate::OpenExchangeEdges { .. })))), "late strict Create ACK still authors real Open after root seal");
    assert!(drives.load(Ordering::SeqCst) > after_seal_drives);
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    for intent in &released {
        if let OperationIntent::UpdateTask(update) = intent {
            acks.publish(OperationAcknowledgement::new(
                intent.operation_id(),
                OperationKind::UpdateTask,
                OperationOutcome::Accepted,
                AckPayload::Update(UpdateTaskReceipt::new(
                    update.identity(),
                    update.domains().iter().map(task_domain_receipt).collect(),
                )),
            ));
        }
    }
    for create in creates
        .iter()
        .filter(|create| create_identity(create) != root)
    {
        for (version, state, complete) in [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ] {
            status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    create_identity(create),
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    None,
                    TaskOutputFacts::new(complete),
                )
                .unwrap()
                .with_installed(),
            )));
            round.turn().unwrap();
        }
    }
    round.turn().unwrap();
    assert!(round.execution().normal_drain_started());
    assert_eq!(
        closes.load(Ordering::SeqCst),
        1,
        "the original pump closes exactly once after the required Task tail"
    );
    assert!(round.failure_cause().is_none());
    assert!(
        round.accepted_root_success_sealed(),
        "finishing strict obligations does not replace the accepted root cut"
    );
}

#[test]
fn mixed_strict_input_ordinary_output_never_bypasses_the_task_stand_down_guard() {
    use novarocks_execution_contract::ExchangeDeliveryRequirement::{DemandDriven, MustDrain};
    for incoming in [DemandDriven, MustDrain] {
        let schedule = chain_schedule(&[0], &[1]);
        let mut edges = chain_edges();
        edges[0].delivery = incoming;
        edges[1].delivery = DemandDriven;
        let graph = build_graph(&schedule, &edges, &backends(2), 512).unwrap();
        let mut harness = Harness::from_graph(graph);
        harness.settle_until_quiet(Duration::from_secs(10));
        let middle_task = harness.stage_tasks(2)[0];
        let middle = harness.identity(middle_task);
        let root = harness.stage_tasks(3)[0];
        for task in [harness.stage_tasks(1)[0], middle_task, root] {
            harness.publish(task, TaskState::Running, None, false);
        }
        // The parent release travels through the real stage reducer. The
        // middle's outgoing edge is ordinary, but it still owns its strict
        // incoming obligation; a second bulk Cancel author must not undo the
        // exact stand_down_task_normally decision.
        harness.publish(root, TaskState::Flushing, None, false);
        let released = harness.released();
        let cancels_middle = released
            .iter()
            .filter(|intent| {
                matches!(intent, OperationIntent::CancelTask(cancel)
                if cancel.identity() == middle
                    && cancel.reason() == CancelReason::UpstreamNoLongerNeeded)
            })
            .count();
        assert_eq!(
            cancels_middle,
            usize::from(incoming == DemandDriven),
            "{incoming:?}"
        );
        assert_eq!(
            harness
                .execution
                .task(middle_task)
                .unwrap()
                .normally_stood_down(),
            incoming == DemandDriven
        );
        assert_eq!(
            harness
                .execution
                .task(middle_task)
                .unwrap()
                .cancel_requested(),
            incoming == DemandDriven
        );
        if incoming == MustDrain {
            assert_eq!(
                harness.execution.task(middle_task).unwrap().task_state(),
                TaskState::Running
            );
            assert!(
                released.iter().all(
                    |intent| !matches!(intent, OperationIntent::UpdateTask(update)
                if update.domains().iter().any(|domain| matches!(domain,
                    TaskDomainUpdate::CloseExchangeDestination { destination, .. }
                    if *destination == middle)))
                ),
                "a parent release cannot withdraw the middle's required input"
            );
            harness.publish(root, TaskState::Finished, None, true);
            let finished_root = harness.released();
            assert!(finished_root.iter().all(|intent| !matches!(intent, OperationIntent::CancelTask(cancel) if cancel.identity() == middle)));
            assert!(
                !harness
                    .execution
                    .task(middle_task)
                    .unwrap()
                    .cancel_requested()
            );
            assert!(harness.execution.failure_cause().is_none());
        }
    }
}

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
fn finished_receiver_projects_only_its_exact_destination_close() {
    let mut harness = Harness::new(&[0], &[1, 2], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let middle = harness.stage_tasks(2);
    let closed = harness.identity(middle[0]);
    let sibling = harness.identity(middle[1]);
    harness.publish(middle[0], TaskState::Running, None, false);
    harness.publish(middle[1], TaskState::Running, None, false);
    harness.publish(middle[0], TaskState::Flushing, None, false);
    harness.publish(middle[0], TaskState::Finished, None, true);

    let closes = harness
        .released()
        .into_iter()
        .filter_map(|intent| match intent {
            OperationIntent::UpdateTask(update) => {
                update.domains().iter().find_map(|domain| match domain {
                    TaskDomainUpdate::CloseExchangeDestination { destination, .. } => {
                        Some((update.identity(), *destination))
                    }
                    _ => None,
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(closes.len(), 1, "only the finished receiver closes");
    assert_eq!(closes[0].1, closed);
    assert_ne!(closes[0].1, sibling);
    assert_eq!(
        closes[0].0.stage_id().get(),
        1,
        "the producer owns the update"
    );
}

#[test]
fn closing_unknown_receiver_does_not_block_needed_sibling_open() {
    let mut harness = Harness::new(&[0], &[1, 2], 512);
    let creates = harness
        .establish_all(Duration::from_secs(10))
        .into_iter()
        .filter(|intent| intent.kind() == OperationKind::CreateTask)
        .collect::<Vec<_>>();
    let middle = harness.stage_tasks(2);
    let closed = harness.identity(middle[0]);
    let needed = harness.identity(middle[1]);
    let leaf = harness.identity(harness.stage_tasks(1)[0]);
    for create in &creates {
        let identity = create_identity(create);
        if identity == closed {
            harness.transport_unknown_ack(create).unwrap();
        } else if identity == needed || identity == leaf {
            harness
                .create_ack(create, OperationOutcome::Accepted)
                .unwrap();
        }
    }
    assert!(harness.released().iter().all(|intent| {
        !matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf)
    }));

    harness
        .execution
        .stand_down_task_normally(closed)
        .expect("the exact local no-more-input authorization is valid");
    let first = harness.released();
    let close = first
        .iter()
        .find(|intent| matches!(intent, OperationIntent::UpdateTask(update)
            if update.identity() == leaf
                && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == closed)))
        .expect("the exact close is projected first")
        .clone();
    assert!(first.iter().all(|intent| {
        !matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf
            && matches!(update.domains(), [TaskDomainUpdate::OpenExchangeEdges { .. }]))
    }));
    assert!(first.iter().all(|intent| {
        !matches!(intent, OperationIntent::CreateTask(create) if create.identity() == closed)
    }));
    harness
        .update_ack(&close, OperationOutcome::Accepted)
        .unwrap();
    let second = harness.released();
    assert!(second.iter().any(|intent| {
        matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf
            && matches!(update.domains(), [TaskDomainUpdate::OpenExchangeEdges { .. }]))
    }));
    for update in &second {
        if update.kind() == OperationKind::UpdateTask {
            harness
                .update_ack(update, OperationOutcome::Accepted)
                .unwrap();
        }
    }
    let late = creates
        .iter()
        .find(|create| create_identity(create) == closed)
        .unwrap();
    harness
        .create_ack(late, OperationOutcome::Accepted)
        .unwrap();
    let after_late = harness.released();
    assert!(after_late.iter().any(|intent| {
        matches!(intent, OperationIntent::CancelTask(cancel) if cancel.identity() == closed
            && cancel.reason() == CancelReason::UpstreamNoLongerNeeded)
    }));
    assert!(after_late.iter().all(|intent| {
        !matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf)
    }));
}

#[test]
fn close_effect_survives_process_queue_backpressure() {
    let mut harness = Harness::new(&[0], &[1, 2], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let leaf = harness.identity(harness.stage_tasks(1)[0]);
    let closed = harness.identity(harness.stage_tasks(2)[0]);
    harness.sink.refuse(
        DispatchLane::Update,
        leaf.backend_process_id(),
        QueueRefusal::Process,
    );
    harness.publish(closed.task_id(), TaskState::Running, None, false);
    harness.publish(closed.task_id(), TaskState::Flushing, None, false);
    harness.publish(closed.task_id(), TaskState::Finished, None, true);
    assert!(harness.released().iter().all(|intent| {
        !matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { .. }]))
    }));

    harness
        .sink
        .admit(DispatchLane::Update, leaf.backend_process_id());
    let released = harness.released();
    assert!(released.iter().any(|intent| {
        matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == closed))
    }));
}

#[test]
fn one_full_producer_does_not_block_another_close_effect() {
    let mut harness = Harness::new(&[0, 1], &[2, 3], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let leaves = harness.stage_tasks(1);
    let blocked = harness.identity(leaves[0]);
    let free = harness.identity(leaves[1]);
    let closed = harness.identity(harness.stage_tasks(2)[0]);
    harness.sink.refuse(
        DispatchLane::Update,
        blocked.backend_process_id(),
        QueueRefusal::Process,
    );
    harness.publish(closed.task_id(), TaskState::Running, None, false);
    harness.publish(closed.task_id(), TaskState::Flushing, None, false);
    harness.publish(closed.task_id(), TaskState::Finished, None, true);
    let first = harness.released();
    assert!(first.iter().any(|intent| {
        matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == free
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == closed))
    }));
    assert!(first.iter().all(|intent| {
        !matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == blocked
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { .. }]))
    }));
    harness
        .sink
        .admit(DispatchLane::Update, blocked.backend_process_id());
    let second = harness.released();
    assert!(second.iter().any(|intent| {
        matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == blocked
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == closed))
    }));
}

#[test]
fn normal_drain_retains_exact_close_updates() {
    let mut harness = Harness::new(&[0], &[1], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let leaf = harness.identity(harness.stage_tasks(1)[0]);
    let middle = harness.identity(harness.stage_tasks(2)[0]);
    harness.execution.begin_normal_drain();
    let released = harness.released();
    assert!(released.iter().any(|intent| {
        matches!(intent, OperationIntent::UpdateTask(update) if update.identity() == leaf
            && matches!(update.domains(), [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == middle))
    }));
}

#[test]
fn production_close_reducer_explores_authorized_unknown_receiver_interleavings() {
    #[derive(Copy, Clone, Debug)]
    enum Event {
        AuthorizeClose,
        AcceptedAck,
        Installed,
        NeededSiblingReady,
        QueueAvailable,
    }
    fn permutations(events: &mut [Event], position: usize, traces: &mut Vec<Vec<Event>>) {
        if position == events.len() {
            traces.push(events.to_vec());
            return;
        }
        for next in position..events.len() {
            events.swap(position, next);
            permutations(events, position + 1, traces);
            events.swap(position, next);
        }
    }
    let mut traces = Vec::new();
    permutations(
        &mut [
            Event::AuthorizeClose,
            Event::AcceptedAck,
            Event::Installed,
            Event::NeededSiblingReady,
            Event::QueueAvailable,
        ],
        0,
        &mut traces,
    );
    let mut closes = 0;
    let mut opens = 0;
    let mut late_ack = 0;
    for trace in &traces {
        let mut harness = Harness::new(&[0], &[1, 2], 512);
        let creates = creates_of(harness.establish_all(Duration::from_secs(10)));
        let middle = harness.stage_tasks(2);
        let closed = harness.identity(middle[0]);
        let needed = harness.identity(middle[1]);
        let producer = harness.identity(harness.stage_tasks(1)[0]);
        let a = creates
            .iter()
            .find(|c| create_identity(c) == closed)
            .unwrap();
        let b = creates
            .iter()
            .find(|c| create_identity(c) == needed)
            .unwrap();
        let p = creates
            .iter()
            .find(|c| create_identity(c) == producer)
            .unwrap();
        harness.create_ack(p, OperationOutcome::Accepted).unwrap();
        harness.sink.refuse(
            DispatchLane::Update,
            producer.backend_process_id(),
            QueueRefusal::Process,
        );
        let installed = TaskStatus::try_new(
            closed,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Planned,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap()
        .with_installed();
        let mut authorized = false;
        let mut observed_closes = 0;
        let mut observed_opens = 0;
        for event in trace {
            match event {
                // This is an explicit caller authorization of the production
                // seam, not a claim that an independent A-only SQL owner exists.
                Event::AuthorizeClose => {
                    authorized = true;
                    harness.execution.stand_down_task_normally(closed).unwrap();
                    harness.execution.stand_down_task_normally(closed).unwrap();
                }
                Event::AcceptedAck => {
                    if authorized {
                        late_ack += 1;
                    }
                    harness
                        .execution
                        .acknowledge(&OperationAcknowledgement::new(
                            a.operation_id(),
                            OperationKind::CreateTask,
                            OperationOutcome::Accepted,
                            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                                closed,
                                Vec::new(),
                                TaskStatus::created(closed),
                            ))),
                        ))
                        .unwrap_or_else(|error| panic!("{trace:?}: Accepted: {error}"));
                }
                Event::Installed => {
                    for _ in 0..2 {
                        harness
                            .execution
                            .intake()
                            .handle()
                            .publish(StatusEvent::Published(Box::new(installed.clone())));
                        harness
                            .execution
                            .apply_status(64)
                            .unwrap_or_else(|error| panic!("{trace:?}: Installed: {error}"));
                    }
                }
                Event::NeededSiblingReady => {
                    harness
                        .create_ack(b, OperationOutcome::Accepted)
                        .unwrap_or_else(|error| panic!("{trace:?}: B ready: {error}"));
                }
                Event::QueueAvailable => {
                    harness
                        .sink
                        .admit(DispatchLane::Update, producer.backend_process_id());
                }
            }
            for _ in 0..4 {
                let released = harness
                    .pump_once()
                    .into_iter()
                    .flat_map(|(_, operations)| operations)
                    .collect::<Vec<_>>();
                if released.is_empty() {
                    break;
                }
                for intent in released {
                    assert!(
                        !authorized
                            || !matches!(&intent,
                        OperationIntent::CreateTask(create) if create.identity() == closed),
                        "{trace:?}: local close restored A Create"
                    );
                    match &intent {
                        OperationIntent::UpdateTask(update) if update.identity() == producer => {
                            for domain in update.domains() {
                                match domain {
                                    TaskDomainUpdate::CloseExchangeDestination {
                                        destination,
                                        ..
                                    } => {
                                        assert_eq!(*destination, closed, "{trace:?}: B was closed");
                                        observed_closes += 1;
                                    }
                                    TaskDomainUpdate::OpenExchangeEdges { .. } => {
                                        observed_opens += 1
                                    }
                                    _ => panic!("{trace:?}: unexpected producer input"),
                                }
                            }
                            harness
                                .update_ack(&intent, OperationOutcome::Accepted)
                                .unwrap();
                        }
                        OperationIntent::CancelTask(cancel) => {
                            assert_eq!(cancel.identity(), closed, "{trace:?}: B canceled");
                            assert_eq!(cancel.reason(), CancelReason::UpstreamNoLongerNeeded);
                            harness.cancel_ack(&intent);
                        }
                        _ => {}
                    }
                }
            }
        }
        assert_eq!(observed_closes, 1, "{trace:?}: lost or duplicated close");
        assert_eq!(observed_opens, 1, "{trace:?}: A blocked necessary B");
        assert!(
            !harness
                .execution
                .task(closed.task_id())
                .unwrap()
                .create_pending(),
            "{trace:?}: replay eligibility revived"
        );
        assert_eq!(
            harness
                .execution
                .enqueue_task_update(closed.task_id(), split_update(SCAN_NODE, 1, false),)
                .unwrap(),
            UpdateAdmission::DiscardedNoLongerNeeded,
            "{trace:?}: late ownership restored ordinary input"
        );
        closes += observed_closes;
        opens += observed_opens;
    }
    assert_eq!(traces.len(), 120);
    assert_eq!((closes, opens, late_ack), (120, 120, 60));
    eprintln!(
        "production close exploration: traces=120 closes={closes} opens={opens} late_ack={late_ack}"
    );
}

#[test]
fn production_close_reducer_explores_normal_drain_and_backpressure() {
    #[derive(Copy, Clone, Debug)]
    enum Event {
        AuthorizeClose,
        QueueAvailable,
        NormalDrain,
        DuplicateAuthorization,
    }
    fn permutations(events: &mut [Event], position: usize, traces: &mut Vec<Vec<Event>>) {
        if position == events.len() {
            traces.push(events.to_vec());
            return;
        }
        for next in position..events.len() {
            events.swap(position, next);
            permutations(events, position + 1, traces);
            events.swap(position, next);
        }
    }
    let mut traces = Vec::new();
    permutations(
        &mut [
            Event::AuthorizeClose,
            Event::QueueAvailable,
            Event::NormalDrain,
            Event::DuplicateAuthorization,
        ],
        0,
        &mut traces,
    );
    let mut delivered = 0;
    for trace in &traces {
        let mut harness = Harness::new(&[0], &[1, 2], 512);
        harness.settle_until_quiet(Duration::from_secs(10));
        let middle = harness.stage_tasks(2);
        let closed = harness.identity(middle[0]);
        let producer = harness.identity(harness.stage_tasks(1)[0]);
        harness.sink.refuse(
            DispatchLane::Update,
            producer.backend_process_id(),
            QueueRefusal::Process,
        );
        let mut observed = BTreeMap::<TaskIdentity, usize>::new();
        for event in trace {
            match event {
                Event::AuthorizeClose | Event::DuplicateAuthorization => {
                    harness.execution.stand_down_task_normally(closed).unwrap()
                }
                Event::QueueAvailable => harness
                    .sink
                    .admit(DispatchLane::Update, producer.backend_process_id()),
                Event::NormalDrain => harness.execution.begin_normal_drain(),
            }
            for _ in 0..4 {
                let released = harness
                    .pump_once()
                    .into_iter()
                    .flat_map(|(_, operations)| operations)
                    .collect::<Vec<_>>();
                if released.is_empty() {
                    break;
                }
                for intent in released {
                    match &intent {
                        OperationIntent::UpdateTask(update) => {
                            if update.identity() == producer {
                                for domain in update.domains() {
                                    if let TaskDomainUpdate::CloseExchangeDestination {
                                        destination,
                                        ..
                                    } = domain
                                    {
                                        *observed.entry(*destination).or_default() += 1;
                                    }
                                }
                            }
                            harness
                                .update_ack(&intent, OperationOutcome::Accepted)
                                .unwrap_or_else(|error| panic!("drain trace {trace:?}: {error}"));
                        }
                        OperationIntent::CancelTask(_) => harness.cancel_ack(&intent),
                        _ => {}
                    }
                }
            }
        }
        assert_eq!(
            observed.get(&closed),
            Some(&1),
            "drain trace {trace:?}: A close lost"
        );
        assert_eq!(
            observed.get(&harness.identity(middle[1])),
            Some(&1),
            "drain trace {trace:?}: whole-attempt drain lost B close"
        );
        assert_eq!(
            observed.len(),
            2,
            "drain trace {trace:?}: non-frozen destination"
        );
        delivered += observed.values().sum::<usize>();
    }
    assert_eq!((traces.len(), delivered), (24, 48));
    eprintln!("production drain exploration: traces=24 exact_closes={delivered}");

    let failure = TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Data,
            novarocks_execution_contract::failure::FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::new(1).unwrap(),
            ),
        ),
        novarocks_execution::task_execution::SafeDetail::truncating("exploration failure"),
    ));
    for close_first in [false, true] {
        let mut harness = Harness::new(&[0], &[1, 2], 512);
        harness.settle_until_quiet(Duration::from_secs(10));
        let a = harness.stage_tasks(2)[0];
        let identity = harness.identity(a);
        if close_first {
            harness
                .execution
                .stand_down_task_normally(identity)
                .unwrap();
        }
        harness.publish(a, TaskState::Running, None, false);
        harness.publish(a, TaskState::Failing, Some(failure.clone()), false);
        harness.publish(a, TaskState::Failed, Some(failure.clone()), false);
        if !close_first {
            harness
                .execution
                .stand_down_task_normally(identity)
                .unwrap();
        }
        assert_eq!(
            harness.execution.failure_cause(),
            Some(&failure),
            "close_first={close_first}: normal close disguised failure before seal"
        );
        // The actual result owner refuses this latched failure at its seal.
    }
    eprintln!("production failure exploration: close_failure_orders=2 preserved_failures=2");
}

#[test]
fn receiver_context_release_waits_for_exact_inbound_sender_stop() {
    let mut harness = Harness::new(&[0], &[1], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let leaf = harness.stage_tasks(1)[0];
    let middle = harness.stage_tasks(2)[0];
    let root = harness.execution.graph().root_task();
    let leaf_context = harness.execution.graph().task(leaf).unwrap().context();
    let middle_context = harness.execution.graph().task(middle).unwrap().context();
    assert_ne!(leaf_context, middle_context);
    harness.execution.activate_covered_observation();

    for task in [leaf, middle, root] {
        harness.publish(task, TaskState::Running, None, false);
    }
    for task in [leaf, middle, root] {
        harness.publish(task, TaskState::Flushing, None, false);
        harness.publish(task, TaskState::Finished, None, true);
    }
    harness.execution.begin_normal_drain();
    let fences = harness
        .released()
        .into_iter()
        .filter(|intent| intent.kind() == OperationKind::QuiesceQueryContext)
        .collect::<Vec<_>>();
    assert_eq!(fences.len(), 2);
    for fence in fences {
        let OperationIntent::QuiesceQueryContext(request) = &fence else {
            unreachable!()
        };
        let context = request.context();
        let accepted = harness
            .execution
            .graph()
            .tasks()
            .filter(|task| task.context() == context)
            .map(|task| task.identity())
            .collect();
        harness
            .execution
            .acknowledge(&OperationAcknowledgement::new(
                fence.operation_id(),
                OperationKind::QuiesceQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                    context,
                    1,
                    accepted,
                    QueryContextState::Quiescing,
                )),
            ))
            .unwrap();
    }
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| intent.kind() != OperationKind::ReleaseQueryContext),
        "terminal status does not prove a sender has actually stopped"
    );

    harness
        .execution
        .apply_covered_entries(vec![covered_frame(
            leaf_context,
            1,
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                harness.identity(leaf),
                TaskConvergenceVersion::FIRST,
            )),
        )])
        .unwrap();
    let released = harness
        .released()
        .into_iter()
        .filter_map(|intent| match intent {
            OperationIntent::ReleaseQueryContext(request) => Some(request.context()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        released.is_empty(),
        "the receiver's own Task must also have actually stopped"
    );

    harness
        .execution
        .apply_covered_entries(vec![covered_frame(
            middle_context,
            1,
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                harness.identity(middle),
                TaskConvergenceVersion::FIRST,
            )),
        )])
        .unwrap();
    let released = harness
        .released()
        .into_iter()
        .filter_map(|intent| match intent {
            OperationIntent::ReleaseQueryContext(request) => Some(request.context()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(released, [middle_context]);

    harness
        .execution
        .apply_covered_entries(vec![covered_frame(
            leaf_context,
            1,
            CoveredStatusStreamFact::TaskConvergence(TaskConvergenceReceipt::actual_stopped(
                harness.identity(root),
                TaskConvergenceVersion::FIRST,
            )),
        )])
        .unwrap();
    let released = harness
        .released()
        .into_iter()
        .filter_map(|intent| match intent {
            OperationIntent::ReleaseQueryContext(request) => Some(request.context()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(released, [leaf_context]);
}

#[test]
fn a_shared_producer_is_cancelled_only_after_every_consumer_stops_consuming() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[0]);
    let graph = build_graph(&schedule, &multicast_edges(), &processes, 512)
        .expect("a CTE producer may feed both the middle and root stages");
    let mut harness = Harness::from_graph(graph);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaves = harness.stage_tasks(1);
    let middle = harness.stage_tasks(2)[0];
    let root = harness.execution.graph().root_task();
    for &leaf in &leaves {
        harness.publish(leaf, TaskState::Running, None, false);
    }
    harness.publish(root, TaskState::Running, None, false);
    harness.publish(middle, TaskState::Running, None, false);
    harness.publish(middle, TaskState::Flushing, None, false);
    harness.publish(middle, TaskState::Finished, None, true);

    assert_eq!(harness.stage_state(2), StageState::Finished);
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| !matches!(intent, OperationIntent::CancelTask(_))),
        "the root still consumes the same producer after the middle finishes"
    );
    for &leaf in &leaves {
        assert!(
            !harness.execution.task(leaf).unwrap().cancel_requested(),
            "one finished multicast branch must not cancel a shared producer"
        );
    }

    // The earlier parent's release remains remembered. The last consumer's
    // transition releases the shared producer without another middle update.
    harness.publish(root, TaskState::Flushing, None, false);
    let cancels = harness
        .released()
        .into_iter()
        .filter_map(|intent| match intent {
            OperationIntent::CancelTask(request) => Some(request),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(cancels.len(), leaves.len());
    assert_eq!(
        cancels
            .iter()
            .map(|request| request.identity().task_id())
            .collect::<BTreeSet<_>>(),
        leaves.into_iter().collect()
    );
    assert!(
        cancels
            .iter()
            .all(|request| { request.reason() == CancelReason::UpstreamNoLongerNeeded })
    );

    harness.execution.apply_status(8).unwrap();
    assert!(
        harness
            .released()
            .iter()
            .all(|intent| !matches!(intent, OperationIntent::CancelTask(_))),
        "repeated propagation must not mint another cancellation"
    );
}

#[test]
fn accepted_root_seal_does_not_wait_for_upstream_cancellation() {
    let harness = Harness::new(&[0], &[0], 512);
    let leaf = harness.stage_tasks(1)[0];
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let source = round.take_root_status_source().unwrap();
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    let leaf = round.execution().task(leaf).unwrap().identity();
    for identity in [root, leaf] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(2).unwrap(),
                TaskState::Running,
                None,
                TaskOutputFacts::new(false),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    // QA owns local End consumption; this test exercises its real control port.
    let mut reply = source.begin_success_seal_request().unwrap();
    round.turn().unwrap();
    assert!(
        reply.try_recv().is_err(),
        "a running Root cannot seal success"
    );
    assert!(!round.accepted_root_success_sealed());
    for (version, state, output) in [
        (3, TaskState::Flushing, false),
        (4, TaskState::Finished, true),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(output),
            )
            .unwrap(),
        )));
        round.turn().unwrap();
        assert_eq!(
            round.execution().task(root.task_id()).unwrap().task_state(),
            state,
        );
    }
    assert_eq!(reply.try_recv().unwrap(), Ok(()));
    assert!(round.accepted_root_success_sealed());
    assert!(
        !round.attempt_drained(),
        "the original upstream/Release tail remains"
    );
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            leaf,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Canceling,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::new(false),
        )
        .unwrap(),
    )));
    round.turn().unwrap();
    assert!(
        round.accepted_root_success_sealed(),
        "upstream stand-down cannot retract the accepted cut"
    );
    assert!(round.failure_cause().is_none());
}

#[test]
fn a_task_failure_latches_once_and_withholds_client_completion() {
    let mut harness = Harness::new(&[0], &[0], 512);
    harness.settle_until_quiet(Duration::from_secs(10));

    let leaf = harness.stage_tasks(1)[0];
    let failure = TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Data,
            novarocks_execution_contract::failure::FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::new(1).unwrap(),
            ),
        ),
        novarocks_execution::task_execution::SafeDetail::truncating("first cause"),
    ));
    harness.publish(leaf, TaskState::Running, None, false);
    harness.publish(leaf, TaskState::Failing, Some(failure.clone()), false);
    harness.publish(leaf, TaskState::Failed, Some(failure.clone()), false);

    assert_eq!(harness.execution.failure_cause(), Some(&failure));
    assert_eq!(harness.stage_state(1), StageState::Failed);
    // The actual result owner refuses this latched failure at its seal.
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
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
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
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
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

// Reuse the original update-admission graph and owner construction. The sink
// still exercises the real QueryTaskExecution/RemoteTask producer path.
fn update_admission_execution(sink: Arc<dyn TaskOperationSink>) -> (QueryTaskExecution, TaskId) {
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
    let wake = Arc::new(CountingWake::default());
    let intake = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::DEFAULT,
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
        Arc::new(ManualClock::new()),
        sink,
        intake,
    )
    .expect("compose execution");

    (execution, leaf)
}

fn split_payload(update: &TaskDomainUpdate) -> &Arc<dyn CodecOwnedContent> {
    let TaskDomainUpdate::SplitAssignment(split) = update else {
        panic!("the fixture retains one exact split input");
    };
    split.payload()
}

#[test]
fn task_update_process_rejection_precedes_remote_task_retention() {
    let sink = Arc::new(QueueBackpressureOnceSink::default());
    let (mut execution, leaf) =
        update_admission_execution(Arc::clone(&sink) as Arc<dyn TaskOperationSink>);
    let update = split_update(SCAN_NODE, 1, false);
    let original = Arc::downgrade(split_payload(&update));
    assert_eq!(
        execution
            .enqueue_task_update(leaf, update)
            .expect("stable producer accepts its input"),
        UpdateAdmission::Preparing
    );
    let task = execution.task(leaf).expect("the task is owned");
    assert_eq!(
        task.pending_updates(),
        0,
        "no domain fact precedes queue admission"
    );
    assert_eq!(
        original.strong_count(),
        1,
        "only the original pending cell owns this input"
    );
    assert!(Arc::ptr_eq(
        &original
            .upgrade()
            .expect("same original input remains live"),
        split_payload(task.preparing_update().expect("same pending preparation")),
    ));

    execution
        .pump(&FakeEstablish)
        .expect("the next turn resumes the same cell");
    let task = execution.task(leaf).expect("the task is owned");
    assert!(task.preparing_update().is_none());
    assert_eq!(task.pending_updates(), 1);
    assert_eq!(original.strong_count(), 1);
    assert!(Arc::ptr_eq(
        &original
            .upgrade()
            .expect("original input was transferred, not rebuilt"),
        split_payload(
            task.first_pending_update()
                .expect("one admitted original update")
        ),
    ));
}

#[test]
fn pending_update_input_is_withdrawn_before_normal_or_terminal_pump() {
    for terminal_cleanup in [false, true] {
        let sink = Arc::new(SwitchableQueueSink::default());
        let (mut execution, leaf) =
            update_admission_execution(Arc::clone(&sink) as Arc<dyn TaskOperationSink>);
        let queued = split_update(SCAN_NODE, 1, false);
        let queued_original = Arc::downgrade(split_payload(&queued));
        assert_eq!(
            execution.enqueue_task_update(leaf, queued).unwrap(),
            UpdateAdmission::Queued
        );
        assert_eq!(sink.live_queue_permits(), 1);
        sink.set_queue_open(false);
        let update = split_update(SCAN_NODE, 2, false);
        let original = Arc::downgrade(split_payload(&update));
        assert_eq!(
            execution.enqueue_task_update(leaf, update).unwrap(),
            UpdateAdmission::Preparing
        );
        assert_eq!(original.strong_count(), 1);
        assert_eq!(sink.reserve_attempts(), 2);
        assert_eq!(sink.live_queue_permits(), 1);
        assert_eq!(execution.task(leaf).unwrap().pending_updates(), 1);
        if terminal_cleanup {
            execution.begin_terminal_cleanup();
        } else {
            execution.begin_normal_drain();
        }
        assert!(
            original.upgrade().is_none(),
            "the never-dispatched original input actually exits"
        );
        assert!(
            queued_original.upgrade().is_none(),
            "the original admitted but unsent input exits too"
        );
        assert_eq!(
            sink.live_queue_permits(),
            0,
            "the actual unsent queue permit exits"
        );
        let task = execution.task(leaf).unwrap();
        assert!(task.preparing_update().is_none());
        assert_eq!(task.pending_updates(), 0);
        assert_eq!(task.discarded_updates(), 2);
        sink.set_queue_open(true);
        execution
            .pump(&FakeEstablish)
            .expect("closure still converges");
        assert_eq!(
            sink.ordinary_update_reserve_attempts(),
            2,
            "no ordinary input physical reservation after withdrawal"
        );
        assert!(
            sink.take()
                .iter()
                .flat_map(|(_, operations)| operations)
                .all(|intent| {
                    let OperationIntent::UpdateTask(request) = intent else {
                        return true;
                    };
                    request.domains().iter().all(|update| {
                        matches!(update, TaskDomainUpdate::CloseExchangeDestination { .. })
                    })
                }),
            "only genuine destination-close controls may continue"
        );
    }
}

#[test]
fn terminal_status_withdraws_preparing_input_before_queue_reopens() {
    let sink = Arc::new(SwitchableQueueSink::default());
    sink.set_queue_open(false);
    let (mut execution, leaf) =
        update_admission_execution(Arc::clone(&sink) as Arc<dyn TaskOperationSink>);
    let update = split_update(SCAN_NODE, 1, false);
    let original = Arc::downgrade(split_payload(&update));
    assert_eq!(
        execution.enqueue_task_update(leaf, update).unwrap(),
        UpdateAdmission::Preparing
    );
    let identity = execution.task(leaf).unwrap().identity();
    for (version, state) in [(2, TaskState::Canceling), (3, TaskState::Canceled)] {
        assert_eq!(
            execution
                .intake()
                .handle()
                .publish(StatusEvent::Published(Box::new(
                    TaskStatus::try_new(
                        identity,
                        TaskStatusVersion::new(version).unwrap(),
                        state,
                        Some(TerminationDetail::Canceled(
                            CancelReason::UpstreamNoLongerNeeded,
                        )),
                        TaskOutputFacts::default(),
                    )
                    .expect("a legal cancellation status"),
                ))),
            StatusIntakeAdmission::Enqueued
        );
        execution
            .apply_status(64)
            .expect("the original status author applies cancellation");
    }
    assert!(original.upgrade().is_none());
    assert!(execution.task(leaf).unwrap().is_terminal());
    assert_eq!(execution.task(leaf).unwrap().pending_updates(), 0);
    sink.set_queue_open(true);
    // Terminal task input is decided before entering the queue gate. Other
    // original context lifecycle operations may still converge in this turn.
    let attempts = sink.reserve_attempts();
    assert_eq!(
        execution
            .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
            .unwrap(),
        UpdateAdmission::DiscardedNoLongerNeeded,
    );
    assert_eq!(sink.reserve_attempts(), attempts);
    assert_eq!(sink.live_queue_permits(), 0);
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
fn terminal_cleanup_stops_new_context_lifecycle_issuance() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let compatibility = NativeCompatibilityId::new([0x42; 32]);
    let mut owner = QueryContextOwner::new(context, 0, compatibility, admission_epoch());
    let admission = owner
        .admission_intent(MonotonicInstant::ORIGIN)
        .expect("the request is legal")
        .expect("admission is required");
    let OperationIntent::AcquireQueryContextAdmissionTicket(request) = admission else {
        unreachable!("admission has its own operation kind");
    };
    owner
        .on_admission_ack(
            &OperationAcknowledgement::transport_unknown(
                request.envelope().operation_id(),
                OperationKind::AcquireQueryContextAdmissionTicket,
            ),
            MonotonicInstant::from_origin(Duration::from_millis(1)),
        )
        .expect("an unknown admission outcome remains one exact in-flight request");

    owner.begin_terminal_cleanup();

    assert!(
        owner
            .admission_intent(MonotonicInstant::from_origin(Duration::from_millis(2)))
            .expect("terminal cleanup is not an admission error")
            .is_none(),
        "terminal cleanup must not replay a normal admission request"
    );
    assert!(
        owner
            .establish_intent(
                FakeEstablish
                    .facts_for(context)
                    .expect("the fake source has facts"),
                MonotonicInstant::from_origin(Duration::from_millis(2)),
            )
            .expect("terminal cleanup is not an establish error")
            .is_none(),
        "a late ticket acknowledgement must not cause a new Establish"
    );
    assert!(
        owner
            .renew_intent(MonotonicInstant::from_origin(Duration::from_secs(30)))
            .expect("terminal cleanup is not a renewal error")
            .is_none(),
        "terminal cleanup must not mint a successor lease renewal"
    );
}

#[test]
fn an_initial_establish_rejection_remains_a_pre_ready_infrastructure_fact() {
    let context = QueryContextRef::new(
        execution_id(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let mut owner = QueryContextOwner::new(
        context,
        0,
        NativeCompatibilityId::new([0x43; 32]),
        admission_epoch(),
    );
    grant_admission(&mut owner, MonotonicInstant::ORIGIN);
    let establish = owner
        .establish_intent(
            FakeEstablish
                .facts_for(context)
                .expect("the fake source has facts"),
            MonotonicInstant::ORIGIN,
        )
        .expect("the establish can be built")
        .expect("the establish is released");

    let error = owner
        .on_context_ack(&OperationAcknowledgement::new(
            establish.operation_id(),
            OperationKind::UpdateQueryContext,
            OperationOutcome::InvalidStateOrRequest,
            AckPayload::None,
        ))
        .expect_err("a rejected initial Establish must terminate its local owner");

    assert!(matches!(
        error,
        TaskExecutionError::PreReadyEstablishRejected{context: rejected_context,operation_id,rejection} if rejection.outcome()==OperationOutcome::InvalidStateOrRequest && rejected_context == context && operation_id == establish.operation_id()
    ));
    assert!(owner.is_released());
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
fn a_full_target_queue_holds_later_creates_back_instead_of_being_exceeded() {
    // Every create carries a static plan near the descriptor bound, so a
    // moderate fan-out to one backend fills that backend's queue. A full
    // queue is backpressure, not a failure: the creates that do not fit wait,
    // unmeasured again and unfrozen, and follow as capacity is released.
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 15 * 1024 * 1024);
    let metadata_freezes = creation_tests::metadata_freezes_on_this_thread();
    let mut created = BTreeSet::new();
    let mut rounds = 0_usize;
    loop {
        rounds += 1;
        assert!(rounds <= 256, "the fan-out must drain");
        let released = harness.released();
        assert_bounds(
            harness.execution.dispatcher(),
            DispatchBudget::DEFAULT,
            TransportBudget::DEFAULT,
        );
        if released.is_empty() {
            break;
        }
        for intent in released {
            match intent.kind() {
                OperationKind::CreateTask => {
                    assert!(
                        created.insert(intent.operation_id()),
                        "a create is released once while its first send is unanswered"
                    );
                    harness
                        .create_ack(&intent, OperationOutcome::Accepted)
                        .expect("a create acknowledgement settles");
                }
                OperationKind::UpdateQueryContext => {
                    harness.context_ack(&intent, Duration::from_secs(30));
                }
                OperationKind::UpdateTask => harness
                    .update_ack(&intent, OperationOutcome::Accepted)
                    .expect("an update acknowledgement settles"),
                kind => unreachable!("no {kind} is produced by this fan-out"),
            }
        }
    }
    assert_eq!(created.len(), 66, "every task of the attempt was created");
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread() - metadata_freezes,
        66,
        "waiting behind a full queue never freezes a create twice"
    );
    assert_eq!(
        CapacityBound::BackendBytes { limit: 1 }.as_operation_outcome(),
        OperationOutcome::ResourceExhausted
    );
}

#[test]
fn an_operation_that_outlives_queue_residence_fails_typed_and_rolls_back_its_owner() {
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 512);
    harness
        .sink
        .backpressured_lanes
        .lock()
        .unwrap()
        .insert(DispatchLane::Create);
    let released = harness.pump();
    assert!(!released.is_empty());

    // The process transport declines Create submission after queue admission.
    // Deployment W now limits admission, so an in-flight RPC alone cannot
    // manufacture additional queued Creates behind it.
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

#[test]
fn queued_admission_does_not_imply_a_worker_context_or_create_send() {
    let mut pending = Harness::new(&[0], &[0], 512);
    let contexts = pending
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let released = pending.pump_once();
    assert!(
        released
            .iter()
            .flat_map(|(_, intents)| intents)
            .any(|intent| {
                matches!(
                    intent,
                    OperationIntent::AcquireQueryContextAdmissionTicket(_)
                )
            })
    );
    assert!(
        released
            .iter()
            .flat_map(|(_, intents)| intents)
            .all(|intent| { !matches!(intent, OperationIntent::CreateTask(_)) })
    );
    assert!(
        contexts
            .iter()
            .all(|&context| pending.execution.context_never_established(context))
    );

    let admission = released
        .iter()
        .flat_map(|(_, intents)| intents)
        .find_map(|intent| match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => Some(request),
            _ => None,
        })
        .unwrap();
    pending
        .execution
        .acknowledge(&OperationAcknowledgement::worker_receipt(
            admission.envelope().operation_id(),
            OperationKind::AcquireQueryContextAdmissionTicket,
            OperationOutcome::Accepted,
            AckPayload::AdmissionTicket(QueryContextAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([0x55; 16]).unwrap(),
                admission.context(),
                admission.valid_for(),
            )),
        ))
        .unwrap();
    assert!(
        !pending
            .execution
            .context_never_established(admission.context())
    );

    let mut establishing = Harness::new(&[0], &[0], 512);
    let contexts = establishing
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    let _ = establishing.pump();
    assert!(
        contexts
            .iter()
            .all(|&context| !establishing.execution.context_never_established(context))
    );
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
        handle.publish(StatusEvent::Published(Box::new(TaskStatus::created(
            identity
        )))),
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
    handle.publish(StatusEvent::Published(Box::new(TaskStatus::created(
        identity,
    ))));
    assert_eq!(
        handle.publish(StatusEvent::Published(Box::new(TaskStatus::created(
            identity
        )))),
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
        .publish(StatusEvent::Published(Box::new(status)));
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
        assert_eq!(task.identity(), target.identity);
        assert_eq!(task.fragment_id(), LEAF_FRAGMENT);
    }
    assert!(
        assignment_targets(&graph, PlanNodeId::new(SCAN_NODE + 1).expect("a plan node")).is_empty()
    );

    // Only a genuinely unknown transport outcome is replayable, and that
    // verdict comes from the delivery owner rather than being restated here.
    let legacy_driver_target = crate::query_execution::split_assignment::AssignmentTarget {
        backend_idx: 0,
        fragment_instance_id: targets[0].fragment_instance_id,
    };
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::Transport {
            target: legacy_driver_target.clone(),
            detail: "acknowledgement lost".to_owned(),
        }),
        crate::task_execution::operation::FrontendAction::RetryExactRequest
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::Rejected {
            target: legacy_driver_target,
            reason: "watermark".to_owned(),
            detail: "gap".to_owned(),
        }),
        crate::task_execution::operation::FrontendAction::FailAttempt
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::NoAdmittedTask {
            scan: crate::query_execution::split_assignment::ScanNodeKey::new(1u32, 9,),
        }),
        crate::task_execution::operation::FrontendAction::FailAttempt
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
    stopped: Mutex<Vec<QueryContextRef>>,
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

    fn stop(&self, context: QueryContextRef) {
        self.stopped
            .lock()
            .expect("subscription stop ledger")
            .push(context);
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

/// Terminal cleanup is a local observation boundary.  It must close every
/// status stream before the bounded remote-convergence period starts, while
/// leaving any already-running Worker work to its own lease and stop rules.
#[test]
fn terminal_cleanup_stops_status_observation_for_every_context() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;

    let harness = Harness::new(&[0, 1], &[0], 512);
    let mut expected = harness
        .execution
        .graph()
        .contexts()
        .copied()
        .collect::<Vec<_>>();
    expected.sort_unstable();

    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let mut round = TaskRound::new(
        harness.execution,
        TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>),
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();

    round.begin_terminal_cleanup();
    round
        .turn()
        .expect("a terminal-cleanup turn only settles already-owned work");

    let mut stopped = subscriptions.stopped.lock().expect("stop ledger").clone();
    stopped.sort_unstable();
    assert_eq!(stopped, expected);
    assert!(
        subscriptions
            .ensured
            .lock()
            .expect("ensure ledger")
            .is_empty(),
        "terminal cleanup must not open a new status observation stream"
    );
    assert!(
        subscriptions
            .resubscribed
            .lock()
            .expect("resubscribe ledger")
            .is_empty(),
        "terminal cleanup must not reopen status observation"
    );
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
pub(crate) fn establish_ack(intent: &OperationIntent) -> OperationAcknowledgement {
    let OperationIntent::EstablishQueryContext(request) = intent else {
        unreachable!("an establish intent carries its exact request");
    };
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
pub(crate) fn admission_ack(intent: &OperationIntent) -> OperationAcknowledgement {
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

pub(crate) fn accepted_create_ack(intent: &OperationIntent) -> OperationAcknowledgement {
    let OperationIntent::CreateTask(request) = intent else {
        unreachable!("a create intent carries its request");
    };
    OperationAcknowledgement::new(
        intent.operation_id(),
        OperationKind::CreateTask,
        OperationOutcome::Accepted,
        AckPayload::Create(Box::new(CreateTaskReceipt::new(
            request.identity(),
            Vec::new(),
            TaskStatus::created(request.identity()),
        ))),
    )
}

#[test]
fn a_rejected_establish_does_not_discard_later_context_acknowledgements() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::dispatch::DispatchOperationState;
    use crate::task_execution::round::TaskRound;

    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();
    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    );
    round.seal_pumps();

    round.turn().expect("admission dispatch");
    for intent in released_admissions(&sink) {
        acks.publish(admission_ack(&intent));
    }
    round.turn().expect("admission settlement");
    let establishes = released_establishes(&sink);
    assert_eq!(establishes.len(), 2);
    let OperationIntent::EstablishQueryContext(rejected) = &establishes[0] else {
        unreachable!("an establish carries its request");
    };
    let OperationIntent::EstablishQueryContext(accepted) = &establishes[1] else {
        unreachable!("an establish carries its request");
    };
    acks.publish(OperationAcknowledgement::new(
        establishes[0].operation_id(),
        OperationKind::UpdateQueryContext,
        OperationOutcome::InvalidStateOrRequest,
        AckPayload::None,
    ));
    acks.publish(establish_ack(&establishes[1]));

    assert!(matches!(
        round.turn(),
        Err(TaskExecutionError::PreReadyEstablishRejected { .. })
    ));
    assert_eq!(
        round.execution().owner(accepted.context()).unwrap().state(),
        QueryContextState::Active,
        "a failed peer must not discard this context's definitive Worker receipt"
    );
    assert!(
        round
            .execution()
            .owner(rejected.context())
            .unwrap()
            .is_released()
    );
    assert_eq!(
        round
            .execution()
            .dispatcher()
            .operation_state(establishes[1].operation_id())
            .unwrap(),
        DispatchOperationState::Absent,
        "the accepted establish cannot remain in flight after its receipt"
    );
}

#[test]
fn an_invalid_send_started_does_not_discard_later_worker_acknowledgements() {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::dispatch::DispatchOperationState;
    use crate::task_execution::round::TaskRound;

    let harness = Harness::new(&[0], &[0], 512);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let intake = TaskAckIntake::new(wake as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();
    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()),
    );
    round.seal_pumps();
    round.turn().expect("admission dispatch");
    for intent in released_admissions(&sink) {
        acks.publish(admission_ack(&intent));
    }
    round.turn().expect("admission settlement");
    let establishes = released_establishes(&sink);
    let OperationIntent::EstablishQueryContext(request) = &establishes[0] else {
        unreachable!("released establish");
    };
    acks.publish_establish_send_started(
        novarocks_execution::task_execution::TaskOperationId::new_v7(),
        request.context(),
    );
    acks.publish(establish_ack(&establishes[0]));
    assert!(matches!(
        round.turn(),
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert_eq!(
        round.execution().owner(request.context()).unwrap().state(),
        QueryContextState::Active,
        "a malformed transport event must not discard a following Worker ownership receipt",
    );
    assert_eq!(
        round
            .execution()
            .dispatcher()
            .operation_state(establishes[0].operation_id())
            .unwrap(),
        DispatchOperationState::Absent,
        "the receipt settles its exact operation despite the earlier error",
    );
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
    let OperationIntent::EstablishQueryContext(first_request) = &establishes[0] else {
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
    use crate::task_execution::error::ParticipantObservationFailure;
    use crate::task_execution::round::TaskRound;

    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let sink = Arc::clone(&harness.sink);
    let wake = Arc::clone(&harness.wake);
    let status_intake = harness.execution.intake().handle();
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

    // A fatal state discovered while reconciling an observation boundary must
    // fail before the round submits another task operation.
    *subscriptions.settled.lock().expect("settled") = Some(SubscriptionState::ProcessMismatch);
    status_intake.note_observation_incomplete();
    let error = round
        .turn()
        .expect_err("a backend out of observation fails the attempt");
    let TaskExecutionError::ParticipantUnobservable { backend, state } = error else {
        panic!("expected an unobservable participant, got {error:?}");
    };
    assert_eq!(
        state,
        ParticipantObservationFailure::IdentityViolation("process_mismatch")
    );
    assert!(
        processes.values().any(|process| *process == backend),
        "the failure has to name a backend of this attempt, got {backend}"
    );
    assert!(
        !subscriptions
            .resubscribed
            .lock()
            .expect("resubscription ledger")
            .is_empty(),
        "the fatal state was checked at the observation-loss reconciliation boundary"
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

    // Sending establishes is neither an accepted Root seal nor a protocol tail.
    assert!(!round.accepted_root_success_sealed());
    assert!(!round.attempt_drained());
    assert!(round.failure_cause().is_none());
    assert_eq!(round.root_task(), round.execution().graph().root_identity());

    // Packet gaps are checked through the actual QA root relay tests.
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
            status_handle.publish(StatusEvent::Published(Box::new(TaskStatus::created(
                identity
            )))),
            StatusIntakeAdmission::Enqueued
        );
    }
    assert_eq!(
        status_handle.publish(StatusEvent::Published(Box::new(TaskStatus::created(
            identity
        )))),
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

/// The real RemoteTask may recover missing observation versions, but an
/// already accepted Task cause is not a missing-event inference. It survives
/// that gap, with only the explicit live derived-peer refinement exception.
#[test]
fn remote_task_catch_up_preserves_accepted_causes_and_allows_only_derived_refinement() {
    use super::remote_task::RemoteTask;
    use novarocks_coordination_contract::StatusObservation;
    use novarocks_execution_contract::{SafeDetail, TaskFailure};
    let original = || {
        TerminationDetail::Failed(TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new("original source").unwrap(),
        ))
    };
    for (held_cause, proposed, legal) in [
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            original(),
            false,
        ),
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Aborted(AbortCause::LeaseExpired),
            false,
        ),
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            true,
        ),
        (
            TerminationDetail::Aborted(AbortCause::QueryFailed),
            original(),
            false,
        ),
        (
            original(),
            TerminationDetail::Failed(TaskFailure::new(
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Data,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
                SafeDetail::new("another source").unwrap(),
            )),
            false,
        ),
        (
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
            original(),
            true,
        ),
        (
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            false,
        ),
    ] {
        let processes = backends(1);
        let schedule = chain_schedule(&[0], &[0]);
        let graph = build_graph(&schedule, &chain_edges(), &processes, 64).unwrap();
        let (_, seeds) = graph.into_seeds();
        let seed = seeds.into_values().next().unwrap();
        let identity = seed.identity();
        let mut task = RemoteTask::new(seed, Vec::new()).unwrap();
        let state_for = |cause: &TerminationDetail, terminal| match (cause, terminal) {
            (TerminationDetail::Canceled(_), false) => TaskState::Canceling,
            (TerminationDetail::Canceled(_), true) => TaskState::Canceled,
            (TerminationDetail::Aborted(_), false) => TaskState::Aborting,
            (TerminationDetail::Aborted(_), true) => TaskState::Aborted,
            (TerminationDetail::Failed(_), false) => TaskState::Failing,
            (TerminationDetail::Failed(_), true) => TaskState::Failed,
        };
        let held = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(2).unwrap(),
            state_for(&held_cause, false),
            Some(held_cause),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert_eq!(
            task.observe_status(&held).unwrap(),
            StatusObservation::Accept
        );
        let later = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(9).unwrap(),
            state_for(&proposed, true),
            Some(proposed),
            TaskOutputFacts::default(),
        )
        .unwrap();
        if legal {
            assert_eq!(
                task.observe_status(&later).unwrap(),
                StatusObservation::Accept
            );
            assert_eq!(task.status(), Some(&later));
            assert!(task.is_terminal());
        } else {
            assert!(matches!(
                task.observe_status(&later),
                Err(TaskExecutionError::Observation(
                    StatusObservation::ConclusionOverwrite
                ))
            ));
            assert_eq!(task.status(), Some(&held));
            assert_eq!(task.cursor().current_version(), Some(held.version()));
            assert!(!task.is_terminal());
        }
    }
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
            .publish(StatusEvent::Published(Box::new(status)));
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
    novarocks_query_application::test_support::LogicalExecutionTestHarness,
    QueryContextRef,
    OperationIntent,
    Arc<ManualClock>,
) {
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionTicketId, LeaseValidFor,
        QueryContextAdmissionTicketReceipt,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, AdmissionIssueSettlement, LogicalExecutionActorConfig,
    };
    use novarocks_query_application::test_support::LogicalExecutionTestHarness;
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
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
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
    let mut logical = LogicalExecutionTestHarness::install(
        tokio::runtime::Handle::current(),
        config,
        execution_id(),
        vec![exact_context],
    )
    .expect("install logical execution");
    logical.activate_initial().await.expect("activate");
    let actor_admission = AcquireQueryContextAdmissionTicket::new(
        TaskOperationId::new_v7(),
        exact_context,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([7; 32]),
        AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
    );
    let (activation, pending) = logical
        .begin_admission_issue_and_abandon(actor_admission)
        .await
        .expect("begin actor admission");
    logical
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
    let released = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    let first_abort = released
        .iter()
        .find(|intent| matches!(intent.kind(), OperationKind::AbortQueryContext))
        .cloned()
        .expect("first actor Abort reaches transport");
    // The logical owner has already committed to Abort. Freeze normal work
    // before these tests isolate replay of its actor-owned control request.
    round.begin_terminal_cleanup();
    (
        round,
        sink,
        ack_handle,
        logical,
        exact_context,
        first_abort,
        clock,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_abort_local_dispatch_refusal_preserves_cause_without_context_closure() {
    use novarocks_query_application::coordination::ContextClosureState;
    use novarocks_workload_control::WorkError;
    let (mut round, _sink, acknowledgements, mut logical, context, abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    acknowledgements.publish(
        crate::native::apply_send_owner::canonical_dispatch_rejection_fixture(
            abort.operation_id(),
            OperationKind::AbortQueryContext,
            WorkError::Capacity("Native dispatch proof work"),
        ),
    );
    assert_eq!(
        round.turn().unwrap_err(),
        TaskExecutionError::Workload(WorkError::Capacity("Native dispatch proof work"))
    );
    assert!(matches!(
        round
            .execution()
            .actor_abort_dispatch_state(abort.operation_id()),
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert!(!round.execution().owner(context).unwrap().is_released());
    assert!(
        round
            .execution()
            .context_convergence_receipt(context)
            .is_none()
    );
    let snapshot = logical.stand_down_snapshot(context).await.unwrap().unwrap();
    assert!(!matches!(
        snapshot.closure(),
        ContextClosureState::NoRemoteResponsibility
            | ContextClosureState::TerminalRetained
            | ContextClosureState::Gone
            | ContextClosureState::WorkerStoppedAndContextFenced
            | ContextClosureState::WorkerProcessReplaced
    ));
    // Only the fixture's independent process fence can settle the residual.
    logical
        .observe_worker_process_replaced(context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_abort_dispatch_cause_does_not_override_wrong_operation_kind() {
    use novarocks_workload_control::WorkError;
    let (mut round, _sink, acknowledgements, mut logical, context, abort, _clock) =
        actor_abort_round_at_first_dispatch().await;
    acknowledgements.publish(
        crate::native::apply_send_owner::canonical_dispatch_rejection_fixture(
            abort.operation_id(),
            OperationKind::CancelTask,
            WorkError::Capacity("Native dispatch proof work"),
        ),
    );
    assert!(matches!(round.turn(), Err(TaskExecutionError::Schedule(_))));
    assert!(!round.execution().owner(context).unwrap().is_released());
    assert!(
        round
            .execution()
            .context_convergence_receipt(context)
            .is_none()
    );
    logical
        .observe_worker_process_replaced(context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[test]
fn terminal_cleanup_dispatches_actor_abort_without_normal_work() {
    let mut harness = Harness::new(&[0], &[0], 64);
    let context = *harness
        .execution
        .graph()
        .contexts()
        .next()
        .expect("one context");
    let intent = OperationIntent::AbortQueryContext(
        novarocks_execution::task_execution::AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            novarocks_execution::task_execution::AbortCause::QueryFailed,
        ),
    );
    let operation_id = intent.operation_id();
    let reservation = harness
        .execution
        .try_reserve_actor_abort(&intent)
        .expect("actor Abort is legal")
        .expect("the isolated lifecycle lane has capacity");
    harness
        .execution
        .enqueue_actor_abort(intent, reservation)
        .expect("actor Abort owns its queue reservation");
    harness.execution.begin_terminal_cleanup();

    let report = harness
        .execution
        .pump(&harness.establish)
        .expect("terminal cleanup still dispatches actor Abort");
    assert_eq!(report.operations, 1);
    let submitted = harness
        .sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .collect::<Vec<_>>();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].operation_id(), operation_id);
    assert_eq!(submitted[0].kind(), OperationKind::AbortQueryContext);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actor_abort_waits_for_real_lifecycle_capacity_and_replays_exactly() {
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_execution::task_execution::{
        AcquireQueryContextAdmissionTicket, AdmissionTicketId, LeaseValidFor,
        QueryContextAdmissionTicketReceipt,
    };
    use novarocks_query_application::coordination::{
        AbortQueryContextEffectPort, AdmissionIssueSettlement, ContextClosureState,
        LogicalExecutionActorConfig,
    };
    use novarocks_query_application::test_support::LogicalExecutionTestHarness;

    use super::status_intake::TestIntakeWake;
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
    let wake = Arc::new(TestIntakeWake::default());
    let status = StatusIntake::new(64, Arc::clone(&wake) as Arc<dyn StatusIntakeWake>);
    let execution = QueryTaskExecution::new(
        graph,
        DispatchBudget::new(16, 12, 1, 4).expect("nonzero dispatch budget"),
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &admission_epochs,
        &admission_epochs
            .keys()
            .map(|&process| (process, 4096))
            .collect(),
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
    let mut logical = LogicalExecutionTestHarness::install(
        tokio::runtime::Handle::current(),
        config,
        execution_id(),
        vec![exact_context],
    )
    .expect("install logical execution");
    logical.activate_initial().await.expect("activate");
    let actor_admission = AcquireQueryContextAdmissionTicket::new(
        TaskOperationId::new_v7(),
        exact_context,
        LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        NativeCompatibilityId::new([7; 32]),
        AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
    );
    let (activation, pending) = logical
        .begin_admission_issue_and_abandon(actor_admission)
        .await
        .expect("begin actor admission");
    logical
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
    loop {
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if round.queued_abort_effects() != 0 {
            break;
        }
        notified.await;
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

    let replay = loop {
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        round.turn().expect("notification-driven retry turn");
        let mut replay = None;
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
        if let Some(replay) = replay {
            break replay;
        }
        notified.await;
    };
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
    assert!(
        round
            .execution()
            .owner(exact_context)
            .unwrap()
            .is_released(),
        "a definitive actor Abort receipt is positive context closure evidence"
    );
    assert!(
        round
            .execution()
            .context_convergence_receipt(exact_context)
            .is_none(),
        "a replayed Abort receipt settles control without manufacturing actual Worker exit"
    );
    ack_handle.publish(OperationAcknowledgement::transport_unknown(
        operation_id,
        OperationKind::AbortQueryContext,
    ));
    let tombstoned = round
        .turn()
        .expect("replay gen2's later callback is absorbed by the exact closing tombstone");
    assert_eq!(tombstoned.acknowledgements, 1);
    assert_eq!(
        logical
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
    logical
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_late_receipt_consumes_an_adapter_queued_replay() {
    use novarocks_query_application::coordination::ContextClosureState;

    let (mut round, sink, ack_handle, mut logical, exact_context, first_abort, _clock) =
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
        if logical
            .stand_down_snapshot(exact_context)
            .await
            .unwrap()
            .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    logical
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_late_receipt_cancels_a_dispatcher_queued_replay() {
    use novarocks_query_application::coordination::ContextClosureState;

    let (mut round, sink, ack_handle, mut logical, exact_context, first_abort, _clock) =
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
        if logical
            .stand_down_snapshot(exact_context)
            .await
            .unwrap()
            .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    logical
        .observe_worker_stopped_and_context_fenced(exact_context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_actor_abort_expiry_returns_definitely_unsent_for_bounded_replay() {
    use novarocks_query_application::coordination::{
        AbortQueryContextIssueState, ContextClosureState,
    };

    let (mut round, sink, ack_handle, mut logical, exact_context, first_abort, clock) =
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
            && logical
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
    logical
        .observe_worker_process_replaced(exact_context)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
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
    round_waiting_on_creates_with_establish_trace(harness, None)
}

fn round_waiting_on_creates_with_establish_trace(
    harness: Harness,
    trace: Option<&mut Vec<(OperationIntent, OperationAcknowledgement)>>,
) -> (
    TaskRoundForTest,
    crate::task_execution::status_intake::StatusIntakeHandle,
    crate::native::task_transport::TaskAckIntakeHandle,
    Vec<OperationIntent>,
) {
    round_waiting_on_creates_with_establish_delivery(harness, trace, true)
}

fn round_waiting_on_creates_with_establish_delivery(
    harness: Harness,
    mut trace: Option<&mut Vec<(OperationIntent, OperationAcknowledgement)>>,
    acknowledge_establish: bool,
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
                    let ack = admission_ack(&intent);
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.push((intent.clone(), ack.clone()));
                    }
                    acks.publish(ack);
                }
                OperationKind::UpdateQueryContext => {
                    let ack = if acknowledge_establish {
                        establish_ack(&intent)
                    } else {
                        let OperationIntent::EstablishQueryContext(request) = &intent else {
                            unreachable!("startup context operation is its original Establish")
                        };
                        // RecordingSink starts this exact released RPC; the
                        // original intake reports the send before its outcome.
                        acks.publish_establish_send_started(
                            intent.operation_id(),
                            request.context(),
                        );
                        OperationAcknowledgement::transport_unknown(
                            intent.operation_id(),
                            OperationKind::UpdateQueryContext,
                        )
                    };
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.push((intent.clone(), ack.clone()));
                    }
                    acks.publish(ack);
                }
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

pub(crate) fn manifest_normal_release_ack_regression_fixture() -> (
    TaskRoundForTest,
    crate::native::task_transport::TaskAckIntakeHandle,
    OperationIntent,
) {
    let (round, acks, release, _) = manifest_normal_release_ack_with_clock_regression_fixture();
    (round, acks, release)
}

pub(crate) fn manifest_normal_release_ack_with_clock_regression_fixture() -> (
    TaskRoundForTest,
    crate::native::task_transport::TaskAckIntakeHandle,
    OperationIntent,
    Arc<ManualClock>,
) {
    let (round, acks, release, clock, _, _) =
        manifest_normal_release_ack_fixture(Harness::new(&[0], &[0], 64), None);
    (round, acks, release, clock)
}

/// The original graph/operations with their accepted ACK author facts retained
/// for the separately installed QA actor in integration tests.
#[cfg(novarocks_dev_controls)]
pub(crate) fn manifest_normal_release_ack_with_actor_facts_fixture() -> (
    TaskRoundForTest,
    crate::native::task_transport::TaskAckIntakeHandle,
    OperationIntent,
    Arc<ManualClock>,
    Vec<(OperationIntent, OperationAcknowledgement)>,
) {
    let (round, acks, release, clock, _, facts) =
        manifest_normal_release_ack_fixture(Harness::new(&[0], &[0], 64), None);
    (round, acks, release, clock, facts)
}

/// Replays only accepted original fixture facts through QA's public test seam.
/// No operation, ticket, context or payload is regenerated here.
#[cfg(novarocks_dev_controls)]
pub(crate) async fn settle_original_actor_establish_facts(
    logical: &novarocks_query_application::test_support::LogicalExecutionTestHarness,
    facts: Vec<(OperationIntent, OperationAcknowledgement)>,
) {
    let mut admission = BTreeMap::new();
    for (intent, ack) in facts {
        assert_eq!(intent.operation_id(), ack.operation_id());
        match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(request) => {
                let AckPayload::AdmissionTicket(ticket) = ack.payload() else {
                    panic!("original admission ACK has its ticket")
                };
                let settlement =
                    novarocks_query_application::coordination::AdmissionIssueSettlement::applied(
                        ack.operation_id(),
                        ack.worker_outcome().unwrap(),
                        *ticket,
                    )
                    .unwrap();
                assert!(
                    admission
                        .insert(request.context(), (request, settlement))
                        .is_none()
                );
            }
            OperationIntent::EstablishQueryContext(request) => {
                let (original, settlement) = admission
                    .remove(&request.context())
                    .expect("original admission preceded this Establish");
                tokio::time::timeout(
                    Duration::from_secs(10),
                    logical.settle_original_applied_establish(
                        original,
                        settlement,
                        request,
                        ack.operation_id(),
                        ack.worker_outcome().unwrap(),
                    ),
                )
                .await
                .expect("test-only watchdog: replay original admission/Establish facts")
                .unwrap();
            }
            _ => panic!("fixture trace contains an unrelated operation"),
        }
    }
    assert!(
        admission.is_empty(),
        "every original admission has its original Establish"
    );
}

pub(crate) type ManifestFixture<Tail, Protocol> = (
    TaskRoundForTest,
    crate::native::task_transport::TaskAckIntakeHandle,
    Tail,
    Arc<ManualClock>,
    Protocol,
    Vec<(OperationIntent, OperationAcknowledgement)>,
);

/// The original lifecycle fixture with its caller execution and facts installed
/// before the root source is transferred. No production constructor is added.
#[cfg(novarocks_dev_controls)]
pub(crate) fn manifest_normal_release_ack_with_protocol_fixture(
    execution: QueryExecutionId,
    plan: novarocks_query_application::api::PlanSeal,
) -> ManifestFixture<OperationIntent, super::protocol_facts::AttemptProtocolFactsReceiver> {
    let harness = manifest_protocol_harness(execution);
    let (round, acks, release, clock, protocol, facts) =
        manifest_normal_release_ack_fixture(harness, Some(plan));
    (
        round,
        acks,
        release,
        clock,
        protocol.expect("original receiver installed once"),
        facts,
    )
}

#[cfg(novarocks_dev_controls)]
fn manifest_protocol_harness(execution: QueryExecutionId) -> Harness {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let edges = chain_edges();
    let graph = build_protocol_graph(
        execution,
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64),
    )
    .expect("the caller execution builds one original graph");
    Harness::from_graph(graph)
}

/// Same original fixture paused before its upstream terminal facts. The root
/// is Finished; the caller starts convergence only after its real decoded receipt.
/// Remaining tasks are Running, and no actor success state is assigned.
#[cfg(novarocks_dev_controls)]
pub(crate) fn manifest_pending_release_with_protocol_fixture(
    execution: QueryExecutionId,
    plan: novarocks_query_application::api::PlanSeal,
) -> ManifestFixture<ManifestReleaseTail, super::protocol_facts::AttemptProtocolFactsReceiver> {
    let (round, acks, tail, clock, protocol, facts) =
        manifest_pending_release_fixture(manifest_protocol_harness(execution), Some(plan));
    (round, acks, tail, clock, protocol.unwrap(), facts)
}

fn manifest_normal_release_ack_fixture(
    harness: Harness,
    plan: Option<novarocks_query_application::api::PlanSeal>,
) -> ManifestFixture<OperationIntent, Option<super::protocol_facts::AttemptProtocolFactsReceiver>> {
    let (mut round, acks, tail, clock, protocol, facts) =
        manifest_pending_release_fixture(harness, plan);
    tail.begin_original_convergence(
        &mut round,
        Some(&novarocks_query_application::api::NativeAttemptTerminal::Completed),
    );
    let release = tail
        .complete(&mut round, &acks)
        .expect("normal completion releases its original context");
    (round, acks, release, clock, protocol, facts)
}

/// Concrete custody of the existing fixture's delayed upstream terminals and
/// exact Quiesce/Release operations. It never writes actor state or receipts.
pub(crate) struct ManifestReleaseTail {
    status: super::status_intake::StatusIntakeHandle,
    upstream: Vec<TaskIdentity>,
    sink: Arc<RecordingSink>,
    split_delivery: Arc<super::split_transport::SplitDeliveryBridge>,
}
impl ManifestReleaseTail {
    /// Real Task status intake authors the writer proof; no actor state is assigned.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn publish_original_writer_success(&self, round: &mut TaskRoundForTest) {
        assert!(!self.upstream.is_empty());
        for &identity in &self.upstream {
            self.status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    identity,
                    TaskStatusVersion::new(3).unwrap(),
                    TaskState::Finished,
                    None,
                    TaskOutputFacts::new(true),
                )
                .unwrap(),
            )));
        }
        round
            .turn()
            .expect("the original status owner accepts writer success");
    }

    #[cfg(novarocks_dev_controls)]
    fn exact_upstream_fragment_task(
        &self,
        round: &TaskRoundForTest,
        fragment: FragmentId,
    ) -> TaskIdentity {
        let mut tasks = round
            .execution()
            .graph()
            .tasks()
            .filter(|task| task.fragment_id() == fragment)
            .map(|task| task.identity())
            .filter(|identity| self.upstream.contains(identity));
        let task = tasks
            .next()
            .expect("the original fixture has this upstream role");
        assert!(
            tasks.next().is_none(),
            "this fixture declares one Task per role"
        );
        task
    }

    #[cfg(novarocks_dev_controls)]
    fn publish_original_fragment_success(
        &self,
        round: &mut TaskRoundForTest,
        fragment: FragmentId,
    ) {
        let identity = self.exact_upstream_fragment_task(round, fragment);
        self.status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap(),
        )));
        round
            .turn()
            .expect("the original status owner accepts this exact role terminal");
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn publish_original_distinct_writer_success(&self, round: &mut TaskRoundForTest) {
        self.publish_original_fragment_success(round, MIDDLE_FRAGMENT);
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn publish_original_summary_success(&self, round: &mut TaskRoundForTest) {
        self.publish_original_fragment_success(round, LEAF_FRAGMENT);
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn publish_original_summary_failure(
        &self,
        round: &mut TaskRoundForTest,
        failure: novarocks_execution_contract::TaskFailure,
    ) -> novarocks_coordination_contract::SelectedTaskTermination {
        let identity = self.exact_upstream_fragment_task(round, LEAF_FRAGMENT);
        let selected = self.publish_original_failure_for_identity(round, identity, failure);
        assert_eq!(
            selected.identity(),
            identity,
            "the original summary Task authors its failure"
        );
        selected
    }

    /// Invoke the original convergence author after the actual cut or failure.
    pub(crate) fn begin_original_convergence(
        &self,
        round: &mut TaskRoundForTest,
        terminal: Option<&novarocks_query_application::api::NativeAttemptTerminal>,
    ) {
        super::manifest_round::begin_convergence(round, &self.split_delivery, terminal);
        assert!(!round.attempt_drained());
        assert!(
            round
                .execution_mut()
                .enqueue_task_update(
                    self.upstream[0].task_id(),
                    split_update(SCAN_NODE, 1, false)
                )
                .is_err()
        );
    }

    pub(crate) fn publish_original_failure(
        &self,
        round: &mut TaskRoundForTest,
        failure: novarocks_execution_contract::TaskFailure,
    ) -> novarocks_coordination_contract::SelectedTaskTermination {
        self.publish_original_failure_for_identity(round, self.upstream[0], failure)
    }

    fn publish_original_failure_for_identity(
        &self,
        round: &mut TaskRoundForTest,
        identity: TaskIdentity,
        failure: novarocks_execution_contract::TaskFailure,
    ) -> novarocks_coordination_contract::SelectedTaskTermination {
        self.status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Failing,
                Some(TerminationDetail::Failed(failure.clone())),
                TaskOutputFacts::new(false),
            )
            .unwrap(),
        )));
        self.status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(4).unwrap(),
                TaskState::Failed,
                Some(TerminationDetail::Failed(failure)),
                TaskOutputFacts::new(false),
            )
            .unwrap(),
        )));
        round
            .turn()
            .expect("the original status owner accepts the Task failure");
        round
            .execution()
            .selected_task_failure()
            .expect("original selected failure")
            .clone()
    }

    pub(crate) fn complete(
        self,
        round: &mut TaskRoundForTest,
        acks: &crate::native::task_transport::TaskAckIntakeHandle,
    ) -> Option<OperationIntent> {
        self.complete_in(round, acks, None)
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn complete_for_original_control(
        self,
        round: &mut TaskRoundForTest,
        acks: &crate::native::task_transport::TaskAckIntakeHandle,
        scope: &novarocks_workload_control::WorkScope,
        reason: novarocks_workload_control::CancellationReason,
    ) -> Option<OperationIntent> {
        self.complete_in(round, acks, Some((scope, reason)))
    }

    fn complete_in(
        self,
        round: &mut TaskRoundForTest,
        acks: &crate::native::task_transport::TaskAckIntakeHandle,
        original_control: Option<(
            &novarocks_workload_control::WorkScope,
            novarocks_workload_control::CancellationReason,
        )>,
    ) -> Option<OperationIntent> {
        if let Some((scope, reason)) = &original_control {
            assert!(matches!(
                reason,
                novarocks_workload_control::CancellationReason::Requested
                    | novarocks_workload_control::CancellationReason::DeadlineExceeded
            ));
            assert_eq!(scope.cancellation().unwrap().reason(), Some(reason.clone()));
            assert!(round.execution().selected_task_failure().is_none());
        }
        let context = *round
            .execution()
            .graph()
            .contexts()
            .next()
            .expect("one backend context");
        let mut controlled_actor_abort = None;
        for identity in self.upstream {
            if round
                .execution()
                .task(identity.task_id())
                .is_some_and(|task| task.is_terminal())
            {
                continue;
            }
            let canceling = TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(3).expect("v3"),
                TaskState::Canceling,
                Some(TerminationDetail::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                )),
                TaskOutputFacts::new(false),
            )
            .expect("a valid canceling upstream task");
            self.status
                .publish(StatusEvent::Published(Box::new(canceling)));
            let canceled = TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(4).expect("v4"),
                TaskState::Canceled,
                Some(TerminationDetail::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                )),
                TaskOutputFacts::new(false),
            )
            .expect("a valid canceled upstream task");
            self.status
                .publish(StatusEvent::Published(Box::new(canceled)));
        }

        let mut releases = Vec::new();
        for _ in 0..8 {
            round
                .turn()
                .expect("normal convergence keeps the Task owner live");
            for intent in self
                .sink
                .take()
                .into_iter()
                .flat_map(|(_, operations)| operations)
            {
                assert!(
                    !matches!(
                        intent.kind(),
                        OperationKind::CreateTask | OperationKind::UpdateTask
                    ),
                    "success drain cannot issue a new Create or Update"
                );
                if intent.kind() == OperationKind::ReleaseQueryContext {
                    releases.push(intent);
                } else if let OperationIntent::AbortQueryContext(request) = &intent {
                    assert_eq!(request.context(), context);
                    if original_control.is_some() {
                        assert_eq!(
                            round
                                .execution()
                                .actor_abort_dispatch_state(intent.operation_id())
                                .expect("the original Root actor owns this exact Abort"),
                            super::execution::ActorAbortDispatchState::InFlight
                        );
                        assert!(
                            controlled_actor_abort
                                .replace(intent.operation_id())
                                .is_none()
                        );
                    } else {
                        assert!(
                            round.execution().selected_task_failure().is_some(),
                            "normal successful computation must release without Abort"
                        );
                    }
                    // Settle this exact dispatched operation through the real
                    // acknowledgement intake. This is not Worker-exit proof.
                    acks.publish(OperationAcknowledgement::worker_receipt(
                        intent.operation_id(),
                        OperationKind::AbortQueryContext,
                        OperationOutcome::ContextTerminalReceipt,
                        AckPayload::Context(QueryContextReceipt::new(
                            request.context(),
                            QueryContextState::TerminalRetained,
                        )),
                    ));
                } else if intent.kind() == OperationKind::QuiesceQueryContext {
                    let context = *round
                        .execution()
                        .graph()
                        .contexts()
                        .next()
                        .expect("one backend context");
                    let mut accepted = round
                        .execution()
                        .graph()
                        .tasks()
                        .map(|task| task.identity())
                        .collect::<Vec<_>>();
                    accepted.sort();
                    acks.publish(OperationAcknowledgement::new(
                        intent.operation_id(),
                        OperationKind::QuiesceQueryContext,
                        OperationOutcome::Accepted,
                        AckPayload::Quiesce(QuiesceQueryContextReceipt::new(
                            context,
                            1,
                            accepted,
                            QueryContextState::Quiescing,
                        )),
                    ));
                }
            }
            if !releases.is_empty() {
                break;
            }
        }
        let owner = round.execution().owner(context).expect("the context owner");
        assert!(
            owner.creates_closed(),
            "all creates settled before the seal"
        );
        assert!(
            owner.locally_drained(),
            "all local tasks and outputs must drain: {owner:?}"
        );
        if let Some((scope, reason)) = original_control {
            assert_eq!(scope.cancellation().unwrap().reason(), Some(reason));
            assert!(
                controlled_actor_abort.is_some(),
                "the original Root must dispatch its Abort"
            );
            assert!(
                owner.is_released(),
                "the exact actor Abort receipt must settle the context"
            );
        }
        if owner.is_released() {
            assert!(
                round.execution().selected_task_failure().is_some()
                    || controlled_actor_abort.is_some(),
                "only original Task failure or proven Root actor control settles Abort instead of Release"
            );
            assert!(releases.is_empty());
            assert!(round.attempt_drained());
            return None;
        }
        assert_eq!(
            releases.len(),
            1,
            "the drained context must be released: owner={owner:?}, queued_actor_aborts={}",
            round.queued_abort_effects()
        );
        releases.pop()
    }
}

#[cfg(novarocks_dev_controls)]
pub(crate) fn manifest_pending_write_with_protocol_fixture(
    execution: QueryExecutionId,
    plan: novarocks_query_application::api::PlanSeal,
) -> ManifestFixture<ManifestReleaseTail, super::protocol_facts::AttemptProtocolFactsReceiver> {
    let (round, acks, tail, clock, protocol, facts) = manifest_pending_release_fixture_with_writers(
        manifest_protocol_harness(execution),
        Some(plan),
        true,
        false,
    );
    (round, acks, tail, clock, protocol.unwrap(), facts)
}

/// The original three-stage Task graph with disjoint roles: its leaf is
/// the summary source, the middle Task is the writer, and the root is finish.
/// This is a completion-owner fixture, not a Router runtime capability claim.
#[cfg(novarocks_dev_controls)]
pub(crate) fn manifest_pending_write_with_summary_source_protocol_fixture(
    execution: QueryExecutionId,
    plan: novarocks_query_application::api::PlanSeal,
) -> ManifestFixture<ManifestReleaseTail, super::protocol_facts::AttemptProtocolFactsReceiver> {
    let (round, acks, tail, clock, protocol, facts) = manifest_pending_release_fixture_with_writers(
        manifest_protocol_harness(execution),
        Some(plan),
        true,
        true,
    );
    (round, acks, tail, clock, protocol.unwrap(), facts)
}

fn manifest_pending_release_fixture(
    harness: Harness,
    plan: Option<novarocks_query_application::api::PlanSeal>,
) -> ManifestFixture<ManifestReleaseTail, Option<super::protocol_facts::AttemptProtocolFactsReceiver>>
{
    manifest_pending_release_fixture_with_writers(harness, plan, false, false)
}

fn manifest_pending_release_fixture_with_writers(
    harness: Harness,
    plan: Option<novarocks_query_application::api::PlanSeal>,
    declare_upstream_writers: bool,
    declare_independent_summary_source: bool,
) -> ManifestFixture<ManifestReleaseTail, Option<super::protocol_facts::AttemptProtocolFactsReceiver>>
{
    let sink = Arc::clone(&harness.sink);
    let clock = Arc::clone(&harness.clock);
    let mut facts = Vec::new();
    let (mut round, status, acks, creates) =
        round_waiting_on_creates_with_establish_trace(harness, Some(&mut facts));
    let split_delivery =
        super::split_transport::SplitDeliveryBridge::for_graph(round.execution().graph());
    // Freeze a nonempty declaration from the original graph before source transfer.
    let writers = if declare_upstream_writers {
        round
            .execution()
            .graph()
            .tasks()
            .filter(|task| task.identity() != round.root_task())
            .filter(|task| {
                !declare_independent_summary_source || task.fragment_id() == MIDDLE_FRAGMENT
            })
            .map(|task| task.identity())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    if declare_upstream_writers {
        assert!(!writers.is_empty());
    }
    let summary_sources = if declare_independent_summary_source {
        assert!(declare_upstream_writers);
        let sources = round
            .execution()
            .graph()
            .tasks()
            .filter(|task| task.fragment_id() == LEAF_FRAGMENT)
            .map(|task| task.identity())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            sources.len(),
            1,
            "this exact fixture has one independent summary source"
        );
        assert_eq!(writers.len(), 1, "this exact fixture has one writer");
        assert!(sources.is_disjoint(&writers));
        sources
    } else {
        BTreeSet::new()
    };
    let protocol = plan.map(|plan| {
        round
            .install_protocol_facts(plan, &writers, &summary_sources)
            .expect("original protocol before root transfer")
    });
    let _root_source = round
        .take_root_status_source()
        .expect("the result owner takes the root projection");
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().expect("the exact creates settle");
    for intent in sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
    {
        if let OperationIntent::EstablishQueryContext(_) = intent {
            let ack = establish_ack(&intent);
            facts.push((intent.clone(), ack.clone()));
            acks.publish(ack);
        }
    }
    round.turn().expect("the context establish settles");
    sink.take();

    let root = round.root_task();
    let upstream = round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .filter(|identity| *identity != root)
        .collect::<Vec<_>>();
    for identity in std::iter::once(root).chain(upstream.iter().copied()) {
        let running = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(2).expect("v2"),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .expect("a valid running task");
        status.publish(StatusEvent::Published(Box::new(running)));
    }
    round.turn().expect("every task is running");
    let finished = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .expect("a valid finished root");
    status.publish(StatusEvent::Published(Box::new(finished)));
    round.turn().expect("the root finish is observed");
    sink.take();
    let tail = ManifestReleaseTail {
        status,
        upstream,
        sink,
        split_delivery,
    };
    (round, acks, tail, clock, protocol, facts)
}

#[test]
fn completed_convergence_releases_after_delayed_upstream_terminal_status() {
    let (mut round, acks, release) = manifest_normal_release_ack_regression_fixture();
    let context = *round.execution().graph().contexts().next().unwrap();
    let OperationIntent::ReleaseQueryContext(request) = &release else {
        unreachable!("the operation was selected by kind");
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
            runtime_filter: None,
        },
    ));
    round.turn().expect("the release acknowledgement settles");
    assert!(
        round.attempt_drained(),
        "the logical lifecycle operations drained"
    );
    assert!(
        round
            .execution()
            .context_convergence_receipt(context)
            .is_none()
    );
    assert!(
        !round.execution().worker_contexts_stopped(),
        "Task terminals, output exit and Release ACK do not prove actual Worker exit"
    );
    let receipt = novarocks_execution_contract::QueryContextConvergenceReceipt::new(
        context,
        novarocks_execution_contract::QueryContextConvergenceVersion::FIRST,
        novarocks_execution_contract::QueryContextConvergenceState::WorkerStoppedAndContextFenced,
    );
    round
        .execution_mut()
        .apply_covered_entries(vec![covered_frame(
            context,
            1,
            CoveredStatusStreamFact::ContextConvergence(receipt),
        )])
        .unwrap();
    assert!(round.execution().worker_contexts_stopped());
}

#[test]
fn result_pump_gate_accepts_installed_terminal_root_before_create_ack() {
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
        status.publish(StatusEvent::Published(Box::new(finished))),
        StatusIntakeAdmission::Enqueued
    );
    round
        .turn()
        .expect("a terminal root status may race ahead of create ACK");
    assert!(round.result_pump_ready());

    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round
        .turn()
        .expect("the late exact create ACK settles without regressing the root");
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
fn installed_status_proves_root_ownership_before_lost_create_ack() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    let harness = Harness::from_graph(graph);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let _projection = round
        .take_root_status_source()
        .expect("the result owner takes the root projection");
    let root = round.root_task();
    let root_create = creates
        .iter()
        .find(|intent| {
            matches!(intent, OperationIntent::CreateTask(request) if request.identity() == root)
        })
        .expect("the root create was sent");
    let installed = TaskStatus::try_new_with_installed(
        root,
        TaskStatusVersion::new(2).expect("v2"),
        TaskState::Planned,
        None,
        TaskOutputFacts::new(false),
        true,
    )
    .expect("the Worker installed the root");
    assert_eq!(
        status.publish(StatusEvent::Published(Box::new(installed))),
        StatusIntakeAdmission::Enqueued
    );
    round
        .turn()
        .expect("Installed crosses the observation stream");
    assert!(round.result_pump_ready());
    assert!(
        round
            .execution()
            .task(root.task_id())
            .expect("the root remains owned")
            .create_ownership_proven()
    );

    acks.publish(OperationAcknowledgement::new(
        root_create.operation_id(),
        OperationKind::CreateTask,
        OperationOutcome::NotReady,
        AckPayload::None,
    ));
    round
        .turn()
        .expect("the late negative response cannot revoke Installed");
    assert!(round.result_pump_ready());
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
    assert!(
        !round.result_pump_ready(),
        "Accepted alone does not install the root"
    );
    let root = round.root_task();
    assert_eq!(
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new_with_installed(
                root,
                TaskStatusVersion::new(2).expect("v2"),
                TaskState::Planned,
                None,
                TaskOutputFacts::new(false),
                true,
            )
            .expect("an installed root status"),
        ))),
        StatusIntakeAdmission::Enqueued
    );
    round.turn().expect("the root installation is observed");
    assert!(round.result_pump_ready());

    let root_derived = TaskStatus::try_new(
        root,
        TaskStatusVersion::new(4).expect("v4"),
        TaskState::Aborted,
        Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
        TaskOutputFacts::new(false),
    )
    .expect("a valid derived terminal");
    assert_eq!(
        status.publish(StatusEvent::Published(Box::new(root_derived))),
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
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
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
        status.publish(StatusEvent::Published(Box::new(origin_failed))),
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
    status.publish(StatusEvent::Published(Box::new(finished)));
    let derived = TaskStatus::try_new(
        derived_origin,
        TaskStatusVersion::new(3).expect("v3"),
        TaskState::Aborted,
        Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
        TaskOutputFacts::new(false),
    )
    .expect("a valid derived upstream terminal");
    status.publish(StatusEvent::Published(Box::new(derived)));
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
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
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
    status.publish(StatusEvent::Published(Box::new(origin_failed)));
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

// A real pending close future retains its capture until the original job
// completes. The test controls completion, never the owner's exit predicate.
struct PumpCloseCapture(Arc<AtomicUsize>);

impl Drop for PumpCloseCapture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

type PumpCloseJob =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TaskExecutionError>> + Send>>;

struct GatedPumpClose {
    name: &'static str,
    closing: bool,
    job: Option<PumpCloseJob>,
    close_turns: Arc<AtomicUsize>,
}

struct PumpCloseControl {
    completion: Option<tokio::sync::oneshot::Sender<Result<(), TaskExecutionError>>>,
    close_turns: Arc<AtomicUsize>,
    capture_drops: Arc<AtomicUsize>,
}

impl PumpCloseControl {
    fn complete(&mut self, outcome: Result<(), TaskExecutionError>) {
        self.completion
            .take()
            .expect("the original close completes once")
            .send(outcome)
            .expect("the original close owner remains present");
    }
}

impl crate::task_execution::round::TurnPump for GatedPumpClose {
    fn name(&self) -> &'static str {
        self.name
    }

    fn drive(&mut self, _: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
        panic!("input production resumed after begin_input_close");
    }

    fn begin_close(&mut self) {
        self.closing = true;
    }

    fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
        assert!(self.closing, "the original close must have been requested");
        self.close_turns.fetch_add(1, Ordering::SeqCst);
        let Some(job) = self.job.as_mut() else {
            return Ok(0);
        };
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        match job.as_mut().poll(&mut context) {
            std::task::Poll::Pending => Ok(0),
            std::task::Poll::Ready(outcome) => {
                self.job = None;
                outcome.map(|()| 1)
            }
        }
    }

    fn is_closed(&self) -> bool {
        self.job.is_none()
    }

    fn take_close_unresolved(
        &mut self,
    ) -> Option<crate::task_execution::round::PumpCloseUnresolved> {
        None
    }
}

fn gated_pump_close(name: &'static str) -> (GatedPumpClose, PumpCloseControl) {
    let (completion, completed) = tokio::sync::oneshot::channel();
    let close_turns = Arc::new(AtomicUsize::new(0));
    let capture_drops = Arc::new(AtomicUsize::new(0));
    let capture = PumpCloseCapture(Arc::clone(&capture_drops));
    let job = Box::pin(async move {
        let outcome = completed.await.expect("original close completion");
        drop(capture);
        outcome
    });
    (
        GatedPumpClose {
            name,
            closing: false,
            job: Some(job),
            close_turns: Arc::clone(&close_turns),
        },
        PumpCloseControl {
            completion: Some(completion),
            close_turns,
            capture_drops,
        },
    )
}

#[test]
fn input_close_advances_all_original_jobs_while_ack_and_status_remain_live() {
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let (failed, mut failed_control) = gated_pump_close("original-close-error");
    let (pending, mut pending_control) = gated_pump_close("original-close-pending");
    round.add_pump(Box::new(failed));
    round.add_pump(Box::new(pending));
    round.begin_input_close();

    let original_error = TaskExecutionError::Schedule("original close domain failure".to_owned());
    failed_control.complete(Err(original_error.clone()));
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            round.root_task(),
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap(),
    )));
    // No normal drain, terminal cleanup, or attempt failure starts this close.
    let report = round
        .turn()
        .expect("local close cannot block protocol progress");
    assert_eq!(report.acknowledgements, creates.len());
    assert_eq!(report.status_events, 1);
    assert!(round.failure_cause().is_none());
    assert_eq!(failed_control.close_turns.load(Ordering::SeqCst), 1);
    assert_eq!(pending_control.close_turns.load(Ordering::SeqCst), 1);
    assert_eq!(failed_control.capture_drops.load(Ordering::SeqCst), 1);
    assert_eq!(pending_control.capture_drops.load(Ordering::SeqCst), 0);
    assert_eq!(round.take_pump_close_error(), Some(original_error));
    assert!(round.take_pump_close_error().is_none());
    assert!(
        !round.pumps_closed(),
        "a close error cannot close the pending peer"
    );
    assert!(!round.pump_close_progress_settled());

    round.receive_without_dispatch().unwrap();
    assert_eq!(pending_control.close_turns.load(Ordering::SeqCst), 2);
    assert_eq!(pending_control.capture_drops.load(Ordering::SeqCst), 0);
    assert!(round.take_pump_close_error().is_none());
    assert!(
        !round.pumps_closed(),
        "another turn is not actual exit evidence"
    );

    pending_control.complete(Ok(()));
    let report = round.receive_without_dispatch().unwrap();
    assert_eq!(report.pumped, 1);
    assert_eq!(pending_control.close_turns.load(Ordering::SeqCst), 3);
    assert_eq!(pending_control.capture_drops.load(Ordering::SeqCst), 1);
    assert!(round.pumps_closed());
    assert!(round.pump_close_progress_settled());
    assert!(round.take_pump_close_error().is_none());
    assert!(round.failure_cause().is_none());
}

#[test]
fn input_close_progress_precedes_an_unrelated_ack_error() {
    let mut round = pre_ready_invalid_ack_observation_round();
    let (failed, mut failed_control) = gated_pump_close("close-before-ack-error");
    let (peer, mut peer_control) = gated_pump_close("peer-close-before-ack-error");
    round.add_pump(Box::new(failed));
    round.add_pump(Box::new(peer));
    round.begin_input_close();
    let original_error = TaskExecutionError::Schedule("retained original close failure".to_owned());
    failed_control.complete(Err(original_error.clone()));
    peer_control.complete(Ok(()));

    assert!(matches!(
        round.turn(),
        Err(TaskExecutionError::UnknownOperation)
    ));
    assert_eq!(failed_control.close_turns.load(Ordering::SeqCst), 1);
    assert_eq!(peer_control.close_turns.load(Ordering::SeqCst), 1);
    assert_eq!(failed_control.capture_drops.load(Ordering::SeqCst), 1);
    assert_eq!(peer_control.capture_drops.load(Ordering::SeqCst), 1);
    assert!(
        round.pumps_closed(),
        "both original futures actually returned"
    );
    assert_eq!(round.take_pump_close_error(), Some(original_error));
    assert!(round.take_pump_close_error().is_none());
    round.receive_without_dispatch().unwrap();
    assert!(round.take_pump_close_error().is_none());
}

#[test]
fn closing_the_feedback_pump_releases_initial_wait_and_fences_late_feedback() {
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::feedback_pump::DynamicFilterFeedbackPump;
    use crate::task_execution::round::TurnPump;

    let mut harness = Harness::new(&[0], &[0], 64);
    let task = TaskId::new(1).unwrap();
    let publisher = harness.identity(task).backend_process_id();
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), feedback_declaration(publisher))
            .expect("feedback state"),
    );
    let reads = Arc::new(RecordingFilterReads::default());
    let mut pump = DynamicFilterFeedbackPump::new(
        Arc::clone(&feedback),
        Arc::clone(&reads) as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
        1,
    )
    .unwrap();
    harness.publish_with_filters(
        task,
        TaskState::Running,
        None,
        false,
        Some(DynamicFilterAdvertisement::new(
            DomainVersion::new(1).unwrap(),
            1,
        )),
    );
    assert!(feedback.is_initial_wait_blocked(SCAN_NODE, [7]));
    // Split assignment now observes the original snapshot each turn; there
    // is no feedback Condvar waiter. Closing the same pump must immediately
    // release that predicate and fence every later original contribution.
    pump.begin_close();
    assert!(pump.is_closed());
    assert!(!feedback.is_initial_wait_blocked(SCAN_NODE, [7]));
    pump.begin_close();
    pump.close_turn().unwrap();
    assert!(pump.is_closed());
    assert_eq!(pump.drive(&mut harness.execution).unwrap(), 0);
    assert!(
        reads.requested().is_empty(),
        "closing cannot start a late fetch"
    );
    feedback
        .admit_task_feedback(&feedback_envelope(7, [9; 32], exact_domain(41)), publisher)
        .expect("closed feedback ignores a late update");
    assert!(!feedback.is_initial_wait_blocked(SCAN_NODE, [7]));
    assert!(feedback.configure(feedback_declaration(publisher)).is_err());
}

#[test]
fn an_attempt_with_no_channel_and_no_vended_credential_installs_no_owner() {
    use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
    use crate::task_execution::attempt_pumps::{AttemptPumps, install_attempt_pumps};

    // The other half of the supply rule: "declared zero" must stay reachable,
    // or every query without runtime filters would fail its first turn.
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, _wake) = round_of(harness);
    let feedback = Arc::new(
        RuntimeFilterFeedbackState::new(execution_id(), Default::default()).expect("state"),
    );
    install_attempt_pumps(
        &mut round,
        AttemptPumps {
            feedback_state: feedback,
            declared_feedback_channels: 0,
            reads: Arc::new(RecordingFilterReads::default())
                as Arc<dyn crate::task_execution::feedback_pump::TaskDynamicFilterReads>,
        },
    );

    assert_eq!(round.installed_pumps(), 0);
    round.turn().expect("an attempt with no owners still turns");
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

#[test]
fn root_success_seal_stops_ordinary_pumps_in_its_own_turn() {
    struct RefusedPump;
    impl crate::task_execution::round::TurnPump for RefusedPump {
        fn name(&self) -> &'static str {
            "ordinary-input-after-seal"
        }
        fn begin_close(&mut self) {}
        fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
            Ok(0)
        }
        fn is_closed(&self) -> bool {
            true
        }
        fn take_close_unresolved(
            &mut self,
        ) -> Option<crate::task_execution::round::PumpCloseUnresolved> {
            None
        }
        fn drive(&mut self, _: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
            panic!("ordinary input pump ran after the accepted root success cut");
        }
    }
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let source = round.take_root_status_source().unwrap();
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().unwrap();
    let root = round.root_task();
    for (version, state, output) in [
        (2, TaskState::Running, false),
        (3, TaskState::Flushing, false),
        (4, TaskState::Finished, true),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(output),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    round.add_pump(Box::new(RefusedPump));
    let mut reply = source.begin_success_seal_request().unwrap();
    round
        .turn()
        .expect("the seal cut freezes ordinary pumps before the rest of its turn");
    assert_eq!(reply.try_recv().unwrap(), Ok(()));
    assert!(round.root_success_sealed());
    for &context in round.execution().graph().contexts() {
        assert_eq!(
            round.classify_covered_subscription_state(
                context,
                crate::native::task_transport::SubscriptionState::BudgetExhausted
            ),
            None,
            "an accepted root seal cannot be revoked by residual observation exhaustion"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                context,
                crate::native::task_transport::SubscriptionState::ConnectionLossExhausted
            ),
            None,
            "an accepted root seal cannot be revoked by residual observation exhaustion"
        );
    }
    assert!(
        round
            .execution_mut()
            .enqueue_task_update(root.task_id(), split_update(SCAN_NODE, 1, false))
            .is_err()
    );
}

#[test]
fn covered_exhaustion_fails_only_still_required_work_without_timing_healthy_tasks() {
    use crate::native::task_transport::SubscriptionState;

    let harness = Harness::new(&[1], &[0], 64);
    let leaf = harness.stage_tasks(1)[0];
    let leaf_context = harness.execution.task(leaf).unwrap().context();
    let clock = Arc::clone(&harness.clock);
    let (mut round, _, acks, creates) = round_waiting_on_creates(harness);
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    clock.advance(Duration::from_secs(600));
    for state in [
        SubscriptionState::Opening,
        SubscriptionState::Live,
        SubscriptionState::Resubscribing,
    ] {
        assert_eq!(
            round.classify_covered_subscription_state(leaf_context, state),
            None
        );
    }
    assert!(!round.accepted_root_success_sealed());
    assert_eq!(
        round.classify_covered_subscription_state(leaf_context, SubscriptionState::BudgetExhausted),
        Some(SubscriptionState::BudgetExhausted),
        "a still-required leaf losing bounded transport recovery must reach the original failure owner"
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            leaf_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        Some(SubscriptionState::ConnectionLossExhausted),
        "a still-required leaf losing bounded transport recovery must reach the original failure owner"
    );
    for state in [
        SubscriptionState::Rejected,
        SubscriptionState::ProcessMismatch,
        SubscriptionState::QueryMismatch,
    ] {
        assert_eq!(
            round.classify_covered_subscription_state(leaf_context, state),
            Some(state)
        );
    }
}

#[test]
fn covered_exhaustion_of_a_stood_down_unknown_leaf_belongs_to_drain() {
    use crate::native::task_transport::SubscriptionState;

    let harness = Harness::new(&[1], &[0], 64);
    let leaf = harness.stage_tasks(1)[0];
    let leaf_identity = harness.execution.task(leaf).unwrap().identity();
    let leaf_context = harness.execution.task(leaf).unwrap().context();
    let (mut round, _, _, _) = round_waiting_on_creates(harness);
    assert!(
        !round
            .execution()
            .task(leaf)
            .unwrap()
            .create_ownership_proven()
    );
    assert_eq!(
        round.classify_covered_subscription_state(leaf_context, SubscriptionState::BudgetExhausted),
        Some(SubscriptionState::BudgetExhausted)
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            leaf_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        Some(SubscriptionState::ConnectionLossExhausted)
    );
    round
        .execution_mut()
        .stand_down_task_normally(leaf_identity)
        .unwrap();
    assert!(
        !round.execution().task(leaf).unwrap().is_terminal(),
        "stand-down is not synthetic Worker-stop evidence"
    );
    assert_eq!(
        round.classify_covered_subscription_state(leaf_context, SubscriptionState::BudgetExhausted),
        None
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            leaf_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        None
    );
    let root_context = round
        .execution()
        .task(round.root_task().task_id())
        .unwrap()
        .context();
    assert_eq!(
        round.classify_covered_subscription_state(root_context, SubscriptionState::BudgetExhausted),
        Some(SubscriptionState::BudgetExhausted)
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            root_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        Some(SubscriptionState::ConnectionLossExhausted)
    );
    round.begin_terminal_cleanup();
    assert_eq!(
        round.classify_covered_subscription_state(root_context, SubscriptionState::BudgetExhausted),
        None
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            root_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        None
    );
}

#[test]
fn covered_exhaustion_respects_exact_root_evidence_before_seal() {
    use crate::native::task_transport::SubscriptionState;
    let harness = Harness::new(&[1], &[0], 64);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    let context = round.execution().task(root.task_id()).unwrap().context();
    for (version, state) in [(2, TaskState::Running), (3, TaskState::Flushing)] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(false),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    for state in [
        SubscriptionState::BudgetExhausted,
        SubscriptionState::ConnectionLossExhausted,
    ] {
        assert_eq!(
            round.classify_covered_subscription_state(context, state),
            Some(state),
            "the exact Root terminal evidence is still missing"
        );
    }
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(4).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap(),
    )));
    round.turn().unwrap();
    for state in [
        SubscriptionState::BudgetExhausted,
        SubscriptionState::ConnectionLossExhausted,
    ] {
        assert_eq!(
            round.classify_covered_subscription_state(context, state),
            None,
            "accepted complete Root evidence survives disconnect; data delivery has its own QA owner"
        );
    }
    assert!(
        !round.accepted_root_success_sealed(),
        "Root status alone does not accept a success seal"
    );
}

#[test]
fn covered_exhaustion_after_result_root_finished_does_not_wait_for_deep_leaf_or_seal_intake() {
    use crate::native::task_transport::SubscriptionState;

    for result_consumer in [false, true] {
        let harness = Harness::new(&[1], &[0], 64);
        let leaf = harness.stage_tasks(1)[0];
        let leaf_context = harness.execution.task(leaf).unwrap().context();
        let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
        let source = result_consumer.then(|| round.take_root_status_source().unwrap());
        if result_consumer {
            assert!(round.take_root_status_source().is_none());
        }
        for create in &creates {
            acks.publish(accepted_create_ack(create));
        }
        round.turn().unwrap();
        let root = round.root_task();
        let root_context = round.execution().task(root.task_id()).unwrap().context();
        assert_eq!(
            round.classify_covered_subscription_state(
                root_context,
                SubscriptionState::BudgetExhausted
            ),
            Some(SubscriptionState::BudgetExhausted),
            "an attached consumer cannot substitute for the root's terminal output fact"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                root_context,
                SubscriptionState::ConnectionLossExhausted
            ),
            Some(SubscriptionState::ConnectionLossExhausted),
            "an attached consumer cannot substitute for the root's terminal output fact"
        );
        for (version, state, output) in [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ] {
            status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    root,
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    None,
                    TaskOutputFacts::new(output),
                )
                .unwrap(),
            )));
        }
        round.turn().unwrap();
        assert_eq!(
            round.execution().task(root.task_id()).unwrap().task_state(),
            TaskState::Finished,
        );
        assert!(!round.root_success_sealed());
        let leaf_task = round.execution().task(leaf).unwrap();
        assert!(!leaf_task.is_terminal());
        assert!(
            !leaf_task.normally_stood_down(),
            "one-layer stage release has not withdrawn this deep leaf"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                leaf_context,
                SubscriptionState::BudgetExhausted
            ),
            (!result_consumer).then_some(SubscriptionState::BudgetExhausted),
            "only an attached Result consumer retires residual transport evidence before seal intake"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                leaf_context,
                SubscriptionState::ConnectionLossExhausted
            ),
            (!result_consumer).then_some(SubscriptionState::ConnectionLossExhausted),
            "only an attached Result consumer retires residual transport evidence before seal intake"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                leaf_context,
                SubscriptionState::ProcessMismatch
            ),
            Some(SubscriptionState::ProcessMismatch),
            "root completion cannot excuse an exact identity violation"
        );
        if let Some(source) = source {
            let mut reply = source.begin_success_seal_request().unwrap();
            round.turn().unwrap();
            assert_eq!(reply.try_recv().unwrap(), Ok(()));
            assert!(round.root_success_sealed());
            assert_eq!(
                round.classify_covered_subscription_state(
                    leaf_context,
                    SubscriptionState::ConnectionLossExhausted
                ),
                None,
                "an accepted success seal cannot be revoked by residual transport loss"
            );
        }
    }
}

#[test]
fn covered_exhaustion_result_exception_preserves_accepted_failure_before_seal() {
    use crate::native::task_transport::SubscriptionState;

    let harness = Harness::new(&[1], &[0], 64);
    let leaf = harness.stage_tasks(1)[0];
    let leaf_context = harness.execution.task(leaf).unwrap().context();
    let leaf_identity = harness.execution.task(leaf).unwrap().identity();
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let source = round.take_root_status_source().unwrap();
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap(),
    )));
    round.turn().unwrap();
    assert_eq!(
        round.classify_covered_subscription_state(leaf_context, SubscriptionState::BudgetExhausted),
        None
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            leaf_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        None
    );

    let failure = TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Data,
            novarocks_execution_contract::failure::FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::new(1).unwrap(),
            ),
        ),
        novarocks_execution::task_execution::SafeDetail::new("accepted deep-leaf failure").unwrap(),
    ));
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            leaf_identity,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Failing,
            Some(failure.clone()),
            TaskOutputFacts::new(false),
        )
        .unwrap(),
    )));
    round.turn().unwrap();
    assert_eq!(round.failure_cause(), Some(&failure));
    assert_eq!(
        round.classify_covered_subscription_state(leaf_context, SubscriptionState::BudgetExhausted),
        Some(SubscriptionState::BudgetExhausted),
        "an accepted failure cannot be hidden by the completed root"
    );
    assert_eq!(
        round.classify_covered_subscription_state(
            leaf_context,
            SubscriptionState::ConnectionLossExhausted
        ),
        Some(SubscriptionState::ConnectionLossExhausted),
        "an accepted failure cannot be hidden by the completed root"
    );
    let mut reply = source.begin_success_seal_request().unwrap();
    round.turn().unwrap();
    let error = reply.try_recv().unwrap().unwrap_err();
    assert!(error.message().contains("empty attempt failure latch"));
    assert!(!round.root_success_sealed());
}

#[test]
fn pending_success_seal_recovers_at_a_new_cut_or_rejects_at_the_original_deadline() {
    for recover in [true, false] {
        let harness = Harness::new(&[0], &[1], 64);
        let clock = Arc::clone(&harness.clock);
        let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
        round.execution_mut().activate_covered_observation();
        let source = round.take_root_status_source().unwrap();
        for intent in &creates {
            acks.publish(accepted_create_ack(intent));
        }
        round.turn().unwrap();
        let root = round.root_task();
        let missing = round
            .execution()
            .graph()
            .tasks()
            .find(|task| task.identity() != root)
            .unwrap()
            .identity();
        let context = round
            .execution()
            .graph()
            .task(missing.task_id())
            .unwrap()
            .context();
        for (version, state, output) in [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ] {
            status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    root,
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    None,
                    TaskOutputFacts::new(output),
                )
                .unwrap(),
            )));
        }
        round.turn().unwrap();
        round
            .execution_mut()
            .apply_covered_entries(vec![
                covered_frame(context, 1, CoveredStatusStreamFact::Gone(missing)),
                covered_frame(
                    context,
                    1,
                    CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                        generation: 1,
                        initial_cut: 1,
                    }),
                ),
            ])
            .unwrap();
        assert_eq!(
            round.classify_covered_subscription_state(
                context,
                crate::native::task_transport::SubscriptionState::BudgetExhausted
            ),
            None,
            "retired transport evidence does not clear a real application gap"
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                context,
                crate::native::task_transport::SubscriptionState::ConnectionLossExhausted
            ),
            None,
            "retired transport evidence does not clear a real application gap"
        );
        let mut reply = source.begin_success_seal_request().unwrap();
        round.turn().unwrap();
        assert!(matches!(
            reply.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(!round.root_success_sealed());
        clock.advance(Duration::from_secs(29));
        round
            .execution_mut()
            .apply_covered_entries(vec![
                covered_frame(context, 2, CoveredStatusStreamFact::Unknown(missing)),
                covered_frame(
                    context,
                    2,
                    CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                        generation: 2,
                        initial_cut: 2,
                    }),
                ),
                covered_frame(
                    context,
                    2,
                    CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                        generation: 2,
                        sequence: 1,
                        covered_prefix: 2,
                        source_cut: 2,
                    }),
                ),
            ])
            .unwrap();
        round.turn().unwrap();
        assert!(
            matches!(
                reply.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "Unknown and bookmarks cannot recover the missing terminal"
        );
        if recover {
            let facts = [TaskState::Canceling, TaskState::Canceled]
                .into_iter()
                .enumerate()
                .map(|(index, state)| {
                    covered_frame(
                        context,
                        3,
                        CoveredStatusStreamFact::Status(
                            TaskStatus::try_new(
                                missing,
                                TaskStatusVersion::new(index as u64 + 2).unwrap(),
                                state,
                                Some(TerminationDetail::Canceled(
                                    CancelReason::UpstreamNoLongerNeeded,
                                )),
                                TaskOutputFacts::new(false),
                            )
                            .unwrap(),
                        ),
                    )
                });
            let mut entries = facts.collect::<Vec<_>>();
            entries.push(covered_frame(
                context,
                3,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 3,
                    initial_cut: 3,
                }),
            ));
            round
                .execution_mut()
                .apply_covered_entries(entries)
                .unwrap();
            round.turn().unwrap();
            assert_eq!(reply.try_recv().unwrap(), Ok(()));
            assert!(
                round.root_success_sealed(),
                "positive catch-up permits a new local cut without waiting for unrelated actual-stopped facts"
            );
        } else {
            clock.advance(Duration::from_secs(1));
            round.turn().unwrap();
            let error = reply.try_recv().unwrap().unwrap_err();
            assert!(
                error
                    .message()
                    .contains("could not be recovered within its budget")
            );
            assert!(!round.root_success_sealed());
        }
    }
}

/// The original round factory before its first Task/lifecycle turn.
pub(crate) fn manifest_unstarted_regression_fixture() -> TaskRoundForTest {
    let (round, _) = round_of(Harness::new(&[0], &[0], 64));
    round
}

pub(crate) fn manifest_terminal_regression_fixture() -> (
    TaskRoundForTest,
    crate::native::task_transport::TaskAckIntakeHandle,
) {
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().unwrap();
    let root = round.root_task();
    for (version, state, output) in [
        (2, TaskState::Running, false),
        (3, TaskState::Flushing, false),
        (4, TaskState::Finished, true),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                root,
                TaskStatusVersion::new(version).unwrap(),
                state,
                None,
                TaskOutputFacts::new(output),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    (round, acks)
}

#[test]
fn covered_terminal_gap_inherits_existing_debt_and_positive_evidence_discharges_it() {
    let mut harness = Harness::new(&[0], &[1], 512);
    harness.settle_until_quiet(Duration::from_secs(10));
    let root = harness.execution.graph().root_task();
    let identity = harness.identity(root);
    let context = harness.execution.graph().task(root).unwrap().context();
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(
                context,
                1,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 1,
                    initial_cut: 0,
                }),
            ),
            covered_frame(
                context,
                1,
                CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 1,
                    sequence: 1,
                    covered_prefix: 0,
                    source_cut: 10,
                }),
            ),
        ])
        .unwrap();
    harness.clock.advance(Duration::from_secs(29));
    assert!(
        !harness.execution.covered_recovery_expired(),
        "debt without an actual gap is not a refusal"
    );
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(context, 2, CoveredStatusStreamFact::Gone(identity)),
            covered_frame(
                context,
                2,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 2,
                    initial_cut: 0,
                }),
            ),
            covered_frame(
                context,
                2,
                CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                    generation: 2,
                    sequence: 1,
                    covered_prefix: 0,
                    source_cut: 20,
                }),
            ),
        ])
        .unwrap();
    assert!(!harness.execution.covered_recovery_expired());
    harness.clock.advance(Duration::from_secs(1));
    assert!(
        harness.execution.covered_recovery_expired(),
        "a new generation cannot renew the original required gap debt"
    );
    harness.publish(root, TaskState::Running, None, false);
    harness.publish(root, TaskState::Flushing, None, false);
    harness.publish(root, TaskState::Finished, None, true);
    let terminal = harness
        .execution
        .task(root)
        .unwrap()
        .status()
        .unwrap()
        .clone();
    harness
        .execution
        .apply_covered_entries(vec![
            covered_frame(context, 3, CoveredStatusStreamFact::Status(terminal)),
            covered_frame(
                context,
                3,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 3,
                    initial_cut: 20,
                }),
            ),
        ])
        .unwrap();
    assert!(
        harness.execution.covered_observation_ready(),
        "exact positive terminal and complete newer catch-up close the gap"
    );
    assert!(!harness.execution.covered_recovery_expired());
}

#[test]
fn actual_gap_inherits_old_debt_but_settled_debt_does_not_age_a_new_gap() {
    for settle_debt in [false, true] {
        let mut harness = Harness::new(&[0], &[1], 512);
        harness.settle_until_quiet(Duration::from_secs(10));
        let root = harness.execution.graph().root_task();
        let identity = harness.identity(root);
        let context = harness.execution.graph().task(root).unwrap().context();
        harness
            .execution
            .apply_covered_entries(vec![
                covered_frame(
                    context,
                    1,
                    CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                        generation: 1,
                        initial_cut: 0,
                    }),
                ),
                covered_frame(
                    context,
                    1,
                    CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                        generation: 1,
                        sequence: 1,
                        covered_prefix: 0,
                        source_cut: 10,
                    }),
                ),
            ])
            .unwrap();
        harness.clock.advance(Duration::from_secs(29));
        if settle_debt {
            harness
                .execution
                .apply_covered_entries(vec![covered_frame(
                    context,
                    1,
                    CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                        generation: 1,
                        sequence: 2,
                        covered_prefix: 10,
                        source_cut: 10,
                    }),
                )])
                .unwrap();
        }
        harness
            .execution
            .apply_covered_entries(vec![covered_frame(
                context,
                2,
                CoveredStatusStreamFact::Gone(identity),
            )])
            .unwrap();
        harness.clock.advance(Duration::from_secs(1));
        if settle_debt {
            assert!(
                !harness.execution.covered_recovery_expired(),
                "settled debt is not history attached to a later real gap"
            );
            harness.clock.advance(Duration::from_secs(29));
            assert!(harness.execution.covered_recovery_expired());
        } else {
            assert!(
                harness.execution.covered_recovery_expired(),
                "a true gap cannot grant old outstanding debt another window"
            );
        }
    }
}

fn exercise_covered_installed_retiring_unknown_create(unknown_first: bool) {
    let processes = backends(2);
    let schedule = chain_schedule(&[1; 3], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).unwrap();
    let mut harness = Harness::from_graph_with_preparing_positions(graph, 2);
    harness.execution.activate_covered_observation();
    let leaf_tasks = harness.stage_tasks(1);
    let target = harness.identity(leaf_tasks[0]);
    let backend = target.backend_process_id();
    let context = harness.execution.task(target.task_id()).unwrap().context();
    let initial = creates_of(harness.establish_all(Duration::from_secs(10)));
    assert_eq!(
        initial
            .iter()
            .filter(|intent| create_identity(intent).backend_process_id() == backend)
            .count(),
        2,
        "the exact backend deployment window must initially be full"
    );
    let mut target_create = None;
    for create in initial {
        if create_identity(&create) == target {
            target_create = Some(create);
        } else {
            // Accepted frees the transport permit while retaining the
            // deployment position until this sibling is Installed.
            harness
                .execution
                .acknowledge(&accepted_create_ack(&create))
                .unwrap();
        }
    }
    let target_create = target_create.expect("the target's first Create was released");
    let (frozen, parts) = {
        let OperationIntent::CreateTask(request) = &target_create else {
            unreachable!("the target operation is a Create");
        };
        assert!(!request.parts().metadata().is_empty());
        request.weak_original_lifetimes()
    };
    assert!(
        harness
            .execution
            .task(target.task_id())
            .unwrap()
            .holds_create_payload()
    );
    assert!(
        harness
            .execution
            .task(target.task_id())
            .unwrap()
            .needs_deployment_window()
    );
    if unknown_first {
        harness.transport_unknown_ack(&target_create).unwrap();
        let task = harness.execution.task(target.task_id()).unwrap();
        assert!(!task.create_ownership_proven());
        assert!(task.holds_create_payload());
        assert!(task.needs_deployment_window());
    }

    // A positive initial covered fact can settle ownership independently of
    // the RPC answer and of the stream's later catch-up boundary.
    let installed = TaskStatus::try_new_with_installed(
        target,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Planned,
        None,
        TaskOutputFacts::default(),
        true,
    )
    .unwrap();
    harness
        .execution
        .apply_covered_entries(vec![covered_frame(
            context,
            1,
            CoveredStatusStreamFact::Status(installed),
        )])
        .unwrap();
    let task = harness.execution.task(target.task_id()).unwrap();
    assert!(task.create_ownership_proven());
    assert!(task.installed());
    assert!(!task.holds_create_payload());
    assert!(!task.needs_deployment_window());
    assert!(
        parts.upgrade().is_some(),
        "the physical first-send carrier still owns its retained body"
    );
    if !unknown_first {
        harness.transport_unknown_ack(&target_create).unwrap();
    }
    let task = harness.execution.task(target.task_id()).unwrap();
    assert!(task.create_ownership_proven());
    assert!(task.installed());
    assert!(!task.holds_create_payload());
    assert!(!task.needs_deployment_window());
    assert_eq!(
        harness
            .execution
            .dispatcher()
            .lane_in_flight(backend, DispatchLane::Create),
        0,
        "the actual unknown RPC answer must settle its transport permit"
    );
    drop(target_create);
    assert!(
        frozen.upgrade().is_none(),
        "neither the Task owner nor a transport/test carrier may retain the frozen Create"
    );
    assert!(
        parts.upgrade().is_none(),
        "the last real body owner must return its retained metadata charge"
    );

    let released = harness.released();
    assert!(
        released.iter().all(|intent| {
            !matches!(intent, OperationIntent::CreateTask(request) if request.identity() == target)
        }),
        "an Installed fact must suppress an unnecessary exact Create replay"
    );
    let next = creates_of(released)
        .into_iter()
        .filter(|intent| create_identity(intent).backend_process_id() == backend)
        .collect::<Vec<_>>();
    assert_eq!(
        next.len(),
        1,
        "Installed must return exactly one deployment position for the waiting leaf"
    );
    assert!(
        leaf_tasks
            .iter()
            .any(|&task| harness.identity(task) == create_identity(&next[0]))
    );
    harness
        .execution
        .acknowledge(&accepted_create_ack(&next[0]))
        .unwrap();
    drop(next);
    assert!(
        harness.released().iter().all(|intent| {
            !matches!(intent, OperationIntent::CreateTask(request) if request.identity() == target)
        }),
        "a later pump must not revive the settled Create"
    );
}

#[test]
fn covered_installed_after_unknown_create_ack_retires_replay_and_deployment_position() {
    exercise_covered_installed_retiring_unknown_create(true);
}

#[test]
fn covered_installed_before_late_unknown_create_ack_retires_replay_and_deployment_position() {
    exercise_covered_installed_retiring_unknown_create(false);
}

#[test]
fn accepted_preparing_tasks_retain_deployment_window_after_rpc_settlement() {
    for preparing_positions in [2, 4096] {
        let processes = backends(2);
        let schedule = chain_schedule(&[1; 20], &[0]);
        let graph = build_graph(&schedule, &chain_edges(), &processes, 512).unwrap();
        let mut harness = Harness::from_graph_with_preparing_positions(graph, preparing_positions);
        let mut accepted = Vec::new();
        for _ in 0..8 {
            let released = harness.released();
            if released.is_empty() {
                break;
            }
            for intent in released {
                if let OperationIntent::CreateTask(request) = &intent {
                    let identity = request.identity();
                    harness.statuses.insert(identity.task_id(), 1);
                    harness
                        .execution
                        .acknowledge(&OperationAcknowledgement::new(
                            intent.operation_id(),
                            OperationKind::CreateTask,
                            OperationOutcome::Accepted,
                            AckPayload::Create(Box::new(CreateTaskReceipt::new(
                                identity,
                                Vec::new(),
                                TaskStatus::created(identity),
                            ))),
                        ))
                        .unwrap();
                    if identity.backend_process_id()
                        == harness
                            .identity(harness.stage_tasks(1)[0])
                            .backend_process_id()
                    {
                        accepted.push(identity);
                    }
                } else if intent.kind() == OperationKind::UpdateQueryContext {
                    harness.context_ack(&intent, Duration::from_secs(10));
                } else if intent.kind() == OperationKind::UpdateTask {
                    harness
                        .update_ack(&intent, OperationOutcome::Accepted)
                        .unwrap();
                } else {
                    panic!("unexpected preparation-only operation");
                }
            }
        }
        assert_eq!(
            accepted.len(),
            preparing_positions.min(DispatchBudget::DEFAULT.create_permits()),
            "Accepted frees transport permits but not the exact deployment positions"
        );
        assert!(harness.released().is_empty());
        harness.publish(accepted[0].task_id(), TaskState::Running, None, false);
        let next = harness.released();
        assert_eq!(
            next.iter()
                .filter(
                    |intent| matches!(intent, OperationIntent::CreateTask(request)
        if request.identity().backend_process_id() == accepted[0].backend_process_id())
                )
                .count(),
            1,
            "Installed returns exactly one position without waiting for Task exit"
        );
        for intent in next {
            match intent.kind() {
                OperationKind::CreateTask => harness
                    .create_ack(&intent, OperationOutcome::Accepted)
                    .unwrap(),
                OperationKind::UpdateTask => harness
                    .update_ack(&intent, OperationOutcome::Accepted)
                    .unwrap(),
                OperationKind::UpdateQueryContext => {
                    harness.context_ack(&intent, Duration::from_secs(10))
                }
                _ => panic!("unexpected normal operation"),
            }
        }
        harness
            .execution
            .stand_down_task_normally(accepted[1])
            .unwrap();
        assert!(
            harness
                .execution
                .task(accepted[1].task_id())
                .unwrap()
                .normally_stood_down()
        );
        let next = harness.released();
        assert!(
            next.iter().any(
                |intent| matches!(intent, OperationIntent::CreateTask(request)
        if request.identity().backend_process_id() == accepted[0].backend_process_id())
            ),
            "permanent local stand-down cannot leave necessary Tasks behind a parked window position"
        );
    }
}

#[test]
fn incompatible_advertised_preparation_capacity_is_refused_without_negotiating_w() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).unwrap();
    let epochs = graph
        .contexts()
        .map(|context| (context.backend_process_id(), admission_epoch()))
        .collect::<BTreeMap<_, _>>();
    let sink = Arc::new(RecordingSink::default());
    let capacities = epochs.keys().map(|&process| (process, 2)).collect();
    let result = QueryTaskExecution::new(
        graph,
        DispatchBudget::DEFAULT,
        TransportBudget::DEFAULT,
        NativeCompatibilityId::new([0x41; 32]),
        &epochs,
        &capacities,
        Arc::new(ManualClock::new()),
        Arc::clone(&sink) as Arc<dyn TaskOperationSink>,
        StatusIntake::new(64, Arc::new(CountingWake::default())),
    );
    assert!(
        matches!(result, Err(TaskExecutionError::Schedule(_))),
        "configured W16 with advertised P2 is incompatible, not an invitation to shrink W"
    );
    assert_eq!(sink.reservations(DispatchLane::Create), 0);
    assert!(sink.take().is_empty());
}

#[test]
fn preparation_failure_in_create_ack_freezes_admission_before_any_stream_repeat() {
    for state in [TaskState::Failing, TaskState::Failed] {
        let mut harness = Harness::new(&[0; 24], &[0], 512);
        let released = harness.released();
        let intent = released
            .iter()
            .find(|intent| matches!(intent, OperationIntent::CreateTask(_)))
            .unwrap();
        let OperationIntent::CreateTask(request) = intent else {
            unreachable!()
        };
        let failure = TerminationDetail::Failed(
            novarocks_execution::task_execution::TaskFailure::new_in_phase(
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(3).unwrap(),
                    ),
                ),
                novarocks_execution::task_execution::SafeDetail::truncating("preparation failed"),
                novarocks_execution::task_execution::TaskFailurePhase::Preparation,
            ),
        );
        let status = TaskStatus::try_new_with_installed(
            request.identity(),
            TaskStatusVersion::FIRST,
            state,
            Some(failure.clone()),
            TaskOutputFacts::default(),
            false,
        )
        .unwrap();
        harness
            .execution
            .acknowledge(&OperationAcknowledgement::new(
                intent.operation_id(),
                OperationKind::CreateTask,
                OperationOutcome::Accepted,
                AckPayload::Create(Box::new(CreateTaskReceipt::new(
                    request.identity(),
                    Vec::new(),
                    status,
                ))),
            ))
            .unwrap();
        assert_eq!(
            harness.execution.failure_cause(),
            Some(&failure),
            "the ACK's terminal is already accepted evidence, not a hint for a future status stream"
        );
        assert!(
            harness.released().is_empty(),
            "returning failed deployment positions cannot deploy another normal Task"
        );
    }
}

/// Reuses the real serial round with one known terminal-observation gap. The
/// loopback transport tests supply its covered source after this startup cut.
pub(crate) fn covered_loopback_recovery_round() -> (
    crate::task_execution::round::TaskRound,
    Arc<ManualClock>,
    QueryContextRef,
    TaskIdentity,
) {
    let harness = Harness::new(&[0], &[0], 64);
    let clock = Arc::clone(&harness.clock);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    for intent in &creates {
        acks.publish(accepted_create_ack(intent));
    }
    round.turn().unwrap();
    let root = round.root_task();
    let missing = round
        .execution()
        .graph()
        .tasks()
        .find(|task| task.identity() != root)
        .unwrap()
        .identity();
    let context = round
        .execution()
        .graph()
        .task(missing.task_id())
        .unwrap()
        .context();
    let identities = round
        .execution()
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect::<Vec<_>>();
    for identity in identities
        .into_iter()
        .filter(|&identity| identity != missing)
    {
        for (version, state, output) in [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ] {
            status.publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    identity,
                    TaskStatusVersion::new(version).unwrap(),
                    state,
                    None,
                    TaskOutputFacts::new(output),
                )
                .unwrap(),
            )));
        }
    }
    round.turn().unwrap();
    round
        .execution_mut()
        .apply_covered_entries(vec![
            covered_frame(context, 1, CoveredStatusStreamFact::Gone(missing)),
            covered_frame(
                context,
                1,
                CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                    generation: 1,
                    initial_cut: 1,
                }),
            ),
        ])
        .unwrap();
    // The transport starts at the next generation. Consume this already-known
    // gap's one reconciliation effect rather than create a second subscription.
    round.execution_mut().take_covered_reconciliations();
    round.execution_mut().begin_normal_drain();
    round.turn().unwrap(); // Issue the exact Context Quiesce before its receipt.
    (round, clock, context, missing)
}

#[test]
fn revoked_result_control_uses_one_deadline_even_without_eos_or_after_root_finished() {
    for finished in [false, true] {
        let harness = Harness::new(&[0], &[1], 64);
        let clock = Arc::clone(&harness.clock);
        let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
        let source = round.take_root_status_source().unwrap();
        for create in &creates {
            acks.publish(accepted_create_ack(create));
        }
        round.turn().unwrap();
        let root = round.root_task();
        if finished {
            for (version, state, output) in [
                (2, TaskState::Running, false),
                (3, TaskState::Flushing, false),
                (4, TaskState::Finished, true),
            ] {
                status.publish(StatusEvent::Published(Box::new(
                    TaskStatus::try_new(
                        root,
                        TaskStatusVersion::new(version).unwrap(),
                        state,
                        None,
                        TaskOutputFacts::new(output),
                    )
                    .unwrap(),
                )));
            }
            round.turn().unwrap();
        }
        let mut first = source.begin_terminal_control_request().unwrap();
        round.turn().unwrap();
        assert!(first.try_recv().unwrap().is_ok());
        assert!(round.execution().result_terminal_control_required());
        assert!(!round.root_success_sealed());
        let root_context = round.execution().task(root.task_id()).unwrap().context();
        assert_eq!(
            round.classify_covered_subscription_state(
                root_context,
                crate::native::task_transport::SubscriptionState::BudgetExhausted
            ),
            Some(crate::native::task_transport::SubscriptionState::BudgetExhausted)
        );
        assert_eq!(
            round.classify_covered_subscription_state(
                root_context,
                crate::native::task_transport::SubscriptionState::ConnectionLossExhausted
            ),
            Some(crate::native::task_transport::SubscriptionState::ConnectionLossExhausted)
        );
        clock.advance(Duration::from_secs(29));
        let mut repeated = source.begin_terminal_control_request().unwrap();
        round.turn().unwrap();
        assert!(repeated.try_recv().unwrap().is_ok());
        assert!(!round.execution().required_result_terminal_control_expired());
        clock.advance(Duration::from_secs(1));
        let error = round.turn().unwrap_err();
        assert!(matches!(
            error,
            super::error::TaskExecutionError::Schedule(_)
        ));
        assert!(
            error
                .to_string()
                .contains("required terminal control for revoked root")
        );
        assert!(!round.root_success_sealed());
    }
}

#[test]
fn revoked_result_originator_fact_wins_and_success_seal_is_never_allowed() {
    let harness = Harness::new(&[0], &[1], 64);
    let clock = Arc::clone(&harness.clock);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    let source = round.take_root_status_source().unwrap();
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    let child = round
        .execution()
        .graph()
        .tasks()
        .find(|task| task.identity() != root)
        .unwrap()
        .identity();
    let mut registered = source.begin_terminal_control_request().unwrap();
    round.turn().unwrap();
    assert!(registered.try_recv().unwrap().is_ok());
    let failure = TerminationDetail::Failed(novarocks_execution::task_execution::TaskFailure::new(
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Data,
            novarocks_execution_contract::failure::FailureReason::new(
                std::num::NonZeroU16::new(u16::MAX).unwrap(),
                std::num::NonZeroU16::new(1).unwrap(),
            ),
        ),
        novarocks_execution::task_execution::SafeDetail::new(
            "original upstream array length error",
        )
        .unwrap(),
    ));
    for (version, state) in [
        (2, TaskState::Running),
        (3, TaskState::Failing),
        (4, TaskState::Failed),
    ] {
        status.publish(StatusEvent::Published(Box::new(
            TaskStatus::try_new(
                child,
                TaskStatusVersion::new(version).unwrap(),
                state,
                (state != TaskState::Running).then_some(failure.clone()),
                TaskOutputFacts::new(false),
            )
            .unwrap(),
        )));
    }
    round.turn().unwrap();
    assert_eq!(round.failure_cause(), Some(&failure));
    clock.advance(Duration::from_secs(30));
    assert!(!round.execution().required_result_terminal_control_expired());
    let mut seal = source.begin_success_seal_request().unwrap();
    round.turn().unwrap();
    assert!(
        seal.try_recv()
            .unwrap()
            .unwrap_err()
            .message()
            .contains("requires terminal control")
    );
    assert!(!round.root_success_sealed());
}

/// A real admitted TaskRound for the synchronous result-consumer controls.
/// No status or clock owner is replaced by the coordinator test transport.
pub(crate) fn synchronous_root_control_round() -> (
    crate::task_execution::round::TaskRound,
    crate::task_execution::status_intake::StatusIntakeHandle,
    Arc<ManualClock>,
) {
    let harness = Harness::new(&[0], &[1], 64);
    let clock = Arc::clone(&harness.clock);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap(),
    )));
    round.turn().unwrap();
    assert!(round.result_pump_ready());
    assert!(!round.accepted_root_success_sealed());
    (round, status, clock)
}

type EstablishObservationFixture = (
    TaskRoundForTest,
    Arc<ManualClock>,
    crate::native::task_transport::TaskAckIntakeHandle,
    Arc<RecordingSink>,
    OperationIntent,
    Arc<RecordingSubscriptions>,
    novarocks_execution::task_execution::AcquireQueryContextAdmissionTicket,
    QueryContextAdmissionTicketReceipt,
);

fn original_establish_observation_round_with_requests() -> EstablishObservationFixture {
    use crate::native::task_transport::TaskAckIntake;
    use crate::task_execution::round::TaskRound;
    let harness = Harness::new(&[0], &[0], 64);
    let sink = Arc::clone(&harness.sink);
    let clock = Arc::clone(&harness.clock);
    let subscriptions = Arc::new(RecordingSubscriptions::default());
    let intake = TaskAckIntake::new(Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>);
    let acks = intake.handle();
    let mut round = TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::clone(&subscriptions) as Arc<dyn crate::task_execution::round::StatusSubscriptions>,
    );
    round.seal_pumps();
    round.turn().unwrap();
    let admissions = released_admissions(&sink);
    assert_eq!(admissions.len(), 1);
    let OperationIntent::AcquireQueryContextAdmissionTicket(admission) = &admissions[0] else {
        unreachable!()
    };
    let admission = *admission;
    let admission_result = admission_ack(&admissions[0]);
    let AckPayload::AdmissionTicket(ticket) = admission_result.payload() else {
        unreachable!()
    };
    let ticket = *ticket;
    acks.publish(admission_result);
    round.turn().unwrap();
    let mut establishes = released_establishes(&sink);
    assert_eq!(establishes.len(), 1);
    (
        round,
        clock,
        acks,
        sink,
        establishes.pop().unwrap(),
        subscriptions,
        admission,
        ticket,
    )
}

fn original_establish_observation_round() -> (
    TaskRoundForTest,
    Arc<ManualClock>,
    crate::native::task_transport::TaskAckIntakeHandle,
    Arc<RecordingSink>,
    OperationIntent,
    Arc<RecordingSubscriptions>,
) {
    let (round, clock, acks, sink, establish, subscriptions, _, _) =
        original_establish_observation_round_with_requests();
    (round, clock, acks, sink, establish, subscriptions)
}

pub(crate) fn pre_ready_observation_round() -> (TaskRoundForTest, Arc<ManualClock>) {
    let (round, clock, _, _, _, _) = original_establish_observation_round();
    (round, clock)
}

/// Drive the existing admission/Establish owner to its actual correlated
/// negative receipt; no private phase or observation horizon is manufactured.
pub(crate) fn pre_ready_identity_rejected_observation_round()
-> (TaskRoundForTest, Arc<ManualClock>, TaskExecutionError) {
    let (mut round, clock, acks, sink, establish, _) = original_establish_observation_round();
    clock.advance(Duration::from_secs(3));
    acks.publish(OperationAcknowledgement::worker_receipt(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
        OperationOutcome::IdentityMismatch,
        AckPayload::None,
    ));
    let error = round.receive_without_dispatch().unwrap_err();
    assert!(
        matches!(&error, TaskExecutionError::PreReadyEstablishRejected {
        operation_id, rejection, ..
    } if *operation_id == establish.operation_id()
        && rejection.outcome() == OperationOutcome::IdentityMismatch)
    );
    assert!(sink.take().is_empty());
    (round, clock, error)
}

/// The same released admission and Establish are borrowed by the actual QA
/// actor in Manifest tests; no second operation or ticket is reconstructed.
pub(crate) fn pre_ready_original_establish_requests() -> (
    TaskRoundForTest,
    Arc<ManualClock>,
    crate::native::task_transport::TaskAckIntakeHandle,
    novarocks_execution::task_execution::AcquireQueryContextAdmissionTicket,
    QueryContextAdmissionTicketReceipt,
    Arc<novarocks_execution::task_execution::EstablishQueryContext>,
) {
    let (round, clock, acks, _, establish, _, admission, ticket) =
        original_establish_observation_round_with_requests();
    let OperationIntent::EstablishQueryContext(request) = establish else {
        unreachable!()
    };
    (round, clock, acks, admission, ticket, request)
}

#[test]
fn rejected_establish_preserves_only_its_original_ticket_observation_horizon() {
    let (round, clock, error) = pre_ready_identity_rejected_observation_round();
    let TaskExecutionError::PreReadyEstablishRejected {
        context,
        operation_id,
        ..
    } = error
    else {
        unreachable!()
    };
    let execution = round.execution();
    assert_eq!(
        execution.pre_ready_observation_remaining(context.backend_process_id()),
        None
    );
    assert_eq!(
        execution.rejected_establish_observation_remaining(context, operation_id),
        Some(super::context_owner::ADMISSION_TICKET_VALID_FOR - Duration::from_secs(3))
    );
    assert_eq!(
        execution.rejected_establish_observation_remaining(context, TaskOperationId::new_v7()),
        None
    );
    let foreign = QueryContextRef::new(
        context.query_execution_id(),
        FrontendProcessId::new_v7(),
        context.backend_process_id(),
    );
    assert_eq!(
        execution.rejected_establish_observation_remaining(foreign, operation_id),
        None
    );
    clock.advance(super::context_owner::ADMISSION_TICKET_VALID_FOR);
    assert_eq!(
        execution.rejected_establish_observation_remaining(context, operation_id),
        Some(Duration::ZERO)
    );
}

#[test]
fn locally_rejected_establish_has_no_worker_replacement_observation() {
    let (mut round, _, acks, _, establish, _) = original_establish_observation_round();
    let original = super::error::local_protocol_refusal(
        OperationKind::UpdateQueryContext,
        OperationOutcome::IdentityMismatch,
    );
    acks.publish(OperationAcknowledgement::local_refusal(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
        original.clone(),
    ));
    let error = round.receive_without_dispatch().unwrap_err();
    assert_eq!(error, TaskExecutionError::Execution(original));
    let context = *round.execution().graph().contexts().next().unwrap();
    assert_eq!(
        round
            .execution()
            .rejected_establish_observation_remaining(context, establish.operation_id()),
        None
    );
    assert!(
        !round.execution().owner(context).unwrap().is_released(),
        "a local refusal cannot supply a Worker exit receipt"
    );
}

pub(crate) fn manifest_preparing_observation_round() -> TaskRoundForTest {
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, _, acks, creates) = round_waiting_on_creates(harness);
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    round
}

#[test]
fn pre_ready_observation_keeps_original_ticket_expiry_across_unknown_replay() {
    let (mut round, clock, acks, sink, establish, _) = original_establish_observation_round();
    let backend = round
        .execution()
        .graph()
        .contexts()
        .next()
        .unwrap()
        .backend_process_id();
    let original = round
        .execution()
        .pre_ready_observation_remaining(backend)
        .unwrap();
    assert_eq!(original, super::context_owner::ADMISSION_TICKET_VALID_FOR);
    clock.advance(Duration::from_secs(3));
    acks.publish(OperationAcknowledgement::transport_unknown(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
    ));
    let report = round.receive_without_dispatch().unwrap();
    assert_eq!(report.acknowledgements, 1);
    assert_eq!(report.operations, 0);
    assert!(sink.take().is_empty());
    assert_eq!(
        round.execution().pre_ready_observation_remaining(backend),
        Some(original - Duration::from_secs(3)),
    );
    round.turn().unwrap();
    let replay = released_establishes(&sink);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].operation_id(), establish.operation_id());
    clock.advance(original);
    assert_eq!(
        round.execution().pre_ready_observation_remaining(backend),
        Some(Duration::ZERO)
    );
    assert_eq!(
        round
            .execution()
            .pre_ready_observation_remaining(BackendProcessId::new_v7()),
        None
    );
}

#[test]
fn receiver_only_turn_settles_original_establish_ack_without_create_or_input_work() {
    struct NoInput;
    impl crate::task_execution::round::TurnPump for NoInput {
        fn name(&self) -> &'static str {
            "no-input-during-process-observation"
        }
        fn drive(&mut self, _: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
            panic!("receiver-only mode must not produce input");
        }
        fn begin_close(&mut self) {}
        fn close_turn(&mut self) -> Result<usize, TaskExecutionError> {
            Ok(0)
        }
        fn is_closed(&self) -> bool {
            true
        }
        fn take_close_unresolved(
            &mut self,
        ) -> Option<crate::task_execution::round::PumpCloseUnresolved> {
            None
        }
    }
    let (mut round, _, acks, sink, establish, subscriptions) =
        original_establish_observation_round();
    round.add_pump(Box::new(NoInput));
    acks.publish(establish_ack(&establish));
    let report = round.receive_without_dispatch().unwrap();
    assert_eq!(report.acknowledgements, 1);
    assert_eq!(report.operations, 0);
    assert_eq!(report.pumped, 0);
    assert!(
        sink.take().is_empty(),
        "no create/update/lifecycle work is sent"
    );
    let context = *round.execution().graph().contexts().next().unwrap();
    assert!(
        round
            .execution()
            .owner(context)
            .unwrap()
            .establish_acknowledged()
    );
    assert_eq!(
        round
            .execution()
            .pre_ready_observation_remaining(context.backend_process_id()),
        None
    );
    assert_eq!(subscriptions.ensured.lock().unwrap().len(), 1);
}

pub(crate) fn manifest_originating_failure_observation_round() -> TaskRoundForTest {
    use novarocks_failure_contract::FailureProjection;
    let harness = Harness::new(&[0], &[0], 64);
    let (mut round, status, acks, creates) = round_waiting_on_creates(harness);
    for create in &creates {
        acks.publish(accepted_create_ack(create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    status.publish(StatusEvent::Published(Box::new(
        TaskStatus::try_new_with_installed(
            root, TaskStatusVersion::new(2).unwrap(), TaskState::Failing,
            Some(TerminationDetail::Failed(novarocks_execution_contract::TaskFailure::new_in_phase(
                novarocks_query_application::execution_error::QueryFailureReason::PreparationFailed.failure_fact(),
                novarocks_execution_contract::SafeDetail::truncating("original preparing Task failure"),
                novarocks_execution_contract::TaskFailurePhase::Preparation,
            ))),
            TaskOutputFacts::new(false),
            false,
        ).unwrap(),
    )));
    round
}

#[test]
fn pending_receiver_dispatches_only_owned_actor_abort_and_preserves_backpressure() {
    let mut harness = Harness::new(&[0], &[0], 64);
    let context = *harness.execution.graph().contexts().next().unwrap();
    // Retain a real ordinary admission behind the urgent Abort in the same
    // lifecycle lane. A filtered control dispatch may not release it.
    harness
        .sink
        .backpressured_lanes
        .lock()
        .unwrap()
        .insert(DispatchLane::Lifecycle);
    harness.execution.pump(&harness.establish).unwrap();
    assert!(harness.sink.take().is_empty());
    let ordinary_queued = harness.execution.dispatcher().queued_items();
    assert_eq!(ordinary_queued, 1);
    let abort =
        OperationIntent::AbortQueryContext(novarocks_execution_contract::AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    let operation_id = abort.operation_id();
    let permit = harness
        .execution
        .try_reserve_actor_abort(&abort)
        .unwrap()
        .unwrap();
    harness
        .execution
        .enqueue_actor_abort(abort, permit)
        .unwrap();
    let first = harness.execution.pump_pending_actor_aborts().unwrap();
    assert_eq!(first.operations, 0);
    assert!(harness.sink.take().is_empty());
    assert_eq!(
        harness.execution.dispatcher().queued_items(),
        ordinary_queued + 1
    );
    harness.sink.backpressured_lanes.lock().unwrap().clear();
    let sent = harness.execution.pump_pending_actor_aborts().unwrap();
    assert_eq!(sent.operations, 1);
    let operations = harness
        .sink
        .take()
        .into_iter()
        .flat_map(|(_, ops)| ops)
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].operation_id(), operation_id);
    assert_eq!(operations[0].kind(), OperationKind::AbortQueryContext);
    assert_eq!(
        harness.execution.dispatcher().queued_items(),
        ordinary_queued
    );
    assert_eq!(
        harness
            .execution
            .pump_pending_actor_aborts()
            .unwrap()
            .operations,
        0
    );
    assert!(
        harness.sink.take().is_empty(),
        "ordinary admission stays retained"
    );
}

pub(crate) fn pre_ready_expired_replay_observation_round() -> (TaskRoundForTest, TaskExecutionError)
{
    let (mut round, clock, acks, sink, establish, _) = original_establish_observation_round();
    sink.backpressured_lanes
        .lock()
        .unwrap()
        .insert(DispatchLane::Lifecycle);
    acks.publish(OperationAcknowledgement::transport_unknown(
        establish.operation_id(),
        OperationKind::UpdateQueryContext,
    ));
    round.turn().unwrap();
    assert!(sink.take().is_empty());
    assert_eq!(round.execution().dispatcher().queued_items(), 1);
    clock.advance(TransportBudget::DEFAULT.frontend_queue_residence());
    let error = round.receive_without_dispatch().unwrap_err();
    assert_eq!(round.execution().dispatcher().queued_items(), 0);
    (round, error)
}

#[test]
fn an_adopted_replacement_ticket_does_not_mint_an_original_observation_clock() {
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
    let received_at = MonotonicInstant::from_origin(Duration::from_secs(100));
    owner
        .adopt_replacement_admission(
            QueryContextAdmissionTicketReceipt::new(
                AdmissionTicketId::try_from_bytes([0x54; 16]).unwrap(),
                context,
                LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            ),
            received_at,
        )
        .unwrap();
    // The existing admission contract still authorizes this first establish.
    // The original request's send timestamp is not known on this Task clock,
    // so receipt observation cannot grant another process-evidence window.
    assert!(
        owner
            .establish_intent(FakeEstablish.facts_for(context).unwrap(), received_at)
            .unwrap()
            .is_some()
    );
    assert_eq!(owner.pre_ready_observation_remaining(received_at), None);
    assert_eq!(
        owner.pre_ready_observation_remaining(received_at.saturating_add(Duration::from_secs(1))),
        None
    );
}

pub(crate) fn pre_ready_invalid_ack_observation_round() -> TaskRoundForTest {
    let (round, _, acks, _, _, _) = original_establish_observation_round();
    acks.publish(OperationAcknowledgement::transport_unknown(
        TaskOperationId::new_v7(),
        OperationKind::UpdateTask,
    ));
    round
}

mod accepted_terminal_index_tests {
    use super::*;
    use novarocks_coordination_contract::StatusObservation;
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TaskFailurePhase, TaskResourceFacts,
    };

    fn originating_failure(detail: &str, phase: TaskFailurePhase) -> TerminationDetail {
        TerminationDetail::Failed(TaskFailure::new_in_phase(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new(detail).unwrap(),
            phase,
        ))
    }

    fn apply_snapshot(
        harness: &mut Harness,
        status: TaskStatus,
    ) -> Result<usize, TaskExecutionError> {
        assert_eq!(
            harness
                .execution
                .intake()
                .handle()
                .publish(StatusEvent::Published(Box::new(status))),
            StatusIntakeAdmission::Enqueued,
        );
        harness
            .execution
            .apply_status(64)
            .map(|report| report.accepted)
    }

    #[test]
    fn create_ack_indexes_its_first_terminal_once_without_waiting_for_status_stream() {
        for preparation_failure in [false, true] {
            let mut harness = Harness::new(&[0], &[0], 64);
            let create = creates_of(harness.establish_all(Duration::from_secs(30)))
                .into_iter()
                .next()
                .unwrap();
            let identity = create_identity(&create);
            assert!(harness.execution.accepted_terminal_tasks().is_empty());
            let (state, cause, output, installed) = if preparation_failure {
                (
                    TaskState::Failed,
                    Some(originating_failure(
                        "original preparation failure",
                        TaskFailurePhase::Preparation,
                    )),
                    TaskOutputFacts::default(),
                    false,
                )
            } else {
                (TaskState::Finished, None, TaskOutputFacts::new(true), true)
            };
            let original = TaskStatus::try_new_with_installed(
                identity,
                TaskStatusVersion::FIRST,
                state,
                cause.clone(),
                output,
                installed,
            )
            .unwrap();
            let ack = OperationAcknowledgement::new(
                create.operation_id(),
                OperationKind::CreateTask,
                OperationOutcome::Accepted,
                AckPayload::Create(Box::new(CreateTaskReceipt::new(
                    identity,
                    Vec::new(),
                    original.clone(),
                ))),
            );
            harness.execution.acknowledge(&ack).unwrap();
            assert_eq!(harness.execution.accepted_terminal_tasks(), &[identity]);
            assert_eq!(
                harness.execution.task(identity.task_id()).unwrap().status(),
                Some(&original)
            );
            let revision = harness.execution.accepted_protocol_revision();
            assert!(revision > 0);

            // A settled operation is no longer an authority. Keep its original
            // UnknownOperation behavior instead of treating it as a new event.
            assert!(matches!(
                harness.execution.acknowledge(&ack),
                Err(TaskExecutionError::UnknownOperation)
            ));
            assert_eq!(apply_snapshot(&mut harness, original.clone()).unwrap(), 0);
            assert_eq!(harness.execution.accepted_terminal_tasks(), &[identity]);
            assert_eq!(harness.execution.accepted_protocol_revision(), revision);

            // Terminal snapshots are immutable, including metric-only changes.
            // Higher versions must be rejected, not accepted as duplicate work.
            let changed_metrics = TaskStatus::try_new_with_installed(
                identity,
                TaskStatusVersion::new(2).unwrap(),
                state,
                cause,
                output,
                installed,
            )
            .unwrap()
            .with_resources(TaskResourceFacts::empty().with_cpu_time(Duration::from_millis(7)));
            assert!(matches!(
                apply_snapshot(&mut harness, changed_metrics),
                Err(TaskExecutionError::Observation(
                    StatusObservation::TerminalOverwrite
                ))
            ));
            assert_eq!(harness.execution.accepted_terminal_tasks(), &[identity]);
            assert_eq!(harness.execution.accepted_protocol_revision(), revision);
            assert_eq!(
                harness.execution.task(identity.task_id()).unwrap().status(),
                Some(&original)
            );
        }
    }

    #[test]
    fn ordinary_status_first_terminal_preserves_exact_coverage_order_and_late_create_ack() {
        let mut harness = Harness::new(&[0], &[0], 64);
        let creates = creates_of(harness.establish_all(Duration::from_secs(30)));
        let frozen = harness
            .execution
            .graph()
            .tasks()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        assert_eq!(creates.len(), frozen.len());
        let order = frozen.iter().copied().rev().collect::<Vec<_>>();
        assert_ne!(
            order, frozen,
            "the fixture must distinguish accepted order from graph order"
        );
        let stream_first = order[0];
        for create in &creates {
            if create_identity(create) != stream_first {
                harness
                    .create_ack(create, OperationOutcome::Accepted)
                    .unwrap();
            }
        }
        assert!(harness.execution.accepted_terminal_tasks().is_empty());
        let mut expected = Vec::new();
        for identity in order {
            let terminal = TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap();
            assert_eq!(apply_snapshot(&mut harness, terminal.clone()).unwrap(), 1);
            expected.push(identity);
            assert_eq!(
                harness.execution.accepted_terminal_tasks(),
                expected.as_slice()
            );
            let revision = harness.execution.accepted_protocol_revision();
            assert_eq!(apply_snapshot(&mut harness, terminal.clone()).unwrap(), 0);
            assert_eq!(
                harness.execution.accepted_terminal_tasks(),
                expected.as_slice()
            );
            assert_eq!(harness.execution.accepted_protocol_revision(), revision);
            if identity == stream_first {
                let create = creates
                    .iter()
                    .find(|create| create_identity(create) == identity)
                    .unwrap();
                let old_ack = accepted_create_ack(create);
                harness.execution.acknowledge(&old_ack).unwrap();
                assert_eq!(
                    harness.execution.accepted_terminal_tasks(),
                    expected.as_slice()
                );
                assert_eq!(harness.execution.accepted_protocol_revision(), revision);
                assert_eq!(
                    harness.execution.task(identity.task_id()).unwrap().status(),
                    Some(&terminal)
                );
                assert!(matches!(
                    harness.execution.acknowledge(&old_ack),
                    Err(TaskExecutionError::UnknownOperation)
                ));
            }
        }
        let indexed = harness.execution.accepted_terminal_tasks();
        assert_eq!(indexed, expected.as_slice());
        assert_eq!(indexed.len(), frozen.len());
        assert_eq!(
            indexed.iter().copied().collect::<BTreeSet<_>>(),
            frozen.iter().copied().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            indexed.iter().copied().collect::<BTreeSet<_>>().len(),
            indexed.len()
        );
        harness.execution.apply_status(64).unwrap();
        assert_eq!(
            harness.execution.accepted_terminal_tasks(),
            expected.as_slice()
        );
    }

    #[test]
    fn live_metrics_and_derived_refinement_create_only_the_first_terminal_ordinal() {
        let mut harness = Harness::new(&[0], &[0], 64);
        let identity = harness.identity(harness.stage_tasks(1)[0]);
        let derived = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
        for version in [2, 3] {
            let live = TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(version).unwrap(),
                TaskState::Aborting,
                Some(derived.clone()),
                TaskOutputFacts::default(),
            )
            .unwrap()
            .with_resources(
                TaskResourceFacts::empty().with_cpu_time(Duration::from_millis(version)),
            );
            assert_eq!(apply_snapshot(&mut harness, live).unwrap(), 1);
            assert!(harness.execution.accepted_terminal_tasks().is_empty());
        }
        let failure = originating_failure("original runtime failure", TaskFailurePhase::Execution);
        let terminal = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(9).unwrap(),
            TaskState::Failed,
            Some(failure.clone()),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert_eq!(apply_snapshot(&mut harness, terminal.clone()).unwrap(), 1);
        assert_eq!(harness.execution.accepted_terminal_tasks(), &[identity]);
        assert_eq!(
            harness.execution.selected_task_failure().unwrap().detail(),
            &failure
        );
        let revision = harness.execution.accepted_protocol_revision();
        assert_eq!(apply_snapshot(&mut harness, terminal.clone()).unwrap(), 0);
        assert_eq!(harness.execution.accepted_protocol_revision(), revision);

        // The allowed live derived refinement does not authorize changing an
        // already terminal Task to a different originating failure afterwards.
        let overwrite = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(10).unwrap(),
            TaskState::Failed,
            Some(originating_failure(
                "replacement runtime failure",
                TaskFailurePhase::Execution,
            )),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert!(matches!(
            apply_snapshot(&mut harness, overwrite),
            Err(TaskExecutionError::Observation(
                StatusObservation::TerminalOverwrite
            ))
        ));
        assert_eq!(harness.execution.accepted_terminal_tasks(), &[identity]);
        assert_eq!(harness.execution.accepted_protocol_revision(), revision);
        assert_eq!(
            harness.execution.task(identity.task_id()).unwrap().status(),
            Some(&terminal)
        );
        assert_eq!(
            harness.execution.selected_task_failure().unwrap().detail(),
            &failure
        );
    }

    #[test]
    fn attempt_failure_refinement_indexes_only_the_new_terminal_task() {
        let mut harness = Harness::new(&[0], &[0], 64);
        let peer = harness.identity(harness.stage_tasks(1)[0]);
        let origin = harness.identity(harness.stage_tasks(2)[0]);
        let peer_terminal = TaskStatus::try_new(
            peer,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert_eq!(
            apply_snapshot(&mut harness, peer_terminal.clone()).unwrap(),
            1
        );
        assert_eq!(harness.execution.accepted_terminal_tasks(), &[peer]);
        assert!(
            harness
                .execution
                .selected_task_failure()
                .unwrap()
                .detail()
                .is_derived()
        );
        let first_revision = harness.execution.accepted_protocol_revision();
        let original_failure =
            originating_failure("different task origin", TaskFailurePhase::Execution);
        let origin_terminal = TaskStatus::try_new(
            origin,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Failed,
            Some(original_failure.clone()),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert_eq!(
            apply_snapshot(&mut harness, origin_terminal.clone()).unwrap(),
            1
        );
        assert_eq!(harness.execution.accepted_terminal_tasks(), &[peer, origin]);
        assert!(harness.execution.accepted_protocol_revision() > first_revision);
        assert_eq!(
            harness
                .execution
                .selected_task_failure()
                .unwrap()
                .identity(),
            origin
        );
        assert_eq!(
            harness.execution.selected_task_failure().unwrap().detail(),
            &original_failure
        );
        let selected_revision = harness.execution.accepted_protocol_revision();
        for duplicate in [peer_terminal, origin_terminal] {
            assert_eq!(apply_snapshot(&mut harness, duplicate).unwrap(), 0);
            assert_eq!(harness.execution.accepted_terminal_tasks(), &[peer, origin]);
            assert_eq!(
                harness.execution.accepted_protocol_revision(),
                selected_revision
            );
            assert_eq!(
                harness
                    .execution
                    .selected_task_failure()
                    .unwrap()
                    .identity(),
                origin
            );
        }
    }
}

#[test]
fn protocol_owner_revision_tracks_ack_status_and_skips_idempotent_and_idle_folds() {
    let mut harness = Harness::new(&[0], &[0], 64);
    assert_eq!(harness.execution.accepted_protocol_revision(), 0);
    let creates = harness.establish_all(Duration::from_secs(30));
    assert_eq!(harness.execution.accepted_protocol_revision(), 0);
    let create = creates
        .into_iter()
        .find(|intent| matches!(intent, OperationIntent::CreateTask(_)))
        .expect("the established graph releases an original Create");
    harness
        .create_ack(&create, OperationOutcome::Accepted)
        .unwrap();
    let revision = harness.execution.accepted_protocol_revision();
    assert!(
        revision > 0,
        "Create ACK's actual status must invalidate protocol projection"
    );
    let OperationIntent::CreateTask(request) = create else {
        unreachable!()
    };
    let original = harness
        .execution
        .task(request.identity().task_id())
        .unwrap()
        .status()
        .unwrap()
        .clone();
    harness
        .execution
        .intake()
        .handle()
        .publish(StatusEvent::Published(Box::new(original)));
    let report = harness.execution.apply_status(64).unwrap();
    assert_eq!(report.accepted, 0);
    assert_eq!(harness.execution.accepted_protocol_revision(), revision);
    harness.execution.apply_status(64).unwrap();
    assert_eq!(harness.execution.accepted_protocol_revision(), revision);
}

#[test]
fn protocol_owner_revision_keeps_failure_refinement_without_root_status_change() {
    let mut harness = Harness::new(&[0], &[0], 64);
    let peer = harness.identity(harness.stage_tasks(1)[0]);
    let origin = harness.identity(harness.stage_tasks(2)[0]);
    let root = harness.execution.graph().root_identity();
    let publish = |harness: &Harness, identity, cause| {
        harness
            .execution
            .intake()
            .handle()
            .publish(StatusEvent::Published(Box::new(
                TaskStatus::try_new(
                    identity,
                    TaskStatusVersion::new(2).unwrap(),
                    TaskState::Aborted,
                    Some(TerminationDetail::Aborted(cause)),
                    TaskOutputFacts::default(),
                )
                .unwrap(),
            )));
    };
    publish(&harness, peer, AbortCause::PeerTaskFailed);
    harness.execution.apply_status(64).unwrap();
    let derived = harness.execution.accepted_protocol_revision();
    assert!(
        harness
            .execution
            .selected_task_failure()
            .unwrap()
            .detail()
            .is_derived()
    );
    assert!(
        harness
            .execution
            .task(root.task_id())
            .unwrap()
            .status()
            .is_none()
    );
    publish(&harness, origin, AbortCause::QueryFailed);
    harness.execution.apply_status(64).unwrap();
    let authoritative = harness.execution.accepted_protocol_revision();
    assert!(authoritative > derived);
    assert_eq!(
        harness
            .execution
            .selected_task_failure()
            .unwrap()
            .identity(),
        origin
    );
    assert!(
        !harness
            .execution
            .selected_task_failure()
            .unwrap()
            .detail()
            .is_derived()
    );
    assert!(
        harness
            .execution
            .task(root.task_id())
            .unwrap()
            .status()
            .is_none()
    );
    publish(&harness, peer, AbortCause::PeerTaskFailed);
    harness.execution.apply_status(64).unwrap();
    assert_eq!(
        harness.execution.accepted_protocol_revision(),
        authoritative
    );
}

#[test]
fn protocol_owner_keeps_one_original_writer_witness_across_a_coalesced_status_fold() {
    let mut harness = Harness::new(&[0], &[0], 64);
    let identity = harness.identity(harness.stage_tasks(1)[0]);
    let original = TaskStatus::try_new(
        identity,
        TaskStatusVersion::new(1).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap()
    .with_writer(novarocks_execution_contract::TaskWriterFacts::empty());
    let later = TaskStatus::try_new(
        identity,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap();
    harness
        .execution
        .intake()
        .handle()
        .publish(StatusEvent::Published(Box::new(original.clone())));
    harness
        .execution
        .intake()
        .handle()
        .publish(StatusEvent::Published(Box::new(later)));
    let report = harness.execution.apply_status(64).unwrap();
    assert_eq!(report.accepted, 2);
    assert!(
        harness
            .execution
            .task(identity.task_id())
            .unwrap()
            .status()
            .unwrap()
            .writer()
            .is_none()
    );
    assert_eq!(
        harness
            .execution
            .accepted_protocol_writer_witness(identity.task_id())
            .unwrap()
            .as_ref(),
        &original
    );
    assert_eq!(harness.execution.accepted_protocol_task_changes().len(), 1);
}

include!("split_delivery_local_terminal_tests.rs");

#[test]
fn actual_batch_limit_keeps_unselected_sources_and_restores_the_same_head() {
    use novarocks_execution::task_execution::CancelTask;
    #[derive(Debug)]
    struct ExactPermit {
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for ExactPermit {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl TaskOperationQueuePermit for ExactPermit {
        fn mark_in_flight(&mut self) {}
    }
    let backend = BackendProcessId::new_v7();
    let execution =
        QueryExecutionId::new(QueryId::new(0x70, 0x08), AttemptId::new(1).unwrap()).unwrap();
    let intents: Vec<_> = (1..=3)
        .map(|task| {
            OperationIntent::CancelTask(CancelTask::new(
                TaskOperationId::new_v7(),
                TaskIdentity::new(
                    execution,
                    StageId::new(1).unwrap(),
                    TaskId::new(task).unwrap(),
                    backend,
                ),
                CancelReason::UpstreamNoLongerNeeded,
            ))
        })
        .collect();
    let ids: Vec<_> = intents.iter().map(OperationIntent::operation_id).collect();
    let dropped = Arc::new(AtomicUsize::new(0));
    let clock = ManualClock::new();
    let now = clock.now();
    let original = TransportBudget::DEFAULT;
    let transport = TransportBudget::new(
        2,
        original.max_batch_encoded_bytes(),
        original.max_descriptor_encoded_bytes(),
        original.max_query_backend_queued_operations(),
        original.max_query_backend_queued_bytes(),
        original.max_backend_queued_operations(),
        original.max_backend_queued_bytes(),
        original.max_tasks_per_context(),
        original.max_active_tasks_per_backend(),
        original.frontend_queue_residence(),
    )
    .unwrap();
    let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, transport);
    for intent in intents {
        dispatcher
            .enqueue_reserved(
                intent,
                now,
                Box::new(ExactPermit {
                    dropped: Arc::clone(&dropped),
                }),
            )
            .unwrap();
    }
    let batch = dispatcher.take_batch().unwrap();
    assert_eq!(
        batch
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>(),
        ids[..2]
    );
    assert_eq!(batch.queued_at(), now);
    assert_eq!(dispatcher.queued_items(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    clock.advance(Duration::from_millis(1));
    dispatcher.restore_backpressured(batch);
    assert_eq!(dispatcher.queued_items(), 3);
    let retried = dispatcher.take_batch().unwrap();
    assert_eq!(
        retried
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>(),
        ids[..2]
    );
    assert_eq!(retried.queued_at(), now);
    drop(retried);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    let last = dispatcher.take_batch().unwrap();
    assert_eq!(last.operations()[0].operation_id(), ids[2]);
    assert_eq!(last.queued_at(), now);
    drop(last);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert_eq!(dispatcher.queued_items(), 0);
}

#[test]
fn actual_batch_limit_keeps_the_whole_configured_method_run() {
    use novarocks_execution::task_execution::CancelTask;
    #[derive(Debug)]
    struct ExactPermit;
    impl TaskOperationQueuePermit for ExactPermit {
        fn mark_in_flight(&mut self) {}
    }
    let backend = BackendProcessId::new_v7();
    let execution =
        QueryExecutionId::new(QueryId::new(0x70, 0x09), AttemptId::new(1).unwrap()).unwrap();
    let count = TransportBudget::DEFAULT.max_batch_items();
    let intents: Vec<_> = (1..=count)
        .map(|task| {
            OperationIntent::CancelTask(CancelTask::new(
                TaskOperationId::new_v7(),
                TaskIdentity::new(
                    execution,
                    StageId::new(1).unwrap(),
                    TaskId::new(task as u32).unwrap(),
                    backend,
                ),
                CancelReason::UpstreamNoLongerNeeded,
            ))
        })
        .collect();
    let clock = ManualClock::new();
    let mut dispatcher = OperationDispatcher::new(
        DispatchBudget::new(count, count, count, count).unwrap(),
        TransportBudget::DEFAULT,
    );
    for intent in intents {
        dispatcher
            .enqueue_reserved(intent, clock.now(), Box::new(ExactPermit))
            .unwrap();
    }
    let batch = dispatcher.take_batch().unwrap();
    assert_eq!(batch.operations().len(), count);
    assert_eq!(dispatcher.queued_items(), 0);
}

/// Supplies both original inputs before a single TaskRound receiver cut.
/// No Native terminal has been published when either input arrives.
pub(crate) fn manifest_task_and_subscription_failure_round(
    observation_first: bool,
    subscription_state: crate::native::task_transport::SubscriptionState,
) -> TaskRoundForTest {
    use novarocks_failure_contract::FailureProjection;
    let (mut round, _, acks, sink, establish, subscriptions) =
        original_establish_observation_round();
    acks.publish(establish_ack(&establish));
    round.turn().unwrap();
    let creates = sink
        .take()
        .into_iter()
        .flat_map(|(_, operations)| operations)
        .filter(|intent| intent.kind() == OperationKind::CreateTask)
        .collect::<Vec<_>>();
    assert_eq!(creates.len(), round.execution().graph().tasks().count());
    for create in creates {
        acks.publish(accepted_create_ack(&create));
    }
    round.turn().unwrap();
    let root = round.root_task();
    let status = round.execution().intake().handle();
    let original = TaskStatus::try_new_with_installed(
        root,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Failing,
        Some(TerminationDetail::Failed(
            novarocks_execution_contract::TaskFailure::new_in_phase(
                novarocks_query_application::execution_error::QueryFailureReason::PreparationFailed
                    .failure_fact(),
                novarocks_execution_contract::SafeDetail::truncating(
                    "same-cut original Task failure",
                ),
                novarocks_execution_contract::TaskFailurePhase::Preparation,
            ),
        )),
        TaskOutputFacts::new(false),
        false,
    )
    .unwrap();
    if observation_first {
        *subscriptions.settled.lock().unwrap() = Some(subscription_state);
        status.publish(StatusEvent::Published(Box::new(original)));
    } else {
        status.publish(StatusEvent::Published(Box::new(original)));
        *subscriptions.settled.lock().unwrap() = Some(subscription_state);
    }
    round
}

#[test]
fn sealed_result_withdrawals_use_exact_graph_demands_without_settling_contexts() {
    let mut harness = Harness::new(&[1], &[2], 512);
    let root = harness.execution.graph().root_identity();
    let activation = novarocks_coordination_contract::AttemptActivationIdentity::from_parts(
        901,
        root.query_execution_id(),
        1,
    );
    assert!(
        harness
            .execution
            .result_context_withdrawals(activation)
            .is_empty()
    );
    harness.publish(root.task_id(), TaskState::Finished, None, true);
    assert!(
        harness
            .execution
            .result_context_withdrawals(activation)
            .is_empty()
    );
    let original = harness
        .execution
        .graph()
        .contexts()
        .map(|&context| {
            let owner = harness.execution.owner(context).unwrap();
            (
                context,
                (
                    owner.state(),
                    owner.establish_acknowledged(),
                    owner.is_released(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    harness.execution.begin_normal_drain();
    let facts = harness.execution.result_context_withdrawals(activation);
    assert_eq!(facts.len(), 2);
    for fact in facts {
        assert_eq!(fact.activation(), activation);
        assert_eq!(fact.root(), root);
        assert_ne!(
            fact.context().backend_process_id(),
            root.backend_process_id()
        );
        let owner = harness.execution.owner(fact.context()).unwrap();
        assert_eq!(
            (
                owner.state(),
                owner.establish_acknowledged(),
                owner.is_released()
            ),
            original[&fact.context()]
        );
        assert!(!owner.establish_acknowledged());
        assert!(!owner.is_released());
    }
    let foreign = novarocks_coordination_contract::AttemptActivationIdentity::from_parts(
        901,
        QueryExecutionId::new(QueryId::new(7, 8), AttemptId::new(1).unwrap()).unwrap(),
        1,
    );
    assert!(
        harness
            .execution
            .result_context_withdrawals(foreign)
            .is_empty()
    );
}

#[test]
fn normal_stand_down_without_finished_root_does_not_withdraw_result_contexts() {
    let mut harness = Harness::new(&[1], &[2], 512);
    let root = harness.execution.graph().root_identity();
    let activation = novarocks_coordination_contract::AttemptActivationIdentity::from_parts(
        902,
        root.query_execution_id(),
        1,
    );
    harness.execution.begin_normal_drain();
    assert!(
        harness
            .execution
            .result_context_withdrawals(activation)
            .is_empty()
    );
}

#[test]
fn exact_output_withdrawal_does_not_infer_an_unclosed_sibling_or_missing_edge() {
    let harness = Harness::new(&[0], &[1, 2], 512);
    let graph = harness.execution.graph();
    let producer = harness.identity(harness.stage_tasks(1)[0]);
    let receivers = harness
        .stage_tasks(2)
        .into_iter()
        .map(|id| harness.identity(id))
        .collect::<Vec<_>>();
    let mut demands = EdgeOpenTracker::from_graph(graph);
    assert_ne!(
        demands.normal_output_withdrawals(graph).get(&producer),
        Some(&true)
    );
    demands.note_normally_closed(receivers[0]);
    assert_ne!(
        demands.normal_output_withdrawals(graph).get(&producer),
        Some(&true)
    );
    demands.note_created(receivers[1]);
    assert_ne!(
        demands.normal_output_withdrawals(graph).get(&producer),
        Some(&true)
    );
    demands.note_normally_closed(receivers[1]);
    assert_eq!(
        demands.normal_output_withdrawals(graph).get(&producer),
        Some(&true)
    );
    assert_ne!(
        demands
            .normal_output_withdrawals(graph)
            .get(&graph.root_identity()),
        Some(&true)
    );
}

/// Original graph/dispatcher constructor with the actor gate installed before
/// the first turn. The returned recorder is only the Worker-facing transport.
pub(crate) fn manifest_original_gated_round_fixture(
    sink: Arc<RecordingSink>,
    transport: Arc<dyn TaskOperationSink>,
) -> (
    TaskRoundForTest,
    super::actor_gate::ActorGateOwner,
    crate::native::task_transport::TaskAckIntakeHandle,
    super::status_intake::StatusIntakeHandle,
    Arc<ManualClock>,
) {
    let schedule = chain_schedule(&[1, 2], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &backends(3), 64).unwrap();
    let wake = Arc::new(CountingWake::default());
    let (gated, owner, observer) = super::actor_gate::ActorGatedTaskOperationSink::pair(
        transport,
        Arc::clone(&wake) as Arc<dyn StatusIntakeWake>,
        NativeCompatibilityId::new([0x41; 32]),
    );
    let harness = Harness::from_graph_with_original_transport(graph, 4096, sink, Arc::new(gated));
    let status = harness.execution.intake().handle();
    let clock = Arc::clone(&harness.clock);
    let intake = crate::native::task_transport::TaskAckIntake::new(
        Arc::clone(&harness.wake) as Arc<dyn StatusIntakeWake>
    );
    let acks = intake.handle();
    let mut round = super::round::TaskRound::new(
        harness.execution,
        intake,
        Box::new(FakeEstablish),
        Arc::new(RecordingSubscriptions::default()) as Arc<dyn super::round::StatusSubscriptions>,
    )
    .observing(observer);
    round.seal_pumps();
    (round, owner, acks, status, clock)
}

/// Same admitted context, original released Create, and real Worker refusal.
/// The trace lets Manifest tests present the original Establish to QA as well.
pub(crate) fn pre_ready_original_create_refusal() -> (
    TaskRoundForTest,
    Arc<ManualClock>,
    TaskExecutionError,
    Vec<(OperationIntent, OperationAcknowledgement)>,
) {
    let harness = Harness::new(&[0], &[0], 64);
    let clock = Arc::clone(&harness.clock);
    let mut trace = Vec::new();
    let (mut round, _, acks, creates) =
        round_waiting_on_creates_with_establish_trace(harness, Some(&mut trace));
    assert!(!creates.is_empty());
    clock.advance(Duration::from_secs(3));
    acks.publish(OperationAcknowledgement::worker_receipt(
        creates[0].operation_id(),
        OperationKind::CreateTask,
        OperationOutcome::IdentityMismatch,
        AckPayload::None,
    ));
    let error = round.receive_without_dispatch().unwrap_err();
    assert!(matches!(&error, TaskExecutionError::OperationFailed {
        kind: OperationKind::CreateTask, operation_id, ..
    } if *operation_id == creates[0].operation_id()));
    (round, clock, error, trace)
}

/// Same released parent operation and explicit Unknown transport result as
/// production pipelining; no Applied Worker receipt is invented.
pub(crate) fn pre_ready_original_unknown_establish_create_refusal() -> (
    TaskRoundForTest,
    Arc<ManualClock>,
    TaskExecutionError,
    Vec<(OperationIntent, OperationAcknowledgement)>,
) {
    let harness = Harness::new(&[0], &[0], 64);
    let clock = Arc::clone(&harness.clock);
    let mut trace = Vec::new();
    let (mut round, _, acks, creates) =
        round_waiting_on_creates_with_establish_delivery(harness, Some(&mut trace), false);
    let context = *round.execution().graph().contexts().next().unwrap();
    assert_eq!(
        round
            .execution()
            .pre_ready_observation_remaining(context.backend_process_id()),
        Some(Duration::from_secs(10)),
        "Unknown keeps the original admission horizon, without claiming a Worker lease"
    );
    let create = &creates[0];
    clock.advance(Duration::from_secs(3));
    acks.publish(OperationAcknowledgement::worker_receipt(
        create.operation_id(),
        OperationKind::CreateTask,
        OperationOutcome::IdentityMismatch,
        AckPayload::None,
    ));
    let error = round.receive_without_dispatch().unwrap_err();
    assert!(matches!(&error, TaskExecutionError::OperationFailed {
        context: observed, operation_id, kind: OperationKind::CreateTask, ..
    } if *observed == context && *operation_id == create.operation_id()));
    (round, clock, error, trace)
}

#[test]
fn rejected_create_observation_keeps_submitted_unknown_establish_horizon() {
    let (mut round, clock, error, trace) = pre_ready_original_unknown_establish_create_refusal();
    let TaskExecutionError::OperationFailed {
        context,
        operation_id,
        ..
    } = error
    else {
        unreachable!("the original Create was refused by its Worker")
    };
    let establish = trace
        .iter()
        .find_map(|(intent, _)| match intent {
            OperationIntent::EstablishQueryContext(request) if request.context() == context => {
                Some(intent.clone())
            }
            _ => None,
        })
        .expect("the same original Establish was actually sent");
    assert_eq!(
        round
            .execution()
            .rejected_create_observation_remaining(context, operation_id),
        Some(Duration::from_secs(7)),
        "the actual submitted parent qualifies before its Applied acknowledgement"
    );
    // Unknown has settled the first transport generation. Release the same
    // original replay before its definitive ACK, as the real dispatcher does.
    round
        .turn()
        .expect("the original Establish replay is released");
    // A later original Applied acknowledgement cannot replace the frozen cut.
    round
        .execution_mut()
        .acknowledge(&establish_ack(&establish))
        .unwrap();
    assert_eq!(
        round
            .execution()
            .rejected_create_observation_remaining(context, operation_id),
        Some(Duration::from_secs(7)),
    );
    clock.advance(Duration::from_secs(8));
    assert_eq!(
        round
            .execution()
            .rejected_create_observation_remaining(context, operation_id),
        Some(Duration::ZERO),
        "late receipt never creates a fresh observation interval"
    );
}

#[test]
fn rejected_create_observation_keeps_exact_operation_context_and_effective_lease() {
    let (round, clock, error, _) = pre_ready_original_create_refusal();
    let TaskExecutionError::OperationFailed {
        context,
        operation_id,
        ..
    } = error
    else {
        unreachable!()
    };
    let execution = round.execution();
    assert_eq!(
        execution.pre_ready_observation_remaining(context.backend_process_id()),
        None
    );
    assert_eq!(
        execution.rejected_create_observation_remaining(context, operation_id),
        Some(Duration::from_secs(27)),
    );
    assert_eq!(
        execution.rejected_create_observation_remaining(context, TaskOperationId::new_v7()),
        None
    );
    let foreign = QueryContextRef::new(
        context.query_execution_id(),
        FrontendProcessId::new_v7(),
        context.backend_process_id(),
    );
    assert_eq!(
        execution.rejected_create_observation_remaining(foreign, operation_id),
        None
    );
    clock.advance(Duration::from_secs(28));
    assert_eq!(
        execution.rejected_create_observation_remaining(context, operation_id),
        Some(Duration::ZERO)
    );
}

#[test]
fn create_observation_requires_the_original_worker_identity_refusal() {
    for case in 0..5 {
        let harness = Harness::new(&[0], &[0], 64);
        let (mut round, _, acks, creates) = round_waiting_on_creates(harness);
        let context = *round.execution().graph().contexts().next().unwrap();
        let operation = creates[0].operation_id();
        let ack = match case {
            0 => OperationAcknowledgement::worker_receipt(
                TaskOperationId::new_v7(),
                OperationKind::CreateTask,
                OperationOutcome::IdentityMismatch,
                AckPayload::None,
            ),
            1 => OperationAcknowledgement::worker_receipt(
                operation,
                OperationKind::UpdateTask,
                OperationOutcome::IdentityMismatch,
                AckPayload::None,
            ),
            2 => OperationAcknowledgement::worker_receipt(
                operation,
                OperationKind::CreateTask,
                OperationOutcome::ContextConflict,
                AckPayload::None,
            ),
            3 => OperationAcknowledgement::transport_unknown(operation, OperationKind::CreateTask),
            _ => OperationAcknowledgement::local_refusal(
                operation,
                OperationKind::CreateTask,
                super::error::local_protocol_refusal(
                    OperationKind::CreateTask,
                    OperationOutcome::IdentityMismatch,
                ),
            ),
        };
        acks.publish(ack);
        let original_outcome = round.receive_without_dispatch();
        if case == 3 {
            assert!(
                original_outcome.is_ok(),
                "unknown keeps the original Create for replay"
            );
        } else {
            assert!(
                original_outcome.is_err(),
                "the original refusal is not swallowed"
            );
        }
        assert_eq!(
            round
                .execution()
                .rejected_create_observation_remaining(context, operation),
            None,
            "case {case}"
        );
    }
}
