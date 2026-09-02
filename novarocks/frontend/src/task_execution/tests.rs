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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution::exec::fragment::program::{FragmentContractVersion, FragmentSinkKind};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    AbortCause, CancelReason, CodecOwnedContent, ConfidentialContent, ContentFingerprint,
    CreateTaskReceipt, CredentialEpoch, CredentialLeaseId, CredentialUpdate, DispatchBudget,
    DispatchLane, LeaseReceipt, LeaseSequence, LeaseValidFor, MonotonicInstant, OperationKind,
    OperationOutcome, PhysicalFragmentPlan, PlanNodeId, QueryContextReceipt, QueryContextRef,
    QueryContextState, ReleaseOutcome, RenewSchedule, SplitAssignmentIntent, SplitSequence,
    StageState, TaskDomainUpdate, TaskIdentity, TaskOutputFacts, TaskState, TaskStatus,
    TaskStatusVersion, TerminationDetail, TransportBudget, UpdateTaskReceipt,
};
use novarocks_sql::plan_read::{
    DataPartition, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind, PartitionKind,
};
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};
use novarocks_types::{AttemptId, QueryId};

use super::clock::{ManualClock, TaskProtocolClock};
use super::context_owner::{
    ContextEstablishFacts, ContextEstablishSource, QueryContextOwner, ReleaseSettlement,
};
use super::dispatch::OperationDispatcher;
use super::error::{CapacityBound, TaskExecutionError};
use super::execution::QueryTaskExecution;
use super::graph::{
    FragmentPlanFacts, FragmentPlanSource, TaskGraph, TaskGraphInputs, build_task_graph,
};
use super::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationSink,
};
use super::remote_task::RemoteTaskState;
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
const SCAN_NODE: i32 = 5;

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
            initial_credential: CredentialUpdate::new(
                CredentialLeaseId::new(7),
                CredentialEpoch::FIRST,
                Arc::new(FakeSecret),
            ),
        })
    }
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
    fn submit(&self, batch: &DispatchBatch) {
        self.batches
            .lock()
            .expect("recording sink")
            .push((batch.lane(), batch.operations().to_vec()));
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

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
        let execution = QueryTaskExecution::new(
            graph,
            DispatchBudget::DEFAULT,
            TransportBudget::DEFAULT,
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
            AckPayload::Update(UpdateTaskReceipt::new(request.identity(), Vec::new()))
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
        let identity = self.identity(task_id);
        let version = self.statuses.entry(task_id).or_insert(1);
        *version += 1;
        let status = TaskStatus::try_new(
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

fn split_update(node: i32, sequence: u64, no_more: bool) -> TaskDomainUpdate {
    let sequence = SplitSequence::new(sequence).expect("a nonzero split sequence");
    TaskDomainUpdate::SplitAssignment(
        SplitAssignmentIntent::new(
            PlanNodeId::new(node).expect("a nonnegative plan node"),
            sequence,
            sequence,
            no_more,
            FakeContent::new(0xb1, 128),
        )
        .expect("a contiguous split batch"),
    )
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
        .create_ack(&leaf_create, OperationOutcome::RetryableTransportUnknown)
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
        .create_ack(&first, OperationOutcome::RetryableTransportUnknown)
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
    let error = harness
        .execution
        .enqueue_task_update(leaf, split_update(SCAN_NODE, 1, false))
        .expect_err("a reused split sequence is not a progression");
    assert!(matches!(error, TaskExecutionError::DomainRegression(_)));
}

#[test]
fn a_split_for_a_plan_node_the_descriptor_does_not_own_is_refused() {
    let mut harness = Harness::new(&[0], &[0], 512);
    let middle = harness.stage_tasks(2)[0];
    let error = harness
        .execution
        .enqueue_task_update(middle, split_update(SCAN_NODE, 1, false))
        .expect_err("a task with no scan node accepts no split");
    assert!(matches!(error, TaskExecutionError::DomainRegression(_)));
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
        .update_ack(&update, OperationOutcome::RetryableTransportUnknown)
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
    let mut owner = QueryContextOwner::new(context, 2);
    assert!(
        owner.release_intent().is_none(),
        "a release may not precede the establish acknowledgement"
    );

    let facts = FakeEstablish
        .facts_for(context)
        .expect("the fake source has facts");
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
        owner.release_intent().is_none(),
        "creates are not closed while one is outstanding"
    );
    owner.note_create_acknowledged();
    assert!(owner.creates_closed());
    assert!(
        owner.release_intent().is_none(),
        "closure alone is not drain"
    );

    owner.note_task_drained();
    owner.note_task_drained();
    owner.note_output_released();
    assert!(
        owner.release_intent().is_none(),
        "one output responsibility is still open"
    );
    owner.note_output_released();
    let release = owner
        .release_intent()
        .expect("closure and drain release the context");

    let not_ready = OperationAcknowledgement::new(
        release.operation_id(),
        OperationKind::ReleaseQueryContext,
        OperationOutcome::Accepted,
        AckPayload::Release {
            receipt: QueryContextReceipt::new(context, QueryContextState::Active),
            outcome: ReleaseOutcome::NotReady,
        },
    );
    assert_eq!(
        owner
            .on_release_ack(&not_ready)
            .expect("a not-ready answer settles"),
        ReleaseSettlement::NotReadyKeepRenewing
    );
    assert!(
        owner.must_keep_renewing(),
        "a not-ready release keeps the lease alive"
    );
    assert!(
        owner.release_intent().is_none(),
        "the identical release waits for local state to advance"
    );

    // Once local state advances the identical request goes out again.
    owner.note_output_released();
    let retry = owner
        .release_intent()
        .expect("progress re-releases the identical request");
    assert_eq!(retry.operation_id(), release.operation_id());

    owner
        .on_release_ack(&OperationAcknowledgement::new(
            retry.operation_id(),
            OperationKind::ReleaseQueryContext,
            OperationOutcome::Accepted,
            AckPayload::Release {
                receipt: QueryContextReceipt::new(context, QueryContextState::TerminalRetained),
                outcome: ReleaseOutcome::Released,
            },
        ))
        .expect("a released answer settles");
    assert!(!owner.must_keep_renewing());
    assert!(owner.is_released());
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
    let batch = harness
        .execution
        .abort_context(context, AbortCause::QueryFailed)
        .expect("an abort is admitted")
        .expect("the abort is released immediately");
    assert_eq!(batch.lane(), DispatchLane::Lifecycle);
    assert_eq!(batch.operations().len(), 1);
    assert_eq!(
        batch.operations()[0].kind(),
        OperationKind::AbortQueryContext
    );

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
fn an_operation_that_outlives_the_queue_residence_bound_is_reported_not_sent_late() {
    let leaves = (0..64).map(|_| 0_usize).collect::<Vec<_>>();
    let mut harness = Harness::new(&leaves, &[0], 512);
    let released = harness.pump();
    assert!(!released.is_empty());

    // Nothing is acknowledged, so the remaining creates stay queued past the
    // residence bound.
    harness
        .clock
        .advance(TransportBudget::DEFAULT.frontend_queue_residence());
    let report = harness
        .execution
        .pump(&FakeEstablish)
        .expect("pumping reports expiry");
    assert!(
        !report.expired.is_empty(),
        "a queue-resident operation is reported rather than sent late"
    );
    assert!(
        report
            .expired
            .iter()
            .all(|expired| expired.waited >= TransportBudget::DEFAULT.frontend_queue_residence())
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
    assert!(
        runner.take_observation_loss(),
        "overflow tells the runner to resubscribe with its cursors"
    );
    assert!(!runner.take_observation_loss());
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
        novarocks_execution::task_execution::FrontendAction::RetryExactRequest
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::Rejected {
            target: targets[0].clone(),
            reason: "watermark".to_owned(),
            detail: "gap".to_owned(),
        }),
        novarocks_execution::task_execution::FrontendAction::FailAttempt
    );
    assert_eq!(
        delivery_action(&SplitAssignmentDriverError::NoAdmittedTask { plan_node_id: 9 }),
        novarocks_execution::task_execution::FrontendAction::FailAttempt
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
