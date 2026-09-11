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

//! Frontend-private binding of an application-owned attempt schedule to one
//! immutable Native template.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::AdmissionEpochCapability;
use novarocks_execution_contract::{ExchangeEdgeId, QueryContextRef, TaskIdentity};
use novarocks_query_application::coordination::{
    AttemptSchedule, ScheduledFrozenUnits, ScheduledScanAssignment,
};
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
use novarocks_sql::plan_read::{FragmentId, PartitionKind};
use novarocks_sql::planning::query_execution::{SealedPreparationPlanId, SealedScanIdentity};
use novarocks_types::identity::{BackendProcessId, QueryExecutionId, StageId};
use novarocks_types::{NativeCompatibilityId, UniqueId};

use crate::common::backend_topology::{BackendTopologySnapshot, LiveBackendTarget};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};

use super::{
    PreparedDistributedNativeTemplate, RequestBoundNativeTemplate, derive_fragment_instance_id,
};

/// Exact process-local facts frozen with one backend in the dormant attempt.
/// Acquire/establish code consumes this value without consulting live
/// membership or reconstructing a compatibility decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestBackend {
    target: LiveBackendTarget,
    backend_idx: usize,
    process_id: BackendProcessId,
    endpoint: RuntimeEndpoint,
    admission_epoch_capability: AdmissionEpochCapability,
    native_compatibility_id: NativeCompatibilityId,
}

impl BoundManifestBackend {
    pub(crate) const fn target(&self) -> &LiveBackendTarget {
        &self.target
    }

    pub(crate) const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub(crate) const fn process_id(&self) -> BackendProcessId {
        self.process_id
    }

    pub(crate) const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub(crate) const fn admission_epoch_capability(&self) -> AdmissionEpochCapability {
        self.admission_epoch_capability
    }

    pub(crate) const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

/// One exact scheduled context and its complete frozen backend facts.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestContext {
    context: QueryContextRef,
    backend: BoundManifestBackend,
}

impl BoundManifestContext {
    pub(crate) const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub(crate) const fn backend(&self) -> &BoundManifestBackend {
        &self.backend
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestFrozenUnits {
    first_ordinal: usize,
    stride: NonZeroUsize,
    len: NonZeroUsize,
}

impl BoundManifestFrozenUnits {
    const fn from_scheduled(units: ScheduledFrozenUnits) -> Self {
        Self {
            first_ordinal: units.first_ordinal(),
            stride: units.stride(),
            len: units.len(),
        }
    }

    pub(crate) const fn first_ordinal(self) -> usize {
        self.first_ordinal
    }

    pub(crate) const fn stride(self) -> NonZeroUsize {
        self.stride
    }

    pub(crate) const fn len(self) -> NonZeroUsize {
        self.len
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundManifestScanAssignment {
    RuntimeSplits,
    WholeRelation,
    FrozenUnits(BoundManifestFrozenUnits),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestScanWork {
    scan: SealedScanIdentity,
    assignment: BoundManifestScanAssignment,
}

impl BoundManifestScanWork {
    pub(crate) const fn scan(&self) -> SealedScanIdentity {
        self.scan
    }

    pub(crate) const fn assignment(&self) -> BoundManifestScanAssignment {
        self.assignment
    }
}

/// Immutable Native binding for one exact application-scheduled Task.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestTask {
    identity: TaskIdentity,
    context: QueryContextRef,
    fragment_id: FragmentId,
    instance_index: usize,
    fragment_instance_id: UniqueId,
    scan_work: Box<[BoundManifestScanWork]>,
}

impl BoundManifestTask {
    pub(crate) const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub(crate) const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) const fn instance_index(&self) -> usize {
        self.instance_index
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn scan_work(&self) -> &[BoundManifestScanWork] {
        &self.scan_work
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestProducer {
    task: TaskIdentity,
    sender_ordinal: u32,
}

/// Frozen transport-neutral exchange distribution carried by the manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundManifestPartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

impl BoundManifestPartitionKind {
    const fn from_plan(kind: PartitionKind) -> Self {
        match kind {
            PartitionKind::Unpartitioned => Self::Unpartitioned,
            PartitionKind::Random => Self::Random,
            PartitionKind::Hash => Self::Hash,
        }
    }
}

impl BoundManifestProducer {
    pub(crate) const fn task(&self) -> TaskIdentity {
        self.task
    }

    pub(crate) const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestEdge {
    edge_id: ExchangeEdgeId,
    source_fragment_id: FragmentId,
    target_fragment_id: FragmentId,
    target_exchange_node_id: i32,
    partition_kind: BoundManifestPartitionKind,
    producers: Box<[BoundManifestProducer]>,
    destinations: Box<[TaskIdentity]>,
    sender_count: NonZeroU32,
}

impl BoundManifestEdge {
    pub(crate) const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub(crate) const fn source_fragment_id(&self) -> FragmentId {
        self.source_fragment_id
    }

    pub(crate) const fn target_fragment_id(&self) -> FragmentId {
        self.target_fragment_id
    }

    pub(crate) const fn target_exchange_node_id(&self) -> i32 {
        self.target_exchange_node_id
    }

    pub(crate) const fn partition_kind(&self) -> BoundManifestPartitionKind {
        self.partition_kind
    }

    pub(crate) fn producers(&self) -> &[BoundManifestProducer] {
        &self.producers
    }

    pub(crate) fn destinations(&self) -> &[TaskIdentity] {
        &self.destinations
    }

    pub(crate) const fn sender_count(&self) -> NonZeroU32 {
        self.sender_count
    }
}

/// Move-only projection of the one topology snapshot captured while preparing
/// a Native attempt.
///
/// Query Application obtains its eligible process identities from the dormant
/// owner that retains this value. Activation then consumes the same value to
/// bind endpoint, admission epoch, and compatibility facts into the Task
/// manifest. There is no API that accepts a later live snapshot at activation.
pub(crate) struct FrozenAttemptTopology {
    revision: u64,
    eligible_backends: Box<[BackendProcessId]>,
    backends: BTreeMap<BackendProcessId, BoundManifestBackend>,
}

impl std::fmt::Debug for FrozenAttemptTopology {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenAttemptTopology")
            .field("revision", &self.revision)
            .field("eligible_backends", &self.eligible_backends)
            .finish_non_exhaustive()
    }
}

impl FrozenAttemptTopology {
    pub(crate) fn capture(
        snapshot: BackendTopologySnapshot,
    ) -> Result<Self, DistributedQueryError> {
        let revision = snapshot.revision();
        let backends = validate_backend_snapshot(snapshot.targets())?;
        let eligible_backends = backends
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            revision,
            eligible_backends,
            backends,
        })
    }

    pub(crate) fn eligible_backends(&self) -> &[BackendProcessId] {
        &self.eligible_backends
    }

    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    fn into_parts(self) -> (u64, BTreeMap<BackendProcessId, BoundManifestBackend>) {
        (self.revision, self.backends)
    }
}

/// Move-only result of joining one immutable Native template to one exact
/// application-owned attempt schedule.
///
/// This value does not submit RPCs, open split sources, or obtain credentials.
/// It consumes and retains the exact static Native template so later encoding
/// cannot pair the validated manifest with another logical execution. The
/// template carries no Connector attempt access or planning lease.
pub(crate) struct TaskManifestBinding {
    execution: QueryExecutionId,
    topology_revision: u64,
    native_template: RequestBoundNativeTemplate,
    tasks: Box<[BoundManifestTask]>,
    contexts: Box<[BoundManifestContext]>,
    edges: Box<[BoundManifestEdge]>,
    root: TaskIdentity,
}

impl std::fmt::Debug for TaskManifestBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskManifestBinding")
            .field("execution", &self.execution)
            .field("topology_revision", &self.topology_revision)
            .field("task_count", &self.tasks.len())
            .field("context_count", &self.contexts.len())
            .field("edge_count", &self.edges.len())
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl TaskManifestBinding {
    pub(super) const fn request_bound_native(&self) -> &RequestBoundNativeTemplate {
        &self.native_template
    }

    pub(crate) fn bind(
        bound_template: RequestBoundNativeTemplate,
        schedule: &AttemptSchedule,
        topology: FrozenAttemptTopology,
    ) -> Result<Self, DistributedQueryError> {
        let RequestBoundNativeTemplate { execution, .. } = bound_template;
        let facts = ScheduleFacts::from_schedule(schedule);
        let (topology_revision, backends) = topology.into_parts();
        Self::bind_facts(
            bound_template,
            execution,
            facts,
            topology_revision,
            backends,
        )
    }

    fn bind_facts(
        bound_template: RequestBoundNativeTemplate,
        expected_execution: QueryExecutionId,
        schedule: ScheduleFacts,
        topology_revision: u64,
        endpoint_by_process: BTreeMap<BackendProcessId, BoundManifestBackend>,
    ) -> Result<Self, DistributedQueryError> {
        validate_schedule_execution(expected_execution, schedule.execution)?;
        let prepared = PreparedManifestFacts::from_template(&bound_template.template)?;
        let expected_fragments = &prepared.fragments;

        let mut scheduled_fragments = BTreeSet::new();
        let mut stage_ids = BTreeSet::new();
        let mut task_ids = BTreeSet::new();
        let mut task_identities = BTreeSet::new();
        let mut tasks_by_fragment = BTreeMap::<FragmentId, Vec<TaskIdentity>>::new();
        let mut task_fragment = BTreeMap::<TaskIdentity, FragmentId>::new();
        let mut expected_contexts = BTreeSet::new();
        let mut context_by_backend = BTreeMap::<BackendProcessId, QueryContextRef>::new();
        let mut fragment_tasks = BTreeMap::<FragmentId, Vec<ProjectedTask>>::new();
        let mut fragment_scan_assignments = BTreeMap::<
            (FragmentId, SealedScanIdentity),
            BTreeMap<usize, ProjectedScanAssignment>,
        >::new();
        let mut bound_tasks = Vec::new();
        let mut fragment_instance_ids = BTreeSet::new();

        for fragment in schedule.fragments {
            if !scheduled_fragments.insert(fragment.fragment_id) {
                return Err(contract_error(format!(
                    "attempt manifest repeats scheduled fragment {}",
                    fragment.fragment_id
                )));
            }
            if !stage_ids.insert(fragment.stage_id) {
                return Err(contract_error(format!(
                    "attempt manifest stage identity {} is shared by multiple fragments",
                    fragment.stage_id
                )));
            }
            if fragment.tasks.is_empty() {
                return Err(contract_error(format!(
                    "attempt manifest fragment {} has no task",
                    fragment.fragment_id
                )));
            }
            let mut instance_indices = BTreeSet::new();
            for task in &fragment.tasks {
                validate_task_identity(
                    expected_execution,
                    fragment.stage_id,
                    task.identity,
                    task.context,
                )?;
                if !instance_indices.insert(task.instance_index) {
                    return Err(contract_error(format!(
                        "attempt manifest fragment {} repeats instance index {}",
                        fragment.fragment_id, task.instance_index
                    )));
                }
                if !task_ids.insert(task.identity.task_id()) {
                    return Err(contract_error(format!(
                        "attempt manifest repeats task id {}",
                        task.identity.task_id()
                    )));
                }
                if !task_identities.insert(task.identity) {
                    return Err(contract_error(format!(
                        "attempt manifest repeats task identity {}",
                        task.identity
                    )));
                }
                let backend = task.identity.backend_process_id();
                if !endpoint_by_process.contains_key(&backend) {
                    return Err(contract_error(format!(
                        "attempt manifest task {} names backend process absent from the frozen endpoint snapshot",
                        task.identity
                    )));
                }
                if let Some(previous) = context_by_backend.insert(backend, task.context)
                    && previous != task.context
                {
                    return Err(contract_error(format!(
                        "attempt manifest backend process {backend} is assigned multiple query contexts"
                    )));
                }
                expected_contexts.insert(task.context);
                tasks_by_fragment
                    .entry(fragment.fragment_id)
                    .or_default()
                    .push(task.identity);
                task_fragment.insert(task.identity, fragment.fragment_id);
                let fragment_instance_id =
                    derive_and_record_fragment_instance_id(&mut fragment_instance_ids, || {
                        derive_fragment_instance_id(
                            expected_execution,
                            fragment.fragment_id,
                            task.instance_index,
                        )
                    })?;
                let mut task_scans = BTreeSet::new();
                let mut bound_scan_work = Vec::new();
                for scan_work in &task.scan_work {
                    let key = (fragment.fragment_id, scan_work.scan);
                    if !task_scans.insert(scan_work.scan) {
                        return Err(contract_error(format!(
                            "attempt manifest task {} repeats scan node {}",
                            task.identity,
                            scan_work.scan.node_id()
                        )));
                    }
                    let expected_identity = prepared
                        .scans_by_node
                        .get(&(fragment.fragment_id, scan_work.scan.node_id()))
                        .copied()
                        .ok_or_else(|| {
                            contract_error(format!(
                                "attempt manifest names scan node {} outside fragment {}",
                                scan_work.scan.node_id(),
                                fragment.fragment_id
                            ))
                        })?;
                    if expected_identity != scan_work.scan {
                        return Err(contract_error(format!(
                            "attempt manifest scan node {} belongs to another sealed preparation artifact",
                            scan_work.scan.node_id()
                        )));
                    }
                    if fragment_scan_assignments
                        .entry(key)
                        .or_default()
                        .insert(task.instance_index, scan_work.assignment)
                        .is_some()
                    {
                        return Err(contract_error(format!(
                            "attempt manifest repeats scan assignment for fragment {} node {} instance {}",
                            fragment.fragment_id,
                            scan_work.scan.node_id(),
                            task.instance_index
                        )));
                    }
                    bound_scan_work.push(BoundManifestScanWork {
                        scan: scan_work.scan,
                        assignment: scan_work.assignment.into_bound(),
                    });
                }
                bound_tasks.push(BoundManifestTask {
                    identity: task.identity,
                    context: task.context,
                    fragment_id: fragment.fragment_id,
                    instance_index: task.instance_index,
                    fragment_instance_id,
                    scan_work: bound_scan_work.into_boxed_slice(),
                });
            }
            let expected_indices = (0..fragment.tasks.len()).collect::<BTreeSet<_>>();
            if instance_indices != expected_indices {
                return Err(contract_error(format!(
                    "attempt manifest fragment {} instance indices are not the exact range 0..{}",
                    fragment.fragment_id,
                    fragment.tasks.len()
                )));
            }
            fragment_tasks.insert(fragment.fragment_id, fragment.tasks);
        }
        if &scheduled_fragments != expected_fragments {
            return Err(set_mismatch(
                "scheduled fragment",
                expected_fragments,
                &scheduled_fragments,
            ));
        }

        let actual_contexts = schedule.contexts.iter().copied().collect::<BTreeSet<_>>();
        if actual_contexts.len() != schedule.contexts.len() {
            return Err(contract_error("attempt manifest repeats a query context"));
        }
        if actual_contexts != expected_contexts {
            return Err(contract_error(format!(
                "attempt manifest context set mismatch: expected={expected_contexts:?} actual={actual_contexts:?}"
            )));
        }
        let mut bound_contexts = Vec::with_capacity(schedule.contexts.len());
        for context in schedule.contexts {
            if context.query_execution_id() != expected_execution {
                return Err(contract_error(format!(
                    "attempt manifest context {context} belongs to another execution"
                )));
            }
            let backend = endpoint_by_process
                .get(&context.backend_process_id())
                .cloned()
                .ok_or_else(|| {
                    contract_error(format!(
                        "attempt manifest context {context} names a backend absent from the frozen endpoint snapshot"
                    ))
                })?;
            bound_contexts.push(BoundManifestContext { context, backend });
        }

        let root_fragment = task_fragment.get(&schedule.root).copied().ok_or_else(|| {
            contract_error("attempt manifest root is absent from the scheduled task set")
        })?;
        if root_fragment != prepared.execution_anchor {
            return Err(contract_error(format!(
                "attempt manifest root belongs to fragment {root_fragment}, expected execution anchor {}",
                prepared.execution_anchor
            )));
        }

        validate_scan_assignments(&prepared, &fragment_tasks, &fragment_scan_assignments)?;
        let bound_edges = validate_edges(
            &prepared,
            schedule.edges,
            &tasks_by_fragment,
            &task_identities,
        )?;

        Ok(Self {
            execution: expected_execution,
            topology_revision,
            native_template: bound_template,
            tasks: bound_tasks.into_boxed_slice(),
            contexts: bound_contexts.into_boxed_slice(),
            edges: bound_edges.into_boxed_slice(),
            root: schedule.root,
        })
    }

    pub(crate) const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub(crate) const fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    pub(crate) fn tasks(&self) -> &[BoundManifestTask] {
        &self.tasks
    }

    pub(crate) fn contexts(&self) -> &[BoundManifestContext] {
        &self.contexts
    }

    pub(crate) fn edges(&self) -> &[BoundManifestEdge] {
        &self.edges
    }

    pub(crate) const fn root(&self) -> TaskIdentity {
        self.root
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectedScanAssignment {
    RuntimeSplits,
    WholeRelation,
    FrozenUnits(BoundManifestFrozenUnits),
}

impl ProjectedScanAssignment {
    fn into_bound(self) -> BoundManifestScanAssignment {
        match self {
            Self::RuntimeSplits => BoundManifestScanAssignment::RuntimeSplits,
            Self::WholeRelation => BoundManifestScanAssignment::WholeRelation,
            Self::FrozenUnits(units) => BoundManifestScanAssignment::FrozenUnits(units),
        }
    }
}

#[derive(Debug)]
struct ProjectedScanWork {
    scan: SealedScanIdentity,
    assignment: ProjectedScanAssignment,
}

#[derive(Debug)]
struct ProjectedTask {
    identity: TaskIdentity,
    context: QueryContextRef,
    instance_index: usize,
    scan_work: Vec<ProjectedScanWork>,
}

#[derive(Debug)]
struct ProjectedFragment {
    fragment_id: FragmentId,
    stage_id: StageId,
    tasks: Vec<ProjectedTask>,
}

#[derive(Debug)]
struct ProjectedEdge {
    edge_id: ExchangeEdgeId,
    source_fragment_id: FragmentId,
    target_fragment_id: FragmentId,
    target_exchange_node_id: i32,
    producers: Vec<(TaskIdentity, u32)>,
    destinations: Vec<TaskIdentity>,
    sender_count: NonZeroU32,
}

#[derive(Debug)]
struct ScheduleFacts {
    execution: QueryExecutionId,
    fragments: Vec<ProjectedFragment>,
    edges: Vec<ProjectedEdge>,
    contexts: Vec<QueryContextRef>,
    root: TaskIdentity,
}

type PreparedEdgeKey = (FragmentId, FragmentId, i32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedScanSource {
    RuntimeSplits,
    WholeRelation,
    FrozenUnits { unit_count: usize },
}

/// Minimal immutable Native facts the schedule is allowed to bind against.
///
/// This projection is constructed from the retained preparation/template
/// owners. Tests can exercise the validation core without creating a second
/// scheduling authority or making `AttemptSchedule` forgeable.
#[derive(Debug)]
struct PreparedManifestFacts {
    fragments: BTreeSet<FragmentId>,
    execution_anchor: FragmentId,
    scans: BTreeMap<(FragmentId, SealedScanIdentity), PreparedScanSource>,
    scans_by_node: BTreeMap<(FragmentId, i32), SealedScanIdentity>,
    edges: BTreeMap<PreparedEdgeKey, BoundManifestPartitionKind>,
}

impl PreparedManifestFacts {
    fn from_template(
        template: &PreparedDistributedNativeTemplate,
    ) -> Result<Self, DistributedQueryError> {
        let prepared = template.prepared.as_ref();
        validate_native_template_plan_seal(template.plan_seal(), prepared.plan_seal())?;
        let fragments = prepared.fragment_ids();
        let native_fragments = template
            .native_template
            .fragments_in_id_order()
            .map(|(fragment_id, _)| fragment_id)
            .collect::<BTreeSet<_>>();
        if native_fragments != fragments {
            return Err(set_mismatch(
                "Native template fragment",
                &fragments,
                &native_fragments,
            ));
        }

        let view = prepared.scheduling_view();
        let mut scans = BTreeMap::new();
        let mut scans_by_node = BTreeMap::new();
        for (fragment_id, scan) in prepared.sealed_scan_identities() {
            let source = match view.typed_connector_work_source(fragment_id, scan.node_id()) {
                Some(ConnectorReadWorkSource::RuntimeSplits) => PreparedScanSource::RuntimeSplits,
                Some(ConnectorReadWorkSource::WholeRelation) => PreparedScanSource::WholeRelation,
                None => PreparedScanSource::FrozenUnits {
                    unit_count: view
                        .scan_ranges(fragment_id, scan.node_id())
                        .ok_or_else(|| {
                            contract_error(format!(
                                "prepared Native scan node {} has no immutable work source",
                                scan.node_id()
                            ))
                        })?
                        .len(),
                },
            };
            if scans.insert((fragment_id, scan), source).is_some()
                || scans_by_node
                    .insert((fragment_id, scan.node_id()), scan)
                    .is_some()
            {
                return Err(contract_error(format!(
                    "prepared Native projection repeats scan occurrence fragment_id={fragment_id} node_id={}",
                    scan.node_id()
                )));
            }
        }

        let mut edges = BTreeMap::new();
        for edge in view.edges() {
            let key = (
                edge.source_fragment_id,
                edge.target_fragment_id,
                edge.target_exchange_node_id,
            );
            if edges
                .insert(
                    key,
                    BoundManifestPartitionKind::from_plan(edge.output_partition.kind),
                )
                .is_some()
            {
                return Err(contract_error(
                    "prepared Native projection repeats an inter-fragment edge",
                ));
            }
        }

        Ok(Self {
            fragments,
            execution_anchor: view.execution_anchor(),
            scans,
            scans_by_node,
            edges,
        })
    }
}

impl ScheduleFacts {
    fn from_schedule(schedule: &AttemptSchedule) -> Self {
        Self {
            execution: schedule.execution(),
            fragments: schedule
                .fragments()
                .iter()
                .map(|fragment| ProjectedFragment {
                    fragment_id: fragment.fragment_id(),
                    stage_id: fragment.stage_id(),
                    tasks: fragment
                        .tasks()
                        .iter()
                        .map(|task| ProjectedTask {
                            identity: task.identity(),
                            context: task.context(),
                            instance_index: task.instance_index(),
                            scan_work: task
                                .scan_work()
                                .iter()
                                .map(|work| ProjectedScanWork {
                                    scan: work.scan(),
                                    assignment: match work.assignment() {
                                        ScheduledScanAssignment::RuntimeSplits => {
                                            ProjectedScanAssignment::RuntimeSplits
                                        }
                                        ScheduledScanAssignment::WholeRelation => {
                                            ProjectedScanAssignment::WholeRelation
                                        }
                                        ScheduledScanAssignment::FrozenUnits { units } => {
                                            ProjectedScanAssignment::FrozenUnits(
                                                BoundManifestFrozenUnits::from_scheduled(*units),
                                            )
                                        }
                                    },
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
            edges: schedule
                .edges()
                .iter()
                .map(|edge| ProjectedEdge {
                    edge_id: edge.edge_id(),
                    source_fragment_id: edge.source_fragment_id(),
                    target_fragment_id: edge.target_fragment_id(),
                    target_exchange_node_id: edge.target_exchange_node_id(),
                    producers: edge
                        .producers()
                        .iter()
                        .map(|producer| (producer.task(), producer.sender_ordinal()))
                        .collect(),
                    destinations: edge.destinations().to_vec(),
                    sender_count: edge.sender_count(),
                })
                .collect(),
            contexts: schedule.contexts().to_vec(),
            root: schedule.root(),
        }
    }
}

fn validate_backend_snapshot(
    snapshot: &[LiveBackendTarget],
) -> Result<BTreeMap<BackendProcessId, BoundManifestBackend>, DistributedQueryError> {
    if snapshot.is_empty() {
        return Err(contract_error(
            "attempt manifest requires a nonempty frozen endpoint snapshot",
        ));
    }
    let mut by_process = BTreeMap::new();
    let mut endpoint_owners = BTreeMap::new();
    for target in snapshot {
        let process = target.process_id().map_err(|error| {
            contract_error(format!(
                "attempt manifest frozen endpoint snapshot has invalid process identity: {error}"
            ))
        })?;
        let endpoint = target.endpoint().map_err(|error| {
            contract_error(format!(
                "attempt manifest frozen endpoint snapshot has invalid endpoint: {error}"
            ))
        })?;
        let native_compatibility_id = target
            .descriptor()
            .native_compatibility_id()
            .map_err(|error| {
                contract_error(format!(
                    "attempt manifest frozen endpoint snapshot has invalid compatibility identity: {error}"
                ))
            })?;
        let backend = BoundManifestBackend {
            target: target.clone(),
            backend_idx: target.backend_idx(),
            process_id: process,
            endpoint: endpoint.clone(),
            admission_epoch_capability: target.admission_epoch_capability(),
            native_compatibility_id,
        };
        if by_process.insert(process, backend).is_some() {
            return Err(contract_error(format!(
                "attempt manifest frozen endpoint snapshot repeats backend process {process}"
            )));
        }
        if let Some(previous) = endpoint_owners.insert(endpoint.clone(), process) {
            return Err(contract_error(format!(
                "attempt manifest frozen endpoint {endpoint} is owned by both {previous} and {process}"
            )));
        }
    }
    Ok(by_process)
}

fn validate_native_template_plan_seal(
    template: SealedPreparationPlanId,
    request: SealedPreparationPlanId,
) -> Result<(), DistributedQueryError> {
    if template != request {
        return Err(contract_error(
            "attempt manifest Native template belongs to another sealed preparation plan",
        ));
    }
    Ok(())
}

fn validate_task_identity(
    execution: QueryExecutionId,
    stage_id: StageId,
    identity: TaskIdentity,
    context: QueryContextRef,
) -> Result<(), DistributedQueryError> {
    if identity.query_execution_id() != execution {
        return Err(contract_error(format!(
            "attempt manifest task {identity} belongs to another execution"
        )));
    }
    if identity.stage_id() != stage_id {
        return Err(contract_error(format!(
            "attempt manifest task {identity} differs from its fragment stage {stage_id}"
        )));
    }
    identity.verify_query_context(context).map_err(|error| {
        contract_error(format!(
            "attempt manifest task {identity} differs from context {context}: {error}"
        ))
    })
}

fn derive_and_record_fragment_instance_id(
    seen: &mut BTreeSet<UniqueId>,
    derive: impl FnOnce() -> Result<UniqueId, DistributedQueryError>,
) -> Result<UniqueId, DistributedQueryError> {
    let fragment_instance_id = derive()?;
    if !seen.insert(fragment_instance_id) {
        return Err(contract_error(format!(
            "attempt manifest fragment-instance identity collision at {fragment_instance_id}"
        )));
    }
    Ok(fragment_instance_id)
}

fn validate_scan_assignments(
    prepared: &PreparedManifestFacts,
    tasks_by_fragment: &BTreeMap<FragmentId, Vec<ProjectedTask>>,
    actual: &BTreeMap<(FragmentId, SealedScanIdentity), BTreeMap<usize, ProjectedScanAssignment>>,
) -> Result<(), DistributedQueryError> {
    let expected = prepared.scans.keys().copied().collect::<BTreeSet<_>>();
    let expected_assignments = prepared
        .scans
        .iter()
        .filter_map(|(&scan, source)| {
            (!matches!(source, PreparedScanSource::FrozenUnits { unit_count: 0 })).then_some(scan)
        })
        .collect::<BTreeSet<_>>();
    let actual_keys = actual.keys().copied().collect::<BTreeSet<_>>();
    if expected_assignments != actual_keys {
        return Err(contract_error(format!(
            "attempt manifest scan assignment set mismatch: expected={expected_assignments:?} actual={actual_keys:?}"
        )));
    }
    let empty_assignments = BTreeMap::new();
    for &(fragment_id, scan) in &expected {
        let tasks = tasks_by_fragment.get(&fragment_id).ok_or_else(|| {
            contract_error(format!(
                "attempt manifest scan node {} has no scheduled fragment {}",
                scan.node_id(),
                fragment_id
            ))
        })?;
        let assignments = actual
            .get(&(fragment_id, scan))
            .unwrap_or(&empty_assignments);
        match prepared.scans[&(fragment_id, scan)] {
            PreparedScanSource::RuntimeSplits => {
                if assignments.len() != tasks.len()
                    || assignments
                        .values()
                        .any(|assignment| *assignment != ProjectedScanAssignment::RuntimeSplits)
                {
                    return Err(contract_error(format!(
                        "attempt manifest runtime-split scan node {} must be assigned to every task",
                        scan.node_id()
                    )));
                }
            }
            PreparedScanSource::WholeRelation => {
                if tasks.len() != 1
                    || assignments.len() != 1
                    || assignments.get(&0) != Some(&ProjectedScanAssignment::WholeRelation)
                {
                    return Err(contract_error(format!(
                        "attempt manifest whole-relation scan node {} must have exactly one task assignment",
                        scan.node_id()
                    )));
                }
            }
            PreparedScanSource::FrozenUnits { unit_count } => {
                validate_frozen_unit_cover(scan, tasks.len(), unit_count, assignments)?;
            }
        }
    }
    Ok(())
}

fn validate_frozen_unit_cover(
    scan: SealedScanIdentity,
    task_count: usize,
    unit_count: usize,
    assignments: &BTreeMap<usize, ProjectedScanAssignment>,
) -> Result<(), DistributedQueryError> {
    if unit_count == 0 {
        if assignments.is_empty() {
            return Ok(());
        }
        return Err(contract_error(format!(
            "attempt manifest empty frozen scan node {} must have no task assignment",
            scan.node_id()
        )));
    }
    let stride = NonZeroUsize::new(task_count)
        .ok_or_else(|| contract_error("attempt manifest frozen scan has no scheduled task"))?;
    let assigned_tasks = unit_count.min(task_count);
    if assignments.len() != assigned_tasks {
        return Err(contract_error(format!(
            "attempt manifest frozen scan node {} assignment count {} differs from expected {assigned_tasks}",
            scan.node_id(),
            assignments.len()
        )));
    }
    for instance_index in 0..task_count {
        let expected = if instance_index < unit_count {
            let len = 1 + (unit_count - 1 - instance_index) / task_count;
            Some((instance_index, stride, NonZeroUsize::new(len).unwrap()))
        } else {
            None
        };
        match (assignments.get(&instance_index), expected) {
            (Some(ProjectedScanAssignment::FrozenUnits(actual)), Some(expected))
                if (actual.first_ordinal(), actual.stride(), actual.len()) == expected => {}
            (None, None) => {}
            _ => {
                return Err(contract_error(format!(
                    "attempt manifest frozen scan node {} does not exactly cover immutable units at task instance {instance_index}",
                    scan.node_id()
                )));
            }
        }
    }
    Ok(())
}

fn validate_edges(
    prepared: &PreparedManifestFacts,
    edges: Vec<ProjectedEdge>,
    tasks_by_fragment: &BTreeMap<FragmentId, Vec<TaskIdentity>>,
    task_identities: &BTreeSet<TaskIdentity>,
) -> Result<Vec<BoundManifestEdge>, DistributedQueryError> {
    let expected = prepared.edges.keys().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut edge_ids = BTreeSet::new();
    for edge in &edges {
        let key = (
            edge.source_fragment_id,
            edge.target_fragment_id,
            edge.target_exchange_node_id,
        );
        if !actual.insert(key) {
            return Err(contract_error(format!(
                "attempt manifest repeats edge {} -> {} at exchange node {}",
                edge.source_fragment_id, edge.target_fragment_id, edge.target_exchange_node_id
            )));
        }
        if !edge_ids.insert(edge.edge_id) {
            return Err(contract_error(format!(
                "attempt manifest repeats exchange edge id {}",
                edge.edge_id
            )));
        }
    }
    if actual != expected {
        return Err(contract_error(format!(
            "attempt manifest edge set mismatch: expected={expected:?} actual={actual:?}"
        )));
    }

    let mut by_exchange = BTreeMap::<(FragmentId, i32), Vec<&ProjectedEdge>>::new();
    for edge in &edges {
        by_exchange
            .entry((edge.target_fragment_id, edge.target_exchange_node_id))
            .or_default()
            .push(edge);
    }
    for (&(target_fragment, exchange_node), feeding_edges) in &by_exchange {
        let destinations = tasks_by_fragment.get(&target_fragment).ok_or_else(|| {
            contract_error(format!(
                "attempt manifest exchange node {exchange_node} names missing target fragment {target_fragment}"
            ))
        })?;
        let mut expected_producers = BTreeSet::new();
        for edge in feeding_edges {
            let source_tasks =
                tasks_by_fragment
                    .get(&edge.source_fragment_id)
                    .ok_or_else(|| {
                        contract_error(format!(
                            "attempt manifest edge names missing source fragment {}",
                            edge.source_fragment_id
                        ))
                    })?;
            expected_producers.extend(source_tasks.iter().copied());
            let edge_producers = edge
                .producers
                .iter()
                .map(|(task, _)| *task)
                .collect::<BTreeSet<_>>();
            if edge_producers.len() != edge.producers.len()
                || edge_producers != source_tasks.iter().copied().collect()
            {
                return Err(contract_error(format!(
                    "attempt manifest edge {} producers do not exactly equal source fragment tasks",
                    edge.edge_id
                )));
            }
            let edge_destinations = edge.destinations.iter().copied().collect::<BTreeSet<_>>();
            if edge_destinations.len() != edge.destinations.len()
                || edge_destinations != destinations.iter().copied().collect()
            {
                return Err(contract_error(format!(
                    "attempt manifest edge {} destinations do not exactly equal target fragment tasks",
                    edge.edge_id
                )));
            }
            if edge
                .producers
                .iter()
                .any(|(task, _)| !task_identities.contains(task))
                || edge
                    .destinations
                    .iter()
                    .any(|task| !task_identities.contains(task))
            {
                return Err(contract_error(format!(
                    "attempt manifest edge {} contains a foreign task identity",
                    edge.edge_id
                )));
            }
        }
        let expected_sender_count = u32::try_from(expected_producers.len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                contract_error(format!(
                    "attempt manifest exchange node {exchange_node} has an invalid producer count"
                ))
            })?;
        let mut ordinals = BTreeSet::new();
        for edge in feeding_edges {
            if edge.sender_count != expected_sender_count {
                return Err(contract_error(format!(
                    "attempt manifest edge {} sender count {} differs from exchange union count {}",
                    edge.edge_id, edge.sender_count, expected_sender_count
                )));
            }
            for &(_, ordinal) in &edge.producers {
                if !ordinals.insert(ordinal) {
                    return Err(contract_error(format!(
                        "attempt manifest exchange node {exchange_node} repeats sender ordinal {ordinal}"
                    )));
                }
            }
        }
        let expected_ordinals = (0..expected_sender_count.get()).collect::<BTreeSet<_>>();
        if ordinals != expected_ordinals {
            return Err(contract_error(format!(
                "attempt manifest exchange node {exchange_node} sender ordinals are not the exact range 0..{}",
                expected_sender_count.get()
            )));
        }
    }

    Ok(edges
        .into_iter()
        .map(|edge| {
            let key = (
                edge.source_fragment_id,
                edge.target_fragment_id,
                edge.target_exchange_node_id,
            );
            BoundManifestEdge {
                edge_id: edge.edge_id,
                source_fragment_id: edge.source_fragment_id,
                target_fragment_id: edge.target_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                partition_kind: prepared.edges[&key],
                producers: edge
                    .producers
                    .into_iter()
                    .map(|(task, sender_ordinal)| BoundManifestProducer {
                        task,
                        sender_ordinal,
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                destinations: edge.destinations.into_boxed_slice(),
                sender_count: edge.sender_count,
            }
        })
        .collect())
}

fn set_mismatch(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> DistributedQueryError {
    contract_error(format!(
        "{label} set mismatch: expected={expected:?} actual={actual:?}"
    ))
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn validate_schedule_execution(
    expected: QueryExecutionId,
    actual: QueryExecutionId,
) -> Result<(), DistributedQueryError> {
    if expected == actual {
        Ok(())
    } else {
        Err(contract_error(
            "attempt manifest schedule execution differs from the dormant attempt",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use novarocks_execution::task_execution::AdmissionEpochCapability;
    use novarocks_execution_contract::{ExchangeEdgeId, QueryContextRef, TaskIdentity};
    use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
    use novarocks_proto_codec::membership::BackendProcessDescriptor;
    use novarocks_sql::planning::query_execution::{SealedPreparationPlan, SealedScanIdentity};
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{NativeCompatibilityId, UniqueId};

    use super::{
        BoundManifestFrozenUnits, BoundManifestPartitionKind, FrozenAttemptTopology,
        PreparedManifestFacts, PreparedScanSource, ProjectedEdge, ProjectedScanAssignment,
        ProjectedTask, derive_and_record_fragment_instance_id, validate_backend_snapshot,
        validate_edges, validate_frozen_unit_cover, validate_native_template_plan_seal,
        validate_scan_assignments, validate_schedule_execution, validate_task_identity,
    };
    use crate::common::backend_topology::{BackendTopologySnapshot, LiveBackendTarget};

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(11, 17),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query")
    }

    fn task(
        execution: QueryExecutionId,
        stage: u32,
        id: u32,
        backend: BackendProcessId,
    ) -> (TaskIdentity, QueryContextRef) {
        let identity = TaskIdentity::new(
            execution,
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(id).expect("nonzero task"),
            backend,
        );
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        (identity, context)
    }

    fn scan_identity() -> SealedScanIdentity {
        let sealed = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).expect("scan fixture"),
        );
        sealed
            .scan_contracts()
            .expect("scan contracts")
            .into_iter()
            .next()
            .expect("one scan")
            .identity()
    }

    #[test]
    fn native_template_rejects_request_description_from_another_plan_seal() {
        let expected = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).expect("scan fixture"),
        );
        let foreign = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).expect("foreign scan fixture"),
        );

        validate_native_template_plan_seal(expected.id(), expected.id())
            .expect("the exact request seal is accepted");
        let error = validate_native_template_plan_seal(expected.id(), foreign.id())
            .expect_err("an isomorphic request plan must not cross the seal");
        assert!(error.message().contains("another sealed preparation plan"));
    }

    #[test]
    fn manifest_rejects_a_schedule_for_another_execution() {
        let expected = execution();
        let foreign = QueryExecutionId::new(
            expected.query_id(),
            AttemptId::new(2).expect("nonzero foreign attempt"),
        )
        .expect("valid foreign execution");

        validate_schedule_execution(expected, expected).expect("exact execution is accepted");
        let error = validate_schedule_execution(expected, foreign)
            .expect_err("a schedule for another attempt must fail closed");
        assert!(error.message().contains("differs from the dormant attempt"));
    }

    fn prepared_scan(
        fragment_id: u32,
        scan: SealedScanIdentity,
        source: PreparedScanSource,
    ) -> PreparedManifestFacts {
        PreparedManifestFacts {
            fragments: [fragment_id].into_iter().collect(),
            execution_anchor: fragment_id,
            scans: [((fragment_id, scan), source)].into_iter().collect(),
            scans_by_node: [((fragment_id, scan.node_id()), scan)]
                .into_iter()
                .collect(),
            edges: Default::default(),
        }
    }

    fn projected_tasks(count: usize) -> Vec<ProjectedTask> {
        let execution = execution();
        (0..count)
            .map(|index| {
                let backend = BackendProcessId::new_v7();
                let (identity, context) = task(execution, 1, index as u32 + 1, backend);
                ProjectedTask {
                    identity,
                    context,
                    instance_index: index,
                    scan_work: Vec::new(),
                }
            })
            .collect()
    }

    fn frozen(first: usize, stride: usize, len: usize) -> ProjectedScanAssignment {
        ProjectedScanAssignment::FrozenUnits(BoundManifestFrozenUnits {
            first_ordinal: first,
            stride: NonZeroUsize::new(stride).expect("nonzero stride"),
            len: NonZeroUsize::new(len).expect("nonzero len"),
        })
    }

    fn live_target(ordinal: usize, process: BackendProcessId, port: u16) -> LiveBackendTarget {
        live_target_with_epoch(ordinal, process, port, 0x61)
    }

    fn live_target_with_epoch(
        ordinal: usize,
        process: BackendProcessId,
        port: u16,
        epoch: u8,
    ) -> LiveBackendTarget {
        let descriptor = BackendProcessDescriptor::new(
            process,
            QueryControlEndpoint::new("127.0.0.1", port).expect("valid endpoint"),
            "test-deployment",
            "test-build",
            NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("valid descriptor");
        LiveBackendTarget::new(
            ordinal,
            descriptor,
            AdmissionEpochCapability::try_from_bytes([epoch; 16]).expect("nonzero admission epoch"),
        )
    }

    #[test]
    fn frozen_attempt_topology_retains_the_exact_admission_epoch_snapshot() {
        let process = BackendProcessId::new_v7();
        let first = BackendTopologySnapshot::try_new(
            41,
            vec![live_target_with_epoch(0, process, 19010, 0x61)],
        )
        .expect("valid first snapshot");
        let replacement = BackendTopologySnapshot::try_new(
            42,
            vec![live_target_with_epoch(0, process, 19010, 0x62)],
        )
        .expect("valid replacement snapshot");

        let frozen = FrozenAttemptTopology::capture(first).expect("capture first snapshot");
        let later = FrozenAttemptTopology::capture(replacement).expect("capture later snapshot");
        assert_eq!(frozen.eligible_backends(), later.eligible_backends());
        assert_eq!(frozen.revision(), 41);
        assert_eq!(later.revision(), 42);

        let (_, exact) = frozen.into_parts();
        assert_eq!(
            exact[&process].admission_epoch_capability().to_bytes(),
            [0x61; 16]
        );
        let (_, replacement) = later.into_parts();
        assert_eq!(
            replacement[&process]
                .admission_epoch_capability()
                .to_bytes(),
            [0x62; 16]
        );
    }

    #[test]
    fn scan_validation_rejects_identity_from_isomorphic_foreign_seal() {
        let expected = scan_identity();
        let foreign = scan_identity();
        assert_eq!(expected.node_id(), foreign.node_id());
        assert_ne!(expected, foreign);

        let prepared = prepared_scan(7, expected, PreparedScanSource::RuntimeSplits);
        let tasks = [(7, projected_tasks(1))].into_iter().collect();
        let actual = [(
            (7, foreign),
            [(0, ProjectedScanAssignment::RuntimeSplits)]
                .into_iter()
                .collect(),
        )]
        .into_iter()
        .collect();

        let error = validate_scan_assignments(&prepared, &tasks, &actual)
            .expect_err("foreign scan seal must fail closed");
        assert!(error.message().contains("scan assignment set mismatch"));
    }

    #[test]
    fn scan_validation_requires_exact_runtime_split_task_cover() {
        let scan = scan_identity();
        let prepared = prepared_scan(7, scan, PreparedScanSource::RuntimeSplits);
        let tasks = [(7, projected_tasks(2))].into_iter().collect();
        let missing = [(
            (7, scan),
            [(0, ProjectedScanAssignment::RuntimeSplits)]
                .into_iter()
                .collect(),
        )]
        .into_iter()
        .collect();

        let error = validate_scan_assignments(&prepared, &tasks, &missing)
            .expect_err("one assignment per scheduled task is required");
        assert!(error.message().contains("assigned to every task"));
    }

    #[test]
    fn frozen_scan_cover_is_validated_in_task_count_time() {
        let scan = scan_identity();
        let exact = [
            (0, frozen(0, 3, 4)),
            (1, frozen(1, 3, 3)),
            (2, frozen(2, 3, 3)),
        ]
        .into_iter()
        .collect();
        validate_frozen_unit_cover(scan, 3, 10, &exact).expect("round-robin cover is exact");

        let wrong_stride = [
            (0, frozen(0, 2, 5)),
            (1, frozen(1, 2, 5)),
            (2, frozen(2, 3, 3)),
        ]
        .into_iter()
        .collect();
        let error = validate_frozen_unit_cover(scan, 3, 10, &wrong_stride)
            .expect_err("overlapping unit cover must fail closed");
        assert!(error.message().contains("does not exactly cover"));
    }

    #[test]
    fn empty_frozen_scan_keeps_fragment_tasks_without_scan_assignment() {
        let scan = scan_identity();
        let prepared = prepared_scan(7, scan, PreparedScanSource::FrozenUnits { unit_count: 0 });
        let tasks = [(7, projected_tasks(2))].into_iter().collect();
        validate_scan_assignments(&prepared, &tasks, &Default::default())
            .expect("empty work has an exact empty assignment cover");

        let unexpected = [((7, scan), [(0, frozen(0, 2, 1))].into_iter().collect())]
            .into_iter()
            .collect();
        let error = validate_scan_assignments(&prepared, &tasks, &unexpected)
            .expect_err("an empty scan cannot carry a task assignment");
        assert!(error.message().contains("scan assignment set mismatch"));
    }

    #[test]
    fn endpoint_snapshot_rejects_duplicate_process_and_endpoint_owners() {
        let retained_process = BackendProcessId::new_v7();
        let retained = validate_backend_snapshot(&[live_target(0, retained_process, 18999)])
            .expect("one valid process forms an exact snapshot");
        let retained = &retained[&retained_process];
        assert_eq!(retained.process_id(), retained_process);
        assert_eq!(retained.endpoint().host(), "127.0.0.1");
        assert_eq!(retained.endpoint().port(), 18999);
        assert_eq!(retained.admission_epoch_capability().to_bytes(), [0x61; 16]);
        assert_eq!(
            retained.native_compatibility_id(),
            NativeCompatibilityId::new([0x71; 32])
        );

        let process = BackendProcessId::new_v7();
        let duplicate_process = [
            live_target(0, process, 19000),
            live_target(1, process, 19001),
        ];
        let error = validate_backend_snapshot(&duplicate_process)
            .expect_err("one process must own one snapshot entry");
        assert!(error.message().contains("repeats backend process"));

        let duplicate_endpoint = [
            live_target(0, BackendProcessId::new_v7(), 19002),
            live_target(1, BackendProcessId::new_v7(), 19002),
        ];
        let error = validate_backend_snapshot(&duplicate_endpoint)
            .expect_err("one endpoint must have one process owner");
        assert!(error.message().contains("owned by both"));
    }

    #[test]
    fn task_identity_rejects_foreign_stage_and_context_process() {
        let execution = execution();
        let backend = BackendProcessId::new_v7();
        let (identity, context) = task(execution, 1, 1, backend);
        let wrong_stage = validate_task_identity(
            execution,
            StageId::new(2).expect("stage"),
            identity,
            context,
        )
        .expect_err("stage mismatch must fail closed");
        assert!(
            wrong_stage
                .message()
                .contains("differs from its fragment stage")
        );

        let foreign_context = QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let error = validate_task_identity(
            execution,
            StageId::new(1).expect("stage"),
            identity,
            foreign_context,
        )
        .expect_err("context process mismatch must fail closed");
        assert!(error.message().contains("differs from context"));
    }

    fn prepared_edge(source: u32, target: u32, exchange: i32) -> PreparedManifestFacts {
        PreparedManifestFacts {
            fragments: [source, target].into_iter().collect(),
            execution_anchor: target,
            scans: Default::default(),
            scans_by_node: Default::default(),
            edges: [((source, target, exchange), BoundManifestPartitionKind::Hash)]
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn edge_validation_rejects_missing_foreign_and_wrong_sender_shape() {
        let execution = execution();
        let backend_a = BackendProcessId::new_v7();
        let backend_b = BackendProcessId::new_v7();
        let (producer, _) = task(execution, 1, 1, backend_a);
        let (destination, _) = task(execution, 2, 2, backend_b);
        let tasks = [(1, vec![producer]), (2, vec![destination])]
            .into_iter()
            .collect();
        let identities = [producer, destination].into_iter().collect();
        let prepared = prepared_edge(1, 2, 77);

        validate_edges(&prepared, Vec::new(), &tasks, &identities)
            .expect_err("missing edge must fail closed");

        let foreign = TaskIdentity::new(
            execution,
            StageId::new(1).expect("stage"),
            TaskId::new(9).expect("task"),
            BackendProcessId::new_v7(),
        );
        let bad_producer = ProjectedEdge {
            edge_id: ExchangeEdgeId::new(1).expect("edge id"),
            source_fragment_id: 1,
            target_fragment_id: 2,
            target_exchange_node_id: 77,
            producers: vec![(foreign, 0)],
            destinations: vec![destination],
            sender_count: NonZeroU32::new(1).expect("sender count"),
        };
        let error = validate_edges(&prepared, vec![bad_producer], &tasks, &identities)
            .expect_err("foreign producer must fail closed");
        assert!(error.message().contains("producers do not exactly equal"));

        let bad_ordinal = ProjectedEdge {
            edge_id: ExchangeEdgeId::new(1).expect("edge id"),
            source_fragment_id: 1,
            target_fragment_id: 2,
            target_exchange_node_id: 77,
            producers: vec![(producer, 1)],
            destinations: vec![destination],
            sender_count: NonZeroU32::new(1).expect("sender count"),
        };
        let error = validate_edges(&prepared, vec![bad_ordinal], &tasks, &identities)
            .expect_err("sender ordinal must be the exact dense range");
        assert!(error.message().contains("sender ordinals"));
    }

    #[test]
    fn edge_validation_rejects_duplicate_edge_identity() {
        let execution = execution();
        let (producer_a, _) = task(execution, 1, 1, BackendProcessId::new_v7());
        let (producer_b, _) = task(execution, 2, 2, BackendProcessId::new_v7());
        let (destination, _) = task(execution, 3, 3, BackendProcessId::new_v7());
        let tasks = [
            (1, vec![producer_a]),
            (2, vec![producer_b]),
            (3, vec![destination]),
        ]
        .into_iter()
        .collect();
        let identities = [producer_a, producer_b, destination].into_iter().collect();
        let prepared = PreparedManifestFacts {
            fragments: [1, 2, 3].into_iter().collect(),
            execution_anchor: 3,
            scans: Default::default(),
            scans_by_node: Default::default(),
            edges: [
                ((1, 3, 77), BoundManifestPartitionKind::Hash),
                ((2, 3, 77), BoundManifestPartitionKind::Hash),
            ]
            .into_iter()
            .collect(),
        };
        let edge_id = ExchangeEdgeId::new(1).expect("edge id");
        let edges = vec![
            ProjectedEdge {
                edge_id,
                source_fragment_id: 1,
                target_fragment_id: 3,
                target_exchange_node_id: 77,
                producers: vec![(producer_a, 0)],
                destinations: vec![destination],
                sender_count: NonZeroU32::new(2).expect("sender count"),
            },
            ProjectedEdge {
                edge_id,
                source_fragment_id: 2,
                target_fragment_id: 3,
                target_exchange_node_id: 77,
                producers: vec![(producer_b, 1)],
                destinations: vec![destination],
                sender_count: NonZeroU32::new(2).expect("sender count"),
            },
        ];

        let error = validate_edges(&prepared, edges, &tasks, &identities)
            .expect_err("edge ids must be unique across the manifest");
        assert!(error.message().contains("repeats exchange edge id"));
    }

    #[test]
    fn fragment_instance_collision_fails_closed() {
        let collision = UniqueId::new(7, 9);
        let mut seen = Default::default();
        derive_and_record_fragment_instance_id(&mut seen, || Ok(collision))
            .expect("first identity is unique");
        let error = derive_and_record_fragment_instance_id(&mut seen, || Ok(collision))
            .expect_err("a repeated derived identity must fail closed");
        assert!(error.message().contains("identity collision"));
    }
}
