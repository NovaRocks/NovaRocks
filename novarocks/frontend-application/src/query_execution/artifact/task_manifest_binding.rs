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

use super::FragmentId;
use crate::query_execution::attempt_plan_facts::AttemptPartitionKind;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::AdmissionEpochCapability;
use novarocks_execution_contract::{ExchangeEdgeId, QueryContextRef, TaskIdentity};
use novarocks_query_application::api::{NativeScanWork, PlanScanIdentity};
use novarocks_query_application::coordination::{
    AttemptSchedule, ScheduledFrozenUnits, ScheduledScanAssignment,
};
use novarocks_types::identity::{BackendProcessId, QueryExecutionId, StageId};
use novarocks_types::{NativeCompatibilityId, UniqueId};

use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks_query_application::api::{BackendTopologySnapshot, LiveBackendTarget};

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

/// An exact attempt Task's declared writer/target occurrence, read from the
/// immutable template this manifest retained. It grants no deletion authority
/// and does not enumerate unknown BE-local driver/object identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundWriterCohort {
    plan: novarocks_query_application::api::PlanSeal,
    task: TaskIdentity,
    fragment: FragmentId,
    writer: crate::native::fragment_encoder::frozen::StaticTableWriter,
}
impl BoundWriterCohort {
    pub(crate) const fn plan(self) -> novarocks_query_application::api::PlanSeal {
        self.plan
    }
    pub(crate) const fn task(self) -> TaskIdentity {
        self.task
    }
    pub(crate) const fn fragment(self) -> FragmentId {
        self.fragment
    }
    pub(crate) const fn writer(self) -> crate::native::fragment_encoder::frozen::StaticTableWriter {
        self.writer
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
    FrozenUnits(BoundManifestFrozenUnits),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BoundManifestScanWork {
    scan: PlanScanIdentity,
    assignment: BoundManifestScanAssignment,
}

impl BoundManifestScanWork {
    pub(crate) const fn scan(&self) -> PlanScanIdentity {
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
    const fn from_plan(kind: AttemptPartitionKind) -> Self {
        match kind {
            AttemptPartitionKind::Unpartitioned => Self::Unpartitioned,
            AttemptPartitionKind::Random => Self::Random,
            AttemptPartitionKind::Hash => Self::Hash,
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
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
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

    pub(crate) const fn delivery(
        &self,
    ) -> novarocks_execution_contract::ExchangeDeliveryRequirement {
        self.delivery
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
        Self::capture_for_candidate(
            snapshot,
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot,
        )
    }

    pub(crate) fn capture_for_candidate(
        snapshot: BackendTopologySnapshot,
        requirement: novarocks_query_application::api::NativeAttemptTopologyRequirement,
    ) -> Result<Self, DistributedQueryError> {
        let revision = snapshot.revision();
        // Retain every exact owner-provided target. Selection constrains this
        // attempt's placement, never the registry's membership snapshot.
        let backends = validate_backend_snapshot(snapshot.targets())?;
        let eligible_backends = backends
            .keys()
            .copied()
            .filter(|process| match requirement {
                novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot => true,
                novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) => *process != excluded,
            })
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

    fn into_parts(
        self,
    ) -> (
        u64,
        BTreeMap<BackendProcessId, BoundManifestBackend>,
        Box<[BackendProcessId]>,
    ) {
        (self.revision, self.backends, self.eligible_backends)
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
    eligible_backends: Box<[BackendProcessId]>,
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
    /// Reentry uses the exact original placement without allocating a second
    /// graph or accepting execution/process identity as a substitute for it.
    pub(crate) fn validate_schedule(
        &self,
        schedule: &AttemptSchedule,
    ) -> Result<(), DistributedQueryError> {
        let same =
            self.execution == schedule.execution()
                && self.root == schedule.root()
                && self
                    .contexts
                    .iter()
                    .map(|context| context.context())
                    .eq(schedule.contexts().iter().copied())
                && self.tasks.len()
                    == schedule
                        .fragments()
                        .iter()
                        .map(|fragment| fragment.tasks().len())
                        .sum::<usize>()
                && self
                    .tasks
                    .iter()
                    .zip(schedule.fragments().iter().flat_map(|fragment| {
                        fragment.tasks().iter().map(move |task| (fragment, task))
                    }))
                    .all(|(bound, (fragment, task))| {
                        bound.fragment_id == fragment.fragment_id()
                            && bound.identity.stage_id() == fragment.stage_id()
                            && bound.identity == task.identity()
                            && bound.context == task.context()
                            && bound.instance_index == task.instance_index()
                            && bound.scan_work.len() == task.scan_work().len()
                            && bound
                                .scan_work
                                .iter()
                                .zip(task.scan_work())
                                .all(|(old, new)| {
                                    let assignment = match new.assignment() {
                                        ScheduledScanAssignment::RuntimeSplits => {
                                            BoundManifestScanAssignment::RuntimeSplits
                                        }
                                        ScheduledScanAssignment::FrozenUnits { units } => {
                                            BoundManifestScanAssignment::FrozenUnits(
                                                BoundManifestFrozenUnits::from_scheduled(*units),
                                            )
                                        }
                                    };
                                    old.scan == new.scan() && old.assignment == assignment
                                })
                    })
                && self.edges.len() == schedule.edges().len()
                && self.edges.iter().zip(schedule.edges()).all(|(old, new)| {
                    old.edge_id == new.edge_id()
                        && old.source_fragment_id == new.source_fragment_id()
                        && old.target_fragment_id == new.target_fragment_id()
                        && old.target_exchange_node_id == new.target_exchange_node_id()
                        && old.sender_count == new.sender_count()
                        && old.destinations.as_ref() == new.destinations()
                        && old.producers.len() == new.producers().len()
                        && old.producers.iter().zip(new.producers()).all(|(old, new)| {
                            old.task == new.task() && old.sender_ordinal == new.sender_ordinal()
                        })
                });
        if same {
            Ok(())
        } else {
            Err(contract_error(
                "qualification reentry changed the original bound schedule",
            ))
        }
    }

    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fn qualification_fixture_for_test(
        execution: QueryExecutionId,
        processes: &[BackendProcessId; 3],
        selected: usize,
    ) -> (Self, AttemptSchedule) {
        tests::qualification_fixture(execution, processes, selected)
    }

    /// Finite neutral protocol topology from the caller's exact attempt and
    /// placements. This does not create a request ticket or executable Router.
    #[cfg(test)]
    pub(crate) fn protocol_fixture_for_test(
        execution: QueryExecutionId,
        frontend: novarocks_types::FrontendProcessId,
        schedule: &crate::query_execution::schedule::SchedulingPlan,
        edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        processes: &BTreeMap<usize, BackendProcessId>,
        fragments: BTreeMap<
            FragmentId,
            std::sync::Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
        >,
    ) -> Result<Self, DistributedQueryError> {
        let (template, scheduled, topology) =
            protocol_fixture_inputs(execution, frontend, schedule, edges, processes, fragments)?;
        Self::bind(template, &scheduled, topology)
    }

    /// Exercise the real bind rejection with one exact endpoint fact absent.
    #[cfg(test)]
    pub(crate) fn protocol_missing_backend_for_test(
        execution: QueryExecutionId,
        frontend: novarocks_types::FrontendProcessId,
        schedule: &crate::query_execution::schedule::SchedulingPlan,
        edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        processes: &BTreeMap<usize, BackendProcessId>,
        fragments: BTreeMap<
            FragmentId,
            std::sync::Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
        >,
        missing: usize,
    ) -> Result<Self, DistributedQueryError> {
        let (template, scheduled, topology) =
            protocol_fixture_inputs(execution, frontend, schedule, edges, processes, fragments)?;
        let (revision, mut backends, eligible) = topology.into_parts();
        let process = processes
            .get(&missing)
            .ok_or_else(|| contract_error("negative fixture must name an original backend"))?;
        backends.remove(process);
        Self::bind_facts(
            template,
            execution,
            ScheduleFacts::from_schedule(&scheduled),
            revision,
            backends,
            eligible,
        )
    }

    /// The production collision recorder rejects the same two original keys.
    #[cfg(test)]
    pub(crate) fn protocol_collision_for_test(
        first: UniqueId,
        second: UniqueId,
    ) -> Result<(), DistributedQueryError> {
        let mut seen = BTreeSet::new();
        derive_and_record_fragment_instance_id(&mut seen, || Ok(first))?;
        derive_and_record_fragment_instance_id(&mut seen, || Ok(second))?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn original_writer_occurrences_for_test() -> Vec<BoundWriterCohort> {
        tests::original_writer_occurrences()
    }

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
        let (topology_revision, backends, eligible_backends) = topology.into_parts();
        Self::bind_facts(
            bound_template,
            execution,
            facts,
            topology_revision,
            backends,
            eligible_backends,
        )
    }

    fn bind_facts(
        bound_template: RequestBoundNativeTemplate,
        expected_execution: QueryExecutionId,
        schedule: ScheduleFacts,
        topology_revision: u64,
        endpoint_by_process: BTreeMap<BackendProcessId, BoundManifestBackend>,
        eligible_backends: Box<[BackendProcessId]>,
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
        let mut fragment_scan_assignments =
            BTreeMap::<PlanScanIdentity, BTreeMap<usize, ProjectedScanAssignment>>::new();
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
                        .entry(scan_work.scan)
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
        validate_root_backend_support(schedule.root, &endpoint_by_process)?;

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
            eligible_backends,
            native_template: bound_template,
            tasks: bound_tasks.into_boxed_slice(),
            contexts: bound_contexts.into_boxed_slice(),
            edges: bound_edges.into_boxed_slice(),
            root: schedule.root,
        })
    }

    /// The original selection survives binding even when only a subset has Tasks.
    /// This is the same Box from FrozenAttemptTopology, including exclusions.
    pub(crate) fn eligible_backends(&self) -> &[BackendProcessId] {
        &self.eligible_backends
    }

    pub(crate) const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub(crate) const fn plan(&self) -> novarocks_query_application::api::PlanSeal {
        self.native_template.template.identity.plan
    }

    pub(crate) const fn topology_revision(&self) -> u64 {
        self.topology_revision
    }

    /// Borrow the existing frozen occurrence list; no per-Task list is cloned.
    /// Binding already proved that every Task's fragment is in this template.
    pub(crate) fn writer_cohorts(&self) -> impl Iterator<Item = BoundWriterCohort> + '_ {
        let template = &self.native_template.template;
        bound_writer_cohorts(template.plan(), &self.tasks, &template.native_template)
    }

    /// Each exact Task that owns the independent summary output of its
    /// retained checked package. The edge is semantic identity, not a branch ordinal.
    pub(crate) fn summary_sources(
        &self,
    ) -> impl Iterator<
        Item = (
            TaskIdentity,
            crate::native::fragment_encoder::frozen::StaticSinkTarget,
        ),
    > + '_ {
        bound_summary_sources(&self.tasks, &self.native_template.template.native_template)
    }

    pub(crate) fn validate_submission_fragment(
        &self,
        execution: QueryExecutionId,
        fragment: &std::sync::Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
    ) -> Result<(), DistributedQueryError> {
        validate_schedule_execution(self.execution, execution)?;
        let id = fragment.facts().fragment_id();
        let original = self
            .native_template
            .template
            .native_template
            .get(id)
            .ok_or_else(|| {
                contract_error(format!(
                    "submission fragment {id} is absent from its retained Native template"
                ))
            })?;
        if !std::sync::Arc::ptr_eq(original, fragment) {
            return Err(contract_error(format!(
                "submission fragment {id} belongs to another frozen Native artifact"
            )));
        }
        Ok(())
    }

    pub(crate) fn tasks(&self) -> &[BoundManifestTask] {
        &self.tasks
    }

    /// Finite facts for actual participant contexts, not all eligible members.
    /// Both original endpoints are projected; the original paid builder performs
    /// canonical union/dedup. This iterator allocates no second backend list,
    /// invokes no network/callback, and carries no admission authority.
    pub(crate) fn selected_network_origins<'a>(
        &'a self,
        transport: &'a novarocks_native_adapter::FrontendNativeTransport,
    ) -> impl Iterator<
        Item = Result<
            novarocks_resource_contract::SelectedNetworkOrigin<'a>,
            novarocks_resource_contract::DependencyFactError,
        >,
    > + 'a {
        self.contexts.iter().flat_map(move |context| {
            let descriptor = context.backend().target().descriptor();
            [
                descriptor.endpoint().native_endpoint(),
                descriptor.control_endpoint().native_endpoint(),
            ]
            .into_iter()
            .map(move |endpoint| transport.selected_network_origin(endpoint))
        })
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn remote_writer_sender(&self) -> Option<TaskIdentity> {
        select_remote_writer_sender(self.writer_cohorts(), self.root, &self.edges)
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

#[cfg(novarocks_dev_controls)]
fn select_remote_writer_sender(
    writers: impl Iterator<Item = BoundWriterCohort>,
    root: TaskIdentity,
    edges: &[BoundManifestEdge],
) -> Option<TaskIdentity> {
    // A fixture selects one existing original; this is no writer-count policy.
    writers
        .map(|writer| writer.task())
        .filter(|task| {
            *task != root
                && task.backend_process_id() != root.backend_process_id()
                && edges.iter().any(|edge| {
                    edge.producers().iter().any(|source| source.task() == *task)
                        && edge.destinations().contains(&root)
                })
        })
        .min()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectedScanAssignment {
    RuntimeSplits,
    FrozenUnits(BoundManifestFrozenUnits),
}

fn bound_summary_sources<'a>(
    tasks: &'a [BoundManifestTask],
    fragments: &'a crate::query_execution::native_fragment::NativeFragmentAttachment,
) -> impl Iterator<
    Item = (
        TaskIdentity,
        crate::native::fragment_encoder::frozen::StaticSinkTarget,
    ),
> + 'a {
    tasks.iter().filter_map(move |task| {
        fragments
            .get(task.fragment_id)
            .expect("bound Task fragment belongs to its retained Native template")
            .facts()
            .logical_summary_output()
            .map(|output| (task.identity, output))
    })
}

fn bound_writer_cohorts<'a>(
    plan: novarocks_query_application::api::PlanSeal,
    tasks: &'a [BoundManifestTask],
    fragments: &'a crate::query_execution::native_fragment::NativeFragmentAttachment,
) -> impl Iterator<Item = BoundWriterCohort> + 'a {
    tasks.iter().flat_map(move |task| {
        fragments
            .get(task.fragment_id)
            .expect("bound Task fragment belongs to its retained Native template")
            .facts()
            .table_writers()
            .iter()
            .copied()
            .map(move |writer| BoundWriterCohort {
                plan,
                task: task.identity,
                fragment: task.fragment_id,
                writer,
            })
    })
}

impl ProjectedScanAssignment {
    fn into_bound(self) -> BoundManifestScanAssignment {
        match self {
            Self::RuntimeSplits => BoundManifestScanAssignment::RuntimeSplits,
            Self::FrozenUnits(units) => BoundManifestScanAssignment::FrozenUnits(units),
        }
    }
}

#[derive(Debug)]
struct ProjectedScanWork {
    scan: PlanScanIdentity,
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
    FrozenUnits { unit_count: usize },
}

/// Minimal immutable Native facts the schedule is allowed to bind against.
///
/// This projection is constructed from the retained preparation/template
/// owners. Tests can exercise the validation core without creating a second
/// scheduling authority or making `AttemptSchedule` forgeable.
#[derive(Clone, Copy, Debug)]
struct PreparedEdgeFacts {
    partition_kind: BoundManifestPartitionKind,
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
}

#[derive(Debug)]
struct PreparedManifestFacts {
    fragments: BTreeSet<FragmentId>,
    execution_anchor: FragmentId,
    scans: BTreeMap<PlanScanIdentity, PreparedScanSource>,
    scans_by_node: BTreeMap<(FragmentId, i32), PlanScanIdentity>,
    edges: BTreeMap<PreparedEdgeKey, PreparedEdgeFacts>,
}

impl PreparedManifestFacts {
    fn from_template(
        template: &PreparedDistributedNativeTemplate,
    ) -> Result<Self, DistributedQueryError> {
        let scheduling = template
            .attempt_scheduling_facts()
            .map_err(contract_error)?;
        let fragments = scheduling
            .fragments
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect::<BTreeSet<_>>();
        let native_fragments = template.native_fragment_ids().collect::<BTreeSet<_>>();
        if native_fragments != fragments {
            return Err(set_mismatch(
                "Native template fragment",
                &fragments,
                &native_fragments,
            ));
        }

        let mut scans = BTreeMap::new();
        let mut scans_by_node = BTreeMap::new();
        for fragment in &scheduling.fragments {
            for scan in &fragment.scans {
                let source = match scan.work {
                    NativeScanWork::RuntimeSplits { .. } => PreparedScanSource::RuntimeSplits,
                    NativeScanWork::Empty => PreparedScanSource::FrozenUnits { unit_count: 0 },
                    NativeScanWork::FrozenUnits { count } => PreparedScanSource::FrozenUnits {
                        unit_count: count.get(),
                    },
                };
                if scans.insert(scan.scan, source).is_some()
                    || scans_by_node
                        .insert((fragment.fragment_id, scan.scan.node_id()), scan.scan)
                        .is_some()
                {
                    return Err(contract_error(format!(
                        "prepared Native projection repeats scan occurrence fragment_id={} node_id={}",
                        fragment.fragment_id,
                        scan.scan.node_id()
                    )));
                }
            }
        }

        let mut edges = BTreeMap::new();
        for edge in template.plan_facts().edges() {
            let key = (
                edge.source_fragment_id,
                edge.target_fragment_id,
                edge.target_exchange_node_id,
            );
            if edges
                .insert(
                    key,
                    PreparedEdgeFacts {
                        partition_kind: BoundManifestPartitionKind::from_plan(edge.partition_kind),
                        delivery: edge.delivery,
                    },
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
            execution_anchor: scheduling.execution_anchor_fragment_id,
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
        let native_compatibility_id = target.descriptor().native_compatibility_id();
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
    actual: &BTreeMap<PlanScanIdentity, BTreeMap<usize, ProjectedScanAssignment>>,
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
    for &scan in &expected {
        let tasks = tasks_by_fragment.get(&scan.fragment_id()).ok_or_else(|| {
            contract_error(format!(
                "attempt manifest scan node {} has no scheduled fragment {}",
                scan.node_id(),
                scan.fragment_id()
            ))
        })?;
        let assignments = actual.get(&scan).unwrap_or(&empty_assignments);
        match prepared.scans[&scan] {
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
            PreparedScanSource::FrozenUnits { unit_count } => {
                validate_frozen_unit_cover(scan, tasks.len(), unit_count, assignments)?;
            }
        }
    }
    Ok(())
}

fn validate_frozen_unit_cover(
    scan: PlanScanIdentity,
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
                partition_kind: prepared.edges[&key].partition_kind,
                delivery: prepared.edges[&key].delivery,
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

/// Require support from the root's original frozen process before activation
/// can obtain Connector capabilities or initialize an execution attempt.
/// Other snapshot candidates cannot lend their support to this placement.
fn validate_root_backend_support(
    root: TaskIdentity,
    backends: &BTreeMap<BackendProcessId, BoundManifestBackend>,
) -> Result<(), DistributedQueryError> {
    let process = root.backend_process_id();
    let backend = backends.get(&process).ok_or_else(|| {
        contract_error(format!(
            "attempt manifest root backend process {process} is absent from its frozen snapshot"
        ))
    })?;
    backend
        .target()
        .descriptor()
        .require_bounded_root_support()
        .map_err(|error| {
            contract_error(format!(
                "attempt manifest root backend process {process} cannot serve bounded root results: {error}"
            ))
        })?;
    Ok(())
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

/// Only the existing development test graph calls this value fixture. Its
/// supplied Result/Stream bytes and neutral control DAG remain separate; no
/// provider, runtime or application preparation ticket is manufactured.
#[cfg(test)]
fn protocol_fixture_inputs(
    execution: QueryExecutionId,
    frontend: novarocks_types::FrontendProcessId,
    schedule: &crate::query_execution::schedule::SchedulingPlan,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    processes: &BTreeMap<usize, BackendProcessId>,
    fragments: BTreeMap<
        FragmentId,
        std::sync::Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
    >,
) -> Result<
    (
        RequestBoundNativeTemplate,
        AttemptSchedule,
        FrozenAttemptTopology,
    ),
    DistributedQueryError,
> {
    use super::native_submission::{
        NativeSubmissionFragmentRole, SubmissionFragmentFacts, SubmissionPlanFacts,
    };
    use crate::query_execution::fragment_scheduling::{
        FragmentSchedulingFacts, SchedulingEdgeFacts, SchedulingFragmentFacts, SchedulingScanFacts,
        SchedulingStreamKind,
    };
    use std::sync::Arc;
    let order = schedule.by_fragment.keys().copied().collect::<Vec<_>>();
    let scheduling = FragmentSchedulingFacts {
        handoff_id: 9,
        order: order.clone(),
        anchor: schedule.root_fragment_id,
        fragments: schedule.by_fragment.iter().map(|(&fragment, placements)| {
            let first = placements.first().ok_or_else(|| contract_error("protocol fixture fragment has no placement"))?;
            let scans = first.scan_ranges.keys().map(|&node| SchedulingScanFacts {
                node_id: node,
                ranges: Vec::new(),
                runtime_splits: Some(novarocks_query_application::api::RuntimeSplitPlacement::Spread),
            }).collect();
            for placement in placements {
                if placement.scan_ranges.keys().ne(first.scan_ranges.keys()) || placement.scan_ranges.values().any(|ranges| !ranges.is_empty()) {
                    return Err(contract_error("finite protocol fixture requires its original empty runtime-split assignments"));
                }
            }
            Ok((fragment, SchedulingFragmentFacts { placement: novarocks_physical_plan::StagePlacementPolicy::ScanWork, scans }))
        }).collect::<Result<_, DistributedQueryError>>()?,
        edges: edges.iter().map(|edge| SchedulingEdgeFacts {
            source: edge.source_fragment_id,
            target: edge.target_fragment_id,
            target_exchange_node_id: edge.target_exchange_node_id,
            native_hash_partitioned: edge.partition_kind == AttemptPartitionKind::Hash,
            stream_kind: if edge.partition_kind == AttemptPartitionKind::Unpartitioned { SchedulingStreamKind::Gather } else { SchedulingStreamKind::Partitioned },
        }).collect(),
    };
    let seal = novarocks_query_application::api::PlanSeal::Version(
        novarocks_physical_plan::PlanVersionId::try_new([9; 16])
            .expect("original protocol fixture version"),
    );
    let native =
        crate::query_execution::native_fragment::NativeFragmentAttachment::for_completed_plan(
            fragments,
            schedule.root_fragment_id,
        )
        .map_err(contract_error)?;
    let facts = crate::query_execution::attempt_plan_facts::AttemptPlanFacts::from_completed(
        scheduling,
        edges.to_vec(),
        Vec::new(),
        SubmissionPlanFacts::for_completed_plan(
            order.clone(),
            order
                .iter()
                .map(|&id| {
                    SubmissionFragmentFacts::for_completed_plan(
                        id,
                        if id == schedule.root_fragment_id {
                            NativeSubmissionFragmentRole::Result
                        } else {
                            NativeSubmissionFragmentRole::NonTerminal
                        },
                        Vec::new(),
                    )
                })
                .collect(),
            edges.iter().map(|e| e.source_fragment_id).collect(),
        ),
        Default::default(),
        None,
    );
    let template = RequestBoundNativeTemplate {
        execution,
        template: PreparedDistributedNativeTemplate {
            identity: super::PreparedDistributedTemplateIdentity::new(9, seal),
            plan_facts: Arc::new(facts),
            native_template: Arc::new(native),
        },
        affinity: Arc::new(super::PreparedDistributedAttemptAffinity),
    };
    let explicit = schedule
        .by_fragment
        .iter()
        .map(|(&fragment, placements)| {
            let backends = placements
                .iter()
                .map(|placement| {
                    processes
                        .get(&placement.backend_idx)
                        .copied()
                        .ok_or_else(|| {
                            contract_error(format!(
                                "protocol fixture backend {} lacks its original process",
                                placement.backend_idx
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((fragment, backends))
        })
        .collect::<Result<BTreeMap<_, _>, DistributedQueryError>>()?;
    let scheduled = novarocks_query_application::test_support::protocol_fixture_schedule(
        execution,
        frontend,
        &template
            .template
            .attempt_scheduling_facts()
            .map_err(contract_error)?,
        &explicit,
    )
    .map_err(contract_error)?;
    let mut placed_endpoints = BTreeMap::new();
    for placement in schedule.by_fragment.values().flatten() {
        if let Some(previous) =
            placed_endpoints.insert(placement.backend_idx, placement.endpoint.clone())
            && previous != placement.endpoint
        {
            return Err(contract_error(
                "one protocol backend process must retain one endpoint",
            ));
        }
    }
    let targets = processes
        .iter()
        .map(|(&index, &process)| {
            let endpoint = placed_endpoints.get(&index).cloned().unwrap_or_else(|| {
                RuntimeEndpoint::new("127.0.0.1", 9000 + index as i32)
                    .expect("declared unused protocol endpoint")
            });
            let control_endpoint = RuntimeEndpoint::new("127.0.0.1", 11000 + index as i32)
                .expect("declared protocol control endpoint");
            let descriptor = novarocks_execution_contract::BackendProcessDescriptor::try_new(
                process,
                endpoint,
                control_endpoint.clone(),
                "test-deployment",
                "test-build",
                NativeCompatibilityId::new([0x71; 32]),
                4096,
            )
            .map_err(|error| contract_error(error.to_string()))?;
            let descriptor = descriptor
                .with_bounded_root_support(
                    novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
                        control_endpoint,
                        novarocks_result_contract::RootProfileId::V1,
                    ),
                )
                .map_err(|error| contract_error(error.to_string()))?;
            Ok(LiveBackendTarget::new(
                index,
                descriptor,
                AdmissionEpochCapability::try_from_bytes([0x61; 16])
                    .expect("original protocol epoch"),
            ))
        })
        .collect::<Result<Vec<_>, DistributedQueryError>>()?;
    let topology = FrozenAttemptTopology::capture(
        BackendTopologySnapshot::try_new(91, targets)
            .map_err(|error| contract_error(error.to_string()))?,
    )?;
    Ok((template, scheduled, topology))
}

#[cfg(test)]
mod tests {
    include!("task_manifest_binding_router_freeze_tests.rs");
    use crate::query_execution::completion_facts::test_support::FinalPlanCompletionDriverTestExt;
    use std::num::{NonZeroU32, NonZeroUsize};

    use novarocks_execution::task_execution::AdmissionEpochCapability;
    use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};
    use novarocks_execution_contract::{ExchangeEdgeId, QueryContextRef, TaskIdentity};
    use novarocks_query_application::api::{PlanScanIdentity, PlanSeal};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{NativeCompatibilityId, UniqueId};

    use super::{
        BoundManifestFrozenUnits, BoundManifestPartitionKind, FrozenAttemptTopology,
        PreparedManifestFacts, PreparedScanSource, ProjectedEdge, ProjectedScanAssignment,
        ProjectedTask, derive_and_record_fragment_instance_id, validate_backend_snapshot,
        validate_edges, validate_frozen_unit_cover, validate_root_backend_support,
        validate_scan_assignments, validate_schedule_execution, validate_task_identity,
    };
    use novarocks_query_application::api::{BackendTopologySnapshot, LiveBackendTarget};

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

    fn scan_identity() -> PlanScanIdentity {
        PlanScanIdentity::new(
            PlanSeal::Version(
                novarocks_physical_plan::PlanVersionId::try_new([31; 16]).expect("plan version"),
            ),
            1,
            0,
        )
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

    fn prepared_scan(scan: PlanScanIdentity, source: PreparedScanSource) -> PreparedManifestFacts {
        let fragment_id = scan.fragment_id();
        PreparedManifestFacts {
            fragments: [fragment_id].into_iter().collect(),
            execution_anchor: fragment_id,
            scans: [(scan, source)].into_iter().collect(),
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
        let descriptor = BackendProcessDescriptor::try_new(
            process,
            RuntimeEndpoint::new("127.0.0.1", i32::from(port)).expect("valid endpoint"),
            RuntimeEndpoint::new(format!("control-{ordinal}.test.invalid"), 19061)
                .expect("valid control endpoint"),
            "test-deployment",
            "test-build",
            NativeCompatibilityId::new([0x71; 32]),
            4096,
        )
        .expect("valid descriptor");
        LiveBackendTarget::new(
            ordinal,
            descriptor,
            AdmissionEpochCapability::try_from_bytes([epoch; 16]).expect("nonzero admission epoch"),
        )
    }

    // This fixture exercises the production bind_facts call, never the support
    // helper directly. It does not construct an application-owned AttemptSchedule:
    // that value is private and move-only across the QA crate boundary.
    #[derive(Clone, Copy)]
    enum FullBindPurpose {
        ClientRows,
        CountOnly,
    }

    fn full_bind_native_template(
        execution: QueryExecutionId,
        purpose: FullBindPurpose,
    ) -> super::super::RequestBoundNativeTemplate {
        use std::sync::Arc;

        use arrow::datatypes::DataType;
        use novarocks_physical_plan::{
            MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
            ScanReadBudget, ValueType,
        };
        use novarocks_query_application::preparation::{
            FinalPlanCompletionDriver, ReadAccessSink, SqlCompletionFactSource,
        };
        use novarocks_result_contract::FrozenRootOutput;
        use novarocks_sql::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog,
        };
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };

        struct NoFacts;
        #[async_trait::async_trait]
        impl SqlCompletionFactSource for NoFacts {
            type Access = crate::query_execution::provider_read_facts::FrozenProviderRead;

            async fn resolve(
                &self,
                _: &SqlNeedBatch,
                _: &ReadAccessSink<Self::Access>,
            ) -> Result<SqlFactBatch, String> {
                panic!("the literal SQL fixture must not request provider facts")
            }
        }

        let version = PlanVersionId::try_new([83; 16]).expect("finite component plan version");
        let request = SqlFinalPlanCompileRequest::new(
            version,
            SqlStatementInput::sql("SELECT 7 AS payload"),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            crate::query_execution::constant_eval::constant_evaluator(),
            crate::application::test_constant_policy(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        workload.mark_ready().expect("workload control ready");
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let scope = root.owner.scope();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("completion runtime");
        // SQL completion keeps the real immutable call-source journal even
        // when constant folding leaves only a literal Values expression.
        let paired = runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(NoFacts))
                    .complete_for_test(request, &scope),
            )
            .expect("the exact original Int64 payload completes from SQL");
        let plan = paired.candidate().plan();
        assert_eq!(plan.version(), version);
        assert_eq!(plan.fragments().len(), 2, "distributed producer and gather");
        let result = paired
            .candidate()
            .original_public_result_port()
            .expect("the original SQL public result declaration");
        let root_fragment = result.fragment.get();
        assert!(plan.fragments().contains_key(&result.fragment));
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].ty, ValueType::new(DataType::Int64, false));
        // ResultField retains the expression label separately from the SQL
        // alias. Public rendering owns the column name seen by the client.
        let render = novarocks_sql::compiler::client_render_schema_from_port(
            Some(result),
            novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 0,
            },
        )
        .expect("render schema from the original SQL public result declaration");
        assert_eq!(render.columns().len(), 1);
        assert_eq!(render.columns()[0].name, "payload");
        assert_eq!(render.columns()[0].source_ordinal, 0);
        let output = match purpose {
            FullBindPurpose::ClientRows => FrozenRootOutput::ClientRows(render),
            FullBindPurpose::CountOnly => FrozenRootOutput::CountOnly,
        };
        let paired = paired
            .freeze_root_output(output)
            .unwrap_or_else(|(error, _access)| panic!("freeze the original root purpose: {error}"));
        let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
            paired,
            &crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
                novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
                novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
            ),
            crate::application::test_constant_policy(),
            None,
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .expect("production physical-to-native encoding");
        assert_eq!(encoded.native.fragment_ids().count(), 2);
        assert_eq!(encoded.access.iter().count(), 0);
        assert!(encoded.plan_facts.scans().is_empty());
        assert_eq!(encoded.plan_facts.edges().len(), 1);
        assert_eq!(encoded.plan_facts.scheduling().anchor, root_fragment);
        let template = encoded.into_attempt_template(version);
        let scheduling = template
            .attempt_scheduling_facts()
            .expect("original scheduling facts");
        assert_eq!(scheduling.fragments.len(), 2);
        assert_eq!(scheduling.edges.len(), 1);
        assert!(
            scheduling
                .fragments
                .iter()
                .all(|fragment| fragment.scans.is_empty())
        );

        // Finite component-only request-bound fixture. All static Native facts
        // and template affinity above came from the real encoder; no provider,
        // runtime, application request ticket or activation is opened here.
        super::super::RequestBoundNativeTemplate {
            execution,
            template: template.native.fork_for_attempt(),
            affinity: std::sync::Arc::new(super::super::PreparedDistributedAttemptAffinity),
        }
    }

    fn full_bind_supported(target: LiveBackendTarget) -> LiveBackendTarget {
        let descriptor = target.descriptor().clone();
        let support = novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
            descriptor.control_endpoint().clone(),
            novarocks_result_contract::RootProfileId::V1,
        );
        LiveBackendTarget::new(
            target.backend_idx(),
            descriptor
                .with_bounded_root_support(support)
                .expect("exact root capability"),
            target.admission_epoch_capability(),
        )
    }

    fn full_bind_schedule(
        execution: QueryExecutionId,
        backend: BackendProcessId,
        template: &super::super::RequestBoundNativeTemplate,
    ) -> super::ScheduleFacts {
        let prepared = super::PreparedManifestFacts::from_template(&template.template)
            .expect("the actual frozen Native graph supplies every manifest fact");
        let scheduling = template
            .template
            .attempt_scheduling_facts()
            .expect("the same actual attempt scheduling facts");
        assert_eq!(prepared.fragments.len(), 2);
        assert_eq!(prepared.edges.len(), 1);
        assert_eq!(scheduling.fragments.len(), 2);
        assert_eq!(scheduling.edges.len(), 1);
        assert_eq!(
            scheduling.execution_anchor_fragment_id,
            prepared.execution_anchor
        );
        assert!(
            scheduling
                .fragments
                .iter()
                .all(|fragment| fragment.scans.is_empty())
        );
        assert_eq!(
            scheduling
                .fragments
                .iter()
                .map(|fragment| fragment.fragment_id)
                .collect::<std::collections::BTreeSet<_>>(),
            prepared.fragments,
        );
        // This existing component seam projects one task per real fragment at
        // the fixture's original DOP=1 and exact selected backend. It does not
        // construct QA's move-only AttemptSchedule or alter the compiled graph.
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let mut identities = std::collections::BTreeMap::new();
        let fragments = prepared
            .fragments
            .iter()
            .enumerate()
            .map(|(index, &fragment_id)| {
                let ordinal = u32::try_from(index + 1).expect("finite fixture task ordinal");
                let stage_id = StageId::new(ordinal).expect("one nonzero stage per real fragment");
                let identity = TaskIdentity::new(
                    execution,
                    stage_id,
                    TaskId::new(ordinal).expect("one nonzero task per real fragment"),
                    backend,
                );
                identities.insert(fragment_id, identity);
                super::ProjectedFragment {
                    fragment_id,
                    stage_id,
                    tasks: vec![ProjectedTask {
                        identity,
                        context,
                        instance_index: 0,
                        scan_work: Vec::new(),
                    }],
                }
            })
            .collect();
        let root = identities[&prepared.execution_anchor];
        let edges = prepared
            .edges
            .iter()
            .enumerate()
            .map(|(index, (&(source, target, exchange), &partition))| {
                assert_eq!(
                    partition.partition_kind,
                    super::BoundManifestPartitionKind::Unpartitioned
                );
                let original = scheduling
                    .edges
                    .iter()
                    .find(|edge| {
                        (
                            edge.source_fragment_id,
                            edge.target_fragment_id,
                            edge.target_exchange_node_id,
                        ) == (source, target, exchange)
                    })
                    .expect("the actual scheduling edge and frozen edge agree");
                assert_eq!(
                    original.stream_kind,
                    novarocks_query_application::api::SchedulingStreamKind::Gather
                );
                assert!(!original.hash_partitioned);
                assert_eq!(
                    identities[&target], root,
                    "the real gather terminates at the public root"
                );
                super::ProjectedEdge {
                    edge_id: ExchangeEdgeId::new(
                        u32::try_from(index + 1).expect("finite fixture edge ordinal"),
                    )
                    .expect("one nonzero edge per real edge"),
                    source_fragment_id: source,
                    target_fragment_id: target,
                    target_exchange_node_id: exchange,
                    producers: vec![(identities[&source], 0)],
                    destinations: vec![identities[&target]],
                    sender_count: NonZeroU32::new(1).expect("one actual producer task"),
                }
            })
            .collect();
        super::ScheduleFacts {
            execution,
            fragments,
            edges,
            contexts: vec![context],
            root,
        }
    }

    #[test]
    fn full_manifest_bind_refuses_missing_root_support_for_both_purposes() {
        for purpose in [FullBindPurpose::ClientRows, FullBindPurpose::CountOnly] {
            let execution = execution();
            let legacy = live_target(0, BackendProcessId::new_v7(), 19010);
            let process = legacy.descriptor().process_id();
            let supported_peer =
                full_bind_supported(live_target(1, BackendProcessId::new_v7(), 19011));
            let snapshot = BackendTopologySnapshot::try_new(83, vec![legacy, supported_peer])
                .expect("original snapshot preserves the supported non-root candidate");
            let (revision, backends, eligible_backends) = FrozenAttemptTopology::capture(snapshot)
                .expect("legacy descriptors are representable before selected-root use")
                .into_parts();
            let template = full_bind_native_template(execution, purpose);
            let schedule = full_bind_schedule(execution, process, &template);
            let error = super::TaskManifestBinding::bind_facts(
                template,
                execution,
                schedule,
                revision,
                backends,
                eligible_backends,
            )
            .expect_err("the production bind call must refuse the exact unsupported root");
            assert_eq!(
                error.kind(),
                crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
            );
            assert!(
                error
                    .message()
                    .contains("cannot serve bounded root results")
            );
            assert!(error.message().contains(&process.to_string()));
        }
    }

    #[test]
    fn full_manifest_bind_accepts_supported_root_and_retains_unsupported_non_root_candidate() {
        for purpose in [FullBindPurpose::ClientRows, FullBindPurpose::CountOnly] {
            let execution = execution();
            let supported = full_bind_supported(live_target(0, BackendProcessId::new_v7(), 19010));
            let process = supported.descriptor().process_id();
            let legacy = live_target(1, BackendProcessId::new_v7(), 19011);
            let legacy_process = legacy.descriptor().process_id();
            let snapshot = BackendTopologySnapshot::try_new(84, vec![supported, legacy])
                .expect("one supported root and one legacy non-root candidate");
            let frozen = FrozenAttemptTopology::capture(snapshot).expect("original snapshot");
            let original_eligible = frozen.eligible_backends().as_ptr();
            let expected_eligible = frozen.eligible_backends().to_vec();
            assert!(frozen.eligible_backends().contains(&legacy_process));
            let (revision, backends, eligible_backends) = frozen.into_parts();
            let template = full_bind_native_template(execution, purpose);
            let schedule = full_bind_schedule(execution, process, &template);
            let root = schedule.root;
            let expected_edge = &schedule.edges[0];
            let expected_key = (
                expected_edge.source_fragment_id,
                expected_edge.target_fragment_id,
                expected_edge.target_exchange_node_id,
            );
            let producer = expected_edge.producers[0].0;
            assert_ne!(producer, root);
            let manifest = super::TaskManifestBinding::bind_facts(
                template,
                execution,
                schedule,
                revision,
                backends,
                eligible_backends,
            )
            .expect("production full binding reaches a valid manifest");
            assert_eq!(manifest.execution(), execution);
            assert_eq!(manifest.root(), root);
            assert_eq!(manifest.topology_revision(), 84);
            assert_eq!(manifest.tasks().len(), 2);
            assert_eq!(manifest.contexts().len(), 1);
            assert_eq!(manifest.eligible_backends(), expected_eligible);
            assert_eq!(manifest.eligible_backends().as_ptr(), original_eligible);
            assert!(manifest.eligible_backends().contains(&legacy_process));
            assert_eq!(manifest.edges().len(), 1);
            assert_eq!(
                manifest
                    .tasks()
                    .iter()
                    .map(|task| task.identity())
                    .collect::<std::collections::BTreeSet<_>>(),
                [producer, root].into_iter().collect()
            );
            assert!(
                manifest
                    .tasks()
                    .iter()
                    .all(|task| task.identity().backend_process_id() == process
                        && task.context() == manifest.contexts()[0].context())
            );
            let edge = &manifest.edges()[0];
            assert_eq!(
                (
                    edge.source_fragment_id(),
                    edge.target_fragment_id(),
                    edge.target_exchange_node_id()
                ),
                expected_key
            );
            assert_eq!(
                edge.partition_kind(),
                super::BoundManifestPartitionKind::Unpartitioned
            );
            assert_eq!(edge.producers().len(), 1);
            assert_eq!(edge.producers()[0].task(), producer);
            assert_eq!(edge.producers()[0].sender_ordinal(), 0);
            assert_eq!(edge.destinations(), &[root]);
            assert_eq!(edge.sender_count().get(), 1);
            assert_eq!(manifest.contexts()[0].backend().process_id(), process);
            assert!(
                manifest.contexts()[0]
                    .backend()
                    .target()
                    .descriptor()
                    .require_bounded_root_support()
                    .is_ok()
            );
        }
    }

    #[test]
    fn bound_manifest_keeps_original_eligibility_exclusions_and_unplaced_candidates() {
        let execution = execution();
        let selected = full_bind_supported(live_target(0, BackendProcessId::new_v7(), 19010));
        let process = selected.descriptor().process_id();
        let excluded = live_target(1, BackendProcessId::new_v7(), 19011);
        let excluded_process = excluded.descriptor().process_id();
        let unplaced = live_target(2, BackendProcessId::new_v7(), 19012);
        let unplaced_process = unplaced.descriptor().process_id();
        let snapshot =
            BackendTopologySnapshot::try_new(87, vec![selected, excluded, unplaced]).unwrap();
        let frozen = FrozenAttemptTopology::capture_for_candidate(
            snapshot,
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                excluded_process,
            ),
        )
        .unwrap();
        let original = frozen.eligible_backends().as_ptr();
        let expected = frozen.eligible_backends().to_vec();
        let (revision, backends, eligible) = frozen.into_parts();
        assert!(
            backends.contains_key(&excluded_process),
            "membership facts remain complete despite placement exclusion"
        );
        let template = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let schedule = full_bind_schedule(execution, process, &template);
        let manifest = super::TaskManifestBinding::bind_facts(
            template, execution, schedule, revision, backends, eligible,
        )
        .unwrap();
        assert_eq!(manifest.eligible_backends(), expected);
        assert_eq!(manifest.eligible_backends().as_ptr(), original);
        assert!(manifest.eligible_backends().contains(&unplaced_process));
        assert!(!manifest.eligible_backends().contains(&excluded_process));
        assert_eq!(
            manifest.contexts().len(),
            1,
            "eligible candidates are not participant contexts"
        );
        assert_eq!(
            manifest.contexts()[0].context().backend_process_id(),
            process
        );
        let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
        let selected = manifest
            .selected_network_origins(&transport)
            .map(|origin| origin.unwrap().into_owned().as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            [
                "http://127.0.0.1:19010",
                "http://control-0.test.invalid:19061"
            ]
        );
        assert_eq!(
            selected.len(),
            2,
            "unplaced and excluded candidates create no selected service facts"
        );
    }

    #[test]
    fn full_manifest_bind_cannot_borrow_supported_backend_for_a_foreign_root_identity() {
        let execution = execution();
        let supported = full_bind_supported(live_target(0, BackendProcessId::new_v7(), 19010));
        let process = supported.descriptor().process_id();
        let snapshot = BackendTopologySnapshot::try_new(85, vec![supported]).expect("snapshot");
        let (revision, backends, eligible_backends) = FrozenAttemptTopology::capture(snapshot)
            .unwrap()
            .into_parts();
        let template = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let mut schedule = full_bind_schedule(execution, process, &template);
        let foreign = QueryExecutionId::new(
            execution.query_id(),
            AttemptId::new(2).expect("another nonzero attempt"),
        )
        .expect("foreign attempt");
        schedule.root = TaskIdentity::new(
            foreign,
            schedule.root.stage_id(),
            schedule.root.task_id(),
            process,
        );
        let error = super::TaskManifestBinding::bind_facts(
            template,
            execution,
            schedule,
            revision,
            backends,
            eligible_backends,
        )
        .expect_err("same supported backend cannot replace the complete scheduled task identity");
        assert!(
            error
                .message()
                .contains("root is absent from the scheduled task set")
        );
    }

    #[test]
    fn manifest_root_cannot_borrow_support_from_another_frozen_backend() {
        let legacy = live_target(0, BackendProcessId::new_v7(), 19010);
        let supported = live_target(1, BackendProcessId::new_v7(), 19011);
        let descriptor = supported.descriptor().clone();
        let support = novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
            descriptor.control_endpoint().clone(),
            novarocks_result_contract::RootProfileId::V1,
        );
        let supported = LiveBackendTarget::new(
            1,
            descriptor.with_bounded_root_support(support).unwrap(),
            supported.admission_epoch_capability(),
        );
        let (legacy_root, _) = task(execution(), 1, 1, legacy.descriptor().process_id());
        let (supported_root, _) = task(execution(), 1, 2, supported.descriptor().process_id());
        let snapshot = BackendTopologySnapshot::try_new(71, vec![legacy, supported]).unwrap();
        let (_, backends, _) = FrozenAttemptTopology::capture(snapshot)
            .unwrap()
            .into_parts();

        let error = validate_root_backend_support(legacy_root, &backends)
            .expect_err("a supported peer cannot authorize a legacy root placement");
        assert_eq!(
            error.kind(),
            crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
        );
        assert!(
            error
                .message()
                .contains(&legacy_root.backend_process_id().to_string())
        );
        assert!(
            error
                .message()
                .contains("cannot serve bounded root results")
        );
        validate_root_backend_support(supported_root, &backends)
            .expect("an unsupported non-root candidate does not change the selected root contract");
    }

    #[test]
    fn manifest_root_support_requires_the_exact_frozen_process() {
        let present = live_target(0, BackendProcessId::new_v7(), 19010);
        let (foreign_root, _) = task(execution(), 1, 1, BackendProcessId::new_v7());
        let snapshot = BackendTopologySnapshot::try_new(72, vec![present]).unwrap();
        let (_, backends, _) = FrozenAttemptTopology::capture(snapshot)
            .unwrap()
            .into_parts();
        let error = validate_root_backend_support(foreign_root, &backends)
            .expect_err("root support must come from its original process");
        assert!(error.message().contains("absent from its frozen snapshot"));
    }

    #[test]
    fn successor_selection_preserves_the_complete_owner_snapshot() {
        let failed = BackendProcessId::new_v7();
        let remaining = BackendProcessId::new_v7();
        let snapshot = BackendTopologySnapshot::try_new(
            7,
            vec![
                live_target(0, failed, 18999),
                live_target(1, remaining, 19000),
            ],
        )
        .unwrap();
        let frozen = FrozenAttemptTopology::capture_for_candidate(
            snapshot,
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                failed,
            ),
        )
        .unwrap();
        assert_eq!(frozen.eligible_backends(), &[remaining]);
        assert_eq!(frozen.revision(), 7);
        let (_, retained, _) = frozen.into_parts();
        assert!(
            retained.contains_key(&failed),
            "failed target remains an exact snapshot fact"
        );
        assert!(retained.contains_key(&remaining));
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

        let (_, exact, _) = frozen.into_parts();
        assert_eq!(
            exact[&process].admission_epoch_capability().to_bytes(),
            [0x61; 16]
        );
        let (_, replacement, _) = later.into_parts();
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
        let foreign = PlanScanIdentity::new(
            PlanSeal::Version(
                novarocks_physical_plan::PlanVersionId::try_new([32; 16])
                    .expect("foreign plan version"),
            ),
            expected.fragment_id(),
            expected.node_id(),
        );
        assert_eq!(expected.node_id(), foreign.node_id());
        assert_ne!(expected, foreign);

        let prepared = prepared_scan(expected, PreparedScanSource::RuntimeSplits);
        let tasks = [(expected.fragment_id(), projected_tasks(1))]
            .into_iter()
            .collect();
        let actual = [(
            foreign,
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
        let prepared = prepared_scan(scan, PreparedScanSource::RuntimeSplits);
        let tasks = [(scan.fragment_id(), projected_tasks(2))]
            .into_iter()
            .collect();
        let missing = [(
            scan,
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
        let prepared = prepared_scan(scan, PreparedScanSource::FrozenUnits { unit_count: 0 });
        let tasks = [(scan.fragment_id(), projected_tasks(2))]
            .into_iter()
            .collect();
        validate_scan_assignments(&prepared, &tasks, &Default::default())
            .expect("empty work has an exact empty assignment cover");

        let unexpected = [(scan, [(0, frozen(0, 2, 1))].into_iter().collect())]
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
            edges: [(
                (source, target, exchange),
                super::PreparedEdgeFacts {
                    partition_kind: BoundManifestPartitionKind::Hash,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                },
            )]
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
                (
                    (1, 3, 77),
                    super::PreparedEdgeFacts {
                        partition_kind: BoundManifestPartitionKind::Hash,
                        delivery:
                            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    },
                ),
                (
                    (2, 3, 77),
                    super::PreparedEdgeFacts {
                        partition_kind: BoundManifestPartitionKind::Hash,
                        delivery:
                            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    },
                ),
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

    pub(super) fn original_writer_occurrences() -> Vec<super::BoundWriterCohort> {
        let (plan, fragments) =
            crate::query_execution::package_writes::tests::frozen_write_fragments();
        let seal = PlanSeal::Version(plan.version());
        let execution = execution();
        let mut tasks = Vec::new();
        let mut ordinal = 1;
        for fragment in plan.fragments().values() {
            // Two placements deliberately share static facts but never Task identity.
            for instance in 0..2 {
                let (identity, context) = task(
                    execution,
                    fragment.id().get(),
                    ordinal,
                    BackendProcessId::new_v7(),
                );
                ordinal += 1;
                tasks.push(super::BoundManifestTask {
                    identity,
                    context,
                    fragment_id: fragment.id().get(),
                    instance_index: instance,
                    fragment_instance_id: UniqueId::new(1, ordinal as i64),
                    scan_work: Box::default(),
                });
            }
        }

        super::bound_writer_cohorts(seal, &tasks, &fragments).collect::<Vec<_>>()
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn selected_remote_writer_follows_original_edges_when_root_backend_changes() {
        let writers = original_writer_occurrences();
        assert_eq!(writers.len(), 2);
        let execution = writers[0].task().query_execution_id();
        for root_backend in [
            writers[0].task().backend_process_id(),
            writers[1].task().backend_process_id(),
        ] {
            let root = TaskIdentity::new(
                execution,
                StageId::new(77).unwrap(),
                novarocks_types::TaskId::new(91).unwrap(),
                root_backend,
            );
            let edges = writers
                .iter()
                .enumerate()
                .map(|(i, writer)| super::BoundManifestEdge {
                    edge_id: ExchangeEdgeId::new(i as u32 + 1).unwrap(),
                    source_fragment_id: writer.fragment(),
                    target_fragment_id: 77,
                    target_exchange_node_id: 0,
                    partition_kind: BoundManifestPartitionKind::Unpartitioned,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    producers: vec![super::BoundManifestProducer {
                        task: writer.task(),
                        sender_ordinal: 0,
                    }]
                    .into_boxed_slice(),
                    destinations: vec![root].into_boxed_slice(),
                    sender_count: NonZeroU32::new(1).unwrap(),
                })
                .collect::<Vec<_>>();
            let expected = writers
                .iter()
                .find(|writer| writer.task().backend_process_id() != root_backend)
                .unwrap()
                .task();
            assert_eq!(
                super::select_remote_writer_sender(writers.iter().copied(), root, &edges),
                Some(expected)
            );
            assert_eq!(
                super::select_remote_writer_sender(writers.iter().copied().rev(), root, &edges),
                Some(expected),
                "selection is deterministic, independent of writer iteration"
            );
            assert_eq!(
                super::select_remote_writer_sender(writers.iter().copied(), root, &[]),
                None,
                "writer membership alone proves no remote edge"
            );
            assert_eq!(
                super::select_remote_writer_sender(
                    writers
                        .iter()
                        .copied()
                        .filter(|writer| writer.task().backend_process_id() == root_backend),
                    root,
                    &edges
                ),
                None,
                "a same-backend writer is not a remote sender"
            );
            let mut disconnected = edges;
            for edge in &mut disconnected {
                edge.destinations = vec![expected].into_boxed_slice();
            }
            assert_eq!(
                super::select_remote_writer_sender(writers.iter().copied(), root, &disconnected),
                None,
                "an edge to another Task cannot stand in for the Root input"
            );
        }
    }

    #[test]
    fn frozen_writer_cohorts_bind_each_exact_task_to_its_original_node_and_target() {
        let (plan, fragments) =
            crate::query_execution::package_writes::tests::frozen_write_fragments();
        let seal = PlanSeal::Version(plan.version());
        let execution = execution();
        let mut expected = Vec::new();
        let mut tasks = Vec::new();
        let mut ordinal = 1;
        for fragment in plan.fragments().values() {
            // Two placements deliberately share static facts but never Task identity.
            for instance in 0..2 {
                let (identity, context) = task(
                    execution,
                    fragment.id().get(),
                    ordinal,
                    BackendProcessId::new_v7(),
                );
                ordinal += 1;
                tasks.push(super::BoundManifestTask {
                    identity,
                    context,
                    fragment_id: fragment.id().get(),
                    instance_index: instance,
                    fragment_instance_id: UniqueId::new(1, ordinal as i64),
                    scan_work: Box::default(),
                });
                for node in fragment.nodes().values() {
                    if let novarocks_physical_plan::NodeKind::TableWriter { target } = &node.kind {
                        expected.push((
                            identity,
                            fragment.id().get(),
                            node.id,
                            target.write_target_ordinal,
                        ));
                    }
                }
            }
        }
        let cohorts = super::bound_writer_cohorts(seal, &tasks, &fragments).collect::<Vec<_>>();
        assert_eq!(cohorts.len(), 2, "finish placements are not writer cohorts");
        assert_eq!(
            cohorts
                .iter()
                .map(|cohort| (
                    cohort.task(),
                    cohort.fragment(),
                    cohort.writer().node(),
                    cohort.writer().target()
                ))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(cohorts.iter().all(|cohort| cohort.plan() == seal));
        assert_eq!(
            super::bound_summary_sources(&tasks, &fragments).count(),
            0,
            "ordinary writer and finish Tasks declare no logical summary source"
        );
        assert_ne!(cohorts[0].task(), cohorts[1].task());
        assert_eq!(cohorts[0].writer(), cohorts[1].writer());
    }

    #[test]
    fn manifest_submission_requires_the_original_attempt_and_frozen_artifact() {
        let execution = execution();
        let template = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let fragment = std::sync::Arc::clone(
            template
                .template
                .native_template
                .get(template.template.native_template.root())
                .unwrap(),
        );
        let supported = full_bind_supported(live_target(0, BackendProcessId::new_v7(), 19010));
        let process = supported.descriptor().process_id();
        let schedule = full_bind_schedule(execution, process, &template);
        let (revision, backends, eligible_backends) = FrozenAttemptTopology::capture(
            BackendTopologySnapshot::try_new(86, vec![supported]).unwrap(),
        )
        .unwrap()
        .into_parts();
        let manifest = super::TaskManifestBinding::bind_facts(
            template,
            execution,
            schedule,
            revision,
            backends,
            eligible_backends,
        )
        .unwrap();
        manifest
            .validate_submission_fragment(execution, &fragment)
            .unwrap();
        let foreign_execution =
            QueryExecutionId::new(execution.query_id(), AttemptId::new(2).unwrap()).unwrap();
        assert!(
            manifest
                .validate_submission_fragment(foreign_execution, &fragment)
                .is_err()
        );
        // Freeze the exact same valid bytes into a distinct original artifact.
        let other = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let other_fragment = other
            .template
            .native_template
            .get(fragment.facts().fragment_id())
            .unwrap();
        assert_eq!(fragment.content().bytes(), other_fragment.content().bytes());
        assert!(
            manifest
                .validate_submission_fragment(execution, other_fragment)
                .unwrap_err()
                .message()
                .contains("another frozen Native artifact")
        );
        assert_eq!(
            manifest.writer_cohorts().count(),
            0,
            "ordinary read tasks cannot become writers from statuses"
        );
    }

    #[test]
    fn actual_participant_domain_projection_preserves_data_control_and_tls_policy_without_io() {
        use novarocks_native_adapter::FrontendNativeTransport;
        use novarocks_native_trust::{
            AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeTransportMode,
            NativeTrust, ValidatedSharedSecret,
        };
        use novarocks_secret::SecretValue;
        let execution = execution();
        let process = BackendProcessId::new_v7();
        let descriptor = BackendProcessDescriptor::try_new(
            process,
            RuntimeEndpoint::new("2001:0DB8::1", 443).unwrap(),
            RuntimeEndpoint::new("CONTROL.EXAMPLE", 443).unwrap(),
            "test-deployment",
            "test-build",
            NativeCompatibilityId::new([0x71; 32]),
            4096,
        )
        .unwrap();
        let selected = full_bind_supported(LiveBackendTarget::new(
            0,
            descriptor,
            AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
        ));
        let unplaced = live_target(1, BackendProcessId::new_v7(), 19012);
        let frozen = FrozenAttemptTopology::capture(
            BackendTopologySnapshot::try_new(88, vec![selected, unplaced]).unwrap(),
        )
        .unwrap();
        let (revision, backends, eligible) = frozen.into_parts();
        let template = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let schedule = full_bind_schedule(execution, process, &template);
        let manifest = super::TaskManifestBinding::bind_facts(
            template, execution, schedule, revision, backends, eligible,
        )
        .unwrap();
        assert_eq!(manifest.contexts().len(), 1);
        assert_eq!(
            manifest.tasks().len(),
            2,
            "the original real encoder supplies both fragments on the same participant"
        );
        let endpoint = manifest.contexts()[0]
            .backend()
            .endpoint()
            .native_endpoint();
        let trust = NativeTrust::new(
            DeploymentId::parse("manifest-domain-pure").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("fe@localhost:19040").unwrap(),
            NativeTransportMode::Automatic,
        );
        let material = AutomaticTlsMaterial::for_endpoint(trust, endpoint.clone()).unwrap();
        for (transport, expected) in [
            (
                FrontendNativeTransport::plaintext(),
                ["http://[2001:db8::1]:443", "http://control.example:443"],
            ),
            (
                FrontendNativeTransport::automatic(material),
                ["https://[2001:db8::1]", "https://control.example"],
            ),
        ] {
            let facts = manifest
                .selected_network_origins(&transport)
                .map(|origin| origin.unwrap().into_owned().as_str().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(facts, expected);
        }
        // No Native open/activation, split source, client, DNS, socket or task.
    }
    #[cfg(novarocks_dev_controls)]
    pub(super) fn qualification_fixture(
        execution: QueryExecutionId,
        processes: &[BackendProcessId; 3],
        selected: usize,
    ) -> (super::TaskManifestBinding, super::AttemptSchedule) {
        let targets = processes
            .iter()
            .enumerate()
            .map(|(index, &process)| {
                full_bind_supported(live_target(index, process, 19010 + index as u16))
            })
            .collect();
        let topology =
            FrozenAttemptTopology::capture(BackendTopologySnapshot::try_new(91, targets).unwrap())
                .unwrap();
        let template = full_bind_native_template(execution, FullBindPurpose::ClientRows);
        let facts = template.template.attempt_scheduling_facts().unwrap();
        let schedule = novarocks_query_application::test_support::qualification_schedule(
            execution,
            FrontendProcessId::new_v7(),
            &[processes[selected]],
            &facts,
        );
        let manifest = super::TaskManifestBinding::bind(template, &schedule, topology).unwrap();
        (manifest, schedule)
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn qualification_reentry_accepts_same_schedule_and_rejects_same_execution_new_placement() {
        let processes = [
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ];
        let execution = execution();
        let (manifest, original) = qualification_fixture(execution, &processes, 0);
        manifest.validate_schedule(&original).unwrap();
        manifest.validate_schedule(&original).unwrap();
        let (_, changed) = qualification_fixture(execution, &processes, 1);
        assert_eq!(original.execution(), changed.execution());
        assert_ne!(
            original.root().backend_process_id(),
            changed.root().backend_process_id()
        );
        assert!(manifest.validate_schedule(&changed).is_err());
        let (_, different_context) = qualification_fixture(execution, &processes, 0);
        assert_eq!(original.root(), different_context.root());
        assert_ne!(original.contexts(), different_context.contexts());
        assert!(
            manifest.validate_schedule(&different_context).is_err(),
            "same root identity is not the same exact schedule"
        );
        manifest.validate_schedule(&original).unwrap();
    }
}
