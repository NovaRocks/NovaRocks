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

//! Query Application-owned attempt scheduling and Native drive authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, EstablishQueryContext, ExchangeEdgeId, QueryContextRef,
    TaskIdentity,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};

use crate::api::{
    ExecutionSchedulingFacts, NativeScanWork, PlanScanIdentity, RuntimeSplitPlacement,
    ScanSchedulingFacts, SchedulingStreamKind,
};

use super::{
    AdmissionIssueDisposition, AdmissionIssueReceipt, AdmissionIssueSettlement,
    EstablishIssuePermit, LogicalExecutionActorError, RunningAttemptDriveAuthority,
    RunningAttemptPermit,
};
use novarocks_coordination_contract::AttemptActivationIdentity;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttemptScheduleError(Arc<str>);

impl AttemptScheduleError {
    fn new(message: impl Into<Arc<str>>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for AttemptScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AttemptScheduleError {}

/// Read-only placement of one exact Task.
#[derive(Debug)]
pub struct ScheduledTask {
    identity: TaskIdentity,
    context: QueryContextRef,
    instance_index: usize,
    scan_work: Box<[ScheduledScanWork]>,
}

impl ScheduledTask {
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn instance_index(&self) -> usize {
        self.instance_index
    }

    pub fn scan_work(&self) -> &[ScheduledScanWork] {
        &self.scan_work
    }
}

/// Query Application-owned assignment for one exact scan occurrence.
#[derive(Debug, Eq, PartialEq)]
pub struct ScheduledScanWork {
    scan: PlanScanIdentity,
    assignment: ScheduledScanAssignment,
}

impl ScheduledScanWork {
    pub const fn scan(&self) -> PlanScanIdentity {
        self.scan
    }

    pub const fn assignment(&self) -> &ScheduledScanAssignment {
        &self.assignment
    }
}

/// Exact work assigned to one Task. Frozen units cannot be selected by Native.
#[derive(Debug, Eq, PartialEq)]
pub enum ScheduledScanAssignment {
    RuntimeSplits,
    FrozenUnits { units: ScheduledFrozenUnits },
}

impl ScheduledScanAssignment {
    pub const fn frozen_units(&self) -> Option<&ScheduledFrozenUnits> {
        match self {
            Self::FrozenUnits { units } => Some(units),
            Self::RuntimeSplits => None,
        }
    }
}

/// Compact strided assignment of frozen unit ordinals to one Task.
///
/// Cardinality may be large, so scheduling never allocates one in-memory
/// ordinal per unit. Native can enumerate this exact finite assignment while
/// binding its attempt-local source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledFrozenUnits {
    first_ordinal: usize,
    stride: NonZeroUsize,
    len: NonZeroUsize,
}

impl ScheduledFrozenUnits {
    pub const fn first_ordinal(self) -> usize {
        self.first_ordinal
    }

    pub const fn stride(self) -> NonZeroUsize {
        self.stride
    }

    pub const fn len(self) -> NonZeroUsize {
        self.len
    }

    pub fn ordinals(self) -> impl ExactSizeIterator<Item = usize> + DoubleEndedIterator {
        (0..self.len.get()).map(move |offset| self.first_ordinal + offset * self.stride.get())
    }
}

/// Read-only placement of every instance of one sealed SQL fragment.
#[derive(Debug)]
pub struct ScheduledFragment {
    fragment_id: u32,
    stage_id: StageId,
    tasks: Box<[ScheduledTask]>,
}

/// One producer and its stable ordinal in an exchange node's complete sender set.
#[derive(Debug, Eq, PartialEq)]
pub struct ScheduledProducer {
    task: TaskIdentity,
    sender_ordinal: u32,
}

impl ScheduledProducer {
    pub const fn task(&self) -> TaskIdentity {
        self.task
    }

    pub const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }
}

/// Complete Task membership of one sealed inter-fragment exchange edge.
#[derive(Debug, Eq, PartialEq)]
pub struct ScheduledEdge {
    edge_id: ExchangeEdgeId,
    source_fragment_id: u32,
    target_fragment_id: u32,
    target_exchange_node_id: i32,
    producers: Box<[ScheduledProducer]>,
    destinations: Box<[TaskIdentity]>,
    sender_count: NonZeroU32,
}

impl ScheduledEdge {
    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub const fn source_fragment_id(&self) -> u32 {
        self.source_fragment_id
    }

    pub const fn target_fragment_id(&self) -> u32 {
        self.target_fragment_id
    }

    pub const fn target_exchange_node_id(&self) -> i32 {
        self.target_exchange_node_id
    }

    pub fn producers(&self) -> &[ScheduledProducer] {
        &self.producers
    }

    pub fn destinations(&self) -> &[TaskIdentity] {
        &self.destinations
    }

    pub const fn sender_count(&self) -> NonZeroU32 {
        self.sender_count
    }
}

impl ScheduledFragment {
    pub const fn fragment_id(&self) -> u32 {
        self.fragment_id
    }

    pub const fn stage_id(&self) -> StageId {
        self.stage_id
    }

    pub fn tasks(&self) -> &[ScheduledTask] {
        &self.tasks
    }
}

/// Query Application-owned immutable manifest for one exact attempt.
///
/// Fields are private and the value is not cloneable. Native receives only
/// borrowed read views and therefore cannot select or replace placement.
#[derive(Debug)]
pub struct AttemptSchedule {
    execution: QueryExecutionId,
    fragments: Box<[ScheduledFragment]>,
    edges: Box<[ScheduledEdge]>,
    contexts: Box<[QueryContextRef]>,
    root: TaskIdentity,
}

impl AttemptSchedule {
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub fn fragments(&self) -> &[ScheduledFragment] {
        &self.fragments
    }

    pub fn edges(&self) -> &[ScheduledEdge] {
        &self.edges
    }

    pub fn contexts(&self) -> &[QueryContextRef] {
        &self.contexts
    }

    pub const fn root(&self) -> TaskIdentity {
        self.root
    }
}

pub(crate) fn build_attempt_schedule(
    execution: QueryExecutionId,
    frontend_process_id: FrontendProcessId,
    eligible_backends: &[BackendProcessId],
    plan: &ExecutionSchedulingFacts,
    #[cfg(novarocks_dev_controls)] preferred_process: Result<Option<BackendProcessId>, String>,
) -> Result<AttemptSchedule, AttemptScheduleError> {
    if eligible_backends.is_empty() {
        return Err(AttemptScheduleError::new(
            "attempt scheduling requires at least one eligible backend",
        ));
    }
    if eligible_backends
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != eligible_backends.len()
    {
        return Err(AttemptScheduleError::new(
            "attempt scheduling received duplicate eligible backends",
        ));
    }
    let mut backends = eligible_backends.to_vec();
    backends.sort_unstable();

    let mut scan_seen = BTreeSet::new();
    for fragment in &plan.fragments {
        for scan in &fragment.scans {
            if !scan_seen.insert(scan.scan) {
                return Err(AttemptScheduleError::new(
                    "attempt scheduling received duplicate scan work",
                ));
            }
        }
    }
    let fragments_by_id = plan
        .fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect::<BTreeMap<_, _>>();
    if fragments_by_id.len() != plan.fragments.len() {
        return Err(AttemptScheduleError::new(
            "scheduling projection repeats a fragment",
        ));
    }

    let ordered = plan
        .topological_fragment_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered.len() != plan.topological_fragment_order.len()
        || ordered != fragments_by_id.keys().copied().collect()
    {
        return Err(AttemptScheduleError::new(
            "scheduling order is not an exact fragment permutation",
        ));
    }

    // Placement and row partitioning answer different questions. Evaluate
    // each frozen stage policy against this one retained backend snapshot.
    let mut predecessors = BTreeMap::<u32, BTreeSet<u32>>::new();
    let mut gathers = BTreeSet::new();
    let positions = plan
        .topological_fragment_order
        .iter()
        .enumerate()
        .map(|(position, &fragment)| (fragment, position))
        .collect::<BTreeMap<_, _>>();
    for edge in &plan.edges {
        let source = positions.get(&edge.source_fragment_id);
        let target = positions.get(&edge.target_fragment_id);
        if !matches!((source, target), (Some(source), Some(target)) if source < target) {
            return Err(AttemptScheduleError::new(
                "scheduling edge violates the exact topological fragment order",
            ));
        }
        predecessors
            .entry(edge.target_fragment_id)
            .or_default()
            .insert(edge.source_fragment_id);
        if edge.stream_kind == SchedulingStreamKind::Gather {
            gathers.insert(edge.target_fragment_id);
        }
    }

    use novarocks_physical_plan::StagePlacementPolicy;
    let anchor = fragments_by_id
        .get(&plan.execution_anchor_fragment_id)
        .ok_or_else(|| AttemptScheduleError::new("scheduling execution anchor is absent"))?;
    if anchor.placement != StagePlacementPolicy::Singleton {
        return Err(AttemptScheduleError::new(
            "scheduling execution anchor must declare singleton placement",
        ));
    }
    let backend_count = backends.len();
    let mut counts = BTreeMap::<u32, usize>::new();
    for &fragment_id in &plan.topological_fragment_order {
        let fragment = fragments_by_id[&fragment_id];
        if gathers.contains(&fragment_id) && fragment.placement != StagePlacementPolicy::Singleton {
            return Err(AttemptScheduleError::new(
                "gather input conflicts with the declared stage placement",
            ));
        }
        let count = match &fragment.placement {
            StagePlacementPolicy::Singleton => 1,
            StagePlacementPolicy::ScanWork => {
                if fragment.scans.is_empty() {
                    return Err(AttemptScheduleError::new(
                        "scan-work placement requires a declared scan",
                    ));
                }
                scan_fragment_parallelism(&fragment.scans, backend_count)
            }
            StagePlacementPolicy::Spread { max_instances } => {
                max_instances.map_or(backend_count, |cap| backend_count.min(cap.get() as usize))
            }
            StagePlacementPolicy::FollowSources { sources } => {
                if sources.is_empty() || sources.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(AttemptScheduleError::new(
                        "placement sources must be nonempty and strictly ordered",
                    ));
                }
                let mut width = 1;
                for source in sources {
                    let source = source.get();
                    if !predecessors
                        .get(&fragment_id)
                        .is_some_and(|set| set.contains(&source))
                    {
                        return Err(AttemptScheduleError::new(
                            "placement source is not an actual predecessor",
                        ));
                    }
                    let source_width = counts.get(&source).ok_or_else(|| {
                        AttemptScheduleError::new("placement source has not been scheduled")
                    })?;
                    width = width.max(*source_width);
                }
                width
            }
        };
        for scan in &fragment.scans {
            if scan.scan.fragment_id() != fragment_id {
                return Err(AttemptScheduleError::new(
                    "scan work belongs to a different fragment",
                ));
            }
        }
        counts.insert(fragment_id, count);
    }

    let preferred = (execution.query_id().low() as usize) % backend_count;
    #[cfg(novarocks_dev_controls)]
    let preferred = match preferred_process.map_err(AttemptScheduleError::new)? { Some(process) => backends.iter().position(|backend| *backend == process).ok_or_else(|| AttemptScheduleError::new("the exact controlled placement process is absent from the retained eligible set"))?, None => preferred };
    let mut next_stage = 1_u32;
    let mut next_task = 1_u32;
    let mut contexts = BTreeSet::new();
    let mut scheduled = Vec::with_capacity(counts.len());
    for (&fragment_id, &count) in &counts {
        let stage_id = StageId::new(next_stage)
            .map_err(|error| AttemptScheduleError::new(error.to_string()))?;
        next_stage = next_stage
            .checked_add(1)
            .ok_or_else(|| AttemptScheduleError::new("attempt stage id space exhausted"))?;
        let fragment = fragments_by_id[&fragment_id];
        let mut tasks = Vec::with_capacity(count);
        for instance_index in 0..count {
            let live_index = if count == 1 {
                preferred
            } else if count == backend_count {
                instance_index
            } else {
                (preferred + instance_index) % backend_count
            };
            let backend = backends[live_index];
            let task_id = TaskId::new(next_task)
                .map_err(|error| AttemptScheduleError::new(error.to_string()))?;
            next_task = next_task
                .checked_add(1)
                .ok_or_else(|| AttemptScheduleError::new("attempt task id space exhausted"))?;
            let context = QueryContextRef::new(execution, frontend_process_id, backend);
            contexts.insert(context);
            tasks.push(ScheduledTask {
                identity: TaskIdentity::new(execution, stage_id, task_id, backend),
                context,
                instance_index,
                scan_work: Box::new([]),
            });
        }
        assign_scan_work(&fragment.scans, &mut tasks)?;
        scheduled.push(ScheduledFragment {
            fragment_id,
            stage_id,
            tasks: tasks.into_boxed_slice(),
        });
    }

    let root = scheduled
        .iter()
        .find(|fragment| fragment.fragment_id == plan.execution_anchor_fragment_id)
        .and_then(|fragment| fragment.tasks.first())
        .map(|task| task.identity)
        .ok_or_else(|| AttemptScheduleError::new("attempt schedule has no root task"))?;
    let edges = schedule_edges(plan, &scheduled)?;
    Ok(AttemptSchedule {
        execution,
        fragments: scheduled.into_boxed_slice(),
        edges: edges.into_boxed_slice(),
        contexts: contexts.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        root,
    })
}

/// Explicit finite protocol placements, without applying the production planner's
/// parallelism policy. The real edge author still validates every exchange.
#[cfg(novarocks_dev_controls)]
pub(crate) fn protocol_fixture_schedule(
    execution: QueryExecutionId,
    frontend: FrontendProcessId,
    plan: &ExecutionSchedulingFacts,
    placements: &BTreeMap<u32, Vec<BackendProcessId>>,
) -> Result<AttemptSchedule, AttemptScheduleError> {
    let facts = plan
        .fragments
        .iter()
        .map(|f| (f.fragment_id, f))
        .collect::<BTreeMap<_, _>>();
    if facts.keys().copied().collect::<BTreeSet<_>>() != placements.keys().copied().collect() {
        return Err(AttemptScheduleError::new(
            "protocol fixture placement fragments differ from its declared DAG",
        ));
    }
    let mut fragments = Vec::new();
    let mut contexts = BTreeSet::new();
    let mut next_task = 1_u32;
    for (stage_index, (&fragment_id, backends)) in placements.iter().enumerate() {
        let stage =
            StageId::new(u32::try_from(stage_index + 1).map_err(|_| {
                AttemptScheduleError::new("protocol fixture stage space exhausted")
            })?)
            .map_err(|error| AttemptScheduleError::new(error.to_string()))?;
        if backends.is_empty() {
            return Err(AttemptScheduleError::new(
                "protocol fixture fragment has no placement",
            ));
        }
        let mut tasks = Vec::new();
        for (instance_index, &backend) in backends.iter().enumerate() {
            let id = TaskId::new(next_task)
                .map_err(|error| AttemptScheduleError::new(error.to_string()))?;
            next_task = next_task.checked_add(1).ok_or_else(|| {
                AttemptScheduleError::new("protocol fixture task space exhausted")
            })?;
            let context = QueryContextRef::new(execution, frontend, backend);
            contexts.insert(context);
            tasks.push(ScheduledTask {
                identity: TaskIdentity::new(execution, stage, id, backend),
                context,
                instance_index,
                scan_work: Box::new([]),
            });
        }
        assign_scan_work(&facts[&fragment_id].scans, &mut tasks)?;
        fragments.push(ScheduledFragment {
            fragment_id,
            stage_id: stage,
            tasks: tasks.into_boxed_slice(),
        });
    }
    let root = fragments
        .iter()
        .find(|f| f.fragment_id == plan.execution_anchor_fragment_id)
        .and_then(|f| f.tasks.first())
        .map(|t| t.identity)
        .ok_or_else(|| AttemptScheduleError::new("protocol fixture root has no placement"))?;
    let edges = schedule_edges(plan, &fragments)?;
    Ok(AttemptSchedule {
        execution,
        fragments: fragments.into_boxed_slice(),
        edges: edges.into_boxed_slice(),
        contexts: contexts.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        root,
    })
}

fn scan_fragment_parallelism(scans: &[ScanSchedulingFacts], backend_count: usize) -> usize {
    let mut parallelism = 1;
    for scan in scans {
        match scan.work {
            NativeScanWork::Empty => {}
            NativeScanWork::RuntimeSplits { placement } => {
                let contribution = match placement {
                    RuntimeSplitPlacement::SingleUnit => 1,
                    RuntimeSplitPlacement::Spread => backend_count,
                };
                parallelism = parallelism.max(contribution);
            }
            NativeScanWork::FrozenUnits { count } => parallelism = parallelism.max(count.get()),
        }
    }
    parallelism.clamp(1, backend_count)
}

fn assign_scan_work(
    scans: &[ScanSchedulingFacts],
    tasks: &mut [ScheduledTask],
) -> Result<(), AttemptScheduleError> {
    let mut assigned = (0..tasks.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for &ScanSchedulingFacts { scan, work } in scans {
        match work {
            NativeScanWork::Empty => {}
            NativeScanWork::RuntimeSplits { .. } => {
                for task in &mut assigned {
                    task.push(ScheduledScanWork {
                        scan,
                        assignment: ScheduledScanAssignment::RuntimeSplits,
                    });
                }
            }
            NativeScanWork::FrozenUnits { count } => {
                let total = count.get();
                let task_count = assigned.len();
                let stride = NonZeroUsize::new(task_count).ok_or_else(|| {
                    AttemptScheduleError::new("frozen scan work has no scheduled task")
                })?;
                for (index, task) in assigned.iter_mut().enumerate() {
                    if index < total {
                        let len = 1 + (total - 1 - index) / task_count;
                        task.push(ScheduledScanWork {
                            scan,
                            assignment: ScheduledScanAssignment::FrozenUnits {
                                units: ScheduledFrozenUnits {
                                    first_ordinal: index,
                                    stride,
                                    len: NonZeroUsize::new(len)
                                        .expect("nonempty strided assignment has nonzero length"),
                                },
                            },
                        });
                    }
                }
            }
        }
    }
    for (task, scan_work) in tasks.iter_mut().zip(assigned) {
        task.scan_work = scan_work.into_boxed_slice();
    }
    Ok(())
}

fn schedule_edges(
    plan: &ExecutionSchedulingFacts,
    fragments: &[ScheduledFragment],
) -> Result<Vec<ScheduledEdge>, AttemptScheduleError> {
    let tasks_by_fragment = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment.tasks.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let mut sources_by_exchange = BTreeMap::<(u32, i32), BTreeSet<u32>>::new();
    for edge in &plan.edges {
        let key = (edge.target_fragment_id, edge.target_exchange_node_id);
        if !sources_by_exchange
            .entry(key)
            .or_default()
            .insert(edge.source_fragment_id)
        {
            return Err(AttemptScheduleError::new(format!(
                "scheduling repeats edge {} -> {} at exchange node {}",
                edge.source_fragment_id, edge.target_fragment_id, edge.target_exchange_node_id
            )));
        }
    }

    let mut sender_sets = BTreeMap::<(u32, i32), (BTreeMap<TaskIdentity, u32>, NonZeroU32)>::new();
    for (&key, sources) in &sources_by_exchange {
        let mut ordinals = BTreeMap::new();
        let mut next = 0_u32;
        for source in sources {
            let tasks = tasks_by_fragment.get(source).ok_or_else(|| {
                AttemptScheduleError::new(format!("exchange source fragment {source} is absent"))
            })?;
            for task in *tasks {
                ordinals.insert(task.identity, next);
                next = next.checked_add(1).ok_or_else(|| {
                    AttemptScheduleError::new("exchange sender ordinal space exhausted")
                })?;
            }
        }
        let count = NonZeroU32::new(next).ok_or_else(|| {
            AttemptScheduleError::new(format!(
                "exchange node {} of fragment {} has no producer",
                key.1, key.0
            ))
        })?;
        sender_sets.insert(key, (ordinals, count));
    }

    let mut next_edge_id = Some(1_u32);
    plan.edges
        .iter()
        .map(|edge| {
            let edge_id = take_exchange_edge_id(&mut next_edge_id)?;
            let key = (edge.target_fragment_id, edge.target_exchange_node_id);
            let (ordinals, sender_count) = &sender_sets[&key];
            let producers = tasks_by_fragment
                .get(&edge.source_fragment_id)
                .ok_or_else(|| {
                    AttemptScheduleError::new(format!(
                        "exchange source fragment {} is absent",
                        edge.source_fragment_id
                    ))
                })?
                .iter()
                .map(|task| ScheduledProducer {
                    task: task.identity,
                    sender_ordinal: ordinals[&task.identity],
                })
                .collect::<Vec<_>>();
            let destinations = tasks_by_fragment
                .get(&edge.target_fragment_id)
                .ok_or_else(|| {
                    AttemptScheduleError::new(format!(
                        "exchange target fragment {} is absent",
                        edge.target_fragment_id
                    ))
                })?
                .iter()
                .map(|task| task.identity)
                .collect::<Vec<_>>();
            Ok(ScheduledEdge {
                edge_id,
                source_fragment_id: edge.source_fragment_id,
                target_fragment_id: edge.target_fragment_id,
                target_exchange_node_id: edge.target_exchange_node_id,
                producers: producers.into_boxed_slice(),
                destinations: destinations.into_boxed_slice(),
                sender_count: *sender_count,
            })
        })
        .collect()
}

fn take_exchange_edge_id(next: &mut Option<u32>) -> Result<ExchangeEdgeId, AttemptScheduleError> {
    let current = next
        .take()
        .ok_or_else(|| AttemptScheduleError::new("exchange edge id space exhausted"))?;
    let edge_id = ExchangeEdgeId::new(current)
        .map_err(|error| AttemptScheduleError::new(error.to_string()))?;
    *next = current.checked_add(1);
    Ok(edge_id)
}

/// Narrow Native capability for actor-owned admission and Establish effects.
///
/// The running permit remains private to the Supervisor. This capability has
/// no terminal or result-delivery authority and becomes stale when that permit
/// is consumed.
pub struct NativeAttemptDrive {
    authority: RunningAttemptDriveAuthority,
}

impl fmt::Debug for NativeAttemptDrive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAttemptDrive")
            .field("identity", &self.authority.identity())
            .finish_non_exhaustive()
    }
}

impl NativeAttemptDrive {
    pub(crate) fn new(permit: &RunningAttemptPermit) -> Self {
        Self {
            authority: permit.native_drive_authority(),
        }
    }

    pub fn identity(&self) -> AttemptActivationIdentity {
        self.authority.identity()
    }

    /// Ask the current logical owner whether this exact refusal may wait for
    /// independent process-replacement evidence within its ORIGINAL horizon.
    /// A positive answer grants no retry, topology, terminal, or exit authority;
    /// callers must ask again before continuing a pending observation. Refusal
    /// or a stale drive leaves the original failure unchanged.
    pub async fn may_observe_pre_ready_replacement(
        &self,
        refusal: &crate::execution_error::ExecutionFailureInput,
    ) -> Result<bool, LogicalExecutionActorError> {
        self.authority
            .may_observe_pre_ready_replacement(refusal)
            .await
    }

    pub async fn begin_admission_issue(
        &self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> Result<AdmissionIssueReceipt, LogicalExecutionActorError> {
        self.authority.begin_admission_issue(request).await
    }

    pub async fn settle_admission_issue(
        &self,
        issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<AdmissionIssueDisposition, LogicalExecutionActorError> {
        self.authority
            .settle_admission_issue(issue, settlement)
            .await
    }

    pub async fn authorize_establish(
        &self,
        request: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        self.authority
            .authorize_establish(request, native_compatibility_id)
            .await
    }

    pub async fn reauthorize_establish(
        &self,
        context: QueryContextRef,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        self.authority.reauthorize_establish(context).await
    }
}

#[cfg(test)]
mod tests {
    mod placement {
        use super::*;
        include!("schedule_placement_tests.rs");
    }
    use super::*;
    use crate::api::{FragmentSchedulingFacts, PlanSeal, SchedulingEdgeFacts};
    use novarocks_types::identity::{AttemptId, QueryId};

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn backends(count: usize) -> Vec<BackendProcessId> {
        (0..count).map(|_| BackendProcessId::new_v7()).collect()
    }

    /// Two fragments with one completed scan and one gather boundary. The
    /// scheduler consumes these values, not a planner tree.
    fn scan_edge_inputs(work: NativeScanWork) -> ExecutionSchedulingFacts {
        let candidate = crate::completed_plan_fixture::completed_scan_candidate([57; 16]);
        let scan = PlanScanIdentity::new(PlanSeal::Version(candidate.plan().version()), 1, 0);
        ExecutionSchedulingFacts {
            topological_fragment_order: vec![1, 2],
            execution_anchor_fragment_id: 2,
            fragments: vec![
                FragmentSchedulingFacts {
                    fragment_id: 1,
                    placement: novarocks_physical_plan::StagePlacementPolicy::ScanWork,
                    scans: vec![ScanSchedulingFacts { scan, work }],
                },
                FragmentSchedulingFacts {
                    fragment_id: 2,
                    placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
                    scans: Vec::new(),
                },
            ],
            edges: vec![SchedulingEdgeFacts {
                source_fragment_id: 1,
                target_fragment_id: 2,
                target_exchange_node_id: 0,
                stream_kind: SchedulingStreamKind::Gather,
                hash_partitioned: false,
            }],
        }
    }

    fn build_scan_edge(
        execution: QueryExecutionId,
        frontend: FrontendProcessId,
        backends: &[BackendProcessId],
        work: NativeScanWork,
    ) -> AttemptSchedule {
        build_attempt_schedule(
            execution,
            frontend,
            backends,
            &scan_edge_inputs(work),
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap()
    }

    #[test]
    fn empty_eligible_backend_set_refuses_distributed_scheduling() {
        let error = build_attempt_schedule(
            execution(1),
            FrontendProcessId::new_v7(),
            &[],
            &scan_edge_inputs(NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            }),
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .expect_err("distributed scheduling requires an eligible backend");
        assert_eq!(
            error.to_string(),
            "attempt scheduling requires at least one eligible backend",
        );
    }

    #[test]
    fn runtime_splits_fan_out_and_freeze_exchange_membership() {
        let execution = execution(1);
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let schedule = build_scan_edge(
            execution,
            frontend,
            &backends,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );

        let scan_fragment = schedule
            .fragments()
            .iter()
            .find(|fragment| !fragment.tasks()[0].scan_work().is_empty())
            .unwrap();
        assert_eq!(scan_fragment.tasks().len(), 3);
        assert!(scan_fragment.tasks().iter().all(|task| matches!(
            task.scan_work()[0].assignment(),
            ScheduledScanAssignment::RuntimeSplits
        )));
        assert_eq!(schedule.root().query_execution_id(), execution);
        assert_eq!(
            schedule
                .fragments()
                .iter()
                .find(|fragment| fragment
                    .tasks()
                    .iter()
                    .any(|task| task.identity() == schedule.root()))
                .unwrap()
                .tasks()
                .len(),
            1
        );
        assert_eq!(schedule.contexts().len(), 3);
        assert!(schedule.contexts().iter().all(|context| {
            context.query_execution_id() == execution && context.frontend_process_id() == frontend
        }));

        let edge = &schedule.edges()[0];
        assert_eq!(edge.edge_id().get(), 1);
        assert_eq!(edge.producers().len(), 3);
        assert_eq!(edge.destinations().len(), 1);
        assert_eq!(edge.sender_count().get(), 3);
        assert_eq!(
            edge.producers()
                .iter()
                .map(ScheduledProducer::sender_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn single_unit_runtime_scan_uses_one_task_with_runtime_assignment() {
        let schedule = build_scan_edge(
            execution(1),
            FrontendProcessId::new_v7(),
            &backends(3),
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::SingleUnit,
            },
        );
        let scan = &schedule.fragments()[0];
        assert_eq!(scan.tasks().len(), 1);
        assert!(matches!(
            scan.tasks()[0].scan_work()[0].assignment(),
            ScheduledScanAssignment::RuntimeSplits
        ));
        assert_eq!(schedule.edges()[0].sender_count().get(), 1);
    }

    #[test]
    fn single_unit_scan_does_not_constrain_a_spread_scan_in_the_same_fragment() {
        let mut inputs = scan_edge_inputs(NativeScanWork::RuntimeSplits {
            placement: RuntimeSplitPlacement::SingleUnit,
        });
        let single = inputs.fragments[0].scans[0].scan;
        let spread = PlanScanIdentity::new(single.plan(), single.fragment_id(), 1);
        inputs.fragments[0].scans.push(ScanSchedulingFacts {
            scan: spread,
            work: NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        });
        let schedule = build_attempt_schedule(
            execution(1),
            FrontendProcessId::new_v7(),
            &backends(3),
            &inputs,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap();
        let tasks = schedule.fragments()[0].tasks();
        assert_eq!(tasks.len(), 3);
        for task in tasks {
            assert_eq!(task.scan_work().len(), 2);
            assert_eq!(task.scan_work()[0].scan(), single);
            assert_eq!(task.scan_work()[1].scan(), spread);
            assert!(
                task.scan_work().iter().all(|work| matches!(
                    work.assignment(),
                    ScheduledScanAssignment::RuntimeSplits
                ))
            );
        }
        assert_eq!(schedule.edges()[0].sender_count().get(), 3);
    }

    #[test]
    fn ignoring_single_unit_hint_still_assigns_runtime_splits_to_every_task() {
        // The provider still enumerates one unit. Choosing wider placement
        // changes only the number of receivers, not the assignment protocol.
        let mut inputs = scan_edge_inputs(NativeScanWork::RuntimeSplits {
            placement: RuntimeSplitPlacement::SingleUnit,
        });
        inputs.fragments[0].scans[0].work = NativeScanWork::RuntimeSplits {
            placement: RuntimeSplitPlacement::Spread,
        };
        let schedule = build_attempt_schedule(
            execution(1),
            FrontendProcessId::new_v7(),
            &backends(3),
            &inputs,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap();
        assert_eq!(schedule.fragments()[0].tasks().len(), 3);
        assert!(schedule.fragments()[0].tasks().iter().all(|task| {
            task.scan_work().len() == 1
                && matches!(
                    task.scan_work()[0].assignment(),
                    ScheduledScanAssignment::RuntimeSplits
                )
        }));
    }

    #[test]
    fn frozen_units_have_exact_task_assignments() {
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let frozen = build_scan_edge(
            execution(2),
            frontend,
            &backends,
            NativeScanWork::FrozenUnits {
                count: std::num::NonZeroUsize::new(5).unwrap(),
            },
        );
        let frozen_scan = frozen
            .fragments()
            .iter()
            .find(|fragment| !fragment.tasks()[0].scan_work().is_empty())
            .unwrap();
        assert_eq!(frozen_scan.tasks().len(), 3);
        let mut units = frozen_scan
            .tasks()
            .iter()
            .flat_map(|task| {
                task.scan_work()[0]
                    .assignment()
                    .frozen_units()
                    .unwrap()
                    .ordinals()
            })
            .collect::<Vec<_>>();
        units.sort_unstable();
        assert_eq!(units, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn empty_static_scan_keeps_one_fragment_task_without_scan_assignment() {
        let execution = execution(1);
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let scheduling = scan_edge_inputs(NativeScanWork::Empty);
        let scan_fragment_id = scheduling
            .fragments
            .iter()
            .find(|fragment| !fragment.scans.is_empty())
            .unwrap()
            .fragment_id;
        let schedule = build_attempt_schedule(
            execution,
            frontend,
            &backends,
            &scheduling,
            #[cfg(novarocks_dev_controls)]
            Ok(None),
        )
        .unwrap();
        let scan_fragment = schedule
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id() == scan_fragment_id)
            .unwrap();

        assert_eq!(scan_fragment.tasks().len(), 1);
        assert!(scan_fragment.tasks()[0].scan_work().is_empty());
    }

    #[test]
    fn frozen_unit_assignment_omits_empty_task_entries() {
        let scan = scan_edge_inputs(NativeScanWork::Empty)
            .fragments
            .iter()
            .find_map(|fragment| fragment.scans.first().map(|scan| scan.scan))
            .expect("the stream-edge fixture reads one provider");
        let execution = execution(1);
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let stage = StageId::new(1).unwrap();
        let mut tasks = backends
            .iter()
            .enumerate()
            .map(|(index, &backend)| ScheduledTask {
                identity: TaskIdentity::new(
                    execution,
                    stage,
                    TaskId::new((index + 1) as u32).unwrap(),
                    backend,
                ),
                context: QueryContextRef::new(execution, frontend, backend),
                instance_index: index,
                scan_work: Box::new([]),
            })
            .collect::<Vec<_>>();
        assign_scan_work(
            &[ScanSchedulingFacts {
                scan,
                work: NativeScanWork::FrozenUnits {
                    count: std::num::NonZeroUsize::new(1).unwrap(),
                },
            }],
            &mut tasks,
        )
        .unwrap();
        assert_eq!(
            tasks[0].scan_work()[0]
                .assignment()
                .frozen_units()
                .unwrap()
                .ordinals()
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert!(tasks[1].scan_work().is_empty());
        assert!(tasks[2].scan_work().is_empty());
    }

    #[test]
    fn maximum_frozen_unit_count_stays_compact_and_exact() {
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let schedule = build_scan_edge(
            execution(1),
            frontend,
            &backends,
            NativeScanWork::FrozenUnits {
                count: NonZeroUsize::new(usize::MAX).unwrap(),
            },
        );
        let scan_fragment = schedule
            .fragments()
            .iter()
            .find(|fragment| !fragment.tasks()[0].scan_work().is_empty())
            .unwrap();
        let assignments = scan_fragment
            .tasks()
            .iter()
            .map(|task| *task.scan_work()[0].assignment().frozen_units().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(assignments.len(), 3);
        assert_eq!(
            assignments
                .iter()
                .map(|units| units.len().get() as u128)
                .sum::<u128>(),
            usize::MAX as u128
        );
        for (index, units) in assignments.into_iter().enumerate() {
            assert_eq!(units.first_ordinal(), index);
            assert_eq!(units.stride().get(), 3);
            assert!(units.ordinals().next_back().unwrap() < usize::MAX);
        }
    }

    #[test]
    fn a_new_attempt_mints_an_entirely_new_identity_manifest() {
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let first = build_scan_edge(
            execution(1),
            frontend,
            &backends,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );
        let second = build_scan_edge(
            execution(2),
            frontend,
            &backends,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );
        assert_ne!(first.root(), second.root());
        assert_eq!(
            first
                .edges()
                .iter()
                .map(ScheduledEdge::edge_id)
                .collect::<Vec<_>>(),
            second
                .edges()
                .iter()
                .map(ScheduledEdge::edge_id)
                .collect::<Vec<_>>(),
            "the same frozen SQL edge order keeps stable attempt-local ids"
        );
        assert!(
            first
                .fragments()
                .iter()
                .flat_map(ScheduledFragment::tasks)
                .all(|task| task.identity().query_execution_id() == execution(1))
        );
        assert!(
            second
                .fragments()
                .iter()
                .flat_map(ScheduledFragment::tasks)
                .all(|task| task.identity().query_execution_id() == execution(2))
        );
    }

    #[test]
    fn eligible_backend_set_order_cannot_change_the_schedule() {
        let execution = execution(1);
        let frontend = FrontendProcessId::new_v7();
        let mut forward = backends(3);
        let mut reverse = forward.clone();
        reverse.reverse();

        let first = build_scan_edge(
            execution,
            frontend,
            &forward,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );
        let second = build_scan_edge(
            execution,
            frontend,
            &reverse,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );
        let identities = |schedule: &AttemptSchedule| {
            schedule
                .fragments()
                .iter()
                .flat_map(ScheduledFragment::tasks)
                .map(ScheduledTask::identity)
                .collect::<Vec<_>>()
        };

        assert_eq!(identities(&first), identities(&second));
        assert_eq!(
            first
                .edges()
                .iter()
                .map(|edge| edge.edge_id().get())
                .collect::<Vec<_>>(),
            second
                .edges()
                .iter()
                .map(|edge| edge.edge_id().get())
                .collect::<Vec<_>>()
        );
        assert_eq!(first.contexts(), second.contexts());
        forward.sort_unstable();
        assert_eq!(
            first.contexts(),
            forward
                .into_iter()
                .map(|backend| QueryContextRef::new(execution, frontend, backend))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn exchange_edge_ids_are_contiguous_and_exhaustion_is_explicit() {
        let mut next = Some(1);
        assert_eq!(take_exchange_edge_id(&mut next).unwrap().get(), 1);
        assert_eq!(take_exchange_edge_id(&mut next).unwrap().get(), 2);

        let mut last = Some(u32::MAX);
        assert_eq!(take_exchange_edge_id(&mut last).unwrap().get(), u32::MAX);
        assert_eq!(
            take_exchange_edge_id(&mut last).unwrap_err().to_string(),
            "exchange edge id space exhausted"
        );
    }
    #[test]
    #[cfg(novarocks_dev_controls)]
    fn controlled_single_fragment_placement_requires_the_retained_exact_process() {
        let execution =
            QueryExecutionId::new(QueryId::new(53, 59), AttemptId::new(1).unwrap()).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let backends = [
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        ];
        let plan = scan_edge_inputs(NativeScanWork::RuntimeSplits {
            placement: RuntimeSplitPlacement::SingleUnit,
        });
        let schedule =
            build_attempt_schedule(execution, frontend, &backends, &plan, Ok(Some(backends[2])))
                .unwrap();
        assert_eq!(
            schedule.contexts().len(),
            1,
            "a placement preference cannot mint service-only contexts"
        );
        assert!(
            schedule
                .fragments()
                .iter()
                .flat_map(ScheduledFragment::tasks)
                .all(|task| task.identity().backend_process_id() == backends[2])
        );
        assert!(
            build_attempt_schedule(
                execution,
                frontend,
                &backends,
                &plan,
                Ok(Some(BackendProcessId::new_v7()))
            )
            .unwrap_err()
            .to_string()
            .contains("retained eligible")
        );
        assert!(
            build_attempt_schedule(
                execution,
                frontend,
                &backends,
                &plan,
                Err("owner preference failed".into())
            )
            .unwrap_err()
            .to_string()
            .contains("owner preference failed")
        );
    }
}
