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
    AcquireQueryContextAdmissionTicket, EstablishQueryContext, QueryContextRef, TaskIdentity,
};
use novarocks_sql::plan_read::{FragmentId, FragmentStreamKind};
use novarocks_sql::planning::query_execution::{SealedScanIdentity, SqlExecutionSchedulingFacts};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};

use crate::api::{NativeScanWork, NativeScanWorkFact};

use super::{
    AdmissionIssueDisposition, AdmissionIssueReceipt, AdmissionIssueSettlement,
    AttemptActivationIdentity, EstablishIssuePermit, LogicalExecutionActorError,
    RunningAttemptPermit,
};

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

/// Query Application-owned assignment for one exact sealed scan occurrence.
#[derive(Debug, Eq, PartialEq)]
pub struct ScheduledScanWork {
    scan: SealedScanIdentity,
    assignment: ScheduledScanAssignment,
}

impl ScheduledScanWork {
    pub const fn scan(&self) -> SealedScanIdentity {
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
    WholeRelation,
    FrozenUnits { units: ScheduledFrozenUnits },
}

impl ScheduledScanAssignment {
    pub const fn frozen_units(&self) -> Option<&ScheduledFrozenUnits> {
        match self {
            Self::FrozenUnits { units } => Some(units),
            Self::RuntimeSplits | Self::WholeRelation => None,
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
    fragment_id: FragmentId,
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
    source_fragment_id: FragmentId,
    target_fragment_id: FragmentId,
    target_exchange_node_id: i32,
    producers: Box<[ScheduledProducer]>,
    destinations: Box<[TaskIdentity]>,
    sender_count: NonZeroU32,
}

impl ScheduledEdge {
    pub const fn source_fragment_id(&self) -> FragmentId {
        self.source_fragment_id
    }

    pub const fn target_fragment_id(&self) -> FragmentId {
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
    pub const fn fragment_id(&self) -> FragmentId {
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
    scan_work: &[NativeScanWorkFact],
    sql: &SqlExecutionSchedulingFacts,
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

    let mut work_by_scan = BTreeMap::new();
    for fact in scan_work {
        if work_by_scan.insert(fact.scan(), fact.work()).is_some() {
            return Err(AttemptScheduleError::new(
                "attempt scheduling received duplicate scan work",
            ));
        }
    }
    let fragments_by_id = sql
        .fragments()
        .iter()
        .map(|fragment| (fragment.fragment_id(), fragment))
        .collect::<BTreeMap<_, _>>();
    if fragments_by_id.len() != sql.fragments().len() {
        return Err(AttemptScheduleError::new(
            "SQL scheduling projection repeats a fragment",
        ));
    }

    let ordered = sql
        .topological_fragment_order()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered.len() != sql.topological_fragment_order().len()
        || ordered != fragments_by_id.keys().copied().collect()
    {
        return Err(AttemptScheduleError::new(
            "SQL scheduling order is not an exact fragment permutation",
        ));
    }

    #[derive(Clone, Copy)]
    struct IncomingEdge {
        source: FragmentId,
        gather: bool,
        hash_partitioned: bool,
    }
    let mut incoming = BTreeMap::<FragmentId, Vec<IncomingEdge>>::new();
    for edge in sql.edges() {
        incoming
            .entry(edge.target_fragment_id())
            .or_default()
            .push(IncomingEdge {
                source: edge.source_fragment_id(),
                gather: edge.stream_kind() == FragmentStreamKind::Gather,
                hash_partitioned: edge.is_hash_partitioned(),
            });
    }

    let backend_count = backends.len();
    let mut counts = BTreeMap::<FragmentId, usize>::new();
    for &fragment_id in sql.topological_fragment_order() {
        let fragment = fragments_by_id.get(&fragment_id).ok_or_else(|| {
            AttemptScheduleError::new(format!("SQL scheduling fragment {fragment_id} is missing"))
        })?;
        let has_gather = incoming
            .get(&fragment_id)
            .is_some_and(|edges| edges.iter().any(|edge| edge.gather));
        let count = if has_gather {
            1
        } else if !fragment.scans().is_empty() {
            scan_fragment_parallelism(fragment.scans(), &work_by_scan, backend_count)?
        } else {
            incoming
                .get(&fragment_id)
                .into_iter()
                .flatten()
                .filter(|edge| edge.hash_partitioned)
                .filter_map(|edge| counts.get(&edge.source).copied())
                .max()
                .unwrap_or(1)
        };
        counts.insert(fragment_id, count);
    }
    if !counts.contains_key(&sql.execution_anchor_fragment_id()) {
        return Err(AttemptScheduleError::new(
            "SQL scheduling execution anchor is absent",
        ));
    }
    counts.insert(sql.execution_anchor_fragment_id(), 1);

    let preferred = (execution.query_id().low() as usize) % backend_count;
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
        assign_scan_work(fragment.scans(), &work_by_scan, &mut tasks)?;
        scheduled.push(ScheduledFragment {
            fragment_id,
            stage_id,
            tasks: tasks.into_boxed_slice(),
        });
    }

    let root = scheduled
        .iter()
        .find(|fragment| fragment.fragment_id == sql.execution_anchor_fragment_id())
        .and_then(|fragment| fragment.tasks.first())
        .map(|task| task.identity)
        .ok_or_else(|| AttemptScheduleError::new("attempt schedule has no root task"))?;
    let edges = schedule_edges(sql, &scheduled)?;
    Ok(AttemptSchedule {
        execution,
        fragments: scheduled.into_boxed_slice(),
        edges: edges.into_boxed_slice(),
        contexts: contexts.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        root,
    })
}

fn scan_fragment_parallelism(
    scans: &[SealedScanIdentity],
    work_by_scan: &BTreeMap<SealedScanIdentity, NativeScanWork>,
    backend_count: usize,
) -> Result<usize, AttemptScheduleError> {
    let mut parallelism = 1;
    for scan in scans {
        match work_by_scan.get(scan).copied().ok_or_else(|| {
            AttemptScheduleError::new(format!(
                "attempt scheduling is missing scan work for node {}",
                scan.node_id()
            ))
        })? {
            NativeScanWork::WholeRelation => return Ok(1),
            NativeScanWork::RuntimeSplits => parallelism = backend_count,
            NativeScanWork::FrozenUnits { count } => parallelism = parallelism.max(count.get()),
        }
    }
    Ok(parallelism.clamp(1, backend_count))
}

fn assign_scan_work(
    scans: &[SealedScanIdentity],
    work_by_scan: &BTreeMap<SealedScanIdentity, NativeScanWork>,
    tasks: &mut [ScheduledTask],
) -> Result<(), AttemptScheduleError> {
    let mut assigned = (0..tasks.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for &scan in scans {
        match work_by_scan.get(&scan).copied().ok_or_else(|| {
            AttemptScheduleError::new(format!(
                "attempt scheduling is missing scan work for node {}",
                scan.node_id()
            ))
        })? {
            NativeScanWork::RuntimeSplits => {
                for task in &mut assigned {
                    task.push(ScheduledScanWork {
                        scan,
                        assignment: ScheduledScanAssignment::RuntimeSplits,
                    });
                }
            }
            NativeScanWork::WholeRelation => assigned[0].push(ScheduledScanWork {
                scan,
                assignment: ScheduledScanAssignment::WholeRelation,
            }),
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
    sql: &SqlExecutionSchedulingFacts,
    fragments: &[ScheduledFragment],
) -> Result<Vec<ScheduledEdge>, AttemptScheduleError> {
    let tasks_by_fragment = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment.tasks.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let mut sources_by_exchange = BTreeMap::<(FragmentId, i32), BTreeSet<FragmentId>>::new();
    for edge in sql.edges() {
        let key = (edge.target_fragment_id(), edge.target_exchange_node_id());
        if !sources_by_exchange
            .entry(key)
            .or_default()
            .insert(edge.source_fragment_id())
        {
            return Err(AttemptScheduleError::new(format!(
                "SQL scheduling repeats edge {} -> {} at exchange node {}",
                edge.source_fragment_id(),
                edge.target_fragment_id(),
                edge.target_exchange_node_id()
            )));
        }
    }

    let mut sender_sets =
        BTreeMap::<(FragmentId, i32), (BTreeMap<TaskIdentity, u32>, NonZeroU32)>::new();
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

    sql.edges()
        .iter()
        .map(|edge| {
            let key = (edge.target_fragment_id(), edge.target_exchange_node_id());
            let (ordinals, sender_count) = &sender_sets[&key];
            let producers = tasks_by_fragment
                .get(&edge.source_fragment_id())
                .ok_or_else(|| {
                    AttemptScheduleError::new(format!(
                        "exchange source fragment {} is absent",
                        edge.source_fragment_id()
                    ))
                })?
                .iter()
                .map(|task| ScheduledProducer {
                    task: task.identity,
                    sender_ordinal: ordinals[&task.identity],
                })
                .collect::<Vec<_>>();
            let destinations = tasks_by_fragment
                .get(&edge.target_fragment_id())
                .ok_or_else(|| {
                    AttemptScheduleError::new(format!(
                        "exchange target fragment {} is absent",
                        edge.target_fragment_id()
                    ))
                })?
                .iter()
                .map(|task| task.identity)
                .collect::<Vec<_>>();
            Ok(ScheduledEdge {
                source_fragment_id: edge.source_fragment_id(),
                target_fragment_id: edge.target_fragment_id(),
                target_exchange_node_id: edge.target_exchange_node_id(),
                producers: producers.into_boxed_slice(),
                destinations: destinations.into_boxed_slice(),
                sender_count: *sender_count,
            })
        })
        .collect()
}

/// Narrow Native capability for actor-owned admission and Establish effects.
///
/// The running permit remains private and is returned only to the Supervisor
/// after Native execution and convergence complete.
pub struct NativeAttemptDrive {
    permit: Option<RunningAttemptPermit>,
}

impl fmt::Debug for NativeAttemptDrive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAttemptDrive")
            .field("identity", &self.permit().identity())
            .finish_non_exhaustive()
    }
}

impl NativeAttemptDrive {
    pub(crate) const fn new(permit: RunningAttemptPermit) -> Self {
        Self {
            permit: Some(permit),
        }
    }

    pub fn identity(&self) -> AttemptActivationIdentity {
        self.permit().identity()
    }

    pub async fn begin_admission_issue(
        &self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> Result<AdmissionIssueReceipt, LogicalExecutionActorError> {
        self.permit().begin_admission_issue(request).await
    }

    pub async fn settle_admission_issue(
        &self,
        issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<AdmissionIssueDisposition, LogicalExecutionActorError> {
        self.permit()
            .settle_admission_issue(issue, settlement)
            .await
    }

    pub async fn authorize_establish(
        &self,
        request: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        self.permit()
            .authorize_establish(request, native_compatibility_id)
            .await
    }

    pub async fn reauthorize_establish(
        &self,
        context: QueryContextRef,
    ) -> Result<EstablishIssuePermit, LogicalExecutionActorError> {
        self.permit().reauthorize_establish(context).await
    }

    pub(crate) fn into_permit(mut self) -> RunningAttemptPermit {
        self.permit
            .take()
            .expect("live Native attempt drive retains its running permit")
    }

    fn permit(&self) -> &RunningAttemptPermit {
        self.permit
            .as_ref()
            .expect("live Native attempt drive retains its running permit")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::planning::query_execution::{
        SealedPreparationPlan, project_execution_scheduling_facts,
    };
    use novarocks_sql::test_support::{NativeEncoderPlanFixture, native_encoder_plan};
    use novarocks_types::identity::{AttemptId, QueryId};

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(91, 92), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn backends(count: usize) -> Vec<BackendProcessId> {
        (0..count).map(|_| BackendProcessId::new_v7()).collect()
    }

    fn scan_edge_inputs() -> (SqlExecutionSchedulingFacts, SealedScanIdentity) {
        let plan = SealedPreparationPlan::seal(
            native_encoder_plan(NativeEncoderPlanFixture::PrunedConnectorScanStreamEdge).unwrap(),
        );
        let scan = plan.scan_contracts().unwrap()[0].identity();
        (project_execution_scheduling_facts(&plan).unwrap(), scan)
    }

    fn build_scan_edge(
        execution: QueryExecutionId,
        frontend: FrontendProcessId,
        backends: &[BackendProcessId],
        work: NativeScanWork,
    ) -> AttemptSchedule {
        let (scheduling, scan) = scan_edge_inputs();
        build_attempt_schedule(
            execution,
            frontend,
            backends,
            &[NativeScanWorkFact::new(scan, work)],
            &scheduling,
        )
        .unwrap()
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
            NativeScanWork::RuntimeSplits,
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
    fn whole_relation_and_frozen_units_have_exact_task_assignments() {
        let frontend = FrontendProcessId::new_v7();
        let backends = backends(3);
        let whole = build_scan_edge(
            execution(1),
            frontend,
            &backends,
            NativeScanWork::WholeRelation,
        );
        let whole_scan = whole
            .fragments()
            .iter()
            .find(|fragment| !fragment.tasks()[0].scan_work().is_empty())
            .unwrap();
        assert_eq!(whole_scan.tasks().len(), 1);
        assert!(matches!(
            whole_scan.tasks()[0].scan_work()[0].assignment(),
            ScheduledScanAssignment::WholeRelation
        ));

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
    fn frozen_unit_assignment_omits_empty_task_entries() {
        let (_, scan) = scan_edge_inputs();
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
        let mut work = BTreeMap::new();
        work.insert(
            scan,
            NativeScanWork::FrozenUnits {
                count: std::num::NonZeroUsize::new(1).unwrap(),
            },
        );
        assign_scan_work(&[scan], &work, &mut tasks).unwrap();
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
            NativeScanWork::RuntimeSplits,
        );
        let second = build_scan_edge(
            execution(2),
            frontend,
            &backends,
            NativeScanWork::RuntimeSplits,
        );
        assert_ne!(first.root(), second.root());
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

        let first = build_scan_edge(execution, frontend, &forward, NativeScanWork::RuntimeSplits);
        let second = build_scan_edge(execution, frontend, &reverse, NativeScanWork::RuntimeSplits);
        let identities = |schedule: &AttemptSchedule| {
            schedule
                .fragments()
                .iter()
                .flat_map(ScheduledFragment::tasks)
                .map(ScheduledTask::identity)
                .collect::<Vec<_>>()
        };

        assert_eq!(identities(&first), identities(&second));
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
}
