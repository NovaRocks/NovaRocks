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

//! The coordinator's per-round split-assignment pump.
//!
//! The pump advances as part of `TaskRound`. Each synchronous Connector call
//! moves one source into the process ordinary blocking-I/O lane; TaskUpdate
//! submission, acknowledgement, retry delay, and source-owner bookkeeping stay
//! on the serial round without occupying that lane. The guard only signals
//! stop, so cancellation and drop never join or block an OS thread.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::read_stack::SplitSourceProfile;

use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::artifact::{PreparedDistributedQuery, ValidatedFragmentSchedule};
use crate::query_execution::split_assignment::{
    AssignmentTarget, RoundSplitAssignment, RoundSplitAssignmentStop, RoundSplitEnumeration,
    RoundSplitEnumerationResult, RoundSplitSource, SplitAssignmentDriverError,
    TaskUpdateRetryPolicy, TaskUpdateTransport, emit_split_source_close_marker,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::execution::QueryTaskExecution;
use crate::task_execution::round::{TaskRound, TurnPump};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_sql::plan_read::FragmentId;

/// How many splits one task may hold before the driver stops pulling for it.
const DEFAULT_MAX_QUEUED_SPLITS_PER_TASK: u64 = 4096;

/// The admitted tasks that will read each typed scan node.
///
/// Derived from the schedule rather than from placed splits: with runtime
/// assignment every instance of a fragment that owns the scan node is a
/// legitimate destination, and deriving from placement would silently exclude
/// an instance that happened to receive nothing at planning time.
pub(crate) fn assignment_targets(
    schedule: &ValidatedFragmentSchedule,
    scan_nodes: &[(FragmentId, i32)],
) -> BTreeMap<i32, Vec<AssignmentTarget>> {
    let placements_by_fragment = schedule.fragment_placements();
    let mut targets: BTreeMap<i32, Vec<AssignmentTarget>> = BTreeMap::new();
    for &(fragment_id, plan_node_id) in scan_nodes {
        let Some(placements) = placements_by_fragment.get(&fragment_id) else {
            continue;
        };
        let entry = targets.entry(plan_node_id).or_default();
        for placement in placements {
            entry.push(AssignmentTarget {
                backend_idx: placement.backend_idx,
                fragment_instance_id: placement.finst_id,
            });
        }
    }
    targets
}

/// Every backend this round may deliver a task update to.
pub(crate) fn assignment_endpoints(
    schedule: &ValidatedFragmentSchedule,
) -> Vec<(usize, RuntimeEndpoint)> {
    let mut endpoints: BTreeMap<usize, RuntimeEndpoint> = BTreeMap::new();
    for placements in schedule.fragment_placements().values() {
        for placement in placements {
            endpoints
                .entry(placement.backend_idx)
                .or_insert_with(|| placement.endpoint.clone());
        }
    }
    endpoints.into_iter().collect()
}

/// Immutable, single-scan input for one attempt source-open call.
///
/// It shares only this scan's process-local access entry. The actor keeps the
/// complete prepared artifact and all previously opened sources.
pub(crate) struct RoundSplitSourceRecipe {
    fragment_id: FragmentId,
    plan_node_id: i32,
    table_scan: crate::query_execution::connector_domain::TableScanNode,
    constraint: novarocks_spi::connector::read_stack::ConnectorReadConstraint,
    access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessEntry>,
}

impl RoundSplitSourceRecipe {
    pub(crate) fn from_artifacts(
        artifacts: &PreparedDistributedQuery,
        fragment_id: FragmentId,
        plan_node_id: i32,
    ) -> Result<Self, String> {
        let scan = artifacts
            .typed_scan(fragment_id, plan_node_id)
            .ok_or_else(|| {
                format!(
                    "typed connector scan fragment_id={fragment_id} node_id={plan_node_id} is absent from its frozen artifact"
                )
            })?;
        let access = artifacts
            .share_connector_attempt_access(fragment_id, plan_node_id)
            .ok_or_else(|| {
                format!(
                    "typed connector scan fragment_id={fragment_id} node_id={plan_node_id} has no attempt access"
                )
            })?;
        Ok(Self {
            fragment_id,
            plan_node_id,
            table_scan: scan.prepared.table_scan.clone(),
            constraint: scan.prepared.constraint.clone(),
            access,
        })
    }

    pub(crate) const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) fn generation(&self) -> &novarocks_spi::connector::read_stack::ConnectorReadBinding {
        self.access.access().frozen().binding()
    }
}

/// One source-open result before the actor attaches attempt-wide feedback.
pub(crate) struct OpenedRoundSplitSource {
    plan_node_id: i32,
    source: Option<Box<dyn novarocks_spi::connector::read_stack::ConnectorReadSplitSource>>,
    encoder: Option<Arc<dyn novarocks_spi::connector::ConnectorReadWireEncoder>>,
    feedback_bindings: Vec<(
        u32,
        novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
    )>,
    blocking_io: ConnectorBlockingIoSupervisor,
}

impl OpenedRoundSplitSource {
    pub(crate) fn into_round_source(
        mut self,
        feedback: Arc<crate::runtime_filter::feedback::RuntimeFilterFeedbackState>,
    ) -> RoundSplitSource {
        RoundSplitSource {
            plan_node_id: self.plan_node_id,
            source: self
                .source
                .take()
                .expect("an opened split source is consumed exactly once"),
            encoder: self
                .encoder
                .take()
                .expect("an opened split source encoder is consumed exactly once"),
            feedback,
            feedback_bindings: std::mem::take(&mut self.feedback_bindings),
            initial_wait_initialized: false,
            initial_wait_deadline: None,
        }
    }
}

impl Drop for OpenedRoundSplitSource {
    fn drop(&mut self) {
        let Some(mut source) = self.source.take() else {
            return;
        };
        let plan_node_id = self.plan_node_id;
        let _ = self.blocking_io.spawn_protected(move || {
            if let Err(error) = source.close() {
                tracing::warn!(
                    plan_node_id,
                    error = %error,
                    "closing an unadopted split source failed"
                );
            }
            emit_split_source_close_marker(plan_node_id);
        });
    }
}

/// Open one exact scan source from frozen logical semantics and one attempt's
/// request capability.
///
/// The attempt initializer is the only source-open owner. Keeping provider
/// reacquisition and `get_splits` in this narrow operation makes the exact
/// generation, pushed constraint, and runtime-filter columns travel through
/// one managed blocking-I/O call.
pub(crate) fn open_round_split_source(
    recipe: RoundSplitSourceRecipe,
    session: &novarocks_spi::connector::read_stack::ConnectorSession,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    blocking_io: ConnectorBlockingIoSupervisor,
) -> Result<OpenedRoundSplitSource, String> {
    let table_scan = &recipe.table_scan;
    let access = &recipe.access;
    let connector_context = crate::connector::context_for_planning_lease_typed(
        access.planning_lease(),
        connector_context.clone(),
    )
    .map_err(|error| error.to_string())?;
    let attempt_context =
        novarocks_spi::connector::ConnectorAttemptContext::from_admitted_request(connector_context);
    let capabilities = access
        .access()
        .for_attempt(&attempt_context, access.planning_lease())
        .map_err(|error| error.to_string())?;
    let source = capabilities
        .splits()
        .get_splits(
            session,
            capabilities.frozen(),
            table_scan.assignments(),
            &table_scan.dynamic_filter_columns(),
            &recipe.constraint,
        )
        .map_err(|error| {
            format!(
                "typed connector scan node_id={} cannot open its split source: {error}",
                recipe.plan_node_id
            )
        })?;
    Ok(OpenedRoundSplitSource {
        plan_node_id: recipe.plan_node_id,
        source: Some(source),
        encoder: Some(capabilities.encoder()),
        feedback_bindings: feedback_bindings(table_scan),
        blocking_io,
    })
}

fn feedback_bindings(
    table_scan: &crate::query_execution::connector_domain::TableScanNode,
) -> Vec<(
    u32,
    novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
)> {
    table_scan
        .dynamic_filters()
        .iter()
        .filter_map(|binding| {
            table_scan
                .assignments()
                .iter()
                .find(|assignment| assignment.variable() == binding.variable())
                .map(|assignment| (binding.filter_id(), assignment.column().clone()))
        })
        .collect()
}

/// Everything one round needs to start assigning splits, built before the
/// prepared artifacts are consumed by staging.
///
/// It already owns open connector split sources, so it closes them if the
/// round never starts — staging or Start can still fail between here and
/// there, and a source dropped without closing leaves the connector holding
/// whatever the enumeration opened.
pub(crate) struct RoundSplitAssignmentPlan {
    targets: BTreeMap<i32, Vec<AssignmentTarget>>,
    sources: Vec<RoundSplitSource>,
    retry_policy: TaskUpdateRetryPolicy,
    initial_dynamic_filter_wait_cap: std::time::Duration,
    /// Every backend this round may address, frozen with the schedule.
    ///
    /// Carried on the plan because the schedule is consumed before either path
    /// builds its transport, and a transport built from a later snapshot could
    /// address a process this attempt never scheduled.
    endpoints: Vec<(usize, RuntimeEndpoint)>,
    blocking_io: ConnectorBlockingIoSupervisor,
}

/// Sources opened while one round is still being assembled.
///
/// Any later scan or target-validation error drops this owner before a plan
/// exists. Its drop therefore schedules exact close work through protected
/// lifecycle capacity instead of relying on `Box` destruction to release a
/// Connector's enumeration state.
pub(crate) struct OpenRoundSplitSources {
    sources: Vec<RoundSplitSource>,
    blocking_io: ConnectorBlockingIoSupervisor,
}

impl OpenRoundSplitSources {
    pub(crate) fn with_capacity(
        capacity: usize,
        blocking_io: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            sources: Vec::with_capacity(capacity),
            blocking_io,
        }
    }

    pub(crate) fn push(&mut self, source: RoundSplitSource) {
        self.sources.push(source);
    }

    pub(crate) fn plan_node_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.sources.iter().map(|source| source.plan_node_id)
    }

    pub(crate) fn into_sources(mut self) -> Vec<RoundSplitSource> {
        std::mem::take(&mut self.sources)
    }
}

impl Drop for OpenRoundSplitSources {
    fn drop(&mut self) {
        schedule_source_close(&self.blocking_io, std::mem::take(&mut self.sources));
    }
}

fn schedule_source_close(
    blocking_io: &ConnectorBlockingIoSupervisor,
    sources: Vec<RoundSplitSource>,
) {
    if sources.is_empty() {
        return;
    }
    let _ = blocking_io.spawn_protected(move || {
        for source in sources {
            RoundSplitAssignment::close_source(source);
        }
    });
}

impl RoundSplitAssignmentPlan {
    /// Everything but the delivery transport.
    ///
    /// The transport arrives at [`SplitAssignmentRoundGuard::install`] because
    /// the two are frozen at different moments: sources must be open before
    /// the attempt's credential leases are sealed, while the task substrate's
    /// delivery bridge cannot exist until the task graph does -- and the graph
    /// is built from the encoder output, which comes later.
    pub(crate) fn new(
        targets: BTreeMap<i32, Vec<AssignmentTarget>>,
        sources: Vec<RoundSplitSource>,
        retry_policy: TaskUpdateRetryPolicy,
        initial_dynamic_filter_wait_cap: std::time::Duration,
        endpoints: Vec<(usize, RuntimeEndpoint)>,
        blocking_io: ConnectorBlockingIoSupervisor,
    ) -> Self {
        Self {
            targets,
            sources,
            retry_policy,
            initial_dynamic_filter_wait_cap,
            endpoints,
            blocking_io,
        }
    }

    /// The scan nodes this plan opened a source for.
    pub(crate) fn plan_node_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.sources.iter().map(|source| source.plan_node_id)
    }

    /// Every backend this round may address.
    pub(crate) fn endpoints(&self) -> &[(usize, RuntimeEndpoint)] {
        &self.endpoints
    }
}

impl Drop for RoundSplitAssignmentPlan {
    fn drop(&mut self) {
        schedule_source_close(&self.blocking_io, std::mem::take(&mut self.sources));
    }
}

struct OwnerSlot<T> {
    owner_live: bool,
    result: Option<T>,
}

impl<T> OwnerSlot<T> {
    fn publish(&mut self, result: T) -> Result<(), T> {
        if self.owner_live {
            self.result = Some(result);
            Ok(())
        } else {
            Err(result)
        }
    }

    fn abandon(&mut self) -> Option<T> {
        self.owner_live = false;
        self.result.take()
    }
}

struct SplitAssignmentPump {
    assignment: Option<RoundSplitAssignment>,
    stop: RoundSplitAssignmentStop,
    data_runtime: FrontendDataRuntime,
    wake: Arc<dyn StatusIntakeWake>,
    enumeration: Arc<Mutex<OwnerSlot<Result<RoundSplitEnumerationResult, String>>>>,
    enumeration_in_flight: bool,
    retry_at: Option<Instant>,
    timer_at: Option<Instant>,
    failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    outcome: Arc<Mutex<Option<Result<SplitSourceProfile, SplitAssignmentDriverError>>>>,
    closing: bool,
}

impl SplitAssignmentPump {
    const PUMP_NAME: &'static str = "split_assignment";

    fn arm_wake(&mut self, deadline: Instant) {
        if self.timer_at.is_some_and(|armed| armed <= deadline) {
            return;
        }
        self.timer_at = Some(deadline);
        let wake = Arc::clone(&self.wake);
        self.data_runtime.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            wake.wake();
        });
    }

    fn close_source_after_owner_exit(data_runtime: &FrontendDataRuntime, source: RoundSplitSource) {
        let supervisor = data_runtime.connector_blocking_io().clone();
        let _ = supervisor.spawn_protected(move || RoundSplitAssignment::close_source(source));
    }

    fn start_close(&mut self, error: Option<SplitAssignmentDriverError>) {
        let Some(mut assignment) = self.assignment.take() else {
            return;
        };
        self.closing = true;
        if let Some(error) = &error {
            let mut failure = self.failure.lock().unwrap_or_else(|lock| lock.into_inner());
            if failure.is_none() {
                *failure = Some(error.clone());
            }
        }
        let failure = Arc::clone(&self.failure);
        let outcome = Arc::clone(&self.outcome);
        let wake = Arc::clone(&self.wake);
        let job = self
            .data_runtime
            .connector_blocking_io()
            .spawn_protected(move || {
                let profile = assignment.profile_snapshot();
                assignment.close();
                profile
            });
        self.data_runtime.spawn(async move {
            let result = match job.finish().await {
                Ok(profile) => error.map_or(Ok(profile), Err),
                Err(worker_error) => Err(SplitAssignmentDriverError::SplitSource {
                    plan_node_id: -1,
                    detail: worker_error.to_string(),
                }),
            };
            if let Err(error) = &result {
                let mut failure = failure.lock().unwrap_or_else(|lock| lock.into_inner());
                if failure.is_none() {
                    *failure = Some(error.clone());
                }
            }
            *outcome.lock().unwrap_or_else(|lock| lock.into_inner()) = Some(result);
            wake.wake();
        });
    }

    fn start_enumeration(
        &mut self,
        request: crate::query_execution::split_assignment::RoundSplitEnumerationRequest,
    ) {
        self.enumeration_in_flight = true;
        let job = self
            .data_runtime
            .connector_blocking_io()
            .spawn_ordinary(move || RoundSplitAssignment::enumerate_source(request));
        let slot = Arc::clone(&self.enumeration);
        let runtime = self.data_runtime.clone();
        let wake = Arc::clone(&self.wake);
        self.data_runtime.spawn(async move {
            let result = job.finish().await.map_err(|error| error.to_string());
            let reap = {
                let mut slot = slot.lock().unwrap_or_else(|lock| lock.into_inner());
                slot.publish(result)
                    .err()
                    .and_then(Result::ok)
                    .map(|result| result.source)
            };
            if let Some(source) = reap {
                Self::close_source_after_owner_exit(&runtime, source);
            }
            wake.wake();
        });
    }
}

impl TurnPump for SplitAssignmentPump {
    fn name(&self) -> &'static str {
        Self::PUMP_NAME
    }

    fn drive(&mut self, _execution: &mut QueryTaskExecution) -> Result<usize, TaskExecutionError> {
        if self.closing {
            return Ok(0);
        }
        if self
            .timer_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            self.timer_at = None;
        }
        if self.enumeration_in_flight {
            let result = self
                .enumeration
                .lock()
                .unwrap_or_else(|lock| lock.into_inner())
                .result
                .take();
            let Some(result) = result else {
                return Ok(0);
            };
            self.enumeration_in_flight = false;
            let result = match result {
                Ok(result) => result,
                Err(detail) => {
                    self.start_close(Some(SplitAssignmentDriverError::SplitSource {
                        plan_node_id: -1,
                        detail,
                    }));
                    return Ok(1);
                }
            };
            let assignment = self
                .assignment
                .as_mut()
                .expect("an enumeration result retains its assignment owner");
            let enumerated = assignment.adopt_enumeration(result);
            if self.stop.is_stopped() {
                self.start_close(None);
                return Ok(1);
            }
            let enumerated = match enumerated {
                Ok(batch) => batch,
                Err(error) => {
                    self.start_close(Some(error));
                    return Ok(1);
                }
            };
            let Some((plan_node_id, batch)) = enumerated else {
                return Ok(1);
            };
            if let Err(error) = assignment.deliver(plan_node_id, batch) {
                self.start_close(Some(error));
                return Ok(1);
            }
            return Ok(1);
        }

        if self.stop.is_stopped() {
            self.start_close(None);
            return Ok(1);
        }

        let assignment = self
            .assignment
            .as_mut()
            .expect("a live split pump owns its assignment");
        if assignment.delivery_in_progress() {
            return match assignment.drive_delivery() {
                Ok(true) => Ok(1),
                Ok(false) => {
                    if let Some(deadline) = assignment.next_delivery_wake_at() {
                        self.arm_wake(deadline);
                    }
                    Ok(0)
                }
                Err(error) => {
                    self.start_close(Some(error));
                    Ok(1)
                }
            };
        }
        if self
            .retry_at
            .is_some_and(|deadline| Instant::now() < deadline)
        {
            return Ok(0);
        }
        self.retry_at = None;
        match assignment.enumerate_once() {
            Ok(RoundSplitEnumeration::Ready(request)) => {
                self.start_enumeration(request);
                Ok(1)
            }
            Ok(RoundSplitEnumeration::Idle(wait)) => {
                let retry_at = Instant::now() + wait;
                self.retry_at = Some(retry_at);
                self.arm_wake(retry_at);
                Ok(0)
            }
            Ok(RoundSplitEnumeration::Finished) => {
                self.start_close(None);
                Ok(1)
            }
            Err(error) => {
                self.start_close(Some(error));
                Ok(1)
            }
        }
    }
}

impl Drop for SplitAssignmentPump {
    fn drop(&mut self) {
        self.stop.stop();
        let pending_source = {
            let mut slot = self
                .enumeration
                .lock()
                .unwrap_or_else(|lock| lock.into_inner());
            slot.abandon()
                .and_then(Result::ok)
                .map(|result| result.source)
        };
        if let Some(source) = pending_source {
            Self::close_source_after_owner_exit(&self.data_runtime, source);
        }
        if let Some(mut assignment) = self.assignment.take() {
            let supervisor = self.data_runtime.connector_blocking_io().clone();
            let _ = supervisor.spawn_protected(move || assignment.close());
        }
    }
}

/// A nonblocking observation handle for the round-owned split pump.
pub(crate) struct SplitAssignmentRoundGuard {
    stop: RoundSplitAssignmentStop,
    failure: Arc<Mutex<Option<SplitAssignmentDriverError>>>,
    outcome: Arc<Mutex<Option<Result<SplitSourceProfile, SplitAssignmentDriverError>>>>,
}

impl SplitAssignmentRoundGuard {
    /// Installs the pump. Returns `None` when this round has no typed scan.
    pub(crate) fn install(
        round: &mut TaskRound,
        execution_id: QueryExecutionId,
        mut plan: RoundSplitAssignmentPlan,
        transport: Arc<dyn TaskUpdateTransport>,
        data_runtime: FrontendDataRuntime,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Option<Self> {
        // Taken, not borrowed: the sources move into the round, so the plan's
        // own drop must not close what the round now owns.
        let sources = std::mem::take(&mut plan.sources);
        let tasks = std::mem::take(&mut plan.targets);
        if sources.is_empty() {
            return None;
        }
        let assignment = RoundSplitAssignment::new(
            execution_id,
            transport,
            tasks,
            DEFAULT_MAX_QUEUED_SPLITS_PER_TASK,
            sources,
            plan.retry_policy,
            plan.initial_dynamic_filter_wait_cap,
        );
        let stop = assignment.stop_handle();
        let failure = Arc::new(Mutex::new(None));
        let outcome = Arc::new(Mutex::new(None));
        round.add_pump(Box::new(SplitAssignmentPump {
            assignment: Some(assignment),
            stop: stop.clone(),
            data_runtime,
            wake,
            enumeration: Arc::new(Mutex::new(OwnerSlot {
                owner_live: true,
                result: None,
            })),
            enumeration_in_flight: false,
            retry_at: None,
            timer_at: None,
            failure: Arc::clone(&failure),
            outcome: Arc::clone(&outcome),
            closing: false,
        }));
        Some(Self {
            stop,
            failure,
            outcome,
        })
    }

    /// Whether the pump has published its final source profile or failure.
    pub(crate) fn is_finished(&self) -> bool {
        self.outcome
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .is_some()
    }

    /// Why delivery stopped, if it stopped by failing.
    ///
    /// A caller may ask on every turn. A pump that finished normally answers
    /// `None`: a round
    /// whose sources are all terminal legitimately stops delivering long
    /// before its scans finish, and that is not a failure.
    ///
    /// This exists because a failed deliverer is otherwise silent. Every
    /// remaining scan then waits for splits nobody will send, the round keeps
    /// turning with nothing to fold, and the query dies on the statement
    /// deadline reporting the wait instead of the cause -- including the case
    /// ADR-0123 froze a bounded budget for, whose exhaustion is longer than a
    /// typical statement timeout and so could never be seen.
    pub(crate) fn failure(&self) -> Option<SplitAssignmentDriverError> {
        self.failure
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone()
    }

    /// Takes the final profile without waiting for the pump.
    ///
    /// An absent outcome is accepted only after the attempt owner explicitly
    /// abandoned residual delivery; an ordinary successful finish may never
    /// turn an unfinished assignment into a successful empty profile.
    pub(crate) fn finish_after_attempt(
        self,
        abandoned: bool,
    ) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .clone();
        match outcome {
            Some(outcome) => outcome,
            None if abandoned => Ok(SplitSourceProfile::default()),
            None => Err(SplitAssignmentDriverError::DeliveryInProgress),
        }
    }
}

impl Drop for SplitAssignmentRoundGuard {
    fn drop(&mut self) {
        self.stop.stop();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use novarocks_spi::connector::ConnectorError;
    use novarocks_spi::connector::read_stack::{
        ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
        ConnectorSplitBatch,
    };
    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    struct CloseSignalSource {
        closed: Option<mpsc::Sender<()>>,
    }

    impl ConnectorReadSplitSource for CloseSignalSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            panic!("source-owner cleanup test must not enumerate")
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            if let Some(closed) = self.closed.take() {
                closed.send(()).expect("publish source close");
            }
            Ok(())
        }
    }

    struct CleanupTestEncoder;

    impl novarocks_spi::connector::ConnectorReadWireEncoder for CleanupTestEncoder {
        fn owner(&self) -> &str {
            "split-cleanup-test"
        }

        fn encode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadRelationPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a relation")
        }

        fn encode_column_payload(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a column")
        }

        fn encode_transaction_payload(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a transaction")
        }

        fn encode_split_payload(
            &self,
            _split: &ConnectorReadSplit,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadSplitPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("cleanup test must not encode a split")
        }
    }

    fn cleanup_test_source(closed: mpsc::Sender<()>) -> RoundSplitSource {
        let execution_id = QueryExecutionId::new(
            QueryId::new(17, 19),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        RoundSplitSource {
            plan_node_id: 7,
            source: Box::new(CloseSignalSource {
                closed: Some(closed),
            }),
            encoder: Arc::new(CleanupTestEncoder),
            feedback: Arc::new(
                crate::runtime_filter::feedback::RuntimeFilterFeedbackState::new(
                    execution_id,
                    Default::default(),
                )
                .expect("empty feedback declaration"),
            ),
            feedback_bindings: Vec::new(),
            initial_wait_initialized: false,
            initial_wait_deadline: None,
        }
    }

    #[tokio::test]
    async fn a_round_with_no_typed_scan_starts_no_worker() {
        let plan = RoundSplitAssignmentPlan::new(
            BTreeMap::new(),
            Vec::new(),
            TaskUpdateRetryPolicy::default(),
            std::time::Duration::ZERO,
            Vec::new(),
            ConnectorBlockingIoSupervisor::new(
                tokio::runtime::Handle::current(),
                crate::task_execution::ConnectorBlockingIoBudget::default(),
            ),
        );
        assert_eq!(plan.plan_node_ids().count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partially_opened_sources_close_through_protected_capacity() {
        let supervisor = ConnectorBlockingIoSupervisor::new(
            tokio::runtime::Handle::current(),
            crate::task_execution::ConnectorBlockingIoBudget::try_new(2, 1)
                .expect("one ordinary and one protected permit"),
        );
        let (release, released) = mpsc::channel();
        let (started, ordinary_started) = mpsc::channel();
        let ordinary = supervisor.spawn_ordinary(move || {
            started.send(()).expect("publish ordinary start");
            released.recv().expect("release ordinary call");
        });
        ordinary_started
            .recv_timeout(Duration::from_secs(2))
            .expect("ordinary call must occupy its lane");

        let (closed, observe_close) = mpsc::channel();
        let mut opened = OpenRoundSplitSources::with_capacity(1, supervisor);
        opened.push(cleanup_test_source(closed));
        drop(opened);

        observe_close
            .recv_timeout(Duration::from_secs(2))
            .expect("cleanup must use reserved protected capacity");
        release.send(()).expect("release ordinary call");
        ordinary.finish().await.expect("ordinary call finishes");
    }

    #[test]
    fn ordinary_finish_refuses_an_unfinished_pump() {
        let stop = RoundSplitAssignmentStop::default();
        let guard = SplitAssignmentRoundGuard {
            stop: stop.clone(),
            failure: Arc::new(Mutex::new(None)),
            outcome: Arc::new(Mutex::new(None)),
        };

        assert!(matches!(
            guard.finish_after_attempt(false),
            Err(SplitAssignmentDriverError::DeliveryInProgress)
        ));
        assert!(
            stop.is_stopped(),
            "consuming the guard signals residual stop"
        );
    }

    #[test]
    fn a_failed_deliverer_is_readable_while_the_round_is_still_turning() {
        // What this catches: a delivery failure that is only visible to
        // `finish`, which runs after the round loop. A deliverer that stopped
        // will never feed the tasks it had left, so that loop never reaches
        // its own exit -- the query runs out its statement deadline and
        // reports what it was waiting on instead of why nobody was sending.
        // A normal stop must stay silent, because a round whose sources all
        // went terminal legitimately stops delivering long before its scans
        // finish.
        let failed = SplitAssignmentRoundGuard {
            stop: RoundSplitAssignmentStop::default(),
            failure: Arc::new(Mutex::new(Some(
                SplitAssignmentDriverError::NoAdmittedTask { plan_node_id: 4 },
            ))),
            outcome: Arc::new(Mutex::new(Some(Err(
                SplitAssignmentDriverError::NoAdmittedTask { plan_node_id: 4 },
            )))),
        };
        let detail = failed
            .failure()
            .expect("a failed deliverer reports its own cause")
            .to_string();
        assert!(detail.contains("no admitted task"), "{detail}");
        // Reaped, not consumed: the same verdict is still the round's own
        // result afterwards.
        assert!(failed.failure().is_some());
        failed
            .finish_after_attempt(false)
            .expect_err("the joined verdict is still the round's result");

        let healthy = SplitAssignmentRoundGuard {
            stop: RoundSplitAssignmentStop::default(),
            failure: Arc::new(Mutex::new(None)),
            outcome: Arc::new(Mutex::new(Some(Ok(SplitSourceProfile::default())))),
        };
        assert!(
            healthy.failure().is_none(),
            "a deliverer that ran out of sources has not failed"
        );
        healthy
            .finish_after_attempt(false)
            .expect("a clean stop is still a clean stop");
    }
}
