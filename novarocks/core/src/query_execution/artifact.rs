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

//! Opaque owned handoffs and neutral scheduling projections.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::Field;
#[cfg(test)]
use novarocks_spi::connector::ConnectorExecutionDeclaration;
use sha2::{Digest, Sha256};

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::backend::LiveBackendTarget;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, QueryId, ResolvedQueryOptions,
};
use crate::query_execution::fragment_transport::{ExpectedOutputSchemaView, FetchedQueryBatch};
use crate::query_execution::lifecycle::{
    ExchangeRouteManifest, QueryExecutionId, QueryInitBarrier, QueryInitOptions,
    QueryLaunchBarrier, QueryLifecycleLease, RuntimeFilterContribution, StageBatch, StageFragment,
    StageParticipantBinding,
};
use crate::query_execution::preparation::{
    PreparedFragment, PreparedFragmentSchedulingView, PreparedFragmentSet, PreparedOutputColumn,
};
use crate::query_execution::schedule::{
    FragmentInstancePlacement, FragmentLifecycleProjection, SchedulingPlan,
};
use crate::query_execution::write_plan::{ConnectorWriteManifest, ConnectorWritePlanAttachment};
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    FragmentEdgeKind, FragmentId as PlannerFragmentId, FragmentStreamKind, PartitionKind,
};

pub use crate::query_execution::connector_binding::{
    ConnectorBindingBackendInstallPlan, ConnectorBindingDispatcher, ConnectorBindingInstallBarrier,
    ConnectorBindingInstallLease, ConnectorBindingInstallObserver, ConnectorBindingInstallPlan,
    DispatchingConnectorBindingBarrier, NoopConnectorBindingInstallObserver,
    new_grpc_connector_binding_dispatcher,
};
pub type FragmentId = u32;
pub type PlanNodeId = i32;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

static NEXT_HANDOFF_ID: AtomicU64 = AtomicU64::new(1);

/// The owned prepared/native pair. It has no public constructor, `Clone`, or
/// inverse `from_parts`, so artifacts from different sealed plans cannot be
/// recombined by a role crate.
pub struct PreparedDistributedQuery {
    handoff_id: u64,
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
}

impl PreparedDistributedQuery {
    pub(super) fn new(prepared: PreparedFragmentSet, native_bundle: NativeFragmentBundle) -> Self {
        Self {
            handoff_id: NEXT_HANDOFF_ID.fetch_add(1, Ordering::Relaxed),
            prepared,
            native_bundle,
        }
    }

    pub fn scheduling_view(&self) -> FragmentSchedulingView<'_> {
        FragmentSchedulingView {
            handoff_id: self.handoff_id,
            inner: self.prepared.scheduling_view(),
        }
    }

    pub fn bind_schedule(
        self,
        schedule: ValidatedFragmentSchedule,
    ) -> Result<ScheduleBoundDistributedQuery, DistributedQueryError> {
        if self.handoff_id != schedule.handoff_id {
            return Err(contract_error(
                "validated fragment schedule belongs to a different prepared query handoff",
            ));
        }
        Ok(ScheduleBoundDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule,
            connector_write_plan: None,
        })
    }
}

/// A core artifact bound to one validated schedule. This type deliberately has
/// no `assemble` method: query initialization/control readiness and the
/// connector install/ACK barrier must first complete.
///
/// ```compile_fail
/// use novarocks::query_execution::artifact::ScheduleBoundDistributedQuery;
///
/// fn query_control_typestate_prevents_submission_before_ready(
///     scheduled: ScheduleBoundDistributedQuery,
/// ) {
///     let _ = scheduled.assemble();
/// }
/// ```
pub struct ScheduleBoundDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

impl ScheduleBoundDistributedQuery {
    /// Terminal write fragments derived from the same prepared artifact that
    /// was sealed before scheduling.  Callers cannot nominate an arbitrary
    /// fragment set when creating a connector writer manifest.
    pub fn terminal_write_fragment_ids(&self) -> BTreeSet<FragmentId> {
        self.prepared
            .scheduling_view()
            .fragments()
            .filter(|fragment| fragment.execution_role().is_terminal_write())
            .map(|fragment| fragment.fragment_id())
            .collect()
    }

    /// Freeze the writer identities after placement and before the BE binding
    /// barrier.  Planning remains caller-owned because only the DML owner has
    /// the operation-specific table, intent, and provider payload.
    pub fn freeze_connector_write_manifest(
        &self,
        terminal_write_fragment_ids: &BTreeSet<FragmentId>,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        cohort_id: novarocks_spi::connector::ConnectorWriteCohortId,
        owner: novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<ConnectorWriteManifest, DistributedQueryError> {
        ConnectorWriteManifest::freeze(
            self.schedule.planning_schedule(),
            terminal_write_fragment_ids,
            operation_id,
            cohort_id,
            owner,
            self.schedule.execution_id(),
        )
        .map_err(|error| contract_error(error.to_string()))
    }

    /// Attach one already-frozen provider-neutral plan.  The attachment owns
    /// the exact control-generation lease and is carried through every later
    /// typestate until the caller receives completion ownership.  No implicit
    /// planning occurs at this layer.
    pub fn attach_connector_write_plan(
        mut self,
        attachment: ConnectorWritePlanAttachment,
    ) -> Result<Self, DistributedQueryError> {
        attach_connector_write_plan(
            &mut self.connector_write_plan,
            self.schedule.planning_schedule(),
            self.schedule.execution_id(),
            attachment,
        )?;
        Ok(self)
    }

    pub fn initialize_query(
        self,
        options: QueryInitOptions,
        barrier: &dyn QueryInitBarrier,
    ) -> Result<ControlReadyDistributedQuery, DistributedQueryError> {
        if options.execution_id() != self.schedule.execution_id {
            return Err(contract_error(
                "query initialization execution id does not match validated schedule",
            ));
        }
        let runtime_filters = crate::query_execution::runtime_filter::compile_contribution_plan(
            options.execution_id(),
            self.prepared.runtime_filter_graph(),
            self.prepared.runtime_filter_join_progress(),
            self.prepared.scheduling_view().edges(),
            &self.schedule.inner,
            options.live_backends(),
            options.runtime_filter_worker_count(),
            options.runtime_filter_lifecycle(),
        )?;
        let plan = crate::query_execution::lifecycle::init_plan::compile_query_init_plan(
            self.schedule.lifecycle_projection(),
            runtime_filters,
            &options,
        )?;
        // Freeze every Stage target and exact fragment set from the same
        // manifest that InitQuery consumes.  No later lifecycle phase may
        // consult live topology again.
        let stage_bindings = plan.stage_participant_bindings()?;
        let query_lifecycle_lease = barrier.initialize_all(plan)?;
        Ok(ControlReadyDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            options,
            query_lifecycle_lease,
            stage_bindings,
            connector_write_plan: self.connector_write_plan,
        })
    }

    /// Assemble the sealed request for the core-only semantic test runtime.
    ///
    /// This deliberately bypasses lifecycle transport only under `cfg(test)`;
    /// frontend production tests continue to exercise the full Init/Stage/Start
    /// protocol and all-in-one production never calls this path.
    #[cfg(test)]
    pub(crate) fn assemble_for_in_process_test(
        self,
        query_id: QueryId,
        options: &ResolvedQueryOptions,
        live_backends: &[LiveBackendTarget],
    ) -> Result<InProcessTestArtifact, DistributedQueryError> {
        let connector_write_plan = self.connector_write_plan;
        let runtime_filters = crate::query_execution::runtime_filter::compile_contribution_plan(
            self.schedule.execution_id,
            self.prepared.runtime_filter_graph(),
            self.prepared.runtime_filter_join_progress(),
            self.prepared.scheduling_view().edges(),
            &self.schedule.inner,
            live_backends,
            1,
            options.runtime_filter_lifecycle(),
        )?
        .into_iter()
        .map(|contribution| {
            let (_, participant_id, lifecycle, install) = contribution.into_parts();
            RuntimeFilterContribution::from_compiled(
                self.schedule.execution_id,
                participant_id,
                lifecycle,
                install,
            )
            .map_err(|error| contract_error(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
        let install_plan = crate::query_execution::connector_binding::compile_install_plan(
            &self.prepared,
            &self.schedule.inner,
            connector_write_plan.as_ref(),
        )?;
        let declarations = install_plan
            .backends()
            .iter()
            .flat_map(|backend| backend.declarations().iter().cloned())
            .collect::<Vec<_>>();
        let assembled = assemble_native_execution(
            self.prepared,
            self.native_bundle,
            self.schedule.inner,
            self.schedule.execution_id,
            NativeSubmissionContext {
                query_id,
                options: options.runtime_options().clone(),
            },
            connector_write_plan.as_ref(),
        )?;
        Ok(InProcessTestArtifact {
            submissions: assembled
                .submissions
                .into_iter()
                .map(|submission| InProcessTestSubmission {
                    plan: submission.plan,
                    instance_params: submission.instance_params,
                })
                .collect(),
            root_fetch: assembled.root_fetch,
            writer_registrations: assembled.writer_registrations,
            expected_output: assembled.expected_output,
            declarations,
            runtime_filters,
        })
    }
}

fn attach_connector_write_plan(
    slot: &mut Option<ConnectorWritePlanAttachment>,
    schedule: &SchedulingPlan,
    execution_id: QueryExecutionId,
    attachment: ConnectorWritePlanAttachment,
) -> Result<(), DistributedQueryError> {
    if slot.is_some() {
        return Err(contract_error(
            "distributed query already has a connector write plan attachment",
        ));
    }
    attachment
        .manifest()
        .validate_schedule(schedule, execution_id)
        .map_err(|error| contract_error(error.to_string()))?;
    *slot = Some(attachment);
    Ok(())
}

/// Query lifecycle is ready, but connector instances still require their
/// independent process-scoped install/ACK barrier before preparing native
/// Stage batches.
pub struct ControlReadyDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
    options: QueryInitOptions,
    query_lifecycle_lease: QueryLifecycleLease,
    stage_bindings: Vec<StageParticipantBinding>,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

impl ControlReadyDistributedQuery {
    pub fn prepare_connector_bindings(
        self,
        barrier: &dyn ConnectorBindingInstallBarrier,
    ) -> Result<ConnectorBindingReadyDistributedQuery, DistributedQueryError> {
        let plan = crate::query_execution::connector_binding::compile_install_plan(
            &self.prepared,
            &self.schedule.inner,
            self.connector_write_plan.as_ref(),
        )?;
        let connector_binding_lease = match barrier.install_all(self.schedule.execution_id, plan) {
            Ok(lease) => lease,
            Err(error) => {
                let kind = error.kind();
                let message = self
                    .query_lifecycle_lease
                    .abort_preserving(error.message().to_string());
                return Err(DistributedQueryError::new(kind, message));
            }
        };
        Ok(ConnectorBindingReadyDistributedQuery {
            prepared: self.prepared,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            options: self.options,
            query_lifecycle_lease: self.query_lifecycle_lease,
            connector_binding_lease,
            stage_bindings: self.stage_bindings,
            connector_write_plan: self.connector_write_plan,
        })
    }
}

/// The only typestate that can prepare native Stage batches. In particular,
/// it is impossible to create a submission before every selected BE has ACKed
/// the connector declarations it will resolve by instance id.
pub struct ConnectorBindingReadyDistributedQuery {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: ValidatedFragmentSchedule,
    options: QueryInitOptions,
    query_lifecycle_lease: QueryLifecycleLease,
    connector_binding_lease: ConnectorBindingInstallLease,
    stage_bindings: Vec<StageParticipantBinding>,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

impl ConnectorBindingReadyDistributedQuery {
    pub fn connector_write_plan(&self) -> Option<&ConnectorWritePlanAttachment> {
        self.connector_write_plan.as_ref()
    }

    pub fn prepare_stage(self) -> Result<StagePreparedDistributedQuery, DistributedQueryError> {
        let ConnectorBindingReadyDistributedQuery {
            prepared,
            native_bundle,
            schedule,
            options,
            query_lifecycle_lease,
            connector_binding_lease,
            stage_bindings,
            connector_write_plan,
        } = self;
        let context = match options.native_submission_context() {
            Ok(context) => context,
            Err(error) => {
                let kind = error.kind();
                let message = query_lifecycle_lease.abort_preserving(error.message().to_string());
                let message = connector_binding_lease.abort_preserving(message);
                return Err(DistributedQueryError::new(kind, message));
            }
        };
        let assembled = assemble_native_execution(
            prepared,
            native_bundle,
            schedule.inner,
            schedule.execution_id,
            context,
            connector_write_plan.as_ref(),
        );
        match assembled {
            Ok(assembled) => {
                let mut fragments_by_backend = BTreeMap::<usize, Vec<StageFragment>>::new();
                for submission in assembled.submissions {
                    let (backend_idx, fragment) = submission.into_stage_fragment()?;
                    fragments_by_backend
                        .entry(backend_idx)
                        .or_default()
                        .push(fragment);
                }
                let mut batches = Vec::with_capacity(stage_bindings.len());
                for binding in stage_bindings {
                    let fragments = fragments_by_backend
                        .remove(&binding.target().backend_idx())
                        .unwrap_or_default();
                    batches.push(
                        StageBatch::new(schedule.execution_id, binding, fragments)
                            .map_err(|error| contract_error(error.to_string()))?,
                    );
                }
                if !fragments_by_backend.is_empty() {
                    let error = contract_error(format!(
                        "native stage assembly produced fragments for unknown participants: {:?}",
                        fragments_by_backend.keys().collect::<Vec<_>>()
                    ));
                    let kind = error.kind();
                    let message =
                        query_lifecycle_lease.abort_preserving(error.message().to_string());
                    let message = connector_binding_lease.abort_preserving(message);
                    return Err(DistributedQueryError::new(kind, message));
                }
                Ok(StagePreparedDistributedQuery {
                    batches,
                    root_fetch: assembled.root_fetch,
                    writer_registrations: assembled.writer_registrations,
                    expected_output: assembled.expected_output,
                    query_lifecycle_lease,
                    connector_binding_lease,
                    connector_write_plan,
                })
            }
            Err(error) => {
                let kind = error.kind();
                let message = query_lifecycle_lease.abort_preserving(error.message().to_string());
                let message = connector_binding_lease.abort_preserving(message);
                Err(DistributedQueryError::new(kind, message))
            }
        }
    }
}

/// Immutable, scalar-only frontend scheduling projection.
#[derive(Clone, Copy)]
pub struct FragmentSchedulingView<'a> {
    handoff_id: u64,
    inner: PreparedFragmentSchedulingView<'a>,
}

impl<'a> FragmentSchedulingView<'a> {
    pub fn fragment_ids(self) -> impl ExactSizeIterator<Item = FragmentId> + 'a {
        self.inner.fragment_ids()
    }

    pub fn fragments(self) -> impl ExactSizeIterator<Item = SchedulingFragmentView<'a>> + 'a {
        self.inner
            .fragments()
            .map(move |fragment| SchedulingFragmentView {
                fragment,
                view: self.inner,
            })
    }

    pub fn topological_order(self) -> &'a [FragmentId] {
        self.inner.topological_order()
    }

    pub fn execution_anchor(self) -> FragmentId {
        self.inner.execution_anchor()
    }

    pub fn edges(self) -> impl ExactSizeIterator<Item = SchedulingEdgeView<'a>> + 'a {
        self.inner
            .edges()
            .iter()
            .map(|edge| SchedulingEdgeView { edge })
    }
}

#[derive(Clone, Copy)]
pub struct SchedulingFragmentView<'a> {
    fragment: &'a PreparedFragment,
    view: PreparedFragmentSchedulingView<'a>,
}

impl<'a> SchedulingFragmentView<'a> {
    pub fn fragment_id(self) -> FragmentId {
        self.fragment.fragment_id()
    }

    pub fn has_scan_nodes(self) -> bool {
        self.fragment.has_scan_nodes()
    }

    pub fn scan_node_ids(self) -> &'a [PlanNodeId] {
        self.fragment.scan_node_ids()
    }

    pub fn scan_range_count(self, node_id: PlanNodeId) -> Option<usize> {
        self.view
            .scan_ranges(self.fragment.fragment_id(), node_id)
            .map(<[_]>::len)
    }

    /// Number of provider-neutral opaque splits available to schedule for a
    /// connector read.  The frontend uses this only as placement cardinality;
    /// split payloads remain opaque until artifact assembly patches the
    /// already-encoded carrier for each BE.
    pub fn connector_split_count(self, node_id: PlanNodeId) -> Option<usize> {
        self.view
            .connector_read(self.fragment.fragment_id(), node_id)
            .map(|read| read.splits.len())
    }

    pub fn is_terminal_write(self) -> bool {
        self.fragment.execution_role().is_terminal_write()
    }

    pub fn is_statistics(self) -> bool {
        self.fragment.execution_role().is_statistics()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

#[derive(Clone, Copy)]
pub struct SchedulingEdgeView<'a> {
    edge: &'a crate::sql::planner::distributed::FragmentEdge,
}

impl SchedulingEdgeView<'_> {
    pub fn source_fragment_id(self) -> FragmentId {
        self.edge.source_fragment_id
    }

    pub fn target_fragment_id(self) -> FragmentId {
        self.edge.target_fragment_id
    }

    pub fn target_exchange_node_id(self) -> PlanNodeId {
        self.edge.target_exchange_node_id
    }

    pub fn is_native_hash_partitioned(self) -> bool {
        matches!(self.edge.output_partition.kind, PartitionKind::Hash)
    }

    pub fn stream_kind(self) -> SchedulingStreamKind {
        let kind = match self.edge.edge_kind {
            FragmentEdgeKind::Stream => self.edge.stream_kind,
            FragmentEdgeKind::CteMulticast { .. } => FragmentStreamKind::Broadcast,
            FragmentEdgeKind::ChangeStreamRouter { .. } => self.edge.stream_kind,
        };
        match kind {
            FragmentStreamKind::Gather => SchedulingStreamKind::Gather,
            FragmentStreamKind::Broadcast => SchedulingStreamKind::Broadcast,
            FragmentStreamKind::Partitioned => SchedulingStreamKind::Partitioned,
            FragmentStreamKind::Other => SchedulingStreamKind::Other,
        }
    }
}

/// A frontend decision for one instance. The native endpoint representation
/// remains core-private.
pub struct BackendPlacement {
    backend_idx: usize,
    endpoint: SocketAddr,
}

impl BackendPlacement {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr) -> Self {
        Self {
            backend_idx,
            endpoint,
        }
    }
}

/// Unvalidated frontend policy output.
pub struct FragmentScheduleDraft {
    by_fragment: BTreeMap<FragmentId, Vec<BackendPlacement>>,
    frozen_live_backends: Option<BTreeMap<usize, LiveBackendTarget>>,
}

impl FragmentScheduleDraft {
    pub fn new() -> Self {
        Self {
            by_fragment: BTreeMap::new(),
            frozen_live_backends: None,
        }
    }

    pub fn freeze_live_backends(
        &mut self,
        live_backends: Vec<LiveBackendTarget>,
    ) -> Result<(), DistributedQueryError> {
        if self.frozen_live_backends.is_some() {
            return Err(contract_error(
                "frontend schedule live-backend topology was frozen more than once",
            ));
        }
        if live_backends.is_empty() {
            return Err(contract_error(
                "frontend schedule requires a nonempty live-backend topology",
            ));
        }
        let mut by_backend = BTreeMap::new();
        let mut endpoints = BTreeSet::new();
        for target in live_backends {
            if target.start_epoch() == 0 {
                return Err(contract_error(format!(
                    "frontend schedule live backend {} has zero start epoch",
                    target.backend_idx()
                )));
            }
            if !endpoints.insert(target.endpoint()) {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats endpoint {}",
                    target.endpoint()
                )));
            }
            if by_backend.insert(target.backend_idx(), target).is_some() {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats backend {}",
                    target.backend_idx()
                )));
            }
        }
        self.frozen_live_backends = Some(by_backend);
        Ok(())
    }

    pub fn assign_fragment(
        &mut self,
        fragment_id: FragmentId,
        placements: Vec<BackendPlacement>,
    ) -> Result<(), DistributedQueryError> {
        if self.by_fragment.insert(fragment_id, placements).is_some() {
            return Err(contract_error(format!(
                "frontend schedule assigned fragment {fragment_id} more than once"
            )));
        }
        Ok(())
    }
}

impl Default for FragmentScheduleDraft {
    fn default() -> Self {
        Self::new()
    }
}

/// Core-validated schedule. It cannot be cloned, deconstructed, or created
/// without the immutable view from the same prepared handoff.
pub struct ValidatedFragmentSchedule {
    handoff_id: u64,
    execution_id: QueryExecutionId,
    inner: SchedulingPlan,
    lifecycle: FragmentLifecycleProjection,
}

impl ValidatedFragmentSchedule {
    pub fn validate(
        view: FragmentSchedulingView<'_>,
        execution_id: QueryExecutionId,
        draft: FragmentScheduleDraft,
    ) -> Result<Self, DistributedQueryError> {
        let FragmentScheduleDraft {
            by_fragment: draft_by_fragment,
            frozen_live_backends,
        } = draft;
        let frozen_live_backends = frozen_live_backends.ok_or_else(|| {
            contract_error("frontend schedule did not freeze its live-backend topology")
        })?;
        let expected = view.fragment_ids().collect::<BTreeSet<_>>();
        let received = draft_by_fragment.keys().copied().collect::<BTreeSet<_>>();
        if expected != received {
            return Err(contract_error(format!(
                "frontend schedule fragment set mismatch: expected={expected:?}, received={received:?}"
            )));
        }

        let mut by_fragment = BTreeMap::new();
        for (fragment_id, placements) in draft_by_fragment {
            if placements.is_empty() {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} has no placements"
                )));
            }
            if placements.len() >= 1 << 16 {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} has too many placements"
                )));
            }
            let mut backend_ids = BTreeSet::new();
            let mut instances = placements
                .into_iter()
                .enumerate()
                .map(|(instance_index, placement)| {
                    if !backend_ids.insert(placement.backend_idx) {
                        return Err(contract_error(format!(
                            "frontend schedule fragment {fragment_id} repeats backend {}",
                            placement.backend_idx
                        )));
                    }
                    let frozen = frozen_live_backends
                        .get(&placement.backend_idx)
                        .ok_or_else(|| {
                            contract_error(format!(
                                "frontend schedule placement backend {} is absent from frozen topology",
                                placement.backend_idx
                            ))
                        })?;
                    if frozen.endpoint() != placement.endpoint {
                        return Err(contract_error(format!(
                            "frontend schedule placement backend {} endpoint {} differs from frozen topology endpoint {}",
                            placement.backend_idx, placement.endpoint, frozen.endpoint()
                        )));
                    }
                    Ok(FragmentInstancePlacement {
                        fragment_id,
                        instance_index,
                        finst_id: derive_fragment_instance_id(
                            execution_id,
                            fragment_id,
                            instance_index,
                        )?,
                        backend_idx: placement.backend_idx,
                        endpoint: RuntimeEndpoint::from_socket_addr(placement.endpoint),
                        scan_ranges: BTreeMap::new(),
                        connector_splits: BTreeMap::new(),
                        destinations: Vec::new(),
                        per_exch_num_senders: BTreeMap::new(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let fragment = view.inner.fragment(fragment_id).ok_or_else(|| {
                contract_error(format!("prepared fragment {fragment_id} is missing"))
            })?;
            let instance_count = instances.len();
            for &node_id in fragment.scan_node_ids() {
                let ranges = view
                    .inner
                    .scan_ranges(fragment_id, node_id)
                    .ok_or_else(|| {
                        contract_error(format!(
                            "prepared scan ranges missing for fragment {fragment_id} node {node_id}"
                        ))
                    })?;
                for instance in &mut instances {
                    instance.scan_ranges.entry(node_id).or_default();
                }
                for (index, range) in ranges.iter().enumerate() {
                    instances[index % instance_count]
                        .scan_ranges
                        .entry(node_id)
                        .or_default()
                        .push(range.clone());
                }
                if let Some(connector_read) = view.inner.connector_read(fragment_id, node_id) {
                    for instance in &mut instances {
                        instance.connector_splits.entry(node_id).or_default();
                    }
                    for (index, split) in connector_read.splits.iter().enumerate() {
                        instances[index % instance_count]
                            .connector_splits
                            .entry(node_id)
                            .or_default()
                            .push(split.clone());
                    }
                }
            }
            let total_ranges = instances
                .iter()
                .flat_map(|instance| instance.scan_ranges.values())
                .map(Vec::len)
                .sum::<usize>()
                + instances
                    .iter()
                    .flat_map(|instance| instance.connector_splits.values())
                    .map(Vec::len)
                    .sum::<usize>();
            if total_ranges > 0
                && instances.iter().any(|instance| {
                    instance.scan_ranges.values().all(Vec::is_empty)
                        && instance.connector_splits.values().all(Vec::is_empty)
                })
            {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} creates an empty scan instance"
                )));
            }
            by_fragment.insert(fragment_id, instances);
        }

        let root_fragment_id = view.execution_anchor();
        let root = by_fragment
            .get(&root_fragment_id)
            .and_then(|placements| placements.first())
            .ok_or_else(|| contract_error("frontend schedule root has no placement"))?;
        let root_finst_id = root.finst_id;
        let root_backend_idx = root.backend_idx;
        let mut inner = SchedulingPlan {
            root_fragment_id,
            by_fragment,
            root_finst_id,
            root_backend_idx,
        };
        populate_destinations(&mut inner, view.inner.edges());
        populate_sender_counts(&mut inner, view.inner.edges());
        let lifecycle =
            build_fragment_lifecycle_projection(&inner, view.inner.edges(), frozen_live_backends)?;
        Ok(Self {
            handoff_id: view.handoff_id,
            execution_id,
            inner,
            lifecycle,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn backend_ids(&self) -> Vec<usize> {
        self.inner
            .by_fragment
            .values()
            .flatten()
            .map(|placement| placement.backend_idx)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn fragment_instance_ids(&self) -> Vec<UniqueId> {
        self.inner
            .by_fragment
            .values()
            .flatten()
            .map(|placement| placement.finst_id)
            .collect()
    }

    pub(crate) const fn lifecycle_projection(&self) -> &FragmentLifecycleProjection {
        &self.lifecycle
    }

    /// Return the immutable placement result used to freeze a connector write
    /// manifest before the binding-install barrier.  The schedule remains
    /// sealed: callers can inspect it for control planning but cannot alter
    /// placements after writer identities have been derived.
    pub(crate) const fn planning_schedule(&self) -> &SchedulingPlan {
        &self.inner
    }
}

const FRAGMENT_INSTANCE_ID_DOMAIN: &[u8] = b"novarocks.query-lifecycle.fragment-instance.v1\0";

fn derive_fragment_instance_id(
    execution_id: QueryExecutionId,
    fragment_id: FragmentId,
    instance_index: usize,
) -> Result<UniqueId, DistributedQueryError> {
    let instance_index = u64::try_from(instance_index)
        .map_err(|_| contract_error("fragment instance index exceeds u64 width"))?;
    let mut digest = Sha256::new();
    digest.update(FRAGMENT_INSTANCE_ID_DOMAIN);
    digest.update(execution_id.query_id().high().to_be_bytes());
    digest.update(execution_id.query_id().low().to_be_bytes());
    digest.update(execution_id.attempt_id().get().to_be_bytes());
    digest.update(fragment_id.to_be_bytes());
    digest.update(instance_index.to_be_bytes());
    let bytes = digest.finalize();
    let hi = i64::from_be_bytes(
        bytes[0..8]
            .try_into()
            .expect("SHA-256 prefix contains eight high bytes"),
    );
    let mut lo = i64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("SHA-256 prefix contains eight low bytes"),
    );
    if hi == 0 && lo == 0 {
        lo = 1;
    }
    Ok(UniqueId { hi, lo })
}

#[cfg(feature = "query-execution-contract-test-support")]
pub fn fragment_instance_id_for_contract_test(
    query_id: QueryId,
    fragment_id: FragmentId,
    instance_index: usize,
) -> UniqueId {
    let execution_id = QueryExecutionId::new(
        query_id,
        crate::query_execution::lifecycle::AttemptId::new(1)
            .expect("contract fixtures use a nonzero initial attempt"),
    )
    .expect("contract fixtures use a nonzero query id");
    derive_fragment_instance_id(execution_id, fragment_id, instance_index)
        .expect("contract fixture fragment identity is representable")
}

fn build_fragment_lifecycle_projection(
    schedule: &SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
    frozen_live_backends: BTreeMap<usize, LiveBackendTarget>,
) -> Result<FragmentLifecycleProjection, DistributedQueryError> {
    let mut instances_by_backend = BTreeMap::<usize, BTreeSet<UniqueId>>::new();
    let mut endpoints_by_backend = BTreeMap::<usize, RuntimeEndpoint>::new();
    for placement in schedule.by_fragment.values().flatten() {
        instances_by_backend
            .entry(placement.backend_idx)
            .or_default()
            .insert(placement.finst_id);
        match endpoints_by_backend.entry(placement.backend_idx) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(placement.endpoint.clone());
            }
            std::collections::btree_map::Entry::Occupied(entry)
                if entry.get() != &placement.endpoint =>
            {
                return Err(contract_error(format!(
                    "scheduled backend {} has inconsistent endpoints",
                    placement.backend_idx
                )));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }

    let mut exchange_routes = Vec::new();
    for edge in edges {
        let sources = schedule
            .by_fragment
            .get(&edge.source_fragment_id)
            .ok_or_else(|| {
                contract_error(format!(
                    "exchange source fragment {} is absent from schedule",
                    edge.source_fragment_id
                ))
            })?;
        let destinations = schedule
            .by_fragment
            .get(&edge.target_fragment_id)
            .ok_or_else(|| {
                contract_error(format!(
                    "exchange destination fragment {} is absent from schedule",
                    edge.target_fragment_id
                ))
            })?;
        let sender_count = u32::try_from(sources.len())
            .map_err(|_| contract_error("exchange sender count exceeds u32 width"))?;
        for (sender_ordinal, source) in sources.iter().enumerate() {
            let sender_ordinal = u32::try_from(sender_ordinal)
                .map_err(|_| contract_error("exchange sender ordinal exceeds u32 width"))?;
            for destination in destinations {
                exchange_routes.push(
                    ExchangeRouteManifest::new(
                        source.finst_id,
                        destination.finst_id,
                        edge.target_exchange_node_id,
                        sender_ordinal,
                        sender_count,
                    )
                    .map_err(|error| contract_error(error.to_string()))?,
                );
            }
        }
    }
    FragmentLifecycleProjection::new(instances_by_backend, endpoints_by_backend, exchange_routes)
        .with_frozen_live_backends(frozen_live_backends.into_values().collect())
}

fn populate_destinations(
    schedule: &mut SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
) {
    for edge in edges {
        let destinations = schedule
            .by_fragment
            .get(&edge.target_fragment_id)
            .into_iter()
            .flatten()
            .map(|placement| {
                FragmentDestination::new(placement.finst_id, placement.endpoint.clone())
            })
            .collect::<Vec<_>>();
        if let Some(sources) = schedule.by_fragment.get_mut(&edge.source_fragment_id) {
            for source in sources {
                source.destinations.extend(destinations.iter().cloned());
            }
        }
    }
}

fn populate_sender_counts(
    schedule: &mut SchedulingPlan,
    edges: &[crate::sql::planner::distributed::FragmentEdge],
) {
    for edge in edges {
        let upstream = schedule
            .by_fragment
            .get(&edge.source_fragment_id)
            .map(Vec::len)
            .unwrap_or_default() as i32;
        if let Some(targets) = schedule.by_fragment.get_mut(&edge.target_fragment_id) {
            for target in targets {
                *target
                    .per_exch_num_senders
                    .entry(edge.target_exchange_node_id)
                    .or_insert(0) += upstream;
            }
        }
    }
}

/// Owned core input for per-placement native submission assembly.
pub struct NativeSubmissionContext {
    pub(crate) query_id: QueryId,
    pub(crate) options: crate::runtime::query_options::QueryOptions,
}

impl NativeSubmissionContext {
    pub fn new(query_id: QueryId, options: &ResolvedQueryOptions) -> Self {
        Self {
            query_id,
            options: options.runtime_options().clone(),
        }
    }
}

pub struct ValidatedNativeSubmission {
    backend_idx: usize,
    finst_id: UniqueId,
    execution_id: QueryExecutionId,
    plan: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
}

impl ValidatedNativeSubmission {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.finst_id
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn into_stage_fragment(self) -> Result<(usize, StageFragment), DistributedQueryError> {
        let fragment = StageFragment::new(self.plan, self.instance_params)
            .map_err(|error| contract_error(error.to_string()))?;
        if fragment.fragment_instance_id() != self.finst_id {
            return Err(contract_error(
                "native stage fragment instance identity differs from sealed submission",
            ));
        }
        Ok((self.backend_idx, fragment))
    }
}

pub struct RootFetchMetadata {
    fragment_id: FragmentId,
    backend_idx: usize,
    finst_id: UniqueId,
    uses_result_buffer: bool,
}

impl RootFetchMetadata {
    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.finst_id
    }

    pub const fn uses_result_buffer(&self) -> bool {
        self.uses_result_buffer
    }
}

pub(crate) struct WriterRegistration {
    pub(crate) query_id: UniqueId,
    /// The immutable native query attempt that owns this writer.  The staged
    /// report envelope repeats this identity, so report aggregation can reject
    /// a late report from a previous attempt before any provider commit work.
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) fragment_id: FragmentId,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) backend_num: i32,
}

pub struct WriterRegistrationSet {
    registrations: Vec<WriterRegistration>,
}

impl WriterRegistrationSet {
    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    pub fn fragment_instance_ids(&self) -> Vec<UniqueId> {
        self.registrations
            .iter()
            .map(|registration| registration.fragment_instance_id)
            .collect()
    }

    pub fn writer_identities(&self) -> Vec<(UniqueId, i32)> {
        self.registrations
            .iter()
            .map(|registration| (registration.fragment_instance_id, registration.backend_num))
            .collect()
    }

    pub(crate) fn into_registrations(self) -> Vec<WriterRegistration> {
        self.registrations
    }
}

pub struct ExpectedOutputSchema {
    output_columns: Vec<PreparedOutputColumn>,
    chunk_schema: ChunkSchemaRef,
}

impl ExpectedOutputSchema {
    pub fn fetch_view(&self) -> ExpectedOutputSchemaView<'_> {
        ExpectedOutputSchemaView::new(&self.chunk_schema)
    }

    pub fn into_query_result(
        self,
        batches: Vec<FetchedQueryBatch>,
    ) -> Result<QueryResult, DistributedQueryError> {
        let chunks = batches
            .into_iter()
            .map(FetchedQueryBatch::into_chunk)
            .collect();
        let chunks = crate::query_execution::assembly::align_fetch_chunks_to_output_columns(
            chunks,
            &self.output_columns,
        )
        .map_err(contract_error)?;
        Ok(QueryResult {
            columns: self
                .output_columns
                .into_iter()
                .map(|column| QueryResultColumn {
                    name: column.name,
                    data_type: column.data_type,
                    nullable: column.nullable,
                    logical_type: None,
                })
                .collect(),
            chunks,
        })
    }
}

pub struct StagePreparedDistributedQuery {
    batches: Vec<StageBatch>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
    query_lifecycle_lease: QueryLifecycleLease,
    connector_binding_lease: ConnectorBindingInstallLease,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

struct AssembledNativeExecution {
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
}

#[cfg(test)]
pub(crate) struct InProcessTestSubmission {
    pub(crate) plan: crate::proto::plan::PlanFragment,
    pub(crate) instance_params: crate::proto::novarocks::InstanceParams,
}

#[cfg(test)]
pub(crate) struct InProcessTestArtifact {
    pub(crate) submissions: Vec<InProcessTestSubmission>,
    pub(crate) root_fetch: RootFetchMetadata,
    pub(crate) writer_registrations: WriterRegistrationSet,
    pub(crate) expected_output: ExpectedOutputSchema,
    pub(crate) declarations: Vec<ConnectorExecutionDeclaration>,
    pub(crate) runtime_filters: Vec<RuntimeFilterContribution>,
}

impl StagePreparedDistributedQuery {
    pub fn connector_write_plan(&self) -> Option<&ConnectorWritePlanAttachment> {
        self.connector_write_plan.as_ref()
    }

    pub fn batches(&self) -> &[StageBatch] {
        &self.batches
    }

    pub fn execution_registration_view(&self) -> ExecutionRegistrationView {
        ExecutionRegistrationView {
            attempted_instances: self
                .batches
                .iter()
                .flat_map(|batch| {
                    batch.request().fragments().iter().map(move |fragment| {
                        (
                            batch.binding().target().backend_idx(),
                            fragment.fragment_instance_id(),
                        )
                    })
                })
                .collect(),
            writer_identities: self.writer_registrations.writer_identities(),
        }
    }

    pub fn stage(
        self,
        barrier: &dyn QueryLaunchBarrier,
    ) -> Result<StagedDistributedQuery, DistributedQueryError> {
        if let Err(error) = barrier.stage_all(&self.batches) {
            let kind = error.kind();
            let message = self
                .query_lifecycle_lease
                .abort_preserving(error.message().to_string());
            let message = self.connector_binding_lease.abort_preserving(message);
            return Err(DistributedQueryError::new(kind, message));
        }
        Ok(StagedDistributedQuery {
            batches: self.batches,
            root_fetch: self.root_fetch,
            writer_registrations: self.writer_registrations,
            expected_output: self.expected_output,
            query_lifecycle_lease: self.query_lifecycle_lease,
            connector_binding_lease: self.connector_binding_lease,
            connector_write_plan: self.connector_write_plan,
        })
    }
}

/// Read-only pre-Start registration data.  It is intentionally separate from
/// result/fetch ownership, which remains unavailable until Running.
pub struct ExecutionRegistrationView {
    attempted_instances: Vec<(usize, UniqueId)>,
    writer_identities: Vec<(UniqueId, i32)>,
}

impl ExecutionRegistrationView {
    pub fn attempted_instances(&self) -> &[(usize, UniqueId)] {
        &self.attempted_instances
    }

    pub fn writer_identities(&self) -> &[(UniqueId, i32)] {
        &self.writer_identities
    }
}

pub struct StagedDistributedQuery {
    batches: Vec<StageBatch>,
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
    query_lifecycle_lease: QueryLifecycleLease,
    connector_binding_lease: ConnectorBindingInstallLease,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

impl StagedDistributedQuery {
    pub fn batches(&self) -> &[StageBatch] {
        &self.batches
    }

    pub fn start(
        self,
        barrier: &dyn QueryLaunchBarrier,
    ) -> Result<RunningDistributedQuery, DistributedQueryError> {
        if let Err(error) = barrier.start_all(&self.batches) {
            let kind = error.kind();
            let message = self
                .query_lifecycle_lease
                .abort_preserving(error.message().to_string());
            let message = self.connector_binding_lease.abort_preserving(message);
            return Err(DistributedQueryError::new(kind, message));
        }
        Ok(RunningDistributedQuery {
            root_fetch: self.root_fetch,
            writer_registrations: self.writer_registrations,
            expected_output: self.expected_output,
            query_lifecycle_lease: self.query_lifecycle_lease,
            connector_binding_lease: self.connector_binding_lease,
            connector_write_plan: self.connector_write_plan,
        })
    }
}

/// Running-only execution ownership. No public constructor or inverse
/// recombination API exists.
pub struct RunningDistributedQuery {
    root_fetch: RootFetchMetadata,
    writer_registrations: WriterRegistrationSet,
    expected_output: ExpectedOutputSchema,
    query_lifecycle_lease: QueryLifecycleLease,
    connector_binding_lease: ConnectorBindingInstallLease,
    connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

impl RunningDistributedQuery {
    pub fn into_parts(self) -> RunningNativeExecutionParts {
        RunningNativeExecutionParts {
            root_fetch: self.root_fetch,
            writer_registrations: self.writer_registrations,
            expected_output: self.expected_output,
            query_lifecycle_lease: self.query_lifecycle_lease,
            connector_binding_lease: self.connector_binding_lease,
            connector_write_plan: self.connector_write_plan,
        }
    }
}

pub struct RunningNativeExecutionParts {
    pub root_fetch: RootFetchMetadata,
    pub writer_registrations: WriterRegistrationSet,
    pub expected_output: ExpectedOutputSchema,
    pub query_lifecycle_lease: QueryLifecycleLease,
    pub connector_binding_lease: ConnectorBindingInstallLease,
    pub connector_write_plan: Option<ConnectorWritePlanAttachment>,
}

fn assemble_native_execution(
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    schedule: SchedulingPlan,
    execution_id: QueryExecutionId,
    context: NativeSubmissionContext,
    connector_write_plan: Option<&ConnectorWritePlanAttachment>,
) -> Result<AssembledNativeExecution, DistributedQueryError> {
    crate::query_execution::assembly::validate_prepared_native_payloads(&prepared, &native_bundle)
        .map_err(contract_error)?;
    crate::query_execution::assembly::validate_artifact_fragment_sets(
        &prepared,
        &native_bundle,
        &schedule,
    )
    .map_err(contract_error)?;
    crate::query_execution::assembly::validate_scheduling_placements(&schedule)
        .map_err(contract_error)?;

    let prepared_ids = prepared.fragment_ids();
    let native_ids = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    let scheduled_ids = schedule.fragment_ids().collect::<BTreeSet<_>>();
    if prepared_ids != native_ids || prepared_ids != scheduled_ids {
        return Err(contract_error(format!(
            "prepared/native/scheduled fragment sets differ: prepared={prepared_ids:?}, native={native_ids:?}, scheduled={scheduled_ids:?}"
        )));
    }

    let query_id = context.query_id.into_unique_id();
    let root_fragment_id = schedule.root_fragment_id;
    let root = prepared
        .fragment(root_fragment_id)
        .ok_or_else(|| contract_error("prepared execution root is missing"))?;
    let expected_output = build_expected_output_schema(root)?;
    let root_fetch = RootFetchMetadata {
        fragment_id: root_fragment_id,
        backend_idx: schedule.root_backend_idx,
        finst_id: schedule.root_finst_id,
        uses_result_buffer: root.execution_role().uses_result_buffer(),
    };

    let edges = prepared.scheduling_view().edges().to_vec();
    let stream_edge_by_source =
        crate::query_execution::assembly::build_stream_edge_by_source(&edges);
    let router_edges_by_source: BTreeMap<
        FragmentId,
        (i32, Vec<&crate::sql::planner::distributed::FragmentEdge>),
    > = crate::query_execution::assembly::group_router_edges_by_source(&edges)
        .into_iter()
        .map(|((source_fragment_id, router_group_id), branch_edges)| {
            (source_fragment_id, (router_group_id, branch_edges))
        })
        .collect();
    let mut cte_consumers: BTreeMap<
        CteId,
        Vec<(
            FragmentId,
            i32,
            crate::proto::plan::DataPartition,
            Vec<i32>,
            Vec<ColumnId>,
        )>,
    > = BTreeMap::new();
    for edge in &edges {
        if let FragmentEdgeKind::CteMulticast {
            cte_id,
            receive_producer_column_ids,
        } = &edge.edge_kind
        {
            let native_partition =
                crate::protocol::native::encode::encode_data_partition(&edge.output_partition)
                    .map_err(contract_error)?;
            cte_consumers.entry(*cte_id).or_default().push((
                edge.target_fragment_id,
                edge.target_exchange_node_id,
                native_partition,
                edge.output_slot_ids.clone(),
                receive_producer_column_ids.clone(),
            ));
        }
    }
    for fragment in prepared.scheduling_view().fragments() {
        for (cte_id, exchange_node_id, receive_producer_column_ids) in
            fragment.boundary_projection().cte_exchange_nodes()
        {
            let consumers = cte_consumers.entry(*cte_id).or_default();
            if !consumers.iter().any(|(fid, nid, _, _, _)| {
                *fid == fragment.fragment_id() && *nid == *exchange_node_id
            }) {
                consumers.push((
                    fragment.fragment_id(),
                    *exchange_node_id,
                    crate::proto::plan::DataPartition {
                        kind: crate::proto::plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    },
                    Vec::new(),
                    receive_producer_column_ids.clone(),
                ));
            }
        }
    }
    let consumer_destinations = schedule
        .by_fragment
        .iter()
        .map(|(fragment_id, placements)| {
            let destinations = placements
                .iter()
                .map(|placement| {
                    FragmentDestination::new(placement.finst_id, placement.endpoint.clone())
                })
                .collect();
            (*fragment_id, destinations)
        })
        .collect::<BTreeMap<_, _>>();

    let mut native_by_fragment = native_bundle
        .into_fragments()
        .collect::<BTreeMap<PlannerFragmentId, _>>();
    let mut submissions_by_fragment = BTreeMap::new();
    let mut writer_registrations = Vec::new();
    let mut consumed_connector_writers = BTreeSet::new();
    for (&fragment_id, placements) in &schedule.by_fragment {
        let fragment = prepared
            .fragment(fragment_id)
            .ok_or_else(|| contract_error(format!("prepared fragment {fragment_id} is missing")))?;
        let template = native_by_fragment.remove(&fragment_id).ok_or_else(|| {
            contract_error(format!("native fragment template {fragment_id} is missing"))
        })?;
        let is_root = fragment_id == root_fragment_id;
        let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
        let router_edges = router_edges_by_source.get(&fragment_id);
        let is_writer = stream_edge.is_none()
            && router_edges.is_none()
            && fragment.boundary_projection().cte_id().is_none()
            && fragment.execution_role().is_terminal_write();
        let is_producer = stream_edge.is_some()
            || router_edges.is_some()
            || fragment.boundary_projection().cte_id().is_some();
        crate::query_execution::assembly::validate_fragment_output_kind(
            fragment_id,
            is_root,
            is_writer,
            is_producer,
            fragment.execution_role(),
        )
        .map_err(contract_error)?;
        crate::query_execution::assembly::ensure_native_fragment_sink_supported(
            fragment_id,
            is_root,
            is_writer,
            stream_edge.is_some(),
            router_edges.is_some(),
            fragment.boundary_projection().cte_id().is_some(),
        )
        .map_err(contract_error)?;
        let fragment_submissions = placements
            .iter()
            .map(|placement| {
                if is_writer {
                    writer_registrations.push(WriterRegistration {
                        query_id,
                        execution_id,
                        fragment_id,
                        fragment_instance_id: placement.finst_id,
                        backend_num: placement.instance_index as i32,
                    });
                }
                let mut native_fragment = template.clone();
                if is_writer {
                    if let Some(attachment) = connector_write_plan {
                        let backend_num = i32::try_from(placement.instance_index).map_err(|_| {
                            contract_error("connector writer backend number exceeds i32 width")
                        })?;
                        let writer_fragment_id = i32::try_from(fragment_id).map_err(|_| {
                            contract_error("connector writer fragment ID exceeds i32 width")
                        })?;
                        let handle = attachment
                            .plan()
                            .handles()
                            .iter()
                            .find(|handle| {
                                let writer = handle.writer();
                                writer.fragment_id() == writer_fragment_id
                                    && writer.backend_num() == backend_num
                                    && writer.fragment_instance_id()
                                        == connector_writer_unique_id_bytes(placement.finst_id)
                                    && writer.sink_ordinal() == 0
                            })
                            .ok_or_else(|| {
                                contract_error(format!(
                                    "connector write plan has no handle for terminal writer fragment={fragment_id} backend_num={backend_num} finst={:?}",
                                    placement.finst_id
                                ))
                            })?;
                        if !consumed_connector_writers.insert(handle.writer().clone()) {
                            return Err(contract_error(format!(
                                "connector write plan reuses a writer handle for terminal writer fragment={fragment_id} backend_num={backend_num}"
                            )));
                        }
                        crate::query_execution::assembly::patch_native_connector_write_sink(
                            &mut native_fragment,
                            fragment_id,
                            placement.finst_id,
                            backend_num,
                            handle,
                        )
                        .map_err(contract_error)?;
                    }
                }
                for (&node_id, splits) in &placement.connector_splits {
                    crate::query_execution::assembly::patch_native_connector_read_splits(
                        &mut native_fragment,
                        node_id,
                        splits,
                    )
                    .map_err(contract_error)?;
                }
                if !is_root && !is_writer && stream_edge.is_none() {
                    if let Some((router_group_id, branch_edges)) = router_edges {
                        crate::query_execution::assembly::
                            patch_native_change_stream_router_sink(
                                &mut native_fragment,
                                fragment_id,
                                *router_group_id,
                                branch_edges,
                                &schedule.by_fragment,
                            )
                            .map_err(contract_error)?;
                    } else if let Some(cte_id) = fragment.boundary_projection().cte_id() {
                        let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                        crate::query_execution::assembly::patch_native_cte_multicast_sink(
                            &mut native_fragment,
                            fragment_id,
                            cte_id,
                            &consumers,
                            &consumer_destinations,
                        )
                        .map_err(contract_error)?;
                    }
                }
                let instance_params = crate::protocol::native::encode::encode_instance_params(
                    &query_id,
                    placement,
                    &context.options,
                    placement.instance_index as i32,
                    fragment_id == root_fragment_id,
                )
                .map_err(contract_error)?;
                Ok(ValidatedNativeSubmission {
                    backend_idx: placement.backend_idx,
                    finst_id: placement.finst_id,
                    execution_id,
                    plan: native_fragment,
                    instance_params,
                })
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        submissions_by_fragment.insert(fragment_id, fragment_submissions);
    }
    if !native_by_fragment.is_empty() {
        return Err(contract_error(format!(
            "native templates remained after assembly: {:?}",
            native_by_fragment.keys().collect::<Vec<_>>()
        )));
    }

    let mut submissions = Vec::new();
    for &fragment_id in prepared.scheduling_view().topological_order().iter().rev() {
        let mut fragment_submissions =
            submissions_by_fragment
                .remove(&fragment_id)
                .ok_or_else(|| {
                    contract_error(format!("assembled fragment {fragment_id} is missing"))
                })?;
        submissions.append(&mut fragment_submissions);
    }
    if !submissions_by_fragment.is_empty() {
        return Err(contract_error(
            "assembled submissions contain unknown fragments",
        ));
    }
    if let Some(attachment) = connector_write_plan {
        let expected = attachment
            .manifest()
            .writers()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if consumed_connector_writers != expected {
            let missing = expected
                .difference(&consumed_connector_writers)
                .collect::<Vec<_>>();
            let unexpected = consumed_connector_writers
                .difference(&expected)
                .collect::<Vec<_>>();
            return Err(contract_error(format!(
                "connector write plan consumption does not exactly cover the frozen manifest: missing={missing:?} unexpected={unexpected:?}"
            )));
        }
    }

    Ok(AssembledNativeExecution {
        submissions,
        root_fetch,
        writer_registrations: WriterRegistrationSet {
            registrations: writer_registrations,
        },
        expected_output,
    })
}

fn connector_writer_unique_id_bytes(value: UniqueId) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.hi.to_be_bytes());
    bytes[8..].copy_from_slice(&value.lo.to_be_bytes());
    bytes
}

fn build_expected_output_schema(
    root: &PreparedFragment,
) -> Result<ExpectedOutputSchema, DistributedQueryError> {
    let output_columns = root.boundary_projection().output_columns().to_vec();
    let chunk_schema = if output_columns.is_empty() {
        Arc::new(ChunkSchema::empty())
    } else {
        let slots = output_columns
            .iter()
            .enumerate()
            .map(|(index, output)| {
                let slot = u32::try_from(index + 1)
                    .map(SlotId::new)
                    .map_err(|_| contract_error("too many root output columns"))?;
                Ok(ChunkSlotSchema::new_with_field(
                    slot,
                    Field::new(
                        output.name.clone(),
                        output.data_type.clone(),
                        output.nullable,
                    ),
                    None,
                    None,
                ))
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Arc::new(ChunkSchema::try_new(slots).map_err(contract_error)?)
    };
    Ok(ExpectedOutputSchema {
        output_columns,
        chunk_schema,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
        ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorProviderId,
        ConnectorRequestContext, ConnectorTableHandle, ConnectorWriteAbortOutcome,
        ConnectorWriteAbortRequest, ConnectorWriteCommitRequest, ConnectorWriteControl,
        ConnectorWriteExecutionId, ConnectorWriteLease, ConnectorWriteOperationId,
        ConnectorWritePlan, ConnectorWritePlanningRequest, ConnectorWriteReceipt,
        ConnectorWriteReconcileRequest, ConnectorWriterHandle,
    };

    use super::{
        attach_connector_write_plan, build_fragment_lifecycle_projection,
        derive_fragment_instance_id,
    };
    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{AttemptId, ExchangeRouteManifest, QueryExecutionId};
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::sql::planner::distributed::{
        DataPartition, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
    };

    fn placement(
        fragment_id: u32,
        instance_index: usize,
        finst_id: UniqueId,
        backend_idx: usize,
    ) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id,
            backend_idx,
            endpoint: RuntimeEndpoint::new("127.0.0.1", 19040 + backend_idx as i32)
                .expect("valid endpoint"),
            scan_ranges: BTreeMap::new(),
            connector_splits: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn stream_edge(source_fragment_id: u32, target_fragment_id: u32, node_id: i32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id,
            target_fragment_id,
            target_exchange_node_id: node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    struct NeverCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct TestWriteControl {
        key: ConnectorExecutionBindingKey,
    }

    struct TestWriteDistribution {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorExecutionDistribution for TestWriteDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
            ConnectorExecutionDeclaration::try_new(
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("test").expect("valid provider ID"),
                    instance_id: self.key.instance_id.clone(),
                },
                self.key.incarnation,
                Bytes::from_static(b"test-write-binding"),
            )
        }
    }

    impl ConnectorWriteControl for TestWriteControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn plan_write(
            &self,
            request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            let handles = request
                .expected_writers
                .into_iter()
                .map(|writer| {
                    ConnectorWriterHandle::try_new(
                        self.key.clone(),
                        writer,
                        1,
                        Bytes::from_static(b"test-handle"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            ConnectorWritePlan::try_new(
                self.key.clone(),
                request.operation_id,
                request.cohort_id,
                request.execution_id,
                handles,
                Bytes::new(),
            )
        }

        fn commit(
            &self,
            _request: ConnectorWriteCommitRequest,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<ConnectorWriteReceipt>,
            ConnectorError,
        > {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test control does not commit",
            ))
        }

        fn abort(
            &self,
            _request: ConnectorWriteAbortRequest,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test control does not abort",
            ))
        }

        fn reconcile(
            &self,
            _request: ConnectorWriteReconcileRequest,
        ) -> Result<
            novarocks_spi::connector::ExternalMutationOutcome<ConnectorWriteReceipt>,
            ConnectorError,
        > {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test control does not reconcile",
            ))
        }
    }

    fn write_owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("test-write").expect("valid instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    fn write_execution() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(3).expect("valid attempt"),
        )
        .expect("valid execution")
    }

    fn write_schedule(finst_id: UniqueId) -> SchedulingPlan {
        SchedulingPlan {
            root_fragment_id: 3,
            by_fragment: BTreeMap::from([(3, vec![placement(3, 0, finst_id, 8)])]),
            root_finst_id: finst_id,
            root_backend_idx: 8,
        }
    }

    fn planned_attachment(schedule: &SchedulingPlan) -> super::ConnectorWritePlanAttachment {
        let owner = write_owner();
        let operation_id = ConnectorWriteOperationId::from_bytes([4; 16]);
        let execution_id = write_execution();
        let manifest = crate::query_execution::write_plan::ConnectorWriteManifest::freeze(
            schedule,
            &BTreeSet::from([3]),
            operation_id,
            novarocks_spi::connector::ConnectorWriteCohortId::primary(operation_id),
            owner.clone(),
            execution_id,
        )
        .expect("freeze manifest");
        let control: Arc<dyn ConnectorWriteControl> =
            Arc::new(TestWriteControl { key: owner.clone() });
        let lease = ConnectorWriteLease::new_with_execution_distribution(
            owner.clone(),
            control,
            Arc::new(TestWriteDistribution { key: owner.clone() }),
            || {},
        )
        .expect("valid exact control lease");
        let query_id = execution_id.query_id();
        let mut query_id_bytes = [0; 16];
        query_id_bytes[..8].copy_from_slice(&query_id.high().to_be_bytes());
        query_id_bytes[8..].copy_from_slice(&query_id.low().to_be_bytes());
        manifest
            .plan(
                lease,
                ConnectorWritePlanningRequest {
                    operation_id,
                    cohort_id: manifest.cohort_id(),
                    execution_id: ConnectorWriteExecutionId::new(
                        query_id_bytes,
                        execution_id.attempt_id().get(),
                    ),
                    table: ConnectorTableHandle::try_new(owner.instance_id.clone(), Bytes::new())
                        .expect("valid table"),
                    intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
                    input_schema: Arc::new(Schema::empty()),
                    expected_writers: Vec::new(),
                    provider_payload: Bytes::new(),
                    context: ConnectorRequestContext::try_new(
                        Instant::now() + Duration::from_secs(1),
                        Arc::new(NeverCancelled),
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                    )
                    .expect("valid context"),
                },
            )
            .expect("provider returns the frozen writer manifest")
    }

    #[test]
    fn connector_write_attachment_rejects_duplicate_and_mismatched_placements() {
        let execution = write_execution();
        let schedule = write_schedule(UniqueId { hi: 3, lo: 30 });
        let mut slot = None;
        attach_connector_write_plan(
            &mut slot,
            &schedule,
            execution,
            planned_attachment(&schedule),
        )
        .expect("first attachment belongs to the exact schedule");
        let duplicate = attach_connector_write_plan(
            &mut slot,
            &schedule,
            execution,
            planned_attachment(&schedule),
        )
        .expect_err("a query may carry only one write attachment");
        assert!(duplicate.message().contains("already has"));

        let mismatched = write_schedule(UniqueId { hi: 3, lo: 31 });
        let mut mismatched_slot = None;
        let mismatch = attach_connector_write_plan(
            &mut mismatched_slot,
            &mismatched,
            execution,
            planned_attachment(&schedule),
        )
        .expect_err("an attachment cannot cross placement manifests");
        assert!(
            mismatch
                .message()
                .contains("does not match a validated fragment placement")
        );
        assert!(mismatched_slot.is_none());
    }

    #[test]
    fn fragment_instance_identity_is_stable_and_attempt_bound() {
        let query_id = QueryId::new(41, 73);
        let first_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("valid attempt"))
                .expect("valid execution id");
        let second_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(2).expect("valid attempt"))
                .expect("valid execution id");

        let first =
            derive_fragment_instance_id(first_attempt, 9, 3).expect("first fragment instance id");
        assert_eq!(
            derive_fragment_instance_id(first_attempt, 9, 3)
                .expect("repeated fragment instance id"),
            first
        );
        assert_ne!(
            derive_fragment_instance_id(second_attempt, 9, 3).expect("second fragment instance id"),
            first
        );
    }

    #[test]
    fn exchange_route_projection_canonicalizes_out_of_order_edges_and_placements() {
        let schedule = SchedulingPlan {
            root_fragment_id: 40,
            by_fragment: BTreeMap::from([
                (
                    10,
                    vec![
                        placement(10, 0, UniqueId { hi: 9, lo: 1 }, 0),
                        placement(10, 1, UniqueId { hi: 1, lo: 1 }, 1),
                    ],
                ),
                (
                    20,
                    vec![
                        placement(20, 0, UniqueId { hi: 8, lo: 1 }, 0),
                        placement(20, 1, UniqueId { hi: 2, lo: 1 }, 1),
                    ],
                ),
                (30, vec![placement(30, 0, UniqueId { hi: 7, lo: 1 }, 0)]),
                (40, vec![placement(40, 0, UniqueId { hi: 6, lo: 1 }, 1)]),
            ]),
            root_finst_id: UniqueId { hi: 6, lo: 1 },
            root_backend_idx: 1,
        };
        let edges = vec![stream_edge(30, 40, 400), stream_edge(10, 20, 200)];
        let live_backends = BTreeMap::from([
            (
                0,
                LiveBackendTarget::new(0, "127.0.0.1:19040".parse().expect("valid endpoint"), 100),
            ),
            (
                1,
                LiveBackendTarget::new(1, "127.0.0.1:19041".parse().expect("valid endpoint"), 101),
            ),
        ]);

        let projection = build_fragment_lifecycle_projection(&schedule, &edges, live_backends)
            .expect("valid lifecycle projection");
        let expected = vec![
            ExchangeRouteManifest::new(
                UniqueId { hi: 1, lo: 1 },
                UniqueId { hi: 2, lo: 1 },
                200,
                1,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 1, lo: 1 },
                UniqueId { hi: 8, lo: 1 },
                200,
                1,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 7, lo: 1 },
                UniqueId { hi: 6, lo: 1 },
                400,
                0,
                1,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 9, lo: 1 },
                UniqueId { hi: 2, lo: 1 },
                200,
                0,
                2,
            )
            .expect("valid route"),
            ExchangeRouteManifest::new(
                UniqueId { hi: 9, lo: 1 },
                UniqueId { hi: 8, lo: 1 },
                200,
                0,
                2,
            )
            .expect("valid route"),
        ];

        assert_eq!(projection.exchange_routes, expected);
    }
}
