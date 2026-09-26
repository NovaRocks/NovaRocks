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

pub(crate) mod native_submission;
#[allow(
    dead_code,
    reason = "The dormant-attempt binding is consumed through the Native execution adapter."
)]
mod task_manifest_binding;

#[allow(
    unused_imports,
    reason = "These move-only bindings form the internal Native adapter contract."
)]
pub(crate) use task_manifest_binding::{
    BoundManifestBackend, BoundManifestContext, BoundManifestEdge, BoundManifestFrozenUnits,
    BoundManifestPartitionKind, BoundManifestProducer, BoundManifestScanAssignment,
    BoundManifestScanWork, BoundManifestTask, FrozenAttemptTopology, TaskManifestBinding,
};

pub use native_submission::{
    NativeSubmissionAttachment, NativeSubmissionEncodingView, NativeSubmissionFragmentRole,
    NativeSubmissionKey,
};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::Field;
use novarocks_spi::connector::{CatalogHandle, CatalogProperties};
use sha2::{Digest, Sha256};

use crate::native::fragment_transport::{ExpectedOutputSchemaView, FetchedQueryBatch};
use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::{QueryCatalogLease, QueryInitOptions};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::preparation::runtime_filter_view::RuntimeFilterDeploymentFactsView;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_proto_models::novarocks;
use novarocks_query_application::api::{BackendTopologySnapshot, LiveBackendTarget};
use novarocks_query_application::api::{QueryResult, ResultField as QueryResultColumn};
#[cfg(test)]
use novarocks_types::QueryId;
use novarocks_types::{BackendProcessId, SlotId, UniqueId};

pub type FragmentId = u32;

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// Opaque identity minted with a sealed prepared handoff. Role crates can
/// compare it and pass it back to Core, but cannot construct a replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterArtifactId(u64);

pub struct RuntimeFilterDeploymentAttachment {
    artifact_id: RuntimeFilterArtifactId,
    execution_id: QueryExecutionId,
    contributions: BTreeMap<usize, novarocks_proto_models::novarocks::RuntimeFilterContribution>,
}

impl RuntimeFilterDeploymentAttachment {
    fn matches(
        &self,
        artifact_id: RuntimeFilterArtifactId,
        execution_id: QueryExecutionId,
    ) -> bool {
        self.artifact_id == artifact_id && self.execution_id == execution_id
    }
}

/// Static Native plan/projection shared by every attempt of one logical
/// execution. It owns no Connector access capability or planning lease.
pub(crate) struct PreparedDistributedNativeTemplate {
    identity: PreparedDistributedTemplateIdentity,
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_template: Arc<NativeFragmentAttachment>,
}

struct PreparedDistributedTemplateAffinity;

/// Private identity shared only by the two siblings minted from one prepared
/// logical execution. Scalar equality is insufficient: the pointer identity
/// prevents an access factory from being spliced onto an isomorphic Native
/// template assembled elsewhere.
#[derive(Clone)]
struct PreparedDistributedTemplateIdentity {
    handoff_id: u64,
    plan: novarocks_query_application::api::PlanSeal,
    affinity: Arc<PreparedDistributedTemplateAffinity>,
}

impl PreparedDistributedTemplateIdentity {
    fn new(handoff_id: u64, plan: novarocks_query_application::api::PlanSeal) -> Self {
        Self {
            handoff_id,
            plan,
            affinity: Arc::new(PreparedDistributedTemplateAffinity),
        }
    }

    fn exactly_matches(&self, other: &Self) -> bool {
        self.handoff_id == other.handoff_id
            && self.plan == other.plan
            && Arc::ptr_eq(&self.affinity, &other.affinity)
    }
}

fn validate_prepared_template_affinity(
    native: &PreparedDistributedTemplateIdentity,
    access: &PreparedDistributedTemplateIdentity,
) -> Result<(), DistributedQueryError> {
    if native.exactly_matches(access) {
        Ok(())
    } else {
        Err(contract_error(
            "Connector attempt access belongs to another prepared Native template",
        ))
    }
}

impl PreparedDistributedNativeTemplate {
    /// Which plan this template was built from.
    pub(crate) const fn plan(&self) -> novarocks_query_application::api::PlanSeal {
        self.identity.plan
    }

    pub(crate) fn plan_facts(
        &self,
    ) -> &crate::query_execution::attempt_plan_facts::AttemptPlanFacts {
        &self.plan_facts
    }

    /// The fragments this template has an encoded native payload for.
    pub(crate) fn native_fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.native_template.fragment_ids()
    }

    /// What the owner that places tasks reads about this plan.
    ///
    /// Projected from the attempt's own plan facts, so the answer does not
    /// depend on which representation produced the plan; the seal it carries
    /// is what keeps it from being joined to another plan's.
    pub(crate) fn attempt_scheduling_facts(
        &self,
    ) -> Result<novarocks_query_application::api::ExecutionSchedulingFacts, String> {
        self.plan_facts
            .scheduling()
            .attempt_scheduling_facts(self.identity.plan)
    }

    /// Bind the static Native template to the exact move-only attempt request
    /// while the request is still available. The resulting typestate is what
    /// the dormant owner carries into activation after `request.bind` consumes
    /// the Query Application ticket.
    fn fork_for_attempt(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            plan_facts: Arc::clone(&self.plan_facts),
            native_template: Arc::clone(&self.native_template),
        }
    }

    fn bind_request(
        self,
        request: &novarocks_query_application::api::NativeAttemptPreparationRequest,
        affinity: Arc<PreparedDistributedAttemptAffinity>,
    ) -> Result<RequestBoundNativeTemplate, DistributedQueryError> {
        validate_native_request_match(request.matches_plan_seal(self.plan()))?;
        Ok(RequestBoundNativeTemplate {
            execution: request.execution(),
            template: self,
            affinity,
        })
    }
}

struct PreparedDistributedAttemptAffinity;

/// Move-only proof that one Native template was checked against the exact
/// Query Application attempt request before that request was consumed.
pub(crate) struct RequestBoundNativeTemplate {
    execution: QueryExecutionId,
    template: PreparedDistributedNativeTemplate,
    affinity: Arc<PreparedDistributedAttemptAffinity>,
}

/// Per-attempt Connector access owner minted beside the request-bound Native
/// template. It is reusable within that attempt, but cannot be paired with a
/// sibling from another attempt or logical template.
pub(crate) struct PreparedDistributedAttemptAccessOwner {
    execution: QueryExecutionId,
    template_identity: PreparedDistributedTemplateIdentity,
    affinity: Arc<PreparedDistributedAttemptAffinity>,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl PreparedDistributedAttemptAccessOwner {
    pub(crate) fn instantiate(
        &self,
        manifest: &TaskManifestBinding,
    ) -> Result<PreparedDistributedQuery, DistributedQueryError> {
        let native = manifest.request_bound_native();
        validate_bound_attempt_affinity(native, self)?;
        Ok(PreparedDistributedQuery {
            handoff_id: native.template.identity.handoff_id,
            plan_facts: Arc::clone(&native.template.plan_facts),
            native_bundle: Arc::clone(&native.template.native_template),
            attempt_access: Arc::clone(&self.attempt_access),
        })
    }
}

/// One request-bound attempt handoff. Consuming it separates the dormant
/// Native owner from the exact Connector access owner without exposing Clone
/// on either capability.
pub(crate) struct PreparedDistributedBoundAttempt {
    native: RequestBoundNativeTemplate,
    access: PreparedDistributedAttemptAccessOwner,
}

impl PreparedDistributedBoundAttempt {
    pub(crate) fn into_native_and_access(
        self,
    ) -> (
        RequestBoundNativeTemplate,
        PreparedDistributedAttemptAccessOwner,
    ) {
        (self.native, self.access)
    }
}

/// Dormant Native inputs bound to the exact topology snapshot used to expose
/// eligible backend identities to Query Application scheduling.
///
/// The snapshot projection remains move-only until activation consumes it
/// together with the request-bound Native template. A caller cannot pass a
/// replacement snapshot to manifest binding later.
pub(crate) struct SnapshotBoundDormantAttemptInputs {
    native: RequestBoundNativeTemplate,
    access: PreparedDistributedAttemptAccessOwner,
    topology: FrozenAttemptTopology,
}

impl std::fmt::Debug for SnapshotBoundDormantAttemptInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotBoundDormantAttemptInputs")
            .field("execution", &self.native.execution)
            .field("topology", &self.topology)
            .finish_non_exhaustive()
    }
}

impl SnapshotBoundDormantAttemptInputs {
    pub(crate) fn capture(
        template: &PreparedDistributedAttemptTemplate,
        request: &novarocks_query_application::api::NativeAttemptPreparationRequest,
        snapshot: BackendTopologySnapshot,
    ) -> Result<Self, DistributedQueryError> {
        let topology =
            FrozenAttemptTopology::capture_for_candidate(snapshot, request.topology_requirement())?;
        let bound = template.bind_attempt(request)?;
        let (native, access) = bound.into_native_and_access();
        Ok(Self {
            native,
            access,
            topology,
        })
    }

    pub(crate) fn eligible_backends(&self) -> &[BackendProcessId] {
        self.topology.eligible_backends()
    }

    pub(crate) const fn topology_revision(&self) -> u64 {
        self.topology.revision()
    }

    pub(crate) fn bind_manifest(
        self,
        schedule: &novarocks_query_application::coordination::AttemptSchedule,
    ) -> Result<ManifestBoundNativeAttemptInputs, DistributedQueryError> {
        let manifest = TaskManifestBinding::bind(self.native, schedule, self.topology)?;
        Ok(ManifestBoundNativeAttemptInputs {
            manifest,
            access: self.access,
        })
    }
}

/// Exact activation inputs after the Query Application schedule has been
/// joined to its originating topology and Native template.
pub(crate) struct ManifestBoundNativeAttemptInputs {
    manifest: TaskManifestBinding,
    access: PreparedDistributedAttemptAccessOwner,
}

impl std::fmt::Debug for ManifestBoundNativeAttemptInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManifestBoundNativeAttemptInputs")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl ManifestBoundNativeAttemptInputs {
    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.manifest.execution()
    }

    /// Starts a borrowed activation transaction.
    ///
    /// The exact manifest and Connector access capability remain in this
    /// dormant owner while fallible Task runtime assembly runs. Only the
    /// derived prepared view can be committed into the active behavior, so an
    /// error, cancellation, or unwind cannot strand the sole dormant inputs.
    pub(crate) fn begin_activation(
        &mut self,
    ) -> Result<ManifestBoundNativeAttemptActivation<'_>, DistributedQueryError> {
        let prepared = self.access.instantiate(&self.manifest)?;
        Ok(ManifestBoundNativeAttemptActivation {
            inputs: self,
            prepared: Some(prepared),
        })
    }
}

/// Borrowed activation transaction over one exact manifest-bound owner.
///
/// Dropping this value simply drops the derived prepared view. The manifest
/// and access owner were never moved, which is the rollback path for every
/// early return and panic during activation.
pub(crate) struct ManifestBoundNativeAttemptActivation<'a> {
    inputs: &'a mut ManifestBoundNativeAttemptInputs,
    prepared: Option<PreparedDistributedQuery>,
}

impl std::fmt::Debug for ManifestBoundNativeAttemptActivation<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManifestBoundNativeAttemptActivation")
            .field("manifest", &self.inputs.manifest)
            .field("prepared_available", &self.prepared.is_some())
            .finish()
    }
}

impl<'a> ManifestBoundNativeAttemptActivation<'a> {
    /// The exact immutable inputs an assembler may project. No replacement
    /// manifest or access owner can be supplied through this API.
    pub(crate) fn parts(&mut self) -> (&mut PreparedDistributedQuery, &TaskManifestBinding) {
        (
            self.prepared
                .as_mut()
                .expect("an uncommitted activation retains its prepared view"),
            &self.inputs.manifest,
        )
    }

    /// Transfers only the derived prepared view after all fallible assembly
    /// has succeeded. The outer active owner continues to retain the exact
    /// manifest and access owner for the attempt lifetime.
    pub(crate) fn commit(mut self) -> PreparedDistributedQuery {
        self.prepared
            .take()
            .expect("one manifest activation can commit only once")
    }

    pub(crate) fn into_prepared_and_manifest(
        mut self,
    ) -> (PreparedDistributedQuery, &'a TaskManifestBinding) {
        let prepared = self
            .prepared
            .take()
            .expect("one manifest activation can project only once");
        (prepared, &self.inputs.manifest)
    }
}

fn validate_bound_attempt_affinity(
    native: &RequestBoundNativeTemplate,
    access: &PreparedDistributedAttemptAccessOwner,
) -> Result<(), DistributedQueryError> {
    validate_bound_attempt_identity(
        native.execution,
        &native.template.identity,
        &native.affinity,
        access.execution,
        &access.template_identity,
        &access.affinity,
    )
}

fn validate_bound_attempt_identity(
    native_execution: QueryExecutionId,
    native_template: &PreparedDistributedTemplateIdentity,
    native_affinity: &Arc<PreparedDistributedAttemptAffinity>,
    access_execution: QueryExecutionId,
    access_template: &PreparedDistributedTemplateIdentity,
    access_affinity: &Arc<PreparedDistributedAttemptAffinity>,
) -> Result<(), DistributedQueryError> {
    if native_execution == access_execution
        && native_template.exactly_matches(access_template)
        && Arc::ptr_eq(native_affinity, access_affinity)
    {
        Ok(())
    } else {
        Err(contract_error(
            "Connector attempt access belongs to another request-bound Native attempt",
        ))
    }
}

fn validate_native_request_match(matches: bool) -> Result<(), DistributedQueryError> {
    if matches {
        Ok(())
    } else {
        Err(contract_error(
            "attempt Native template belongs to another sealed preparation plan",
        ))
    }
}

/// Logical-execution owner that can materialize the Connector access view for
/// an attempt. Keeping it separate prevents a pure task manifest from retaining
/// catalog control leases or becoming a resource owner.
pub(crate) struct PreparedDistributedAttemptAccessFactory {
    identity: PreparedDistributedTemplateIdentity,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl PreparedDistributedAttemptAccessFactory {
    fn fork_for_attempt(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            attempt_access: Arc::clone(&self.attempt_access),
        }
    }

    fn instantiate(
        &self,
        native: &PreparedDistributedNativeTemplate,
    ) -> Result<PreparedDistributedQuery, DistributedQueryError> {
        validate_prepared_template_affinity(&native.identity, &self.identity)?;
        Ok(PreparedDistributedQuery {
            handoff_id: native.identity.handoff_id,
            plan_facts: Arc::clone(&native.plan_facts),
            native_bundle: Arc::clone(&native.native_template),
            attempt_access: Arc::clone(&self.attempt_access),
        })
    }
}

/// Immutable logical-execution owner of both static Native facts and the
/// separately held attempt-resource factory.
pub(crate) struct PreparedDistributedAttemptTemplate {
    native: PreparedDistributedNativeTemplate,
    access: PreparedDistributedAttemptAccessFactory,
}

impl PreparedDistributedAttemptTemplate {
    /// The same template, for a plan that was completed rather than sealed.
    ///
    /// The encoding that produced these facts already stamped the native
    /// fragments with its own provenance, and the scheduling facts carry it
    /// too; that provenance is this template's handoff identity, so nothing
    /// here can be paired with the products of another encoding.
    pub(crate) fn for_completed_plan(
        plan: novarocks_query_application::api::PlanSeal,
        plan_facts: crate::query_execution::attempt_plan_facts::AttemptPlanFacts,
        native_template: NativeFragmentAttachment,
        attempt_access: crate::query_execution::preparation::ConnectorAttemptAccessPlan,
    ) -> Self {
        let identity =
            PreparedDistributedTemplateIdentity::new(plan_facts.scheduling().handoff_id, plan);
        Self {
            native: PreparedDistributedNativeTemplate {
                identity: identity.clone(),
                plan_facts: Arc::new(plan_facts),
                native_template: Arc::new(native_template),
            },
            access: PreparedDistributedAttemptAccessFactory {
                identity,
                attempt_access: Arc::new(attempt_access),
            },
        }
    }

    pub(crate) const fn native_manifest_template(&self) -> &PreparedDistributedNativeTemplate {
        &self.native
    }

    /// What the owner that places tasks reads about this plan.
    pub(crate) fn attempt_scheduling_facts(
        &self,
    ) -> Result<novarocks_query_application::api::ExecutionSchedulingFacts, String> {
        self.native.attempt_scheduling_facts()
    }

    pub(crate) fn instantiate(&self) -> PreparedDistributedQuery {
        self.access
            .instantiate(&self.native)
            .expect("a prepared attempt template retains its exact sibling affinity")
    }

    /// Mint one fresh pair of request-bound attempt owners while retaining the
    /// logical template for later recovery attempts.
    pub(crate) fn bind_attempt(
        &self,
        request: &novarocks_query_application::api::NativeAttemptPreparationRequest,
    ) -> Result<PreparedDistributedBoundAttempt, DistributedQueryError> {
        let affinity = Arc::new(PreparedDistributedAttemptAffinity);
        let native = self
            .native
            .fork_for_attempt()
            .bind_request(request, Arc::clone(&affinity))?;
        let access_factory = self.access.fork_for_attempt();
        let access = PreparedDistributedAttemptAccessOwner {
            execution: request.execution(),
            template_identity: access_factory.identity,
            affinity,
            attempt_access: access_factory.attempt_access,
        };
        Ok(PreparedDistributedBoundAttempt { native, access })
    }
}

/// One attempt's owned prepared/native typestate. It can only be instantiated
/// from the logical execution's immutable attempt template.
///
/// Its native payload is the template's own, shared: an attempt holds the
/// static plans the completed plan froze and never a copy of them.
pub struct PreparedDistributedQuery {
    handoff_id: u64,
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_bundle: Arc<NativeFragmentAttachment>,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl PreparedDistributedQuery {
    /// Whether two attempts create their tasks from the same frozen plans.
    #[cfg(test)]
    pub(crate) fn shares_native_fragments_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.native_bundle, &other.native_bundle)
    }

    /// The facts scheduling reads, as a value rather than a borrow of the
    /// plan carrier.
    ///
    /// Scheduling never wanted the carrier: it reads which fragments exist, in
    /// what order, which of them read a provider and how that read gets its
    /// work, and how rows flow between them. Handing it a value keeps it that
    /// way, and is what lets a completed plan and a sealed plan reach the same
    /// scheduler by projecting into the same facts.
    pub fn scheduling_facts(
        &self,
    ) -> &crate::query_execution::fragment_scheduling::FragmentSchedulingFacts {
        self.plan_facts.scheduling()
    }

    pub(crate) fn write_root_targets(
        &self,
    ) -> Option<&[novarocks_spi::connector::write_stack::WriteTargetOrdinal]> {
        self.plan_facts.write_root_targets()
    }

    /// Every typed connector scan this round must enumerate splits for.
    ///
    /// Preparation deliberately never calls a split manager: enumeration is
    /// lazy and belongs to the execution round, which owns the sources it
    /// opens and closes them when the round ends.
    pub(crate) fn typed_scans(&self) -> impl Iterator<Item = (FragmentId, i32)> + '_ {
        self.plan_facts
            .scans()
            .iter()
            .map(|scan| (scan.fragment_id, scan.plan_node_id))
    }

    /// Resolve one exact scan from the immutable logical execution.
    ///
    /// Attempt initialization uses this borrowed view only to clone the
    /// secret-free fields of one scan into a blocking-call recipe. The
    /// immutable artifact itself remains owned by the actor.
    /// The plan facts one exact scan was frozen with.
    ///
    /// Attempt initialization joins these to the capability it holds for that
    /// scan; the facts themselves carry no capability and no lease.
    pub(crate) fn scan_facts(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&crate::query_execution::attempt_plan_facts::AttemptScanFacts> {
        self.plan_facts.scan(fragment_id, node_id)
    }

    pub(crate) fn share_connector_attempt_access(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<Arc<crate::query_execution::preparation::ConnectorAttemptAccessEntry>> {
        self.attempt_access.share(fragment_id, node_id)
    }

    /// Project the exact application-owned Task manifest into the existing
    /// Native carrier encoder input. This is a one-way representation change:
    /// every placement, scan unit and exchange route is copied from the
    /// manifest, and no scheduler or live topology capability is consulted.
    pub(crate) fn native_schedule_from_manifest(
        &self,
        manifest: &TaskManifestBinding,
    ) -> Result<ValidatedFragmentSchedule, DistributedQueryError> {
        if self.handoff_id != manifest.request_bound_native().template.identity.handoff_id {
            return Err(contract_error(
                "Task manifest belongs to another prepared Native query",
            ));
        }

        let mut frozen_live_backends = BTreeMap::new();
        let mut backend_by_process = BTreeMap::new();
        for context in manifest.contexts() {
            let backend = context.backend();
            backend_by_process.insert(backend.process_id(), backend);
            match frozen_live_backends.insert(backend.backend_idx(), backend.target().clone()) {
                Some(previous) if previous != *backend.target() => {
                    return Err(contract_error(format!(
                        "Task manifest backend ordinal {} names conflicting frozen targets",
                        backend.backend_idx()
                    )));
                }
                _ => {}
            }
        }

        let scheduling = self.scheduling_facts();
        let mut by_fragment = BTreeMap::<FragmentId, Vec<FragmentInstancePlacement>>::new();
        let mut task_location = BTreeMap::new();
        for task in manifest.tasks() {
            let backend = backend_by_process
                .get(&task.identity().backend_process_id())
                .copied()
                .ok_or_else(|| {
                    contract_error(format!(
                        "Task manifest task {} has no frozen backend context",
                        task.identity()
                    ))
                })?;
            let mut scan_ranges = BTreeMap::new();
            for work in task.scan_work() {
                let node_id = work.scan().node_id();
                let ranges = scheduling
                    .scan_ranges(task.fragment_id(), node_id)
                    .ok_or_else(|| {
                        contract_error(format!(
                            "Task manifest scan node {node_id} has no frozen scan work in fragment {}",
                            task.fragment_id()
                        ))
                    })?;
                let assigned = match work.assignment() {
                    BoundManifestScanAssignment::RuntimeSplits
                    | BoundManifestScanAssignment::WholeRelation => Vec::new(),
                    BoundManifestScanAssignment::FrozenUnits(units) => {
                        let mut selected = Vec::with_capacity(units.len().get());
                        for offset in 0..units.len().get() {
                            let index = units
                                .stride()
                                .get()
                                .checked_mul(offset)
                                .and_then(|delta| units.first_ordinal().checked_add(delta))
                                .ok_or_else(|| {
                                    contract_error(format!(
                                        "Task manifest frozen scan assignment overflows for node {node_id}"
                                    ))
                                })?;
                            selected.push(ranges.get(index).cloned().ok_or_else(|| {
                                contract_error(format!(
                                    "Task manifest frozen scan assignment index {index} is outside node {node_id} work"
                                ))
                            })?);
                        }
                        selected
                    }
                };
                scan_ranges.insert(node_id, assigned);
            }
            let placements = by_fragment.entry(task.fragment_id()).or_default();
            if placements.len() != task.instance_index() {
                return Err(contract_error(format!(
                    "Task manifest fragment {} instance order is not contiguous at {}",
                    task.fragment_id(),
                    task.instance_index()
                )));
            }
            task_location.insert(task.identity(), (task.fragment_id(), task.instance_index()));
            placements.push(FragmentInstancePlacement {
                fragment_id: task.fragment_id(),
                instance_index: task.instance_index(),
                finst_id: task.fragment_instance_id(),
                backend_idx: backend.backend_idx(),
                endpoint: backend.endpoint().clone(),
                scan_ranges,
            });
        }

        let (root_fragment_id, root_instance_index) = task_location
            .get(&manifest.root())
            .copied()
            .ok_or_else(|| contract_error("Task manifest root has no placement"))?;
        let root_finst_id = by_fragment[&root_fragment_id][root_instance_index].finst_id;
        let root_backend_idx = by_fragment[&root_fragment_id][root_instance_index].backend_idx;
        Ok(ValidatedFragmentSchedule {
            handoff_id: self.handoff_id,
            execution_id: manifest.execution(),
            inner: SchedulingPlan {
                root_fragment_id,
                by_fragment,
                root_finst_id,
                root_backend_idx,
            },
            frozen_live_backends,
        })
    }

    /// Whether this attempt's native payload still needs its runtime-filter
    /// binding tables written into it.
    ///
    /// Completed fragments are encoded with their tables using the same
    /// binding numbering everything else joins on. A missing table is a
    /// contract error at the execution boundary.
    pub fn needs_runtime_filter_bindings(&self) -> bool {
        !self.native_bundle.carries_runtime_filter_bindings()
    }

    /// The payload already carries its binding tables, so this attempt moves
    /// on without attaching any.
    pub fn retain_encoded_runtime_filter_bindings(
        self,
    ) -> RuntimeFilterBoundPreparedDistributedQuery {
        RuntimeFilterBoundPreparedDistributedQuery {
            handoff_id: self.handoff_id,
            plan_facts: self.plan_facts,
            native_bundle: self.native_bundle,
            attempt_access: self.attempt_access,
        }
    }
}

/// A binding attachment can only be consumed once. It is the first RF-bound
/// distributed-query typestate and the only state that may bind a schedule.
pub struct RuntimeFilterBoundPreparedDistributedQuery {
    handoff_id: u64,
    /// Carried only so the native-submission encoder at the end of this
    /// chain can read it; nothing in between reads the plan through it.
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_bundle: Arc<NativeFragmentAttachment>,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl RuntimeFilterBoundPreparedDistributedQuery {
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
            handoff_id: self.handoff_id,
            plan_facts: self.plan_facts,
            native_bundle: self.native_bundle,
            schedule,
            attempt_access: self.attempt_access,
        })
    }
}

/// A core artifact bound to one validated schedule. Query initialization/control
/// readiness and the connector install/ACK barrier must first complete.
pub struct ScheduleBoundDistributedQuery {
    handoff_id: u64,
    /// Carried only so the native-submission encoder at the end of this
    /// chain can read it; nothing in between reads the plan through it.
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_bundle: Arc<NativeFragmentAttachment>,
    schedule: ValidatedFragmentSchedule,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl ScheduleBoundDistributedQuery {
    pub fn runtime_filter_scheduled_view(
        &self,
    ) -> Result<RuntimeFilterScheduledView<'_>, DistributedQueryError> {
        let frozen_live_backends = self
            .schedule
            .frozen_live_backends
            .values()
            .map(|target| {
                Ok(RuntimeFilterBackendTopologyEntry {
                    backend_idx: target.backend_idx(),
                    endpoint: target.endpoint().map_err(|error| {
                        contract_error(format!(
                            "runtime filter frozen backend {} has invalid endpoint: {error}",
                            target.backend_idx()
                        ))
                    })?,
                    process_id: target.process_id().map_err(|error| {
                        contract_error(format!(
                            "runtime filter frozen backend {} has invalid process identity: {error}",
                            target.backend_idx()
                        ))
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Ok(RuntimeFilterScheduledView {
            artifact_id: RuntimeFilterArtifactId(self.schedule.handoff_id),
            execution_id: self.schedule.execution_id,
            scheduled_backend_ids: self.schedule.backend_ids(),
            frozen_live_backend_ids: self.schedule.frozen_live_backend_ids(),
            frozen_live_backends,
            has_runtime_filter_channels: self.plan_facts.runtime_filters().has_channels(),
            deployment_facts: RuntimeFilterDeploymentFactsView::new(
                self.plan_facts.runtime_filters(),
                self.plan_facts.edges(),
                self.schedule.planning_schedule(),
            ),
            _private: std::marker::PhantomData,
        })
    }

    pub fn seal_runtime_filter_deployment(
        &self,
        contributions: impl IntoIterator<
            Item = (
                usize,
                novarocks_proto_models::novarocks::RuntimeFilterContribution,
            ),
        >,
    ) -> Result<RuntimeFilterDeploymentAttachment, DistributedQueryError> {
        self.runtime_filter_scheduled_view()?.seal(contributions)
    }

    pub fn attach_runtime_filter_deployment(
        self,
        attachment: RuntimeFilterDeploymentAttachment,
    ) -> Result<RuntimeFilterDeploymentReadyDistributedQuery, DistributedQueryError> {
        if !attachment.matches(
            RuntimeFilterArtifactId(self.schedule.handoff_id),
            self.schedule.execution_id,
        ) {
            return Err(contract_error(
                "runtime filter deployment attachment belongs to a different attempt schedule",
            ));
        }
        Ok(RuntimeFilterDeploymentReadyDistributedQuery {
            handoff_id: self.handoff_id,
            plan_facts: self.plan_facts,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            runtime_filter_contributions: attachment.contributions,
            attempt_access: self.attempt_access,
        })
    }
}

/// Frontend-only schedule/topology facts. It has no constructor and exposes no
/// mutable schedule, graph, or SQL planner type.
#[derive(Clone)]
pub struct RuntimeFilterScheduledView<'a> {
    artifact_id: RuntimeFilterArtifactId,
    execution_id: QueryExecutionId,
    scheduled_backend_ids: Vec<usize>,
    frozen_live_backend_ids: Vec<usize>,
    frozen_live_backends: Vec<RuntimeFilterBackendTopologyEntry>,
    has_runtime_filter_channels: bool,
    deployment_facts: RuntimeFilterDeploymentFactsView<'a>,
    _private: std::marker::PhantomData<&'a ()>,
}

impl<'a> RuntimeFilterScheduledView<'a> {
    pub fn artifact_id(self) -> RuntimeFilterArtifactId {
        self.artifact_id
    }

    pub fn execution_id(self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn query_id_wire(self) -> novarocks_proto_models::common::UniqueId {
        novarocks_proto_models::common::UniqueId {
            hi: self.execution_id.query_id().high(),
            lo: self.execution_id.query_id().low(),
        }
    }

    pub fn deployment_epoch(self) -> u64 {
        self.execution_id.attempt_id().get()
    }

    pub fn scheduled_backend_ids(self) -> impl ExactSizeIterator<Item = usize> + 'a {
        self.scheduled_backend_ids.into_iter()
    }

    pub fn frozen_live_backend_ids(self) -> impl ExactSizeIterator<Item = usize> + 'a {
        self.frozen_live_backend_ids.into_iter()
    }

    pub fn frozen_live_backends(
        self,
    ) -> impl ExactSizeIterator<Item = RuntimeFilterBackendTopologyEntry> + 'a {
        self.frozen_live_backends.into_iter()
    }

    /// A sealed graph-level fact only. It deliberately exposes neither the
    /// SQL graph nor a reconstruction handle; Frontend uses it to preserve
    /// the distinct empty-graph lifecycle contract.
    pub fn has_runtime_filter_channels(&self) -> bool {
        self.has_runtime_filter_channels
    }

    /// The deployment owner receives immutable planner and schedule facts,
    /// never a SQL graph, placement draft, or compiler handle.
    pub fn deployment_facts(&self) -> RuntimeFilterDeploymentFactsView<'a> {
        self.deployment_facts
    }

    pub fn seal(
        self,
        contributions: impl IntoIterator<
            Item = (
                usize,
                novarocks_proto_models::novarocks::RuntimeFilterContribution,
            ),
        >,
    ) -> Result<RuntimeFilterDeploymentAttachment, DistributedQueryError> {
        let mut by_backend = BTreeMap::new();
        for (backend_idx, contribution) in contributions {
            if by_backend.insert(backend_idx, contribution).is_some() {
                return Err(contract_error(format!(
                    "runtime filter deployment attachment repeats backend {backend_idx}"
                )));
            }
        }
        if !by_backend.is_empty() {
            let expected = self
                .frozen_live_backend_ids
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let actual = by_backend.keys().copied().collect::<BTreeSet<_>>();
            if actual != expected {
                let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
                let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();
                return Err(contract_error(format!(
                    "runtime filter deployment attachment backend set mismatch: missing={missing:?} unknown={unknown:?}"
                )));
            }
        }
        Ok(RuntimeFilterDeploymentAttachment {
            artifact_id: self.artifact_id,
            execution_id: self.execution_id,
            contributions: by_backend,
        })
    }
}

/// One backend of the frozen live topology, addressed by its round-local
/// scheduling ordinal. Runtime-filter deployment turns these into participants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterBackendTopologyEntry {
    backend_idx: usize,
    endpoint: RuntimeEndpoint,
    process_id: BackendProcessId,
}

impl RuntimeFilterBackendTopologyEntry {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub const fn process_id(&self) -> BackendProcessId {
        self.process_id
    }
}

/// Only this typestate represents a fully bound RF query. Its generic Init
/// entrypoint is intentionally introduced separately from the legacy
/// transition entrypoint while owner-local deployment compilation migrates.
pub struct RuntimeFilterDeploymentReadyDistributedQuery {
    handoff_id: u64,
    /// Carried only so the native-submission encoder at the end of this
    /// chain can read it; nothing in between reads the plan through it.
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_bundle: Arc<NativeFragmentAttachment>,
    schedule: ValidatedFragmentSchedule,
    runtime_filter_contributions:
        BTreeMap<usize, novarocks_proto_models::novarocks::RuntimeFilterContribution>,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl RuntimeFilterDeploymentReadyDistributedQuery {
    /// Freeze this attempt's shared facts for the task protocol.
    ///
    /// It performs the same admission the lifecycle path performs before it
    /// may encode a submission -- the execution-id check and the query-wide
    /// catalog freeze -- and deliberately stops there: on the task path a
    /// query context is established by the substrate itself, so there is no
    /// barrier to enter and no participant manifest to compile.
    pub fn prepare_task_execution(
        self,
        options: QueryInitOptions,
    ) -> Result<TaskExecutionPreparedQuery, DistributedQueryError> {
        if options.execution_id().query_id() != self.schedule.execution_id.query_id()
            || options.execution_id().attempt_id().get()
                != self.schedule.execution_id.attempt_id().get()
        {
            return Err(contract_error(
                "task execution preparation execution id does not match validated schedule",
            ));
        }
        let catalog_lease =
            freeze_query_catalog_lease(&self.attempt_access, options.catalog_set())?;
        let options = options.with_catalog_set(catalog_lease.catalog_set().clone());
        Ok(TaskExecutionPreparedQuery {
            handoff_id: self.handoff_id,
            plan_facts: self.plan_facts,
            native_bundle: self.native_bundle,
            schedule: self.schedule,
            options,
            catalog_lease,
            runtime_filter_contributions: self.runtime_filter_contributions,
            attempt_access: self.attempt_access,
        })
    }
}

/// One attempt's frozen inputs for the task protocol.
///
/// It exists so that the execution-id check, the query-wide catalog freeze and
/// the retained planning leases are done once here rather than duplicated at
/// the call site. A query context is established by the substrate itself, so
/// this value goes straight from frozen facts to task creation: there is no
/// barrier between the two for it to sit behind.
pub struct TaskExecutionPreparedQuery {
    handoff_id: u64,
    plan_facts: Arc<crate::query_execution::attempt_plan_facts::AttemptPlanFacts>,
    native_bundle: Arc<NativeFragmentAttachment>,
    schedule: ValidatedFragmentSchedule,
    options: QueryInitOptions,
    /// Held, never read: the FE control leases inside it are released when it
    /// drops, and this value is what keeps them alive for the whole attempt.
    /// A backend still resolving a catalog through one of them must not find
    /// its planning ownership already gone.
    #[expect(
        dead_code,
        reason = "the catalog planning leases are retained for the attempt's lifetime, not read"
    )]
    catalog_lease: QueryCatalogLease,
    runtime_filter_contributions: BTreeMap<usize, novarocks::RuntimeFilterContribution>,
    attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
}

impl TaskExecutionPreparedQuery {
    /// Stable placement identity facts for the owner-local native submission
    /// mapper, exactly as the lifecycle path's own typestate exposes them.
    pub fn native_submission_view(
        &self,
    ) -> Result<NativeSubmissionEncodingView<'_>, DistributedQueryError> {
        native_submission_encoding_view(
            self.handoff_id,
            self.schedule.execution_id,
            self.plan_facts.submission(),
            &self.native_bundle,
            &self.schedule.inner,
            self.options.native_submission_options(),
        )
    }

    /// The frozen placement result the task graph is built from.
    pub(crate) const fn scheduling_plan(&self) -> &SchedulingPlan {
        &self.schedule.inner
    }

    /// The exchange edges of this plan, in the planner's own order.
    pub(crate) fn fragment_edges(
        &self,
    ) -> &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts] {
        self.plan_facts.edges()
    }

    /// The query-wide catalog contribution every query context establishes.
    pub(crate) fn catalog_set(&self) -> &CatalogSet {
        self.options.catalog_set()
    }

    /// The runtime-filter contribution each scheduled backend establishes.
    ///
    /// Keyed by backend index because that is what the compiler produced; the
    /// caller translates to process identity through the same ownership map
    /// the graph is built with, so a contribution can never be established on
    /// a process the schedule did not name.
    pub(crate) const fn runtime_filter_contributions(
        &self,
    ) -> &BTreeMap<usize, novarocks::RuntimeFilterContribution> {
        &self.runtime_filter_contributions
    }

    pub(crate) const fn init_options(&self) -> &QueryInitOptions {
        &self.options
    }

    /// Takes this attempt's vended-storage capability, once.
    ///
    /// Called after the establish froze its own copy of the material: the
    /// table moves into the capability so the attempt keeps one owner of the
    /// secrets. A deployment that vends nothing has no capability to hand out
    /// and gets `None`, which is a different statement from a denied one.
    pub(crate) fn take_terminal_storage_resolver(
        &mut self,
    ) -> Option<Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>> {
        self.options
            .take_credential_leases()
            .into_attempt_storage_resolver()
    }

    /// Consume a matching native submission attachment.
    ///
    /// The attachment is validated against this exact handoff for the same
    /// reason the lifecycle path validates it: a submission set produced for
    /// another artifact would place tasks this schedule never admitted.
    pub fn seal_task_submission(
        &self,
        attachment: NativeSubmissionAttachment,
    ) -> Result<TaskExecutionSubmission, DistributedQueryError> {
        if !attachment.matches(self.handoff_id, self.schedule.execution_id) {
            return Err(contract_error(
                "native submission attachment does not belong to this task execution handoff",
            ));
        }
        let (submissions, root_fetch, expected_output) = attachment.into_parts();
        Ok(TaskExecutionSubmission {
            submissions,
            root_fetch,
            expected_output,
        })
    }
}

/// Everything the coordinator needs after the encoder has run.
///
/// The catalog lease deliberately does not travel with it: it stays on
/// [`TaskExecutionPreparedQuery`], which the coordinator keeps alive for the
/// whole attempt. Dropping the FE control leases while tasks are running would
/// release planning ownership of a catalog the backends are still reading
/// through.
pub struct TaskExecutionSubmission {
    submissions: Vec<ValidatedNativeSubmission>,
    root_fetch: RootFetchMetadata,
    expected_output: ExpectedOutputSchema,
}

impl TaskExecutionSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<ValidatedNativeSubmission>,
        RootFetchMetadata,
        ExpectedOutputSchema,
    ) {
        (self.submissions, self.root_fetch, self.expected_output)
    }
}

/// Freeze every catalog required by typed reads into the exact query-wide Init
/// contribution. `CatalogSet` is the sole BE materialization input: never
/// recover these values from the current FE control host after query assembly
/// has started.
/// Freeze every catalog-bound execution artifact into one exact query set and
/// retain the FE control leases that produced typed reads.  A write session
/// contributes its own catalog through the query's Init options, so only typed
/// reads are merged here.
fn freeze_query_catalog_lease(
    attempt_access: &crate::query_execution::preparation::ConnectorAttemptAccessPlan,
    existing: &CatalogSet,
) -> Result<QueryCatalogLease, DistributedQueryError> {
    let typed_reads = attempt_access
        .iter()
        .map(|(_, _, access)| {
            access
                .catalog_properties()
                .backend_execution_projection()
                .map_err(|error| {
                    contract_error(format!(
                        "project catalog generation for backend execution: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let catalog_set = merge_catalog_properties(existing, typed_reads)?;
    Ok(QueryCatalogLease::new(catalog_set, Vec::new()))
}

fn merge_catalog_properties(
    existing: &CatalogSet,
    additional: impl IntoIterator<Item = CatalogProperties>,
) -> Result<CatalogSet, DistributedQueryError> {
    let mut by_handle: BTreeMap<CatalogHandle, CatalogProperties> = existing
        .catalogs()
        .map_err(|error| contract_error(format!("invalid existing query catalog set: {error}")))?
        .into_iter()
        .map(|properties| (properties.handle().clone(), properties))
        .collect();
    for properties in additional {
        let handle = properties.handle().clone();
        match by_handle.get(&handle) {
            Some(existing) if existing != &properties => {
                return Err(contract_error(
                    "typed scans freeze conflicting materialization inputs for one catalog handle",
                ));
            }
            Some(_) => {}
            None => {
                by_handle.insert(handle, properties);
            }
        }
    }
    CatalogSet::new(by_handle.into_values())
        .map_err(|error| contract_error(format!("invalid query catalog set: {error}")))
}

/// A frontend decision for one instance. The native endpoint representation
/// remains core-private.
pub struct BackendPlacement {
    backend_idx: usize,
    endpoint: RuntimeEndpoint,
}

impl BackendPlacement {
    pub fn new(backend_idx: usize, endpoint: RuntimeEndpoint) -> Self {
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
        let mut process_ids = BTreeSet::new();
        for target in live_backends {
            let process_id = target.process_id().map_err(|error| {
                contract_error(format!(
                    "frontend schedule live backend {} has invalid process identity: {error}",
                    target.backend_idx()
                ))
            })?;
            let endpoint = target.endpoint().map_err(|error| {
                contract_error(format!(
                    "frontend schedule live backend {} has invalid endpoint: {error}",
                    target.backend_idx()
                ))
            })?;
            if !process_ids.insert(process_id) {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats backend process identity {process_id}"
                )));
            }
            if !endpoints.insert(endpoint.clone()) {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats endpoint {}",
                    endpoint
                )));
            }
            let backend_idx = target.backend_idx();
            if by_backend.insert(backend_idx, target).is_some() {
                return Err(contract_error(format!(
                    "frontend schedule live topology repeats backend {}",
                    backend_idx
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
    /// The live-backend topology this schedule was frozen against, keyed by
    /// the round-local scheduling ordinal. Runtime-filter deployment reads it
    /// to decide which backends a contribution set must cover.
    frozen_live_backends: BTreeMap<usize, LiveBackendTarget>,
}

impl ValidatedFragmentSchedule {
    /// Where each fragment's instances were admitted.
    ///
    /// Deliberately narrower than the whole plan: split assignment needs only
    /// the placements, and handing out the plan would let a caller re-derive
    /// routing decisions this schedule already froze.
    pub(crate) fn fragment_placements(
        &self,
    ) -> &BTreeMap<FragmentId, Vec<crate::query_execution::schedule::FragmentInstancePlacement>>
    {
        &self.inner.by_fragment
    }

    pub fn validate(
        facts: &crate::query_execution::fragment_scheduling::FragmentSchedulingFacts,
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
        let expected = facts.fragments.keys().copied().collect::<BTreeSet<_>>();
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
                    let frozen_endpoint = frozen.endpoint().map_err(|error| {
                        contract_error(format!(
                            "frontend schedule frozen backend {} has invalid endpoint: {error}",
                            placement.backend_idx
                        ))
                    })?;
                    if frozen_endpoint != placement.endpoint {
                        return Err(contract_error(format!(
                            "frontend schedule placement backend {} endpoint {} differs from frozen topology endpoint {}",
                            placement.backend_idx, placement.endpoint, frozen_endpoint
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
                        endpoint: placement.endpoint.clone(),
                        scan_ranges: BTreeMap::new(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let fragment = facts.fragment(fragment_id).ok_or_else(|| {
                contract_error(format!("prepared fragment {fragment_id} is missing"))
            })?;
            let instance_count = instances.len();
            for scan in &fragment.scans {
                let node_id = scan.node_id;
                for instance in &mut instances {
                    instance.scan_ranges.entry(node_id).or_default();
                }
                for (index, range) in scan.ranges.iter().enumerate() {
                    instances[index % instance_count]
                        .scan_ranges
                        .entry(node_id)
                        .or_default()
                        .push(range.clone());
                }
            }
            // A connector scan legitimately starts with no work: its splits
            // arrive at runtime, and a task that ends up with none still has to
            // be admitted so it can be told there are none. Only frozen scan
            // ranges can make an instance provably empty at planning time.
            let total_ranges = instances
                .iter()
                .flat_map(|instance| instance.scan_ranges.values())
                .map(Vec::len)
                .sum::<usize>();
            if total_ranges > 0
                && instances
                    .iter()
                    .any(|instance| instance.scan_ranges.values().all(Vec::is_empty))
            {
                return Err(contract_error(format!(
                    "frontend schedule fragment {fragment_id} creates an empty scan instance"
                )));
            }
            by_fragment.insert(fragment_id, instances);
        }

        let root_fragment_id = facts.anchor;
        let root = by_fragment
            .get(&root_fragment_id)
            .and_then(|placements| placements.first())
            .ok_or_else(|| contract_error("frontend schedule root has no placement"))?;
        let root_finst_id = root.finst_id;
        let root_backend_idx = root.backend_idx;
        let inner = SchedulingPlan {
            root_fragment_id,
            by_fragment,
            root_finst_id,
            root_backend_idx,
        };
        Ok(Self {
            handoff_id: facts.handoff_id,
            execution_id,
            inner,
            frozen_live_backends,
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

    pub(crate) fn frozen_live_backend_ids(&self) -> Vec<usize> {
        self.frozen_live_backends.keys().copied().collect()
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
    Ok(UniqueId::new(hi, lo))
}

#[cfg(test)]
pub fn fragment_instance_id_for_contract_test(
    query_id: QueryId,
    fragment_id: FragmentId,
    instance_index: usize,
) -> UniqueId {
    let execution_id = QueryExecutionId::new(
        query_id,
        novarocks_proto_codec::lifecycle::AttemptId::new(1)
            .expect("contract fixtures use a nonzero initial attempt"),
    )
    .expect("contract fixtures use a nonzero query id");
    derive_fragment_instance_id(execution_id, fragment_id, instance_index)
        .expect("contract fixture fragment identity is representable")
}

/// The task-local creation facts of one placement, before its task is bound
/// to exact exchange edges.
///
/// These are the only per-instance facts the frontend decides at placement:
/// the instance's position in its fragment, the task width its fragment was
/// given, and the scan ranges frozen into it. Identity, kernel key and
/// topology come from the task graph, and query-wide options from the
/// established context, so none of them is repeated here.
#[derive(Clone, Debug)]
pub(crate) struct NativePlacementAssignment {
    instance_ordinal: u32,
    pipeline_dop: std::num::NonZeroUsize,
    initial_scan_ranges: BTreeMap<i32, Vec<novarocks_proto_codec::lifecycle::ScanRangeParams>>,
}

impl NativePlacementAssignment {
    pub(crate) fn new(
        instance_ordinal: u32,
        pipeline_dop: std::num::NonZeroUsize,
        initial_scan_ranges: BTreeMap<i32, Vec<novarocks_proto_codec::lifecycle::ScanRangeParams>>,
    ) -> Self {
        Self {
            instance_ordinal,
            pipeline_dop,
            initial_scan_ranges,
        }
    }

    /// This instance's position among its fragment's instances.
    pub(crate) const fn instance_ordinal(&self) -> u32 {
        self.instance_ordinal
    }

    /// The task width this instance's fragment was given.
    pub(crate) const fn pipeline_dop(&self) -> std::num::NonZeroUsize {
        self.pipeline_dop
    }

    /// Moves the scan ranges frozen into this instance out, per scan plan
    /// node, so the creation seed that takes them is their only holder.
    ///
    /// A node served by runtime split delivery is present with no ranges:
    /// the entry is how the instance says the node is its, and the ranges
    /// arrive later as a separate domain.
    pub(crate) fn into_initial_scan_ranges(
        self,
    ) -> BTreeMap<i32, Vec<novarocks_proto_codec::lifecycle::ScanRangeParams>> {
        self.initial_scan_ranges
    }
}

pub struct ValidatedNativeSubmission {
    backend_idx: usize,
    finst_id: UniqueId,
    execution_id: QueryExecutionId,
    fragment: Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
    assignment: NativePlacementAssignment,
}

impl ValidatedNativeSubmission {
    pub(crate) fn new(
        backend_idx: usize,
        fragment_instance_id: UniqueId,
        execution_id: QueryExecutionId,
        fragment: Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
        assignment: NativePlacementAssignment,
    ) -> Self {
        Self {
            backend_idx,
            finst_id: fragment_instance_id,
            execution_id,
            fragment,
            assignment,
        }
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.finst_id
    }

    pub fn fragment_id(&self) -> FragmentId {
        self.fragment.facts().fragment_id()
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    /// Whether this instance's plan contains a connector table writer.
    ///
    /// A distributed write's writer set is what decides whether the write
    /// completed, and "the query is a write" says nothing about which of its
    /// fragments actually write. The frozen fragment read it off its own plan.
    pub(crate) fn declares_table_writer(&self) -> bool {
        self.fragment.facts().declares_table_writer()
    }

    /// The plan-node id of this fragment's root (output) node, which EXPLAIN
    /// ANALYZE keys a fragment by.
    pub(crate) fn fragment_root_plan_node_id(&self) -> Result<i32, String> {
        Ok(self.fragment.facts().root_plan_node_id())
    }

    /// Hands the shared frozen fragment and this instance's own assignment to
    /// the task protocol.
    ///
    /// Every placement of one fragment holds the same frozen fragment. The
    /// task graph binds the instance to its exact edges later, and only then
    /// can its creation metadata exist.
    pub(crate) fn into_creation_parts(
        self,
    ) -> (
        Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
        NativePlacementAssignment,
    ) {
        (self.fragment, self.assignment)
    }
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct ExpectedOutputSchema {
    output_columns: Vec<PlanOutputColumn>,
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
                .map(|column| {
                    QueryResultColumn::new(column.name, column.data_type, column.nullable, None)
                })
                .collect(),
            batches: chunks.into_iter().map(|chunk| chunk.batch).collect(),
        })
    }
}

fn native_submission_encoding_view<'a>(
    handoff_id: u64,
    execution_id: QueryExecutionId,
    plan: &'a crate::query_execution::artifact::native_submission::SubmissionPlanFacts,
    native_bundle: &'a NativeFragmentAttachment,
    schedule: &'a SchedulingPlan,
    options: &'a novarocks_execution::runtime::query_options::QueryOptions,
) -> Result<NativeSubmissionEncodingView<'a>, DistributedQueryError> {
    crate::query_execution::assembly::validate_artifact_fragment_sets(
        &plan.fragment_ids(),
        native_bundle,
        schedule,
    )
    .map_err(contract_error)?;
    crate::query_execution::assembly::validate_scheduling_placements(schedule)
        .map_err(contract_error)?;
    let keys = schedule
        .by_fragment
        .iter()
        .flat_map(|(&fragment_id, placements)| {
            placements.iter().map(move |placement| {
                NativeSubmissionKey::new(placement.backend_idx, fragment_id, placement.finst_id)
            })
        })
        .collect::<Vec<_>>();
    let root = NativeSubmissionKey::new(
        schedule.root_backend_idx,
        schedule.root_fragment_id,
        schedule.root_finst_id,
    );
    let plan_root = plan.fragment(schedule.root_fragment_id).ok_or_else(|| {
        contract_error(format!(
            "scheduled execution root fragment {} is absent from the plan",
            schedule.root_fragment_id
        ))
    })?;
    let root_fetch = RootFetchMetadata {
        fragment_id: schedule.root_fragment_id,
        backend_idx: schedule.root_backend_idx,
        finst_id: schedule.root_finst_id,
        uses_result_buffer: plan_root.role().uses_result_buffer(),
    };
    let expected_output = build_expected_output_schema(plan_root.output_columns())?;
    NativeSubmissionEncodingView::new(
        handoff_id,
        execution_id,
        keys,
        root,
        plan.clone(),
        native_bundle,
        schedule,
        options,
        root_fetch,
        expected_output,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_expected_output_schema(
    output_columns: &[PlanOutputColumn],
) -> Result<ExpectedOutputSchema, DistributedQueryError> {
    let output_columns = output_columns.to_vec();
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId, ConnectorSplit,
    };

    use super::{
        PreparedDistributedAttemptAffinity, PreparedDistributedTemplateIdentity,
        RuntimeFilterArtifactId, RuntimeFilterDeploymentAttachment, derive_fragment_instance_id,
        merge_catalog_properties, validate_bound_attempt_identity, validate_native_request_match,
        validate_prepared_template_affinity,
    };
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_types::QueryId;

    fn catalog_properties(name: &str, version: u8, warehouse: &str) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse(name).expect("valid catalog name"),
                CatalogVersion::from_bytes([version; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![CatalogProperty::new("warehouse", warehouse).expect("valid warehouse")],
            Vec::new(),
        )
        .expect("valid catalog properties")
    }

    #[test]
    fn attempt_template_affinity_rejects_cross_assembly_splice() {
        let plan_seal = novarocks_query_application::api::PlanSeal::Version(
            novarocks_physical_plan::PlanVersionId::try_new([41; 16]).expect("plan version"),
        );
        let native_identity = PreparedDistributedTemplateIdentity::new(41, plan_seal);
        let exact_access_identity = native_identity.clone();
        let foreign_access_identity = PreparedDistributedTemplateIdentity::new(41, plan_seal);

        validate_prepared_template_affinity(&native_identity, &exact_access_identity)
            .expect("siblings minted together retain the same private token");
        let error = validate_prepared_template_affinity(&native_identity, &foreign_access_identity)
            .expect_err("equal scalar identities from separate assemblies must be rejected");
        assert!(error.message().contains("another prepared Native template"));
    }

    #[test]
    fn request_binding_rejects_a_foreign_plan_seal_match() {
        validate_native_request_match(true).expect("an exact request seal match is accepted");
        let error = validate_native_request_match(false)
            .expect_err("a foreign request seal must not produce a bound typestate");
        assert!(error.message().contains("another sealed preparation plan"));
    }

    #[test]
    fn attempt_access_rejects_a_cross_attempt_native_splice() {
        let query_id = QueryId::new(43, 79);
        let first_execution =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("valid first attempt"))
                .expect("valid first execution");
        let second_execution =
            QueryExecutionId::new(query_id, AttemptId::new(2).expect("valid second attempt"))
                .expect("valid second execution");
        let plan_seal = novarocks_query_application::api::PlanSeal::Version(
            novarocks_physical_plan::PlanVersionId::try_new([47; 16]).expect("plan version"),
        );
        let template = PreparedDistributedTemplateIdentity::new(47, plan_seal);
        let exact_template = template.clone();
        let affinity = Arc::new(PreparedDistributedAttemptAffinity);
        let exact_affinity = Arc::clone(&affinity);
        let foreign_affinity = Arc::new(PreparedDistributedAttemptAffinity);

        validate_bound_attempt_identity(
            first_execution,
            &template,
            &affinity,
            first_execution,
            &exact_template,
            &exact_affinity,
        )
        .expect("siblings minted for one request bind are accepted");
        let error = validate_bound_attempt_identity(
            first_execution,
            &template,
            &affinity,
            second_execution,
            &exact_template,
            &foreign_affinity,
        )
        .expect_err("access from another attempt must not splice onto the Native owner");
        assert!(
            error
                .message()
                .contains("another request-bound Native attempt")
        );
    }

    fn connector_split(split_id: &str, estimated_bytes: Option<u64>) -> ConnectorSplit {
        ConnectorSplit::try_new(
            ConnectorInstanceId::parse("placement-test").expect("valid instance"),
            split_id,
            Bytes::new(),
            estimated_bytes,
        )
        .expect("valid split")
    }

    #[test]
    fn catalog_set_merges_exact_typed_read_materializations_once() {
        let existing = CatalogSet::new([catalog_properties("catalog.alpha", 1, "s3://alpha")])
            .expect("valid initial catalog set");
        let merged = merge_catalog_properties(
            &existing,
            [
                catalog_properties("catalog.alpha", 1, "s3://alpha"),
                catalog_properties("catalog.beta", 2, "s3://beta"),
            ],
        )
        .expect("exact duplicate typed read materialization is idempotent");
        let catalogs = merged.catalogs().expect("valid merged catalog set");
        assert_eq!(catalogs.len(), 2);
        assert_eq!(
            catalogs[0].handle().catalog_name().as_str(),
            "catalog.alpha"
        );
        assert_eq!(catalogs[1].handle().catalog_name().as_str(), "catalog.beta");
    }

    #[test]
    fn catalog_set_rejects_conflicting_typed_read_materialization() {
        let existing = CatalogSet::new([catalog_properties("catalog.alpha", 1, "s3://alpha")])
            .expect("valid initial catalog set");
        let error = merge_catalog_properties(
            &existing,
            [catalog_properties("catalog.alpha", 1, "s3://other-alpha")],
        )
        .expect_err("one catalog handle cannot name two materialization inputs");
        assert!(
            error
                .message()
                .contains("conflicting materialization inputs")
        );
    }

    fn placed_split_ids(placements: Vec<Vec<ConnectorSplit>>) -> Vec<Vec<String>> {
        placements
            .into_iter()
            .map(|splits| {
                splits
                    .into_iter()
                    .map(|split| split.split_id().to_owned())
                    .collect()
            })
            .collect()
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
    fn runtime_filter_deployment_attachment_is_bound_to_one_attempt() {
        let query_id = QueryId::new(41, 74);
        let first_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("valid attempt"))
                .expect("valid execution id");
        let second_attempt =
            QueryExecutionId::new(query_id, AttemptId::new(2).expect("valid attempt"))
                .expect("valid execution id");
        let artifact_id = RuntimeFilterArtifactId(17);
        let attachment = RuntimeFilterDeploymentAttachment {
            artifact_id,
            execution_id: first_attempt,
            contributions: BTreeMap::new(),
        };

        assert!(attachment.matches(artifact_id, first_attempt));
        assert!(!attachment.matches(artifact_id, second_attempt));
        assert!(!attachment.matches(RuntimeFilterArtifactId(18), first_attempt));
    }
}
