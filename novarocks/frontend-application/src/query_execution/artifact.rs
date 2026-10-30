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
    BoundManifestScanWork, BoundManifestTask, BoundWriterCohort, FrozenAttemptTopology,
    TaskManifestBinding,
};

pub use native_submission::{
    NativeSubmissionAttachment, NativeSubmissionEncodingView, NativeSubmissionFragmentRole,
    NativeSubmissionKey,
};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_spi::connector::{CatalogHandle, CatalogProperties};
use sha2::{Digest, Sha256};

use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::{QueryCatalogLease, QueryInitOptions};
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::preparation::runtime_filter_view::RuntimeFilterDeploymentFactsView;
use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};
#[cfg(test)]
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_proto_models::novarocks;
use novarocks_query_application::api::{BackendTopologySnapshot, LiveBackendTarget};
#[cfg(test)]
use novarocks_query_application::api::{QueryResult, ResultField as QueryResultColumn};
#[cfg(test)]
use novarocks_types::QueryId;
#[cfg(test)]
use novarocks_types::SlotId;
use novarocks_types::{BackendProcessId, UniqueId};

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

#[cfg(test)]
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
        Ok(project_static_native_attempt(
            &native.template,
            &self.attempt_access,
        ))
    }
}

/// Project the original static fragments after the caller has checked the
/// applicable template or request-bound affinity. This pure projection creates
/// no request ticket, execution identity, manifest or activation authority.
fn project_static_native_attempt(
    native: &PreparedDistributedNativeTemplate,
    attempt_access: &Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
) -> PreparedDistributedQuery {
    PreparedDistributedQuery {
        handoff_id: native.identity.handoff_id,
        plan_facts: Arc::clone(&native.plan_facts),
        native_bundle: Arc::clone(&native.native_template),
        attempt_access: Arc::clone(attempt_access),
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
    pub(crate) const fn manifest(&self) -> &TaskManifestBinding {
        &self.manifest
    }

    pub(crate) fn eligible_backends(&self) -> &[BackendProcessId] {
        self.manifest.eligible_backends()
    }

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
    /// The encoding produces the native fragments and scheduling facts
    /// together; the scheduling facts carry this template's handoff identity,
    /// and the single completed-plan owner moves both products here.
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

    /// Exercise only static-fragment sharing at the encoding unit boundary.
    /// Production attempts still require the exact QA request and manifest;
    /// this fixture does not mint or bypass either of those authorities.
    #[cfg(test)]
    pub(crate) fn instantiate_static_fragment_fixture(&self) -> PreparedDistributedQuery {
        validate_prepared_template_affinity(&self.native.identity, &self.access.identity)
            .expect("one encoded template retains its exact sibling affinity");
        project_static_native_attempt(&self.native, &self.access.attempt_access)
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
                    BoundManifestScanAssignment::RuntimeSplits => Vec::new(),
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
                #[cfg(test)]
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
            _catalog_lease: catalog_lease,
            runtime_filter_contributions: self.runtime_filter_contributions,
            _attempt_access: self.attempt_access,
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
    _catalog_lease: QueryCatalogLease,
    runtime_filter_contributions: BTreeMap<usize, novarocks::RuntimeFilterContribution>,
    _attempt_access: Arc<crate::query_execution::preparation::ConnectorAttemptAccessPlan>,
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

    /// Retain the original frozen table, including sealed-empty identity,
    /// after Establish has borrowed it. Internal handoff checks its author.
    pub(crate) fn take_terminal_storage(
        &mut self,
    ) -> Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage> {
        self.options.take_credential_leases().into_storage()
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
        let (submissions, root_fetch) = attachment.into_parts();
        Ok(TaskExecutionSubmission {
            submissions,
            root_fetch,
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
}

impl TaskExecutionSubmission {
    pub(crate) fn into_parts(self) -> (Vec<ValidatedNativeSubmission>, RootFetchMetadata) {
        (self.submissions, self.root_fetch)
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

/// Add the original sealed writer's BE materialization input without resolving
/// a current catalog or acquiring another generation. Typed reads merge through
/// this same conflict-checking author during task preparation.
pub(crate) fn merge_write_session_catalog(
    existing: &CatalogSet,
    session: &crate::query_execution::write_session::ConnectorWriteSession,
) -> Result<CatalogSet, DistributedQueryError> {
    let writer = session
        .catalog_properties()
        .backend_execution_projection()
        .map_err(|error| {
            contract_error(format!(
                "project write catalog for backend execution: {error}"
            ))
        })?;
    merge_catalog_properties(existing, [writer])
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

#[cfg(test)]
#[derive(Clone)]
struct ExpectedOutputSchema {
    output_columns: Vec<PlanOutputColumn>,
    chunk_schema: ChunkSchemaRef,
}

#[cfg(test)]
impl ExpectedOutputSchema {
    #[cfg(test)]
    pub fn into_query_result(
        self,
        chunks: Vec<novarocks_execution::exec::chunk::Chunk>,
    ) -> Result<QueryResult, DistributedQueryError> {
        let chunks = crate::query_execution::assembly::align_fetch_chunks_to_output_columns(
            chunks,
            &self.output_columns,
        )
        .map_err(contract_error)?;
        Ok(QueryResult {
            columns: self
                .output_columns
                .into_iter()
                .map(|column| Ok(QueryResultColumn::new(column.name, column.ty)))
                .collect::<Result<Vec<_>, DistributedQueryError>>()?,
            batches: chunks
                .into_iter()
                .map(|chunk| chunk.record_batch().clone())
                .collect(),
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
    validate_root_output_columns(plan_root.output_columns())?;
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
    )
}

fn validate_root_output_columns(outputs: &[PlanOutputColumn]) -> Result<(), DistributedQueryError> {
    u32::try_from(outputs.len()).map_err(|_| contract_error("too many root output columns"))?;
    Ok(())
}

#[cfg(test)]
fn build_expected_output_schema(
    output_columns: &[PlanOutputColumn],
) -> Result<ExpectedOutputSchema, DistributedQueryError> {
    validate_root_output_columns(output_columns)?;
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
                ChunkSlotSchema::try_new_typed(
                    slot,
                    Arc::new(arrow::datatypes::Field::new(
                        &output.name,
                        output.data_type().clone(),
                        output.nullable(),
                    )),
                    output.ty.typed_binding().clone(),
                    None,
                    novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                )
                .map_err(|source| contract_error(source.to_string()))
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Arc::new(
            ChunkSchema::try_new_typed(
                slots,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
            .map_err(|source| contract_error(source.to_string()))?,
        )
    };
    Ok(ExpectedOutputSchema {
        output_columns,
        chunk_schema,
    })
}

#[cfg(test)]
mod tests {
    // Exercise the installed BE receive boundary with the fixture's complete
    // frozen declaration. Returned chunks retain finite creation responsibility.
    fn test_receive_root_chunks(
        payload: &[u8],
        schema: &novarocks_execution::exec::chunk::ChunkSchemaRef,
    ) -> Result<Vec<novarocks_execution::exec::chunk::Chunk>, String> {
        use novarocks_execution::runtime::exchange::{
            ExchangeColumnBinding, ExchangeKey, ExchangeSenderIdentity, ExecutionExchangeRegistry,
        };
        let registry = ExecutionExchangeRegistry::default();
        let key = ExchangeKey {
            finst_id_hi: 47,
            finst_id_lo: 83,
            node_id: 1,
        };
        let factory = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().map_err(|e|e.to_string())?,
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("root consumer fixture"),
            std::sync::Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
        ).map_err(|e|e.to_string())?;
        registry
            .try_register_expected_chunk_schema_with_binding(
                key,
                1,
                schema.clone(),
                ExchangeColumnBinding::Positional,
                factory,
            )
            .map_err(|source| source.to_string())?;
        registry
            .decode_chunks_for_sender(key, ExchangeSenderIdentity::local(1, 1), payload)
            .map_err(|e| e.to_string())
    }

    /// These tiny one-column fixtures exercise the production bounded encoder
    /// after IPC projection, with the exact domain supplied by the test plan.
    fn render_test_cells(
        result: &novarocks_query_application::api::QueryResult,
        domain: novarocks_physical_plan::ResultValueDomain,
    ) -> Vec<Option<Vec<u8>>> {
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_result_contract::{
            ClientRenderSchema, NativeRenderType as N, OpaqueRenderType as O, RenderColumn,
            RenderField, RenderPresentation as P,
        };
        use novarocks_result_render::{
            BoundedClientRowEncoder, ClientRowEncoder, RenderTurnStatus,
        };
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.batches.len(), 1);
        let field = &result.columns[0];
        let (native_type, presentation) = match domain {
            D::Hll => (N::Opaque(O::Hll), P::OpaqueNull),
            D::Bitmap => (N::Opaque(O::Bitmap), P::OpaqueNull),
            D::Object => (N::Opaque(O::Object), P::OpaqueNull),
            D::Percentile => (N::Opaque(O::Percentile), P::OpaqueNull),
            D::Variant => (N::Variant, P::VariantSerializedBytes),
            D::Json => (N::Json, P::JsonText),
            D::Plain => match field.data_type() {
                arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                    (N::Binary, P::ScalarText)
                }
                _ => (N::String, P::ScalarText),
            },
        };
        let schema = ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name: field.name().into(),
                field: RenderField {
                    native_type,
                    presentation,
                    nullable: field.nullable(),
                },
            }],
            1,
        )
        .unwrap();
        let mut encoder =
            ClientRowEncoder::try_new(Arc::new(schema), result.batches[0].clone()).unwrap();
        let mut body = Vec::new();
        for _ in 0..1024 {
            let mut segment = [0; 4096];
            let turn = encoder.step(&mut segment).unwrap();
            body.extend_from_slice(&segment[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        let mut cells = Vec::new();
        let mut at = 0;
        while at < body.len() {
            let length = u32::from_le_bytes(body[at..at + 4].try_into().unwrap()) as usize;
            at += 4;
            let payload = &body[at..at + length];
            at += length;
            cells.push(if payload == [0xfb] {
                None
            } else {
                assert!(payload[0] < 0xfb, "fixture requires a short scalar cell");
                assert_eq!(payload.len(), usize::from(payload[0]) + 1);
                Some(payload[1..].to_vec())
            });
        }
        assert_eq!(cells.len(), result.row_count());
        cells
    }

    fn test_decode_root_chunk(
        payload: &[u8],
        schema: &novarocks_execution::exec::chunk::ChunkSchemaRef,
    ) -> Result<novarocks_execution::exec::chunk::Chunk, String> {
        let mut chunks = test_receive_root_chunks(payload, schema)?;
        if chunks.len() != 1 {
            return Err("test payload must contain exactly one chunk".into());
        }
        Ok(chunks.remove(0))
    }
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion, ConnectorInstanceId,
        ConnectorProviderId,
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

    #[test]
    fn original_writer_catalog_survives_without_reads_and_merges_exact_read_once() {
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationFinalization,
            ExternalMutationOutcome,
        };
        let fixture = crate::query_execution::write_session::tests::fixture_with_outcome(
            1,
            8,
            ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "catalog projection fixture does not publish",
                ),
                cleanup: ExternalMutationFinalization::Complete,
            },
        );
        let original = fixture.session.catalog_properties();
        let expected = original.backend_execution_projection().unwrap();
        let no_reads = CatalogSet::new([]).unwrap();
        let writer = super::merge_write_session_catalog(&no_reads, &fixture.session).unwrap();
        let catalogs = writer.catalogs().unwrap();
        assert_eq!(catalogs, vec![expected.clone()]);
        assert_eq!(catalogs[0].handle(), original.handle());
        let round_trip = CatalogSet::parse(writer.as_proto().clone()).unwrap();
        assert_eq!(round_trip.catalogs().unwrap(), catalogs);
        let read_first = CatalogSet::new([expected.clone()]).unwrap();
        let exact_writer =
            super::merge_write_session_catalog(&read_first, &fixture.session).unwrap();
        assert_eq!(exact_writer.catalogs().unwrap(), vec![expected.clone()]);
        // The same merge used by freeze_query_catalog_lease adds typed reads.
        let both = merge_catalog_properties(&writer, [expected.clone()]).unwrap();
        assert_eq!(both.catalogs().unwrap(), vec![expected.clone()]);
        let conflicting = CatalogProperties::new(
            expected.handle().clone(),
            expected.provider_id().clone(),
            expected.config_format_version(),
            vec![CatalogProperty::new("warehouse", "s3://conflicting-generation-input").unwrap()],
            expected.credential_bindings().to_vec(),
        )
        .unwrap();
        assert!(
            merge_catalog_properties(&writer, [conflicting])
                .unwrap_err()
                .message()
                .contains("conflicting materialization inputs")
        );
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

    fn typed_result_chunk(
        array: arrow::array::ArrayRef,
        ty: &novarocks_type_contract::FunctionValueType,
    ) -> novarocks_execution::exec::chunk::Chunk {
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
        use novarocks_type_contract::ValueTypeStorageLifetime as S;
        let field = Arc::new(Field::new("wire", array.data_type().clone(), ty.nullable()));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![array]).unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            novarocks_types::SlotId::new(73),
            field,
            ty.typed_binding().clone(),
            None,
            S::pure(),
        )
        .unwrap();
        Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()))
            .unwrap()
    }

    fn assert_shared_payload(before: &arrow::array::ArrayData, after: &arrow::array::ArrayData) {
        assert_eq!(before.len(), after.len());
        assert_eq!(before.buffers().len(), after.buffers().len());
        for (before, after) in before.buffers().iter().zip(after.buffers()) {
            assert!(before.ptr_eq(after));
        }
        if let (Some(before), Some(after)) = (before.nulls(), after.nulls()) {
            assert!(before.buffer().ptr_eq(after.buffer()));
        }
        assert_eq!(before.child_data().len(), after.child_data().len());
        for (before, after) in before.child_data().iter().zip(after.child_data()) {
            assert_shared_payload(before, after);
        }
    }

    #[test]
    fn completed_scalar_bindings_survive_ipc_alias_and_client_metadata_projection() {
        use crate::query_execution::attempt_plan_facts::{PlanOutputColumn, declared_test_value};
        use arrow::array::{ArrayRef, BinaryArray, LargeBinaryArray, StringArray};
        use arrow::datatypes::DataType;
        use novarocks_physical_plan::ResultValueDomain as D;
        for (domain, carrier) in [
            (D::Hll, DataType::Binary),
            (D::Bitmap, DataType::Binary),
            (D::Object, DataType::Binary),
            (D::Percentile, DataType::Binary),
            (D::Variant, DataType::LargeBinary),
            (D::Json, DataType::Utf8),
            (D::Plain, DataType::Binary),
            (D::Plain, DataType::Utf8),
        ] {
            // JSON presentation validates JSON bytes; the frozen domain does
            // not turn an arbitrary UTF8 fixture into a valid JSON document.
            let bytes: &[u8] = if domain == D::Json {
                b"\"nonempty\""
            } else {
                b"nonempty"
            };
            let array: ArrayRef = match carrier {
                DataType::Binary => Arc::new(BinaryArray::from(vec![bytes])),
                DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![bytes])),
                DataType::Utf8 => {
                    Arc::new(StringArray::from(vec![std::str::from_utf8(bytes).unwrap()]))
                }
                _ => unreachable!(),
            };
            let ty = declared_test_value(carrier, false, domain).unwrap();
            let chunk = typed_result_chunk(array, &ty);
            let payload =
                novarocks_execution::runtime::exchange::encode_chunks(&[chunk], true).unwrap();
            let output = PlanOutputColumn {
                name: "declared_value".into(),
                ty: ty.clone(),
            };
            let expected = super::build_expected_output_schema(&[output]).unwrap();
            let decoded = test_decode_root_chunk(&payload, &expected.chunk_schema).unwrap();
            let result = expected.into_query_result(vec![decoded]).unwrap();
            assert_eq!(result.columns[0].value_type(), &ty);
            assert_eq!(result.columns[0].name(), "declared_value");
            assert!(result.batches[0].schema().field(0).metadata().is_empty());
            let rows = render_test_cells(&result, domain);
            if matches!(domain, D::Hll | D::Bitmap | D::Object | D::Percentile) {
                assert_eq!(rows[0], None);
            } else {
                assert_eq!(rows[0].as_deref(), Some(bytes));
            }
        }
    }

    #[test]
    fn completed_nominal_author_refuses_an_incompatible_carrier() {
        use arrow::datatypes::DataType;
        use novarocks_physical_plan::ResultValueDomain as D;
        for domain in [
            D::Json,
            D::Variant,
            D::Hll,
            D::Bitmap,
            D::Object,
            D::Percentile,
        ] {
            assert!(
                crate::query_execution::attempt_plan_facts::declared_test_value(
                    DataType::Int32,
                    false,
                    domain
                )
                .is_err()
            );
        }
    }

    #[test]
    fn string_dictionary_ipc_preserves_selected_nulls_payload_and_full_binding() {
        use crate::query_execution::attempt_plan_facts::{PlanOutputColumn, declared_test_value};
        use arrow::array::{ArrayRef, DictionaryArray, Int32Array, LargeStringArray, StringArray};
        use arrow::datatypes::{DataType, Int32Type};
        use novarocks_physical_plan::ResultValueDomain as D;
        for large in [false, true] {
            for domain in [D::Plain, D::Json] {
                // JSON's installed declaration is UTF8; a LargeUTF8 source has
                // its own exact layout rather than borrowing that declaration.
                if large && domain == D::Json {
                    continue;
                }
                let (a, b) = if domain == D::Json {
                    ("{}", "[]")
                } else {
                    ("a", "b")
                };
                let values: ArrayRef = if large {
                    Arc::new(LargeStringArray::from(vec![Some(a), None, Some(b)]))
                } else {
                    Arc::new(StringArray::from(vec![Some(a), None, Some(b)]))
                };
                let array: ArrayRef = Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![Some(2), None, Some(1), Some(0)]),
                        values,
                    )
                    .unwrap(),
                );
                let ty = declared_test_value(
                    if large {
                        DataType::LargeUtf8
                    } else {
                        DataType::Utf8
                    },
                    true,
                    domain,
                )
                .unwrap();
                let chunk = typed_result_chunk(array, &ty);
                let payload =
                    novarocks_execution::runtime::exchange::encode_chunks(&[chunk], true).unwrap();
                let expected = super::build_expected_output_schema(&[PlanOutputColumn {
                    name: "declared".into(),
                    ty: ty.clone(),
                }])
                .unwrap();
                let decoded = test_decode_root_chunk(&payload, &expected.chunk_schema).unwrap();
                let original = decoded.record_batch().column(0).clone();
                let result = expected.into_query_result(vec![decoded]).unwrap();
                assert_eq!(result.columns[0].value_type(), &ty);
                assert_shared_payload(&original.to_data(), &result.batches[0].column(0).to_data());
                assert_eq!(
                    render_test_cells(&result, domain),
                    vec![
                        Some(b.as_bytes().to_vec()),
                        None,
                        None,
                        Some(a.as_bytes().to_vec())
                    ]
                );
            }
        }
    }

    mod complete_nested_outputs {
        use super::*;
        use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
        use arrow::array::{
            Array, ArrayRef, BinaryArray, ListArray, MapArray, StringArray, StructArray,
        };
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::{DataType, Field};
        use novarocks_type_contract::{
            FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType as L,
            ValueTypeRef, ValueTypeStorageLifetime as S,
        };

        fn source(
            shape: usize,
            semantic: L,
            child_nullable: bool,
            attribute: Option<&str>,
        ) -> (ArrayRef, FunctionValueType) {
            let child = if semantic == L::Physical {
                ValueKind::Binary
            } else {
                ValueKind::Nominal(semantic)
            };
            let child = ValueTypeRef::try_new(child, child_nullable, S::pure()).unwrap();
            let mut field = Field::new("value", DataType::Binary, child_nullable);
            if let Some(attribute) = attribute {
                field = field.with_metadata(std::collections::HashMap::<String, String>::from([(
                    "nr_logical_type".into(),
                    attribute.into(),
                )]));
            }
            let field = Arc::new(field);
            let values: ArrayRef =
                Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"b".as_slice()]));
            let (array, kind): (ArrayRef, ValueKind) = match shape {
                0 => (
                    Arc::new(StructArray::new(vec![field].into(), vec![values], None)),
                    ValueKind::Struct(
                        vec![ValueField::new("value", child, S::pure())].into_boxed_slice(),
                    ),
                ),
                1 => (
                    Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
                        values,
                        None,
                    )),
                    ValueKind::List(child),
                ),
                2 => {
                    let key = ValueTypeRef::try_new(ValueKind::String, false, S::pure()).unwrap();
                    let entries = StructArray::new(
                        vec![Arc::new(Field::new("key", DataType::Utf8, false)), field].into(),
                        vec![Arc::new(StringArray::from(vec!["k1", "k2"])), values],
                        None,
                    );
                    let entries_field = Arc::new(
                        Field::new("entries", entries.data_type().clone(), false).with_metadata(
                            std::collections::HashMap::<String, String>::from([(
                                "nr_logical_type".into(),
                                "uninterpreted-map-source-attribute".into(),
                            )]),
                        ),
                    );
                    (
                        Arc::new(MapArray::new(
                            entries_field,
                            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
                            entries,
                            None,
                            false,
                        )),
                        ValueKind::Map { key, value: child },
                    )
                }
                _ => unreachable!(),
            };
            let layout = ValueLayout::try_new(array.data_type(), S::pure()).unwrap();
            let ty = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(kind, false, S::pure()).unwrap(),
                layout,
            )
            .unwrap();
            (array, ty)
        }

        #[test]
        fn same_layout_nested_domains_and_child_nullability_remain_distinct() {
            for shape in 0..3 {
                let (array, ty) = source(shape, L::Bitmap, false, None);
                let chunk = typed_result_chunk(array, &ty);
                for (domain, nullable) in [(L::Hll, false), (L::Physical, false), (L::Bitmap, true)]
                {
                    let (_, other) = source(shape, domain, nullable, None);
                    let error =
                        crate::query_execution::assembly::align_fetch_chunks_to_output_columns(
                            vec![chunk.clone()],
                            &[PlanOutputColumn {
                                name: "renamed".into(),
                                ty: other,
                            }],
                        )
                        .unwrap_err();
                    assert!(error.contains("complete value type differs"));
                }
            }
        }

        #[test]
        fn unknown_nested_attributes_do_not_author_domains_or_copy_payload() {
            for shape in 0..3 {
                let (array, ty) = source(shape, L::Bitmap, false, Some("unknown"));
                let before = array.to_data();
                let chunk = typed_result_chunk(array, &ty);
                let aligned =
                    crate::query_execution::assembly::align_fetch_chunks_to_output_columns(
                        vec![chunk],
                        &[PlanOutputColumn {
                            name: "renamed".into(),
                            ty: ty.clone(),
                        }],
                    )
                    .unwrap()
                    .remove(0);
                assert_shared_payload(&before, &aligned.record_batch().column(0).to_data());
                assert_eq!(
                    aligned.chunk_schema().slots()[0].typed_binding(),
                    ty.typed_binding()
                );
                assert_eq!(aligned.record_batch().schema().field(0).name(), "renamed");
                assert_eq!(aligned.record_batch().column(0).data_type(), ty.data_type());
            }
        }
    }
}
