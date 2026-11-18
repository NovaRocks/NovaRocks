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

//! Actual AP protocol recipe bound to QA's original schedule and request.
//! This recipe has no catalog preparation, Native Ready or manifest-access
//! owner. Its real protocol Sources use the sole production installer.

#[path = "native_recipe/split.rs"]
pub(crate) mod split;

use super::{FrontendFixture, ProgressBudget, ProgressSelection};
use crate::control_runtime::FrontendAttemptRuntime;
use crate::native::{
    data_runtime::FrontendDataRuntime,
    fragment_encoder::frozen::tests::frozen_package,
    task_result_transport::{NativeTaskResultTransport, TaskReadGrace},
};
use crate::query_execution::{
    artifact::{
        NativePlacementAssignment, TaskManifestBinding, ValidatedNativeSubmission,
        task_key_for_contract_test,
    },
    lifecycle_plan::QueryCredentialLeases,
    schedule::{SchedulingPlan, TaskPlacement},
};
use crate::query_execution::{
    internal_consumer::attempt_handoff::{AttemptHandoffProducer, AttemptHandoffResources},
    logical_binding::LogicalAttemptBindingCell,
    native_execution_adapter::{AttemptSourceInputs, source_cohort::NativeSourceCohort},
};
use crate::task_execution::host::install::InstalledAttemptHost;
use crate::task_execution::{
    clock::ManualClock,
    host::{
        install::InstallAttemptHost,
        transport::{FrozenSubscriptionStageInputs, stage_subscription_endpoints},
    },
    lease::RequestedLeaseDurations,
    manifest_projection::{AttemptPlanPolicy, ManifestAttemptInputs},
    sources::{AttemptEstablishFacts, SubmissionTaskPlans},
};
use novarocks_attempt_protocol::{config::CoordinationBudgets, ids::IdSeed};
use novarocks_coordination_contract::ExecutionEffect;
use novarocks_execution_contract::BackendProcessDescriptor;
use novarocks_execution_contract::TaskIdentity;
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_proto_models::{
    catalog::CatalogSet,
    novarocks::{QueryOptions, RuntimeFilterContribution},
};
use novarocks_query_application::{
    api::{
        ActivatedNativeAttempt, DormantNativeAttemptOwner, ExecutionSchedulingFacts,
        FragmentSchedulingFacts, NativeAttemptActivationFailure, NativeAttemptActivationFuture,
        NativeAttemptConvergenceFuture, NativeAttemptPreparationFuture,
        NativeAttemptPreparationPort, NativeAttemptPreparationWork, PlanSeal,
    },
    coordination::{AttemptSchedule, RootRelayBinding, RootRelayFrontier},
};
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_task_codec::TransportBudget;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::joint_admission::NativeAsyncActionAuthority;
use novarocks_workload_control::{CancellationView, ResultWindowAlias, WorkScope};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

/// Failure of the original Native activation carrier, never of the AP Source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActivationDropFault {
    Future,
    Dormant,
}

#[derive(Debug)]
pub(crate) struct OriginalActivationDropPanic {
    pub(crate) fault: ActivationDropFault,
    drops: Arc<AtomicUsize>,
}
impl Drop for OriginalActivationDropPanic {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ActivationDropObservation {
    pub(crate) activation_calls: usize,
    pub(crate) returned: usize,
    pub(crate) future_drops: usize,
    pub(crate) dormant_drops: usize,
    pub(crate) original_panic_alive: bool,
    pub(crate) original_panic_fault: Option<ActivationDropFault>,
    pub(crate) original_panic_drops: usize,
}

/// Passive snapshot of the SAME installed protocol graph. TaskNode is ids
/// only; StageNode/EdgeNode retain only frozen values. No creation seed,
/// descriptor, fragment bytes, Work, Scope or factory is retained here.
#[derive(Clone, Debug)]
pub(crate) struct RecipeGraphSnapshot {
    pub(crate) plan: PlanSeal,
    pub(crate) root: TaskIdentity,
    pub(crate) tasks: Vec<crate::task_execution::graph::TaskNode>,
    pub(crate) fragments: Vec<u32>,
    pub(crate) stages: Vec<crate::task_execution::graph::StageNode>,
    pub(crate) edges: Vec<crate::task_execution::graph::EdgeNode>,
    pub(crate) participations: Vec<novarocks_execution_contract::ParticipationRef>,
}
impl RecipeGraphSnapshot {
    fn capture(plan: PlanSeal, original: &crate::task_execution::graph::TaskGraph) -> Self {
        Self {
            plan,
            root: original.root_identity(),
            tasks: original.tasks().cloned().collect(),
            fragments: original
                .tasks()
                .map(|task| task.fragment_id())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            stages: original.stages().cloned().collect(),
            edges: original.edges().cloned().collect(),
            participations: original.participations().copied().collect(),
        }
    }
}

#[derive(Default)]
pub(crate) struct RecipeObservation {
    pub(crate) split: Mutex<Option<Arc<split::OriginalSplitObservation>>>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) handoff_binding_gate: Mutex<Option<Arc<OriginalHandoffBindingGate>>>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) protocol:
        Mutex<Option<crate::task_execution::protocol_facts::AttemptProtocolObservation>>,
    /// Passive same-watch alias only; it never keeps an AP/LP Source alive.
    pub(crate) profile_observer:
        Mutex<Option<crate::task_execution::profile_diagnostics::ProfileDiagnosticObserver>>,
    pub(crate) graph_capture_requested: AtomicBool,
    pub(crate) graph_capture: Mutex<Option<RecipeGraphSnapshot>>,
    /// Optional component transport for the captured collector ONLY. The
    /// actual Root relay and installed AP keep the original Native transport.
    pub(crate) profile_transport:
        Mutex<Option<Arc<dyn crate::native::task_result_transport::TaskResultTransport>>>,
    // Optional component observer of the SAME frozen graph and transport.
    // It grants no Source publication/selection or Ready authority.
    pub(crate) profile_capture_requested: AtomicBool,
    pub(crate) profile_capture:
        Mutex<Option<crate::task_execution::profile_diagnostics::TaskFinalInfoCollector>>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) credentials_requested: AtomicBool,
    #[cfg(novarocks_dev_controls)]
    pub(crate) credentials: Mutex<
        Option<std::sync::Weak<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>>,
    >,
    pub(crate) installed: Mutex<Option<Arc<InstalledAttemptHost>>>,
    pub(crate) root: Mutex<Option<TaskIdentity>>,
    pub(crate) tasks: Mutex<Vec<TaskIdentity>>,
    pub(crate) participations: Mutex<Vec<novarocks_execution_contract::ParticipationRef>>,
    pub(crate) activation:
        Mutex<Option<novarocks_coordination_contract::AttemptActivationIdentity>>,
    pub(crate) root_port:
        Mutex<Option<Arc<crate::native::task_result_transport::NativeBoundedRootReadPort>>>,
    pub(crate) read_close_gate: Mutex<Option<Arc<ReadCloseGate>>>,
    drop_fault: Mutex<Option<ActivationDropFault>>,
    activation_calls: AtomicUsize,
    activation_returns: AtomicUsize,
    activation_future_drops: AtomicUsize,
    dormant_drops: AtomicUsize,
    original_drop_panic: Mutex<Option<std::sync::Weak<OriginalActivationDropPanic>>>,
    original_drop_panic_drops: Arc<AtomicUsize>,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}

/// Passive values copied from the SAME activation action at its original
/// stage/bind boundary. These values grant no Ready source or capacity.
#[cfg(novarocks_dev_controls)]
#[derive(Clone, Copy)]
pub(crate) struct OriginalHandoffStage {
    pub(crate) activation: novarocks_coordination_contract::AttemptActivationIdentity,
    pub(crate) root: TaskIdentity,
    pub(crate) plan: PlanSeal,
}
#[cfg(novarocks_dev_controls)]
#[derive(Default)]
pub(crate) struct OriginalHandoffBindingGate {
    entered: Mutex<Option<OriginalHandoffStage>>,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}
#[cfg(novarocks_dev_controls)]
impl OriginalHandoffBindingGate {
    pub(crate) fn entered(&self) -> Option<OriginalHandoffStage> {
        *self.entered.lock().unwrap()
    }
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    async fn wait(&self, stage: OriginalHandoffStage) {
        assert!(self.entered.lock().unwrap().replace(stage).is_none());
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Default)]
pub(crate) struct ReadCloseGate {
    entered: AtomicBool,
    calls: std::sync::atomic::AtomicUsize,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl ReadCloseGate {
    pub(crate) fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub(crate) fn release_guard(self: &Arc<Self>) -> ReadCloseRelease {
        ReadCloseRelease(self.clone())
    }
    pub(crate) fn wait(&self) {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered.store(true, Ordering::Release);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
}
pub(crate) struct ReadCloseRelease(Arc<ReadCloseGate>);
impl Drop for ReadCloseRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct GatedRootRead {
    original: Arc<crate::native::task_result_transport::NativeBoundedRootReadPort>,
    gate: Arc<ReadCloseGate>,
}
impl novarocks_query_application::api::BoundedRootReadPort for GatedRootRead {
    fn read(
        &self,
        request: novarocks_execution_contract::root_result::RootResultRead,
        guard: ResultWindowAlias,
    ) -> novarocks_query_application::api::LogicalFuture<
        'static,
        Result<
            novarocks_query_application::api::RetainedRootReply,
            novarocks_query_application::coordination::RootResultFetchFailure,
        >,
    > {
        self.original.read(request, guard)
    }
    fn seal(
        &self,
        sealed: novarocks_execution_contract::root_lifetime::RootReadSealed,
    ) -> Result<(), novarocks_query_application::api::QueryExecutionError> {
        self.gate.wait();
        self.original.seal(sealed)
    }
}
impl RecipeObservation {
    pub(crate) fn arm_activation_drop(&self, fault: ActivationDropFault) {
        assert_eq!(self.activation_calls.load(Ordering::Acquire), 0);
        assert!(self.drop_fault.lock().unwrap().replace(fault).is_none());
    }
    pub(crate) fn activation_drop_observation(&self) -> ActivationDropObservation {
        let original = self
            .original_drop_panic
            .lock()
            .unwrap()
            .clone()
            .and_then(|weak| weak.upgrade());
        ActivationDropObservation {
            activation_calls: self.activation_calls.load(Ordering::Acquire),
            returned: self.activation_returns.load(Ordering::Acquire),
            future_drops: self.activation_future_drops.load(Ordering::Acquire),
            dormant_drops: self.dormant_drops.load(Ordering::Acquire),
            original_panic_alive: original.is_some(),
            original_panic_fault: original.as_ref().map(|original| original.fault),
            original_panic_drops: self.original_drop_panic_drops.load(Ordering::Acquire),
        }
    }
    fn original_drop_panic(&self, fault: ActivationDropFault) -> ! {
        let original = Arc::new(OriginalActivationDropPanic {
            fault,
            drops: self.original_drop_panic_drops.clone(),
        });
        assert!(
            self.original_drop_panic
                .lock()
                .unwrap()
                .replace(Arc::downgrade(&original))
                .is_none()
        );
        // This is the raw panic of this original destructor, not an output
        // field or a test-only active owner. QA must retain this SAME payload.
        std::panic::panic_any(original);
    }
    pub(crate) fn release_activation(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    async fn wait_release(&self) {
        loop {
            let notified = self.changed.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// Explicit static operands of this finite protocol recipe. Encoded roots
/// preserve the original completed candidate's private wire domain and seal.
#[derive(Debug)]
pub(crate) enum RecipeRoot {
    EmptyCountOnly,
    Encoded(EncodedRecipeRoot),
    ProtocolChain(ProtocolChainRecipe),
}
#[derive(Debug)]
pub(crate) struct EncodedRecipeRoot {
    fragment: Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>,
    kind: RootOutputKind,
    plan: PlanSeal,
}
#[derive(Debug)]
pub(crate) struct ProtocolChainRecipe {
    root: u32,
    predecessors: [u32; 2],
    plan: PlanSeal,
    kind: RootOutputKind,
    fragments: BTreeMap<u32, Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>>,
    edges: Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts>,
    write_roles: Option<RecipeWriteProtocolRoles>,
}

/// These are the original component's frozen protocol declarations, not a
/// TableWriter/Router factory or evidence that any BE writer actually ran.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RecipeWriteProtocolRoles {
    UpstreamWriters,
    IndependentSummarySource,
}
impl RecipeRoot {
    /// Seal the complete original no-expression component program through the
    /// existing pure package author. This is neither SQL semantics nor a
    /// Worker/TableWriter implementation; no SQL source journal is invented.
    #[cfg(test)]
    pub(crate) fn from_structural_component(
        candidate: &novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
        seal: PlanSeal,
        carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::query_execution::package_freeze::PackageFreezeError> {
        use crate::query_execution::package_freeze::PackageFreezeError;
        use novarocks_physical_plan as p;
        fn refused(error: &(dyn std::error::Error + 'static)) -> PackageFreezeError {
            // Preserve the same typed cancellation/deadline/control cause
            // through the validators' original error/source chain.
            let mut source = Some(error);
            for _ in 0..8 {
                let Some(original) = source else { break };
                if let Some(original) =
                    original.downcast_ref::<novarocks_type_contract::CompileControlError>()
                {
                    return PackageFreezeError::Control(original.clone());
                }
                source = original.source();
            }
            PackageFreezeError::Facts(format!("pure component package refused: {error}"))
        }
        let plan = candidate.plan();
        if PlanSeal::Version(plan.version()) != seal
            || plan.fragments().len() != 1
            || !plan.edges().is_empty()
            || !plan.runtime_filters().is_empty()
        {
            return Err(PackageFreezeError::Facts(
                "pure component requires its original seal and complete single-fragment graph"
                    .into(),
            ));
        }
        let result = plan.result_port().ok_or_else(|| {
            PackageFreezeError::Facts("pure component requires its original result port".into())
        })?;
        let fragment = plan.fragments().get(&result.fragment).ok_or_else(|| {
            PackageFreezeError::Facts("pure component result fragment is missing".into())
        })?;
        let one_empty_values = fragment.nodes().len() == 1
            && fragment.nodes().get(&fragment.root()).is_some_and(
                |node| matches!(&node.kind, p::NodeKind::Values { rows } if rows.is_empty()),
            );
        if !one_empty_values
            || !fragment.values().is_empty()
            || fragment
                .nodes()
                .values()
                .any(|node| node.expressions.is_some())
            || !result.fields.is_empty()
            || !result.output.columns.is_empty()
        {
            return Err(PackageFreezeError::Facts(
                "pure component requires one original empty Values node with no values or expressions".into(),
            ));
        }
        let p::FragmentSink::RootResult(contract) = fragment.sink() else {
            return Err(PackageFreezeError::Facts(
                "pure component requires its original frozen RootResult purpose".into(),
            ));
        };
        contract
            .validate_purpose()
            .map_err(|error| refused(&error))?;
        let kind = contract.kind();
        if !matches!(
            kind,
            RootOutputKind::InternalFacts(
                novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1
                    | novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1
            )
        ) {
            return Err(PackageFreezeError::Facts(
                "pure component requires its original Statistics or PreparedWrite domain".into(),
            ));
        }
        let id = fragment.id();
        let pruning = p::FrozenFragmentPruning::try_new(id, Vec::new(), control)
            .map_err(|error| refused(&error))?;
        // Full-plan extraction proves exact fragment coverage and preserves
        // the original plan's version/result/constants/parameters/metadata.
        let mut packages = p::extract_fragment_packages(
            plan,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &[(id, pruning)].into(),
            &[(id, *carrier.admission())].into(),
            control,
        )
        .map_err(|error| match error {
            p::FragmentPackageExtractionError::Control(original) => {
                PackageFreezeError::Control(original)
            }
            p::FragmentPackageExtractionError::Local(original) => refused(&original),
            other => refused(&other),
        })?;
        if packages.len() != plan.fragments().len() {
            return Err(PackageFreezeError::Facts(
                "pure component package extraction changed full-plan coverage".into(),
            ));
        }
        let package = packages.remove(&id).ok_or_else(|| {
            PackageFreezeError::Facts(
                "pure component package extraction lost its original fragment".into(),
            )
        })?;
        let artifact = crate::native::fragment_encoder::frozen::FragmentArtifact::freeze_package(
            &package,
            plan.edges(),
            carrier,
            control,
        )?;
        Ok(Self::Encoded(EncodedRecipeRoot {
            fragment: artifact,
            kind,
            plan: seal,
        }))
    }

    fn fragment_id(&self) -> u32 {
        match self {
            Self::EmptyCountOnly => 1,
            Self::Encoded(original) => original.fragment.facts().fragment_id(),
            Self::ProtocolChain(original) => original.root,
        }
    }
    fn kind(&self) -> RootOutputKind {
        match self {
            Self::EmptyCountOnly => RootOutputKind::CountOnly,
            Self::Encoded(original) => original.kind,
            Self::ProtocolChain(original) => original.kind,
        }
    }
    fn fragment(
        &self,
        plan: PlanSeal,
    ) -> Arc<crate::native::fragment_encoder::frozen::FragmentArtifact> {
        match self {
            Self::EmptyCountOnly => frozen_package(
                1,
                100,
                [9; 16],
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                None,
                0,
            ),
            Self::Encoded(original) => {
                assert_eq!(original.plan, plan);
                original.fragment.clone()
            }
            Self::ProtocolChain(original) => {
                assert_eq!(original.plan, plan);
                original.fragments[&original.root].clone()
            }
        }
    }
    /// A finite protocol graph, not a SQL/Native writer-runtime capability.
    /// The root artifact remains the caller's original frozen purpose/seal;
    /// these two checked sender packages are the existing neutral test author.
    pub(crate) fn with_three_task_protocol_chain(
        self,
        plan: PlanSeal,
        predecessors: [u32; 2],
    ) -> Self {
        self.with_three_task_protocol_chain_with_delivery(
            plan,
            predecessors,
            novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
        )
    }
    /// Freeze explicit component delivery in BOTH original sender packages
    /// and graph edges. Result membership alone cannot grant MustDrain.
    pub(crate) fn with_three_task_protocol_chain_with_delivery(
        self,
        plan: PlanSeal,
        predecessors: [u32; 2],
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    ) -> Self {
        let [leaf, middle] = predecessors;
        let root = self.fragment_id();
        assert!(leaf != middle && leaf != root && middle != root);
        let root_artifact = self.fragment(plan);
        let PlanSeal::Version(version) = plan;
        let middle_node = i32::try_from(middle).unwrap().checked_mul(100).unwrap();
        let leaf_node = i32::try_from(leaf).unwrap().checked_mul(100).unwrap();
        let root_node = root_artifact.facts().root_plan_node_id();
        let dop = PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        };
        let leaf_artifact = frozen_package(
            leaf,
            leaf_node,
            *version.as_bytes(),
            dop,
            Some((middle, middle_node, delivery)),
            0,
        );
        let middle_artifact = frozen_package(
            middle,
            middle_node,
            *version.as_bytes(),
            dop,
            Some((root, root_node, delivery)),
            0,
        );
        let edges = vec![
            crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
                source_fragment_id: leaf,
                target_fragment_id: middle,
                target_exchange_node_id: middle_node,
                partition_kind:
                    crate::query_execution::attempt_plan_facts::AttemptPartitionKind::Hash,
                delivery,
            },
            crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
                source_fragment_id: middle,
                target_fragment_id: root,
                target_exchange_node_id: root_node,
                partition_kind:
                    crate::query_execution::attempt_plan_facts::AttemptPartitionKind::Hash,
                delivery,
            },
        ];
        let kind = self.kind();
        Self::ProtocolChain(ProtocolChainRecipe {
            root,
            predecessors,
            write_roles: None,
            plan,
            kind,
            edges,
            fragments: [
                (leaf, leaf_artifact),
                (middle, middle_artifact),
                (root, root_artifact),
            ]
            .into(),
        })
    }
    pub(crate) fn with_write_protocol_roles(mut self, roles: RecipeWriteProtocolRoles) -> Self {
        let Self::ProtocolChain(original) = &mut self else {
            panic!("write protocol roles require the original three-task graph");
        };
        assert!(original.write_roles.replace(roles).is_none());
        self
    }
    fn bind_write_protocol_roles(&self, inputs: &mut ManifestAttemptInputs) {
        let Self::ProtocolChain(original) = self else {
            return;
        };
        let Some(roles) = original.write_roles else {
            return;
        };
        assert!(inputs.writers.is_empty());
        assert!(inputs.summary_sources.is_empty());
        let task_for = |fragment| {
            let mut tasks = inputs
                .graph
                .tasks()
                .filter(|task| task.fragment_id() == fragment)
                .map(|task| task.identity());
            let task = tasks
                .next()
                .expect("one actual scheduled Task per frozen role");
            assert!(tasks.next().is_none());
            task
        };
        let leaf = task_for(original.predecessors[0]);
        let middle = task_for(original.predecessors[1]);
        assert_ne!(leaf, middle);
        assert_ne!(leaf, inputs.graph.root_identity());
        assert_ne!(middle, inputs.graph.root_identity());
        match roles {
            RecipeWriteProtocolRoles::UpstreamWriters => {
                inputs.writers = [leaf, middle].into();
            }
            RecipeWriteProtocolRoles::IndependentSummarySource => {
                inputs.writers = [middle].into();
                inputs.summary_sources.insert(
                    novarocks_result_contract::SummaryKind::WriteCounts,
                    [leaf].into(),
                );
            }
        }
    }
    fn fragments_for_protocol(
        &self,
        plan: PlanSeal,
    ) -> BTreeMap<u32, Arc<crate::native::fragment_encoder::frozen::FragmentArtifact>> {
        match self {
            Self::ProtocolChain(original) => {
                assert_eq!(original.plan, plan);
                original.fragments.clone()
            }
            _ => [(self.fragment_id(), self.fragment(plan))].into(),
        }
    }
    fn protocol_edges(&self) -> Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts> {
        match self {
            Self::ProtocolChain(original) => original.edges.clone(),
            _ => Vec::new(),
        }
    }
    fn protocol_order(&self) -> Vec<u32> {
        match self {
            Self::ProtocolChain(original) => vec![
                original.predecessors[0],
                original.predecessors[1],
                original.root,
            ],
            _ => vec![self.fragment_id()],
        }
    }
    fn scheduling(&self) -> ExecutionSchedulingFacts {
        let order = self.protocol_order();
        ExecutionSchedulingFacts {
            topological_fragment_order: order.clone(),
            execution_anchor_fragment_id: self.fragment_id(),
            fragments: order
                .into_iter()
                .map(|fragment_id| FragmentSchedulingFacts {
                    fragment_id,
                    placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
                    scans: Vec::new(),
                })
                .collect(),
            edges: self
                .protocol_edges()
                .into_iter()
                .map(
                    |edge| novarocks_query_application::api::SchedulingEdgeFacts {
                        source_fragment_id: edge.source_fragment_id,
                        target_fragment_id: edge.target_fragment_id,
                        target_exchange_node_id: edge.target_exchange_node_id,
                        stream_kind:
                            novarocks_query_application::api::SchedulingStreamKind::Partitioned,
                        hash_partitioned: true,
                    },
                )
                .collect(),
        }
    }
}

pub(crate) struct ProtocolRecipe {
    scheduling: Option<ExecutionSchedulingFacts>,
    owner: Option<Box<dyn DormantNativeAttemptOwner>>,
}
impl std::fmt::Debug for ProtocolRecipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolRecipe").finish_non_exhaustive()
    }
}
impl ProtocolRecipe {
    pub(crate) fn new(owner: RecipeDormant) -> Self {
        Self {
            scheduling: Some(owner.root.scheduling()),
            owner: Some(Box::new(owner)),
        }
    }
}
impl NativeAttemptPreparationPort for ProtocolRecipe {
    fn prepare_in<'a>(
        &'a mut self,
        work: &'a mut NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a> {
        Box::pin(async move {
            work.bind_in(&mut self.scheduling, &mut self.owner)
                .map_err(Into::into)
        })
    }
}

/// Frozen diagnostic operand for the SAME installed recipe. TaskResultTransport
/// supplies only its original final-info/filter I/O; Root relay keeps its
/// independent NativeBoundedRootReadPort. This creates no task/Ready/Work.
pub(crate) struct OriginalRecipeDiagnostics {
    pub(crate) transport: Arc<dyn crate::native::task_result_transport::TaskResultTransport>,
}

pub(crate) struct RecipeDormant {
    pub(crate) process: std::sync::Weak<FrontendAttemptRuntime>,
    pub(crate) data: FrontendDataRuntime,
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) descriptor: BackendProcessDescriptor,
    pub(crate) eligible: [BackendProcessId; 1],
    pub(crate) scope: WorkScope,
    pub(crate) cohort: NativeSourceCohort,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) window: ResultWindowAlias,
    pub(crate) logical: Arc<LogicalAttemptBindingCell>,
    pub(crate) handoff: Option<AttemptHandoffProducer>,
    // Optional SAME original storage drained once by the domain credential
    // author, before activation. It is moved once into the Source handoff.
    pub(crate) original_credentials:
        Option<Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>>,
    pub(crate) plan: PlanSeal,
    pub(crate) root: RecipeRoot,
    pub(crate) diagnostics: Option<OriginalRecipeDiagnostics>,
    pub(crate) observation: Arc<RecipeObservation>,
    pub(crate) fixture_observation_budget:
        Option<crate::task_execution::host::observations::ObservationBudget>,
    pub(crate) original_exit: Option<crate::control_runtime::host::HostExit>,
}
impl std::fmt::Debug for RecipeDormant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecipeDormant").finish_non_exhaustive()
    }
}
impl Drop for RecipeDormant {
    fn drop(&mut self) {
        self.observation
            .dormant_drops
            .fetch_add(1, Ordering::AcqRel);
        let fault = *self.observation.drop_fault.lock().unwrap();
        if fault == Some(ActivationDropFault::Dormant)
            && self.observation.activation_returns.load(Ordering::Acquire) != 0
        {
            self.observation
                .original_drop_panic(ActivationDropFault::Dormant);
        }
    }
}

/// Wrap the SAME actual installer future. The successfully returned binding
/// remains with QA's receive callback before this destructor can panic.
struct OriginalActivationFuture<'a> {
    inner: NativeAttemptActivationFuture<'a>,
    observation: Arc<RecipeObservation>,
    returned: bool,
}
impl std::future::Future for OriginalActivationFuture<'_> {
    type Output = Result<ActivatedNativeAttempt, NativeAttemptActivationFailure>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll(cx);
        if let Poll::Ready(Ok(_)) = &result {
            assert!(!this.returned, "one successful original activation return");
            this.returned = true;
            this.observation
                .activation_returns
                .fetch_add(1, Ordering::AcqRel);
        }
        result
    }
}
impl Drop for OriginalActivationFuture<'_> {
    fn drop(&mut self) {
        self.observation
            .activation_future_drops
            .fetch_add(1, Ordering::AcqRel);
        let fault = *self.observation.drop_fault.lock().unwrap();
        if self.returned && fault == Some(ActivationDropFault::Future) {
            self.observation
                .original_drop_panic(ActivationDropFault::Future);
        }
    }
}

impl DormantNativeAttemptOwner for RecipeDormant {
    fn eligible_backends(&self) -> &[BackendProcessId] {
        &self.eligible
    }
    fn poll_bind_qualification(
        &mut self,
        _schedule: &AttemptSchedule,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), NativeAttemptActivationFailure>> {
        panic!("no-recovery protocol recipe cannot request replacement qualification")
    }
    fn activate<'a>(
        &'a mut self,
        activation: novarocks_coordination_contract::AttemptActivationIdentity,
        actual: &'a AttemptSchedule,
        replacements: Option<
            Box<[novarocks_query_application::coordination::ReplacementWorkerAdmissionEvidence]>,
        >,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a> {
        self.observation
            .activation_calls
            .fetch_add(1, Ordering::AcqRel);
        let observation = self.observation.clone();
        let inner = Box::pin(async move {
            assert!(replacements.is_none());
            assert_eq!(actual.execution(), activation.execution());
            let fragment_id = self.root.fragment_id();
            let kind = self.root.kind();
            let task_key =
                task_key_for_contract_test(actual.execution().query_id(), fragment_id, 0);
            let fragments = self.root.fragments_for_protocol(self.plan);
            let edges = self.root.protocol_edges();
            let placements = SchedulingPlan {
                root_fragment_id: fragment_id,
                by_fragment: fragments
                    .keys()
                    .copied()
                    .map(|fragment_id| {
                        (
                            fragment_id,
                            vec![TaskPlacement {
                                fragment_id,
                                instance_index: 0,
                                task_key: task_key_for_contract_test(
                                    actual.execution().query_id(),
                                    fragment_id,
                                    0,
                                ),
                                backend_idx: 0,
                                endpoint: self.descriptor.endpoint().clone(),
                                scan_ranges: BTreeMap::new(),
                            }],
                        )
                    })
                    .collect(),
                root_task_key: task_key,
                root_backend_idx: 0,
            };
            let manifest = TaskManifestBinding::protocol_fixture_with_schedule_for_test(
                actual,
                self.plan,
                &placements,
                &edges,
                &[(0, self.eligible[0])].into(),
                fragments.clone(),
                &[(0, self.descriptor.clone())].into(),
            )
            .unwrap();
            manifest.validate_schedule(actual).unwrap();
            assert_eq!(manifest.plan(), self.plan);
            let submissions = fragments
                .into_iter()
                .map(|(fragment_id, fragment)| {
                    ValidatedNativeSubmission::new(
                        0,
                        task_key_for_contract_test(actual.execution().query_id(), fragment_id, 0),
                        actual.execution(),
                        fragment,
                        NativePlacementAssignment::new(0, NonZeroUsize::MIN, BTreeMap::new()),
                    )
                })
                .collect();
            let mut plans = SubmissionTaskPlans::index_manifest(submissions, &manifest).unwrap();
            let mut original_inputs = ManifestAttemptInputs::freeze(
                &manifest,
                &mut plans,
                self.descriptor.native_compatibility_id(),
                TransportBudget::DEFAULT,
            )
            .unwrap();
            // SAME graph identities reach both FrozenAttemptPlan and the
            // existing ProtocolFacts::for_graph author before any publication.
            self.root.bind_write_protocol_roles(&mut original_inputs);
            let mut prepared = original_inputs.prepare_publications().unwrap();
            #[cfg(novarocks_dev_controls)]
            let credentials = if self.original_credentials.is_none()
                && self
                    .observation
                    .credentials_requested
                    .load(Ordering::Acquire)
            {
                let original =
                    crate::query_execution::internal_consumer::test_fixture::original_credentials(
                        actual.execution(),
                    );
                *self.observation.credentials.lock().unwrap() = Some(Arc::downgrade(&original));
                Some(original)
            } else {
                None
            };
            #[cfg(not(novarocks_dev_controls))]
            let credentials = None;
            let backends: Arc<[BackendProcessDescriptor]> = Arc::from([self.descriptor.clone()]);
            let results = Arc::new(
                NativeTaskResultTransport::new(
                    backends.clone(),
                    self.data.clone(),
                    TaskReadGrace::new(Duration::from_secs(1)),
                )
                .unwrap(),
            );
            let root_port = Arc::new(
                crate::native::task_result_transport::NativeBoundedRootReadPort::new(
                    results.clone(),
                ),
            );
            let retain_diagnostics = self.diagnostics.is_some();
            let diagnostic_results: Arc<
                dyn crate::native::task_result_transport::TaskResultTransport,
            > = self
                .diagnostics
                .as_ref()
                .map(|original| original.transport.clone())
                .unwrap_or_else(|| results.clone());
            let credentials = match self.original_credentials.take() {
                Some(original) => {
                    assert!(credentials.is_none(), "one original credential author only");
                    #[cfg(novarocks_dev_controls)]
                    {
                        let previous = self
                            .observation
                            .credentials
                            .lock()
                            .unwrap()
                            .replace(Arc::downgrade(&original));
                        assert!(previous.is_none());
                    }
                    Some(original)
                }
                None => credentials,
            };
            let (profile, profile_observer) = if retain_diagnostics {
                let mut collector =
                    crate::task_execution::profile_diagnostics::TaskFinalInfoCollector::from_graph(
                        diagnostic_results.clone(),
                        true,
                        prepared.graph(),
                        Arc::new(crate::wake::CountingWake::default()),
                    );
                let observer = collector.enable_profile_observation().unwrap();
                assert!(
                    self.observation
                        .profile_observer
                        .lock()
                        .unwrap()
                        .replace(observer.clone_for_fixture())
                        .is_none()
                );
                (Some(collector), Some(observer))
            } else {
                (None, None)
            };
            let protocol = prepared.publications.protocol_facts.take().unwrap();
            #[cfg(novarocks_dev_controls)]
            {
                let observation = protocol.observation_for_fixture();
                assert!(
                    self.observation
                        .protocol
                        .lock()
                        .unwrap()
                        .replace(observation)
                        .is_none()
                );
            }
            self.handoff
                .as_mut()
                .unwrap()
                .stage(
                    actual.execution(),
                    actual.root(),
                    self.plan,
                    AttemptHandoffResources::new(protocol, credentials, profile_observer),
                )
                .unwrap();
            #[cfg(novarocks_dev_controls)]
            {
                let gate = {
                    self.observation
                        .handoff_binding_gate
                        .lock()
                        .unwrap()
                        .clone()
                };
                if let Some(gate) = gate {
                    // The producer stays in THIS same original Dormant slot,
                    // then moves through the ordinary installer into Source
                    // originals. Only this real async action is held here.
                    gate.wait(OriginalHandoffStage {
                        activation,
                        root: actual.root(),
                        plan: self.plan,
                    })
                    .await;
                }
            }
            self.handoff.as_mut().unwrap().bind(activation).unwrap();
            let staged = stage_subscription_endpoints(
                &manifest
                    .participations()
                    .iter()
                    .map(|p| crate::control_runtime::membership::FrozenMember {
                        participation: p.participation(),
                        endpoint: self.descriptor.endpoint().clone(),
                    })
                    .collect::<Vec<_>>(),
                FrozenSubscriptionStageInputs {
                    backends: &backends,
                    compatibility: self.descriptor.native_compatibility_id(),
                    data: &self.data,
                    scope: &self.scope,
                    cohort: &self.cohort,
                    execution: actual.execution(),
                },
            )
            .unwrap();
            let establish = AttemptEstablishFacts::freeze(
                CatalogSet::default(),
                [(self.eligible[0], RuntimeFilterContribution::default())],
                QueryOptions::default(),
                &QueryCredentialLeases::empty(),
            )
            .unwrap();
            let mut establish_preparation =
                crate::task_execution::host::transport::EstablishMaterialPreparation::new();
            establish_preparation
                .freeze(&establish, actual.participations().iter().copied())
                .unwrap();
            let process = self
                .process
                .upgrade()
                .expect("same original fixture process");
            let logical = self.logical.bound().unwrap();
            let link = logical.links.borrow(activation).unwrap();
            let split_observation = { self.observation.split.lock().unwrap().take() };
            let mut split = match split_observation {
                Some(observation) => Some(
                    split::original_plan(
                        actual.root(),
                        fragment_id,
                        task_key,
                        &self.scope,
                        &self.window,
                        &self.data,
                        observation,
                    )
                    .await,
                ),
                None => None,
            };
            let mut inputs = logical.inputs.loan().await.unwrap();
            if self
                .observation
                .graph_capture_requested
                .load(Ordering::Acquire)
            {
                let snapshot = RecipeGraphSnapshot::capture(manifest.plan(), prepared.graph());
                let mut target = self.observation.graph_capture.lock().unwrap();
                assert!(target.is_none(), "the same original graph is captured once");
                *target = Some(snapshot);
            }
            if self
                .observation
                .profile_capture_requested
                .load(Ordering::Acquire)
            {
                let component_transport =
                    { self.observation.profile_transport.lock().unwrap().take() };
                let profile_transport: Arc<
                    dyn crate::native::task_result_transport::TaskResultTransport,
                > = component_transport.unwrap_or_else(|| results.clone());
                let collector =
                    crate::task_execution::profile_diagnostics::TaskFinalInfoCollector::from_graph(
                        profile_transport,
                        true,
                        prepared.graph(),
                        Arc::new(crate::wake::CountingWake::default()),
                    );
                assert!(
                    self.observation
                        .profile_capture
                        .lock()
                        .unwrap()
                        .replace(collector)
                        .is_none(),
                    "the original graph collector is captured once before installation"
                );
            }
            let lease = RequestedLeaseDurations::DEFAULT;
            let installed = Arc::new(
                InstallAttemptHost {
                    fixture_observation_budget: self.fixture_observation_budget,
                    #[cfg(novarocks_dev_controls)]
                    fixture_establish_handoff: None,
                    initial_now: novarocks_coordination_contract::MonotonicClock::now(
                        self.clock.as_ref(),
                    ),
                    process: &process,
                    data: &self.data,
                    clock: self.clock.clone(),
                    activation,
                    authorization_limits:
                        novarocks_coordination_contract::AttemptAuthorizationLimits {
                            activation,
                            establish_per_participation: logical
                                .start
                                .budgets
                                .establish_per_participation,
                            abort_per_participation: logical.start.budgets.abort_per_participation,
                        },
                    logical: &logical,
                    link: &link,
                    logical_inputs: &mut inputs,
                    original_exit: &mut self.original_exit,
                    prepared,
                    establish: &mut establish_preparation,
                    replacements: None,
                    subscriptions: staged,
                    authority: self.authority.clone(),
                    results: diagnostic_results,
                    profile,
                    dynamic: None,
                    originals: AttemptSourceInputs::ProtocolFixture {
                        activation,
                        split: &mut split,
                        handoff: &mut self.handoff,
                    },
                    policy: AttemptPlanPolicy {
                        coordination: CoordinationBudgets::DEFAULT,
                        retry: Default::default(),
                        initial_lease: lease.initial(),
                        steady_lease: lease.steady(),
                    },
                    ids: IdSeed::new(1, 100),
                    retain_diagnostics,
                    observe_dev_root: false,
                    observe_lifecycle: false,
                    #[cfg(novarocks_dev_controls)]
                    root_observer: None,
                }
                .install()?,
            );
            drop(inputs);
            drop(link);
            *self.observation.root_port.lock().unwrap() = Some(root_port.clone());
            *self.observation.root.lock().unwrap() = Some(actual.root());
            *self.observation.participations.lock().unwrap() = actual.participations().to_vec();
            *self.observation.tasks.lock().unwrap() = manifest
                .tasks()
                .iter()
                .map(|task| task.identity())
                .collect();
            *self.observation.activation.lock().unwrap() = Some(activation);
            *self.observation.installed.lock().unwrap() = Some(installed.clone());
            self.observation.changed.notify_waiters();
            let mut startup = installed.startup.clone();
            crate::query_execution::native_execution_adapter::wait_original_attempt_initialization(
                &mut startup,
                self.logical.as_ref(),
                activation,
                &cancellation,
            )
            .await?;
            self.observation.wait_release().await;
            let relay_port: Arc<dyn novarocks_query_application::api::BoundedRootReadPort> =
                match self.observation.read_close_gate.lock().unwrap().clone() {
                    Some(gate) => Arc::new(GatedRootRead {
                        original: root_port,
                        gate,
                    }),
                    None => root_port,
                };
            Ok(ActivatedNativeAttempt::relayed_rows(
                activation,
                RootRelayBinding {
                    port: relay_port,
                    frontier: RootRelayFrontier::new(actual.root(), RootProfileId::V1, kind, None)
                        .unwrap(),
                    window: self.window.clone(),
                    max_wait: Duration::from_millis(10),
                },
            ))
        });
        Box::pin(OriginalActivationFuture {
            inner,
            observation,
            returned: false,
        })
    }
    fn converge<'a>(
        &'a mut self,
        _cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a> {
        Box::pin(async move {
            let activation = *self.observation.activation.lock().unwrap();
            if let Some(activation) = activation {
                novarocks_query_application::api::NativeInstalledAttemptPort::resources_exited(
                    self.logical.as_ref(),
                    activation,
                )
                .await
            } else {
                // Before installation this finite protocol recipe has started no
                // provider/transport work and retains its original sync values.
                if let Some(mut producer) = self.handoff.take() {
                    producer.retire_unclaimed();
                }
                Ok(())
            }
        })
    }
}
pub(crate) fn count_description() -> (
    novarocks_query_application::preparation::FrozenExecutionDescription,
    PlanSeal,
) {
    use novarocks_physical_plan::{
        FragmentBuilder, FragmentId, FragmentSink, OutputPort, PlanBuilder, PlanVersionId,
        ResultPort,
    };
    use novarocks_query_application::{
        api::QueryExecutionKind,
        preparation::{
            CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
            FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
        },
    };
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract};
    let id = FragmentId::new(1);
    let mut fragment = FragmentBuilder::new(id);
    let node = fragment.reserve_node_id().unwrap();
    fragment
        .add_values(node, Box::default(), Box::default())
        .unwrap();
    let fragment = fragment
        .finish_definition(
            node,
            FragmentSink::RootResult(Box::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            ))),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: true,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let version = PlanVersionId::try_new([9; 16]).unwrap();
    let mut plan = PlanBuilder::new(version, crate::application::test_constant_policy());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: id,
        output: OutputPort {
            node,
            columns: Box::default(),
        },
        fields: Box::default(),
    })
    .unwrap();
    let candidate = CompletedPhysicalPlanCandidate::for_program(
        plan.finish().unwrap(),
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    let kind = QueryExecutionKind::Statistics;
    let output = OutputContract::from_completed_candidate(kind, &candidate).unwrap();
    let description = FrozenExecutionDescription::for_completed_plan(
        kind,
        candidate,
        Vec::new(),
        output,
        ExecutionEffect::None,
        novarocks_coordination_contract::RecoveryMode::NoRecovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
    .into_internal_computation()
    .unwrap();
    (description, PlanSeal::Version(version))
}

struct CurrentPeer(BackendProcessDescriptor);
impl novarocks_query_application::api::BackendProcessObservationPort for CurrentPeer {
    fn observe_process_at_endpoint(
        &self,
        process: BackendProcessId,
        endpoint: &novarocks_execution_contract::RuntimeEndpoint,
    ) -> Result<
        novarocks_query_application::api::BackendProcessObservation,
        novarocks_query_application::api::BackendTopologyError,
    > {
        assert_eq!(process, self.0.process_id());
        assert_eq!(endpoint, self.0.endpoint());
        Ok(novarocks_query_application::api::BackendProcessObservation::Current)
    }
}

// This is the normal production process owner without its separately-owned
// poller and membership service. The test gives the same queues finite turns.
fn manual_process_with_observation_budget(
    data: &FrontendDataRuntime,
    clock: Arc<dyn novarocks_coordination_contract::MonotonicClock>,
    backend_processes: novarocks_query_application::api::BackendProcessObservationService,
    history: Option<novarocks_control_runtime::history::HistoryCapture>,
    budget: crate::task_execution::host::observations::ObservationBudget,
) -> FrontendAttemptRuntime {
    use crate::control_runtime::{
        actor::FrontendClock, host::HostRegistry, membership::MembershipRegistry,
        metrics::FrontendHostObservations,
    };
    use crate::task_execution::host::observations::backing::ObservationBackingQuote;
    use novarocks_control_runtime::{
        actor_turn::{ActorRuntime, Clock},
        observation::RuntimeObservation,
        transaction::RuntimeLedger,
    };
    let layout = data.transport_capacity().clone();
    let clock: Arc<dyn Clock> = Arc::new(FrontendClock(clock));
    let observation = RuntimeObservation::new(Some(clock.clone()));
    let limit = NonZeroUsize::new(8).unwrap();

    let limits = layout
        .process_limits()
        .checked_add(&budget.byte_windows(limit).unwrap())
        .unwrap()
        .checked_add(&ObservationBackingQuote::process_limit(budget, limit).unwrap())
        .unwrap();
    let ledger = RuntimeLedger::with_keyed_families_and_observation(
        limits,
        layout.keyed_families(),
        [],
        history.clone(),
        observation.clone(),
    )
    .unwrap();
    let runtime = ActorRuntime::with_observation(history, observation.clone());
    let host_observation = Arc::new(FrontendHostObservations::new(clock.clone()));
    let host = Arc::new(HostRegistry::new(
        runtime.clone(),
        ledger.clone(),
        host_observation.clone(),
    ));
    FrontendAttemptRuntime {
        registration_open: Mutex::new(true),
        runtime,
        ledger,
        layout,
        host,
        members: Arc::new(MembershipRegistry::new()),
        backend_processes,
        clock,
        observation,
        host_observation,
        unresolved_replacements: Mutex::new(BTreeMap::new()),
    }
}

/// The original process/control bootstrap before any Root or Native request.
/// A caller-supplied CPU owner binds to this SAME Control before Ready. This
/// helper issues no result window, payload, child stage or computation grant.
pub(crate) struct PreparedOriginalRecipeProcess {
    pub(crate) fixture: FrontendFixture,
    pub(crate) process: Arc<FrontendAttemptRuntime>,
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) joint: novarocks_workload_control::joint_admission::JointProgressDriver,
    pub(crate) workload: novarocks_workload_control::WorkloadControl,
    pub(crate) supervisor: novarocks_query_application::coordination::LogicalExecutionSupervisor,
    pub(crate) client: novarocks_query_application::api::QueryExecutionClient,
    pub(crate) capacity: novarocks_workload_control::ResultCapacityHandle,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) descriptor: BackendProcessDescriptor,
}
pub(crate) fn prepare_original_recipe_process(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    result: novarocks_workload_control::ResultCapacityConfig,
    cpu: Option<&mut crate::query_execution::internal_result_cpu::InternalResultCpuOwner>,
) -> PreparedOriginalRecipeProcess {
    prepare_original_recipe_process_inner(
        data,
        descriptor.clone(),
        history,
        result,
        cpu,
        OriginalRecipeProcessPolicy {
            supervisor: original_recipe_supervisor_config(None),
            namespace: novarocks_types::identity::QueryProcessNamespace::new(712),
            observation_budget: None,
            process_observation: Arc::new(CurrentPeer(descriptor.clone())),
        },
    )
}
pub(crate) fn prepare_original_recipe_process_with_budgets(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    result: novarocks_workload_control::ResultCapacityConfig,
    cpu: Option<&mut crate::query_execution::internal_result_cpu::InternalResultCpuOwner>,
    diagnostics: Duration,
    remote: Duration,
) -> PreparedOriginalRecipeProcess {
    prepare_original_recipe_process_inner(
        data,
        descriptor.clone(),
        history,
        result,
        cpu,
        OriginalRecipeProcessPolicy {
            supervisor: original_recipe_supervisor_config(Some((diagnostics, remote))),
            namespace: novarocks_types::identity::QueryProcessNamespace::new(712),
            observation_budget: None,
            process_observation: Arc::new(CurrentPeer(descriptor.clone())),
        },
    )
}
/// Freeze the original QA issuer namespace and observation policy before
/// constructing its client/Root. Independent issuers never reuse guessed IDs.
pub(crate) fn prepare_original_recipe_process_with_namespace(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    result: novarocks_workload_control::ResultCapacityConfig,
    cpu: Option<&mut crate::query_execution::internal_result_cpu::InternalResultCpuOwner>,
    namespace: novarocks_types::identity::QueryProcessNamespace,
    observation: (Duration, Duration),
) -> PreparedOriginalRecipeProcess {
    prepare_original_recipe_process_inner(
        data,
        descriptor.clone(),
        history,
        result,
        cpu,
        OriginalRecipeProcessPolicy {
            supervisor: original_recipe_supervisor_config(Some(observation)),
            namespace,
            observation_budget: None,
            process_observation: Arc::new(CurrentPeer(descriptor.clone())),
        },
    )
}

fn original_recipe_supervisor_config(
    budgets: Option<(Duration, Duration)>,
) -> novarocks_query_application::coordination::LogicalExecutionSupervisorConfig {
    use novarocks_query_application::coordination::{
        LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig,
    };
    let config = LogicalExecutionSupervisorConfig::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        LogicalExecutionRowsConfig::new(
            NonZeroUsize::MIN,
            std::num::NonZeroU32::new(1).unwrap(),
            Duration::from_secs(5),
        ),
    );
    match budgets {
        Some((diagnostics, remote)) => config
            .with_diagnostic_timeout(diagnostics)
            .with_remote_cleanup_timeout(remote),
        None => config,
    }
}

/// All caller policy and exact external observation ports freeze before
/// process queues, QA issuer/client or Root exist. No lookup or fallback occurs.
pub(crate) struct OriginalRecipeProcessPolicy {
    pub(crate) supervisor:
        novarocks_query_application::coordination::LogicalExecutionSupervisorConfig,
    pub(crate) namespace: novarocks_types::identity::QueryProcessNamespace,
    pub(crate) observation_budget:
        Option<crate::task_execution::host::observations::ObservationBudget>,
    pub(crate) process_observation:
        novarocks_query_application::api::BackendProcessObservationService,
}
pub(crate) fn prepare_original_recipe_process_with_policy(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    result: novarocks_workload_control::ResultCapacityConfig,
    cpu: Option<&mut crate::query_execution::internal_result_cpu::InternalResultCpuOwner>,
    policy: OriginalRecipeProcessPolicy,
) -> PreparedOriginalRecipeProcess {
    prepare_original_recipe_process_inner(data, descriptor, history, result, cpu, policy)
}

fn prepare_original_recipe_process_inner(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    result: novarocks_workload_control::ResultCapacityConfig,
    cpu: Option<&mut crate::query_execution::internal_result_cpu::InternalResultCpuOwner>,
    policy: OriginalRecipeProcessPolicy,
) -> PreparedOriginalRecipeProcess {
    use crate::native::task_transport::NativeTaskOperationSink;
    use crate::query_execution::native_execution_adapter::FrontendLogicalExecutionNativePort;
    use novarocks_query_application::coordination::LogicalExecutionSupervisor;
    use novarocks_types::FrontendProcessId;
    use novarocks_workload_control::{
        ControlStorageConfig, PollRoleSourceBacking, WorkloadConfig, WorkloadControl,
        joint_admission::ResponsibilityConfig,
    };
    use std::alloc::Layout;
    let OriginalRecipeProcessPolicy {
        supervisor: config,
        namespace,
        observation_budget: fixture_observation_budget,
        process_observation,
    } = policy;
    let count = 8;
    let handle = tokio::runtime::Handle::current();
    let clock = Arc::new(ManualClock::new());
    let process = Arc::new(manual_process_with_observation_budget(
        data,
        clock.clone(),
        process_observation,
        Some(history.clone()),
        fixture_observation_budget.unwrap_or_else(
            crate::task_execution::host::observations::ObservationBudget::for_task_attempt,
        ),
    ));
    let fixture = FrontendFixture::from_original_process(process.clone(), clock.clone(), history);
    let roles = [LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap()];
    let send_geometry =
        NativeTaskOperationSink::proposal_wait_source_geometry(TransportBudget::DEFAULT).unwrap();
    let send_backing = Layout::array::<u8>(send_geometry.resident_bytes().unwrap()).unwrap();
    let cohort_backing = crate::query_execution::native_execution_adapter::source_cohort::process_source_backing_layouts().unwrap();
    let storage = ControlStorageConfig::new(count, &roles).with_process_source_backing(
        PollRoleSourceBacking::from_layouts(&cohort_backing, &[send_backing]).unwrap(),
    );
    let mut workload = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, joint) = workload
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    workload.configure_control_storage(storage).unwrap();
    let capacity = workload.configure_result_capacity(result).unwrap();
    let (supervisor, client) = LogicalExecutionSupervisor::try_with_clock(
        &mut workload,
        0,
        NonZeroUsize::new(7).unwrap(),
        handle.clone(),
        Arc::new(FrontendLogicalExecutionNativePort),
        namespace,
        FrontendProcessId::new_v7(),
        config,
        clock.clone(),
    )
    .unwrap();
    if let Some(cpu) = cpu {
        cpu.bind_for_test_before_ready(&mut workload).unwrap();
    }
    workload.mark_ready().unwrap();
    PreparedOriginalRecipeProcess {
        fixture,
        process,
        clock,
        joint,
        workload,
        supervisor,
        client,
        capacity,
        handle,
        descriptor,
    }
}

// Statistics keeps its original SQL/compiler development-test eligibility.
#[cfg(all(test, novarocks_dev_controls))]
pub(crate) use crate::control_runtime::installed_subscription_tests::statistics_recipe::{
    StatisticsRecipeCase, TypedStatisticsCustody, admit_statistics_originals, drive_typed,
    original_program, original_statistics_body, run_actual_statistics_case,
};

/// The caller supplies the SAME configured process/driver/client. No Root,
/// CPU, P1 or result position is created; the SAME client issues the query identity.
pub(crate) struct OriginalRecipeContext<'a> {
    pub(crate) fixture: &'a mut FrontendFixture,
    pub(crate) joint: &'a mut novarocks_workload_control::joint_admission::JointProgressDriver,
    pub(crate) process: &'a Arc<FrontendAttemptRuntime>,
    pub(crate) data: &'a FrontendDataRuntime,
    pub(crate) clock: &'a Arc<ManualClock>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) descriptor: BackendProcessDescriptor,
    pub(crate) client: novarocks_query_application::api::QueryExecutionClient,
}
/// Concrete caller-owned operands. In a statistics case the consumer/output
/// retain the original decoded CPU/P1 grant; this helper never substitutes it.
pub(crate) struct OriginalRecipeOperands<O> {
    pub(crate) owner: novarocks_workload_control::WorkOwner,
    pub(crate) binding:
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    pub(crate) description: novarocks_query_application::preparation::FrozenExecutionDescription,
    pub(crate) plan: PlanSeal,
    pub(crate) root: RecipeRoot,
    pub(crate) consumer: Arc<dyn novarocks_query_application::api::InternalComputationConsumer>,
    pub(crate) offers:
        crate::query_execution::internal_consumer::attempt_handoff::AttemptHandoffOfferPort,
    pub(crate) output: O,
}
/// Shared original assembly extracted from installed_subscription_tests. Its
/// only admission is the same paid network binding and original handoff/link.
pub(crate) fn install_original_recipe<O>(
    context: OriginalRecipeContext<'_>,
    originals: OriginalRecipeOperands<O>,
) -> (ActualLogicalCase, NativeSourceCohort, O) {
    install_original_recipe_with_diagnostics(context, originals, None)
}

pub(crate) fn install_original_recipe_with_diagnostics<O>(
    context: OriginalRecipeContext<'_>,
    originals: OriginalRecipeOperands<O>,
    diagnostics: Option<OriginalRecipeDiagnostics>,
) -> (ActualLogicalCase, NativeSourceCohort, O) {
    install_original_recipe_parts(context, originals, diagnostics, None, None, None)
}

/// SAME actual assembly, using the domain's original issued reservation and
/// original storage. No query identity, source, Ready token or M is rebuilt.
#[cfg(novarocks_dev_controls)]
pub(crate) fn install_original_recipe_with_initial<O>(
    context: OriginalRecipeContext<'_>,
    originals: OriginalRecipeOperands<O>,
    initial: crate::query_execution::logical_execution::InitialComputationReservation,
    storage: Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>,
) -> (ActualLogicalCase, NativeSourceCohort, O) {
    assert_eq!(
        originals.description.recovery(),
        novarocks_coordination_contract::RecoveryMode::NoRecovery
    );
    let original_credentials = initial.credentials();
    original_credentials
        .verify_native_storage(&storage)
        .unwrap();
    let execution = original_credentials.execution();
    let (reserved, transferred) = initial.into_start_parts_for_fixture();
    assert_eq!(reserved.execution(), execution);
    assert!(Arc::ptr_eq(&transferred, &original_credentials));
    // The caller's real typed custody/Session still owns this same credential
    // author and seals it at its original actual terminal boundary.
    drop((transferred, original_credentials));
    install_original_recipe_parts(
        context,
        originals,
        None,
        Some(reserved),
        Some(storage),
        None,
    )
}

fn install_original_recipe_parts<O>(
    context: OriginalRecipeContext<'_>,
    originals: OriginalRecipeOperands<O>,
    diagnostics: Option<OriginalRecipeDiagnostics>,
    reserved: Option<novarocks_query_application::api::ReservedExecutionIdentity>,
    original_credentials: Option<
        Arc<crate::query_execution::lifecycle_plan::AttemptCredentialStorage>,
    >,
    fixture_observation_budget: Option<
        crate::task_execution::host::observations::ObservationBudget,
    >,
) -> (ActualLogicalCase, NativeSourceCohort, O) {
    assert_eq!(reserved.is_some(), original_credentials.is_some());
    let retain_diagnostics = diagnostics.is_some();
    let OriginalRecipeContext {
        fixture,
        joint,
        process,
        data,
        clock,
        handle,
        descriptor,
        client,
    } = context;
    let OriginalRecipeOperands {
        owner,
        binding: result_binding,
        description,
        plan,
        root,
        consumer,
        offers,
        output,
    } = originals;
    assert!(description.matches_plan_seal(plan));
    let scope = owner.scope();
    let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
    let mut binding = scope.begin_control_binding().unwrap();
    for endpoint in [descriptor.endpoint(), descriptor.control_endpoint()] {
        binding
            .push_selected_network(
                transport
                    .selected_network_origin(endpoint.native_endpoint())
                    .unwrap(),
            )
            .unwrap();
    }
    binding.end_change().unwrap();
    let mut admission = binding.submit().unwrap();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
    let Poll::Ready(Ok(mut granted)) =
        Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("original native control binding must grant");
    };
    granted
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    drop((granted, admission));
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
    let window = result_binding.window_alias();
    let authority = result_binding.native_action_authority().unwrap();
    let cohort = crate::query_execution::native_execution_adapter::source_cohort::tests::cohort(
        &scope, 1, joint,
    );
    let reserved = reserved.unwrap_or_else(|| client.reserve_identity().unwrap());
    let execution = reserved.execution();
    let mut handoff = None;
    offers
        .reserve_view()
        .reserve_in(execution, &mut handoff)
        .unwrap();
    let cell = LogicalAttemptBindingCell::new(
        retain_diagnostics,
        Arc::new(
            crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics::default(),
        ),
    );
    let probe = Arc::new(RecipeObservation::default());
    let recipe = ProtocolRecipe::new(RecipeDormant {
        process: Arc::downgrade(process),
        data: data.clone(),
        clock: clock.clone(),
        eligible: [descriptor.process_id()],
        descriptor,
        scope,
        cohort: cohort.clone(),
        authority,
        window,
        logical: cell.clone(),
        handoff,
        original_credentials,
        plan,
        root,
        diagnostics,
        observation: probe.clone(),
        fixture_observation_budget,
        original_exit: None,
    });
    let request = novarocks_query_application::api::QueryExecutionRequest::bind_native(
        description,
        None,
        recipe,
    )
    .with_result_capacity(result_binding)
    .unwrap()
    .with_internal_consumer(consumer)
    .unwrap()
    .with_logical_runtime(
        novarocks_query_application::api::LogicalRuntimeComposition::new(
            Arc::new(
                crate::control_runtime::logical::LogicalRuntimeInstaller::new(
                    process.clone(),
                    handle,
                    cell.clone(),
                ),
            ),
            cell.clone(),
            Some(cell.clone()),
            retain_diagnostics,
        ),
    );
    let logical = fixture.install_logical(
        ObservedLogicalRequest {
            client,
            request,
            reserved,
            owner,
            probe,
            binding: cell,
        },
        (),
    );
    (logical, cohort, output)
}

/// Stable request/receiving slots of the same common four-operation facade.
/// No ScriptExecutor or fixture-issued scheduling identity participates here.
pub(crate) struct ObservedLogicalRequest {
    pub(crate) client: novarocks_query_application::api::QueryExecutionClient,
    pub(crate) request: novarocks_query_application::api::QueryExecutionRequest,
    pub(crate) reserved: novarocks_query_application::api::ReservedExecutionIdentity,
    pub(crate) owner: novarocks_workload_control::WorkOwner,
    pub(crate) probe: Arc<RecipeObservation>,
    pub(crate) binding: Arc<LogicalAttemptBindingCell>,
}
pub(crate) struct ActualLogicalCase {
    pub(crate) input: Option<novarocks_query_application::coordination::OriginalLogicalStartInput>,
    pub(crate) owner: Option<novarocks_workload_control::WorkOwner>,
    pub(crate) ready: Option<novarocks_query_application::api::QueryExecutionFuture>,
    pub(crate) exit:
        Option<novarocks_query_application::coordination::LogicalExecutionExitObserver>,
    pub(crate) probe: Arc<RecipeObservation>,
    pub(crate) binding: Arc<LogicalAttemptBindingCell>,
}
impl super::InstallFixture<()> for ObservedLogicalRequest {
    type Case = ActualLogicalCase;
    fn install(self, _fixture: &mut FrontendFixture, _: ()) -> Self::Case {
        let input = self
            .client
            .prepare_observed_start(self.request, Some(self.reserved))
            .unwrap_or_else(|(error, _, _)| panic!("original observed request rejected: {error}"));
        let mut case = ActualLogicalCase {
            input: Some(input),
            owner: Some(self.owner),
            ready: None,
            exit: None,
            probe: self.probe,
            binding: self.binding,
        };
        self.client
            .start_observed_in(
                &mut case.input,
                &mut case.owner,
                &mut case.ready,
                &mut case.exit,
            )
            .unwrap()
            .finish()
            .unwrap();
        case
    }
}
impl super::ObserveFixture for ActualLogicalCase {
    type Observation = Option<Arc<InstalledAttemptHost>>;
    fn observe(&self, _fixture: &FrontendFixture) -> Self::Observation {
        self.probe.installed.lock().unwrap().clone()
    }
}
impl
    super::ActivateFixture<
        &mut novarocks_workload_control::joint_admission::JointProgressDriver,
        bool,
    > for ActualLogicalCase
{
    type Attempt = Arc<InstalledAttemptHost>;
    async fn activate(
        &self,
        fixture: &mut FrontendFixture,
        joint: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
        unstarted: bool,
    ) -> Self::Attempt {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut held = None;
        loop {
            crate::query_execution::native_execution_adapter::source_cohort::tests::drive(joint);
            if unstarted {
                let original = &fixture.state().process.original;
                original
                    .runtime
                    .advance_time(original.clock.now(), 64)
                    .unwrap();
                for _ in 0..64 {
                    if !original
                        .runtime
                        .poll_one_with_clock(original.clock.as_ref())
                        .unwrap()
                    {
                        break;
                    }
                }
                if let Some(turn) = original.runtime.claim_host_actor() {
                    let logical = self.binding.bound().ok();
                    if logical
                        .as_ref()
                        .is_some_and(|p| turn.matches_registration(&p.registration))
                    {
                        original.host.drive(original.clock.now(), turn);
                    } else {
                        assert!(held.is_none(), "only the original AP Host may be held");
                        held = Some(turn);
                    }
                }
            } else {
                fixture
                    .progress(ProgressSelection::All, ProgressBudget::default())
                    .await;
            }
            if let Some(installed) = fixture.observe(self) {
                if unstarted {
                    let original = &fixture.state().process.original;
                    let snapshot = original
                        .runtime
                        .snapshot(installed.fixture_sources.0)
                        .unwrap();
                    assert_eq!(snapshot.inputs, 0);
                    assert_eq!(snapshot.batches, 0);
                    assert_eq!(
                        original
                            .runtime
                            .pending_timer_input(installed.fixture_sources.0)
                            .unwrap(),
                        None
                    );
                    installed.fixture_exit.request_close().unwrap();
                    if let Some(turn) = held.take() {
                        turn.release(true).unwrap();
                    }
                }
                return installed;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "actual QA/AP installation did not publish its original Source"
            );
            tokio::task::yield_now().await;
        }
    }
}

/// SAME concrete assembly used by the installed Source regressions and P14.
pub(crate) struct OriginalFixture {
    pub(crate) fixture: FrontendFixture,
    pub(crate) logical: ActualLogicalCase,
    pub(crate) process: Arc<FrontendAttemptRuntime>,
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) joint: novarocks_workload_control::joint_admission::JointProgressDriver,
    pub(crate) workload: novarocks_workload_control::WorkloadControl,
    pub(crate) supervisor: novarocks_query_application::coordination::LogicalExecutionSupervisor,
    pub(crate) scope: novarocks_workload_control::WorkScope,
    pub(crate) business: novarocks_workload_control::BusinessPermit,
    pub(crate) capacity: novarocks_workload_control::ResultCapacityHandle,
    pub(crate) window: novarocks_workload_control::ResultWindowGrant,
    pub(crate) cohort:
        crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort,
    pub(crate) requester: novarocks_workload_control::WorkCancellationRequester,
    pub(crate) output: crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
}
pub(crate) fn install_original_fixture(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
) -> OriginalFixture {
    install_original_fixture_shape(data, descriptor, history, false)
}
pub(crate) fn install_original_fixture_shape(
    data: &FrontendDataRuntime,
    descriptor: BackendProcessDescriptor,
    history: novarocks_control_runtime::history::HistoryCapture,
    three_tasks: bool,
) -> OriginalFixture {
    let PreparedOriginalRecipeProcess {
        mut fixture,
        process,
        clock,
        mut joint,
        workload,
        supervisor,
        client,
        capacity,
        handle,
        descriptor,
    } = prepare_original_recipe_process(
        data,
        descriptor,
        history,
        novarocks_workload_control::ResultCapacityConfig::V1,
        None,
    );
    let original = workload
        .try_begin_root(novarocks_workload_control::WorkRequest::new(
            novarocks_workload_control::WorkClass::Query,
        ))
        .unwrap();
    let scope = original.owner.scope();
    let requester = original.owner.cancellation_requester();
    let window = capacity
        .try_acquire(
            &scope,
            novarocks_workload_control::ResultWindowClass::Internal,
        )
        .unwrap();
    let (description, plan) = count_description();
    let (consumer, offers, output) =
        crate::query_execution::internal_consumer::profile::profile_computation_consumer(
            &description,
            plan,
        )
        .unwrap();
    let (logical, cohort, output) = install_original_recipe(
        OriginalRecipeContext {
            fixture: &mut fixture,
            joint: &mut joint,
            process: &process,
            data,
            clock: &clock,
            handle,
            descriptor,
            client,
        },
        OriginalRecipeOperands {
            owner: original.owner,
            binding: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(&scope, window.retain_alias()).unwrap(),
            description,
            plan,
            root: if three_tasks {
                RecipeRoot::EmptyCountOnly.with_three_task_protocol_chain(plan, [2, 3])
            } else {
                RecipeRoot::EmptyCountOnly
            },
            consumer,
            offers,
            output,
        },
    );
    OriginalFixture {
        fixture,
        logical,
        process,
        clock,
        joint,
        workload,
        supervisor,
        scope,
        business: original.business,
        capacity,
        window,
        cohort,
        requester,
        output,
    }
}
