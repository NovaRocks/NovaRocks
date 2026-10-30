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

//! Compiled hash join: the build sink and probe processor of one compiled
//! LocalProgram Join node.
//!
//! Keys and the residual are evaluated only through per-driver compiled root
//! instances. A key root reads its own side's batch as the operator receives
//! it; the residual reads the gathered candidate pairs in the join-scope
//! layout. The hash map, build store, gather and match-decision kernels work
//! on the evaluated arrays; nothing reads a legacy expression arena.
//!
//! The local orientation is fixed by the compiler: the probe is the program's
//! left input and the build its right input. One build driver publishes one
//! artifact that every probe driver of the instance shares: every physical
//! distribution has already delivered this instance its whole share of both
//! sides. This milestone executes Inner, LeftOuter, LeftSemi, LeftAnti and
//! NullAwareLeftAnti; a kind that must publish unmatched build rows is
//! refused when the pipeline is built.
//!
//! Output batches are exactly the node's frozen canonical layout: a
//! NULL-extended build column is already nullable there, so no schema is
//! patched at runtime. A row data error of a key or of the residual over a
//! candidate pair is a required error.
//!
//! The build driver also feeds the join's membership runtime-filter
//! producers: each evaluated build-key array is submitted once, the producers
//! close after the artifact is published, and any build failure or cancel
//! fails them. The instance's one build driver is their one local partition.

use super::join_hash_map::build_store::BuildStoreChunkProjection;
use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_local_program::{
    JoinDistributionMode, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use super::broadcast_join_shared::BroadcastJoinSharedState;
use super::build_artifact::{BuildView, JoinBuildArtifact};
use super::build_requirements::{
    BuildComponentRequirements, NullKeyRequirement, required_build_components,
};
use super::join_hash_map::build_store::BuildStoreBuilder;
use super::join_hash_map::finalize::is_all_match_one;
use super::join_hash_map::gather::{
    JoinGatherError, MAX_JOIN_OUTPUT_ROWS_PER_BATCH, TypedJoinBatch, filter_probe_typed,
    gather_left_with_null_right_typed, gather_probe_build_batches_typed,
};
use super::join_hash_map::method::{JoinCursorFailure, JoinHashMap, JoinKeyBuild, JoinPairCursor};
use super::join_hash_map::search::JoinSelection;
use super::native_runtime_filter::{
    NativeRuntimeFilterProducerFactory, NativeRuntimeFilterProducerSet,
};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::partition_keys::source_key_owner;
use crate::exec::hash_table::{
    allocation::AggregateAllocator,
    key_index::{BulkWork, PreparedKeyBatch},
};
use crate::exec::node::join::JoinType;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_all, instances, root_value_type,
};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{Transform, TransformKernel},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime_filter::RuntimeFilterProducerFailure;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::decode::ValueConversionContext;

#[derive(Debug)]
struct HashJoinContractFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for HashJoinContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for HashJoinContractFailure {}
impl novarocks_failure_contract::FailureProjection for HashJoinContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::HashJoinExecutionContract.reason(),
        )
    }
}
fn hash_join_contract(detail: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(HashJoinContractFailure(
        novarocks_failure_contract::BoundedDetail::truncating(detail),
    ))
}
#[derive(Debug)]
struct JoinBuildCountOverflow {
    current: usize,
    incoming: usize,
}
impl std::fmt::Display for JoinBuildCountOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hash join build cannot add {} rows to {}",
            self.incoming, self.current
        )
    }
}
impl std::error::Error for JoinBuildCountOverflow {}
impl novarocks_failure_contract::FailureProjection for JoinBuildCountOverflow {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::JoinBuildCountExtent.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Operator),
            dimension: ResourceDimension::Positions,
            requested: u64::try_from(self.incoming).ok(),
            required_growth: u64::try_from(self.incoming).ok(),
            limit: u64::try_from(usize::MAX).ok(),
            live: u64::try_from(self.current).ok(),
            committed: None,
        })
    }
}

/// Runtime role schemas retain their own finite source/new storage holds.
/// They never retain the declaring plan, program or creating factory.
pub(crate) struct PreparedJoinSchemas {
    pub(crate) build: ChunkSchemaRef,
    pub(crate) scope: ChunkSchemaRef,
    pub(crate) output: ChunkSchemaRef,
}

pub(crate) fn prepare_join_schemas(
    factory: &dyn InputConstructionFactory,
    build: &novarocks_local_program::StaticLayout,
    scope: &novarocks_local_program::StaticLayout,
    output: &novarocks_local_program::StaticLayout,
) -> ExecutionResult<PreparedJoinSchemas> {
    let build = factory.create_compiled_chunk_schema(build)?;
    let scope = factory.create_compiled_chunk_schema(scope)?;
    let output = factory.create_compiled_chunk_schema(output)?;
    Ok(PreparedJoinSchemas {
        build,
        scope,
        output,
    })
}

/// The frozen facts of one compiled Join node that both sides share.
pub(crate) struct CompiledHashJoinPlan {
    node: ProgramNodeId,
    join_type: JoinType,
    probe_sites: Vec<ProgramExpressionRootSite>,
    build_sites: Vec<ProgramExpressionRootSite>,
    build_key_bindings: Vec<FunctionValueType>,
    probe_key_bindings: Vec<FunctionValueType>,
    residual: Option<ProgramExpressionRootSite>,
    eq_null_safe: Vec<bool>,
    build: novarocks_local_program::StaticLayout,
    scope: novarocks_local_program::StaticLayout,
    output: novarocks_local_program::StaticLayout,
    requirements: BuildComponentRequirements,
}

impl CompiledHashJoinPlan {
    pub(crate) fn try_new(program: &LocalProgram, node: ProgramNodeId) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled join node {} is absent", node.index()),
            )
        })?;
        let ProgramNodeKind::Join {
            join_type,
            distribution_mode,
            left_layout,
            right_layout,
            join_scope_layout,
            condition,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled node {} is not a hash join", node.index()),
            ));
        };
        validate_join_scope(node, left_layout, right_layout, join_scope_layout)?;
        let join_type = match join_type {
            novarocks_local_program::JoinType::Inner => JoinType::Inner,
            novarocks_local_program::JoinType::LeftOuter => JoinType::LeftOuter,
            novarocks_local_program::JoinType::LeftSemi => JoinType::LeftSemi,
            novarocks_local_program::JoinType::LeftAnti => JoinType::LeftAnti,
            novarocks_local_program::JoinType::NullAwareLeftAnti => JoinType::NullAwareLeftAnti,
            novarocks_local_program::JoinType::RightOuter => JoinType::RightOuter,
            novarocks_local_program::JoinType::FullOuter => JoinType::FullOuter,
            novarocks_local_program::JoinType::RightSemi => JoinType::RightSemi,
            novarocks_local_program::JoinType::RightAnti => JoinType::RightAnti,
        };
        if *distribution_mode != JoinDistributionMode::SharedBuild {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled hash join at local node {} with a local partitioned build is not executable yet",
                    node.index()
                ),
            ));
        }
        let keys = condition.keys();
        if keys.is_empty() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled hash join at local node {} has no exact key pairs",
                    node.index()
                ),
            ));
        }
        let site = |role| ProgramExpressionRootSite::Node { node, role };
        let ordinal = |key: usize| {
            u32::try_from(key).map_err(|_| {
                program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "join key count exceeds u32",
                )
            })
        };
        let mut probe_sites = Vec::with_capacity(keys.len());
        let mut build_sites = Vec::with_capacity(keys.len());
        for key in 0..keys.len() {
            let key = ordinal(key)?;
            probe_sites.push(site(if condition.is_null_aware() {
                ProgramNodeExpressionRole::NaaValueProbeKey { key }
            } else {
                ProgramNodeExpressionRole::JoinProbeKey { key }
            }));
            build_sites.push(site(if condition.is_null_aware() {
                ProgramNodeExpressionRole::NaaValueBuildKey { key }
            } else {
                ProgramNodeExpressionRole::JoinBuildKey { key }
            }));
        }
        let build_key_bindings = build_sites
            .iter()
            .map(|site| root_value_type(program, *site).map_err(ExecutionFailure::from))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let probe_key_bindings = probe_sites
            .iter()
            .map(|site| root_value_type(program, *site).map_err(ExecutionFailure::from))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let residual = condition.residual_predicate().map(|_| {
            site(if condition.is_null_aware() {
                ProgramNodeExpressionRole::NaaEligibility
            } else {
                ProgramNodeExpressionRole::JoinResidual
            })
        });
        let requirements = required_build_components(join_type, residual.is_some(), true, true);
        Ok(Self {
            node,
            join_type,
            probe_sites,
            build_sites,
            build_key_bindings,
            probe_key_bindings,
            residual,
            eq_null_safe: keys.iter().map(|key| key.null_safe).collect(),
            build: right_layout.clone(),
            scope: join_scope_layout.clone(),
            output: graph_node.output_layout().clone(),
            requirements,
        })
    }

    /// The null-safe equality flag of every key pair, in key order.
    pub(crate) fn eq_null_safe(&self) -> &[bool] {
        &self.eq_null_safe
    }

    /// The build-key root of key `ordinal`.
    pub(crate) fn build_site(&self, ordinal: usize) -> Option<ProgramExpressionRootSite> {
        self.build_sites.get(ordinal).copied()
    }
}

mod build;
mod finalize;
mod frame;
mod input;
mod phased_probe;
mod probe;
mod tail;

pub(crate) use build::CompiledHashJoinBuildSinkFactory;
pub(crate) use finalize::CompiledJoinFinalizeSourceFactory;
pub(crate) use frame::CompiledJoinFrame;
#[cfg(test)]
pub(crate) use frame::{frame_fixture_chunks, frame_fixture_inputs};
pub(crate) use input::CompiledJoinInputSinkFactory;
pub(crate) use phased_probe::CompiledJoinProbeProcessorFactory;
pub(crate) use probe::CompiledHashJoinProbeProcessorFactory;

/// The scope retains the probe then build slots and complete frozen fields.
pub(crate) fn validate_join_scope(
    node: ProgramNodeId,
    left: &novarocks_local_program::StaticLayout,
    right: &novarocks_local_program::StaticLayout,
    scope: &novarocks_local_program::StaticLayout,
) -> ExecutionResult<()> {
    let slots = scope
        .slots()
        .iter()
        .eq(left.slots().iter().chain(right.slots()));
    let fields = scope.schema().fields().iter().eq(left
        .schema()
        .fields()
        .iter()
        .chain(right.schema().fields()));
    let complete = left.is_typed()
        && right.is_typed()
        && scope.is_typed()
        && scope.slots().len() == left.slots().len() + right.slots().len()
        && (0..scope.slots().len()).all(|index| {
            let source = if index < left.slots().len() {
                left.value_binding_at(index)
            } else {
                right.value_binding_at(index - left.slots().len())
            };
            match (source, scope.value_binding_at(index)) {
                (Some(source), Some(actual)) => source == actual,
                _ => false,
            }
        });
    if slots && fields && complete {
        Ok(())
    } else {
        Err(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "compiled join at local node {} has a scope other than its left then right layout",
                node.index()
            ),
        ))
    }
}

/// TRUE rows of a Boolean root: FALSE and NULL are both excluded.
pub(crate) fn true_mask(truth: &ArrayRef) -> ExecutionResult<BooleanArray> {
    let truth = truth
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled join predicate is not Boolean",
            )))
        })?;
    Ok(if truth.null_count() == 0 {
        truth.clone()
    } else {
        truth
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect()
    })
}

pub(crate) use tail::CompiledJoinTailSourceFactory;
