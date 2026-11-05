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

//! Frozen join metadata and the installed E/F/Q/T factories.
//!
//! E captures evaluated roots and payload pages without constructing lookup
//! tables. Independent F sources poll the original K task graph and join-owned
//! directory/CSR/bitmap work. Q evaluates original compiled roots over stable
//! physical rows; independent T sources publish retained build rows after the
//! original Q completion barrier. Every participant has an exact installed slot.
//!
//! Canonical output roles and signed null extension come from LocalCompiler.
//! SharedBuild is a local layout, independent of the network distribution.
//! Runtime-filter producers receive each key once from the actual E participants.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray};
use novarocks_local_program::{
    JoinDistributionMode, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind,
};

use super::build_requirements::{BuildComponentRequirements, required_build_components};
use super::native_runtime_filter::{
    NativeRuntimeFilterProducerFactory, NativeRuntimeFilterProducerSet,
};
use super::typed_gather::JoinGatherError;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::partition_keys::source_key_owner;
use crate::exec::hash_table::{allocation::AggregateAllocator, key_index::BulkWork};
use crate::exec::node::join::JoinType;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, instances, root_value_type,
};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{PreparedInput, RawInput},
    shape::{Transform, TransformKernel},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter::RuntimeFilterProducerFailure;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::decode::ValueConversionContext;

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
    probe_sites: Vec<ProgramExpressionRootSite>,
    build_sites: Vec<ProgramExpressionRootSite>,
    build_key_bindings: Vec<FunctionValueType>,
    probe_key_bindings: Vec<FunctionValueType>,
    eq_null_safe: Vec<bool>,
    build: novarocks_local_program::StaticLayout,
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
            novarocks_local_program::JoinType::LeftMark => JoinType::LeftMark,
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
        if join_type == JoinType::LeftMark
            && (keys.len() != 1
                || !condition.is_null_aware()
                || !condition.scope_keys().is_empty()
                || condition.residual_predicate().is_some()
                || keys[0].null_safe
                || probe_key_bindings
                    .iter()
                    .chain(&build_key_bindings)
                    .any(|ty| ty.data_type() != &arrow::datatypes::DataType::Utf8))
        {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "mark join requires its frozen single NULL-aware Utf8 equality key",
            ));
        }
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
            probe_sites,
            build_sites,
            build_key_bindings,
            probe_key_bindings,
            eq_null_safe: keys.iter().map(|key| key.null_safe).collect(),
            build: right_layout.clone(),
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

mod finalize;
mod frame;
mod input;
mod phased_probe;
mod tail;

pub(crate) use finalize::CompiledJoinFinalizeSourceFactory;
pub(crate) use frame::CompiledJoinFrame;
#[cfg(test)]
pub(crate) use frame::{frame_fixture_chunks, frame_fixture_inputs};
pub(crate) use input::CompiledJoinInputSinkFactory;
pub(crate) use phased_probe::CompiledJoinProbeProcessorFactory;

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

pub(crate) use tail::CompiledJoinTailSourceFactory;
