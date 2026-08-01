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
//! Hash-aggregation processor for grouped and global aggregate execution.
//!
//! Responsibilities:
//! - Builds and updates group-key hash tables with aggregate kernels over streaming input chunks.
//! - Finalizes in-memory aggregate states into output chunks while tracking memory consumption.
//!
//! Key exported interfaces:
//! - Types: `AggregateProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

pub(crate) mod final_domain;
pub(crate) mod native_runtime_filter;
pub(crate) mod streaming_sink;
pub(crate) mod streaming_source;
pub(crate) mod streaming_state;
pub(crate) mod topn_boundary;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::common::failpoint;
use crate::common::ids::SlotId;
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::agg;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::exec::node::aggregate::{AggFunction, AggregateTopNRuntimeFilterProducerBinding};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime_filter::service::NativeRuntimeFilterExecutionContext;

use crate::exec::hash_table::key_builder::build_group_key_views;
use crate::exec::hash_table::key_column::build_output_schema_from_kernels;
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::port::identity::PartitionId;
use crate::runtime_filter::port::producer::{ProducerFailureReason, RuntimeContractViolationKind};
use crate::runtime_filter::port::value_domain::MembershipValues;
#[cfg(test)]
use crate::runtime_filter::port::value_domain::ValueDomainDelta;
use crate::runtime_filter::service::{FinalDomainCompletionSession, FinalDomainPartitionCommitter};

use self::native_runtime_filter::{
    AggregateTopNProducerSession, AggregateTopNProducerSessionFactory,
};
use self::topn_boundary::{
    AggregateTopNBoundaryBinding, build_topn_boundary_bindings, observe_key_table_group,
    validate_topn_boundary_specs,
};

pub(super) const ENABLE_GROUP_KEY_OPTIMIZATIONS: bool = true;

/// Factory-owned capability for binding one final hash-aggregate driver per local partition.
///
/// The completion session stays here so its owner lease covers the complete factory lifetime.
/// Operators receive only their one-shot partition committer.
pub(crate) struct AggregateFinalDomainSessionBuilder {
    session: FinalDomainCompletionSession,
    declared_dop: i32,
    installed_membership_key_type: DataType,
    max_domain_canonical_bytes: usize,
    #[cfg(test)]
    partition_observer: Option<AggregateFinalDomainPartitionObserver>,
}

#[cfg(test)]
type AggregateFinalDomainPartitionObserver =
    Arc<dyn Fn(PartitionId, &ValueDomainDelta) + Send + Sync>;

struct AggregateFinalDomainPartitionCommitter {
    #[cfg(test)]
    partition_id: PartitionId,
    committer: FinalDomainPartitionCommitter,
}

impl AggregateFinalDomainSessionBuilder {
    pub(crate) fn new(
        session: FinalDomainCompletionSession,
        declared_dop: i32,
        max_domain_canonical_bytes: usize,
    ) -> Result<Self, String> {
        if declared_dop <= 0 {
            let _ = session.fail(ProducerFailureReason::ExecutionFailed);
            return Err(format!(
                "aggregate final-domain session requires positive declared DOP, got {declared_dop}"
            ));
        }
        if max_domain_canonical_bytes == 0 {
            let _ = session.fail(ProducerFailureReason::ExecutionFailed);
            return Err(
                "aggregate final-domain session requires a positive canonical domain budget"
                    .to_string(),
            );
        }
        let installed_membership_key_type = session.membership_key_type().clone();
        Ok(Self {
            session,
            declared_dop,
            installed_membership_key_type,
            max_domain_canonical_bytes,
            #[cfg(test)]
            partition_observer: None,
        })
    }

    #[cfg(test)]
    fn observe_partitions_for_test(
        mut self,
        observer: AggregateFinalDomainPartitionObserver,
    ) -> Self {
        self.partition_observer = Some(observer);
        self
    }

    fn partition(
        &self,
        actual_dop: i32,
        driver_id: i32,
    ) -> Result<AggregateFinalDomainPartitionCommitter, String> {
        if actual_dop != self.declared_dop {
            let _ = self.session.fail(ProducerFailureReason::ExecutionFailed);
            return Err(format!(
                "aggregate final-domain DOP mismatch: declared={} actual={actual_dop}",
                self.declared_dop
            ));
        }
        if driver_id < 0 || driver_id >= actual_dop {
            let _ = self.session.fail(ProducerFailureReason::ExecutionFailed);
            return Err(format!(
                "aggregate final-domain driver id is outside the actual DOP: driver_id={driver_id} dop={actual_dop}"
            ));
        }
        let partition_id = PartitionId::new(driver_id as u32);
        let committer = self
            .session
            .partition(partition_id)
            .map_err(|error| {
                format!(
                    "aggregate final-domain partition acquisition failed for driver_id={driver_id}: {error}"
                )
            })?;
        Ok(AggregateFinalDomainPartitionCommitter {
            #[cfg(test)]
            partition_id,
            committer,
        })
    }

    fn fail(&self) {
        let _ = self.session.fail(ProducerFailureReason::ExecutionFailed);
    }
}

pub(super) fn build_agg_views<'a>(
    kernels: &[agg::AggKernelEntry],
    functions: &[AggFunction],
    arrays: &'a [Option<ArrayRef>],
) -> Result<Vec<agg::AggInputView<'a>>, String> {
    if arrays.len() != kernels.len() || arrays.len() != functions.len() {
        return Err("aggregate arrays length mismatch".to_string());
    }
    let mut views = Vec::with_capacity(kernels.len());
    for idx in 0..kernels.len() {
        let array = arrays
            .get(idx)
            .ok_or_else(|| "aggregate input missing".to_string())?;
        let view = if functions[idx].input_is_intermediate {
            kernels[idx].build_merge_view(array)?
        } else {
            kernels[idx].build_input_view(array)?
        };
        views.push(view);
    }
    Ok(views)
}

pub(super) fn align_schema_with_arrays(
    schema: &SchemaRef,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<SchemaRef, String> {
    if schema.fields().len() != arrays.len() {
        return Err(format!(
            "{context} schema/array length mismatch: schema_fields={} arrays={}",
            schema.fields().len(),
            arrays.len()
        ));
    }
    for (idx, (field, array)) in schema.fields().iter().zip(arrays.iter()).enumerate() {
        if field.data_type() != array.data_type() {
            return Err(format!(
                "{context} type mismatch at column {idx}: descriptor={:?} actual={:?}",
                field.data_type(),
                array.data_type()
            ));
        }
    }
    Ok(Arc::clone(schema))
}

pub(super) fn is_compatible_aggregate_data_type(expected: &DataType, actual: &DataType) -> bool {
    expected == actual
}

pub(super) fn is_compatible_aggregate_group_data_type(
    expected: &DataType,
    actual: &DataType,
) -> bool {
    if matches!(
        (expected, actual),
        (
            DataType::Utf8,
            DataType::Dictionary(key, value)
        ) if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
    ) {
        return true;
    }
    if matches!(
        (expected, actual),
        (
            DataType::LargeUtf8,
            DataType::Dictionary(key, value)
        ) if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::LargeUtf8
    ) {
        return true;
    }
    expected == actual
}

pub(super) fn normalize_aggregate_group_arrays(
    expected: &[DataType],
    arrays: Vec<ArrayRef>,
) -> Result<Vec<ArrayRef>, String> {
    if expected.len() != arrays.len() {
        return Err("group by type length mismatch".to_string());
    }
    expected
        .iter()
        .zip(arrays)
        .enumerate()
        .map(|(idx, (expected_type, array))| {
            let actual_type = array.data_type();
            if expected_type == actual_type
                || is_compatible_aggregate_group_data_type(expected_type, actual_type)
            {
                return Ok(array);
            }
            check_exact(expected_type, actual_type).map_err(|_| {
                format!(
                    "group by type mismatch at {}: expected {:?}, got {:?}",
                    idx, expected_type, actual_type
                )
            })?;
            retag_column(&array, expected_type).map_err(|mismatch| {
                format!(
                    "retag aggregate group by column {} to target type {:?} failed: {:?}",
                    idx, expected_type, mismatch
                )
            })
        })
        .collect()
}

pub(super) fn aggregate_accepts_encoded_group_column(
    arena: &ExprArena,
    group_by: &[ExprId],
    functions: &[AggFunction],
    slot_id: SlotId,
    data_type: &DataType,
) -> bool {
    if group_by.len() != 1 {
        return false;
    }
    if !matches!(
        data_type,
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int32
                && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
    ) {
        return false;
    }
    if !matches!(arena.node(group_by[0]), Some(ExprNode::SlotId(group_slot)) if *group_slot == slot_id)
    {
        return false;
    }
    !functions.iter().any(|function| {
        function
            .inputs
            .iter()
            .any(|expr| expr_references_slot(arena, *expr, slot_id))
    })
}

fn expr_references_slot(arena: &ExprArena, expr: ExprId, slot_id: SlotId) -> bool {
    let Some(node) = arena.node(expr) else {
        return false;
    };
    match node {
        ExprNode::Literal(_) => false,
        ExprNode::SlotId(slot) => *slot == slot_id,
        ExprNode::ArrayExpr { elements } | ExprNode::StructExpr { fields: elements } => elements
            .iter()
            .any(|child| expr_references_slot(arena, *child, slot_id)),
        ExprNode::LambdaFunction {
            body,
            common_sub_exprs,
            ..
        } => {
            expr_references_slot(arena, *body, slot_id)
                || common_sub_exprs
                    .iter()
                    .any(|(_, child)| expr_references_slot(arena, *child, slot_id))
        }
        ExprNode::DictDecode { child, .. }
        | ExprNode::Cast(child)
        | ExprNode::CastTime(child)
        | ExprNode::CastTimeFromDatetime(child)
        | ExprNode::Not(child)
        | ExprNode::IsNull(child)
        | ExprNode::IsNotNull(child)
        | ExprNode::Clone(child) => expr_references_slot(arena, *child, slot_id),
        ExprNode::Add(left, right)
        | ExprNode::Sub(left, right)
        | ExprNode::Mul(left, right)
        | ExprNode::Div(left, right)
        | ExprNode::Mod(left, right)
        | ExprNode::Eq(left, right)
        | ExprNode::EqForNull(left, right)
        | ExprNode::Ne(left, right)
        | ExprNode::Lt(left, right)
        | ExprNode::Le(left, right)
        | ExprNode::Gt(left, right)
        | ExprNode::Ge(left, right)
        | ExprNode::And(left, right)
        | ExprNode::Or(left, right) => {
            expr_references_slot(arena, *left, slot_id)
                || expr_references_slot(arena, *right, slot_id)
        }
        ExprNode::In { child, values, .. } => {
            expr_references_slot(arena, *child, slot_id)
                || values
                    .iter()
                    .any(|value| expr_references_slot(arena, *value, slot_id))
        }
        ExprNode::Case { children, .. } => children
            .iter()
            .any(|child| expr_references_slot(arena, *child, slot_id)),
        ExprNode::FunctionCall { args, .. } => args
            .iter()
            .any(|child| expr_references_slot(arena, *child, slot_id)),
    }
}

/// Factory that constructs aggregate processors backed by group-key hash tables and aggregate kernels.
pub struct AggregateProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    output_intermediate: bool,
    direct_input: bool,
    output_chunk_schema: ChunkSchemaRef,
    runtime_filter_execution: AggregateRuntimeFilterExecution,
    native_topn_session_factory: Option<Arc<AggregateTopNProducerSessionFactory>>,
    final_domain_session: Option<AggregateFinalDomainSessionBuilder>,
    final_domain_shape_error: Option<String>,
    #[cfg(test)]
    fail_output_construction: bool,
}

#[derive(Clone)]
struct AggregateRuntimeFilterExecution {
    topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
}

impl AggregateProcessorFactory {
    pub(crate) fn new_native(
        node_id: i32,
        arena: Arc<ExprArena>,
        group_by: Vec<ExprId>,
        functions: Vec<AggFunction>,
        output_intermediate: bool,
        direct_input: bool,
        output_chunk_schema: ChunkSchemaRef,
        topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
        runtime_filter_context: Option<NativeRuntimeFilterExecutionContext>,
        local_partition_count: i32,
        final_domain_session: Option<AggregateFinalDomainSessionBuilder>,
    ) -> Result<Self, String> {
        let name = if node_id >= 0 {
            format!("AGGREGATE (id={node_id})")
        } else {
            "AGGREGATE".to_string()
        };
        validate_topn_boundary_specs(&topn_producers).map_err(|error| error.to_string())?;
        let final_domain_shape_error = final_domain_session.as_ref().and_then(|session| {
            let error = if output_intermediate || !direct_input {
                Some(
                    "aggregate final-domain session may bind only to a merge/final factory"
                        .to_string(),
                )
            } else if group_by.len() != 1 {
                Some(format!(
                    "aggregate final-domain session requires exactly one group key, got {}",
                    group_by.len()
                ))
            } else if functions
                .iter()
                .any(|function| !function.input_is_intermediate)
            {
                Some(
                    "aggregate final-domain session requires merge-stage aggregate functions"
                        .to_string(),
                )
            } else {
                let key_type = arena
                    .data_type(group_by[0])
                    .expect("validated aggregate group expression id");
                if MembershipValues::empty_for_data_type(key_type).is_none() {
                    Some(format!(
                        "unsupported aggregate final-domain membership key type: {key_type:?}"
                    ))
                } else if key_type != &session.installed_membership_key_type {
                    Some(format!(
                        "aggregate final-domain key type mismatch: installed={:?} aggregate={key_type:?}",
                        session.installed_membership_key_type
                    ))
                } else {
                    None
                }
            };
            if error.is_some() {
                session.fail();
            }
            error
        });
        let native_topn_session_factory = if topn_producers.is_empty() {
            None
        } else {
            let context = runtime_filter_context.ok_or_else(|| {
                format!(
                    "native aggregate TopN producer binding_id={} requires an installed runtime-filter context",
                    topn_producers[0].binding_id
                )
            })?;
            Some(Arc::new(AggregateTopNProducerSessionFactory::from_plan(
                &topn_producers,
                &context,
                local_partition_count,
            )?))
        };
        Ok(Self {
            name,
            arena,
            group_by,
            functions,
            output_intermediate,
            direct_input,
            output_chunk_schema,
            runtime_filter_execution: AggregateRuntimeFilterExecution { topn_producers },
            native_topn_session_factory,
            final_domain_session,
            final_domain_shape_error,
            #[cfg(test)]
            fail_output_construction: false,
        })
    }

    #[cfg(test)]
    fn fail_output_construction_for_test(&mut self) {
        self.fail_output_construction = true;
    }
}

impl OperatorFactory for AggregateProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut final_domain_bind_error = self.final_domain_shape_error.clone();
        let final_domain_committer = if final_domain_bind_error.is_none() {
            self.final_domain_session.as_ref().and_then(|session| {
                match session.partition(dop, driver_id) {
                    Ok(committer) => Some(committer),
                    Err(error) => {
                        final_domain_bind_error = Some(error);
                        None
                    }
                }
            })
        } else {
            None
        };
        let (native_topn_session, native_topn_bind_error) =
            match self.native_topn_session_factory.as_ref() {
                Some(factory) => match factory.create_for_driver(dop, driver_id) {
                    Ok(session) => (Some(session), None),
                    Err(error) => (None, Some(error)),
                },
                None => (None, None),
            };
        Box::new(AggregateProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            group_by: self.group_by.clone(),
            functions: self.functions.clone(),
            key_table: None,
            state_arena: agg::AggStateArena::new(64 * 1024),
            group_states: Vec::new(),
            state_ptrs: Vec::new(),
            kernels: None,
            output_intermediate: self.output_intermediate,
            direct_input: self.direct_input,
            initialized: false,
            data_initialized: false,
            pending_output: None,
            finishing: false,
            finalized: false,
            finished: false,
            output_schema: None,
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            observed_group_key_nullable: vec![false; self.group_by.len()],
            profile_initialized: false,
            profiles: None,
            key_table_mem_tracker: None,
            runtime_filter_execution: self.runtime_filter_execution.clone(),
            topn_rf_rows_since_publish: 0,
            topn_boundary_bindings: Vec::new(),
            native_topn_session,
            native_topn_bind_error,
            final_domain_committer,
            final_domain_session_bound: self.final_domain_session.is_some(),
            final_domain_bind_error,
            max_domain_canonical_bytes: self
                .final_domain_session
                .as_ref()
                .map(|session| session.max_domain_canonical_bytes),
            #[cfg(test)]
            final_domain_partition_observer: self
                .final_domain_session
                .as_ref()
                .and_then(|session| session.partition_observer.clone()),
            #[cfg(test)]
            fail_output_construction: self.fail_output_construction,
        })
    }

    #[cfg(test)]
    fn native_aggregate_topn_producers(&self) -> &[AggregateTopNRuntimeFilterProducerBinding] {
        &self.runtime_filter_execution.topn_producers
    }
}

struct AggregateProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    key_table: Option<KeyTable>,
    state_arena: agg::AggStateArena,
    group_states: Vec<agg::AggStatePtr>,
    state_ptrs: Vec<agg::AggStatePtr>,
    kernels: Option<agg::AggKernelSet>,
    output_intermediate: bool,
    direct_input: bool,
    initialized: bool,
    data_initialized: bool,
    pending_output: Option<Chunk>,
    finishing: bool,
    finalized: bool,
    finished: bool,
    output_schema: Option<SchemaRef>,
    output_chunk_schema: ChunkSchemaRef,
    observed_group_key_nullable: Vec<bool>,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    key_table_mem_tracker: Option<Arc<MemTracker>>,
    runtime_filter_execution: AggregateRuntimeFilterExecution,
    topn_rf_rows_since_publish: usize,
    topn_boundary_bindings: Vec<AggregateTopNBoundaryBinding>,
    native_topn_session: Option<AggregateTopNProducerSession>,
    native_topn_bind_error: Option<String>,
    final_domain_committer: Option<AggregateFinalDomainPartitionCommitter>,
    final_domain_session_bound: bool,
    final_domain_bind_error: Option<String>,
    max_domain_canonical_bytes: Option<usize>,
    #[cfg(test)]
    final_domain_partition_observer: Option<AggregateFinalDomainPartitionObserver>,
    #[cfg(test)]
    fail_output_construction: bool,
}

impl Operator for AggregateProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let arena = MemTracker::new_child("AggStateArena", &tracker);
        self.state_arena.set_mem_tracker(Arc::clone(&arena));

        let key_table = MemTracker::new_child("KeyTable", &tracker);
        if let Some(table) = self.key_table.as_mut() {
            table.set_mem_tracker(Arc::clone(&key_table));
        }
        self.key_table_mem_tracker = Some(key_table);
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        if let Some(error) = self.final_domain_bind_error.clone() {
            self.fail_final_domain();
            return Err(error);
        }
        let result = self.init_from_plan();
        if result.is_err() {
            self.fail_final_domain();
        }
        result
    }

    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if let Some(error) = self.native_topn_bind_error.take() {
            return Err(error);
        }
        if let Some(session) = self.native_topn_session.as_mut() {
            session.bind()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        let _ = self.fail_native_topn_producers(ProducerFailureReason::Cancelled);
    }

    fn on_driver_failure(&mut self) {
        let _ = self.fail_native_topn_producers(ProducerFailureReason::ExecutionFailed);
    }

    fn close(&mut self) -> Result<(), String> {
        self.fail_native_topn_producers(ProducerFailureReason::ExecutionFailed)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl AggregateProcessorOperator {
    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;
        let grouping_keys = self.group_by.len();
        let funcs = self
            .functions
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("GroupingKeys", format!("{grouping_keys}"));
            profile.common.add_info_string("AggregateFunctions", funcs);
        }
    }

    fn rebuild_output_schema(&mut self, group_key_nullable: Option<&[bool]>) -> Result<(), String> {
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        let key_columns = self
            .key_table
            .as_ref()
            .map(|table| table.key_columns())
            .unwrap_or(&[]);
        self.output_schema = Some(build_output_schema_from_kernels(
            key_columns,
            &kernels.entries,
            self.output_intermediate,
            &self.output_chunk_schema,
            group_key_nullable,
        )?);
        Ok(())
    }

    fn try_submit_native_topn_bound(&mut self) -> Result<(), String> {
        const PUBLISH_THRESHOLD: usize = 4096;

        let Some(session) = self.native_topn_session.as_mut() else {
            return Ok(());
        };
        if self.topn_rf_rows_since_publish < PUBLISH_THRESHOLD {
            return Ok(());
        }
        session.submit_pending(&mut self.topn_boundary_bindings)?;
        self.topn_rf_rows_since_publish = 0;
        Ok(())
    }

    fn finish_native_topn_producers(&mut self) -> Result<(), String> {
        let Some(session) = self.native_topn_session.as_mut() else {
            return Ok(());
        };
        session.finish(&mut self.topn_boundary_bindings)
    }

    fn fail_native_topn_producers(&mut self, reason: ProducerFailureReason) -> Result<(), String> {
        let Some(session) = self.native_topn_session.as_mut() else {
            return Ok(());
        };
        session.fail(reason)
    }

    fn process(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }
        self.init_profile_if_needed();

        if chunk.is_empty() && chunk.schema().fields().is_empty() {
            return Ok(None);
        }

        let group_arrays = self.eval_group_by_arrays(&chunk)?;
        let group_arrays =
            normalize_aggregate_group_arrays(&self.expected_group_types()?, group_arrays)?;
        let agg_arrays = self.eval_agg_arrays(&chunk)?;

        self.ensure_data_initialized(&group_arrays, &agg_arrays)
            .map_err(|e| e.to_string())?;
        self.refresh_output_schema_for_group_arrays(&group_arrays)
            .map_err(|e| e.to_string())?;

        if chunk.is_empty() {
            return Ok(None);
        }

        if !self.group_by.is_empty() {
            let failpoint_name = if self.functions.is_empty() {
                failpoint::AGG_HASH_SET_BAD_ALLOC
            } else {
                failpoint::AGGREGATE_BUILD_HASH_MAP_BAD_ALLOC
            };
            failpoint::maybe_error(
                failpoint_name,
                "Mem usage has exceed the limit of BE: BE:10004",
            )?;
        }

        let num_rows = chunk.len();
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .counter_add_unit("InputRowCount", num_rows as i64);
        }
        if self.group_by.is_empty() {
            self.ensure_scalar_group().map_err(|e| e.to_string())?;
            let state_ptr = *self
                .group_states
                .first()
                .ok_or_else(|| "aggregate scalar state missing".to_string())?;
            self.state_ptrs.clear();
            self.state_ptrs.resize(num_rows, state_ptr);
            let kernels = self
                .kernels
                .as_ref()
                .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
            let agg_views = build_agg_views(&kernels.entries, &self.functions, &agg_arrays)
                .map_err(|e| e.to_string())?;
            for (idx, (kernel, view)) in kernels.entries.iter().zip(agg_views.iter()).enumerate() {
                if self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false)
                {
                    kernel
                        .merge_batch(&self.state_ptrs, view)
                        .map_err(|e| e.to_string())?;
                } else {
                    kernel
                        .update_batch(&self.state_ptrs, view)
                        .map_err(|e| e.to_string())?;
                }
            }
            return Ok(None);
        }

        let key_views = build_group_key_views(&group_arrays).map_err(|e| e.to_string())?;
        let mut key_table = self
            .key_table
            .take()
            .ok_or_else(|| "aggregate key table missing".to_string())?;
        let result: Result<(), String> = (|| {
            let mut group_ids = Vec::with_capacity(num_rows);
            match key_table.key_strategy() {
                GroupKeyStrategy::Serialized => {
                    let rows_result = key_table.build_rows(&group_arrays);
                    let fallback_rows = match &rows_result {
                        Ok(_) => None,
                        Err(err) if err.contains("row converter not initialized") => Some(
                            key_table
                                .build_rows_fallback(&group_arrays)
                                .map_err(|e| e.to_string())?,
                        ),
                        Err(err) => return Err(err.to_string()),
                    };
                    let rows = rows_result.ok();
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                        let row_bytes = if let Some(rows) = rows.as_ref() {
                            rows.row(row).data()
                        } else {
                            fallback_rows
                                .as_ref()
                                .and_then(|all| all.get(row))
                                .map(|v| v.as_slice())
                                .ok_or_else(|| {
                                    format!(
                                        "fallback serialized group row missing at row={} (rows={})",
                                        row, num_rows
                                    )
                                })?
                        };
                        let lookup = key_table
                            .find_or_insert_from_row(&key_views, row, row_bytes, hash)
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup, &group_arrays, row)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::Scalar => {
                    return Err("group key strategy Scalar is invalid for group by".to_string());
                }
                GroupKeyStrategy::OneNumber => {
                    let view = key_views
                        .first()
                        .ok_or_else(|| "one number key view missing".to_string())?;
                    let hashes = key_table
                        .build_one_number_hashes(view, num_rows)
                        .map_err(|e| e.to_string())?;
                    for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                        let lookup = key_table
                            .find_or_insert_one_number(view, row, hash)
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup, &group_arrays, row)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::OneString => {
                    let view = key_views
                        .first()
                        .ok_or_else(|| "one string key view missing".to_string())?;
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                        let lookup = key_table
                            .find_or_insert_one_string_like(view, row, hash)
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup, &group_arrays, row)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::FixedSize => {
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                        let lookup = key_table
                            .find_or_insert_fixed_size(&key_views, row, hash)
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup, &group_arrays, row)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::CompressedFixed => {
                    let keys = key_table
                        .build_compressed_flags(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    let mut rows_opt = None;
                    for (row, (key, hash)) in keys
                        .iter()
                        .copied()
                        .zip(hashes.iter().copied())
                        .enumerate()
                        .take(num_rows)
                    {
                        let lookup = if key {
                            key_table
                                .find_or_insert_compressed(&key_views, row, hash)
                                .map_err(|e| e.to_string())?
                        } else {
                            if rows_opt.is_none() {
                                rows_opt = Some(
                                    key_table
                                        .build_rows(&group_arrays)
                                        .map_err(|e| e.to_string())?,
                                );
                            }
                            let rows = rows_opt.as_ref().expect("group rows");
                            let row_bytes = rows.row(row).data();
                            key_table
                                .find_or_insert_from_row(&key_views, row, row_bytes, hash)
                                .map_err(|e| e.to_string())?
                        };
                        self.ensure_group_state(&lookup, &group_arrays, row)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
            }

            if group_ids.len() != num_rows {
                return Err("aggregate group id count mismatch".to_string());
            }

            self.state_ptrs.clear();
            self.state_ptrs.reserve(num_rows);
            for &group_id in &group_ids {
                let state_ptr = *self
                    .group_states
                    .get(group_id)
                    .ok_or_else(|| "aggregate state missing".to_string())?;
                self.state_ptrs.push(state_ptr);
            }
            let kernels = self
                .kernels
                .as_ref()
                .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
            let agg_views = build_agg_views(&kernels.entries, &self.functions, &agg_arrays)
                .map_err(|e| e.to_string())?;
            for (idx, (kernel, view)) in kernels.entries.iter().zip(agg_views.iter()).enumerate() {
                if self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false)
                {
                    kernel
                        .merge_batch(&self.state_ptrs, view)
                        .map_err(|e| e.to_string())?;
                } else {
                    kernel
                        .update_batch(&self.state_ptrs, view)
                        .map_err(|e| e.to_string())?;
                }
            }
            Ok(())
        })();
        self.key_table = Some(key_table);
        result?;

        Ok(None)
    }

    fn finish(&mut self) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        if !self.initialized {
            return Err("aggregate operator not prepared".to_string());
        }

        if !self.group_by.is_empty() {
            let observed = self
                .key_table
                .as_ref()
                .map(|table| {
                    table
                        .key_columns()
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| {
                            self.observed_group_key_nullable
                                .get(idx)
                                .copied()
                                .unwrap_or(false)
                                || col.has_nulls()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| self.observed_group_key_nullable.clone());
            self.rebuild_output_schema(Some(&observed))?;
        }

        if self.group_states.is_empty() {
            if self.group_by.is_empty() {
                self.ensure_scalar_group().map_err(|e| e.to_string())?;
            } else {
                let schema = self
                    .output_schema
                    .clone()
                    .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
                let batch = RecordBatch::new_empty(schema);
                return Ok(Some(self.output_chunk_from_batch(batch)?));
            }
        }

        let schema = self
            .output_schema
            .clone()
            .unwrap_or_else(|| Arc::new(Schema::new(Vec::<Field>::new())));
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        let key_count = self
            .key_table
            .as_ref()
            .map(|table| table.key_columns().len())
            .unwrap_or(0);
        let mut arrays = Vec::with_capacity(key_count + kernels.entries.len());
        if let Some(table) = self.key_table.as_ref() {
            for col in table.key_columns() {
                arrays.push(col.to_array().map_err(|e| e.to_string())?);
            }
        }
        for kernel in &kernels.entries {
            arrays.push(
                kernel
                    .build_array(&self.group_states, self.output_intermediate)
                    .map_err(|e| e.to_string())?,
            );
        }
        let schema = align_schema_with_arrays(&schema, &arrays, "aggregate finalize output")?;

        let batch = if arrays.is_empty() {
            let options = arrow::array::RecordBatchOptions::new()
                .with_row_count(Some(self.group_states.len()));
            RecordBatch::try_new_with_options(schema, arrays, &options)
        } else {
            RecordBatch::try_new(schema, arrays)
        }
        .map_err(|e| e.to_string())?;
        Ok(Some(self.output_chunk_from_batch(batch)?))
    }
}

impl ProcessorOperator for AggregateProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let result = (|| {
            if self.finished {
                return if self.final_domain_session_bound {
                    Err("aggregate received input after set_finishing".to_string())
                } else {
                    Ok(())
                };
            }
            if self.finishing {
                return Err("aggregate received input after set_finishing".to_string());
            }
            if self.pending_output.is_some() {
                return Err("aggregate received input while output buffer is full".to_string());
            }
            let num_rows = chunk.len();
            let out = self.process(chunk)?;
            if out.is_some() {
                return Err("aggregate produced output before finishing".to_string());
            }
            self.topn_rf_rows_since_publish += num_rows;
            self.try_submit_native_topn_bound()?;
            Ok(())
        })();
        if result.is_err() {
            self.fail_final_domain();
            let _ = self.fail_native_topn_producers(ProducerFailureReason::ExecutionFailed);
        }
        result
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.finalized && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        let result = (|| {
            if self.finished {
                return Ok(());
            }
            self.finishing = true;
            if self.finalized {
                return Ok(());
            }
            if self.pending_output.is_some() {
                return Ok(());
            }
            let out = self.finish()?;
            self.finish_native_topn_producers()?;
            if self.final_domain_committer.is_some() {
                self.seal_final_domain()?;
            }
            self.release_finalized_state();
            if failpoint::should_trigger(
                failpoint::FORCE_RESET_AGGREGATOR_AFTER_STREAMING_SINK_FINISH,
            ) {
                self.reset_after_streaming_finish();
            }
            self.pending_output = out;
            self.finalized = true;
            if self.pending_output.is_none() {
                self.finished = true;
            }
            Ok(())
        })();
        if result.is_err() {
            self.fail_final_domain();
            let _ = self.fail_native_topn_producers(ProducerFailureReason::ExecutionFailed);
        }
        result
    }

    fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
        aggregate_accepts_encoded_group_column(
            &self.arena,
            &self.group_by,
            &self.functions,
            slot_id,
            data_type,
        )
    }
}

impl AggregateProcessorOperator {
    fn fail_final_domain(&mut self) {
        drop(self.final_domain_committer.take());
    }

    fn seal_final_domain(&mut self) -> Result<(), String> {
        let max_domain_canonical_bytes = self.max_domain_canonical_bytes.ok_or_else(|| {
            "aggregate final-domain canonical domain budget is missing".to_string()
        })?;
        let domain = match final_domain::extract_final_aggregate_domain(
            self.key_table
                .as_ref()
                .map(|table| table.key_columns())
                .unwrap_or(&[]),
            max_domain_canonical_bytes,
        ) {
            Ok(domain) => domain,
            Err(final_domain::FinalAggregateDomainError::ResourceOrSize) => {
                self.fail_final_domain();
                return Ok(());
            }
            Err(error) => return Err(error.to_string()),
        };
        let mut partition = self
            .final_domain_committer
            .take()
            .expect("checked aggregate final-domain committer");
        #[cfg(test)]
        let observed_domain = domain.clone();
        if let Err(error) = partition.committer.seal(domain) {
            if error.kind() == RuntimeContractViolationKind::ServiceUnavailable {
                return Ok(());
            }
            return Err(error.to_string());
        }
        #[cfg(test)]
        if let Some(observer) = self.final_domain_partition_observer.as_ref() {
            observer(partition.partition_id, &observed_domain);
        }
        if let Err(error) = partition.committer.close() {
            if error.kind() == RuntimeContractViolationKind::ServiceUnavailable {
                return Ok(());
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    fn release_finalized_state(&mut self) {
        self.drop_group_states();
        self.state_ptrs.clear();
        self.key_table = None;
    }

    fn reset_after_streaming_finish(&mut self) {
        self.drop_group_states();
        self.state_ptrs.clear();
        self.observed_group_key_nullable.clear();
        self.key_table = None;
        self.kernels = None;
        self.output_schema = None;
    }

    fn eval_group_by_arrays(&self, chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
        if self.direct_input {
            if self.output_chunk_schema.slot_ids().len() < self.group_by.len() {
                return Err(format!(
                    "aggregate direct input missing group by slot ids: group_by={} output_slots={}",
                    self.group_by.len(),
                    self.output_chunk_schema.slot_ids().len()
                ));
            }
            let mut arrays = Vec::with_capacity(self.group_by.len());
            for slot_id in self
                .output_chunk_schema
                .slot_ids()
                .iter()
                .take(self.group_by.len())
            {
                arrays.push(
                    chunk
                        .column_by_slot_id(*slot_id)
                        .map_err(|e| e.to_string())?,
                );
            }
            return Ok(arrays);
        }
        let mut arrays = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let array = self.arena.eval(*expr, chunk).map_err(|e| e.to_string())?;
            arrays.push(array);
        }
        Ok(arrays)
    }

    fn eval_agg_arrays(&self, chunk: &Chunk) -> Result<Vec<Option<ArrayRef>>, String> {
        if self.direct_input {
            let start = self.group_by.len();
            if self.output_chunk_schema.slot_ids().len() < start + self.functions.len() {
                return Err(format!(
                    "aggregate direct input missing aggregate slot ids: group_by={} functions={} output_slots={}",
                    self.group_by.len(),
                    self.functions.len(),
                    self.output_chunk_schema.slot_ids().len()
                ));
            }
            let mut arrays = Vec::with_capacity(self.functions.len());
            for idx in 0..self.functions.len() {
                let slot_id = *self
                    .output_chunk_schema
                    .slot_ids()
                    .get(start + idx)
                    .ok_or_else(|| {
                        format!(
                            "aggregate direct input missing slot id at index {} (output_slots={})",
                            start + idx,
                            self.output_chunk_schema.slot_ids().len()
                        )
                    })?;
                arrays.push(Some(
                    chunk
                        .column_by_slot_id(slot_id)
                        .map_err(|e| e.to_string())?,
                ));
            }
            return Ok(arrays);
        }
        let mut arrays = Vec::with_capacity(self.functions.len());
        for func in &self.functions {
            let array = if func.inputs.is_empty() {
                None
            } else if func.inputs.len() == 1 {
                Some(
                    self.arena
                        .eval(func.inputs[0], chunk)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                return Err(format!(
                    "aggregate inputs must be packed into a single struct expression: {} has {} inputs",
                    func.name,
                    func.inputs.len()
                ));
            };
            arrays.push(array);
        }
        Ok(arrays)
    }

    fn ensure_data_initialized(
        &mut self,
        group_arrays: &[ArrayRef],
        agg_arrays: &[Option<ArrayRef>],
    ) -> Result<(), String> {
        if !self.initialized {
            return Err("aggregate operator not prepared".to_string());
        }
        if self.data_initialized {
            return Ok(());
        }

        if !self.group_by.is_empty() && group_arrays.len() != self.group_by.len() {
            return Err("group_by arrays length mismatch".to_string());
        }
        if agg_arrays.len() != self.functions.len() {
            return Err("aggregate arrays length mismatch".to_string());
        }

        let expected_group_types = self.expected_group_types()?;
        let expected_agg_types = self.expected_agg_input_types()?;
        self.validate_group_array_types(&expected_group_types, group_arrays)?;
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        self.validate_agg_array_types(&expected_agg_types, &kernels.entries, agg_arrays)?;

        if let Some(table) = self.key_table.as_mut()
            && table.key_strategy() == GroupKeyStrategy::CompressedFixed
            && table.compressed_ctx().is_none()
        {
            if group_arrays.first().map_or(0, |array| array.len()) == 0 {
                return Ok(());
            }
            let views = build_group_key_views(group_arrays)?;
            table.ensure_compressed_ctx(&views)?;
        }
        self.data_initialized = true;
        Ok(())
    }
    fn init_from_plan(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }

        let native_topn_producers = self.runtime_filter_execution.topn_producers.as_slice();
        self.topn_boundary_bindings = build_topn_boundary_bindings(native_topn_producers)
            .map_err(|error| error.to_string())?;

        let expected_group_types = self.expected_group_types()?;
        let expected_agg_types = self.expected_agg_input_types()?;

        if !expected_group_types.is_empty() {
            self.key_table = Some(KeyTable::new(
                expected_group_types.clone(),
                ENABLE_GROUP_KEY_OPTIMIZATIONS,
            )?);
        }

        let kernels = agg::build_kernel_set(&self.functions, &expected_agg_types)?;
        self.kernels = Some(kernels);
        if self.kernels.is_some() {
            self.rebuild_output_schema(None)?;
        }
        self.initialized = true;
        Ok(())
    }

    fn refresh_output_schema_for_group_arrays(
        &mut self,
        group_arrays: &[ArrayRef],
    ) -> Result<(), String> {
        if self.group_by.is_empty() {
            return Ok(());
        }
        let group_key_nullable: Vec<bool> = group_arrays
            .iter()
            .map(|array| array.null_count() > 0)
            .collect();
        if self.observed_group_key_nullable.len() != group_key_nullable.len() {
            self.observed_group_key_nullable = vec![false; group_key_nullable.len()];
        }
        let mut changed = false;
        for (observed, current) in self
            .observed_group_key_nullable
            .iter_mut()
            .zip(group_key_nullable.iter())
        {
            let next = *observed || *current;
            changed |= next != *observed;
            *observed = next;
        }
        if !changed
            && !self
                .observed_group_key_nullable
                .iter()
                .any(|nullable| *nullable)
        {
            return Ok(());
        }
        let observed = self.observed_group_key_nullable.clone();
        self.rebuild_output_schema(Some(&observed))
    }

    fn output_chunk_from_batch(&self, batch: RecordBatch) -> Result<Chunk, String> {
        #[cfg(test)]
        if self.fail_output_construction {
            return Err("injected aggregate output construction failure".to_string());
        }
        let output_len = batch.num_columns();
        if self.output_chunk_schema.slot_ids().len() < output_len {
            return Err(format!(
                "aggregate output slot count mismatch: batch_columns={} output_slots={}",
                output_len,
                self.output_chunk_schema.slot_ids().len()
            ));
        }
        {
            let batch_schema = batch.schema();
            let slot_schemas = self.output_chunk_schema.slot_ids()[..output_len]
                .iter()
                .enumerate()
                .map(|(idx, slot_id)| {
                    let slot_schema = self.output_chunk_schema.slot(*slot_id).ok_or_else(|| {
                        format!(
                            "aggregate explicit output chunk schema missing slot {}",
                            slot_id
                        )
                    })?;
                    let field = batch_schema.field(idx);
                    slot_schema.with_field_and_slot_id(*slot_id, field.as_ref().clone())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::try_new(slot_schemas)?))
        }
    }

    fn expected_group_types(&self) -> Result<Vec<DataType>, String> {
        let mut types = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let data_type = self
                .arena
                .data_type(*expr)
                .ok_or_else(|| "group by type missing".to_string())?
                .clone();
            if matches!(data_type, DataType::Null) {
                return Err("group by type is null".to_string());
            }
            types.push(data_type);
        }
        Ok(types)
    }

    fn expected_agg_input_types(&self) -> Result<Vec<Option<DataType>>, String> {
        let mut types = Vec::with_capacity(self.functions.len());
        for func in &self.functions {
            if func.input_is_intermediate {
                // Merge aggregates consume *intermediate state* produced by a previous aggregation
                // stage. In StarRocks plans, the input SlotRef for that intermediate column may
                // still carry the *final output type* (e.g. avg(decimal) has ret_type DECIMAL but
                // intermediate_type VARBINARY), so relying on the expression type can be wrong.
                //
                // Prefer FE-provided type signature (TFunction.aggregate_fn.intermediate_type)
                // when available to build the correct merge view and kernel spec.
                if let Some(sig) = func.types.as_ref()
                    && let Some(intermediate) = sig.intermediate_type.as_ref()
                {
                    if matches!(intermediate, DataType::Null) {
                        return Err("aggregate intermediate type is null".to_string());
                    }
                    types.push(Some(intermediate.clone()));
                    continue;
                }
            }
            let data_type = match (func.name.as_str(), func.inputs.as_slice()) {
                ("count", []) => None,
                (_, [expr]) => Some(
                    self.arena
                        .data_type(*expr)
                        .ok_or_else(|| "aggregate input type missing".to_string())?
                        .clone(),
                ),
                (_, []) => return Err("aggregate input missing".to_string()),
                (_, _) => {
                    return Err(format!(
                        "aggregate inputs must be packed into a single struct expression: {} has {} inputs",
                        func.name,
                        func.inputs.len()
                    ));
                }
            };
            types.push(data_type);
        }
        Ok(types)
    }

    fn validate_group_array_types(
        &self,
        expected: &[DataType],
        arrays: &[ArrayRef],
    ) -> Result<(), String> {
        if expected.len() != arrays.len() {
            return Err("group by type length mismatch".to_string());
        }
        for (idx, (expected_type, array)) in expected.iter().zip(arrays.iter()).enumerate() {
            let actual_type = array.data_type();
            if !is_compatible_aggregate_group_data_type(expected_type, actual_type) {
                return Err(format!(
                    "group by type mismatch at {}: expected {:?}, got {:?}",
                    idx, expected_type, actual_type
                ));
            }
        }
        Ok(())
    }

    fn validate_agg_array_types(
        &self,
        expected_input_types: &[Option<DataType>],
        kernels: &[agg::AggKernelEntry],
        arrays: &[Option<ArrayRef>],
    ) -> Result<(), String> {
        if expected_input_types.len() != arrays.len() || kernels.len() != arrays.len() {
            return Err("aggregate type length mismatch".to_string());
        }
        for (idx, array_opt) in arrays.iter().enumerate() {
            if self
                .functions
                .get(idx)
                .map(|f| f.input_is_intermediate)
                .unwrap_or(false)
            {
                let array = array_opt
                    .as_ref()
                    .ok_or_else(|| "aggregate intermediate input missing".to_string())?;
                let expected_type = kernels[idx].output_type(true);
                let actual_type = array.data_type();
                let is_struct_wrapped = match actual_type {
                    DataType::Struct(fields) if !fields.is_empty() => {
                        is_compatible_aggregate_data_type(&expected_type, fields[0].data_type())
                    }
                    _ => false,
                };
                if !is_compatible_aggregate_data_type(&expected_type, actual_type)
                    && !is_struct_wrapped
                {
                    return Err(format!(
                        "aggregate intermediate type mismatch at {}: expected {:?}, got {:?}",
                        idx, expected_type, actual_type
                    ));
                }
                continue;
            }

            if self.functions[idx].name == "count" && self.functions[idx].inputs.is_empty() {
                if array_opt.is_some() {
                    return Err("count input should be none".to_string());
                }
                continue;
            }

            let expected_type = expected_input_types
                .get(idx)
                .and_then(|t| t.as_ref())
                .ok_or_else(|| "aggregate input type missing".to_string())?;
            let array = array_opt
                .as_ref()
                .ok_or_else(|| "aggregate input missing".to_string())?;
            if !is_compatible_aggregate_data_type(expected_type, array.data_type()) {
                return Err(format!(
                    "aggregate input type mismatch at {}: expected {:?}, got {:?}",
                    idx,
                    expected_type,
                    array.data_type()
                ));
            }
        }
        Ok(())
    }

    fn ensure_scalar_group(&mut self) -> Result<(), String> {
        if !self.group_states.is_empty() {
            return Ok(());
        }
        self.alloc_group_state(0)?;
        Ok(())
    }

    fn ensure_group_state(
        &mut self,
        lookup: &KeyLookup,
        group_arrays: &[ArrayRef],
        row: usize,
    ) -> Result<(), String> {
        if lookup.is_new {
            self.alloc_group_state(lookup.group_id)?;
            observe_key_table_group(&mut self.topn_boundary_bindings, lookup, group_arrays, row)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    fn alloc_group_state(&mut self, group_id: usize) -> Result<(), String> {
        let kernels = self
            .kernels
            .as_ref()
            .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
        if group_id != self.group_states.len() {
            return Err("aggregate group id out of bounds".to_string());
        }
        let align = kernels
            .entries
            .iter()
            .map(|entry| entry.state_align())
            .max()
            .unwrap_or(1);
        let state_ptr = self.state_arena.alloc(kernels.layout.total_size, align);
        for kernel in &kernels.entries {
            kernel.init_state(state_ptr);
        }
        self.group_states.push(state_ptr);
        Ok(())
    }

    fn drop_group_states(&mut self) {
        let Some(kernels) = self.kernels.as_ref() else {
            self.group_states.clear();
            return;
        };
        for &state in &self.group_states {
            for kernel in &kernels.entries {
                kernel.drop_state(state);
            }
        }
        self.group_states.clear();
    }
}

impl Drop for AggregateProcessorOperator {
    fn drop(&mut self) {
        self.drop_group_states();
    }
}

#[cfg(test)]
fn aggregate_topn_test_operator(
    topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
    session_factory: AggregateTopNProducerSessionFactory,
) -> Box<dyn Operator> {
    AggregateProcessorFactory {
        name: "AGGREGATE_TOPN_TEST".to_string(),
        arena: Arc::new(ExprArena::default()),
        group_by: Vec::new(),
        functions: Vec::new(),
        output_intermediate: false,
        direct_input: false,
        output_chunk_schema: Arc::new(ChunkSchema::empty()),
        runtime_filter_execution: AggregateRuntimeFilterExecution { topn_producers },
        native_topn_session_factory: Some(Arc::new(session_factory)),
        final_domain_session: None,
        final_domain_shape_error: None,
        fail_output_construction: false,
    }
    .create(1, 0)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        AggregateFinalDomainSessionBuilder, AggregateProcessorFactory,
        is_compatible_aggregate_data_type, is_compatible_aggregate_group_data_type,
        normalize_aggregate_group_arrays,
    };
    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, CompletionRequirement, ContributionKind,
        CoverageWitnessId, NullSemantics, ReductionRequirement, RuntimeFilterLifecycle,
        RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        MaterializationPolicy, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, local_participant_install_for_test,
    };
    use crate::runtime_filter::port::subscription::UnavailableReason;
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
    use crate::runtime_filter::service::RuntimeFilterService;

    const RF_BINDING: BindingId = BindingId::new(10);
    const RF_CHANNEL: ChannelId = ChannelId::new(1);
    const RF_INSTANCE: UniqueId = UniqueId::new(70, 10);
    const GROUP_SLOT: SlotId = SlotId::new(1);

    struct TestClock(Instant);

    impl RuntimeFilterClock for TestClock {
        fn now(&self) -> Instant {
            self.0
        }
    }

    #[derive(Default)]
    struct TestMemory {
        retained: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for TestMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.retained.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.retained.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    #[derive(Default)]
    struct TestEvents(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for TestEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().expect("test event lock").push(event);
        }
    }

    impl TestEvents {
        fn snapshot(&self) -> Vec<RuntimeFilterEvent> {
            self.0.lock().expect("test event lock").clone()
        }
    }

    struct FinalDomainFixture {
        service: Arc<RuntimeFilterService>,
        events: Arc<TestEvents>,
    }

    impl FinalDomainFixture {
        fn new() -> Self {
            let witness = CoverageWitnessId::new(101);
            let coverage = Coverage::AllOf(vec![Coverage::Leaf(witness)]);
            let deployment = RuntimeFilterChannelDeployment::new(
                RF_CHANNEL,
                RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NullSafeEqual,
                },
                RuntimeFilterLifecycle::CompleteOnce,
                coverage.clone(),
                coverage,
                ReductionRequirement::SetUnion,
                BTreeSet::from([
                    ContributionKind::FinalDomainShard,
                    ContributionKind::ProducerClosed,
                ]),
                CompletionRequirement::FencedFinalDomain(
                    CompletionFenceKind::CommittedDomainFrozen,
                ),
                RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 1024,
                    max_artifact_bytes: 1024,
                    deadline_ms: 1_000,
                    max_retries: 2,
                },
                RuntimeFilterCoreBudget::new(8192),
                MaterializationPolicy::for_test(),
                BTreeMap::from([(
                    RF_BINDING,
                    ProducerDeployment::new(witness, BTreeSet::from([RF_INSTANCE])),
                )]),
                BTreeMap::new(),
            );
            let install = local_participant_install_for_test(RuntimeFilterInstallView::new(
                DeploymentEpoch::new(9),
                RuntimeFilterParticipantId::new(3),
                BTreeMap::from([(RF_CHANNEL, deployment)]),
            ));
            let events = Arc::new(TestEvents::default());
            let service = Arc::new(RuntimeFilterService::new_for_lifecycle_test(
                UniqueId::new(70, 0),
                Arc::new(TestClock(Instant::now())),
                events.clone(),
                Arc::new(TestMemory::default()),
            ));
            service.install(install).expect("final-domain install");
            Self { service, events }
        }

        fn session_builder(&self, dop: i32) -> AggregateFinalDomainSessionBuilder {
            self.session_builder_with_budget(dop, 512)
        }

        fn session_builder_with_budget(
            &self,
            dop: i32,
            max_domain_canonical_bytes: usize,
        ) -> AggregateFinalDomainSessionBuilder {
            let session = self
                .service
                .open_final_aggregate_producer(RF_BINDING, RF_INSTANCE, dop as u32)
                .expect("final-domain completion session");
            AggregateFinalDomainSessionBuilder::new(session, dop, max_domain_canonical_bytes)
                .expect("aggregate final-domain session builder")
        }

        fn accepted_partitions(&self) -> Vec<u32> {
            self.events
                .snapshot()
                .into_iter()
                .filter_map(|event| match event {
                    RuntimeFilterEvent::FinalDomainShardAccepted { identity } => {
                        Some(identity.stream().partition_id().get())
                    }
                    _ => None,
                })
                .collect()
        }

        fn failure_count(&self) -> usize {
            self.events
                .snapshot()
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ProducerInstanceFailed { .. }))
                .count()
        }

        fn producer_failed_unavailable_count(&self) -> usize {
            self.events
                .snapshot()
                .iter()
                .filter(|event| {
                    matches!(
                        event,
                        RuntimeFilterEvent::ChannelUnavailable {
                            reason: UnavailableReason::ProducerFailed,
                            ..
                        }
                    )
                })
                .count()
        }
    }

    fn aggregate_factory(
        session: Option<AggregateFinalDomainSessionBuilder>,
    ) -> AggregateProcessorFactory {
        aggregate_factory_with_output_type(session, DataType::Int64)
    }

    fn aggregate_factory_with_output_type(
        session: Option<AggregateFinalDomainSessionBuilder>,
        output_type: DataType,
    ) -> AggregateProcessorFactory {
        aggregate_factory_with_shape(session, output_type, false, true)
    }

    fn aggregate_factory_with_group_key_type(
        session: Option<AggregateFinalDomainSessionBuilder>,
        group_key_type: DataType,
    ) -> AggregateProcessorFactory {
        let mut arena = ExprArena::default();
        let group_expr = arena.push_typed(ExprNode::SlotId(GROUP_SLOT), group_key_type.clone());
        let output_field = Field::new("group_key", group_key_type, true);
        let output_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(GROUP_SLOT, &output_field, None)
                    .expect("aggregate output slot"),
            ])
            .expect("aggregate output schema"),
        );
        AggregateProcessorFactory::new_native(
            7,
            Arc::new(arena),
            vec![group_expr],
            Vec::new(),
            false,
            true,
            output_schema,
            Vec::new(),
            None,
            1,
            session,
        )
        .expect("build aggregate factory")
    }

    fn aggregate_factory_with_shape(
        session: Option<AggregateFinalDomainSessionBuilder>,
        output_type: DataType,
        output_intermediate: bool,
        direct_input: bool,
    ) -> AggregateProcessorFactory {
        let mut arena = ExprArena::default();
        let group_expr = arena.push_typed(ExprNode::SlotId(GROUP_SLOT), DataType::Int64);
        let output_field = Field::new("group_key", output_type, true);
        let output_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(GROUP_SLOT, &output_field, None)
                    .expect("aggregate output slot"),
            ])
            .expect("aggregate output schema"),
        );
        AggregateProcessorFactory::new_native(
            7,
            Arc::new(arena),
            vec![group_expr],
            Vec::new(),
            output_intermediate,
            direct_input,
            output_schema,
            Vec::new(),
            None,
            1,
            session,
        )
        .expect("build aggregate factory")
    }

    fn group_chunk(values: impl IntoIterator<Item = i64>) -> Chunk {
        let field = Field::new("group_key", DataType::Int64, false);
        let values = values.into_iter().collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .expect("aggregate input batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(GROUP_SLOT, &field, None)
                    .expect("aggregate input slot"),
            ])
            .expect("aggregate input schema"),
        );
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("aggregate input chunk")
    }

    fn malformed_group_chunk() -> Chunk {
        let field = Field::new("wrong_slot", DataType::Int64, false);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("malformed aggregate input batch");
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(SlotId::new(99), &field, None)
                    .expect("malformed aggregate input slot"),
            ])
            .expect("malformed aggregate input schema"),
        );
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("malformed aggregate chunk")
    }

    fn prepare_processor(
        factory: &AggregateProcessorFactory,
        dop: i32,
        driver_id: i32,
    ) -> Box<dyn crate::exec::pipeline::operator::Operator> {
        let mut operator = factory.create(dop, driver_id);
        operator.prepare().expect("prepare aggregate");
        operator
    }

    fn finish_operator(
        operator: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        values: impl IntoIterator<Item = i64>,
    ) -> Chunk {
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("aggregate processor");
        processor
            .push_chunk(&state, group_chunk(values))
            .expect("push aggregate input");
        processor.set_finishing(&state).expect("finish aggregate");
        processor
            .pull_chunk(&state)
            .expect("pull aggregate output")
            .expect("aggregate output")
    }

    fn sorted_int64_output(chunk: &Chunk) -> Vec<i64> {
        let values = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 aggregate output");
        let mut values = values.values().iter().copied().collect::<Vec<_>>();
        values.sort_unstable();
        values
    }

    #[test]
    fn aggregate_freezes_domain_only_after_set_finishing() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(1)));
        let mut operator = prepare_processor(&factory, 1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("aggregate processor");

        processor
            .push_chunk(&state, group_chunk([1, 2, 2]))
            .expect("push aggregate input");
        assert!(fixture.accepted_partitions().is_empty());

        processor.set_finishing(&state).expect("finish aggregate");
        assert_eq!(fixture.accepted_partitions(), vec![0]);
    }

    #[test]
    fn aggregate_rejects_input_after_domain_freeze() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(1)));
        let mut operator = prepare_processor(&factory, 1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("aggregate processor");
        processor
            .push_chunk(&state, group_chunk([1]))
            .expect("push aggregate input");
        processor.set_finishing(&state).expect("finish aggregate");

        let error = processor
            .push_chunk(&state, group_chunk([2]))
            .expect_err("aggregate must reject post-freeze input");

        assert_eq!(error, "aggregate received input after set_finishing");
        assert_eq!(fixture.accepted_partitions(), vec![0]);

        processor
            .pull_chunk(&state)
            .expect("pull aggregate output")
            .expect("aggregate output");
        assert!(processor.is_finished());
        let error = processor
            .push_chunk(&state, group_chunk([3]))
            .expect_err("finished aggregate must reject post-freeze input");
        assert_eq!(error, "aggregate received input after set_finishing");
        assert_eq!(fixture.accepted_partitions(), vec![0]);
    }

    #[test]
    fn aggregate_factory_maps_driver_id_to_partition() {
        let fixture = FinalDomainFixture::new();
        let observed = Arc::new(Mutex::new(BTreeMap::<u32, ValueDomainDelta>::new()));
        let observer_state = Arc::clone(&observed);
        let session = fixture
            .session_builder(2)
            .observe_partitions_for_test(Arc::new(move |partition_id, domain| {
                observer_state
                    .lock()
                    .expect("partition observer lock")
                    .insert(partition_id.get(), domain.clone());
            }));
        let factory = aggregate_factory(Some(session));
        let mut driver_1 = prepare_processor(&factory, 2, 1);
        let mut driver_0 = prepare_processor(&factory, 2, 0);

        finish_operator(&mut driver_1, [20, 21]);
        assert_eq!(
            observed
                .lock()
                .expect("partition observer lock")
                .get(&1)
                .map(ValueDomainDelta::values),
            Some(&MembershipValues::int64([20, 21]))
        );
        assert!(
            observed
                .lock()
                .expect("partition observer lock")
                .get(&0)
                .is_none()
        );
        finish_operator(&mut driver_0, [10, 11]);

        assert_eq!(
            *observed.lock().expect("partition observer lock"),
            BTreeMap::from([
                (
                    0,
                    ValueDomainDelta::new(MembershipValues::int64([10, 11]), false)
                ),
                (
                    1,
                    ValueDomainDelta::new(MembershipValues::int64([20, 21]), false)
                ),
            ])
        );
        assert_eq!(fixture.accepted_partitions(), vec![0, 1]);
    }

    #[test]
    fn aggregate_rejects_partial_and_nonmerge_session_miswire() {
        for (output_intermediate, direct_input, expected) in [
            (
                true,
                false,
                "aggregate final-domain session may bind only to a merge/final factory",
            ),
            (
                false,
                false,
                "aggregate final-domain session may bind only to a merge/final factory",
            ),
        ] {
            let fixture = FinalDomainFixture::new();
            let factory = aggregate_factory_with_shape(
                Some(fixture.session_builder(1)),
                DataType::Int64,
                output_intermediate,
                direct_input,
            );
            let mut operator = factory.create(1, 0);

            assert_eq!(
                operator.prepare().expect_err("structural miswire"),
                expected
            );
            assert_eq!(fixture.failure_count(), 1);
            assert_eq!(fixture.producer_failed_unavailable_count(), 1);
            assert!(fixture.accepted_partitions().is_empty());
        }
    }

    #[test]
    fn aggregate_rejects_declared_actual_dop_mismatch() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(2)));
        let mut operator = factory.create(3, 0);

        assert_eq!(
            operator.prepare().expect_err("DOP mismatch"),
            "aggregate final-domain DOP mismatch: declared=2 actual=3"
        );
        assert_eq!(fixture.failure_count(), 1);
        assert_eq!(fixture.producer_failed_unavailable_count(), 1);
        assert!(fixture.accepted_partitions().is_empty());
    }

    #[test]
    fn aggregate_prepare_rejects_unsupported_final_domain_key_before_mutation() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory_with_group_key_type(
            Some(fixture.session_builder(1)),
            DataType::Decimal256(10, 2),
        );
        let mut operator = factory.create(1, 0);

        assert_eq!(
            operator
                .prepare()
                .expect_err("unsupported final-domain key must fail prepare"),
            "unsupported aggregate final-domain membership key type: Decimal256(10, 2)"
        );
        assert!(
            !operator
                .as_processor_ref()
                .expect("aggregate processor")
                .has_output()
        );
        assert!(fixture.accepted_partitions().is_empty());
        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_prepare_rejects_installed_schema_mismatch_before_mutation() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory_with_group_key_type(
            Some(fixture.session_builder(1)),
            DataType::Int32,
        );
        let mut operator = factory.create(1, 0);

        assert_eq!(
            operator
                .prepare()
                .expect_err("installed schema mismatch must fail prepare"),
            "aggregate final-domain key type mismatch: installed=Int64 aggregate=Int32"
        );
        assert!(
            !operator
                .as_processor_ref()
                .expect("aggregate processor")
                .has_output()
        );
        assert!(fixture.accepted_partitions().is_empty());
        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_rejects_out_of_range_and_duplicate_driver_ids() {
        {
            let fixture = FinalDomainFixture::new();
            let factory = aggregate_factory(Some(fixture.session_builder(2)));
            let mut operator = factory.create(2, 2);

            assert_eq!(
                operator.prepare().expect_err("out-of-range driver"),
                "aggregate final-domain driver id is outside the actual DOP: driver_id=2 dop=2"
            );
            assert_eq!(fixture.failure_count(), 1);
            assert_eq!(fixture.producer_failed_unavailable_count(), 1);
        }

        {
            let fixture = FinalDomainFixture::new();
            let factory = aggregate_factory(Some(fixture.session_builder(2)));
            let first = prepare_processor(&factory, 2, 0);
            let mut duplicate = factory.create(2, 0);

            let error = duplicate.prepare().expect_err("duplicate driver");
            assert!(error.contains("partition acquisition failed for driver_id=0"));
            assert!(error.contains("already created"));
            assert_eq!(fixture.failure_count(), 1);
            assert_eq!(fixture.producer_failed_unavailable_count(), 1);
            drop(first);
            drop(factory);
            assert_eq!(fixture.failure_count(), 1);
        }
    }

    #[test]
    fn aggregate_dop_waits_for_last_driver() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(2)));
        let mut driver_0 = prepare_processor(&factory, 2, 0);
        let mut driver_1 = prepare_processor(&factory, 2, 1);

        finish_operator(&mut driver_0, [10]);
        assert!(fixture.accepted_partitions().is_empty());

        finish_operator(&mut driver_1, [20]);
        assert_eq!(fixture.accepted_partitions(), vec![0, 1]);
    }

    #[test]
    fn aggregate_error_propagates_and_fails_rf_session_once() {
        let baseline_factory = aggregate_factory(None);
        let mut baseline = prepare_processor(&baseline_factory, 1, 0);
        let state = RuntimeState::default();
        let expected = baseline
            .as_processor_mut()
            .expect("baseline aggregate processor")
            .push_chunk(&state, malformed_group_chunk())
            .expect_err("malformed input");

        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(1)));
        let mut operator = prepare_processor(&factory, 1, 0);
        let processor = operator.as_processor_mut().expect("aggregate processor");
        let actual = processor
            .push_chunk(&state, malformed_group_chunk())
            .expect_err("malformed input");
        assert_eq!(actual, expected);
        assert_eq!(fixture.failure_count(), 1);

        let repeated = processor
            .push_chunk(&state, malformed_group_chunk())
            .expect_err("repeated malformed input");
        assert_eq!(repeated, expected);
        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_drop_fails_session_once() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(1)));
        let operator = prepare_processor(&factory, 1, 0);

        drop(operator);
        drop(factory);

        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_factory_drop_before_all_declared_drivers_fails_session() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(2)));
        let operator = prepare_processor(&factory, 2, 0);

        drop(factory);
        assert_eq!(fixture.failure_count(), 1);
        drop(operator);
        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_output_construction_failure_publishes_no_domain() {
        let fixture = FinalDomainFixture::new();
        let mut factory = aggregate_factory(Some(fixture.session_builder(1)));
        factory.fail_output_construction_for_test();
        let mut operator = prepare_processor(&factory, 1, 0);
        let state = RuntimeState::default();
        let processor = operator.as_processor_mut().expect("aggregate processor");
        processor
            .push_chunk(&state, group_chunk([1]))
            .expect("push aggregate input");

        processor
            .set_finishing(&state)
            .expect_err("output type mismatch must fail finalization");

        assert!(fixture.accepted_partitions().is_empty());
        assert_eq!(fixture.failure_count(), 1);
    }

    #[test]
    fn aggregate_resource_or_size_failure_is_rf_fail_open() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder_with_budget(1, 1)));
        let mut operator = prepare_processor(&factory, 1, 0);
        let output = finish_operator(&mut operator, [3, 1, 3, 2]);

        assert_eq!(sorted_int64_output(&output), vec![1, 2, 3]);
        assert!(operator.is_finished());
        assert!(fixture.accepted_partitions().is_empty());
        assert_eq!(fixture.failure_count(), 1);
        assert_eq!(fixture.producer_failed_unavailable_count(), 1);
    }

    #[test]
    fn aggregate_service_unavailable_is_rf_fail_open() {
        let fixture = FinalDomainFixture::new();
        let factory = aggregate_factory(Some(fixture.session_builder(2)));
        let mut surviving = prepare_processor(&factory, 2, 0);
        let failed = prepare_processor(&factory, 2, 1);
        drop(failed);
        assert_eq!(fixture.failure_count(), 1);
        assert_eq!(fixture.producer_failed_unavailable_count(), 1);

        let output = finish_operator(&mut surviving, [3, 1, 3, 2]);

        assert_eq!(sorted_int64_output(&output), vec![1, 2, 3]);
        assert!(surviving.is_finished());
        assert!(fixture.accepted_partitions().is_empty());
        assert_eq!(fixture.failure_count(), 1);
        assert_eq!(fixture.producer_failed_unavailable_count(), 1);
    }

    #[test]
    fn aggregate_without_session_has_fixed_output_and_lifecycle() {
        let factory = aggregate_factory(None);
        let mut operator = prepare_processor(&factory, 1, 0);
        let state = RuntimeState::default();
        assert!(!operator.is_finished());

        let processor = operator.as_processor_mut().expect("aggregate processor");
        processor
            .push_chunk(&state, group_chunk([3, 1, 3, 2]))
            .expect("push aggregate input");
        processor.set_finishing(&state).expect("finish aggregate");
        assert!(!processor.is_finished());

        let output = processor
            .pull_chunk(&state)
            .expect("pull aggregate output")
            .expect("aggregate output");
        assert_eq!(sorted_int64_output(&output), vec![1, 2, 3]);
        assert_eq!(output.chunk_schema().slot_ids(), &[GROUP_SLOT]);
        assert!(processor.is_finished());
        assert!(
            processor
                .pull_chunk(&state)
                .expect("terminal pull")
                .is_none()
        );
        processor
            .push_chunk(&state, group_chunk([4]))
            .expect("sessionless completed aggregate preserves terminal no-op input");
        assert!(processor.is_finished());
        assert!(!processor.has_output());
    }

    #[test]
    fn aggregate_rejects_decimal_precision_drift() {
        assert!(!is_compatible_aggregate_data_type(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 2)
        ));
        assert!(!is_compatible_aggregate_data_type(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 3)
        ));
        assert!(is_compatible_aggregate_data_type(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(10, 2)
        ));
    }

    #[test]
    fn aggregate_accepts_dictionary_runtime_type_for_utf8_group_key() {
        assert!(is_compatible_aggregate_group_data_type(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ));
        assert!(is_compatible_aggregate_group_data_type(
            &DataType::LargeUtf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
        ));
        assert!(!is_compatible_aggregate_group_data_type(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
        ));
        assert!(!is_compatible_aggregate_group_data_type(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8)),
        ));
        assert!(!is_compatible_aggregate_data_type(
            &DataType::Utf8,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ));
    }

    #[test]
    fn aggregate_group_key_retags_nested_metadata_only_type() {
        use arrow::datatypes::Field;

        let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let actual_field = Field::new("item", DataType::Int32, true).with_metadata(HashMap::from(
            [("PARQUET:field_id".to_string(), "10".to_string())],
        ));
        let actual = Arc::new(ListArray::new(
            Arc::new(actual_field),
            OffsetBuffer::from_lengths([2]),
            values,
            None,
        )) as ArrayRef;
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));

        let normalized =
            normalize_aggregate_group_arrays(std::slice::from_ref(&expected), vec![actual])
                .expect("metadata-only retag");

        assert_eq!(normalized[0].data_type(), &expected);
    }

    #[test]
    fn aggregate_encoded_gate_accepts_only_single_direct_group_slot() {
        use crate::common::ids::SlotId;
        use crate::exec::expr::{ExprArena, ExprNode};
        use crate::exec::node::aggregate::AggFunction;

        let slot = SlotId::new(7);
        let other_slot = SlotId::new(8);
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let large_dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));
        let binary_dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));

        let mut arena = ExprArena::default();
        let group_expr = arena.push_typed(ExprNode::SlotId(slot), DataType::Utf8);
        assert!(super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[],
            slot,
            &dict_type,
        ));
        assert!(super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[],
            slot,
            &large_dict_type,
        ));
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[],
            other_slot,
            &dict_type,
        ));
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[],
            slot,
            &binary_dict_type,
        ));
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[],
            slot,
            &DataType::Utf8,
        ));

        let cast_expr = arena.push_typed(ExprNode::Cast(group_expr), DataType::Utf8);
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[cast_expr],
            &[],
            slot,
            &dict_type,
        ));

        let other_group = arena.push_typed(ExprNode::SlotId(other_slot), DataType::Utf8);
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr, other_group],
            &[],
            slot,
            &dict_type,
        ));

        let function_using_group_slot = AggFunction {
            name: "min".to_string(),
            inputs: vec![group_expr],
            ..Default::default()
        };
        assert!(!super::aggregate_accepts_encoded_group_column(
            &arena,
            &[group_expr],
            &[function_using_group_slot],
            slot,
            &dict_type,
        ));
    }

    #[test]
    fn aggregate_output_schema_rejects_runtime_type_drift() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "__avg_state",
            DataType::Utf8,
            true,
        )]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(10_i64)]))];

        let err = super::align_schema_with_arrays(&schema, &arrays, "p5 aggregate output")
            .expect_err("aggregate output must not adopt actual array type");
        assert!(
            err.contains("p5 aggregate output type mismatch"),
            "err={err}"
        );
    }
}
