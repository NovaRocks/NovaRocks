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
//! Streaming aggregate sink operator for Phase 1 pre-aggregation.
//!
//! Responsibilities:
//! - Owns the hash table and aggregate state for streaming pre-aggregation.
//! - Follows the sink convention: `has_output() = false`, `pull_chunk() = None`.
//! - On `set_finishing()`, materializes all groups and pushes result chunks into a shared
//!   `AggregateStreamingState` buffer.
//! - Publishes TopN runtime filters during `push_chunk()`.
//!
//! Key exported interfaces:
//! - Types: `AggregateStreamingSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use crate::runtime_filter as execution;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use novarocks_functions::ResolvedAggregateSignature;

use super::streaming_state::AggregateStreamingState;
use super::{
    ENABLE_GROUP_KEY_OPTIMIZATIONS, align_schema_with_arrays, build_agg_batches,
    expected_agg_input_types, normalize_aggregate_group_arrays,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::agg;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::failpoint;
use crate::exec::hash_table::key_builder::build_group_key_views;
use crate::exec::hash_table::key_column::build_output_schema_from_kernels;
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::exec::node::aggregate::{AggFunction, AggregateTopNRuntimeFilterProducerBinding};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::RuntimeFilterProducerFailure;
use novarocks_types::SlotId;

use super::native_runtime_filter::{
    AggregateTopNProducerSession, AggregateTopNProducerSessionFactory,
};
use super::retained::{
    AggregateOperatorVectorsMemory, AggregateRetainedMemory, AggregateStatePointers,
    TouchedAggregateStates,
};
use super::topn_boundary::{
    AggregateTopNBoundaryBinding, build_topn_boundary_bindings, observe_key_table_group,
    validate_topn_boundary_specs,
};

/// Factory that constructs streaming aggregate sink operators for Phase 1 pre-aggregation.
pub struct AggregateStreamingSinkFactory {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    kernels: agg::AggKernelSet,
    output_intermediate: bool,
    output_chunk_schema: ChunkSchemaRef,
    runtime_filter_execution: StreamingAggregateRuntimeFilterExecution,
    native_topn_session_factory: Option<Arc<AggregateTopNProducerSessionFactory>>,
    streaming_state: AggregateStreamingState,
}

#[derive(Clone)]
struct StreamingAggregateRuntimeFilterExecution {
    topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
}

impl AggregateStreamingSinkFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "The factory mirrors the frozen aggregate plan contract without an intermediate allocation."
    )]
    pub(crate) fn new_native(
        node_id: i32,
        arena: Arc<ExprArena>,
        group_by: Vec<ExprId>,
        functions: Vec<AggFunction>,
        function_set: Arc<agg::SealedExecutionFunctionSet>,
        resolved_aggregates: Vec<ResolvedAggregateSignature>,
        output_intermediate: bool,
        output_chunk_schema: ChunkSchemaRef,
        state: AggregateStreamingState,
        topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
        runtime_filter_session: Option<execution::RuntimeFilterSessionRef>,
        local_partition_count: i32,
    ) -> Result<Self, String> {
        let name = if node_id >= 0 {
            format!("AGGREGATE_STREAMING_SINK (id={node_id})")
        } else {
            "AGGREGATE_STREAMING_SINK".to_string()
        };
        validate_topn_boundary_specs(&topn_producers).map_err(|error| error.to_string())?;
        let native_topn_session_factory = if topn_producers.is_empty() {
            None
        } else {
            let session = runtime_filter_session.ok_or_else(|| {
                format!(
                    "native aggregate TopN producer binding_id={} requires an execution runtime-filter session",
                    topn_producers[0].binding_id()
                )
            })?;
            Some(Arc::new(AggregateTopNProducerSessionFactory::from_plan(
                &topn_producers,
                session,
                local_partition_count,
            )?))
        };
        let expected_agg_types = expected_agg_input_types(&arena, &functions)?;
        let kernels = agg::build_kernel_set(
            &function_set,
            &functions,
            &expected_agg_types,
            &resolved_aggregates,
        )?;
        Ok(Self {
            name,
            arena,
            group_by,
            functions,
            kernels,
            output_intermediate,
            output_chunk_schema,
            runtime_filter_execution: StreamingAggregateRuntimeFilterExecution { topn_producers },
            native_topn_session_factory,
            streaming_state: state,
        })
    }
}

impl OperatorFactory for AggregateStreamingSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let (native_topn_session, native_topn_bind_error) =
            match self.native_topn_session_factory.as_ref() {
                Some(factory) => match factory.create_for_driver(dop, driver_id) {
                    Ok(session) => (Some(session), None),
                    Err(error) => (None, Some(error)),
                },
                None => (None, None),
            };
        Box::new(AggregateStreamingSinkOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            group_by: self.group_by.clone(),
            functions: self.functions.clone(),
            key_table: None,
            state_arena: agg::AggStateArena::new(64 * 1024),
            group_states: AggregateStatePointers::new(),
            state_ptrs: AggregateStatePointers::new(),
            kernels: Some(self.kernels.clone()),
            output_intermediate: self.output_intermediate,
            initialized: false,
            data_initialized: false,
            finishing: false,
            finished: false,
            output_schema: None,
            output_chunk_schema: Arc::clone(&self.output_chunk_schema),
            observed_group_key_nullable: vec![false; self.group_by.len()],
            key_table_mem_tracker: None,
            aggregate_retained_memory: AggregateRetainedMemory::new(),
            touched_aggregate_states: TouchedAggregateStates::new(),
            operator_vectors_memory: AggregateOperatorVectorsMemory::new(),
            memory_bind_error: None,
            runtime_filter_execution: self.runtime_filter_execution.clone(),
            topn_rf_rows_since_publish: 0,
            topn_boundary_bindings: Vec::new(),
            native_topn_session,
            native_topn_bind_error,
            streaming_state: self.streaming_state.clone(),
        })
    }

    #[cfg(test)]
    fn native_aggregate_topn_producers(&self) -> &[AggregateTopNRuntimeFilterProducerBinding] {
        &self.runtime_filter_execution.topn_producers
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct AggregateStreamingSinkOperator {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    key_table: Option<KeyTable>,
    state_arena: agg::AggStateArena,
    group_states: AggregateStatePointers,
    state_ptrs: AggregateStatePointers,
    kernels: Option<agg::AggKernelSet>,
    output_intermediate: bool,
    initialized: bool,
    data_initialized: bool,
    finishing: bool,
    finished: bool,
    output_schema: Option<SchemaRef>,
    output_chunk_schema: ChunkSchemaRef,
    observed_group_key_nullable: Vec<bool>,
    key_table_mem_tracker: Option<Arc<MemTracker>>,
    aggregate_retained_memory: AggregateRetainedMemory,
    touched_aggregate_states: TouchedAggregateStates,
    operator_vectors_memory: AggregateOperatorVectorsMemory,
    memory_bind_error: Option<String>,
    runtime_filter_execution: StreamingAggregateRuntimeFilterExecution,
    topn_rf_rows_since_publish: usize,
    topn_boundary_bindings: Vec<AggregateTopNBoundaryBinding>,
    native_topn_session: Option<AggregateTopNProducerSession>,
    native_topn_bind_error: Option<String>,
    streaming_state: AggregateStreamingState,
}

impl Operator for AggregateStreamingSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let arena = MemTracker::new_child("AggStateArena", &tracker);
        if let Err(error) = self.state_arena.try_set_mem_tracker(Arc::clone(&arena)) {
            self.memory_bind_error = Some(error);
        }

        let key_table = MemTracker::new_child("KeyTable", &tracker);
        self.key_table_mem_tracker = Some(key_table);

        if let Err(error) = self
            .aggregate_retained_memory
            .set_tracker(MemTracker::new_child("AggregateRetainedHeap", &tracker))
        {
            self.memory_bind_error = Some(error);
        }
        if let Err(error) = self
            .touched_aggregate_states
            .set_tracker(MemTracker::new_child("AggregateTouchedStates", &tracker))
        {
            self.memory_bind_error = Some(error);
        }
        if let Err(error) = self.operator_vectors_memory.set_tracker(
            &mut self.group_states,
            &mut self.state_ptrs,
            MemTracker::new_child("AggregateOperatorVectors", &tracker),
        ) {
            self.memory_bind_error = Some(error);
        }
    }

    fn prepare(&mut self) -> Result<(), String> {
        if let Some(error) = self.memory_bind_error.clone() {
            return Err(error);
        }
        self.operator_vectors_memory
            .ensure_bound(&mut self.group_states, &mut self.state_ptrs)?;
        self.init_from_plan()
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
        let _ = self.fail_native_topn_producers(RuntimeFilterProducerFailure::Cancelled);
    }

    fn on_driver_failure(&mut self) {
        let _ = self.fail_native_topn_producers(RuntimeFilterProducerFailure::ExecutionFailed);
    }

    fn close(&mut self) -> Result<(), String> {
        self.fail_native_topn_producers(RuntimeFilterProducerFailure::ExecutionFailed)
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

impl AggregateStreamingSinkOperator {
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

    fn fail_native_topn_producers(
        &mut self,
        reason: RuntimeFilterProducerFailure,
    ) -> Result<(), String> {
        let Some(session) = self.native_topn_session.as_mut() else {
            return Ok(());
        };
        session.fail(reason)
    }

    fn process(&mut self, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.aggregate_retained_memory.ensure_available()?;
        self.touched_aggregate_states.ensure_available()?;
        self.operator_vectors_memory.ensure_available()?;
        self.state_arena.ensure_available()?;

        if chunk.is_empty() && chunk.schema().fields().is_empty() {
            return Ok(());
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
            return Ok(());
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
        if self.group_by.is_empty() {
            self.ensure_scalar_group().map_err(|e| e.to_string())?;
            let state_ptr = *self
                .group_states
                .first()
                .ok_or_else(|| "aggregate scalar state missing".to_string())?;
            self.state_ptrs.clear();
            self.operator_vectors_memory.reserve_state_ptrs(
                &self.group_states,
                &mut self.state_ptrs,
                num_rows,
            )?;
            self.state_ptrs.resize(num_rows, state_ptr);
            let kernels = self
                .kernels
                .as_ref()
                .ok_or_else(|| "aggregate kernels not initialized".to_string())?;
            let agg_batches = build_agg_batches(&agg_arrays, num_rows)?;
            for (idx, (kernel, batch)) in kernels
                .entries
                .iter()
                .zip(agg_batches.iter().copied())
                .enumerate()
            {
                let merge = self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false);
                self.aggregate_retained_memory.run_bounded_batch(
                    &mut self.touched_aggregate_states,
                    kernel,
                    &self.state_ptrs,
                    batch,
                    merge,
                )?;
            }
            return Ok(());
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
                    let rows = key_table
                        .build_rows_fallback(&group_arrays)
                        .map_err(|e| e.to_string())?;
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                        let row_bytes = rows.get(row).map(|v| v.as_slice()).ok_or_else(|| {
                            format!(
                                "canonical serialized group row missing at row={} (rows={})",
                                row, num_rows
                            )
                        })?;
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
                                        .build_rows_fallback(&group_arrays)
                                        .map_err(|e| e.to_string())?,
                                );
                            }
                            let rows = rows_opt.as_ref().expect("group rows");
                            let row_bytes =
                                rows.get(row).map(|row| row.as_slice()).ok_or_else(|| {
                                    "canonical compressed fallback row missing".to_string()
                                })?;
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
            self.operator_vectors_memory.reserve_state_ptrs(
                &self.group_states,
                &mut self.state_ptrs,
                num_rows,
            )?;
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
            let agg_batches = build_agg_batches(&agg_arrays, num_rows)?;
            for (idx, (kernel, batch)) in kernels
                .entries
                .iter()
                .zip(agg_batches.iter().copied())
                .enumerate()
            {
                let merge = self
                    .functions
                    .get(idx)
                    .map(|f| f.input_is_intermediate)
                    .unwrap_or(false);
                self.aggregate_retained_memory.run_bounded_batch(
                    &mut self.touched_aggregate_states,
                    kernel,
                    &self.state_ptrs,
                    batch,
                    merge,
                )?;
            }
            Ok(())
        })();
        self.key_table = Some(key_table);
        result?;

        Ok(())
    }

    fn finish_to_chunks(&mut self) -> Result<Vec<Chunk>, String> {
        if self.finished {
            return Ok(vec![]);
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
                return Ok(vec![self.output_chunk_from_batch(batch)?]);
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
            arrays.push(self.aggregate_retained_memory.around(
                kernel,
                &self.group_states,
                || kernel.build_array(&self.group_states, self.output_intermediate),
            )?);
        }
        let schema = align_schema_with_arrays(&schema, &arrays, "aggregate streaming output")?;

        let batch = if arrays.is_empty() {
            let options = arrow::array::RecordBatchOptions::new()
                .with_row_count(Some(self.group_states.len()));
            RecordBatch::try_new_with_options(schema, arrays, &options)
        } else {
            RecordBatch::try_new(schema, arrays)
        }
        .map_err(|e| e.to_string())?;
        self.drop_group_states();
        Ok(vec![self.output_chunk_from_batch(batch)?])
    }

    fn init_from_plan(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }

        let native_topn_producers = self.runtime_filter_execution.topn_producers.as_slice();
        self.topn_boundary_bindings = build_topn_boundary_bindings(native_topn_producers)
            .map_err(|error| error.to_string())?;

        let expected_group_types = self.expected_group_types()?;

        if !expected_group_types.is_empty() {
            let key_table = match self.key_table_mem_tracker.as_ref() {
                Some(tracker) => KeyTable::new_with_tracker(
                    expected_group_types.clone(),
                    ENABLE_GROUP_KEY_OPTIMIZATIONS,
                    Arc::clone(tracker),
                )?,
                #[cfg(test)]
                None => {
                    KeyTable::new(expected_group_types.clone(), ENABLE_GROUP_KEY_OPTIMIZATIONS)?
                }
                #[cfg(not(test))]
                None => {
                    return Err(
                        "streaming aggregate key table memory tracker must be bound before initialization"
                            .to_string(),
                    );
                }
            };
            self.key_table = Some(key_table);
        }

        if self.kernels.is_some() {
            self.rebuild_output_schema(None)?;
        }
        self.initialized = true;
        Ok(())
    }

    fn eval_group_by_arrays(&self, chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
        let mut arrays = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let array = self.arena.eval(*expr, chunk).map_err(|e| e.to_string())?;
            arrays.push(array);
        }
        Ok(arrays)
    }

    fn eval_agg_arrays(&self, chunk: &Chunk) -> Result<Vec<Option<ArrayRef>>, String> {
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
        expected_agg_input_types(&self.arena, &self.functions)
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
            if !super::is_compatible_aggregate_group_data_type(expected_type, actual_type) {
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
                        super::is_compatible_aggregate_data_type(
                            &expected_type,
                            fields[0].data_type(),
                        )
                    }
                    _ => false,
                };
                if !super::is_compatible_aggregate_data_type(&expected_type, actual_type)
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
            if !super::is_compatible_aggregate_data_type(expected_type, array.data_type()) {
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
        self.operator_vectors_memory.reserve_group_states(
            &mut self.group_states,
            &self.state_ptrs,
            1,
        )?;
        let state_ptr = self
            .state_arena
            .try_alloc(kernels.layout.total_size, align)?;
        for (initialized, kernel) in kernels.entries.iter().enumerate() {
            if let Err(error) = self
                .aggregate_retained_memory
                .initialize_state(kernel, state_ptr)
            {
                for initialized_kernel in kernels.entries[..initialized].iter().rev() {
                    self.aggregate_retained_memory
                        .drop_initialized_state(initialized_kernel, state_ptr);
                }
                return Err(error);
            }
        }
        self.group_states.push(state_ptr);
        Ok(())
    }

    fn drop_group_states(&mut self) {
        if !self.group_states.is_bound() {
            return;
        }
        let Some(kernels) = self.kernels.as_ref() else {
            self.group_states.clear();
            return;
        };
        for &state in self.group_states.iter() {
            for kernel in &kernels.entries {
                self.aggregate_retained_memory
                    .drop_initialized_state(kernel, state);
            }
        }
        self.group_states.clear();
        self.aggregate_retained_memory.release_all();
    }
}

impl ProcessorOperator for AggregateStreamingSinkOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let result = (|| {
            if self.finished {
                return Ok(());
            }
            if self.finishing {
                return Err(
                    "aggregate streaming sink received input after set_finishing".to_string(),
                );
            }
            let num_rows = chunk.len();
            self.process(chunk)?;
            self.topn_rf_rows_since_publish += num_rows;
            self.try_submit_native_topn_bound()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.fail_native_topn_producers(RuntimeFilterProducerFailure::ExecutionFailed);
        }
        result
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        let result = (|| {
            if self.finished {
                return Ok(());
            }
            self.finishing = true;
            let chunks = self.finish_to_chunks()?;
            self.finish_native_topn_producers()?;
            for chunk in chunks {
                self.streaming_state.offer_chunk(chunk);
            }
            self.streaming_state.mark_sink_finished();
            self.finished = true;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.fail_native_topn_producers(RuntimeFilterProducerFailure::ExecutionFailed);
        }
        result
    }

    fn accepts_encoded_column(&self, slot_id: SlotId, data_type: &DataType) -> bool {
        super::aggregate_accepts_encoded_group_column(
            &self.arena,
            &self.group_by,
            &self.functions,
            slot_id,
            data_type,
        )
    }
}

impl Drop for AggregateStreamingSinkOperator {
    fn drop(&mut self) {
        self.drop_group_states();
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained as a streaming aggregate TopN runtime-filter regression fixture."
)]
pub(super) fn aggregate_streaming_topn_test_operator(
    topn_producers: Vec<AggregateTopNRuntimeFilterProducerBinding>,
    session_factory: AggregateTopNProducerSessionFactory,
) -> Box<dyn Operator> {
    AggregateStreamingSinkFactory {
        name: "AGGREGATE_STREAMING_TOPN_TEST".to_string(),
        arena: Arc::new(ExprArena::default()),
        group_by: Vec::new(),
        functions: Vec::new(),
        kernels: agg::AggKernelSet {
            entries: Vec::new(),
            layout: agg::AggStateLayout {
                total_size: 1,
                descs: Vec::new(),
            },
        },
        output_intermediate: false,
        output_chunk_schema: Arc::new(ChunkSchema::empty()),
        runtime_filter_execution: StreamingAggregateRuntimeFilterExecution { topn_producers },
        native_topn_session_factory: Some(Arc::new(session_factory)),
        streaming_state: AggregateStreamingState::new(1),
    }
    .create(1, 0)
}

#[cfg(test)]
mod retained_memory_tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    use super::{AggregateStreamingSinkFactory, AggregateStreamingState};
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
    use crate::exec::operators::aggregate::empty_execution_function_set;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeState;

    const VALUE_SLOT: SlotId = SlotId::new(101);
    const INTERMEDIATE_SLOT: SlotId = SlotId::new(102);
    const FINAL_SLOT: SlotId = SlotId::new(103);

    fn scalar_distinct_factory(
        input_slot: SlotId,
        input_type: DataType,
        output_slot: SlotId,
        input_is_intermediate: bool,
        output_intermediate: bool,
    ) -> (AggregateStreamingSinkFactory, AggregateStreamingState) {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(input_slot), input_type);
        let output_type = if output_intermediate {
            DataType::Binary
        } else {
            DataType::Int64
        };
        let function = AggFunction {
            name: "multi_distinct_count".to_string(),
            inputs: vec![input],
            input_is_intermediate,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(output_type.clone()),
                input_arg_type: Some(DataType::Int64),
            }),
            order: Default::default(),
        };
        let function_set = empty_execution_function_set();
        let selected = function_set
            .catalog()
            .resolve_aggregate_trusted("multi_distinct_count", &[DataType::Int64])
            .expect("resolve distinct aggregate");
        let output_field = Field::new("distinct_count", output_type, false);
        let output_schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(output_slot, &output_field, None)
                    .expect("streaming aggregate output slot"),
            ])
            .expect("streaming aggregate output schema"),
        );
        let state = AggregateStreamingState::new(1);
        let factory = AggregateStreamingSinkFactory::new_native(
            91,
            Arc::new(arena),
            Vec::new(),
            vec![function],
            function_set,
            vec![selected],
            output_intermediate,
            output_schema,
            state.clone(),
            Vec::new(),
            None,
            1,
        )
        .expect("build streaming aggregate factory");
        (factory, state)
    }

    fn int64_chunk(slot: SlotId, values: impl IntoIterator<Item = i64>) -> Chunk {
        let field = Field::new("value", DataType::Int64, false);
        let values = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![values])
            .expect("streaming aggregate input batch");
        let schema = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(slot, &field, None)
                    .expect("streaming aggregate input slot"),
            ])
            .expect("streaming aggregate input schema"),
        );
        Chunk::try_new_with_chunk_schema(batch, schema).expect("streaming aggregate input chunk")
    }

    fn finish_and_take_output(
        operator: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        state: &AggregateStreamingState,
    ) -> Chunk {
        operator
            .as_processor_mut()
            .expect("streaming aggregate processor")
            .set_finishing(&RuntimeState::default())
            .expect("finish streaming aggregate");
        state.poll_chunk().expect("streaming aggregate output")
    }

    #[test]
    fn streaming_update_growth_final_and_drop_release_query_memory() {
        let (factory, state) =
            scalar_distinct_factory(VALUE_SLOT, DataType::Int64, FINAL_SLOT, false, false);
        let tracker = MemTracker::new_root("streaming-update");
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare streaming aggregate");
        operator
            .as_processor_mut()
            .expect("streaming aggregate processor")
            .push_chunk(&RuntimeState::default(), int64_chunk(VALUE_SLOT, 0..256))
            .expect("update streaming aggregate");
        let before_finish = tracker.current();
        assert!(before_finish > 0, "state growth must be query-accounted");

        let output = finish_and_take_output(&mut operator, &state);
        assert!(
            tracker.current() < before_finish,
            "finalization must release the aggregate state's dynamic heap"
        );
        let count = output.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("distinct count output");
        assert_eq!(count.value(0), 256);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn streaming_merge_growth_final_and_drop_release_query_memory() {
        let (update_factory, update_state) =
            scalar_distinct_factory(VALUE_SLOT, DataType::Int64, INTERMEDIATE_SLOT, false, true);
        let update_tracker = MemTracker::new_root("streaming-partial");
        let mut update = update_factory.create(1, 0);
        update.set_mem_tracker(Arc::clone(&update_tracker));
        update.prepare().expect("prepare partial aggregate");
        update
            .as_processor_mut()
            .expect("partial aggregate processor")
            .push_chunk(&RuntimeState::default(), int64_chunk(VALUE_SLOT, 0..256))
            .expect("build partial distinct state");
        let intermediate = finish_and_take_output(&mut update, &update_state);
        drop(update);
        assert_eq!(update_tracker.current(), 0);
        assert!(
            intermediate.columns()[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .is_some(),
            "partial output must use the catalog intermediate carrier"
        );

        let (merge_factory, merge_state) =
            scalar_distinct_factory(INTERMEDIATE_SLOT, DataType::Binary, FINAL_SLOT, true, false);
        let merge_tracker = MemTracker::new_root("streaming-merge");
        let mut merge = merge_factory.create(1, 0);
        merge.set_mem_tracker(Arc::clone(&merge_tracker));
        merge.prepare().expect("prepare merge aggregate");
        merge
            .as_processor_mut()
            .expect("merge aggregate processor")
            .push_chunk(&RuntimeState::default(), intermediate)
            .expect("merge distinct state");
        let before_finish = merge_tracker.current();
        assert!(before_finish > 0, "merge growth must be query-accounted");
        let output = finish_and_take_output(&mut merge, &merge_state);
        assert!(merge_tracker.current() < before_finish);
        let count = output.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("merged distinct count output");
        assert_eq!(count.value(0), 256);
        drop(merge);
        assert_eq!(merge_tracker.current(), 0);
    }

    #[test]
    fn streaming_update_oom_fails_before_distinct_state_mutation() {
        let (factory, state) =
            scalar_distinct_factory(VALUE_SLOT, DataType::Int64, FINAL_SLOT, false, false);
        let tracker = MemTracker::new_root("streaming-oom");
        let mut operator = factory.create(1, 0);
        operator.set_mem_tracker(Arc::clone(&tracker));
        operator.prepare().expect("prepare streaming aggregate");
        operator
            .as_processor_mut()
            .expect("streaming aggregate processor")
            .push_chunk(&RuntimeState::default(), int64_chunk(VALUE_SLOT, [1, 2, 3]))
            .expect("seed distinct state");
        let before_oom = tracker.current();
        tracker
            .install_limit_once(before_oom)
            .expect("freeze memory limit at live usage");

        let error = operator
            .as_processor_mut()
            .expect("streaming aggregate processor")
            .push_chunk(&RuntimeState::default(), int64_chunk(VALUE_SLOT, [4]))
            .expect_err("new distinct value must require tracked allocation");
        assert!(error.contains("ResourceExhausted"), "{error}");
        assert_eq!(tracker.current(), before_oom);

        let output = finish_and_take_output(&mut operator, &state);
        let count = output.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("distinct count output");
        assert_eq!(count.value(0), 3, "failed allocation must not mutate state");
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}
