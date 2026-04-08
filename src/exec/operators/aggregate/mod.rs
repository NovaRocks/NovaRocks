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

pub(crate) mod streaming_state;

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::common::failpoint;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::agg;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::exec::node::aggregate::{AggFunction, TopNRuntimeFilterSpec};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::runtime_filter::min_max::RuntimeMinMaxFilter;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;

use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};
use crate::exec::hash_table::key_column::build_output_schema_from_kernels;
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

const ENABLE_GROUP_KEY_OPTIMIZATIONS: bool = true;

fn build_agg_views<'a>(
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

/// Factory that constructs aggregate processors backed by group-key hash tables and aggregate kernels.
pub struct AggregateProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    group_by: Vec<ExprId>,
    functions: Vec<AggFunction>,
    output_intermediate: bool,
    direct_input: bool,
    output_chunk_schema: ChunkSchemaRef,
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
}

impl AggregateProcessorFactory {
    pub fn new(
        node_id: i32,
        arena: Arc<ExprArena>,
        group_by: Vec<ExprId>,
        functions: Vec<AggFunction>,
        output_intermediate: bool,
        direct_input: bool,
        output_chunk_schema: ChunkSchemaRef,
        topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
        runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("AGGREGATE (id={node_id})")
        } else {
            "AGGREGATE".to_string()
        };
        Self {
            name,
            arena,
            group_by,
            functions,
            output_intermediate,
            direct_input,
            output_chunk_schema,
            topn_rf_specs,
            runtime_filter_hub,
        }
    }
}

impl OperatorFactory for AggregateProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
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
            topn_rf_specs: self.topn_rf_specs.clone(),
            runtime_filter_hub: self.runtime_filter_hub.clone(),
            topn_rf_rows_since_publish: 0,
        })
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
    topn_rf_specs: Vec<TopNRuntimeFilterSpec>,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    topn_rf_rows_since_publish: usize,
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
        self.init_from_plan()
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

    fn try_publish_topn_runtime_filter(&mut self) {
        const PUBLISH_THRESHOLD: usize = 4096;

        if self.topn_rf_specs.is_empty() {
            return;
        }
        let Some(hub) = &self.runtime_filter_hub else {
            return;
        };
        let Some(key_table) = &self.key_table else {
            return;
        };
        if self.topn_rf_rows_since_publish < PUBLISH_THRESHOLD {
            return;
        }

        for spec in &self.topn_rf_specs {
            if key_table.group_count() < spec.limit {
                continue;
            }
            // Get the group-by key column at expr_order.
            let key_columns = key_table.key_columns();
            let Some(key_col) = key_columns.get(spec.expr_order) else {
                continue;
            };
            let column_array = match key_col.to_array() {
                Ok(arr) => arr,
                Err(_) => continue,
            };
            let filter = match RuntimeMinMaxFilter::from_array(spec.build_type, &column_array) {
                Ok(f) => f,
                Err(_) => continue,
            };
            hub.publish_min_max_filter(spec.filter_id, filter);
        }
        self.topn_rf_rows_since_publish = 0;
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
            profile.common.counter_add(
                "InputRowCount",
                crate::metrics::TUnit::UNIT,
                num_rows as i64,
            );
        }
        if self.group_by.is_empty() {
            self.ensure_scalar_group().map_err(|e| e.to_string())?;
            let state_ptr = *self
                .group_states
                .get(0)
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
                    for row in 0..num_rows {
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
                            .find_or_insert_from_row(&key_views, row, row_bytes, hashes[row])
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::Scalar => {
                    return Err("group key strategy Scalar is invalid for group by".to_string());
                }
                GroupKeyStrategy::OneNumber => {
                    let view = key_views
                        .get(0)
                        .ok_or_else(|| "one number key view missing".to_string())?;
                    let hashes = key_table
                        .build_one_number_hashes(view, num_rows)
                        .map_err(|e| e.to_string())?;
                    for row in 0..num_rows {
                        let lookup = key_table
                            .find_or_insert_one_number(view, row, hashes[row])
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::OneString => {
                    let view = key_views
                        .get(0)
                        .ok_or_else(|| "one string key view missing".to_string())?;
                    let GroupKeyArrayView::Utf8(arr) = view else {
                        return Err("one string key expects Utf8 view".to_string());
                    };
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for row in 0..num_rows {
                        let lookup = if arr.is_null(row) {
                            key_table
                                .find_or_insert_one_string(view, row, None, hashes[row])
                                .map_err(|e| e.to_string())?
                        } else {
                            let key = arr.value(row);
                            key_table
                                .find_or_insert_one_string(view, row, Some(key), hashes[row])
                                .map_err(|e| e.to_string())?
                        };
                        self.ensure_group_state(&lookup)
                            .map_err(|e| e.to_string())?;
                        group_ids.push(lookup.group_id);
                    }
                }
                GroupKeyStrategy::FixedSize => {
                    let hashes = key_table
                        .build_group_hashes(&key_views, num_rows)
                        .map_err(|e| e.to_string())?;
                    for row in 0..num_rows {
                        let lookup = key_table
                            .find_or_insert_fixed_size(&key_views, row, hashes[row])
                            .map_err(|e| e.to_string())?;
                        self.ensure_group_state(&lookup)
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
                    for row in 0..num_rows {
                        let lookup = if keys[row] {
                            key_table
                                .find_or_insert_compressed(&key_views, row, hashes[row])
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
                                .find_or_insert_from_row(&key_views, row, row_bytes, hashes[row])
                                .map_err(|e| e.to_string())?
                        };
                        self.ensure_group_state(&lookup)
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

        let batch = if arrays.is_empty() {
            let options = arrow::array::RecordBatchOptions::new()
                .with_row_count(Some(self.group_states.len()));
            RecordBatch::try_new_with_options(schema, arrays, &options)
        } else {
            RecordBatch::try_new(schema, arrays)
        }
        .map_err(|e| e.to_string())?;
        self.drop_group_states();
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
        if self.finished {
            return Ok(());
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
        self.try_publish_topn_runtime_filter();
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing && self.finalized && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
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
        if failpoint::should_trigger(failpoint::FORCE_RESET_AGGREGATOR_AFTER_STREAMING_SINK_FINISH)
        {
            self.reset_after_streaming_finish();
        }
        self.pending_output = out;
        self.finalized = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

impl AggregateProcessorOperator {
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

        if let Some(table) = self.key_table.as_mut() {
            if table.key_strategy() == GroupKeyStrategy::CompressedFixed
                && table.compressed_ctx().is_none()
            {
                if group_arrays.first().map_or(0, |array| array.len()) == 0 {
                    return Ok(());
                }
                let views = build_group_key_views(group_arrays)?;
                table.ensure_compressed_ctx(&views)?;
            }
        }
        self.data_initialized = true;
        Ok(())
    }
    fn init_from_plan(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }

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
            return Chunk::try_new_with_chunk_schema(
                batch,
                Arc::new(ChunkSchema::try_new(slot_schemas)?),
            );
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
                if let Some(sig) = func.types.as_ref() {
                    if let Some(intermediate) = sig.intermediate_type.as_ref() {
                        if matches!(intermediate, DataType::Null) {
                            return Err("aggregate intermediate type is null".to_string());
                        }
                        types.push(Some(intermediate.clone()));
                        continue;
                    }
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
            if matches!(data_type, Some(DataType::Null)) {
                return Err("aggregate input type is null".to_string());
            }
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
            if expected_type != actual_type {
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
                        fields[0].data_type() == &expected_type
                    }
                    _ => false,
                };
                if actual_type != &expected_type && !is_struct_wrapped {
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
            if expected_type != array.data_type() {
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

    fn ensure_group_state(&mut self, lookup: &KeyLookup) -> Result<(), String> {
        if lookup.is_new {
            self.alloc_group_state(lookup.group_id)?;
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
