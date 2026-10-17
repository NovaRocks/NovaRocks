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
//! Core probe engine for hash-join output assembly.
//!
//! Responsibilities:
//! - Executes key lookup, match expansion, and join-type specific row construction.
//! - Implements null-aware semantics and row-shaping rules reused by probe processors.
//!
//! Key exported interfaces:
//! - Types: `HashJoinProbeCore`.
//! - Functions: `join_type_str`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::join_hash_map::build_store::BuildStoreChunkProjection;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::build_artifact::{BuildView, JoinBuildArtifact};
#[cfg(test)]
use super::build_requirements;
use super::build_requirements::{BuildComponentRequirements, required_build_components};
use super::join_hash_map::finalize::{finalize_probe_rows, is_all_match_one, mark_build_matches};
use super::join_hash_map::gather::GatherTimings;
use super::join_hash_map::match_flags::BuildMatchFlags;
use super::join_hash_map::method::{ExprEvaluationCallback, JoinHashMap};
use super::join_hash_map::search::{JoinSelection, SearchStats, append_cross_selection};
use super::join_probe_utils::cross_join_batches;
use crate::exec::chunk::{
    ArrayNormalizationStep, Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema,
};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::profile::{CounterRef, clamp_u128_to_i64};

type JoinInputCreator<'a> =
    dyn FnMut(RecordBatch, ChunkSchemaRef, &[&Chunk], &[usize]) -> ExecutionResult<Chunk> + 'a;

#[cfg(test)]
fn create_join_evaluation_input(
    batch: RecordBatch,
    schema: ChunkSchemaRef,
    sources: &[&Chunk],
    factory: &dyn InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    create_join_input(batch, schema, sources, &[], factory)
}

fn create_join_input(
    batch: RecordBatch,
    schema: ChunkSchemaRef,
    sources: &[&Chunk],
    nullable_slots: &[usize],
    factory: &dyn InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    let owner = factory.begin_typed_construction(&schema)?;
    let mut storage = owner.storage_lifetime();
    for source in sources {
        if !source.chunk_schema().is_typed() {
            return Err(ConstructionFailure::InvalidSource(
                "join expression input requires original typed source bindings".into(),
            )
            .into());
        }
        let source_storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        storage = storage.retaining_observed::<ConstructionFailure>(&source_storage, |layout| {
            owner.request(layout)
        })?;
    }
    // Only the join's explicit NULL-padding author may widen a root. The
    // canonical child declarations and occurrence identities remain original.
    let schema = if nullable_slots.is_empty() {
        schema
    } else {
        let schema_storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(&schema.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        owner.request(
            std::alloc::Layout::array::<ChunkSlotSchema>(schema.slots().len()).map_err(|_| {
                ConstructionFailure::InvalidSource("join output slot layout overflow".into())
            })?,
        )?;
        let mut slots = Vec::with_capacity(schema.slots().len());
        for (index, slot) in schema.slots().iter().enumerate() {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
            if !nullable_slots.contains(&index) || slot.nullable() {
                slots.push(slot.clone());
                continue;
            }
            let slot_storage = owner
                .storage_lifetime()
                .retaining_observed::<ConstructionFailure>(&slot.storage_lifetime(), |layout| {
                    owner.request(layout)
                })?;
            let binding = slot.typed_binding().with_nullable_observed(
                true,
                slot_storage.clone(),
                |layout| owner.request(layout),
                |units| {
                    owner.observe(
                        0,
                        &[],
                        ArrayNormalizationStep::InspectConstructionWork { units },
                    )
                },
            )?;
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::RebuildField {
                    source: slot.field(),
                    target: binding.data_type(),
                    name: slot.name(),
                },
            )?;
            let field = Arc::new(slot.field().clone().with_nullable(true));
            slots.push(ChunkSlotSchema::try_new_typed_observed::<
                ConstructionFailure,
                _,
            >(
                slot.slot_id(),
                field,
                binding,
                slot.unique_id(),
                slot_storage,
                |step| owner.observe(0, &[], step),
            )?);
        }
        let schema = ChunkSchema::try_new_typed_observed::<ConstructionFailure>(
            slots,
            schema_storage,
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<ChunkSchema>(),
            )
            .map_err(ConstructionFailure::from)?,
        )?;
        Arc::new(schema)
    };
    Ok(Chunk::try_new_typed_record_batch_owned_observed::<
        ConstructionFailure,
        _,
        _,
    >(
        batch,
        schema,
        storage,
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |step| owner.observe(0, &[], step),
    )?)
}

#[cfg(test)]
fn reference_join_evaluation_input(
    batch: RecordBatch,
    schema: ChunkSchemaRef,
    sources: &[&Chunk],
    nullable_slots: &[usize],
) -> ExecutionResult<Chunk> {
    if schema.is_typed()
        || sources
            .iter()
            .any(|source| source.chunk_schema().is_typed())
    {
        return Err("reference join evaluation requires explicitly untyped source facts".into());
    }
    let schema = if nullable_slots.is_empty() {
        schema
    } else {
        Arc::new(ChunkSchema::try_new(
            schema
                .slots()
                .iter()
                .enumerate()
                .map(|(index, slot)| {
                    if nullable_slots.contains(&index) {
                        slot.with_nullable(true)
                    } else {
                        slot.clone()
                    }
                })
                .collect(),
        )?)
    };
    Ok(Chunk::try_new_with_chunk_schema(batch, schema)?)
}

fn concat_compatible_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    context: &str,
) -> Result<RecordBatch, String> {
    for batch in batches {
        if schema.fields().len() != batch.num_columns() {
            return Err(format!(
                "{context} column count mismatch: schema_fields={} batch_columns={}",
                schema.fields().len(),
                batch.num_columns()
            ));
        }
        for (idx, column) in batch.columns().iter().enumerate() {
            let descriptor = schema.field(idx).data_type();
            if descriptor != column.data_type() {
                return Err(format!(
                    "{context} type mismatch at column {idx}: descriptor={:?} actual={:?}",
                    descriptor,
                    column.data_type()
                ));
            }
        }
    }
    let concat_schema = concat_schema_for_batches(schema, batches);
    concat_batches(&concat_schema, batches).map_err(|e| e.to_string())
}

fn concat_schema_for_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> SchemaRef {
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut changed = false;

    for batch in batches {
        let batch_schema = batch.schema();
        for (idx, field) in fields.iter_mut().enumerate() {
            let nullable = field.is_nullable()
                || batch_schema.field(idx).is_nullable()
                || batch.column(idx).null_count() > 0;
            if nullable != field.is_nullable() {
                *field = field.clone().with_nullable(nullable);
                changed = true;
            }
        }
    }

    if !changed {
        return Arc::clone(schema);
    }
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Core hash-join probing engine that performs key lookup and join-type specific row assembly.
pub(crate) struct HashJoinProbeCore {
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    has_equi_keys: bool,
    left_chunk_schema: ChunkSchemaRef,
    right_chunk_schema: ChunkSchemaRef,
    join_scope_chunk_schema: ChunkSchemaRef,
    join_scope_schema: SchemaRef,
    build_loaded: bool,
    build_requirements: Option<BuildComponentRequirements>,
    build_view: Option<BuildView>,
    build_chunk: Option<BuildStoreChunkProjection>,
    build_null_key_rows: Option<Arc<Vec<u32>>>,
    build_table: Option<Arc<JoinHashMap>>,
    output_schema: Option<SchemaRef>,
    build_matched: Option<BuildMatchFlags>,
    global_build_row_count: usize,
    global_build_has_null_key: bool,
    build_partition_row_count: usize,
    build_partition_has_null_key: bool,
    output_rows: u64,
    lookup_hit_rows: u64,
    lookup_miss_rows: u64,
    residual_rows_checked: u64,
    residual_eval_pairs: u64,
    residual_eval_batches: u64,
    residual_matched_rows: u64,
    residual_group_rows_total: u64,
    pending_output_batches: VecDeque<Chunk>,
    search_timer: Option<CounterRef>,
    out_build_timer: Option<CounterRef>,
    out_probe_timer: Option<CounterRef>,
}

#[allow(
    dead_code,
    reason = "Probe helpers are retained for join-shape coverage and native runtime-filter integration."
)]
impl HashJoinProbeCore {
    #[expect(
        clippy::too_many_arguments,
        reason = "The probe core constructor owns the complete join plan contract."
    )]
    pub(crate) fn new_native(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        has_equi_keys: bool,
        left_chunk_schema: ChunkSchemaRef,
        right_chunk_schema: ChunkSchemaRef,
        join_scope_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self::new_in_mode(
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            has_equi_keys,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_chunk_schema,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_in_mode(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        has_equi_keys: bool,
        left_chunk_schema: ChunkSchemaRef,
        right_chunk_schema: ChunkSchemaRef,
        join_scope_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            has_equi_keys,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_schema: join_scope_chunk_schema.arrow_schema_ref(),
            join_scope_chunk_schema,
            build_loaded: false,
            build_requirements: None,
            build_view: None,
            build_chunk: None,
            build_null_key_rows: None,
            build_table: None,
            output_schema: None,
            build_matched: None,
            global_build_row_count: 0,
            global_build_has_null_key: false,
            build_partition_row_count: 0,
            build_partition_has_null_key: false,
            output_rows: 0,
            lookup_hit_rows: 0,
            lookup_miss_rows: 0,
            residual_rows_checked: 0,
            residual_eval_pairs: 0,
            residual_eval_batches: 0,
            residual_matched_rows: 0,
            residual_group_rows_total: 0,
            pending_output_batches: VecDeque::new(),
            search_timer: None,
            out_build_timer: None,
            out_probe_timer: None,
        }
    }

    pub(crate) fn set_phase_timers(
        &mut self,
        search_timer: CounterRef,
        out_build_timer: CounterRef,
        out_probe_timer: CounterRef,
    ) {
        self.search_timer = Some(search_timer);
        self.out_build_timer = Some(out_build_timer);
        self.out_probe_timer = Some(out_probe_timer);
    }

    #[inline]
    fn record_timer_ns(timer: Option<&CounterRef>, start: std::time::Instant) {
        if let Some(timer) = timer {
            timer.add(clamp_u128_to_i64(start.elapsed().as_nanos()));
        }
    }

    #[inline]
    fn add_timer_ns(timer: Option<&CounterRef>, value: i64) {
        if let Some(timer) = timer {
            timer.add(value);
        }
    }

    #[inline]
    fn record_search_ns(&self, start: std::time::Instant) {
        Self::record_timer_ns(self.search_timer.as_ref(), start);
    }

    #[inline]
    fn record_out_build_ns(&self, start: std::time::Instant) {
        Self::record_timer_ns(self.out_build_timer.as_ref(), start);
    }

    #[inline]
    fn record_out_probe_ns(&self, start: std::time::Instant) {
        Self::record_timer_ns(self.out_probe_timer.as_ref(), start);
    }

    #[inline]
    fn record_out_timings(&self, timings: GatherTimings) {
        Self::add_timer_ns(self.out_build_timer.as_ref(), timings.build_ns);
        Self::add_timer_ns(self.out_probe_timer.as_ref(), timings.probe_ns);
    }

    fn build_view(&self) -> Result<&BuildView, String> {
        self.build_view
            .as_ref()
            .ok_or_else(|| "hash join build artifact not loaded".to_string())
    }

    fn require_build_chunk(
        &self,
        context: &'static str,
    ) -> Result<Option<BuildStoreChunkProjection>, String> {
        if self.build_partition_row_count == 0 {
            return Ok(None);
        }
        self.build_view()?.build_chunk(context).map(Some)
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[allow(dead_code)] // used by planned right-semi/anti join paths
    pub(crate) fn probe_chunk_schema(&self) -> &ChunkSchemaRef {
        if self.probe_is_left {
            &self.left_chunk_schema
        } else {
            &self.right_chunk_schema
        }
    }

    pub(crate) fn build_chunk_schema(&self) -> &ChunkSchemaRef {
        if self.probe_is_left {
            &self.right_chunk_schema
        } else {
            &self.left_chunk_schema
        }
    }

    pub(crate) fn join_scope_chunk_schema(&self) -> &ChunkSchemaRef {
        &self.join_scope_chunk_schema
    }

    fn outer_nullable_slots(&self) -> Vec<usize> {
        let left_len = self.left_chunk_schema.slots().len();
        (0..self.join_scope_chunk_schema.slots().len())
            .filter(|&index| match self.join_type {
                JoinType::LeftOuter => index >= left_len,
                JoinType::RightOuter => index < left_len,
                JoinType::FullOuter => true,
                _ => false,
            })
            .collect()
    }

    fn outer_output_schema(&self) -> SchemaRef {
        let nullable_slots = self.outer_nullable_slots();
        Arc::new(Schema::new(
            self.join_scope_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    if nullable_slots.contains(&index) {
                        Arc::new(field.as_ref().clone().with_nullable(true))
                    } else {
                        Arc::clone(field)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    }

    pub(crate) fn is_build_loaded(&self) -> bool {
        self.build_loaded
    }

    pub(crate) fn probe_is_left(&self) -> bool {
        self.probe_is_left
    }

    pub(crate) fn set_build_artifact(
        &mut self,
        artifact: Arc<JoinBuildArtifact>,
        global_build_row_count: usize,
        global_build_has_null_key: bool,
    ) -> Result<(), String> {
        if self.build_loaded {
            return Ok(());
        }
        let requirements = required_build_components(
            self.join_type,
            self.residual_predicate.is_some(),
            self.probe_is_left,
            self.has_equi_keys,
        );
        let view = BuildView::new(Arc::clone(&artifact), requirements)?;
        self.build_chunk = view.optional_build_chunk();
        self.build_null_key_rows = view.build_null_key_rows();
        self.build_table = view.build_table();
        self.build_partition_row_count = view.build_row_count();
        self.build_partition_has_null_key = view.build_has_null_key();
        self.global_build_row_count = global_build_row_count;
        self.global_build_has_null_key = global_build_has_null_key;
        if requirements.requires_match_flags() {
            self.build_matched = Some(BuildMatchFlags::new(view.build_row_count()));
        }
        self.build_requirements = Some(requirements);
        self.build_view = Some(view);
        self.build_loaded = true;
        Ok(())
    }

    pub(crate) fn probe_keys_len(&self) -> usize {
        self.probe_keys.len()
    }

    pub(crate) fn has_residual_predicate(&self) -> bool {
        self.residual_predicate.is_some()
    }

    pub(crate) fn build_store_rows(&self) -> usize {
        self.build_chunk
            .as_ref()
            .map(|chunk| chunk.len())
            .unwrap_or(0)
    }

    pub(crate) fn build_table_present(&self) -> bool {
        self.build_table.is_some()
    }

    pub(crate) fn output_rows(&self) -> u64 {
        self.output_rows
    }

    pub(crate) fn build_partition_row_count(&self) -> usize {
        self.build_partition_row_count
    }

    pub(crate) fn build_partition_has_null_key(&self) -> bool {
        self.build_partition_has_null_key
    }

    pub(crate) fn lookup_hit_rows(&self) -> u64 {
        self.lookup_hit_rows
    }

    pub(crate) fn lookup_miss_rows(&self) -> u64 {
        self.lookup_miss_rows
    }

    pub(crate) fn residual_rows_checked(&self) -> u64 {
        self.residual_rows_checked
    }

    pub(crate) fn residual_eval_pairs(&self) -> u64 {
        self.residual_eval_pairs
    }

    pub(crate) fn residual_eval_batches(&self) -> u64 {
        self.residual_eval_batches
    }

    pub(crate) fn residual_matched_rows(&self) -> u64 {
        self.residual_matched_rows
    }

    pub(crate) fn residual_group_rows_total(&self) -> u64 {
        self.residual_group_rows_total
    }

    pub(crate) fn has_pending_output(&self) -> bool {
        !self.pending_output_batches.is_empty()
    }

    pub(crate) fn pop_pending_output(&mut self) -> Result<Option<Chunk>, String> {
        Ok(self.pending_output_batches.pop_front())
    }

    /// Extend a probe-only batch with NULL-filled build-side columns so that
    /// the result matches the full join-scope schema.  This mirrors StarRocks
    /// BE's `_build_default_output` for SEMI/ANTI joins: the pruned side's
    /// slots must still appear in the output chunk (as NULLs) because
    /// downstream operators (SORT sort_tuple_slot_exprs, EXCHANGE, ANALYTIC)
    /// may reference them.
    fn extend_with_null_build_columns(
        &self,
        probe_batch: RecordBatch,
        sources: &[&Chunk],
        create_input: &mut JoinInputCreator<'_>,
    ) -> ExecutionResult<Chunk> {
        use arrow::array::new_null_array;
        let num_rows = probe_batch.num_rows();
        let output_start = std::time::Instant::now();
        let build_schema = self.build_chunk_schema();
        let build_col_count = build_schema.slots().len();
        let columns: Vec<arrow::array::ArrayRef> = if self.probe_is_left {
            let mut columns = probe_batch.columns().to_vec();
            for slot in build_schema.slots() {
                columns.push(new_null_array(slot.field().data_type(), num_rows));
            }
            columns
        } else {
            let mut columns = Vec::with_capacity(build_col_count + probe_batch.num_columns());
            for slot in build_schema.slots() {
                columns.push(new_null_array(slot.field().data_type(), num_rows));
            }
            columns.extend(probe_batch.columns().iter().cloned());
            columns
        };
        let nullable_slots = (0..self.join_scope_chunk_schema.slots().len())
            .filter(|&index| {
                if self.probe_is_left {
                    index >= probe_batch.num_columns()
                } else {
                    index < build_col_count
                }
            })
            .collect::<Vec<_>>();
        let fields = self
            .join_scope_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if nullable_slots.contains(&index) {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| error.to_string())?;
        let chunk = create_input(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
            sources,
            &nullable_slots,
        )?;
        self.record_out_build_ns(output_start);
        Ok(chunk)
    }

    /// Extend a build-only batch with NULL-filled probe-side columns.
    /// Mirror of `extend_with_null_build_columns` for RIGHT SEMI/ANTI.
    fn extend_with_null_probe_columns(
        &self,
        build_batch: RecordBatch,
        sources: &[&Chunk],
        create_input: &mut JoinInputCreator<'_>,
    ) -> ExecutionResult<Chunk> {
        use arrow::array::new_null_array;
        let num_rows = build_batch.num_rows();
        let output_start = std::time::Instant::now();
        let probe_schema = self.probe_chunk_schema();
        let probe_col_count = probe_schema.slots().len();
        let mut columns: Vec<arrow::array::ArrayRef> =
            Vec::with_capacity(probe_col_count + build_batch.num_columns());
        for slot in probe_schema.slots() {
            columns.push(new_null_array(slot.field().data_type(), num_rows));
        }
        columns.extend(build_batch.columns().iter().cloned());
        let nullable_slots = (0..probe_col_count).collect::<Vec<_>>();
        let fields = self
            .join_scope_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if nullable_slots.contains(&index) {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| error.to_string())?;
        let chunk = create_input(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
            sources,
            &nullable_slots,
        )?;
        self.record_out_probe_ns(output_start);
        Ok(chunk)
    }

    pub(crate) fn right_semi_anti_output_chunk(
        &mut self,
        build_output: Option<RecordBatch>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> ExecutionResult<Option<Chunk>> {
        let Some(build_batch) = build_output else {
            return Ok(None);
        };
        self.output_rows = self
            .output_rows
            .saturating_add(build_batch.num_rows() as u64);
        Ok(Some(
            self.extend_with_null_probe_columns(
                build_batch,
                &self
                    .build_chunk
                    .as_ref()
                    .map(|build| vec![build.as_ref()])
                    .unwrap_or_default(),
                create_input,
            )?,
        ))
    }

    #[cfg(test)]
    pub(crate) fn join_probe_chunks(
        &mut self,
        probe_chunks: Vec<Chunk>,
    ) -> Result<Option<Chunk>, String> {
        if probe_chunks
            .iter()
            .any(|input| input.chunk_schema().is_typed())
        {
            return Err("reference join evaluation requires untyped inputs".into());
        }
        let arena = Arc::clone(&self.arena);
        self.join_probe_chunks_with(
            probe_chunks,
            &mut |id, input| {
                if input.chunk_schema().is_typed() {
                    return Err(
                        "reference join evaluation requires an untyped derived input".into(),
                    );
                }
                arena.eval(id, input).map_err(Into::into)
            },
            &mut reference_join_evaluation_input,
        )
        .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub(crate) fn finish(
        &mut self,
        probe_chunks: Vec<Chunk>,
        flags: Option<Vec<bool>>,
    ) -> Result<Option<Chunk>, String> {
        let out = self.join_probe_chunks(probe_chunks)?;
        self.finish_from_probe_output(out, flags, true)
    }

    pub(crate) fn join_probe_chunks_evaluated(
        &mut self,
        probe_chunks: Vec<Chunk>,
        factory: &dyn InputConstructionFactory,
        evaluate: &mut ExprEvaluationCallback<'_>,
    ) -> ExecutionResult<Option<Chunk>> {
        self.join_probe_chunks_with(
            probe_chunks,
            evaluate,
            &mut |batch, schema, sources, nullable_slots| {
                create_join_input(batch, schema, sources, nullable_slots, factory)
            },
        )
    }

    fn join_probe_chunks_with(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        if probe_chunks.is_empty() {
            return Ok(None);
        }
        let probe_chunks = self.apply_runtime_filters(probe_chunks)?;
        if probe_chunks.is_empty() {
            return Ok(None);
        }

        match self.join_type {
            JoinType::Inner => Ok((self.join_inner(probe_chunks, evaluate, create_input))?),
            JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                Ok((self.join_outer(probe_chunks, evaluate, create_input))?)
            }
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti => {
                Ok((self.join_semi_anti(probe_chunks, evaluate, create_input))?)
            }
            JoinType::NullAwareLeftAnti => {
                Ok((self.join_null_aware_left_anti(probe_chunks, evaluate, create_input))?)
            }
        }
    }

    pub(crate) fn finish_evaluated(
        &mut self,
        probe_chunks: Vec<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
        factory: &dyn InputConstructionFactory,
        evaluate: &mut ExprEvaluationCallback<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        let out = self.join_probe_chunks_evaluated(probe_chunks, factory, evaluate)?;
        self.finish_from_probe_output_evaluated(out, merged_build_flags, true, factory)
    }

    #[cfg(test)]
    pub(crate) fn finish_from_probe_output(
        &mut self,
        out: Option<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
        count_build_rows: bool,
    ) -> Result<Option<Chunk>, String> {
        self.finish_from_probe_output_with(
            out,
            merged_build_flags,
            count_build_rows,
            &mut reference_join_evaluation_input,
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn finish_from_probe_output_evaluated(
        &mut self,
        out: Option<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
        count_build_rows: bool,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Option<Chunk>> {
        self.finish_from_probe_output_with(
            out,
            merged_build_flags,
            count_build_rows,
            &mut |batch, schema, sources, nullable_slots| {
                create_join_input(batch, schema, sources, nullable_slots, factory)
            },
        )
    }

    fn finish_from_probe_output_with(
        &mut self,
        mut out: Option<Chunk>,
        merged_build_flags: Option<Vec<bool>>,
        count_build_rows: bool,
        create_input: &mut JoinInputCreator<'_>,
    ) -> ExecutionResult<Option<Chunk>> {
        match self.join_type {
            JoinType::RightAnti if self.probe_is_left => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let build_out = self.build_right_semi_anti_output_with_flags(&flags, false)?;
                out = self.right_semi_anti_output_chunk(build_out, create_input)?;
            }
            JoinType::RightSemi if self.probe_is_left => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let build_out = self.build_right_semi_anti_output_with_flags(&flags, true)?;
                out = self.right_semi_anti_output_chunk(build_out, create_input)?;
            }
            JoinType::FullOuter | JoinType::RightOuter => {
                let flags = merged_build_flags
                    .or_else(|| self.take_build_matched())
                    .unwrap_or_default();
                let build_unmatched = self.build_unmatched_build_output_from_flags(&flags)?;
                out =
                    self.merge_join_outputs(out, build_unmatched, count_build_rows, create_input)?;
            }
            _ => {}
        }
        Ok(out)
    }

    fn compact_selection_by_residual(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        selection: &mut JoinSelection,
        pred: ExprId,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<(), ExecutionFailure> {
        if selection.is_empty() {
            return Ok(());
        }
        let mut kept = JoinSelection::new();
        let mut offset = 0usize;
        while offset < selection.len() {
            let end = (offset
                + crate::exec::operators::hashjoin::join_hash_map::gather::MAX_JOIN_OUTPUT_ROWS_PER_BATCH)
                .min(selection.len());
            let gathered =
                crate::exec::operators::hashjoin::join_hash_map::gather::gather_probe_build_batches(
                    probe,
                    build,
                    &selection.probe[offset..end],
                    &selection.build[offset..end],
                    &self.join_scope_schema,
                    self.probe_is_left,
                    false,
                )?;
            self.record_out_timings(gathered.timings);
            let candidate = gathered
                .batches
                .into_iter()
                .next()
                .ok_or_else(|| "join residual candidate batch missing".to_string())?;
            let candidate_chunk = create_input(
                candidate,
                Arc::clone(&self.join_scope_chunk_schema),
                &[probe, build],
                &[],
            )?;
            let mask_arr = evaluate(pred, &candidate_chunk)?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "join residual predicate must return boolean array".to_string())?;
            if mask.len() != end - offset {
                return Err((format!(
                    "join residual mask length mismatch: mask={} selection={}",
                    mask.len(),
                    end - offset
                ))
                .into());
            }
            let matched_before = kept.len();
            for mask_idx in 0..mask.len() {
                if mask.is_valid(mask_idx) && mask.value(mask_idx) {
                    kept.push(
                        selection.probe[offset + mask_idx],
                        selection.build[offset + mask_idx],
                    );
                }
            }
            self.residual_eval_batches = self.residual_eval_batches.saturating_add(1);
            self.residual_eval_pairs = self
                .residual_eval_pairs
                .saturating_add((end - offset) as u64);
            self.residual_matched_rows = self
                .residual_matched_rows
                .saturating_add((kept.len() - matched_before) as u64);
            offset = end;
        }
        *selection = kept;
        Ok(())
    }

    fn mark_probe_matches_from_selection(
        matched: &mut [bool],
        selection: &JoinSelection,
        context: &str,
    ) -> Result<u64, String> {
        let mut newly_marked = 0u64;
        for &probe_row in &selection.probe {
            let slot = probe_row as usize;
            let Some(matched_slot) = matched.get_mut(slot) else {
                return Err(format!(
                    "{context} probe row out of bounds: row={} rows={}",
                    slot,
                    matched.len()
                ));
            };
            if !*matched_slot {
                *matched_slot = true;
                newly_marked = newly_marked.saturating_add(1);
            }
        }
        Ok(newly_marked)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Residual-match bookkeeping is kept explicit to avoid a mutable shared state bundle."
    )]
    fn compact_null_aware_selection(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        selection: &mut JoinSelection,
        pred: ExprId,
        matched: &mut [bool],
        residual_matched_probe_rows: &mut [bool],
        context: &str,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<(), ExecutionFailure> {
        if selection.is_empty() {
            return Ok(());
        }

        let residual_matched_rows_before = self.residual_matched_rows;
        self.compact_selection_by_residual(probe, build, selection, pred, evaluate, create_input)?;
        Self::mark_probe_matches_from_selection(matched, selection, context)?;
        let unique_probe_rows = Self::mark_probe_matches_from_selection(
            residual_matched_probe_rows,
            selection,
            context,
        )?;
        self.residual_matched_rows = residual_matched_rows_before.saturating_add(unique_probe_rows);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Cross-product probing keeps its independently-owned buffers explicit."
    )]
    fn compact_cross_selection_in_chunks(
        &mut self,
        probe: &Chunk,
        build: &Chunk,
        probe_rows: &[u32],
        build_rows: &[u32],
        pred: ExprId,
        matched: &mut [bool],
        residual_matched_probe_rows: &mut [bool],
        context: &str,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<(), ExecutionFailure> {
        if probe_rows.is_empty() || build_rows.is_empty() {
            return Ok(());
        }

        const MAX_CROSS_SELECTION_PAIRS: usize = 16 * 1024;
        let mut probe_pos = 0usize;
        let mut build_pos = 0usize;
        while probe_pos < probe_rows.len() {
            let mut selection = JoinSelection::new();
            while probe_pos < probe_rows.len() && selection.len() < MAX_CROSS_SELECTION_PAIRS {
                let probe_row = probe_rows[probe_pos];
                let probe_slot = probe_row as usize;
                let Some(matched_slot) = matched.get(probe_slot) else {
                    return Err((format!(
                        "{context} probe row out of bounds: row={} rows={}",
                        probe_slot,
                        matched.len()
                    ))
                    .into());
                };
                if *matched_slot {
                    probe_pos += 1;
                    build_pos = 0;
                    continue;
                }
                if build_pos >= build_rows.len() {
                    probe_pos += 1;
                    build_pos = 0;
                    continue;
                }

                let before_len = selection.len();
                let stopped = append_cross_selection(
                    &mut selection,
                    &[probe_row],
                    &build_rows[build_pos..],
                    MAX_CROSS_SELECTION_PAIRS,
                );
                let appended = selection.len() - before_len;
                build_pos = build_pos
                    .checked_add(appended)
                    .ok_or_else(|| "join residual cross selection overflow".to_string())?;
                if build_pos >= build_rows.len() {
                    probe_pos += 1;
                    build_pos = 0;
                }
                if stopped {
                    break;
                }
            }

            if selection.is_empty() {
                continue;
            }
            self.compact_null_aware_selection(
                probe,
                build,
                &mut selection,
                pred,
                matched,
                residual_matched_probe_rows,
                context,
                evaluate,
                create_input,
            )?;
        }
        Ok(())
    }

    fn build_unmatched_build_output_from_flags(
        &mut self,
        flags: &[bool],
    ) -> Result<Option<RecordBatch>, String> {
        let Some(build_chunk) = self.require_build_chunk("unmatched build output")? else {
            return Ok(None);
        };
        if flags.len() != build_chunk.len() {
            return Err(format!(
                "join build match flags length mismatch: flags={} build_rows={}",
                flags.len(),
                build_chunk.len()
            ));
        }
        let indices = flags
            .iter()
            .enumerate()
            .filter_map(|(row, matched)| (!matched).then_some(row as u32))
            .collect::<Vec<_>>();
        let output_start = std::time::Instant::now();
        let output_schema = self.outer_output_schema();
        let out = if self.probe_is_left {
            crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                &build_chunk,
                &indices,
                &self.left_chunk_schema.arrow_schema_ref(),
                &output_schema,
            )?
        } else {
            crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                &build_chunk,
                &indices,
                &self.right_chunk_schema.arrow_schema_ref(),
                &output_schema,
            )?
        };
        self.record_out_build_ns(output_start);
        Ok(out)
    }

    /// Take ownership of the local build-matched flags.  Used by broadcast
    /// join to merge per-driver flags into a shared accumulator.
    pub(crate) fn take_build_matched(&mut self) -> Option<Vec<bool>> {
        Some(self.build_matched.take()?.into_vec())
    }

    /// Produce output from externally-provided build-matched flags (e.g.
    /// after merging flags from all broadcast probe drivers).
    pub(crate) fn build_right_semi_anti_output_with_flags(
        &mut self,
        flags: &[bool],
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        self.build_right_semi_anti_output_from_flat_flags(flags, want_matched)
    }

    pub(crate) fn build_right_semi_anti_output(
        &mut self,
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        if self
            .build_chunk
            .as_ref()
            .map(|chunk| chunk.is_empty())
            .unwrap_or(true)
        {
            return Ok(None);
        }
        let Some(flags) = self.build_matched.as_ref() else {
            return Ok(None);
        };
        // Clone to satisfy borrow checker (flags borrows self).
        let flags = flags.clone().into_vec();
        self.build_right_semi_anti_output_from_flat_flags(&flags, want_matched)
    }

    fn build_right_semi_anti_output_from_flat_flags(
        &mut self,
        flags: &[bool],
        want_matched: bool,
    ) -> Result<Option<RecordBatch>, String> {
        let Some(build_chunk) = self.build_chunk.as_ref() else {
            return Ok(None);
        };
        if flags.len() != build_chunk.len() {
            return Err(format!(
                "join build match flags length mismatch: flags={} build_rows={}",
                flags.len(),
                build_chunk.len()
            ));
        }
        let mask = flags
            .iter()
            .map(|v| if want_matched { *v } else { !*v })
            .collect::<Vec<_>>();
        let mask = BooleanArray::from(mask);
        let output_start = std::time::Instant::now();
        let filtered =
            filter_record_batch(&build_chunk.record_batch(), &mask).map_err(|e| e.to_string())?;
        self.record_out_build_ns(output_start);
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(filtered))
    }

    pub(crate) fn merge_join_outputs(
        &mut self,
        left: Option<Chunk>,
        right: Option<RecordBatch>,
        count_right_rows: bool,
        create_input: &mut JoinInputCreator<'_>,
    ) -> ExecutionResult<Option<Chunk>> {
        if count_right_rows && let Some(batch) = right.as_ref() {
            self.output_rows = self.output_rows.saturating_add(batch.num_rows() as u64);
        }
        let build = self.build_chunk.clone();
        let mut sources = left.iter().collect::<Vec<_>>();
        if let Some(build) = &build {
            sources.push(build.as_ref());
        }
        let nullable_slots = self.outer_nullable_slots();
        let output_schema = self.outer_output_schema();
        let batch = match (left.as_ref(), right) {
            (None, None) => return Ok(None),
            (Some(_), None) => return Ok(left),
            (None, Some(batch)) => batch,
            (Some(left_chunk), Some(right_batch)) => {
                if right_batch.num_rows() == 0 {
                    return Ok(left);
                }
                if left_chunk.is_empty() {
                    right_batch
                } else {
                    let batches = vec![left_chunk.record_batch().clone(), right_batch];
                    concat_compatible_batches(&output_schema, &batches, "join merge")?
                }
            }
        };
        Ok(Some(create_input(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
            &sources,
            &nullable_slots,
        )?))
    }

    fn join_inner(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        let build_chunk_opt = self.build_chunk.clone();
        if self.probe_keys.is_empty()
            && build_chunk_opt
                .as_ref()
                .map(|chunk| chunk.is_empty())
                .unwrap_or(true)
        {
            return Ok(None);
        }
        let output_schema = Arc::clone(
            self.output_schema
                .get_or_insert_with(|| Arc::clone(&self.join_scope_schema)),
        );

        let mut output_batches = Vec::new();
        let mut residual_applied_during_selection = false;
        if self.probe_keys.is_empty() {
            let Some(right) = build_chunk_opt.as_ref() else {
                return Ok(None);
            };
            for left in &probe_chunks {
                let batches = cross_join_batches(&left, right, &output_schema)?;
                output_batches.extend(batches);
            }
        } else {
            let Some(table) = self.build_table.clone() else {
                return Ok(None);
            };
            let Some(build_chunk) = self.build_chunk.clone() else {
                return Ok(None);
            };
            if table.is_empty() || build_chunk.is_empty() {
                return Ok(None);
            }
            residual_applied_during_selection = self.residual_predicate.is_some();
            for probe in &probe_chunks {
                let search_start = std::time::Instant::now();
                let (mut selection, stats) =
                    table.search_pairs_evaluated(&self.probe_keys, &probe, evaluate)?;
                self.record_search_ns(search_start);
                self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
                self.lookup_miss_rows =
                    self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);
                if selection.is_empty() {
                    continue;
                }
                if let Some(pred) = self.residual_predicate {
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(selection.len() as u64);
                    self.compact_selection_by_residual(
                        &probe,
                        &build_chunk,
                        &mut selection,
                        pred,
                        evaluate,
                        create_input,
                    )?;
                }
                if selection.is_empty() {
                    continue;
                }
                let all_match_one = is_all_match_one(&selection, probe.len());
                let gathered =
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_probe_build_batches(
                        &probe,
                        build_chunk.as_ref(),
                        &selection.probe,
                        &selection.build,
                        &output_schema,
                        self.probe_is_left,
                        all_match_one,
                    )?;
                self.record_out_timings(gathered.timings);
                output_batches.extend(gathered.batches);
            }
        }

        if !residual_applied_during_selection && let Some(pred) = self.residual_predicate {
            let mut filtered = Vec::with_capacity(output_batches.len());
            for batch in output_batches.into_iter() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let mut sources = probe_chunks.iter().collect::<Vec<_>>();
                if let Some(build) = build_chunk_opt.as_ref() {
                    sources.push(build.as_ref());
                }
                let chunk = create_input(
                    batch,
                    Arc::clone(&self.join_scope_chunk_schema),
                    &sources,
                    &[],
                )?;
                let mask_arr = evaluate(pred, &chunk)?;
                let mask = mask_arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        "join residual predicate must return boolean array".to_string()
                    })?;
                let filtered_batch = filter_record_batch(&chunk.record_batch(), mask)
                    .map_err(|e| format!("join residual filter failed: {e}"))?;
                if filtered_batch.num_rows() > 0 {
                    filtered.push(filtered_batch);
                }
            }
            output_batches = filtered;
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let mut sources = probe_chunks.iter().collect::<Vec<_>>();
        if let Some(build) = &build_chunk_opt {
            sources.push(build.as_ref());
        }
        let first = create_input(
            output_batches.remove(0),
            Arc::clone(&self.join_scope_chunk_schema),
            &sources,
            &[],
        )?;
        for batch in output_batches {
            self.pending_output_batches.push_back(create_input(
                batch,
                Arc::clone(&self.join_scope_chunk_schema),
                &sources,
                &[],
            )?);
        }
        Ok(Some(first))
    }

    fn join_outer(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        if self.probe_keys.is_empty() {
            return Err(("outer join requires non-empty eq join keys".to_string()).into());
        }
        let output_schema = self.outer_output_schema();
        let output_unmatched_probe =
            matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter);
        let track_build_matches =
            matches!(self.join_type, JoinType::FullOuter | JoinType::RightOuter);

        let table_opt = self.build_table.clone();
        let build_chunk_opt = self.build_chunk.clone();
        let has_build = table_opt.as_ref().map(|t| !t.is_empty()).unwrap_or(false)
            && build_chunk_opt
                .as_ref()
                .map(|chunk| !chunk.is_empty())
                .unwrap_or(false);

        let mut output_batches = Vec::new();
        for probe in &probe_chunks {
            if probe.is_empty() {
                continue;
            }

            if !has_build {
                if !output_unmatched_probe {
                    continue;
                }
                let indices = (0..probe.len()).map(|i| i as u32).collect::<Vec<_>>();
                let output_start = std::time::Instant::now();
                let batch = if self.probe_is_left {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                        &probe,
                        &indices,
                        &self.right_chunk_schema.arrow_schema_ref(),
                        &output_schema,
                    )?
                } else {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                        &probe,
                        &indices,
                        &self.left_chunk_schema.arrow_schema_ref(),
                        &output_schema,
                    )?
                };
                self.record_out_probe_ns(output_start);
                if let Some(batch) = batch {
                    output_batches.push(batch);
                }
                continue;
            }

            let table = table_opt.as_ref().expect("build table");
            let build_chunk = build_chunk_opt.as_ref().expect("build chunk");
            let search_start = std::time::Instant::now();
            let (mut selection, stats) =
                table.search_pairs_evaluated(&self.probe_keys, &probe, evaluate)?;
            self.record_search_ns(search_start);
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);
            if !selection.is_empty()
                && let Some(pred) = self.residual_predicate
            {
                self.residual_rows_checked = self
                    .residual_rows_checked
                    .saturating_add(stats.lookup_hit_rows);
                self.residual_group_rows_total = self
                    .residual_group_rows_total
                    .saturating_add(selection.len() as u64);
                self.compact_selection_by_residual(
                    &probe,
                    build_chunk,
                    &mut selection,
                    pred,
                    evaluate,
                    create_input,
                )?;
            }

            let probe_finalize = finalize_probe_rows(probe.len(), &selection, false, "outer join")?;
            let unmatched = probe_finalize.selected;
            if track_build_matches && let Some(flags) = self.build_matched.as_mut() {
                mark_build_matches(flags, &selection)?;
            }
            if !selection.is_empty() {
                let all_match_one = is_all_match_one(&selection, probe.len());
                let gathered =
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_probe_build_batches(
                        &probe,
                        build_chunk.as_ref(),
                        &selection.probe,
                        &selection.build,
                        &output_schema,
                        self.probe_is_left,
                        all_match_one,
                    )?;
                self.record_out_timings(gathered.timings);
                output_batches.extend(gathered.batches);
            }

            if output_unmatched_probe && !unmatched.is_empty() {
                let output_start = std::time::Instant::now();
                let batch = if self.probe_is_left {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_left_with_null_right(
                            &probe,
                            &unmatched,
                            &self.right_chunk_schema.arrow_schema_ref(),
                            &output_schema,
                        )?
                } else {
                    crate::exec::operators::hashjoin::join_hash_map::gather::gather_null_left_with_right(
                            &probe,
                            &unmatched,
                            &self.left_chunk_schema.arrow_schema_ref(),
                            &output_schema,
                        )?
                };
                self.record_out_probe_ns(output_start);
                if let Some(batch) = batch {
                    output_batches.push(batch);
                }
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let batch = if output_batches.len() == 1 {
            output_batches.remove(0)
        } else {
            concat_compatible_batches(&output_schema, &output_batches, "inner join concat")?
        };
        let mut sources = probe_chunks.iter().collect::<Vec<_>>();
        if let Some(build) = &build_chunk_opt {
            sources.push(build.as_ref());
        }
        Ok(Some(create_input(
            batch,
            Arc::clone(&self.join_scope_chunk_schema),
            &sources,
            &self.outer_nullable_slots(),
        )?))
    }

    fn join_semi_anti(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        if self.probe_keys.is_empty() {
            return Err(("semi/anti join requires non-empty eq join keys".to_string()).into());
        }

        if self.probe_is_left && matches!(self.join_type, JoinType::RightSemi | JoinType::RightAnti)
        {
            // For both RIGHT SEMI and RIGHT ANTI: only mark which build
            // rows matched, do not produce output during probing.  The
            // actual output is deferred to `finish_one()` where per-driver
            // flags are merged to avoid duplicates across parallel drivers.
            self.mark_build_matches_for_semi_anti(probe_chunks, evaluate, create_input)?;
            return Ok(None);
        }

        let output_schema = probe_chunks[0].schema();

        let is_semi = matches!(self.join_type, JoinType::LeftSemi | JoinType::RightSemi);
        let is_anti = !is_semi;

        let Some(table) = self.build_table.clone() else {
            if is_anti {
                let batches: Vec<_> = probe_chunks
                    .iter()
                    .map(|c| c.record_batch().clone())
                    .collect();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                let probe_batch = if batches.len() == 1 {
                    batches.into_iter().next().expect("one batch")
                } else {
                    let output_start = std::time::Instant::now();
                    let batch =
                        concat_compatible_batches(&output_schema, &batches, "anti join concat")?;
                    self.record_out_probe_ns(output_start);
                    batch
                };
                return Ok(Some(self.extend_with_null_build_columns(
                    probe_batch,
                    &probe_chunks.iter().collect::<Vec<_>>(),
                    create_input,
                )?));
            }
            return Ok(None);
        };
        if table.is_empty() {
            if is_anti {
                let batches: Vec<_> = probe_chunks
                    .iter()
                    .map(|c| c.record_batch().clone())
                    .collect();
                if batches.is_empty() {
                    return Ok(None);
                }
                let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                self.output_rows = self.output_rows.saturating_add(output_rows as u64);
                let probe_batch = if batches.len() == 1 {
                    batches.into_iter().next().expect("one batch")
                } else {
                    let output_start = std::time::Instant::now();
                    let batch =
                        concat_compatible_batches(&output_schema, &batches, "anti join concat")?;
                    self.record_out_probe_ns(output_start);
                    batch
                };
                return Ok(Some(self.extend_with_null_build_columns(
                    probe_batch,
                    &probe_chunks.iter().collect::<Vec<_>>(),
                    create_input,
                )?));
            }
            return Ok(None);
        }

        if self.probe_is_left
            && matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            && self.residual_predicate.is_none()
        {
            let is_semi = self.join_type == JoinType::LeftSemi;
            return Ok((self.join_left_semi_anti_by_membership(
                probe_chunks,
                &output_schema,
                &table,
                is_semi,
                evaluate,
                create_input,
            ))?);
        }

        if matches!(
            self.join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        ) {
            let is_semi = matches!(self.join_type, JoinType::LeftSemi | JoinType::RightSemi);
            let mut output_batches = Vec::new();
            for probe in &probe_chunks {
                let search_start = std::time::Instant::now();
                let (mut selection, stats) =
                    table.search_pairs_evaluated(&self.probe_keys, &probe, evaluate)?;
                self.record_search_ns(search_start);
                self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
                self.lookup_miss_rows =
                    self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);

                let residual_matched_rows_before = if let Some(pred) = self.residual_predicate
                    && !selection.is_empty()
                {
                    let Some(build_chunk) =
                        self.require_build_chunk("semi/anti residual evaluation")?
                    else {
                        return Ok(None);
                    };
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(selection.len() as u64);
                    let residual_matched_rows_before = self.residual_matched_rows;
                    self.compact_selection_by_residual(
                        &probe,
                        &build_chunk,
                        &mut selection,
                        pred,
                        evaluate,
                        create_input,
                    )?;
                    Some(residual_matched_rows_before)
                } else {
                    None
                };

                let probe_finalize =
                    finalize_probe_rows(probe.len(), &selection, is_semi, "semi/anti join")?;
                if let Some(residual_matched_rows_before) = residual_matched_rows_before {
                    let matched_probe_rows = probe_finalize
                        .matched
                        .iter()
                        .filter(|matched| **matched)
                        .count();
                    self.residual_matched_rows =
                        residual_matched_rows_before.saturating_add(matched_probe_rows as u64);
                }

                let keep = probe_finalize
                    .matched
                    .into_iter()
                    .map(|matched| if is_semi { matched } else { !matched })
                    .collect::<Vec<bool>>();
                let output_start = std::time::Instant::now();
                let mask = BooleanArray::from(keep);
                let filtered_batch = filter_record_batch(&probe.record_batch(), &mask)
                    .map_err(|e| format!("semi/anti filter failed: {e}"))?;
                self.record_out_probe_ns(output_start);
                if filtered_batch.num_rows() > 0 {
                    output_batches.push(filtered_batch);
                }
            }

            if output_batches.is_empty() {
                return Ok(None);
            }
            let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
            self.output_rows = self.output_rows.saturating_add(output_rows as u64);
            let probe_batch = if output_batches.len() == 1 {
                output_batches.remove(0)
            } else {
                let output_start = std::time::Instant::now();
                let batch = concat_compatible_batches(
                    &output_schema,
                    &output_batches,
                    "semi anti join concat",
                )?;
                self.record_out_probe_ns(output_start);
                batch
            };
            return Ok(Some(self.extend_with_null_build_columns(
                probe_batch,
                &probe_chunks.iter().collect::<Vec<_>>(),
                create_input,
            )?));
        }
        Ok(None)
    }

    fn join_left_semi_anti_by_membership(
        &mut self,
        probe_chunks: Vec<Chunk>,
        output_schema: &SchemaRef,
        table: &JoinHashMap,
        is_semi: bool,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        if let Some(requirements) = self.build_requirements {
            debug_assert!(!requirements.requires_row_payload());
        }

        let mut output_batches = Vec::new();
        for probe in &probe_chunks {
            let search_start = std::time::Instant::now();
            let (membership, stats) =
                table.search_membership_evaluated(&self.probe_keys, &probe, evaluate)?;
            self.record_search_ns(search_start);
            if membership.len() != probe.len() {
                return Err((format!(
                    "semi/anti membership length mismatch: membership={} rows={}",
                    membership.len(),
                    probe.len()
                ))
                .into());
            }
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);

            let keep = membership
                .into_vec()
                .into_iter()
                .map(|matched| if is_semi { matched } else { !matched })
                .collect::<Vec<bool>>();
            let output_start = std::time::Instant::now();
            let mask = BooleanArray::from(keep);
            let filtered_batch = filter_record_batch(&probe.record_batch(), &mask)
                .map_err(|e| format!("semi/anti membership filter failed: {e}"))?;
            self.record_out_probe_ns(output_start);
            if filtered_batch.num_rows() > 0 {
                output_batches.push(filtered_batch);
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let probe_batch = if output_batches.len() == 1 {
            output_batches.remove(0)
        } else {
            let output_start = std::time::Instant::now();
            let batch = concat_compatible_batches(
                output_schema,
                &output_batches,
                "semi anti membership join concat",
            )?;
            self.record_out_probe_ns(output_start);
            batch
        };
        Ok(Some(self.extend_with_null_build_columns(
            probe_batch,
            &probe_chunks.iter().collect::<Vec<_>>(),
            create_input,
        )?))
    }

    fn join_null_aware_left_anti(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<Option<Chunk>, ExecutionFailure> {
        if self.probe_keys.is_empty() {
            return Err(("semi/anti join requires non-empty eq join keys".to_string()).into());
        }
        if self.global_build_row_count == 0 {
            let output_schema = probe_chunks[0].schema();
            let batches: Vec<_> = probe_chunks
                .iter()
                .map(|c| c.record_batch().clone())
                .collect();
            if batches.is_empty() {
                return Ok(None);
            }
            let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            self.output_rows = self.output_rows.saturating_add(output_rows as u64);
            let probe_batch = if batches.len() == 1 {
                batches.into_iter().next().expect("one batch")
            } else {
                let output_start = std::time::Instant::now();
                let batch = concat_compatible_batches(
                    &output_schema,
                    &batches,
                    "null aware anti join concat",
                )?;
                self.record_out_probe_ns(output_start);
                batch
            };
            return Ok(Some(self.extend_with_null_build_columns(
                probe_batch,
                &probe_chunks.iter().collect::<Vec<_>>(),
                create_input,
            )?));
        }

        let output_schema = probe_chunks[0].schema();
        let table_opt = self.build_table.clone();
        let has_residual = self.residual_predicate.is_some();
        if !has_residual && self.global_build_has_null_key {
            return Ok(None);
        }
        if has_residual && self.build_null_key_rows.is_none() {
            return Err(
                ("null-aware left anti join with residual requires build null-key rows"
                    .to_string())
                .into(),
            );
        }

        let build_null_key_rows = self.build_null_key_rows.clone();
        let flat_build_null_key_rows: &[u32] = build_null_key_rows
            .as_ref()
            .map(|rows| rows.as_slice())
            .unwrap_or(&[]);

        let mut output_batches = Vec::new();
        for probe in &probe_chunks {
            let mut probe_key_arrays = Vec::with_capacity(self.probe_keys.len());
            for key in &self.probe_keys {
                probe_key_arrays.push(evaluate(*key, &probe)?);
            }

            let (group_ids, mut equal_selection) = if let Some(table) = table_opt.as_ref() {
                let search_start = std::time::Instant::now();
                let result = if has_residual {
                    let (selection, stats) =
                        table.search_pairs_evaluated(&self.probe_keys, &probe, evaluate)?;
                    let group_ids =
                        table.lookup_group_ids_evaluated(&self.probe_keys, &probe, evaluate)?;
                    debug_assert_eq!(
                        stats.lookup_hit_rows + stats.lookup_miss_rows,
                        group_ids.len() as u64
                    );
                    (group_ids, selection)
                } else {
                    (
                        table.lookup_group_ids_evaluated(&self.probe_keys, &probe, evaluate)?,
                        JoinSelection::new(),
                    )
                };
                self.record_search_ns(search_start);
                result
            } else {
                (vec![None; probe.len()], JoinSelection::new())
            };

            let probe_null_rows = (0..probe.len())
                .map(|row| {
                    probe_key_arrays
                        .iter()
                        .any(|key_array| !key_array.is_valid(row))
                })
                .collect::<Vec<_>>();
            let mut matched_equal = vec![false; probe.len()];
            let mut matched_null_key = vec![false; probe.len()];
            let mut matched_any = vec![false; probe.len()];
            let mut residual_matched_probe_rows = vec![false; probe.len()];

            if let Some(pred) = self.residual_predicate {
                if table_opt.is_some() {
                    let stats = SearchStats::from_group_ids(&group_ids);
                    self.residual_rows_checked = self
                        .residual_rows_checked
                        .saturating_add(stats.lookup_hit_rows);
                    self.residual_group_rows_total = self
                        .residual_group_rows_total
                        .saturating_add(equal_selection.len() as u64);
                    if !equal_selection.is_empty() {
                        let Some(build_chunk) =
                            self.require_build_chunk("null-aware anti residual evaluation")?
                        else {
                            return Ok(None);
                        };
                        self.compact_null_aware_selection(
                            &probe,
                            &build_chunk,
                            &mut equal_selection,
                            pred,
                            &mut matched_equal,
                            &mut residual_matched_probe_rows,
                            "null-aware anti equality residual",
                            evaluate,
                            create_input,
                        )?;
                    }
                }

                if !flat_build_null_key_rows.is_empty() {
                    let Some(build_chunk) =
                        self.require_build_chunk("null-aware anti residual evaluation")?
                    else {
                        return Ok(None);
                    };
                    let probe_rows = (0..probe.len()).map(|row| row as u32).collect::<Vec<_>>();
                    self.compact_cross_selection_in_chunks(
                        &probe,
                        &build_chunk,
                        &probe_rows,
                        flat_build_null_key_rows,
                        pred,
                        &mut matched_null_key,
                        &mut residual_matched_probe_rows,
                        "null-aware anti null-key residual",
                        evaluate,
                        create_input,
                    )?;
                }

                let has_local_build_rows = self.build_partition_row_count > 0;
                if has_local_build_rows && probe_null_rows.iter().any(|is_null| *is_null) {
                    let Some(build_chunk) =
                        self.require_build_chunk("null-aware anti residual evaluation")?
                    else {
                        return Ok(None);
                    };
                    let all_build_rows = (0..build_chunk.len())
                        .map(|row| {
                            u32::try_from(row)
                                .map_err(|_| "join residual build row id overflow".to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let probe_rows = probe_null_rows
                        .iter()
                        .enumerate()
                        .filter_map(|(row, is_null)| is_null.then_some(row as u32))
                        .collect::<Vec<_>>();
                    self.compact_cross_selection_in_chunks(
                        &probe,
                        &build_chunk,
                        &probe_rows,
                        &all_build_rows,
                        pred,
                        &mut matched_any,
                        &mut residual_matched_probe_rows,
                        "null-aware anti all-build residual",
                        evaluate,
                        create_input,
                    )?;
                }
            }

            let mut keep = Vec::with_capacity(probe.len());
            for (row, group_id_opt) in group_ids.iter().enumerate() {
                let key_is_null = probe_null_rows[row];
                if self.residual_predicate.is_some() {
                    if key_is_null {
                        keep.push(!matched_any[row]);
                    } else {
                        keep.push(!(matched_equal[row] || matched_null_key[row]));
                    }
                } else {
                    let matched = group_id_opt.is_some();
                    keep.push(!key_is_null && !matched);
                }
            }

            let output_start = std::time::Instant::now();
            let mask = BooleanArray::from(keep);
            let filtered_batch = filter_record_batch(&probe.record_batch(), &mask)
                .map_err(|e| format!("null-aware anti filter failed: {e}"))?;
            self.record_out_probe_ns(output_start);
            if filtered_batch.num_rows() > 0 {
                output_batches.push(filtered_batch);
            }
        }

        if output_batches.is_empty() {
            return Ok(None);
        }
        let output_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
        self.output_rows = self.output_rows.saturating_add(output_rows as u64);
        let probe_batch = if output_batches.len() == 1 {
            output_batches.remove(0)
        } else {
            let output_start = std::time::Instant::now();
            let batch = concat_compatible_batches(
                &output_schema,
                &output_batches,
                "null aware anti join output concat",
            )?;
            self.record_out_probe_ns(output_start);
            batch
        };
        Ok(Some(self.extend_with_null_build_columns(
            probe_batch,
            &probe_chunks.iter().collect::<Vec<_>>(),
            create_input,
        )?))
    }

    fn mark_build_matches_for_semi_anti(
        &mut self,
        probe_chunks: Vec<Chunk>,
        evaluate: &mut ExprEvaluationCallback<'_>,
        create_input: &mut JoinInputCreator<'_>,
    ) -> Result<(), ExecutionFailure> {
        if probe_chunks.is_empty() {
            return Ok(());
        }
        let Some(table) = self.build_table.clone() else {
            return Ok(());
        };
        let Some(build_chunk) = self.build_chunk.clone() else {
            return Ok(());
        };
        if table.is_empty() || build_chunk.is_empty() {
            return Ok(());
        }
        if self.build_matched.is_none() {
            return Ok(());
        }

        for probe in &probe_chunks {
            if probe.is_empty() {
                continue;
            }
            let search_start = std::time::Instant::now();
            let (mut selection, stats) =
                table.search_pairs_evaluated(&self.probe_keys, &probe, evaluate)?;
            self.record_search_ns(search_start);
            self.lookup_hit_rows = self.lookup_hit_rows.saturating_add(stats.lookup_hit_rows);
            self.lookup_miss_rows = self.lookup_miss_rows.saturating_add(stats.lookup_miss_rows);

            if selection.is_empty() {
                continue;
            }
            let residual_matched_rows_before = if let Some(pred) = self.residual_predicate {
                self.residual_rows_checked = self
                    .residual_rows_checked
                    .saturating_add(stats.lookup_hit_rows);
                self.residual_group_rows_total = self
                    .residual_group_rows_total
                    .saturating_add(selection.len() as u64);
                let residual_matched_rows_before = self.residual_matched_rows;
                self.compact_selection_by_residual(
                    &probe,
                    &build_chunk,
                    &mut selection,
                    pred,
                    evaluate,
                    create_input,
                )?;
                Some(residual_matched_rows_before)
            } else {
                None
            };

            let Some(flags) = self.build_matched.as_mut() else {
                return Ok(());
            };
            let unique_build_rows_marked = mark_build_matches(flags, &selection)?;
            if let Some(residual_matched_rows_before) = residual_matched_rows_before {
                self.residual_matched_rows =
                    residual_matched_rows_before.saturating_add(unique_build_rows_marked);
            }
        }

        Ok(())
    }

    fn apply_runtime_filters(&self, chunks: Vec<Chunk>) -> Result<Vec<Chunk>, String> {
        Ok(chunks)
    }

    fn should_apply_runtime_filters(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }
}

/// Return a stable string label for one join type, used by diagnostics and errors.
pub(crate) fn join_type_str(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "INNER",
        JoinType::LeftOuter => "LEFT_OUTER",
        JoinType::RightOuter => "RIGHT_OUTER",
        JoinType::FullOuter => "FULL_OUTER",
        JoinType::LeftSemi => "LEFT_SEMI",
        JoinType::RightSemi => "RIGHT_SEMI",
        JoinType::LeftAnti => "LEFT_ANTI",
        JoinType::RightAnti => "RIGHT_ANTI",
        JoinType::NullAwareLeftAnti => "NULL_AWARE_LEFT_ANTI",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use super::build_requirements::required_build_components;
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::ExprNode;
    use crate::exec::operators::hashjoin::join_hash_map::build_store::BuildStore;
    use novarocks_types::SlotId;

    const LEFT_K_SLOT_ID: SlotId = SlotId::new(1);
    const LEFT_V_SLOT_ID: SlotId = SlotId::new(2);
    const RIGHT_K_SLOT_ID: SlotId = SlotId::new(3);
    const RIGHT_W_SLOT_ID: SlotId = SlotId::new(4);

    fn schema_kv(k_name: &str, v_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(k_name, DataType::Int32, false),
            Field::new(v_name, DataType::Int32, false),
        ]))
    }

    fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
        let mut fields = left
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.extend(right.fields().iter().map(|field| field.as_ref().clone()));
        Arc::new(Schema::new(fields))
    }

    fn outer_join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
        let mut fields = left
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>();
        fields.extend(
            right
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true)),
        );
        Arc::new(Schema::new(fields))
    }

    fn chunk_schema_of(schema: &SchemaRef, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    fn chunk_of_two(schema: SchemaRef, slot_ids: &[SlotId], k: &[i32], v: &[i32]) -> Chunk {
        assert_eq!(k.len(), v.len());
        let k_arr = Arc::new(Int32Array::from(k.to_vec())) as ArrayRef;
        let v_arr = Arc::new(Int32Array::from(v.to_vec())) as ArrayRef;
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![k_arr, v_arr]).expect("record batch");
        Chunk::new_with_chunk_schema(batch, chunk_schema_of(&schema, slot_ids))
    }

    fn int32_values(chunk: &Chunk, column: usize) -> Vec<Option<i32>> {
        let array = chunk
            .columns()
            .get(column)
            .expect("column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..chunk.len())
            .map(|row| {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            })
            .collect()
    }

    fn direct_build_artifact_with_contract(
        build: Chunk,
        join_type: JoinType,
        has_residual_predicate: bool,
        build_has_null_key: bool,
        build_null_key_rows: Option<Arc<Vec<u32>>>,
    ) -> JoinBuildArtifact {
        let key_arrays = vec![build.column_by_slot_id(RIGHT_K_SLOT_ID).expect("build key")];
        let batch = crate::exec::operators::hashjoin::join_hash_map::method::BuildKeyBatch::new(
            key_arrays,
            build.len(),
        )
        .expect("key batch");
        let build_row_count = build.len();
        let build_table =
            crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMap::build_from_key_batches(
                vec![DataType::Int32],
                vec![false],
                &[batch],
                crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapBuildOptions::default(),
            )
            .expect("direct build table");
        assert!(matches!(
            build_table.method_kind(),
            crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapMethodKind::DirectInt { .. }
        ));
        JoinBuildArtifact::new_native(
            required_build_components(join_type, has_residual_predicate, true, true),
            Some(BuildStore::new(build)),
            Some(build_table),
            build_row_count,
            build_has_null_key,
            build_null_key_rows,
        )
    }

    fn direct_build_artifact_from_build_chunk(build: Chunk) -> JoinBuildArtifact {
        direct_build_artifact_with_contract(
            build,
            JoinType::Inner,
            false,
            false,
            Some(Arc::new(Vec::new())),
        )
    }

    fn direct_set_build_artifact_from_build_chunk(build: Chunk) -> JoinBuildArtifact {
        let key_arrays = vec![build.column_by_slot_id(RIGHT_K_SLOT_ID).expect("build key")];
        let batch = crate::exec::operators::hashjoin::join_hash_map::method::BuildKeyBatch::new(
            key_arrays,
            build.len(),
        )
        .expect("key batch");
        let build_row_count = build.len();
        let build_table =
            crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMap::build_from_key_batches(
                vec![DataType::Int32],
                vec![false],
                &[batch],
                crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapBuildOptions {
                    purpose: crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapBuildPurpose::PresenceOnly,
                    ..crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapBuildOptions::default()
                },
            )
            .expect("direct set build table");
        assert!(matches!(
            build_table.method_kind(),
            crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapMethodKind::DirectIntSet { .. }
        ));
        JoinBuildArtifact::new_native(
            required_build_components(JoinType::LeftSemi, false, true, true),
            None,
            Some(build_table),
            build_row_count,
            false,
            None,
        )
    }

    #[allow(
        dead_code,
        reason = "Retained as a focused left-semi join regression fixture."
    )]
    fn left_semi_probe_core() -> HashJoinProbeCore {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        HashJoinProbeCore::new_native(
            Arc::new(arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        )
    }

    #[allow(
        dead_code,
        reason = "Retained as a focused hash-join state regression assertion."
    )]
    fn assert_probe_core_unloaded(core: &HashJoinProbeCore) {
        assert!(!core.is_build_loaded());
        assert!(core.build_requirements.is_none());
        assert!(core.build_view.is_none());
        assert!(core.build_chunk.is_none());
        assert!(core.build_null_key_rows.is_none());
        assert!(core.build_table.is_none());
        assert!(core.build_matched.is_none());
        assert_eq!(core.global_build_row_count, 0);
        assert!(!core.global_build_has_null_key);
        assert_eq!(core.build_partition_row_count, 0);
        assert!(!core.build_partition_has_null_key);
    }

    #[test]
    fn set_build_artifact_rejects_component_contract_mismatch() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100],
            &[10],
        );
        let artifact = Arc::new(direct_set_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );

        let err = core
            .set_build_artifact(artifact, 1, false)
            .expect_err("probe core must validate build artifact contract");
        assert!(err.contains("component contract mismatch"), "err={err}");
    }

    #[test]
    fn residual_left_semi_rejects_missing_row_payload_at_load() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[10, 20],
            &[100, 200],
        );
        let mut table =
            JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("build table");
        table
            .add_build_rows(
                &[build_chunk.column_by_slot_id(RIGHT_K_SLOT_ID).expect("key")],
                build_chunk.len(),
            )
            .expect("add rows");
        table.finalize().expect("finalize table");

        let artifact = Arc::new(JoinBuildArtifact::new_native(
            required_build_components(JoinType::LeftSemi, true, true, true),
            None,
            Some(table),
            build_chunk.len(),
            false,
            None,
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        let err = core
            .set_build_artifact(artifact, build_chunk.len(), false)
            .expect_err("residual semi must require row payload");
        assert!(err.contains("row payload required"));
    }

    #[test]
    fn inner_cross_join_allows_missing_lookup_table() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let arena = Arc::new(ExprArena::default());
        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[10, 20],
            &[100, 200],
        );
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            required_build_components(JoinType::Inner, false, true, false),
            Some(BuildStore::new(build_chunk.clone())),
            None,
            build_chunk.len(),
            false,
            None,
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            Vec::new(),
            None,
            true,
            false,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, build_chunk.len(), false)
            .expect("cross join build artifact should not require lookup table");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[1, 2],
            &[11, 22],
        );
        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("cross join probe")
            .expect("cross join output");
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn all_match_one_is_decided_after_residual_compaction() {
        let selection = crate::exec::operators::hashjoin::join_hash_map::search::JoinSelection {
            probe: vec![0, 0, 1],
            build: vec![2, 0, 1],
        };
        assert!(
            !crate::exec::operators::hashjoin::join_hash_map::finalize::is_all_match_one(
                &selection, 2
            )
        );
        let compacted = crate::exec::operators::hashjoin::join_hash_map::search::JoinSelection {
            probe: vec![0, 1],
            build: vec![2, 1],
        };
        assert!(
            crate::exec::operators::hashjoin::join_hash_map::finalize::is_all_match_one(
                &compacted, 2
            )
        );
    }

    #[test]
    fn inner_join_uses_direct_map_and_preserves_duplicate_matches() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 101, 100],
            &[10, 11, 12],
        );
        let artifact = Arc::new(direct_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 102],
            &[1, 2],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("inner join output");
        assert_eq!(out.len(), 2);
        assert_eq!(core.lookup_hit_rows(), 1);
        assert_eq!(core.lookup_miss_rows(), 1);
    }

    #[test]
    fn left_outer_join_marks_null_padded_right_slots_nullable() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100],
            &[10],
        );
        let artifact = Arc::new(direct_build_artifact_with_contract(
            build_chunk,
            JoinType::LeftOuter,
            false,
            false,
            None,
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftOuter,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 1, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[200],
            &[2],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("left outer output");

        assert!(!out.chunk_schema().slots()[0].nullable());
        assert!(!out.chunk_schema().slots()[1].nullable());
        assert!(out.chunk_schema().slots()[2].nullable());
        assert!(out.chunk_schema().slots()[3].nullable());
        assert_eq!(int32_values(&out, 0), vec![Some(200)]);
        assert_eq!(int32_values(&out, 2), vec![None]);
        assert_eq!(int32_values(&out, 3), vec![None]);
    }

    #[test]
    fn full_outer_search_pairs_marks_build_and_probe_unmatched() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = outer_join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 101, 103],
            &[10, 11, 13],
        );
        let artifact = Arc::new(direct_build_artifact_with_contract(
            build_chunk,
            JoinType::FullOuter,
            false,
            false,
            None,
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::FullOuter,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 102],
            &[1, 2],
        );

        let probe_out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("full outer probe output");
        assert_eq!(probe_out.len(), 2);

        let finish_out = core
            .finish_from_probe_output(Some(probe_out), None, true)
            .expect("finish")
            .expect("full outer final output");
        assert_eq!(finish_out.len(), 4);
        assert_eq!(
            int32_values(&finish_out, 0),
            vec![Some(100), Some(102), None, None]
        );
        assert_eq!(
            int32_values(&finish_out, 1),
            vec![Some(1), Some(2), None, None]
        );
        assert_eq!(
            int32_values(&finish_out, 2),
            vec![Some(100), None, Some(101), Some(103)]
        );
        assert_eq!(
            int32_values(&finish_out, 3),
            vec![Some(10), None, Some(11), Some(13)]
        );
        assert_eq!(core.lookup_hit_rows(), 1);
        assert_eq!(core.lookup_miss_rows(), 1);
        assert_eq!(core.output_rows(), 4);
    }

    #[test]
    fn left_semi_without_residual_uses_direct_set_membership() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 100, 102],
            &[10, 12, 30],
        );
        let artifact = Arc::new(direct_set_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[1, 2, 3],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("left semi output");

        assert_eq!(out.len(), 2);
        assert_eq!(core.lookup_hit_rows(), 2);
        assert_eq!(core.lookup_miss_rows(), 1);
        assert_eq!(core.residual_group_rows_total(), 0);
    }

    #[test]
    fn left_anti_without_residual_uses_direct_set_membership() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 102, 102],
            &[10, 30, 31],
        );
        let artifact = Arc::new(direct_set_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            None,
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[1, 2, 3],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("left anti output");

        assert_eq!(out.len(), 1);
        assert_eq!(core.lookup_hit_rows(), 2);
        assert_eq!(core.lookup_miss_rows(), 1);
        assert_eq!(core.residual_group_rows_total(), 0);
    }

    #[test]
    fn left_semi_with_residual_uses_pairs_before_probe_collapse() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 100, 102],
            &[0, 10, 30],
        );
        let artifact = Arc::new(direct_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[5, 2, 40],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("left semi output");
        assert_eq!(out.len(), 1);
        assert_eq!(core.lookup_hit_rows(), 2);
        assert_eq!(core.lookup_miss_rows(), 1);
        assert_eq!(core.residual_group_rows_total(), 3);
    }

    #[test]
    fn left_anti_with_residual_counts_matched_probe_rows_not_output_rows() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 100, 102],
            &[0, 10, 30],
        );
        let artifact = Arc::new(direct_build_artifact_from_build_chunk(build_chunk));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[5, 2, 40],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("left anti output");
        assert_eq!(out.len(), 2);
        assert_eq!(int32_values(&out, 0), vec![Some(101), Some(102)]);
        assert_eq!(core.lookup_hit_rows(), 2);
        assert_eq!(core.lookup_miss_rows(), 1);
        assert_eq!(core.residual_group_rows_total(), 3);
        assert_eq!(core.residual_matched_rows(), 1);
    }

    #[test]
    fn concat_compatible_batches_rejects_decimal_precision_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let array = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(38, 2)
                .expect("decimal type"),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Decimal128(38, 2),
                false,
            )])),
            vec![array],
        )
        .expect("record batch");

        let err = super::concat_compatible_batches(&schema, &[batch], "join merge")
            .expect_err("join concat must reject actual-widen decimal");

        assert!(
            err.contains("join merge type mismatch at column 0"),
            "err={err}"
        );
        assert!(err.contains("Decimal128(10, 2)"), "err={err}");
        assert!(err.contains("Decimal128(38, 2)"), "err={err}");
    }

    #[test]
    fn concat_compatible_batches_widens_nullable_batch_fields() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count(1)",
            DataType::Int64,
            false,
        )]));
        let nullable_schema = Arc::new(Schema::new(vec![Field::new(
            "count(1)",
            DataType::Int64,
            true,
        )]));
        let non_null_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("non-null batch");
        let null_batch = RecordBatch::try_new(
            nullable_schema,
            vec![Arc::new(Int64Array::from(vec![None])) as ArrayRef],
        )
        .expect("nullable batch");

        let batch =
            super::concat_compatible_batches(&schema, &[non_null_batch, null_batch], "join merge")
                .expect("join concat");

        assert!(batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 1);
    }

    #[test]
    fn right_semi_residual_counts_unique_build_rows_marked() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[1],
            &[10],
        );
        let mut build_table =
            JoinHashMap::new_chained(vec![DataType::Int32], vec![false]).expect("build table");
        let build_key_arrays = vec![
            build_chunk
                .column_by_slot_id(RIGHT_K_SLOT_ID)
                .expect("build key"),
        ];
        build_table
            .add_build_rows(&build_key_arrays, build_chunk.len())
            .expect("add build rows");
        build_table.finalize().expect("finalize build table");
        let artifact = Arc::new(JoinBuildArtifact::new_native(
            required_build_components(JoinType::RightSemi, true, true, true),
            Some(BuildStore::new(build_chunk.clone())),
            Some(build_table),
            1,
            false,
            None,
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 1, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[1, 1],
            &[1, 2],
        );

        let probe_out = core.join_probe_chunks(vec![probe_chunk]).expect("probe");
        assert!(probe_out.is_none());

        let build_out = core
            .build_right_semi_anti_output(true)
            .expect("right semi output")
            .expect("build output");
        assert_eq!(build_out.num_rows(), 1);
        assert_eq!(core.residual_matched_rows(), 1);
    }

    #[test]
    fn null_aware_left_anti_with_residual_keeps_existing_decision_table() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 102, 103],
            &[10, 5, 99],
        );
        let artifact = Arc::new(direct_build_artifact_with_contract(
            build_chunk,
            JoinType::NullAwareLeftAnti,
            true,
            false,
            Some(Arc::new(Vec::new())),
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::NullAwareLeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[1, 2, 40],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("probe")
            .expect("null aware anti output");
        assert_eq!(out.len(), 2);
        assert_eq!(core.lookup_hit_rows(), 0);
        assert_eq!(core.residual_group_rows_total(), 2);
    }

    #[test]
    fn null_aware_left_anti_residual_allows_empty_local_build_partition() {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let artifact = Arc::new(JoinBuildArtifact::new_native(
            required_build_components(JoinType::NullAwareLeftAnti, true, true, true),
            None,
            None,
            0,
            false,
            Some(Arc::new(Vec::new())),
        ));
        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::NullAwareLeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 1, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[1, 2],
            &[100, 200],
        );

        let out = core
            .join_probe_chunks(vec![probe_chunk])
            .expect("empty local build partition should not require build chunk")
            .expect("probe rows survive empty local build partition");

        assert_eq!(out.len(), 2);
        let left_k = out
            .columns()
            .first()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let left_v = out
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..out.len())
                .map(|row| (left_k.value(row), left_v.value(row)))
                .collect::<Vec<_>>(),
            vec![(1, 100), (2, 200)]
        );
    }
    fn evaluated_null_aware_fixture() -> (
        HashJoinProbeCore,
        Arc<ExprArena>,
        crate::exec::expr::ExprId,
        crate::exec::expr::ExprId,
        Chunk,
    ) {
        let left_schema = schema_kv("lk", "lv");
        let right_schema = schema_kv("rk", "rw");
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let build_chunk = chunk_of_two(
            Arc::clone(&right_schema),
            &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID],
            &[100, 102, 103],
            &[10, 5, 99],
        );
        let artifact = Arc::new(direct_build_artifact_with_contract(
            build_chunk,
            JoinType::NullAwareLeftAnti,
            true,
            false,
            Some(Arc::new(Vec::new())),
        ));

        let mut core = HashJoinProbeCore::new_native(
            Arc::clone(&arena),
            JoinType::NullAwareLeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            true,
            chunk_schema_of(&left_schema, &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID]),
            chunk_schema_of(&right_schema, &[RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID]),
            chunk_schema_of(
                &join_scope_schema,
                &[
                    LEFT_K_SLOT_ID,
                    LEFT_V_SLOT_ID,
                    RIGHT_K_SLOT_ID,
                    RIGHT_W_SLOT_ID,
                ],
            ),
        );
        core.set_build_artifact(artifact, 3, false)
            .expect("set build");

        let probe_chunk = chunk_of_two(
            Arc::clone(&left_schema),
            &[LEFT_K_SLOT_ID, LEFT_V_SLOT_ID],
            &[100, 101, 102],
            &[1, 2, 40],
        );

        (core, arena, probe_key, residual, probe_chunk)
    }

    #[test]
    fn null_aware_evaluated_probe_preserves_repeated_key_order_and_derived_scope() {
        let (mut core, arena, key, residual, probe) = evaluated_null_aware_fixture();
        let mut calls = Vec::new();
        let out = core
            .join_probe_chunks_with(
                vec![probe],
                &mut |id, input| {
                    calls.push((id, input.len(), input.record_batch().num_columns()));
                    arena.eval(id, input).map_err(Into::into)
                },
                &mut reference_join_evaluation_input,
            )
            .unwrap()
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(
            &calls[..3],
            &[(key, 3, 2); 3],
            "manual NULL keys, pair search and group lookup each retain their original invocation"
        );
        assert_eq!(
            calls[3],
            (residual, 2, 4),
            "residual uses the actual derived join input, not the original probe input"
        );
        assert_eq!(calls.len(), 4);
    }

    #[test]
    fn evaluated_probe_never_renders_or_recovers_an_originating_runtime_failure() {
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let mut failures: Vec<ExecutionFailure> = vec![
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
            KernelFailure::Operational(KernelDiagnostic::new("exact operational")),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        failures.push(ExecutionFailure::from("cancelled").at_operator(41, PipelineOperation::Pull));
        for original in failures {
            for stop in 0..4 {
                let (mut core, arena, _, _, probe) = evaluated_null_aware_fixture();
                let mut calls = 0;
                let failure = core
                    .join_probe_chunks_with(
                        vec![probe],
                        &mut |id, input| {
                            let at = calls;
                            calls += 1;
                            assert!(at <= stop, "evaluation after originating refusal");
                            if at == stop {
                                return Err(original.clone());
                            }
                            arena.eval(id, input).map_err(Into::into)
                        },
                        &mut reference_join_evaluation_input,
                    )
                    .unwrap_err();
                assert_eq!(failure, original, "evaluation ordinal {stop}");
                assert_eq!(calls, stop + 1);
            }
        }
    }
    #[test]
    fn real_join_derived_input_has_its_creator_and_original_finite_sources() {
        use crate::exec::chunk::ChunkSlotSchema;
        use novarocks_type_contract::{
            FunctionValueType, ValueTypeStorageHold, ValueTypeStorageLifetime,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Source(Arc<AtomicUsize>);
        impl ValueTypeStorageHold for Source {}
        impl Drop for Source {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn typed_schema(name: &str, storage: ValueTypeStorageLifetime) -> ChunkSchemaRef {
            let value = FunctionValueType::new(DataType::Int32, false);
            Arc::new(
                ChunkSchema::try_new_typed(
                    vec![
                        ChunkSlotSchema::try_new_typed(
                            LEFT_K_SLOT_ID,
                            Arc::new(Field::new(name, DataType::Int32, false)),
                            value.typed_binding().clone(),
                            None,
                            storage.clone(),
                        )
                        .unwrap(),
                    ],
                    storage,
                )
                .unwrap(),
            )
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let source_schema = typed_schema(
            "original",
            ValueTypeStorageLifetime::held(Arc::new(Source(drops.clone()))),
        );
        let source = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                source_schema.arrow_schema_ref(),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .unwrap(),
            source_schema,
        )
        .unwrap();
        let derived_schema = typed_schema("derived", ValueTypeStorageLifetime::pure());
        let batch =
            RecordBatch::try_new(derived_schema.arrow_schema_ref(), source.columns().to_vec())
                .unwrap();
        let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
        let factory =
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root(
                    "join actual derived creator fixture",
                ),
                error.clone(),
            )
            .unwrap();
        let derived =
            create_join_evaluation_input(batch, derived_schema, &[&source], factory.as_ref())
                .unwrap();
        assert!(derived.chunk_schema().is_typed());
        assert!(!derived.construction_storage_lifetime().is_pure());
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let owner = factory.begin_input_construction(&derived).unwrap();
        let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(error);
        let actual =
            crate::exec::expr::ExprEvaluationContext::new(&arena, owner.as_ref(), &control)
                .eval(id, &derived)
                .unwrap();
        assert_eq!(
            actual
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
        drop(actual);
        drop(owner);
        drop(derived);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn real_join_null_padding_changes_only_declared_roots_and_keeps_canonical_children() {
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let storage = ValueTypeStorageLifetime::pure();
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(12, 2),
            false,
        )));
        let declarations = [
            (LEFT_K_SLOT_ID, "probe", DataType::Int32),
            (RIGHT_K_SLOT_ID, "pruned", nested.clone()),
        ];
        let slots = declarations
            .iter()
            .enumerate()
            .map(|(index, (id, name, datatype))| {
                let value = FunctionValueType::new(datatype.clone(), false);
                ChunkSlotSchema::try_new_typed(
                    *id,
                    Arc::new(Field::new(*name, datatype.clone(), false).with_metadata(
                        std::collections::HashMap::from([(
                            "original_source_attribute".to_owned(),
                            name.to_string(),
                        )]),
                    )),
                    value.typed_binding().clone(),
                    Some(index as i32 + 41),
                    storage.clone(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let original_child_kind = slots[1].typed_binding().value_type().kind().clone();
        let scope = Arc::new(ChunkSchema::try_new_typed(slots, storage.clone()).unwrap());
        let source_schema =
            Arc::new(ChunkSchema::try_new_typed(vec![scope.slots()[0].clone()], storage).unwrap());
        let source = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                source_schema.arrow_schema_ref(),
                vec![Arc::new(Int32Array::from(vec![3, 7]))],
            )
            .unwrap(),
            source_schema,
        )
        .unwrap();
        let batch_schema = Arc::new(Schema::new(vec![
            scope.slots()[0].field().clone(),
            scope.slots()[1].field().clone().with_nullable(true),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                source.columns()[0].clone(),
                arrow::array::new_null_array(&nested, 2),
            ],
        )
        .unwrap();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("join NULL padding creator fixture"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let output =
            create_join_input(batch, scope.clone(), &[&source], &[1], factory.as_ref()).unwrap();
        assert!(!scope.slots()[1].nullable());
        assert!(!output.chunk_schema().slots()[0].nullable());
        assert!(output.chunk_schema().slots()[1].nullable());
        assert_eq!(
            output.chunk_schema().slots()[1]
                .typed_binding()
                .value_type()
                .kind(),
            &original_child_kind
        );
        assert_eq!(output.chunk_schema().slots()[1].slot_id(), RIGHT_K_SLOT_ID);
        assert_eq!(output.chunk_schema().slots()[1].unique_id(), Some(42));
        for index in 0..2 {
            assert_eq!(
                output.chunk_schema().slots()[index].field().metadata(),
                scope.slots()[index].field().metadata()
            );
        }
        assert_eq!(output.columns()[1].null_count(), 2);
        assert!(!output.construction_storage_lifetime().is_pure());
    }
}
