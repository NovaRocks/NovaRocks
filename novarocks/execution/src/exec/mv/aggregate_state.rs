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

//! Runtime kernel for aggregate materialized-view state.
//!
//! The kernel accepts only Arrow chunks and the runtime layout contract. SQL
//! planning, physical DDL, and application-level result carriers deliberately
//! do not enter this module.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use novarocks_types::mv_aggregate_layout::{
    MvAggregateRuntimeKind, MvAggregateRuntimeLayout, MvAggregateStateRole,
    MvAggregateVisibleOutput,
};

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::agg::{AggScalarValue, agg_scalar_from_array, build_agg_scalar_array};
use crate::exec::expr::decimal::{div_round_i128, pow10_i128};
use crate::exec::expr::function::mv_state::{
    approx_count_distinct_state_union, approx_count_distinct_state_visible, avg_state_union,
    bool_and_state_union, bool_and_state_visible, bool_or_state_union, bool_or_state_visible,
    count_distinct_state_union, count_distinct_state_visible, count_state_union,
    count_state_visible, max_state_union, max_state_visible_key_value, min_state_union,
    min_state_visible_key_value, sum_state_union,
};
use crate::exec::mv::state_codec::{
    KeyValue, decode_avg_decimal128, decode_avg_int64, decode_count_state, decode_sum_decimal128,
    decode_sum_int64,
};

/// One decoded physical aggregate-MV row.  This remains an execution value;
/// callers are responsible for any durable encoding or provider mutation.
#[derive(Clone, Debug)]
pub struct MvAggregatePhysicalRow {
    pub row_id: String,
    pub visible_values: Vec<Option<AggScalarValue>>,
    pub state_values: Vec<Option<AggScalarValue>>,
}

/// Aggregate state merge output split by provider application intent.
#[derive(Debug)]
pub struct MvAggregateMergeResult {
    pub upsert_chunks: Vec<Chunk>,
    pub delete_chunks: Vec<Chunk>,
    pub row_delta: i64,
}

/// Materializes executor state-shaped chunks into physical aggregate-MV rows.
pub fn materialize_aggregate_result_chunks(
    chunks: Vec<Chunk>,
    layout: &MvAggregateRuntimeLayout,
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| materialize_aggregate_result_batch(&chunk.batch, layout))
        .collect()
}

/// Loads active physical rows and rejects duplicate group row identifiers.
pub fn load_aggregate_physical_rows(
    chunks: &[Chunk],
    layout: &MvAggregateRuntimeLayout,
) -> Result<HashMap<String, MvAggregatePhysicalRow>, String> {
    let mut rows = HashMap::new();
    for chunk in chunks {
        for row in load_rows_from_batch(&chunk.batch, layout, false)? {
            let row_id = row.row_id.clone();
            if rows.insert(row_id.clone(), row).is_some() {
                return Err(format!(
                    "aggregate MV state corruption: duplicate row id `{row_id}`"
                ));
            }
        }
    }
    Ok(rows)
}

/// Loads signed delta rows.  Retraction counts may be negative here.
pub fn load_aggregate_physical_rows_for_delta(
    chunks: &[Chunk],
    layout: &MvAggregateRuntimeLayout,
) -> Result<Vec<MvAggregatePhysicalRow>, String> {
    let mut rows = Vec::new();
    for chunk in chunks {
        rows.extend(load_rows_from_batch(&chunk.batch, layout, true)?);
    }
    Ok(rows)
}

/// Loads current storage state with an application-facing corruption label.
pub fn build_old_state_map(
    chunks: &[Chunk],
    layout: &MvAggregateRuntimeLayout,
) -> Result<HashMap<String, MvAggregatePhysicalRow>, String> {
    load_aggregate_physical_rows(chunks, layout).map_err(|err| {
        if err.contains("duplicate row id") {
            format!("active aggregate MV state corruption: duplicate active MV row id: {err}")
        } else {
            err
        }
    })
}

/// Merges existing physical rows with state-shaped delta chunks.
pub fn merge_aggregate_state_batches(
    old_rows: &HashMap<String, MvAggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &MvAggregateRuntimeLayout,
) -> Result<Vec<Chunk>, String> {
    Ok(
        merge_aggregate_state_batches_with_retractions(old_rows, delta_chunks, layout)?
            .upsert_chunks,
    )
}

/// Merges existing physical rows with state-shaped deltas, separating retracted
/// rows from upserts for the provider/application owner.
pub fn merge_aggregate_state_batches_with_retractions(
    old_rows: &HashMap<String, MvAggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &MvAggregateRuntimeLayout,
) -> Result<MvAggregateMergeResult, String> {
    let old_row_count = old_rows.len();
    let mut merged = old_rows.clone();
    for delta in load_aggregate_physical_rows_for_delta(delta_chunks, layout)? {
        let row = merged
            .entry(delta.row_id.clone())
            .or_insert_with(|| zero_base_row(&delta, layout));
        if row.visible_values.len() != layout.visible_columns().len()
            || row.state_values.len() != layout.state_columns().len()
        {
            return Err(format!(
                "aggregate MV state corruption for row id `{}`: row shape does not match layout",
                row.row_id
            ));
        }
        for (state_index, state_column) in layout.state_columns().iter().enumerate() {
            row.state_values[state_index] = merge_state_value(
                row.state_values.get(state_index).cloned().unwrap_or(None),
                delta.state_values.get(state_index).cloned().unwrap_or(None),
                state_column,
            )?;
        }
        update_visible_values_from_state(row, layout)?;
    }
    let mut kept = Vec::new();
    let mut deleted = Vec::new();
    for row in merged.into_values() {
        if all_count_states_zero(&row, layout) {
            deleted.push(row);
        } else {
            kept.push(row);
        }
    }
    let row_delta = i64::try_from(kept.len())
        .and_then(|new_count| i64::try_from(old_row_count).map(|old_count| new_count - old_count))
        .map_err(|_| "aggregate MV row count overflow".to_string())?;
    Ok(MvAggregateMergeResult {
        upsert_chunks: physical_rows_to_chunks(kept, layout)?,
        delete_chunks: physical_rows_to_chunks(deleted, layout)?,
        row_delta,
    })
}

fn all_count_states_zero(row: &MvAggregatePhysicalRow, layout: &MvAggregateRuntimeLayout) -> bool {
    let mut saw_count = false;
    for (index, column) in layout.state_columns().iter().enumerate() {
        let is_count_role = matches!(
            (
                column.aggregate_kind(),
                column.state_role(),
                column.count_star()
            ),
            (
                MvAggregateRuntimeKind::Count,
                MvAggregateStateRole::Single,
                true
            ) | (
                MvAggregateRuntimeKind::Count,
                MvAggregateStateRole::RetractionCount,
                true
            )
        );
        if !is_count_role {
            continue;
        }
        saw_count = true;
        let zero = match (
            column.state_role(),
            row.state_values.get(index).cloned().unwrap_or(None),
        ) {
            (MvAggregateStateRole::RetractionCount, Some(AggScalarValue::Int64(value))) => {
                value == 0
            }
            (MvAggregateStateRole::Single, Some(AggScalarValue::Binary(bytes))) => {
                count_state_visible(&bytes)
                    .map(|value| value == 0)
                    .unwrap_or(false)
            }
            (MvAggregateStateRole::Single, None) => true,
            _ => false,
        };
        if !zero {
            return false;
        }
    }
    saw_count
}

fn materialize_aggregate_result_batch(
    batch: &RecordBatch,
    layout: &MvAggregateRuntimeLayout,
) -> Result<Chunk, String> {
    let visible_outputs = layout.visible_output_order();
    let reordered = reorder_state_shaped_input_batch_by_name(batch, layout, visible_outputs)?;
    let batch = reordered.as_ref().unwrap_or(batch);
    validate_state_shaped_input_schema(batch, layout, visible_outputs)?;
    let (group_key_batch_cols, state_batch_cols) =
        compute_batch_col_indexes(visible_outputs, layout);
    let expected = layout.group_key_source_indexes().len() + layout.state_columns().len();
    if batch.num_columns() != expected {
        return Err(format!(
            "aggregate MV materialize column count mismatch: batch_columns={} expected={expected} (group_keys={} + state_columns={})",
            batch.num_columns(),
            layout.group_key_source_indexes().len(),
            layout.state_columns().len()
        ));
    }
    let rows = batch.num_rows();
    let mut states = vec![Vec::with_capacity(rows); layout.state_columns().len()];
    for (index, batch_col) in state_batch_cols.iter().enumerate() {
        for row in 0..rows {
            states[index].push(agg_scalar_from_array(batch.column(*batch_col), row)?);
        }
    }
    let mut group_visible = HashMap::new();
    for (index, visible) in layout.group_key_source_indexes().iter().enumerate() {
        group_visible.insert(*visible, group_key_batch_cols[index]);
    }
    let mut visible = vec![Vec::with_capacity(rows); layout.visible_columns().len()];
    for row in 0..rows {
        let mut decoded = MvAggregatePhysicalRow {
            row_id: String::new(),
            visible_values: vec![None; visible.len()],
            state_values: states.iter().map(|column| column[row].clone()).collect(),
        };
        update_visible_values_from_state(&mut decoded, layout)?;
        for (&visible_index, &batch_index) in &group_visible {
            decoded.visible_values[visible_index] =
                agg_scalar_from_array(batch.column(batch_index), row)?;
        }
        for (index, value) in decoded.visible_values.into_iter().enumerate() {
            visible[index].push(value);
        }
    }
    let mut arrays = Vec::with_capacity(1 + visible.len() + states.len());
    arrays.push(build_row_id_array(batch, &group_key_batch_cols)?);
    for (index, column) in layout.visible_columns().iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            column.data_type(),
            std::mem::take(&mut visible[index]),
        )?);
    }
    for (index, column) in layout.state_columns().iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            column.data_type(),
            std::mem::take(&mut states[index]),
        )?);
    }
    chunk_from_batch(
        RecordBatch::try_new(Arc::new(physical_schema(layout)), arrays)
            .map_err(|error| format!("build aggregate MV physical batch failed: {error}"))?,
    )
}

fn reorder_state_shaped_input_batch_by_name(
    batch: &RecordBatch,
    layout: &MvAggregateRuntimeLayout,
    outputs: &[MvAggregateVisibleOutput],
) -> Result<Option<RecordBatch>, String> {
    let expected = state_shaped_input_fields(layout, outputs)?;
    if batch.num_columns() != expected.len() {
        return Ok(None);
    }
    let schema = batch.schema();
    let mut used = vec![false; batch.num_columns()];
    let mut permutation = Vec::with_capacity(expected.len());
    for (index, expected_field) in expected.iter().enumerate() {
        let matches = (0..batch.num_columns())
            .filter(|candidate| {
                !used[*candidate]
                    && schema
                        .field(*candidate)
                        .name()
                        .eq_ignore_ascii_case(expected_field.name())
                    && state_shaped_field_matches(
                        index,
                        schema.field(*candidate),
                        expected_field,
                        outputs,
                    )
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Ok(None);
        }
        used[matches[0]] = true;
        permutation.push(matches[0]);
    }
    if permutation
        .iter()
        .enumerate()
        .all(|(index, old)| index == *old)
        && expected
            .iter()
            .enumerate()
            .all(|(index, field)| schema.field(index).name() == field.name())
    {
        return Ok(None);
    }
    let fields: Vec<_> = permutation
        .iter()
        .zip(expected.iter())
        .map(|(old, expected)| {
            Arc::new(
                schema
                    .field(*old)
                    .clone()
                    .with_name(expected.name().clone()),
            )
        })
        .collect();
    let columns = permutation
        .iter()
        .map(|old| batch.column(*old).clone())
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map(Some)
        .map_err(|error| format!("reorder aggregate MV state-shaped input batch failed: {error}"))
}

fn validate_state_shaped_input_schema(
    batch: &RecordBatch,
    layout: &MvAggregateRuntimeLayout,
    outputs: &[MvAggregateVisibleOutput],
) -> Result<(), String> {
    let expected = state_shaped_input_fields(layout, outputs)?;
    if batch.num_columns() != expected.len() {
        return Ok(());
    }
    let schema = batch.schema();
    for (index, field) in expected.iter().enumerate() {
        let actual = schema.field(index);
        if !actual.name().eq_ignore_ascii_case(field.name())
            || !state_shaped_field_matches(index, actual, field, outputs)
        {
            return Err(format!(
                "aggregate MV state-shaped input schema mismatch at column {index}: got {}:{:?}:{} expected {}:{:?}:{}",
                actual.name(),
                actual.data_type(),
                actual.is_nullable(),
                field.name(),
                field.data_type(),
                field.is_nullable()
            ));
        }
    }
    Ok(())
}

fn state_shaped_field_matches(
    index: usize,
    actual: &Field,
    expected: &Field,
    outputs: &[MvAggregateVisibleOutput],
) -> bool {
    (actual.data_type() == expected.data_type()
        || (state_shaped_field_is_state(index, outputs)
            && is_varbinary(actual.data_type())
            && is_varbinary(expected.data_type())))
        && (actual.is_nullable() == expected.is_nullable()
            || (actual.is_nullable()
                && !expected.is_nullable()
                && state_shaped_field_is_state(index, outputs)))
}

fn state_shaped_field_is_state(index: usize, outputs: &[MvAggregateVisibleOutput]) -> bool {
    index >= outputs.len()
        || matches!(
            outputs.get(index),
            Some(MvAggregateVisibleOutput::Aggregate(_))
        )
}

fn state_shaped_input_fields(
    layout: &MvAggregateRuntimeLayout,
    outputs: &[MvAggregateVisibleOutput],
) -> Result<Vec<Field>, String> {
    let mut fields = Vec::with_capacity(outputs.len() + layout.state_columns().len());
    for output in outputs {
        match output {
            MvAggregateVisibleOutput::GroupKey(index) => {
                let visible_index = *layout.group_key_source_indexes().get(*index).ok_or_else(
                    || {
                        format!(
                            "aggregate MV state-shaped schema group key index {index} out of range"
                        )
                    },
                )?;
                let visible = layout.visible_columns().get(visible_index).ok_or_else(|| format!("aggregate MV state-shaped schema visible source index {visible_index} out of range"))?;
                fields.push(Field::new(
                    visible.name(),
                    visible.data_type().clone(),
                    visible.nullable(),
                ));
            }
            MvAggregateVisibleOutput::Aggregate(index) => {
                let state = layout.state_columns().iter().find(|column| column.state_role() == MvAggregateStateRole::Single && column.aggregate_index() == *index).ok_or_else(|| format!("aggregate MV state-shaped schema missing state column for aggregate index {index}"))?;
                fields.push(Field::new(state.name(), state.data_type().clone(), false));
            }
        }
    }
    for state in layout
        .state_columns()
        .iter()
        .filter(|column| column.state_role() == MvAggregateStateRole::RetractionCount)
    {
        fields.push(Field::new(state.name(), state.data_type().clone(), false));
    }
    Ok(fields)
}

fn compute_batch_col_indexes(
    outputs: &[MvAggregateVisibleOutput],
    layout: &MvAggregateRuntimeLayout,
) -> (Vec<usize>, Vec<usize>) {
    let mut group = vec![0; layout.group_key_source_indexes().len()];
    let mut aggregate = vec![0; layout.aggregate_input_types().len()];
    let mut current = 0;
    for output in outputs {
        match output {
            MvAggregateVisibleOutput::GroupKey(index) => group[*index] = current,
            MvAggregateVisibleOutput::Aggregate(index) => aggregate[*index] = current,
        }
        current += 1;
    }
    let mut trailing = current;
    let state = layout
        .state_columns()
        .iter()
        .map(|column| match column.state_role() {
            MvAggregateStateRole::Single => aggregate[column.aggregate_index()],
            MvAggregateStateRole::RetractionCount => {
                let index = trailing;
                trailing += 1;
                index
            }
        })
        .collect();
    (group, state)
}

fn build_row_id_array(batch: &RecordBatch, group_columns: &[usize]) -> Result<ArrayRef, String> {
    let columns = group_columns
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect::<Vec<_>>();
    crate::exec::mv::group_row_id::aggregate_group_row_id_array(&columns)
}

fn load_rows_from_batch(
    batch: &RecordBatch,
    layout: &MvAggregateRuntimeLayout,
    allow_negative_counts: bool,
) -> Result<Vec<MvAggregatePhysicalRow>, String> {
    let expected = 1 + layout.visible_columns().len() + layout.state_columns().len();
    if batch.num_columns() < expected {
        return Err(format!(
            "aggregate MV physical column count mismatch: batch_columns={} expected_at_least={expected}",
            batch.num_columns()
        ));
    }
    let row_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "aggregate MV physical row id column must be Utf8".to_string())?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if row_ids.is_null(row) {
            return Err("aggregate MV physical row id cannot be NULL".to_string());
        }
        let visible_values = (0..layout.visible_columns().len())
            .map(|index| agg_scalar_from_array(batch.column(1 + index), row))
            .collect::<Result<Vec<_>, _>>()?;
        let offset = 1 + layout.visible_columns().len();
        let state_values = (0..layout.state_columns().len())
            .map(|index| agg_scalar_from_array(batch.column(offset + index), row))
            .collect::<Result<Vec<_>, _>>()?;
        let row_id = row_ids.value(row).to_string();
        validate_loaded_row(
            batch,
            row,
            &row_id,
            &state_values,
            layout,
            allow_negative_counts,
        )?;
        rows.push(MvAggregatePhysicalRow {
            row_id,
            visible_values,
            state_values,
        });
    }
    Ok(rows)
}

fn validate_loaded_row(
    batch: &RecordBatch,
    row: usize,
    row_id: &str,
    states: &[Option<AggScalarValue>],
    layout: &MvAggregateRuntimeLayout,
    allow_negative_counts: bool,
) -> Result<(), String> {
    let computed = physical_row_id_from_visible_group_keys(batch, row, layout)?;
    if computed != row_id {
        return Err(format!(
            "aggregate MV state corruption: stored row id `{row_id}` does not match visible group key row id `{computed}`"
        ));
    }
    for (index, column) in layout.state_columns().iter().enumerate() {
        if column.aggregate_kind() != MvAggregateRuntimeKind::Count {
            continue;
        }
        let count = match (column.state_role(), states.get(index).cloned().unwrap_or(None)) {
            (MvAggregateStateRole::RetractionCount, Some(AggScalarValue::Int64(value))) => value,
            (MvAggregateStateRole::Single, Some(AggScalarValue::Binary(bytes))) => decode_count_state(&bytes).map_err(|error| format!("aggregate MV state corruption: COUNT state column `{}` has invalid VARBINARY for row id `{row_id}`: {error}", column.name()))?,
            (_, None) => return Err(format!("aggregate MV state corruption: COUNT state column `{}` is NULL for row id `{row_id}`", column.name())),
            (_, other) => return Err(format!("aggregate MV state corruption: COUNT state column `{}` has invalid value {other:?} for row id `{row_id}`", column.name())),
        };
        if !allow_negative_counts && !(count > 0 || (!column.count_star() && count == 0)) {
            let restriction = if column.count_star() {
                "positive"
            } else {
                "non-negative"
            };
            return Err(format!(
                "aggregate MV state corruption: COUNT state column `{}` must be {restriction} for row id `{row_id}`, got {count}",
                column.name()
            ));
        }
    }
    Ok(())
}

fn physical_row_id_from_visible_group_keys(
    batch: &RecordBatch,
    row: usize,
    layout: &MvAggregateRuntimeLayout,
) -> Result<String, String> {
    let columns = layout
        .group_key_source_indexes()
        .iter()
        .map(|index| batch.column(1 + *index).clone())
        .collect::<Vec<_>>();
    crate::exec::mv::group_row_id::aggregate_group_row_id_at(&columns, row)
}

fn physical_rows_to_chunks(
    mut rows: Vec<MvAggregatePhysicalRow>,
    layout: &MvAggregateRuntimeLayout,
) -> Result<Vec<Chunk>, String> {
    rows.sort_by(|left, right| left.row_id.cmp(&right.row_id));
    let mut arrays =
        Vec::with_capacity(1 + layout.visible_columns().len() + layout.state_columns().len());
    arrays.push(Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.row_id.as_str())
            .collect::<Vec<_>>(),
    )) as ArrayRef);
    for (index, column) in layout.visible_columns().iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            column.data_type(),
            rows.iter()
                .map(|row| row.visible_values[index].clone())
                .collect(),
        )?);
    }
    for (index, column) in layout.state_columns().iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            column.data_type(),
            rows.iter()
                .map(|row| row.state_values[index].clone())
                .collect(),
        )?);
    }
    Ok(vec![chunk_from_batch(
        RecordBatch::try_new(Arc::new(physical_schema(layout)), arrays)
            .map_err(|error| format!("build aggregate MV merged physical batch failed: {error}"))?,
    )?])
}

fn chunk_from_batch(batch: RecordBatch) -> Result<Chunk, String> {
    let slot_ids = (1..=batch.num_columns())
        .map(|index| SlotId::new(index as u32))
        .collect::<Vec<_>>();
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Chunk::try_new_with_chunk_schema(batch, schema)
}

fn zero_base_row(
    delta: &MvAggregatePhysicalRow,
    layout: &MvAggregateRuntimeLayout,
) -> MvAggregatePhysicalRow {
    MvAggregatePhysicalRow {
        row_id: delta.row_id.clone(),
        visible_values: delta.visible_values.clone(),
        state_values: layout
            .state_columns()
            .iter()
            .map(|column| match column.state_role() {
                MvAggregateStateRole::Single => Some(AggScalarValue::Binary(Vec::new())),
                MvAggregateStateRole::RetractionCount => Some(AggScalarValue::Int64(0)),
            })
            .collect(),
    }
}

fn merge_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    column: &novarocks_types::mv_aggregate_layout::MvAggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    if column.state_role() == MvAggregateStateRole::RetractionCount {
        let old = int64_state_value(old, column.name())?;
        let delta = int64_state_value(delta, column.name())?;
        return Ok(Some(AggScalarValue::Int64(
            old.checked_add(delta).ok_or_else(|| {
                format!(
                    "aggregate MV state merge overflow for column `{}`",
                    column.name()
                )
            })?,
        )));
    }
    let old = binary_state_value(old, column.name())?;
    let delta = binary_state_value(delta, column.name())?;
    let merged = match column.aggregate_kind() {
        MvAggregateRuntimeKind::Count => count_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::Sum => sum_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::Avg => avg_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::Min => min_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::Max => max_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::BoolOr => bool_or_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::BoolAnd => bool_and_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::CountDistinct => count_distinct_state_union(&old, &delta)?,
        MvAggregateRuntimeKind::ApproxCountDistinct => {
            approx_count_distinct_state_union(&old, &delta)?
        }
    };
    Ok(Some(AggScalarValue::Binary(merged)))
}

fn binary_state_value(value: Option<AggScalarValue>, name: &str) -> Result<Vec<u8>, String> {
    match value {
        Some(AggScalarValue::Binary(bytes)) => Ok(bytes),
        None => Ok(Vec::new()),
        other => Err(format!(
            "aggregate MV state type mismatch for column `{name}`: expected VARBINARY, got {other:?}"
        )),
    }
}

fn int64_state_value(value: Option<AggScalarValue>, name: &str) -> Result<i64, String> {
    match value {
        Some(AggScalarValue::Int64(value)) => Ok(value),
        None => Err(format!(
            "aggregate MV state corruption: COUNT state column `{name}` is NULL"
        )),
        other => Err(format!(
            "aggregate MV state type mismatch for column `{name}`: expected integer, got {other:?}"
        )),
    }
}

fn physical_schema(layout: &MvAggregateRuntimeLayout) -> Schema {
    let mut fields =
        Vec::with_capacity(1 + layout.visible_columns().len() + layout.state_columns().len());
    fields.push(Field::new(
        layout.row_id_column_name(),
        DataType::Utf8,
        false,
    ));
    fields.extend(
        layout
            .visible_columns()
            .iter()
            .map(|column| Field::new(column.name(), column.data_type().clone(), column.nullable())),
    );
    fields.extend(
        layout
            .state_columns()
            .iter()
            .map(|column| Field::new(column.name(), column.data_type().clone(), column.nullable())),
    );
    Schema::new(fields)
}

fn update_visible_values_from_state(
    row: &mut MvAggregatePhysicalRow,
    layout: &MvAggregateRuntimeLayout,
) -> Result<(), String> {
    for (index, column) in layout.state_columns().iter().enumerate() {
        if column.state_role() == MvAggregateStateRole::RetractionCount {
            continue;
        }
        let bytes = match row.state_values[index].as_ref() {
            Some(AggScalarValue::Binary(bytes)) => bytes.as_slice(),
            None => &[],
            Some(other) => {
                return Err(format!(
                    "aggregate MV state on column `{}` must be VARBINARY, got {other:?}",
                    column.name()
                ));
            }
        };
        let visible_index = column.visible_source_index();
        let input_type = layout
            .aggregate_input_types()
            .get(column.aggregate_index())
            .and_then(Option::as_ref);
        row.visible_values[visible_index] = derive_visible(
            column.aggregate_kind(),
            bytes,
            layout.visible_columns()[visible_index].data_type(),
            input_type,
            column.name(),
        )?;
    }
    Ok(())
}

fn derive_visible(
    kind: MvAggregateRuntimeKind,
    state: &[u8],
    visible_type: &DataType,
    input_type: Option<&DataType>,
    name: &str,
) -> Result<Option<AggScalarValue>, String> {
    match kind {
        MvAggregateRuntimeKind::Count => {
            Ok(Some(AggScalarValue::Int64(count_state_visible(state)?)))
        }
        MvAggregateRuntimeKind::Sum => match visible_type {
            DataType::Decimal128(_, _) => {
                let (count, sum) = decode_sum_decimal128(state)?;
                Ok((count != 0).then_some(AggScalarValue::Decimal128(sum)))
            }
            _ => {
                let (count, sum) = decode_sum_int64(state)?;
                Ok((count != 0).then_some(AggScalarValue::Int64(sum)))
            }
        },
        MvAggregateRuntimeKind::Avg => derive_avg_visible(state, visible_type, input_type),
        MvAggregateRuntimeKind::Min => min_state_visible_key_value(state, visible_type)
            .map(|value| value.map(key_value_to_agg_scalar))
            .map_err(|error| format!("derive visible for column `{name}` failed: {error}")),
        MvAggregateRuntimeKind::Max => max_state_visible_key_value(state, visible_type)
            .map(|value| value.map(key_value_to_agg_scalar))
            .map_err(|error| format!("derive visible for column `{name}` failed: {error}")),
        MvAggregateRuntimeKind::BoolOr => bool_or_state_visible(state)
            .map(|value| value.map(AggScalarValue::Bool))
            .map_err(|error| format!("derive visible for column `{name}` failed: {error}")),
        MvAggregateRuntimeKind::BoolAnd => bool_and_state_visible(state)
            .map(|value| value.map(AggScalarValue::Bool))
            .map_err(|error| format!("derive visible for column `{name}` failed: {error}")),
        MvAggregateRuntimeKind::CountDistinct => Ok(Some(AggScalarValue::Int64(
            count_distinct_state_visible(state)?,
        ))),
        MvAggregateRuntimeKind::ApproxCountDistinct => Ok(Some(AggScalarValue::Int64(
            approx_count_distinct_state_visible(state)?,
        ))),
    }
}

fn derive_avg_visible(
    state: &[u8],
    visible_type: &DataType,
    input_type: Option<&DataType>,
) -> Result<Option<AggScalarValue>, String> {
    match visible_type {
        DataType::Float64 => {
            let (count, sum) = decode_avg_int64(state)?;
            Ok((count != 0).then_some(AggScalarValue::Float64(sum as f64 / count as f64)))
        }
        DataType::Decimal128(_, output_scale) => {
            let Some(DataType::Decimal128(_, input_scale)) = input_type else {
                return Err(
                    "AVG Decimal128 visible derivation requires input decimal scale metadata"
                        .to_string(),
                );
            };
            let (count, sum) = decode_avg_decimal128(state)?;
            if count == 0 {
                return Ok(None);
            }
            let difference = i32::from(*output_scale) - i32::from(*input_scale);
            let scaled = if difference == 0 {
                sum
            } else {
                let factor = pow10_i128(difference.unsigned_abs() as usize)?;
                if difference > 0 {
                    sum.checked_mul(factor)
                        .ok_or_else(|| "decimal overflow".to_string())?
                } else {
                    sum / factor
                }
            };
            Ok(Some(AggScalarValue::Decimal128(div_round_i128(
                scaled,
                count as i128,
            ))))
        }
        other => Err(format!(
            "AVG visible derivation unsupported for output type {other:?}"
        )),
    }
}

fn key_value_to_agg_scalar(value: KeyValue) -> AggScalarValue {
    match value {
        KeyValue::Bool(value) => AggScalarValue::Bool(value),
        KeyValue::Int8(value) => AggScalarValue::Int64(value as i64),
        KeyValue::Int16(value) => AggScalarValue::Int64(value as i64),
        KeyValue::Int32(value) => AggScalarValue::Int64(value as i64),
        KeyValue::Int64(value) => AggScalarValue::Int64(value),
        KeyValue::Float32(bits) => AggScalarValue::Float64(f32::from_bits(bits) as f64),
        KeyValue::Float64(bits) => AggScalarValue::Float64(f64::from_bits(bits)),
        KeyValue::Decimal128(value) => AggScalarValue::Decimal128(value),
        KeyValue::Date32(value) => AggScalarValue::Date32(value),
        KeyValue::Timestamp(value) => AggScalarValue::Timestamp(value),
        KeyValue::Utf8(value) => AggScalarValue::Utf8(value),
    }
}

fn is_varbinary(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::mv::state_codec::encode_count_state;
    use arrow::array::{Int64Array, LargeBinaryArray};
    use novarocks_types::mv_aggregate_layout::{MvAggregateStateColumn, MvAggregateVisibleColumn};

    fn layout() -> MvAggregateRuntimeLayout {
        MvAggregateRuntimeLayout::try_new(
            "__row_id__".to_string(),
            vec![
                MvAggregateVisibleColumn::new("group_key".to_string(), DataType::Int64, false, 0),
                MvAggregateVisibleColumn::new("count_v".to_string(), DataType::Int64, false, 1),
            ],
            vec![MvAggregateStateColumn::new(
                "count_state".to_string(),
                DataType::LargeBinary,
                false,
                1,
                0,
                MvAggregateRuntimeKind::Count,
                MvAggregateStateRole::Single,
                true,
            )],
            vec![None],
            vec![0],
        )
        .expect("layout")
    }

    #[test]
    fn materialize_and_merge_count_state() {
        let layout = layout();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("group_key", DataType::Int64, false),
                Field::new("count_state", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                Arc::new(LargeBinaryArray::from(vec![
                    encode_count_state(2).as_slice(),
                ])) as ArrayRef,
            ],
        )
        .expect("batch");
        let source = chunk_from_batch(batch).expect("source chunk");
        let materialized =
            materialize_aggregate_result_chunks(vec![source], &layout).expect("materialize");
        let old = build_old_state_map(&materialized, &layout).expect("old state");
        let merged = merge_aggregate_state_batches_with_retractions(&old, &materialized, &layout)
            .expect("merge");
        assert_eq!(merged.row_delta, 0);
        assert_eq!(merged.upsert_chunks[0].batch.num_rows(), 1);
        let counts = merged.upsert_chunks[0]
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count visible");
        assert_eq!(counts.value(0), 4);
    }
}
