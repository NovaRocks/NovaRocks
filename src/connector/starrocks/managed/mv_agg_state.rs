//! Aggregate MV state helpers for aggregate MV incremental refresh.
//
// TODO: file is ~4500 lines; consider splitting into mv_agg_state/{layout,
// merge,negate,derive_visible,value_count_map}.rs submodules if it grows past
// 5000.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, MapArray, StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_buffer::OffsetBuffer;

use crate::connector::starrocks::managed::ddl::{ManagedPhysicalColumn, managed_physical_column};
use crate::connector::starrocks::managed::mv_ddl;
use crate::connector::starrocks::managed::mv_shape::{
    AggregateFunctionKind, AggregateInput, AggregateMvShape, VisibleAggregateOutput,
};
use crate::engine::{QueryResult, record_batch_to_chunk};
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{
    AggScalarValue, agg_scalar_from_array, build_agg_scalar_array, compare_agg_scalar_values,
};
use crate::sql::analysis::OutputColumn;
use crate::sql::parser::ast::SqlType;

pub(crate) const ROW_ID_COLUMN: &str = "__row_id__";
pub(crate) const AGG_STATE_PREFIX: &str = "__agg_state_";
pub(crate) const AGG_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AggregateMvLayout {
    pub(crate) row_id_column: ManagedPhysicalColumn,
    pub(crate) visible_columns: Vec<AggregateVisibleColumn>,
    pub(crate) state_columns: Vec<AggregateStateColumn>,
    pub(crate) group_key_source_indexes: Vec<usize>,
    pub(crate) physical_columns: Vec<ManagedPhysicalColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateVisibleColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) sql_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) source_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) sql_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) visible_source_index: usize,
    /// Index into `AggregateMvShape::aggregates` — multiple state columns
    /// (e.g., AVG's AvgSum + AvgCount) share the same `aggregate_index`.
    pub(crate) aggregate_index: usize,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) state_role: AggregateStateRole,
    pub(crate) count_star: bool,
}

/// Identifies a state column's role within its logical aggregate.
///
/// Cardinality contract: at most one `Single` per `aggregate_index`,
/// or exactly one `AvgSum` + one `AvgCount` pair per `aggregate_index`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateStateRole {
    /// Single state column: state value IS the aggregate result.
    /// Used by COUNT, SUM, MIN, MAX.
    Single,
    /// AVG sum sub-state (Int64 for integer inputs, Decimal128 for decimal inputs).
    AvgSum,
    /// AVG count sub-state (always Int64).
    AvgCount,
    /// Hidden row-count state used only to decide whether a group has been fully retracted.
    RetractionCount,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregatePhysicalRow {
    pub(crate) row_id: String,
    pub(crate) visible_values: Vec<Option<AggScalarValue>>,
    pub(crate) state_values: Vec<Option<AggScalarValue>>,
}

pub(crate) struct AggregateMergeResult {
    pub(crate) upsert_chunks: Vec<Chunk>,
    pub(crate) delete_chunks: Vec<Chunk>,
    pub(crate) row_delta: i64,
}

pub(crate) fn build_aggregate_mv_layout(
    shape: &AggregateMvShape,
    output_columns: &[OutputColumn],
) -> Result<AggregateMvLayout, String> {
    if output_columns.len() != shape.visible_outputs.len() {
        return Err(format!(
            "aggregate MV output count mismatch: shape_outputs={} analyzed_outputs={}",
            shape.visible_outputs.len(),
            output_columns.len()
        ));
    }

    let row_id_column = managed_physical_column(
        ROW_ID_COLUMN.to_string(),
        SqlType::String,
        false,
        false,
        true,
    );
    let mut physical_columns = vec![row_id_column.clone()];
    let group_key_source_indexes = group_key_source_indexes(shape)?;

    let visible_columns = output_columns
        .iter()
        .enumerate()
        .map(|(source_index, column)| {
            let sql_type = mv_ddl::arrow_data_type_to_sql_type(&column.data_type)?;
            physical_columns.push(managed_physical_column(
                column.name.clone(),
                sql_type.clone(),
                column.nullable,
                true,
                false,
            ));
            Ok(AggregateVisibleColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                sql_type,
                nullable: column.nullable,
                source_index,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut state_columns = Vec::new();
    for (aggregate_index, aggregate) in shape.aggregates.iter().enumerate() {
        let visible_source_index = aggregate_visible_source_index(shape, aggregate_index)?;
        let visible = output_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "aggregate MV visible source index out of range: aggregate_index={aggregate_index} source_index={visible_source_index}"
            )
        })?;
        let visible_sql_type = mv_ddl::arrow_data_type_to_sql_type(&visible.data_type)?;
        let sanitized = sanitize_state_column_name(&aggregate.output_name);
        let count_star = matches!(aggregate.input, AggregateInput::Star);

        match aggregate.function {
            AggregateFunctionKind::Count | AggregateFunctionKind::Sum => {
                let state_name = format!("{}{}", AGG_STATE_PREFIX, sanitized);
                validate_state_column_type(
                    aggregate.function,
                    AggregateStateRole::Single,
                    &visible.data_type,
                    &state_name,
                )?;
                physical_columns.push(managed_physical_column(
                    state_name.clone(),
                    visible_sql_type.clone(),
                    visible.nullable,
                    false,
                    false,
                ));
                state_columns.push(AggregateStateColumn {
                    name: state_name,
                    data_type: visible.data_type.clone(),
                    sql_type: visible_sql_type,
                    nullable: visible.nullable,
                    visible_source_index,
                    aggregate_index,
                    function: aggregate.function,
                    state_role: AggregateStateRole::Single,
                    count_star,
                });
            }
            AggregateFunctionKind::Min | AggregateFunctionKind::Max => {
                // IVM-P5: MIN/MAX state is a value-count detail map
                // (`Map<input_type, Int64>`). Map keys are the distinct values
                // observed in the group; values are the per-key occurrence
                // counts. The rewriter / merge / derive / DDL paths are all
                // wired through Phase 5. Float keys are still rejected by
                // `validate_state_column_type` until canonical-NaN handling
                // lands; non-Float MIN/MAX is fully incremental, including
                // on DELETE deltas.
                let state_name = format!("{}{}", AGG_STATE_PREFIX, sanitized);
                let key_arrow_type = visible.data_type.clone();
                let key_sql_type = mv_ddl::arrow_data_type_to_sql_type(&key_arrow_type)?;
                // Iceberg-rust convention: entries-struct field is named
                // "key_value" (iceberg-0.9 `DEFAULT_MAP_FIELD_NAME`) and the
                // value field is nullable. Aligning the MV state column
                // declaration with this convention lets the Iceberg sink
                // re-annotate field IDs on the runtime chunk without a
                // schema-name mismatch (the runtime aggregate emits the same
                // shape via `map_value_count`).
                let entries_struct = DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new(
                        "key",
                        key_arrow_type,
                        /* nullable = */ false,
                    )),
                    Arc::new(Field::new(
                        "value",
                        DataType::Int64,
                        /* nullable = */ true,
                    )),
                ]));
                let map_arrow_type = DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        entries_struct,
                        /* nullable = */ false,
                    )),
                    /* keys_sorted = */ false,
                );
                let map_sql_type = SqlType::Map(Box::new(key_sql_type), Box::new(SqlType::BigInt));
                validate_state_column_type(
                    aggregate.function,
                    AggregateStateRole::Single,
                    &map_arrow_type,
                    &state_name,
                )?;
                physical_columns.push(managed_physical_column(
                    state_name.clone(),
                    map_sql_type.clone(),
                    /* nullable = */ false,
                    false,
                    false,
                ));
                state_columns.push(AggregateStateColumn {
                    name: state_name,
                    data_type: map_arrow_type,
                    sql_type: map_sql_type,
                    nullable: false,
                    visible_source_index,
                    aggregate_index,
                    function: aggregate.function,
                    state_role: AggregateStateRole::Single,
                    count_star,
                });
            }
            AggregateFunctionKind::Avg => {
                let (sum_dt, sum_sql) =
                    avg_sum_state_type(&visible.data_type).ok_or_else(|| {
                        format!(
                            "AVG state type is unsupported for column `{}{}__sum`: {:?}",
                            AGG_STATE_PREFIX, sanitized, visible.data_type
                        )
                    })?;
                let count_dt = DataType::Int64;
                let count_sql = SqlType::BigInt;

                let sum_name = format!("{}{}__sum", AGG_STATE_PREFIX, sanitized);
                let count_name = format!("{}{}__count", AGG_STATE_PREFIX, sanitized);

                validate_state_column_type(
                    AggregateFunctionKind::Avg,
                    AggregateStateRole::AvgSum,
                    &sum_dt,
                    &sum_name,
                )?;

                physical_columns.push(managed_physical_column(
                    sum_name.clone(),
                    sum_sql.clone(),
                    /* nullable */ true,
                    false,
                    false,
                ));
                physical_columns.push(managed_physical_column(
                    count_name.clone(),
                    count_sql.clone(),
                    /* nullable */ false,
                    false,
                    false,
                ));

                state_columns.push(AggregateStateColumn {
                    name: sum_name,
                    data_type: sum_dt,
                    sql_type: sum_sql,
                    nullable: true,
                    visible_source_index,
                    aggregate_index,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgSum,
                    count_star: false,
                });
                state_columns.push(AggregateStateColumn {
                    name: count_name,
                    data_type: count_dt,
                    sql_type: count_sql,
                    nullable: false,
                    visible_source_index,
                    aggregate_index,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgCount,
                    count_star: false,
                });
            }
        }
    }

    if aggregate_shape_needs_retraction_count_state(shape) {
        validate_state_column_type(
            AggregateFunctionKind::Count,
            AggregateStateRole::RetractionCount,
            &DataType::Int64,
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        )?;
        physical_columns.push(managed_physical_column(
            AGG_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            SqlType::BigInt,
            false,
            false,
            false,
        ));
        state_columns.push(AggregateStateColumn {
            name: AGG_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: false,
            visible_source_index: 0,
            aggregate_index: shape.aggregates.len(),
            function: AggregateFunctionKind::Count,
            state_role: AggregateStateRole::RetractionCount,
            count_star: true,
        });
    }

    Ok(AggregateMvLayout {
        row_id_column,
        visible_columns,
        state_columns,
        group_key_source_indexes,
        physical_columns,
    })
}

pub(crate) fn aggregate_shape_needs_retraction_count_state(shape: &AggregateMvShape) -> bool {
    !shape.aggregates.iter().any(|aggregate| {
        aggregate.function == AggregateFunctionKind::Count
            && matches!(aggregate.input, AggregateInput::Star)
    })
}

pub(crate) fn materialize_aggregate_result_chunks(
    result: QueryResult,
    layout: &AggregateMvLayout,
    shape: &AggregateMvShape,
) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| materialize_aggregate_result_batch(&chunk.batch, layout, shape))
        .collect()
}

pub(crate) fn load_aggregate_physical_rows(
    chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<HashMap<String, AggregatePhysicalRow>, String> {
    let mut rows = HashMap::new();
    for chunk in chunks {
        load_aggregate_physical_rows_from_batch(
            &chunk.batch,
            layout,
            &mut rows,
            /* allow_negative_counts */ false,
        )?;
    }
    Ok(rows)
}

/// Permissive variant for loading delta chunks during incremental
/// merge. Skips count-state positivity checks (negated DELETE-branch
/// state values are valid by construction post-`negate_aggregate_state_chunks`)
/// and skips the visible/state equality invariant (negation flips the
/// state column but leaves visible columns unchanged, so equality
/// no longer holds — and visible values are unused by the merge math
/// anyway).
pub(crate) fn load_aggregate_physical_rows_for_delta(
    chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<AggregatePhysicalRow>, String> {
    let mut rows = Vec::new();
    for chunk in chunks {
        rows.extend(load_aggregate_physical_rows_from_batch_owned(
            &chunk.batch,
            layout,
            /* allow_negative_counts */ true,
        )?);
    }
    Ok(rows)
}

pub(crate) fn build_old_state_map(
    chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<HashMap<String, AggregatePhysicalRow>, String> {
    load_aggregate_physical_rows(chunks, layout).map_err(|err| {
        if err.contains("duplicate row id") {
            format!("active aggregate MV state corruption: duplicate active MV row id: {err}")
        } else {
            err
        }
    })
}

#[allow(dead_code)]
pub(crate) fn merge_aggregate_state_batches(
    old_rows: &HashMap<String, AggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    Ok(
        merge_aggregate_state_batches_with_retractions(old_rows, delta_chunks, layout)?
            .upsert_chunks,
    )
}

pub(crate) fn merge_aggregate_state_batches_with_retractions(
    old_rows: &HashMap<String, AggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<AggregateMergeResult, String> {
    let old_row_count = old_rows.len();
    let mut merged = old_rows.clone();
    let delta_rows = load_aggregate_physical_rows_for_delta(delta_chunks, layout)?;
    for delta in delta_rows {
        let row = merged
            .entry(delta.row_id.clone())
            .or_insert_with(|| zero_base_row(&delta, layout));
        if row.visible_values.len() != layout.visible_columns.len()
            || row.state_values.len() != layout.state_columns.len()
        {
            return Err(format!(
                "aggregate MV state corruption for row id `{}`: row shape does not match layout",
                row.row_id
            ));
        }
        // Step A: merge state values
        for (state_index, state_column) in layout.state_columns.iter().enumerate() {
            let next_value = merge_state_value(
                row.state_values.get(state_index).cloned().unwrap_or(None),
                delta.state_values.get(state_index).cloned().unwrap_or(None),
                state_column,
            )?;
            row.state_values[state_index] = next_value;
        }

        // Step B: derive visible values per-aggregate (Single = direct copy of state)
        update_visible_values_from_state(row, layout)?;
    }
    let mut merged_kept = Vec::new();
    let mut merged_deleted = Vec::new();
    for row in merged.into_values() {
        if all_count_states_zero(&row, layout) {
            merged_deleted.push(row);
        } else {
            merged_kept.push(row);
        }
    }
    let row_delta = i64::try_from(merged_kept.len())
        .and_then(|new_count| i64::try_from(old_row_count).map(|old_count| new_count - old_count))
        .map_err(|_| "aggregate MV row count overflow".to_string())?;
    Ok(AggregateMergeResult {
        upsert_chunks: physical_rows_to_chunks(merged_kept, layout)?,
        delete_chunks: physical_rows_to_chunks(merged_deleted, layout)?,
        row_delta,
    })
}

/// Return true when every row-cardinality state on the row has merged to zero.
/// SUM/AVG-count/COUNT(expr) states do not influence the decision: a visible
/// aggregate can be zero or NULL while the group still has remaining rows.
fn all_count_states_zero(row: &AggregatePhysicalRow, layout: &AggregateMvLayout) -> bool {
    let mut saw_count = false;
    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        let is_count_role = matches!(
            (
                state_column.function,
                state_column.state_role,
                state_column.count_star
            ),
            (
                AggregateFunctionKind::Count,
                AggregateStateRole::Single,
                true
            ) | (
                AggregateFunctionKind::Count,
                AggregateStateRole::RetractionCount,
                true
            )
        );
        if !is_count_role {
            continue;
        }
        saw_count = true;
        let value = row.state_values.get(state_index).cloned().unwrap_or(None);
        let is_zero = matches!(value, Some(AggScalarValue::Int64(0)));
        if !is_zero {
            return false;
        }
    }
    // If the layout has no count-state columns at all, never drop.
    saw_count
}

/// Materialize a state-shaped executor result batch into a physical batch.
///
/// **State-shaped input**: the executor output after `rewrite_select_sql_for_state` has been
/// applied. Column layout (in `shape.visible_outputs` order):
/// - GroupKey columns: one column per group key, in the order they appear in the projection.
/// - Single-role aggregate (COUNT, SUM, MIN, MAX): one column per aggregate, carrying the
///   state value directly (visible == state for these functions).
/// - AVG aggregate: two consecutive columns — AvgSum first, then AvgCount — replacing the
///   one AVG-result column that the un-rewritten query would have produced.
///
/// The output is a physical batch in `physical_schema(layout)` layout:
/// `[__row_id__, visible_cols..., state_cols...]`.
fn materialize_aggregate_result_batch(
    batch: &RecordBatch,
    layout: &AggregateMvLayout,
    shape: &AggregateMvShape,
) -> Result<Chunk, String> {
    let (group_key_batch_cols, state_col_batch_cols) = compute_batch_col_indexes(shape, layout);

    let expected = shape.group_keys.len() + layout.state_columns.len();
    if batch.num_columns() != expected {
        return Err(format!(
            "aggregate MV materialize column count mismatch: \
             batch_columns={} expected={expected} \
             (group_keys={} + state_columns={})",
            batch.num_columns(),
            shape.group_keys.len(),
            layout.state_columns.len()
        ));
    }

    let num_rows = batch.num_rows();
    let num_state_cols = layout.state_columns.len();
    let num_visible_cols = layout.visible_columns.len();

    // Collect all state column values row by row.
    //
    // For AvgSum columns with Decimal128 type: the executor SUM output arrives at the
    // input column's scale (SUM preserves input scale), while the state column is declared
    // at the analyzer-promoted visible scale. We rescale the raw i128 on ingestion so that
    // `derive_avg_visible` can perform integer division directly at the stored scale.
    //
    // Example: AVG(Decimal(20,4)) -> visible Decimal128(38,10), SUM output Decimal128(38,4).
    //   raw i128 300.5000 = 3005000 at scale 4; multiply by 10^(10-4)=10^6 -> 3005000000000.
    //   derive_avg_visible: 3005000000000 / count gives the correct scale-10 result.
    let mut all_state_values: Vec<Vec<Option<AggScalarValue>>> =
        vec![Vec::with_capacity(num_rows); num_state_cols];
    for (sc_idx, &batch_col) in state_col_batch_cols.iter().enumerate() {
        let column = batch.column(batch_col);
        let sc = &layout.state_columns[sc_idx];
        // Compute scale-up factor for AvgSum Decimal128 columns where batch scale < state scale.
        let decimal_scale_factor: Option<i128> = if sc.state_role == AggregateStateRole::AvgSum {
            if let (DataType::Decimal128(_, state_scale), DataType::Decimal128(_, batch_scale)) =
                (&sc.data_type, column.data_type())
            {
                let diff = (*state_scale as i32) - (*batch_scale as i32);
                if diff > 0 {
                    Some(10_i128.checked_pow(diff as u32).ok_or_else(|| {
                            format!(
                                "AVG Decimal128 sum rescale factor overflow: state_scale={state_scale} batch_scale={batch_scale}"
                            )
                        })?)
                } else if diff == 0 {
                    None
                } else {
                    return Err(format!(
                        "AVG Decimal128 sum scale mismatch: state_scale={state_scale} batch_scale={batch_scale}"
                    ));
                }
            } else {
                None
            }
        } else {
            None
        };

        for row in 0..num_rows {
            let mut val = agg_scalar_from_array(column, row)?;
            if let (Some(factor), Some(AggScalarValue::Decimal128(raw))) =
                (decimal_scale_factor, &val)
            {
                val = Some(AggScalarValue::Decimal128(
                    raw.checked_mul(factor).ok_or_else(|| {
                        format!("AVG Decimal128 sum rescale overflow: raw={raw} factor={factor}")
                    })?,
                ));
            }
            all_state_values[sc_idx].push(val);
        }
    }

    // Map visible_source_index → batch column index for group key columns.
    let mut group_key_visible_to_batch: HashMap<usize, usize> = HashMap::new();
    for (gk_idx, &visible_src) in layout.group_key_source_indexes.iter().enumerate() {
        group_key_visible_to_batch.insert(visible_src, group_key_batch_cols[gk_idx]);
    }

    // Derive all visible values per row.
    let mut all_visible_values: Vec<Vec<Option<AggScalarValue>>> =
        vec![Vec::with_capacity(num_rows); num_visible_cols];
    for row in 0..num_rows {
        let state_values: Vec<Option<AggScalarValue>> = all_state_values
            .iter()
            .map(|col| col[row].clone())
            .collect();
        let mut scratch = AggregatePhysicalRow {
            row_id: String::new(),
            visible_values: vec![None; num_visible_cols],
            state_values,
        };
        // Derive aggregate visible values from state (handles Single copy and AVG division).
        update_visible_values_from_state(&mut scratch, layout)?;
        // Override group key visible slots with direct batch values.
        for (&visible_src, &batch_col) in &group_key_visible_to_batch {
            scratch.visible_values[visible_src] =
                agg_scalar_from_array(batch.column(batch_col), row)?;
        }
        for (v_idx, val) in scratch.visible_values.into_iter().enumerate() {
            all_visible_values[v_idx].push(val);
        }
    }

    // Build the output physical batch: [row_id, visible_cols..., state_cols...].
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + num_visible_cols + num_state_cols);
    arrays.push(build_row_id_array(batch, &group_key_batch_cols)?);
    for (v_idx, visible_col) in layout.visible_columns.iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            &visible_col.data_type,
            std::mem::take(&mut all_visible_values[v_idx]),
        )?);
    }
    for (sc_idx, state_col) in layout.state_columns.iter().enumerate() {
        arrays.push(build_agg_scalar_array(
            &state_col.data_type,
            std::mem::take(&mut all_state_values[sc_idx]),
        )?);
    }

    let physical_batch = RecordBatch::try_new(Arc::new(physical_schema(layout)), arrays)
        .map_err(|e| format!("build aggregate MV physical batch failed: {e}"))?;
    record_batch_to_chunk(physical_batch)
}

/// Compute the batch column indexes for group keys and state columns in a state-shaped
/// executor result batch.
///
/// The state-shaped batch column order is determined by walking `shape.visible_outputs`:
/// - Each GroupKey output contributes one column.
/// - Each Single-role aggregate (COUNT, SUM, MIN, MAX) contributes one column.
/// - Each AVG aggregate contributes two columns (AvgSum at offset 0, AvgCount at offset 1).
///
/// Returns `(group_key_batch_cols, state_col_batch_cols)` where:
/// - `group_key_batch_cols[gk_idx]` = batch column index for group key `gk_idx`.
/// - `state_col_batch_cols[sc_idx]` = batch column index for state column `sc_idx`.
fn compute_batch_col_indexes(
    shape: &AggregateMvShape,
    layout: &AggregateMvLayout,
) -> (Vec<usize>, Vec<usize>) {
    let mut group_key_batch_col = vec![0usize; shape.group_keys.len()];
    let mut agg_batch_col_start = vec![0usize; shape.aggregates.len()];

    let mut batch_col = 0usize;
    for output in &shape.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(gk_idx) => {
                group_key_batch_col[*gk_idx] = batch_col;
                batch_col += 1;
            }
            VisibleAggregateOutput::Aggregate(agg_idx) => {
                agg_batch_col_start[*agg_idx] = batch_col;
                batch_col += if shape.aggregates[*agg_idx].function == AggregateFunctionKind::Avg {
                    2
                } else {
                    1
                };
            }
        }
    }

    let mut state_col_batch_col = vec![0usize; layout.state_columns.len()];
    let mut trailing_state_batch_col = batch_col;
    for (sc_idx, sc) in layout.state_columns.iter().enumerate() {
        state_col_batch_col[sc_idx] = match sc.state_role {
            AggregateStateRole::RetractionCount => {
                let col = trailing_state_batch_col;
                trailing_state_batch_col += 1;
                col
            }
            AggregateStateRole::Single | AggregateStateRole::AvgSum => {
                agg_batch_col_start[sc.aggregate_index]
            }
            AggregateStateRole::AvgCount => agg_batch_col_start[sc.aggregate_index] + 1,
        };
    }

    (group_key_batch_col, state_col_batch_col)
}

fn build_row_id_array(
    batch: &RecordBatch,
    group_key_batch_cols: &[usize],
) -> Result<ArrayRef, String> {
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(group_key_batch_cols.len());
        for &column_index in group_key_batch_cols {
            let array = batch.column(column_index);
            cells.push(hex_encode(&encoded_cell(array, row)?));
        }
        row_ids.push(cells.join("|"));
    }
    Ok(Arc::new(StringArray::from(row_ids)))
}

fn load_aggregate_physical_rows_from_batch(
    batch: &RecordBatch,
    layout: &AggregateMvLayout,
    out: &mut HashMap<String, AggregatePhysicalRow>,
    allow_negative_counts: bool,
) -> Result<(), String> {
    for row in load_aggregate_physical_rows_from_batch_owned(batch, layout, allow_negative_counts)?
    {
        let row_id = row.row_id.clone();
        if out.insert(row_id.clone(), row).is_some() {
            return Err(format!(
                "aggregate MV state corruption: duplicate row id `{row_id}`"
            ));
        }
    }
    Ok(())
}

fn load_aggregate_physical_rows_from_batch_owned(
    batch: &RecordBatch,
    layout: &AggregateMvLayout,
    allow_negative_counts: bool,
) -> Result<Vec<AggregatePhysicalRow>, String> {
    let expected_columns = 1 + layout.visible_columns.len() + layout.state_columns.len();
    if batch.num_columns() != expected_columns {
        return Err(format!(
            "aggregate MV physical column count mismatch: batch_columns={} expected={expected_columns}",
            batch.num_columns()
        ));
    }
    let row_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "aggregate MV physical row id column must be Utf8".to_string())?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if row_ids.is_null(row) {
            return Err("aggregate MV physical row id cannot be NULL".to_string());
        }
        let visible_values = (0..layout.visible_columns.len())
            .map(|idx| agg_scalar_from_array(batch.column(1 + idx), row))
            .collect::<Result<Vec<_>, _>>()?;
        let state_offset = 1 + layout.visible_columns.len();
        let state_values = (0..layout.state_columns.len())
            .map(|idx| agg_scalar_from_array(batch.column(state_offset + idx), row))
            .collect::<Result<Vec<_>, _>>()?;
        let row_id = row_ids.value(row).to_string();
        validate_loaded_physical_row(
            batch,
            row,
            &row_id,
            &visible_values,
            &state_values,
            layout,
            allow_negative_counts,
        )?;
        out.push(AggregatePhysicalRow {
            row_id,
            visible_values,
            state_values,
        });
    }
    Ok(out)
}

fn physical_rows_to_chunks(
    mut rows: Vec<AggregatePhysicalRow>,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    rows.sort_by(|left, right| left.row_id.cmp(&right.row_id));
    let mut arrays =
        Vec::with_capacity(1 + layout.visible_columns.len() + layout.state_columns.len());
    arrays.push(Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.row_id.as_str())
            .collect::<Vec<_>>(),
    )) as ArrayRef);
    for (column_index, visible_column) in layout.visible_columns.iter().enumerate() {
        let values = rows
            .iter()
            .map(|row| row.visible_values[column_index].clone())
            .collect::<Vec<_>>();
        arrays.push(build_agg_scalar_array(&visible_column.data_type, values)?);
    }
    for (column_index, state_column) in layout.state_columns.iter().enumerate() {
        let values = rows
            .iter()
            .map(|row| row.state_values[column_index].clone())
            .collect::<Vec<_>>();
        arrays.push(build_agg_scalar_array(&state_column.data_type, values)?);
    }
    let batch = RecordBatch::try_new(Arc::new(physical_schema(layout)), arrays)
        .map_err(|e| format!("build aggregate MV merged physical batch failed: {e}"))?;
    Ok(vec![record_batch_to_chunk(batch)?])
}

/// Negate every state-column value across the given chunks. Used by
/// the aggregate-IVM delete branch: post-aggregate, the SELECT over
/// deleted rows produces positive count/sum values; flipping them to
/// negatives lets the existing `merge_aggregate_state_batches` apply
/// `old + (-delta)` arithmetic without further reversibility logic.
///
/// Visible columns and the row-id column are unchanged. Only the
/// state columns get sign-flipped.
#[allow(dead_code)]
pub(crate) fn negate_aggregate_state_chunks(
    chunks: Vec<Chunk>,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    if layout.state_columns.is_empty() {
        return Ok(chunks);
    }
    let row_id_offset = 1;
    let visible_count = layout.visible_columns.len();
    let state_offset = row_id_offset + visible_count;
    let mut out = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let batch = chunk.batch.clone();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
        for (state_index, state_column) in layout.state_columns.iter().enumerate() {
            let column_index = state_offset + state_index;
            let original = arrays
                .get(column_index)
                .ok_or_else(|| {
                    format!(
                        "negate_aggregate_state_chunks: state column index {column_index} out of bounds; batch has {} columns",
                        arrays.len()
                    )
                })?
                .clone();
            arrays[column_index] = if matches!(
                state_column.function,
                AggregateFunctionKind::Min | AggregateFunctionKind::Max
            ) {
                // IVM-P5 Phase 4: MIN/MAX state is `Map<K, Int64>`. Negate
                // the per-entry counts and rebuild the MapArray, preserving
                // keys and per-row offsets.
                negate_map_state_array(&original, state_column)?
            } else {
                negate_state_array(&original, state_column)?
            };
        }
        let new_batch = RecordBatch::try_new(batch.schema(), arrays)
            .map_err(|e| format!("rebuild negated state chunk: {e}"))?;
        out.push(record_batch_to_chunk(new_batch)?);
    }
    Ok(out)
}

#[allow(dead_code)]
fn negate_state_array(
    array: &ArrayRef,
    state_column: &AggregateStateColumn,
) -> Result<ArrayRef, String> {
    use arrow::compute::kernels::numeric::neg;
    neg(array.as_ref()).map_err(|e| {
        format!(
            "negate state column `{}` ({:?}): {e}",
            state_column.name, state_column.data_type
        )
    })
}

/// Negate every per-entry count of a Map-typed state column. Keys are
/// preserved; per-row offsets and the row-level null mask are passed
/// through unchanged. Used by `negate_aggregate_state_chunks` for the
/// MIN/MAX detail-map state.
fn negate_map_state_array(
    array: &ArrayRef,
    state_column: &AggregateStateColumn,
) -> Result<ArrayRef, String> {
    let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        format!(
            "negate_map_state_array: state column `{}` is declared Map but array is {:?}",
            state_column.name,
            array.data_type()
        )
    })?;
    let original_entries = map_array.entries();
    if original_entries.num_columns() != 2 {
        return Err(format!(
            "negate_map_state_array: state column `{}` entries struct must have 2 fields, got {}",
            state_column.name,
            original_entries.num_columns()
        ));
    }
    let keys = original_entries.column(0).clone();
    let values_array = original_entries.column(1);
    let values_i64 = values_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            format!(
                "negate_map_state_array: state column `{}` map value must be Int64, got {:?}",
                state_column.name,
                values_array.data_type()
            )
        })?;
    let mut negated: Vec<Option<i64>> = Vec::with_capacity(values_i64.len());
    for row in 0..values_i64.len() {
        if values_i64.is_null(row) {
            negated.push(None);
        } else {
            let neg = values_i64.value(row).checked_neg().ok_or_else(|| {
                format!(
                    "negate_map_state_array: i64::MIN cannot be negated on column `{}`",
                    state_column.name
                )
            })?;
            negated.push(Some(neg));
        }
    }
    let new_values: ArrayRef = Arc::new(Int64Array::from(negated));

    let entries_fields = match original_entries.data_type() {
        DataType::Struct(fields) => fields.clone(),
        other => {
            return Err(format!(
                "negate_map_state_array: state column `{}` entries type must be Struct, got {other:?}",
                state_column.name
            ));
        }
    };
    let new_entries = StructArray::new(
        entries_fields,
        vec![keys, new_values],
        original_entries.nulls().cloned(),
    );

    let (map_field, ordered) = match &state_column.data_type {
        DataType::Map(field, ordered) => (field.clone(), *ordered),
        other => {
            return Err(format!(
                "negate_map_state_array: state column `{}` declared type must be Map, got {other:?}",
                state_column.name
            ));
        }
    };
    let new_map = MapArray::try_new(
        map_field,
        OffsetBuffer::new(map_array.value_offsets().to_vec().into()),
        new_entries,
        map_array.nulls().cloned(),
        ordered,
    )
    .map_err(|e| {
        format!(
            "negate_map_state_array: rebuild MapArray for column `{}` failed: {e}",
            state_column.name
        )
    })?;
    Ok(Arc::new(new_map))
}

fn zero_base_row(delta: &AggregatePhysicalRow, layout: &AggregateMvLayout) -> AggregatePhysicalRow {
    AggregatePhysicalRow {
        row_id: delta.row_id.clone(),
        visible_values: delta.visible_values.clone(),
        state_values: layout.state_columns.iter().map(zero_state_value).collect(),
    }
}

fn merge_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    match (state_column.function, state_column.state_role) {
        (AggregateFunctionKind::Count, AggregateStateRole::Single)
        | (AggregateFunctionKind::Count, AggregateStateRole::RetractionCount) => {
            merge_count_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Sum, AggregateStateRole::Single) => {
            merge_sum_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum) => {
            // Same arithmetic as SUM (NULL-permissive int/decimal addition).
            merge_sum_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => {
            // Same arithmetic as COUNT (NULL-rejecting int addition).
            merge_count_state_value(old, delta, state_column)
        }
        (AggregateFunctionKind::Min, AggregateStateRole::Single)
        | (AggregateFunctionKind::Max, AggregateStateRole::Single) => {
            // IVM-P5 (Phase 4): MIN/MAX state is a `Map<K, Int64>` value-count
            // detail map; merge is key-wise count addition followed by
            // zero-pruning (spec §3.5 — eager pruning at every merge step).
            merge_value_count_map_state(old, delta, state_column)
        }
        (function, role) => Err(format!(
            "internal: invalid (function, state_role) pair: ({function:?}, {role:?}) for column `{}`",
            state_column.name
        )),
    }
}

fn merge_count_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    match &state_column.data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let old = int64_state_value(old, &state_column.name)?;
            let delta = int64_state_value(delta, &state_column.name)?;
            let value = old.checked_add(delta).ok_or_else(|| {
                format!(
                    "aggregate MV state merge overflow for column `{}`",
                    state_column.name
                )
            })?;
            Ok(Some(AggScalarValue::Int64(value)))
        }
        other => Err(format!(
            "aggregate MV state merge does not support {:?} for column `{}`",
            other, state_column.name
        )),
    }
}

fn merge_sum_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    match &state_column.data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let old = nullable_int64_state_value(old, &state_column.name)?;
            let delta = nullable_int64_state_value(delta, &state_column.name)?;
            match (old, delta) {
                (Some(old), Some(delta)) => {
                    let value = old.checked_add(delta).ok_or_else(|| {
                        format!(
                            "aggregate MV state merge overflow for column `{}`",
                            state_column.name
                        )
                    })?;
                    Ok(Some(AggScalarValue::Int64(value)))
                }
                (Some(value), None) | (None, Some(value)) => Ok(Some(AggScalarValue::Int64(value))),
                (None, None) => Ok(None),
            }
        }
        DataType::Decimal128(_, _) => {
            let old = nullable_decimal128_state_value(old, &state_column.name)?;
            let delta = nullable_decimal128_state_value(delta, &state_column.name)?;
            match (old, delta) {
                (Some(old), Some(delta)) => {
                    let value = old.checked_add(delta).ok_or_else(|| {
                        format!(
                            "aggregate MV state merge overflow for column `{}`",
                            state_column.name
                        )
                    })?;
                    Ok(Some(AggScalarValue::Decimal128(value)))
                }
                (Some(value), None) | (None, Some(value)) => {
                    Ok(Some(AggScalarValue::Decimal128(value)))
                }
                (None, None) => Ok(None),
            }
        }
        other => Err(format!(
            "aggregate MV state merge does not support {:?} for column `{}`",
            other, state_column.name
        )),
    }
}

/// MIN vs MAX selector used by the detail-map derive-visible path.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MinMax {
    Min,
    Max,
}

// ---- IVM-P5 Phase 4: Map<K, Int64> detail-state helpers --------------------
//
// MIN/MAX state is a per-group `Map<K, Int64>` where K is the input scalar
// type and the Int64 value is the (possibly signed) row count of K in the
// group. Merge is key-wise addition; derive-visible is a MIN/MAX over keys
// with count > 0; negate flips the sign of every count; prune drops `count
// == 0` entries.
//
// In `AggScalarValue` (re-exported from `exec::expr::agg`), Map is encoded
// as `AggScalarValue::Map(Vec<(Option<key>, Option<value>)>)`. By contract
// for this state shape, both key and value are always `Some(...)` — NULL
// keys/values are never produced by `map_value_count` / `map_value_count_signed`
// (NULL inputs are skipped by the aggregate).

/// One `(key, value)` map entry as stored inside `AggScalarValue::Map`.
/// Keeps the helper signatures short and dodges clippy `type_complexity`.
type MapEntry = (Option<AggScalarValue>, Option<AggScalarValue>);

/// Merge two detail-map state values key-wise and prune zero entries.
///
/// Inputs may be `None` (e.g. zero state for a brand-new group, or an empty
/// delta). At least one of `old` / `delta` is expected to be `Some(Map)` in
/// practice; the `(None, None)` arm returns `Some(empty Map)` so the
/// resulting state column stays non-NULL (matches the layout declaration
/// `nullable = false`).
fn merge_value_count_map_state(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Option<AggScalarValue>, String> {
    let old_entries = take_map_entries(old, state_column)?;
    let delta_entries = take_map_entries(delta, state_column)?;
    let mut merged = old_entries;
    for (key, value) in delta_entries {
        accumulate_map_entry(&mut merged, key, value, state_column)?;
    }
    let pruned = prune_zero_entries_from_map_entries(merged);
    // Sort by key for stable / deterministic output (matches what
    // `map_value_count`'s `build_array` produces).
    let pruned = sort_map_entries_by_key(pruned, state_column)?;
    Ok(Some(AggScalarValue::Map(pruned)))
}

/// Extract the entries of a Map-typed state value. `None` is treated as an
/// empty map. Any other variant is a type-mismatch error.
fn take_map_entries(
    value: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<Vec<MapEntry>, String> {
    match value {
        None => Ok(Vec::new()),
        Some(AggScalarValue::Map(entries)) => Ok(entries),
        Some(other) => Err(format!(
            "MIN/MAX detail-map state type mismatch on column `{}`: expected Map, got {other:?}",
            state_column.name
        )),
    }
}

/// Accumulate one (key, count) pair into a Vec of map entries, summing
/// counts on key collisions. Linear scan — fine for the per-group map
/// sizes we expect (distinct values in a delta or merged group).
///
/// TODO: For high-cardinality groups, switch to a hash-based merge
/// (`key_fingerprint` → entry index). Current O(N*M) is intentional
/// simplicity for the expected per-group cardinality.
fn accumulate_map_entry(
    entries: &mut Vec<MapEntry>,
    key: Option<AggScalarValue>,
    value: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
) -> Result<(), String> {
    let new_count = match value {
        Some(AggScalarValue::Int64(v)) => v,
        // NULL count entries are not produced by our aggregates; treat
        // defensively as zero so they don't pollute the merged state.
        None => 0,
        Some(other) => {
            return Err(format!(
                "MIN/MAX detail-map state on column `{}` has non-Int64 value entry: {other:?}",
                state_column.name
            ));
        }
    };
    for (existing_key, existing_value) in entries.iter_mut() {
        if scalar_keys_equal(existing_key, &key) {
            let prev = match existing_value {
                Some(AggScalarValue::Int64(v)) => *v,
                None => 0,
                Some(other) => {
                    return Err(format!(
                        "MIN/MAX detail-map state on column `{}` has non-Int64 value entry: {other:?}",
                        state_column.name
                    ));
                }
            };
            let summed = prev.checked_add(new_count).ok_or_else(|| {
                format!(
                    "MIN/MAX detail-map state count overflow on column `{}`",
                    state_column.name
                )
            })?;
            *existing_value = Some(AggScalarValue::Int64(summed));
            return Ok(());
        }
    }
    entries.push((key, Some(AggScalarValue::Int64(new_count))));
    Ok(())
}

/// Stable equality for map keys. We compare via `compare_agg_scalar_values`
/// (the same ordering the executor uses for stable map output). `None` keys
/// are never produced by our aggregates, but we handle them defensively so
/// behaviour stays deterministic if a corrupted state ever appears.
///
/// **NaN handling** (IVM-P5 Float follow-up): IEEE 754 says `NaN != NaN`, so
/// `compare_agg_scalar_values` returns `Err` on NaN-vs-NaN, which would let
/// the same NaN value re-enter the detail map as a fresh key on every refresh.
/// We short-circuit two `Float64::NaN` values as equal (StarRocks-style, but
/// going further: where StarRocks lets the phmap silently duplicate, we
/// canonicalize so the detail map stays compact). Any specific NaN bit
/// pattern (signaling vs quiet, payload) is treated the same — `f64::is_nan`
/// is bit-pattern-agnostic.
fn scalar_keys_equal(left: &Option<AggScalarValue>, right: &Option<AggScalarValue>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(AggScalarValue::Float64(l)), Some(AggScalarValue::Float64(r)))
            if l.is_nan() && r.is_nan() =>
        {
            true
        }
        (Some(l), Some(r)) => matches!(
            compare_agg_scalar_values(l, r),
            Ok(std::cmp::Ordering::Equal)
        ),
        _ => false,
    }
}

/// Drop entries whose count is exactly zero. Negative counts are PRESERVED
/// (whether they survive into "final" state is a flow-level question handled
/// upstream — see spec §3.5 and Phase-4 task description).
fn prune_zero_entries_from_map_entries(entries: Vec<MapEntry>) -> Vec<MapEntry> {
    entries
        .into_iter()
        .filter(|(_, v)| !matches!(v, Some(AggScalarValue::Int64(0))))
        .collect()
}

/// Sort map entries by key using the same ordering the aggregate executor
/// emits. Keeps the merged state deterministic across runs and makes
/// equality assertions in unit tests stable.
///
/// **NaN handling** (IVM-P5 Float follow-up): IEEE 754 makes `NaN` unordered
/// w.r.t. every other value (including itself), so `compare_agg_scalar_values`
/// returns `Err` on any NaN comparison. We treat NaN as the maximum element
/// (sorts to the end of the run) and equal to other NaNs. This keeps Float
/// MIN/MAX detail-state usable: `derive_visible_from_detail_map` skips NaN
/// keys when reducing min/max (NaN never participates in the result), and the
/// stable position lets `merge_value_count_map_state` produce a deterministic
/// output across refreshes. Bit-pattern-agnostic via `f64::is_nan`.
fn sort_map_entries_by_key(
    mut entries: Vec<MapEntry>,
    state_column: &AggregateStateColumn,
) -> Result<Vec<MapEntry>, String> {
    let mut had_error: Option<String> = None;
    entries.sort_by(|a, b| match (&a.0, &b.0) {
        (Some(l), Some(r)) => {
            // NaN-aware short-circuit before reaching the strict comparator.
            if let (AggScalarValue::Float64(lx), AggScalarValue::Float64(rx)) = (l, r) {
                match (lx.is_nan(), rx.is_nan()) {
                    (true, true) => return std::cmp::Ordering::Equal,
                    (true, false) => return std::cmp::Ordering::Greater,
                    (false, true) => return std::cmp::Ordering::Less,
                    (false, false) => {} // both finite: fall through to strict comparator
                }
            }
            match compare_agg_scalar_values(l, r) {
                Ok(ord) => ord,
                Err(_) => {
                    had_error.get_or_insert_with(|| {
                        format!(
                            "MIN/MAX detail-map state on column `{}` has incomparable keys: {l:?} vs {r:?}",
                            state_column.name
                        )
                    });
                    std::cmp::Ordering::Equal
                }
            }
        }
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    });
    if let Some(err) = had_error {
        return Err(err);
    }
    Ok(entries)
}

/// Derive the visible MIN/MAX value for a group from its detail-map state.
///
/// Iterates entries, skips those with `count <= 0` (the entry is fully
/// retracted or pending retraction), and reduces over the remaining keys.
/// Returns `None` when no entry has a positive count — the group is
/// effectively empty for visible purposes (the existing
/// `__ivm_row_count == 0` retraction logic will drop the row).
pub(crate) fn derive_visible_from_detail_map(
    value: &AggScalarValue,
    op: MinMax,
) -> Result<Option<AggScalarValue>, String> {
    let AggScalarValue::Map(entries) = value else {
        return Err(format!(
            "derive_visible_from_detail_map: expected Map, got {value:?}"
        ));
    };
    let mut current: Option<AggScalarValue> = None;
    for (key, count) in entries {
        let count_i64 = match count {
            Some(AggScalarValue::Int64(v)) => *v,
            None => 0,
            Some(other) => {
                return Err(format!(
                    "derive_visible_from_detail_map: expected Int64 count, got {other:?}"
                ));
            }
        };
        if count_i64 <= 0 {
            continue;
        }
        let Some(key) = key.clone() else {
            // NULL key entries are not produced by our aggregates; skip
            // defensively.
            continue;
        };
        // IVM-P5 Float follow-up: NaN handling is centralized in
        // `pick_min_max_scalar` via `f64::total_cmp` (IEEE 754 total order:
        // +NaN is the maximum, finite values sort below). This mirrors
        // NovaRocks's plain MIN/MAX accumulator (`update_max_float` /
        // `update_min_float` in `src/exec/expr/agg/functions/{max,min}.rs`)
        // which also uses `v.total_cmp(&state.value)`. Net effect:
        //   - MIN over (NaN, finite, ...) → finite minimum (NaN > finite in
        //     total_cmp, never picked as MIN)
        //   - MAX over (NaN, finite, ...) → NaN (NaN > finite, picked as MAX)
        //   - MIN/MAX over NaN-only group → NaN (the single entry)
        current = Some(match current {
            None => key,
            Some(c) => pick_min_max_scalar(c, key, op)?,
        });
    }
    Ok(current)
}

/// Pick min or max of two scalar values using the executor's stable
/// comparator.
///
/// **Float NaN handling**: For Float64-vs-Float64 we use `f64::total_cmp`
/// (IEEE 754 total order) instead of `compare_agg_scalar_values` (which
/// returns `Err` on any NaN comparison). Total order places +NaN at the
/// extreme so:
///   - MIN(NaN, finite) → finite minimum (finite < +NaN in total order;
///     NaN never picked as MIN unless every entry is NaN)
///   - MAX(NaN, finite) → NaN (NaN > finite in total order)
///   - MIN(NaN-only) / MAX(NaN-only) → NaN (the single entry)
///
/// This matches NovaRocks's plain MIN/MAX accumulator (`update_max_float` /
/// `update_min_float` in `src/exec/expr/agg/functions/{max,min}.rs`, both
/// using `v.total_cmp(&state.value)`), so the MV always mirrors plain
/// GROUP BY semantics on Float columns.
///
/// For non-Float types, `compare_agg_scalar_values` is used as before;
/// returns `Err` if the two values are incomparable (different variants) —
/// should not happen for well-formed state since `map_value_count` keys
/// come from one typed input column.
fn pick_min_max_scalar(
    a: AggScalarValue,
    b: AggScalarValue,
    op: MinMax,
) -> Result<AggScalarValue, String> {
    let ordering = if let (AggScalarValue::Float64(av), AggScalarValue::Float64(bv)) = (&a, &b) {
        av.total_cmp(bv)
    } else {
        compare_agg_scalar_values(&a, &b).map_err(|e| {
            format!("derive_visible_from_detail_map: incomparable map keys: {a:?} vs {b:?}: {e}")
        })?
    };
    let pick_a = match op {
        MinMax::Min => {
            ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
        }
        MinMax::Max => {
            ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
        }
    };
    Ok(if pick_a { a } else { b })
}

fn int64_state_value(value: Option<AggScalarValue>, state_name: &str) -> Result<i64, String> {
    match value {
        Some(AggScalarValue::Int64(v)) => Ok(v),
        None => Err(format!(
            "aggregate MV state corruption: COUNT state column `{state_name}` is NULL"
        )),
        other => Err(format!(
            "aggregate MV state type mismatch for column `{state_name}`: expected integer, got {other:?}"
        )),
    }
}

fn nullable_int64_state_value(
    value: Option<AggScalarValue>,
    state_name: &str,
) -> Result<Option<i64>, String> {
    match value {
        Some(AggScalarValue::Int64(v)) => Ok(Some(v)),
        None => Ok(None),
        other => Err(format!(
            "aggregate MV state type mismatch for column `{state_name}`: expected integer, got {other:?}"
        )),
    }
}

fn nullable_decimal128_state_value(
    value: Option<AggScalarValue>,
    state_name: &str,
) -> Result<Option<i128>, String> {
    match value {
        Some(AggScalarValue::Decimal128(v)) => Ok(Some(v)),
        None => Ok(None),
        other => Err(format!(
            "aggregate MV state type mismatch for column `{state_name}`: expected Decimal128, got {other:?}"
        )),
    }
}

fn zero_state_value(state_column: &AggregateStateColumn) -> Option<AggScalarValue> {
    match (state_column.function, state_column.state_role) {
        (AggregateFunctionKind::Count, AggregateStateRole::Single)
        | (AggregateFunctionKind::Count, AggregateStateRole::RetractionCount) => {
            Some(AggScalarValue::Int64(0))
        }
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => {
            Some(AggScalarValue::Int64(0))
        }
        (AggregateFunctionKind::Sum, AggregateStateRole::Single)
        | (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum) => None,
        // MIN/MAX detail-state is a `Map<K, Int64>` value-count map; the
        // "zero" for a brand-new group is None here. `merge_value_count_map_state`
        // treats a `None` old as an empty map and folds the delta entries in,
        // so the first incoming delta populates the map directly. Note the
        // asymmetry with SUM (also None) is intentional: SUM uses scalar
        // None as the additive identity, while MIN/MAX None means "empty
        // detail map", and the merge helper distinguishes them by state shape.
        (AggregateFunctionKind::Min, _) | (AggregateFunctionKind::Max, _) => None,
        // Catch-all for unexpected combinations.
        (function, role) => {
            // This should never happen with well-formed layouts.
            tracing::warn!(
                "zero_state_value: unexpected (function, state_role) pair ({function:?}, {role:?})"
            );
            None
        }
    }
}

fn validate_loaded_physical_row(
    batch: &RecordBatch,
    row: usize,
    row_id: &str,
    visible_values: &[Option<AggScalarValue>],
    state_values: &[Option<AggScalarValue>],
    layout: &AggregateMvLayout,
    allow_negative_counts: bool,
) -> Result<(), String> {
    if visible_values.len() != layout.visible_columns.len() {
        return Err(format!(
            "aggregate MV state corruption for row id `{row_id}`: visible column count mismatch"
        ));
    }
    if state_values.len() != layout.state_columns.len() {
        return Err(format!(
            "aggregate MV state corruption for row id `{row_id}`: state column count mismatch"
        ));
    }
    let computed_row_id = physical_row_id_from_visible_group_keys(batch, row, layout)?;
    if computed_row_id != row_id {
        return Err(format!(
            "aggregate MV state corruption: stored row id `{row_id}` does not match visible group key row id `{computed_row_id}`"
        ));
    }

    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        let state_value = &state_values[state_index];
        let is_count_role = matches!(
            (state_column.function, state_column.state_role),
            (AggregateFunctionKind::Count, AggregateStateRole::Single)
                | (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount)
                | (
                    AggregateFunctionKind::Count,
                    AggregateStateRole::RetractionCount
                )
        );
        if is_count_role {
            validate_loaded_count_state(
                state_value,
                &state_column.name,
                row_id,
                state_column.count_star,
                allow_negative_counts,
            )?;
        }
        // Skip visible/state equality for non-Single states (e.g. AVG AvgSum/AvgCount
        // state values differ from the visible AVG output).
        // In delta-mode the visible column carries pre-negation values while the state
        // column has been sign-flipped, so equality is expected to fail.
        // The merge math reads only state_values, so mismatches are harmless.
        // We keep the check for strict-mode Single-role states only.
        //
        // IVM-P5 Phase 4: MIN/MAX-Single state is a `Map<K, Int64>` while the
        // visible column is scalar K. They never compare equal, so skip the
        // check for MIN/MAX too — correctness for those is enforced by the
        // post-merge derive-visible step in `update_visible_values_from_state`.
        let is_min_max_single = matches!(
            (state_column.function, state_column.state_role),
            (
                AggregateFunctionKind::Min | AggregateFunctionKind::Max,
                AggregateStateRole::Single
            )
        );
        if !allow_negative_counts
            && matches!(state_column.state_role, AggregateStateRole::Single)
            && !is_min_max_single
        {
            let visible_value =
                visible_values
                    .get(state_column.visible_source_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state corruption: visible source index {} is out of range for state column `{}`",
                            state_column.visible_source_index, state_column.name
                        )
                    })?;
            if !agg_scalar_values_equal(visible_value, state_value) {
                return Err(format!(
                    "aggregate MV state corruption: visible aggregate column `{}` does not match state column `{}` for row id `{row_id}`",
                    layout.visible_columns[state_column.visible_source_index].name,
                    state_column.name
                ));
            }
        }
    }
    Ok(())
}

fn physical_row_id_from_visible_group_keys(
    batch: &RecordBatch,
    row: usize,
    layout: &AggregateMvLayout,
) -> Result<String, String> {
    let mut cells = Vec::with_capacity(layout.group_key_source_indexes.len());
    for &source_index in &layout.group_key_source_indexes {
        let column_index = 1 + source_index;
        let array = batch.column(column_index);
        cells.push(hex_encode(&encoded_cell(array, row)?));
    }
    Ok(cells.join("|"))
}

fn validate_loaded_count_state(
    state_value: &Option<AggScalarValue>,
    state_name: &str,
    row_id: &str,
    count_star: bool,
    allow_negative_counts: bool,
) -> Result<(), String> {
    match state_value {
        // Permissive delta-mode: any non-NULL Int64 (including
        // negatives produced by `negate_aggregate_state_chunks`) is
        // acceptable. We still reject NULLs and non-Int64 types.
        Some(AggScalarValue::Int64(_)) if allow_negative_counts => Ok(()),
        Some(AggScalarValue::Int64(v)) if *v > 0 => Ok(()),
        Some(AggScalarValue::Int64(0)) if !count_star => Ok(()),
        Some(AggScalarValue::Int64(v)) if !count_star => Err(format!(
            "aggregate MV state corruption: COUNT state column `{state_name}` must be non-negative for row id `{row_id}`, got {v}"
        )),
        Some(AggScalarValue::Int64(v)) => Err(format!(
            "aggregate MV state corruption: COUNT state column `{state_name}` must be positive for row id `{row_id}`, got {v}"
        )),
        None => Err(format!(
            "aggregate MV state corruption: COUNT state column `{state_name}` is NULL for row id `{row_id}`"
        )),
        other => Err(format!(
            "aggregate MV state corruption: COUNT state column `{state_name}` has invalid value {other:?} for row id `{row_id}`"
        )),
    }
}

fn agg_scalar_values_equal(left: &Option<AggScalarValue>, right: &Option<AggScalarValue>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(AggScalarValue::Bool(left)), Some(AggScalarValue::Bool(right))) => left == right,
        (Some(AggScalarValue::Int64(left)), Some(AggScalarValue::Int64(right))) => left == right,
        // Float64: use bit equality so that NaN == NaN (consistent with merge's NaN preservation).
        (Some(AggScalarValue::Float64(left)), Some(AggScalarValue::Float64(right))) => {
            left.to_bits() == right.to_bits()
        }
        (Some(AggScalarValue::Utf8(left)), Some(AggScalarValue::Utf8(right))) => left == right,
        (Some(AggScalarValue::Date32(left)), Some(AggScalarValue::Date32(right))) => left == right,
        (Some(AggScalarValue::Timestamp(left)), Some(AggScalarValue::Timestamp(right))) => {
            left == right
        }
        (Some(AggScalarValue::Decimal128(left)), Some(AggScalarValue::Decimal128(right))) => {
            left == right
        }
        (Some(AggScalarValue::Decimal256(left)), Some(AggScalarValue::Decimal256(right))) => {
            left == right
        }
        _ => false,
    }
}

/// Map the visible (output) DataType of an AVG aggregate to the (sum_data_type, sum_sql_type)
/// pair used for the AvgSum state column.
///
/// The sum state is declared at the **visible** scale so that `derive_avg_visible` can produce
/// the correct visible-scale result directly from integer division. The materialize step is
/// responsible for rescaling the SUM executor's output (which arrives at input/SUM scale)
/// up to the visible scale when storing into the state column.
///
/// Layout sees only the AVG visible type:
/// - AVG over integer inputs produces visible Float64 and uses an Int64 sum state.
/// - AVG over Decimal128 inputs produces visible Decimal128 and uses a Decimal128 sum state
///   at the analyzer-promoted visible scale.
///
/// AVG over Float32/Float64 is rejected in the DDL analyzer validation path, where the
/// input type is still available. Do not reject Float64 here, because that is also the
/// visible type for supported integer AVG.
///
/// Returns `None` for unsupported visible types.
fn avg_sum_state_type(visible_dt: &DataType) -> Option<(DataType, SqlType)> {
    match visible_dt {
        // Integer inputs produce Float64 visible output; sum state is Int64.
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some((DataType::Int64, SqlType::BigInt))
        }
        DataType::Float64 => Some((DataType::Int64, SqlType::BigInt)),
        DataType::Decimal128(_, visible_scale) => {
            // Store sum state at the visible (promoted) scale. The materialize step will
            // rescale the SUM executor output (at input scale) to this scale on write.
            Some((
                DataType::Decimal128(38, *visible_scale),
                SqlType::Decimal {
                    precision: 38,
                    scale: *visible_scale,
                },
            ))
        }
        _ => None,
    }
}

fn validate_state_column_type(
    function: AggregateFunctionKind,
    state_role: AggregateStateRole,
    data_type: &DataType,
    state_name: &str,
) -> Result<(), String> {
    match (function, state_role) {
        (AggregateFunctionKind::Count, AggregateStateRole::Single)
        | (AggregateFunctionKind::Count, AggregateStateRole::RetractionCount) => match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(()),
            other => Err(format!(
                "aggregate MV COUNT state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Sum, AggregateStateRole::Single) => match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _) => Ok(()),
            other => Err(format!(
                "aggregate MV SUM state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum) => match data_type {
            DataType::Int64 | DataType::Decimal128(_, _) => Ok(()),
            other => Err(format!(
                "AVG sum state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount) => match data_type {
            DataType::Int64 => Ok(()),
            other => Err(format!(
                "AVG count state must be Int64 for column `{state_name}`: {other:?}"
            )),
        },
        (AggregateFunctionKind::Min, AggregateStateRole::Single)
        | (AggregateFunctionKind::Max, AggregateStateRole::Single) => {
            // IVM-P5 (Phase 2): MIN/MAX state is a `Map<input_type, Int64>`
            // value-count detail map. The Arrow shape follows the iceberg-rust
            // convention so the Iceberg sink can re-annotate batch field IDs
            // without a structural mismatch:
            //   entries-struct field name = "key_value"
            //   value field nullable = true
            //   key field non-null
            // Reject any other Arrow shape with a clear error so future schema
            // bugs are loud, not silent.
            let DataType::Map(entries_field, _keys_sorted) = data_type else {
                return Err(format!(
                    "MIN/MAX state type for column `{state_name}` must be Map<K, Int64>, got {data_type:?}"
                ));
            };
            if entries_field.name() != "key_value" {
                return Err(format!(
                    "MIN/MAX state map entries field for column `{state_name}` must be named `key_value` (iceberg-rust convention), got `{}`",
                    entries_field.name()
                ));
            }
            let DataType::Struct(struct_fields) = entries_field.data_type() else {
                return Err(format!(
                    "MIN/MAX state map entries type for column `{state_name}` must be Struct, got {:?}",
                    entries_field.data_type()
                ));
            };
            if struct_fields.len() != 2 {
                return Err(format!(
                    "MIN/MAX state map entries struct for column `{state_name}` must have exactly 2 fields, got {}",
                    struct_fields.len()
                ));
            }
            let value_field = struct_fields
                .iter()
                .find(|f| f.name() == "value")
                .ok_or_else(|| {
                    format!(
                        "MIN/MAX state map entries struct for column `{state_name}` is missing `value` field"
                    )
                })?;
            if value_field.data_type() != &DataType::Int64 {
                return Err(format!(
                    "MIN/MAX state map value type for column `{state_name}` must be Int64, got {:?}",
                    value_field.data_type()
                ));
            }
            if !value_field.is_nullable() {
                return Err(format!(
                    "MIN/MAX state map value field for column `{state_name}` must be nullable (iceberg-rust convention)"
                ));
            }
            // Validate the key Arrow type matches the scalar primitives we
            // accept as MIN/MAX inputs (mirrors the pre-Phase-2 acceptance
            // list, minus Boolean which AggScalarValue does not support).
            let key_field = struct_fields
                .iter()
                .find(|f| f.name() == "key")
                .ok_or_else(|| {
                    format!(
                        "MIN/MAX state map entries struct for column `{state_name}` is missing `key` field"
                    )
                })?;
            if key_field.is_nullable() {
                return Err(format!(
                    "MIN/MAX state map key field for column `{state_name}` must be non-null"
                ));
            }
            match key_field.data_type() {
                // IVM-P5 Float follow-up: Float MIN/MAX now supported.
                // NaN is handled in three sites:
                //   1. `scalar_keys_equal` — bit-pattern-agnostic NaN==NaN so
                //      detail map aggregates duplicate NaNs into one entry.
                //   2. `sort_map_entries_by_key` — NaN sorts to the end of
                //      the run (Ordering::Greater) instead of erroring out.
                //   3. `derive_visible_from_detail_map` — skips NaN keys
                //      when reducing MIN/MAX (NaN is ignored, like NULL).
                // Float32 is widened to Float64 at AggScalarValue level
                // (`scalar_from_array`), so handling Float64 covers both.
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(_, _) => Ok(()),
                DataType::Boolean => Err(format!(
                    "MIN/MAX state key type is unsupported for column `{state_name}`: Boolean"
                )),
                other => Err(format!(
                    "MIN/MAX state key type is unsupported for column `{state_name}`: {other:?}"
                )),
            }
        }
        (function, role) => Err(format!(
            "internal: invalid (function, state_role) pair: ({function:?}, {role:?}) for column `{state_name}`"
        )),
    }
}

fn aggregate_visible_source_index(
    shape: &AggregateMvShape,
    aggregate_index: usize,
) -> Result<usize, String> {
    shape
        .visible_outputs
        .iter()
        .position(|output| matches!(output, VisibleAggregateOutput::Aggregate(idx) if *idx == aggregate_index))
        .ok_or_else(|| {
            format!(
                "aggregate MV aggregate output is not visible: aggregate_index={aggregate_index}"
            )
        })
}

fn group_key_source_indexes(shape: &AggregateMvShape) -> Result<Vec<usize>, String> {
    let mut source_indexes_by_group_key = vec![None; shape.group_keys.len()];
    for (source_index, output) in shape.visible_outputs.iter().enumerate() {
        let VisibleAggregateOutput::GroupKey(group_key_index) = output else {
            continue;
        };
        let slot = source_indexes_by_group_key
            .get_mut(*group_key_index)
            .ok_or_else(|| {
                format!(
                    "aggregate MV group key output index out of range: group_key_index={} group_keys={}",
                    group_key_index,
                    shape.group_keys.len()
                )
            })?;
        if slot.replace(source_index).is_some() {
            return Err(format!(
                "aggregate MV group key output is duplicated: group_key_index={group_key_index}"
            ));
        }
    }
    source_indexes_by_group_key
        .into_iter()
        .enumerate()
        .map(|(group_key_index, source_index)| {
            source_index.ok_or_else(|| {
                format!(
                    "aggregate MV group key output is missing: group_key_index={group_key_index}"
                )
            })
        })
        .collect()
}

fn physical_schema(layout: &AggregateMvLayout) -> Schema {
    let mut fields =
        Vec::with_capacity(1 + layout.visible_columns.len() + layout.state_columns.len());
    fields.push(Field::new(ROW_ID_COLUMN, DataType::Utf8, false));
    fields.extend(
        layout
            .visible_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable)),
    );
    fields.extend(
        layout
            .state_columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable)),
    );
    Schema::new(fields)
}

pub(crate) fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

fn encoded_cell(array: &ArrayRef, row: usize) -> Result<Vec<u8>, String> {
    match array.data_type() {
        DataType::Boolean => encode_typed_cell::<BooleanArray, _>(array, row, "boolean", |arr| {
            vec![u8::from(arr.value(row))]
        }),
        DataType::Int8 => encode_typed_cell::<Int8Array, _>(array, row, "int8", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int16 => encode_typed_cell::<Int16Array, _>(array, row, "int16", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int32 => encode_typed_cell::<Int32Array, _>(array, row, "int32", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Date32 => encode_typed_cell::<Date32Array, _>(array, row, "date32", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Int64 => encode_typed_cell::<Int64Array, _>(array, row, "int64", |arr| {
            arr.value(row).to_le_bytes().to_vec()
        }),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            encode_typed_cell::<TimestampMicrosecondArray, _>(
                array,
                row,
                "timestamp_microsecond",
                |arr| arr.value(row).to_le_bytes().to_vec(),
            )
        }
        DataType::Utf8 => encode_typed_cell::<StringArray, _>(array, row, "utf8", |arr| {
            arr.value(row).as_bytes().to_vec()
        }),
        DataType::Decimal128(precision, scale) => {
            let type_name = format!("decimal128({precision},{scale})");
            encode_typed_cell::<Decimal128Array, _>(array, row, &type_name, |arr| {
                arr.value(row).to_le_bytes().to_vec()
            })
        }
        other => Err(format!(
            "aggregate MV row id does not support group key type {:?}",
            other
        )),
    }
}

fn encode_typed_cell<A, F>(
    array: &ArrayRef,
    row: usize,
    type_name: &str,
    value_bytes: F,
) -> Result<Vec<u8>, String>
where
    A: Array + 'static,
    F: FnOnce(&A) -> Vec<u8>,
{
    let typed = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| format!("aggregate MV row id downcast failed for {type_name}"))?;
    let mut out = Vec::new();
    out.extend_from_slice(type_name.as_bytes());
    out.push(b':');
    if typed.is_null(row) {
        out.extend_from_slice(b"N");
    } else {
        out.extend_from_slice(b"V:");
        out.extend_from_slice(&value_bytes(typed));
    }
    Ok(out)
}

/// Derive visible column values from the current state values after a merge step.
///
/// - For COUNT/SUM-Single the visible is a direct copy of the state (1:1 mapping).
/// - For MIN/MAX-Single the visible is derived from the detail-map state via
///   min/max over keys with count > 0.
/// - For AVG the visible is computed as AvgSum / AvgCount.
fn update_visible_values_from_state(
    row: &mut AggregatePhysicalRow,
    layout: &AggregateMvLayout,
) -> Result<(), String> {
    use std::collections::HashMap;
    // Group state column indexes by aggregate_index.
    let mut by_aggregate: HashMap<usize, Vec<usize>> = HashMap::new();
    for (state_index, state_column) in layout.state_columns.iter().enumerate() {
        if state_column.state_role == AggregateStateRole::RetractionCount {
            continue;
        }
        by_aggregate
            .entry(state_column.aggregate_index)
            .or_default()
            .push(state_index);
    }

    for state_indexes in by_aggregate.values() {
        let primary = &layout.state_columns[state_indexes[0]];
        match primary.function {
            AggregateFunctionKind::Count | AggregateFunctionKind::Sum => {
                // Single state role: visible = state.
                let state_index = state_indexes[0];
                let state_column = &layout.state_columns[state_index];
                row.visible_values[state_column.visible_source_index] =
                    row.state_values[state_index].clone();
            }
            AggregateFunctionKind::Min | AggregateFunctionKind::Max => {
                // IVM-P5 Phase 4: visible MIN/MAX is derived from the
                // detail-map state by scanning entries with count > 0
                // and reducing to the min/max key.
                //
                // On the materialize/insert path, this is the sole
                // derivation of visible for MIN/MAX (the rewriter no
                // longer projects a scalar MIN/MAX value after Phase 3,
                // so the visible slot starts as None and is populated
                // here). On the merge path, this overwrites the prior
                // physical visible value with the post-merge derived
                // value, which is correct because the detail-map state
                // reflects every retraction and re-insertion.
                let state_index = state_indexes[0];
                let state_column = &layout.state_columns[state_index];
                let op = match primary.function {
                    AggregateFunctionKind::Min => MinMax::Min,
                    AggregateFunctionKind::Max => MinMax::Max,
                    _ => unreachable!(),
                };
                let derived = match row.state_values[state_index].as_ref() {
                    Some(value @ AggScalarValue::Map(_)) => {
                        derive_visible_from_detail_map(value, op).map_err(|e| {
                            format!(
                                "derive visible for column `{}` failed: {e}",
                                state_column.name
                            )
                        })?
                    }
                    Some(other) => {
                        return Err(format!(
                            "MIN/MAX state on column `{}` must be Map, got {other:?}",
                            state_column.name
                        ));
                    }
                    None => None,
                };
                row.visible_values[state_column.visible_source_index] = derived;
            }
            AggregateFunctionKind::Avg => {
                let (sum_idx, count_idx) = avg_state_indexes(layout, state_indexes)?;
                let visible_idx = layout.state_columns[sum_idx].visible_source_index;
                let visible_dt = &layout.visible_columns[visible_idx].data_type;
                let sum_val = row.state_values[sum_idx].clone();
                let count_val = row.state_values[count_idx].clone();
                row.visible_values[visible_idx] =
                    derive_avg_visible(sum_val, count_val, visible_dt)?;
            }
        }
    }
    Ok(())
}

/// Locate the AvgSum and AvgCount state indexes within a set of state indexes
/// that all belong to the same AVG aggregate.
fn avg_state_indexes(
    layout: &AggregateMvLayout,
    state_indexes: &[usize],
) -> Result<(usize, usize), String> {
    let mut sum_idx = None;
    let mut count_idx = None;
    for &i in state_indexes {
        match layout.state_columns[i].state_role {
            AggregateStateRole::AvgSum => sum_idx = Some(i),
            AggregateStateRole::AvgCount => count_idx = Some(i),
            AggregateStateRole::Single => {
                return Err(format!(
                    "internal: AVG aggregate has Single state_role on state column index {i}"
                ));
            }
            AggregateStateRole::RetractionCount => {
                return Err(format!(
                    "internal: AVG aggregate has RetractionCount state_role on state column index {i}"
                ));
            }
        }
    }
    Ok((
        sum_idx.ok_or("internal: AVG aggregate missing AvgSum state column")?,
        count_idx.ok_or("internal: AVG aggregate missing AvgCount state column")?,
    ))
}

/// Compute the AVG visible value from sum and count sub-states.
///
/// NULL semantics:
/// - count = 0  → NULL (empty group)
/// - sum = NULL → NULL (all inputs were NULL)
/// - otherwise  → sum / count
///
/// For Decimal128 inputs: division is integer division at the stored scale.
/// This is exact for the stored scale's precision but may lose fractional
/// digits below the scale due to truncation. Production-grade Decimal AVG
/// with extended-precision intermediate scaling is deferred.
fn derive_avg_visible(
    sum: Option<AggScalarValue>,
    count: Option<AggScalarValue>,
    visible_dt: &DataType,
) -> Result<Option<AggScalarValue>, String> {
    let count_i64 = match count {
        Some(AggScalarValue::Int64(c)) => c,
        Some(other) => {
            return Err(format!("AVG count state must be Int64, got {other:?}"));
        }
        None => return Err("AVG count state must not be NULL".to_string()),
    };
    if count_i64 == 0 {
        return Ok(None);
    }
    let sum = match sum {
        Some(v) => v,
        None => return Ok(None),
    };
    match (visible_dt, sum) {
        (DataType::Float64, AggScalarValue::Int64(s)) => Ok(Some(AggScalarValue::Float64(
            (s as f64) / (count_i64 as f64),
        ))),
        (DataType::Decimal128(_p, _scale), AggScalarValue::Decimal128(s)) => {
            // Stored sum = real_sum * 10^scale; count is dimensionless.
            // real_avg = real_sum / count = (stored_sum / 10^scale) / count
            // stored_avg = real_avg * 10^scale = stored_sum / count
            // Integer division truncates — acceptable as Phase-1 approximation.
            let result = s
                .checked_div(count_i64 as i128)
                .ok_or("AVG decimal divide failed (overflow)")?;
            Ok(Some(AggScalarValue::Decimal128(result)))
        }
        (dt, sum) => Err(format!(
            "AVG visible derivation unsupported: visible_dt={dt:?} sum={sum:?}"
        )),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::mv_shape::{
        IncrementalMvShape, classify_incremental_mv_query,
    };
    use crate::sql::column_id::ColumnId;
    use arrow::array::{Array, Int64Array, StringArray};

    fn test_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, count(*) as c, sum(v1) as s from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn aggregate_first_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select count(*) as c, k1 from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn count_expr_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, count(v1) as c from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn sum_only_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, sum(v1) as s from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ]
    }

    fn aggregate_first_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ]
    }

    fn count_expr_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ]
    }

    fn sum_only_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ]
    }

    fn visible_result_batch(k1: Vec<i64>, c: Vec<i64>, s: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(c)),
                Arc::new(Int64Array::from(s)),
            ],
        )
        .expect("batch")
    }

    fn visible_result_batch_nullable_sum(
        k1: Vec<i64>,
        c: Vec<i64>,
        s: Vec<Option<i64>>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(c)),
                Arc::new(Int64Array::from(s)),
            ],
        )
        .expect("batch")
    }

    fn sum_only_state_result_batch(k1: Vec<i64>, s: Vec<i64>, row_count: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
                Field::new("__agg_state___ivm_row_count", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(s)),
                Arc::new(Int64Array::from(row_count)),
            ],
        )
        .expect("batch")
    }

    fn aggregate_first_result_batch(c: Vec<i64>, k1: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("c", DataType::Int64, false),
                Field::new("k1", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(c)),
                Arc::new(Int64Array::from(k1)),
            ],
        )
        .expect("batch")
    }

    fn count_expr_result_batch(k1: Vec<i64>, c: Vec<i64>) -> RecordBatch {
        let rows = k1.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state___ivm_row_count", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(c)),
                Arc::new(Int64Array::from(vec![1_i64; rows])),
            ],
        )
        .expect("batch")
    }

    fn physical_chunks_with_count_state(
        layout: &AggregateMvLayout,
        shape: &AggregateMvShape,
        count_state: Option<i64>,
    ) -> Vec<Chunk> {
        let mut chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("chunk"),
                ],
            },
            layout,
            shape,
        )
        .expect("physical");
        let batch = &chunks[0].batch;
        let mut columns = batch.columns().to_vec();
        columns[4] = Arc::new(Int64Array::from(vec![count_state]));
        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| Field::new(field.name(), field.data_type().clone(), idx == 4))
            .collect::<Vec<_>>();
        let corrupted =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("corrupted batch");
        chunks[0] = record_batch_to_chunk(corrupted).expect("corrupted chunk");
        chunks
    }

    fn physical_chunks_with_bad_row_id(
        layout: &AggregateMvLayout,
        shape: &AggregateMvShape,
    ) -> Vec<Chunk> {
        let mut chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("chunk"),
                ],
            },
            layout,
            shape,
        )
        .expect("physical");
        let batch = &chunks[0].batch;
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(StringArray::from(vec!["bad-row-id"]));
        let corrupted = RecordBatch::try_new(batch.schema(), columns).expect("corrupted batch");
        chunks[0] = record_batch_to_chunk(corrupted).expect("corrupted chunk");
        chunks
    }

    fn physical_chunks_with_mismatched_sum_state(
        layout: &AggregateMvLayout,
        shape: &AggregateMvShape,
    ) -> Vec<Chunk> {
        let mut chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("chunk"),
                ],
            },
            layout,
            shape,
        )
        .expect("physical");
        let batch = &chunks[0].batch;
        let mut columns = batch.columns().to_vec();
        columns[5] = Arc::new(Int64Array::from(vec![31]));
        let corrupted = RecordBatch::try_new(batch.schema(), columns).expect("corrupted batch");
        chunks[0] = record_batch_to_chunk(corrupted).expect("corrupted chunk");
        chunks
    }

    #[test]
    fn materialize_physical_chunks_adds_row_id_and_state_columns() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let batch = visible_result_batch(vec![1], vec![2], vec![30]);
        let result = QueryResult {
            columns: Vec::new(),
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        };

        let chunks =
            materialize_aggregate_result_chunks(result, &layout, &shape).expect("materialize");
        let schema = chunks[0].batch.schema();
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                ROW_ID_COLUMN,
                "k1",
                "c",
                "s",
                "__agg_state_c",
                "__agg_state_s"
            ]
        );
    }

    #[test]
    fn merge_count_sum_state_adds_delta_to_old_state() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("old chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("old physical");
        let delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![3], vec![70]))
                        .expect("delta chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delta physical");
        let old_rows = load_aggregate_physical_rows(&old, &layout).expect("old rows");

        let merged =
            merge_aggregate_state_batches(&old_rows, &delta, &layout).expect("merged chunks");
        let batch = &merged[0].batch;
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        let s = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s");
        let state_c = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state c");
        let state_s = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state s");
        assert_eq!(c.value(0), 5);
        assert_eq!(s.value(0), 100);
        assert_eq!(state_c.value(0), 5);
        assert_eq!(state_s.value(0), 100);
    }

    #[test]
    fn merge_sum_state_preserves_null_for_new_all_null_group() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch_nullable_sum(
                        vec![1],
                        vec![1],
                        vec![None],
                    ))
                    .expect("delta chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delta physical");

        let merged =
            merge_aggregate_state_batches(&HashMap::new(), &delta, &layout).expect("merged chunks");
        let batch = &merged[0].batch;
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        let s = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s");
        let state_s = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state s");
        assert_eq!(c.value(0), 1);
        assert!(s.is_null(0));
        assert!(state_s.is_null(0));
    }

    #[test]
    fn build_layout_rejects_float_sum_state_type() {
        let mut columns = output_columns();
        columns[2].data_type = DataType::Float64;
        let err = build_aggregate_mv_layout(&test_shape(), &columns)
            .expect_err("float SUM state should be rejected");
        assert!(err.contains("SUM state type is unsupported"), "err={err}");
        assert!(err.contains("__agg_state_s"), "err={err}");
        assert!(err.contains("Float64"), "err={err}");
    }

    /// Shape: `select k1, min(v1) as m from ice.ns.orders group by k1`
    fn min_only_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, min(v1) as m from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    /// Shape: `select k1, max(v1) as m from ice.ns.orders group by k1`
    fn max_only_shape() -> AggregateMvShape {
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, max(v1) as m from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    /// Output columns for `min/max(v1) as m group by k1`: [k1: Int64, m: <typ>].
    fn min_max_output_columns(value_type: DataType) -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "m".to_string(),
                data_type: value_type,
                nullable: true,
            },
        ]
    }

    /// Build the Arrow Map<key_type, Int64> type used by MIN/MAX state.
    /// Mirrors the iceberg-rust convention applied in `build_aggregate_mv_layout`
    /// (entries-field name `"key_value"`, value field nullable).
    fn expected_min_max_state_arrow_type(key_type: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", key_type, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn build_layout_min_int64_input_produces_map_state_column() {
        let shape = min_only_shape();
        let layout = build_aggregate_mv_layout(&shape, &min_max_output_columns(DataType::Int64))
            .expect("layout");
        assert_eq!(layout.state_columns.len(), 2);
        let state = &layout.state_columns[0];
        assert_eq!(state.name, "__agg_state_m");
        assert_eq!(state.function, AggregateFunctionKind::Min);
        assert_eq!(state.state_role, AggregateStateRole::Single);
        assert!(!state.nullable);
        assert!(!state.count_star);
        assert_eq!(
            state.data_type,
            expected_min_max_state_arrow_type(DataType::Int64)
        );
        assert_eq!(
            state.sql_type,
            SqlType::Map(Box::new(SqlType::BigInt), Box::new(SqlType::BigInt))
        );
        // The retraction-count hidden state is the second state column.
        assert_eq!(
            layout.state_columns[1].state_role,
            AggregateStateRole::RetractionCount
        );
    }

    #[test]
    fn build_layout_max_utf8_input_produces_map_state_column() {
        let shape = max_only_shape();
        let layout = build_aggregate_mv_layout(&shape, &min_max_output_columns(DataType::Utf8))
            .expect("layout");
        let state = &layout.state_columns[0];
        assert_eq!(state.name, "__agg_state_m");
        assert_eq!(state.function, AggregateFunctionKind::Max);
        assert_eq!(state.state_role, AggregateStateRole::Single);
        assert!(!state.nullable);
        assert_eq!(
            state.data_type,
            expected_min_max_state_arrow_type(DataType::Utf8)
        );
        assert_eq!(
            state.sql_type,
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::BigInt))
        );
    }

    #[test]
    fn build_layout_sum_count_avg_branches_unchanged() {
        // Regression: SUM/COUNT/AVG state columns remain scalar Int64; no Map
        // shape leaks in from the Phase-2 MIN/MAX change.
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, sum(v1) as s, count(*) as c, avg(v1) as a from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        let columns = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let layout = build_aggregate_mv_layout(&shape, &columns).expect("layout");
        // Expected state columns (ordering follows shape.aggregates):
        //   SUM:Single -> Int64
        //   COUNT(*):Single -> Int64
        //   AVG:AvgSum -> Int64, AvgCount -> Int64
        // No RetractionCount hidden state because the shape includes COUNT(*).
        let state_types: Vec<&DataType> =
            layout.state_columns.iter().map(|c| &c.data_type).collect();
        for (idx, dt) in state_types.iter().enumerate() {
            assert!(
                !matches!(dt, DataType::Map(..)),
                "state column {idx} unexpectedly Map: {dt:?}"
            );
            assert_eq!(**dt, DataType::Int64, "state column {idx} expected Int64");
        }
        // Sanity-check the (function, state_role) ordering: SUM Single,
        // COUNT(*) Single, AVG AvgSum, AVG AvgCount.
        let roles: Vec<(AggregateFunctionKind, AggregateStateRole)> = layout
            .state_columns
            .iter()
            .map(|c| (c.function, c.state_role))
            .collect();
        assert_eq!(
            roles,
            vec![
                (AggregateFunctionKind::Sum, AggregateStateRole::Single),
                (AggregateFunctionKind::Count, AggregateStateRole::Single),
                (AggregateFunctionKind::Avg, AggregateStateRole::AvgSum),
                (AggregateFunctionKind::Avg, AggregateStateRole::AvgCount),
            ]
        );
    }

    #[test]
    fn build_layout_min_max_combined_with_sum_count() {
        // Combined shape: min(v1) -> Int64, sum(v1) -> Int64,
        // count(*) -> Int64, max(s1) -> Utf8.
        let shape = classify_incremental_mv_query(&parse_query(
            "select k1, min(v1) as mn, sum(v1) as s, count(*) as c, max(s1) as mx \
             from ice.ns.orders group by k1",
        ))
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        let columns = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "mn".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "mx".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ];
        let layout = build_aggregate_mv_layout(&shape, &columns).expect("layout");
        // Expected: [Map<Int64,Int64>, Int64, Int64, Map<Utf8,Int64>],
        // matching the (Min, Sum, Count(*), Max) aggregate ordering. No
        // RetractionCount hidden state because COUNT(*) covers it.
        let expected_types = vec![
            expected_min_max_state_arrow_type(DataType::Int64),
            DataType::Int64,
            DataType::Int64,
            expected_min_max_state_arrow_type(DataType::Utf8),
        ];
        let actual_types: Vec<DataType> = layout
            .state_columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();
        assert_eq!(actual_types, expected_types);
        // Sanity-check the function ordering matches our expectation.
        let funcs: Vec<AggregateFunctionKind> =
            layout.state_columns.iter().map(|c| c.function).collect();
        assert_eq!(
            funcs,
            vec![
                AggregateFunctionKind::Min,
                AggregateFunctionKind::Sum,
                AggregateFunctionKind::Count,
                AggregateFunctionKind::Max,
            ]
        );
    }

    #[test]
    fn validate_state_column_type_accepts_map_int64_for_min() {
        let ok_map = expected_min_max_state_arrow_type(DataType::Int64);
        validate_state_column_type(
            AggregateFunctionKind::Min,
            AggregateStateRole::Single,
            &ok_map,
            "__agg_state_m",
        )
        .expect("Map<Int64,Int64> must be accepted for MIN");

        let ok_map_utf8 = expected_min_max_state_arrow_type(DataType::Utf8);
        validate_state_column_type(
            AggregateFunctionKind::Max,
            AggregateStateRole::Single,
            &ok_map_utf8,
            "__agg_state_m",
        )
        .expect("Map<Utf8,Int64> must be accepted for MAX");
    }

    #[test]
    fn validate_state_column_type_rejects_map_with_non_int64_value_for_min() {
        // Map<Int64, Utf8> — wrong value type, must be rejected. Uses the
        // iceberg-rust entries-field name ("key_value") and nullable value
        // field so we exercise the value-type check, not the shape check.
        let bad_map = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ])),
                false,
            )),
            false,
        );
        let err = validate_state_column_type(
            AggregateFunctionKind::Min,
            AggregateStateRole::Single,
            &bad_map,
            "__agg_state_m",
        )
        .expect_err("Map<Int64, Utf8> must be rejected for MIN");
        assert!(err.contains("value type"), "err={err}");
        assert!(err.contains("Int64"), "err={err}");
    }

    #[test]
    fn validate_state_column_type_rejects_scalar_for_min() {
        // Pre-Phase-2 scalar Int64 state must now be rejected — MIN/MAX state
        // is required to be a value-count detail Map.
        let err = validate_state_column_type(
            AggregateFunctionKind::Min,
            AggregateStateRole::Single,
            &DataType::Int64,
            "__agg_state_m",
        )
        .expect_err("scalar Int64 must be rejected as MIN state type");
        assert!(err.contains("Map<K, Int64>"), "err={err}");
    }

    #[test]
    fn validate_state_column_type_accepts_float_min_max() {
        // IVM-P5 Float follow-up: Float MIN/MAX detail-state is now supported.
        // NaN handling lives in three sites: `scalar_keys_equal` treats two
        // NaNs as bit-equal, `sort_map_entries_by_key` sorts NaN to the end,
        // and `derive_visible_from_detail_map` skips NaN keys when reducing
        // MIN/MAX. See the tests below for each behaviour.
        for key_type in [DataType::Float32, DataType::Float64] {
            let map = expected_min_max_state_arrow_type(key_type.clone());
            for function in [AggregateFunctionKind::Min, AggregateFunctionKind::Max] {
                validate_state_column_type(
                    function,
                    AggregateStateRole::Single,
                    &map,
                    "__agg_state_m",
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "expected Float MIN/MAX to be accepted for {key_type:?} {function:?}, got error: {err}"
                    );
                });
            }
        }
    }

    #[test]
    fn scalar_keys_equal_treats_nan_as_equal() {
        // IEEE-754 says NaN != NaN, but for detail-map keys we want every
        // NaN to collapse to a single entry. This is bit-pattern-agnostic:
        // any NaN payload (signaling/quiet, different mantissas) must hash
        // to the same equivalence class.
        let nan1 = Some(AggScalarValue::Float64(f64::NAN));
        // Different NaN bit pattern (mantissa payload differs). Still a NaN
        // because the exponent is all-1s and the mantissa is non-zero.
        let nan2_value = f64::from_bits(f64::NAN.to_bits() ^ 0x42);
        assert!(nan2_value.is_nan(), "nan2 should still be NaN");
        let nan2 = Some(AggScalarValue::Float64(nan2_value));
        assert!(scalar_keys_equal(&nan1, &nan2));
        assert!(scalar_keys_equal(&nan1, &nan1));

        // NaN vs finite is NOT equal.
        let finite = Some(AggScalarValue::Float64(1.5));
        assert!(!scalar_keys_equal(&nan1, &finite));
        assert!(!scalar_keys_equal(&finite, &nan1));

        // Finite-vs-finite still works through the strict comparator.
        let a = Some(AggScalarValue::Float64(1.5));
        let b = Some(AggScalarValue::Float64(1.5));
        assert!(scalar_keys_equal(&a, &b));
        let c = Some(AggScalarValue::Float64(2.5));
        assert!(!scalar_keys_equal(&a, &c));
    }

    #[test]
    fn sort_map_entries_by_key_with_nan_sorts_to_end_no_error() {
        // Before the Float follow-up, NaN keys would cause sort_map_entries_by_key
        // to return Err (the strict comparator rejected them). Now NaN sorts
        // to the end of the run and the function succeeds.
        let state_col = AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "key_value",
                    DataType::Struct(arrow::datatypes::Fields::from(vec![
                        arrow::datatypes::Field::new("key", DataType::Float64, false),
                        arrow::datatypes::Field::new("value", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            sql_type: SqlType::Map(Box::new(SqlType::Double), Box::new(SqlType::BigInt)),
            nullable: false,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        };

        let entries: Vec<MapEntry> = vec![
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(1)),
            ),
            (
                Some(AggScalarValue::Float64(2.5)),
                Some(AggScalarValue::Int64(1)),
            ),
            (
                Some(AggScalarValue::Float64(1.5)),
                Some(AggScalarValue::Int64(1)),
            ),
        ];

        let sorted = sort_map_entries_by_key(entries, &state_col).expect("sort should succeed");
        assert_eq!(sorted.len(), 3);
        // Expect: 1.5, 2.5, NaN (NaN sorts to the end).
        match &sorted[0].0 {
            Some(AggScalarValue::Float64(v)) => assert_eq!(*v, 1.5),
            other => panic!("expected 1.5 at index 0, got {other:?}"),
        }
        match &sorted[1].0 {
            Some(AggScalarValue::Float64(v)) => assert_eq!(*v, 2.5),
            other => panic!("expected 2.5 at index 1, got {other:?}"),
        }
        match &sorted[2].0 {
            Some(AggScalarValue::Float64(v)) => assert!(v.is_nan()),
            other => panic!("expected NaN at index 2, got {other:?}"),
        }
    }

    #[test]
    fn derive_visible_from_detail_map_with_nan_min_finite_max_nan() {
        // {1.5: 1, NaN: 1, 2.5: 1} per `f64::total_cmp` ordering:
        //   - MIN should be 1.5 (NaN is total-cmp max, never picked as MIN)
        //   - MAX should be NaN (NaN dominates in total-cmp order)
        // This matches NovaRocks's plain MIN/MAX over Float64 (both use
        // total_cmp). The MV thus mirrors plain GROUP BY exactly.
        let map = AggScalarValue::Map(vec![
            (
                Some(AggScalarValue::Float64(1.5)),
                Some(AggScalarValue::Int64(1)),
            ),
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(1)),
            ),
            (
                Some(AggScalarValue::Float64(2.5)),
                Some(AggScalarValue::Int64(1)),
            ),
        ]);

        let min = derive_visible_from_detail_map(&map, MinMax::Min).expect("derive min");
        assert!(matches!(min, Some(AggScalarValue::Float64(v)) if v == 1.5));

        let max = derive_visible_from_detail_map(&map, MinMax::Max).expect("derive max");
        assert!(matches!(max, Some(AggScalarValue::Float64(v)) if v.is_nan()));
    }

    #[test]
    fn derive_visible_from_detail_map_all_nan_returns_nan() {
        // Group with only NaN entries: total_cmp picks NaN for both MIN
        // and MAX (the single entry is NaN; no finite value to beat it
        // for MIN, no finite value to lose to it for MAX).
        // This matches plain MIN/MAX over a NaN-only Float64 column.
        let map = AggScalarValue::Map(vec![
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(3)),
            ),
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(1)),
            ),
        ]);

        let min = derive_visible_from_detail_map(&map, MinMax::Min).expect("derive min");
        assert!(matches!(min, Some(AggScalarValue::Float64(v)) if v.is_nan()));

        let max = derive_visible_from_detail_map(&map, MinMax::Max).expect("derive max");
        assert!(matches!(max, Some(AggScalarValue::Float64(v)) if v.is_nan()));
    }

    #[test]
    fn merge_value_count_map_state_aggregates_nan_into_one_entry() {
        // Two delta states both containing NaN: after merge, the result map
        // should have a SINGLE NaN entry with the summed count — not two
        // separate NaN entries (which would happen with IEEE-754 equality).
        // This is where NovaRocks goes further than StarRocks (whose phmap
        // silently duplicates NaN keys).
        let state_col = AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "key_value",
                    DataType::Struct(arrow::datatypes::Fields::from(vec![
                        arrow::datatypes::Field::new("key", DataType::Float64, false),
                        arrow::datatypes::Field::new("value", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            sql_type: SqlType::Map(Box::new(SqlType::Double), Box::new(SqlType::BigInt)),
            nullable: false,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        };

        let old = Some(AggScalarValue::Map(vec![
            (
                Some(AggScalarValue::Float64(1.5)),
                Some(AggScalarValue::Int64(1)),
            ),
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(2)),
            ),
        ]));
        let delta = Some(AggScalarValue::Map(vec![
            (
                Some(AggScalarValue::Float64(f64::NAN)),
                Some(AggScalarValue::Int64(3)),
            ),
            (
                Some(AggScalarValue::Float64(2.5)),
                Some(AggScalarValue::Int64(1)),
            ),
        ]));

        let merged = merge_value_count_map_state(old, delta, &state_col)
            .expect("merge")
            .expect("non-empty result");
        let AggScalarValue::Map(entries) = merged else {
            panic!("expected Map result");
        };

        // Expect 3 entries: 1.5 (count=1), 2.5 (count=1), NaN (count=5).
        assert_eq!(entries.len(), 3);
        let nan_entries: Vec<_> = entries
            .iter()
            .filter(|(k, _)| matches!(k, Some(AggScalarValue::Float64(v)) if v.is_nan()))
            .collect();
        assert_eq!(
            nan_entries.len(),
            1,
            "NaN keys should aggregate to one entry"
        );
        match &nan_entries[0].1 {
            Some(AggScalarValue::Int64(c)) => assert_eq!(*c, 5),
            other => panic!("expected NaN count=5, got {other:?}"),
        }
    }

    #[test]
    fn row_id_uses_group_key_source_index_when_aggregate_is_projected_first() {
        let shape = aggregate_first_shape();
        let layout =
            build_aggregate_mv_layout(&shape, &aggregate_first_output_columns()).expect("layout");
        assert_eq!(layout.group_key_source_indexes, vec![1]);
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(aggregate_first_result_batch(vec![1, 1], vec![10, 20]))
                        .expect("old chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("old physical");
        let old_rows = load_aggregate_physical_rows(&old, &layout).expect("old rows");
        assert_eq!(old_rows.len(), 2);
        let delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(aggregate_first_result_batch(vec![2, 3], vec![10, 20]))
                        .expect("delta chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delta physical");

        let merged =
            merge_aggregate_state_batches(&old_rows, &delta, &layout).expect("merged chunks");
        let batch = &merged[0].batch;
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        let k1 = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("k1");
        let merged_by_key = (0..batch.num_rows())
            .map(|row| (k1.value(row), c.value(row)))
            .collect::<HashMap<_, _>>();
        assert_eq!(merged_by_key.get(&10), Some(&3));
        assert_eq!(merged_by_key.get(&20), Some(&4));
    }

    #[test]
    fn merge_rejects_duplicate_old_row_id() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(
                        vec![1, 1],
                        vec![2, 4],
                        vec![30, 40],
                    ))
                    .expect("old chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("old physical");

        let err = load_aggregate_physical_rows(&old, &layout).expect_err("duplicate rejected");
        assert!(err.contains("duplicate row id"), "err={err}");
    }

    #[test]
    fn load_rejects_null_count_state_as_corruption() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let chunks = physical_chunks_with_count_state(&layout, &shape, None);

        let err = load_aggregate_physical_rows(&chunks, &layout).expect_err("null count rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("COUNT"), "err={err}");
        assert!(err.contains("NULL"), "err={err}");
    }

    #[test]
    fn load_rejects_zero_count_state_as_corruption() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let chunks = physical_chunks_with_count_state(&layout, &shape, Some(0));

        let err = load_aggregate_physical_rows(&chunks, &layout).expect_err("zero count rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("COUNT"), "err={err}");
        assert!(err.contains("state"), "err={err}");
    }

    #[test]
    fn load_allows_zero_count_expr_state() {
        let shape = count_expr_shape();
        let layout =
            build_aggregate_mv_layout(&shape, &count_expr_output_columns()).expect("layout");
        let chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(count_expr_result_batch(vec![1], vec![0]))
                        .expect("chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("physical");

        let rows = load_aggregate_physical_rows(&chunks, &layout).expect("loaded");

        let row = rows.values().next().expect("row");
        assert!(matches!(
            row.visible_values[1],
            Some(AggScalarValue::Int64(0))
        ));
        assert!(matches!(
            row.state_values[0],
            Some(AggScalarValue::Int64(0))
        ));
    }

    #[test]
    fn load_rejects_row_id_mismatch_as_corruption() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let chunks = physical_chunks_with_bad_row_id(&layout, &shape);

        let err =
            load_aggregate_physical_rows(&chunks, &layout).expect_err("row id mismatch rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("row id"), "err={err}");
        assert!(err.contains("visible group key"), "err={err}");
    }

    #[test]
    fn load_rejects_visible_aggregate_state_mismatch_as_corruption() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let chunks = physical_chunks_with_mismatched_sum_state(&layout, &shape);

        let err = load_aggregate_physical_rows(&chunks, &layout)
            .expect_err("visible/state mismatch rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("visible aggregate"), "err={err}");
        assert!(err.contains("state"), "err={err}");
    }

    #[test]
    fn merge_combines_duplicate_delta_row_id() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(
                        vec![1, 1],
                        vec![2, 3],
                        vec![30, 40],
                    ))
                    .expect("delta chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delta physical");

        let merged =
            merge_aggregate_state_batches(&HashMap::new(), &delta, &layout).expect("merge");
        let batch = &merged[0].batch;
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        let s = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(c.value(0), 5);
        assert_eq!(s.value(0), 70);
    }

    #[test]
    fn merge_combines_insert_and_delete_delta_for_same_row_id() {
        let shape = test_shape();
        let layout = build_aggregate_mv_layout(&shape, &output_columns()).expect("layout");
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![3], vec![130]))
                        .expect("old chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("old physical");
        let old_rows = load_aggregate_physical_rows(&old, &layout).expect("old rows");
        let insert_delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![320]))
                        .expect("insert chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("insert delta");
        let delete_delta_positive = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![3], vec![130]))
                        .expect("delete chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delete delta");
        let delete_delta =
            negate_aggregate_state_chunks(delete_delta_positive, &layout).expect("negate");
        let mut delta = Vec::new();
        delta.extend(insert_delta);
        delta.extend(delete_delta);

        let merged = merge_aggregate_state_batches(&old_rows, &delta, &layout)
            .expect("same-row insert/delete delta should merge");
        let batch = &merged[0].batch;
        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c");
        let s = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("s");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(c.value(0), 2);
        assert_eq!(s.value(0), 320);
    }

    #[test]
    fn negate_aggregate_state_chunks_flips_count_and_sum() {
        // Build a minimal layout with one Int64 state column.
        let layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                source_index: 0,
            }],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };
        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["row1", "row2"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 3])) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");
        let negated = negate_aggregate_state_chunks(vec![chunk], &layout).expect("negate");
        assert_eq!(negated.len(), 1);
        let state = negated[0]
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state col");
        assert_eq!(state.value(0), -5);
        assert_eq!(state.value(1), -3);
        // Visible column should be unchanged.
        let visible = negated[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("visible col");
        assert_eq!(visible.value(0), 5);
        assert_eq!(visible.value(1), 3);
    }

    #[test]
    fn merge_drops_rows_with_count_fully_retracted() {
        // The merge's load_aggregate_physical_rows call validates
        // delta count states as non-negative (count_star=false) or
        // strictly positive (count_star=true), so we cannot hand it a
        // chunk with a literal negative count. Instead, we exercise
        // the drop branch by pre-seeding old_rows with the state
        // value the merge would produce after the delta has been
        // applied (i.e. zero), and pass an empty delta. The merge
        // function leaves merged state untouched and then runs the
        // new drop filter — which is what we want to exercise here.
        //
        // PR-3 Task 10 will call merge with the negated delta on a
        // load path that allows negative counts; PR-4 will replace
        // the post-hoc negation with a proper reversible operator.
        let layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                source_index: 0,
            }],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };

        // Pre-merged old state for the group: count already at zero.
        let mut old_rows: HashMap<String, AggregatePhysicalRow> = HashMap::new();
        old_rows.insert(
            "g1".to_string(),
            AggregatePhysicalRow {
                row_id: "g1".to_string(),
                visible_values: vec![Some(AggScalarValue::Int64(0))],
                state_values: vec![Some(AggScalarValue::Int64(0))],
            },
        );

        let merged =
            merge_aggregate_state_batches(&old_rows, &[], &layout).expect("merge zero count");
        let total_rows: usize = merged.iter().map(|c| c.batch.num_rows()).sum();
        assert_eq!(total_rows, 0, "row should be dropped after full retraction");

        // Sanity check: a non-zero count must be retained.
        old_rows.get_mut("g1").unwrap().state_values[0] = Some(AggScalarValue::Int64(1));
        old_rows.get_mut("g1").unwrap().visible_values[0] = Some(AggScalarValue::Int64(1));
        let kept = merge_aggregate_state_batches(&old_rows, &[], &layout).expect("merge nonzero");
        let kept_rows: usize = kept.iter().map(|c| c.batch.num_rows()).sum();
        assert_eq!(kept_rows, 1, "non-zero count row should be retained");
    }

    #[test]
    fn build_sum_only_layout_adds_hidden_retraction_count_state() {
        let shape = sum_only_shape();
        let layout = build_aggregate_mv_layout(&shape, &sum_only_output_columns()).expect("layout");

        assert_eq!(layout.state_columns.len(), 2);
        let hidden = layout
            .state_columns
            .iter()
            .find(|column| column.name == "__agg_state___ivm_row_count")
            .expect("hidden retraction count state");
        assert_eq!(hidden.data_type, DataType::Int64);
        assert_eq!(hidden.state_role, AggregateStateRole::RetractionCount);
    }

    #[test]
    fn merge_sum_only_state_drops_group_when_retraction_count_reaches_zero() {
        let shape = sum_only_shape();
        let layout = build_aggregate_mv_layout(&shape, &sum_only_output_columns()).expect("layout");
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(sum_only_state_result_batch(vec![1], vec![100], vec![1]))
                        .expect("old chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("old physical");
        let old_rows = load_aggregate_physical_rows(&old, &layout).expect("old rows");
        let delta = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(sum_only_state_result_batch(
                        vec![1],
                        vec![-100],
                        vec![-1],
                    ))
                    .expect("delta chunk"),
                ],
            },
            &layout,
            &shape,
        )
        .expect("delta physical");

        let merged =
            merge_aggregate_state_batches(&old_rows, &delta, &layout).expect("merged chunks");
        let total_rows: usize = merged.iter().map(|chunk| chunk.batch.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "fully retracted SUM-only group must be dropped"
        );
    }

    // ---- AVG helper layout ----

    fn make_avg_layout_int_to_double() -> AggregateMvLayout {
        AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "a".to_string(),
                data_type: DataType::Float64,
                sql_type: SqlType::Double,
                nullable: true,
                source_index: 0,
            }],
            state_columns: vec![
                AggregateStateColumn {
                    name: "__agg_state_a__sum".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: true,
                    visible_source_index: 0,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgSum,
                    count_star: false,
                },
                AggregateStateColumn {
                    name: "__agg_state_a__count".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    visible_source_index: 0,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgCount,
                    count_star: false,
                },
            ],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        }
    }

    /// Build a minimal AVG layout for AVG(Decimal128(20,2)) -> visible Decimal128(38, 8).
    /// (scale 2 + 6 = 8 per analyzer promotion rule for s <= 6.)
    /// sum state: Decimal128(38, 8) (at visible scale, so derive_avg_visible does direct division)
    /// count state: Int64
    fn make_avg_layout_decimal() -> AggregateMvLayout {
        AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "a".to_string(),
                data_type: DataType::Decimal128(38, 8),
                sql_type: SqlType::Decimal {
                    precision: 38,
                    scale: 8,
                },
                nullable: true,
                source_index: 0,
            }],
            state_columns: vec![
                AggregateStateColumn {
                    name: "__agg_state_a__sum".to_string(),
                    data_type: DataType::Decimal128(38, 8),
                    sql_type: SqlType::Decimal {
                        precision: 38,
                        scale: 8,
                    },
                    nullable: true,
                    visible_source_index: 0,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgSum,
                    count_star: false,
                },
                AggregateStateColumn {
                    name: "__agg_state_a__count".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    visible_source_index: 0,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Avg,
                    state_role: AggregateStateRole::AvgCount,
                    count_star: false,
                },
            ],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        }
    }

    // ---- AVG layout tests ----

    #[test]
    fn build_layout_avg_produces_state_columns_with_hidden_retraction_count() {
        use crate::connector::starrocks::managed::mv_shape::{
            AggregateCallShape, AggregateInput, AggregateMvShape, GroupKeyShape,
            VisibleAggregateOutput,
        };
        use sqlparser::ast::ObjectName;

        let shape = AggregateMvShape {
            base_table: ObjectName(vec![]),
            group_keys: vec![GroupKeyShape {
                output_name: "k".to_string(),
                expr: sqlparser::ast::Expr::Identifier("k".into()),
            }],
            aggregates: vec![AggregateCallShape {
                output_name: "a".to_string(),
                function: AggregateFunctionKind::Avg,
                input: AggregateInput::Expr(Box::new(sqlparser::ast::Expr::Identifier("v".into()))),
            }],
            visible_outputs: vec![
                VisibleAggregateOutput::GroupKey(0),
                VisibleAggregateOutput::Aggregate(0),
            ],
        };
        let outputs = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a".to_string(),
                data_type: DataType::Decimal128(38, 10),
                nullable: true,
            },
        ];
        let layout = build_aggregate_mv_layout(&shape, &outputs).expect("layout build");
        assert_eq!(layout.state_columns.len(), 3);
        assert_eq!(
            layout.state_columns[0].state_role,
            AggregateStateRole::AvgSum
        );
        assert_eq!(layout.state_columns[0].name, "__agg_state_a__sum");
        assert_eq!(layout.state_columns[0].aggregate_index, 0);
        // AvgSum state column is at visible scale (10) to allow direct integer division.
        assert_eq!(
            layout.state_columns[0].data_type,
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            layout.state_columns[1].state_role,
            AggregateStateRole::AvgCount
        );
        assert_eq!(layout.state_columns[1].name, "__agg_state_a__count");
        assert_eq!(layout.state_columns[1].data_type, DataType::Int64);
        assert_eq!(layout.state_columns[1].aggregate_index, 0);
        assert_eq!(
            layout.state_columns[2].state_role,
            AggregateStateRole::RetractionCount
        );
        assert_eq!(
            layout.state_columns[2].name,
            AGG_RETRACTION_COUNT_STATE_COLUMN
        );

        let float_outputs = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];
        let float_layout =
            build_aggregate_mv_layout(&shape, &float_outputs).expect("Float64 AVG visible layout");
        assert_eq!(float_layout.state_columns[0].data_type, DataType::Int64);
        assert_eq!(float_layout.state_columns[1].data_type, DataType::Int64);
        assert_eq!(float_layout.state_columns[2].data_type, DataType::Int64);
    }

    // ---- AVG visible derivation tests ----

    /// AVG(Decimal128): sum=30_00000000 (3.000... at scale 8 * 10), count=4 -> visible=7.5 at scale 8
    /// 30_00000000 / 4 = 7_50000000 = 7.50000000 at scale 8
    #[test]
    fn materialize_visible_value_avg_decimal_divides_correctly() {
        let layout = make_avg_layout_decimal();
        let mut row = AggregatePhysicalRow {
            row_id: "g".to_string(),
            visible_values: vec![None],
            // sum = 3000000000 represents 30.00000000 at scale 8; count = 4
            // expected visible = 30.00000000 / 4 = 7.50000000 = raw 750000000
            state_values: vec![
                Some(AggScalarValue::Decimal128(3_000_000_000_i128)),
                Some(AggScalarValue::Int64(4)),
            ],
        };
        update_visible_values_from_state(&mut row, &layout).expect("derive");
        assert!(
            matches!(
                row.visible_values[0],
                Some(AggScalarValue::Decimal128(750_000_000_i128))
            ),
            "expected Decimal128(750000000) = 7.50000000 at scale 8, got {:?}",
            row.visible_values[0]
        );
    }

    #[test]
    fn materialize_visible_value_avg_count_zero_returns_null() {
        let layout = make_avg_layout_decimal();
        let mut row = AggregatePhysicalRow {
            row_id: "g".to_string(),
            visible_values: vec![Some(AggScalarValue::Decimal128(0))],
            state_values: vec![None, Some(AggScalarValue::Int64(0))],
        };
        update_visible_values_from_state(&mut row, &layout).expect("derive");
        assert!(
            row.visible_values[0].is_none(),
            "expected None, got {:?}",
            row.visible_values[0]
        );
    }

    // ---- AVG merge tests ----

    #[test]
    fn merge_state_value_avg_sum_int64() {
        let column = AggregateStateColumn {
            name: "__agg_state_a__sum".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: true,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Avg,
            state_role: AggregateStateRole::AvgSum,
            count_star: false,
        };
        // Some + Some
        let r = merge_state_value(
            Some(AggScalarValue::Int64(10)),
            Some(AggScalarValue::Int64(20)),
            &column,
        )
        .expect("merge");
        assert!(matches!(r, Some(AggScalarValue::Int64(30))), "got {r:?}");
        // Some + None
        let r = merge_state_value(Some(AggScalarValue::Int64(10)), None, &column).expect("merge");
        assert!(matches!(r, Some(AggScalarValue::Int64(10))), "got {r:?}");
        // None + None
        let r = merge_state_value(None, None, &column).expect("merge");
        assert!(r.is_none(), "got {r:?}");
    }

    #[test]
    fn merge_state_value_avg_count_int64() {
        let column = AggregateStateColumn {
            name: "__agg_state_a__count".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: false,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Avg,
            state_role: AggregateStateRole::AvgCount,
            count_star: false,
        };
        let r = merge_state_value(
            Some(AggScalarValue::Int64(2)),
            Some(AggScalarValue::Int64(3)),
            &column,
        )
        .expect("merge");
        assert!(matches!(r, Some(AggScalarValue::Int64(5))), "got {r:?}");
    }

    // ---- AVG negate test ----

    #[test]
    fn negate_aggregate_state_chunks_avg_flips_both_substates() {
        let layout = make_avg_layout_int_to_double();
        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["g1"])) as ArrayRef,
                Arc::new(arrow::array::Float64Array::from(vec![Some(7.5)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![30_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![4_i64])) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");
        let negated = negate_aggregate_state_chunks(vec![chunk], &layout).expect("negate");
        let sum = negated[0]
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cnt = negated[0]
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(sum.value(0), -30);
        assert_eq!(cnt.value(0), -4);
    }

    // ---- AVG materialize test (state-shaped input) ----

    fn avg_state_shape() -> AggregateMvShape {
        let shape = crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
            &parse_query("select k1, avg(v2) as a from ice.ns.orders group by k1"),
        )
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    #[test]
    fn materialize_aggregate_result_avg_state_shaped_input() {
        use arrow::array::Float64Array;
        // AVG(v2) AS a: visible = Float64, state = [__agg_state_a__sum Int64, __agg_state_a__count Int64]
        let shape = avg_state_shape();
        let output_columns = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ];
        let layout = build_aggregate_mv_layout(&shape, &output_columns).expect("layout");

        // State-shaped input: [k1, __agg_state_a__sum, __agg_state_a__count, row_count]
        // (visible_outputs = [GroupKey(0), Aggregate(0)] plus hidden retraction count)
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("__agg_state_a__sum", DataType::Int64, true),
                Field::new("__agg_state_a__count", DataType::Int64, false),
                Field::new(AGG_RETRACTION_COUNT_STATE_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![30_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![4_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![4_i64])) as ArrayRef,
            ],
        )
        .expect("state-shaped batch");

        let chunk =
            materialize_aggregate_result_batch(&batch, &layout, &shape).expect("materialize");

        // Physical schema: [__row_id__, k1, a, __agg_state_a__sum, __agg_state_a__count]
        let batch_schema = chunk.batch.schema();
        let schema_names: Vec<&str> = batch_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            schema_names,
            vec![
                ROW_ID_COLUMN,
                "k1",
                "a",
                "__agg_state_a__sum",
                "__agg_state_a__count",
                AGG_RETRACTION_COUNT_STATE_COLUMN
            ],
            "unexpected schema"
        );

        // Visible 'a' = 30 / 4 = 7.5
        let visible_a = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("visible a Float64");
        assert!(
            (visible_a.value(0) - 7.5).abs() < 1e-12,
            "expected visible a = 7.5, got {}",
            visible_a.value(0)
        );

        // State sum = 30, count = 4
        let state_sum = chunk
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state sum Int64");
        let state_cnt = chunk
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("state count Int64");
        assert_eq!(state_sum.value(0), 30, "state sum");
        assert_eq!(state_cnt.value(0), 4, "state count");
    }

    // ---- IVM-P5 Phase 4: MIN/MAX detail-map state helpers ----

    /// Build a `Map<key_type, Int64>` MIN-Single state column for use in
    /// merge / negate / derive tests. The visible column type is the input
    /// scalar type (matches `build_aggregate_mv_layout`'s wiring).
    fn min_int64_map_state_column() -> AggregateStateColumn {
        AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Map(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(arrow::datatypes::Fields::from(vec![
                        Arc::new(Field::new("key", DataType::Int64, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ])),
                    false,
                )),
                false,
            ),
            sql_type: SqlType::Map(Box::new(SqlType::BigInt), Box::new(SqlType::BigInt)),
            nullable: false,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        }
    }

    fn max_int64_map_state_column() -> AggregateStateColumn {
        let mut col = min_int64_map_state_column();
        col.name = "__agg_state_mx".to_string();
        col.function = AggregateFunctionKind::Max;
        col
    }

    /// Build a `Map<Int64, Int64>` AggScalarValue from raw `(key, count)` pairs.
    /// Constructing via `Vec<(Some(...), Some(...))>` keeps the test data
    /// close to what `agg_scalar_from_array` produces from a real MapArray.
    fn make_map_state(entries: &[(i64, i64)]) -> AggScalarValue {
        AggScalarValue::Map(
            entries
                .iter()
                .map(|(k, v)| {
                    (
                        Some(AggScalarValue::Int64(*k)),
                        Some(AggScalarValue::Int64(*v)),
                    )
                })
                .collect(),
        )
    }

    /// Extract `(key, count)` pairs from a Map AggScalarValue for assertions.
    /// Panics on type mismatch — tests should never trigger that, so we
    /// don't bother propagating errors.
    fn map_state_pairs(value: &AggScalarValue) -> Vec<(i64, i64)> {
        let AggScalarValue::Map(entries) = value else {
            panic!("expected Map AggScalarValue, got {value:?}");
        };
        entries
            .iter()
            .map(|(k, v)| {
                let k = match k {
                    Some(AggScalarValue::Int64(k)) => *k,
                    other => panic!("expected Int64 key, got {other:?}"),
                };
                let v = match v {
                    Some(AggScalarValue::Int64(v)) => *v,
                    other => panic!("expected Int64 value, got {other:?}"),
                };
                (k, v)
            })
            .collect()
    }

    #[test]
    fn merge_value_count_map_state_empty_plus_empty() {
        let column = min_int64_map_state_column();
        let r = merge_state_value(
            Some(make_map_state(&[])),
            Some(make_map_state(&[])),
            &column,
        )
        .expect("merge");
        let value = r.expect("merged map");
        assert_eq!(map_state_pairs(&value), Vec::<(i64, i64)>::new());
    }

    #[test]
    fn merge_value_count_map_state_populated_plus_empty() {
        let column = min_int64_map_state_column();
        let r = merge_state_value(
            Some(make_map_state(&[(1, 1), (2, 1)])),
            Some(make_map_state(&[])),
            &column,
        )
        .expect("merge");
        let value = r.expect("merged map");
        assert_eq!(map_state_pairs(&value), vec![(1, 1), (2, 1)]);
    }

    #[test]
    fn merge_value_count_map_state_disjoint_keys() {
        let column = min_int64_map_state_column();
        let r = merge_state_value(
            Some(make_map_state(&[(1, 1), (2, 1)])),
            Some(make_map_state(&[(3, 1), (4, 1)])),
            &column,
        )
        .expect("merge");
        let value = r.expect("merged map");
        assert_eq!(
            map_state_pairs(&value),
            vec![(1, 1), (2, 1), (3, 1), (4, 1)]
        );
    }

    #[test]
    fn merge_value_count_map_state_overlapping_keys() {
        let column = min_int64_map_state_column();
        let r = merge_state_value(
            Some(make_map_state(&[(1, 1), (2, 2)])),
            Some(make_map_state(&[(2, 3), (3, 1)])),
            &column,
        )
        .expect("merge");
        let value = r.expect("merged map");
        // 2 -> 2 + 3 = 5
        assert_eq!(map_state_pairs(&value), vec![(1, 1), (2, 5), (3, 1)]);
    }

    #[test]
    fn merge_value_count_map_state_negative_count_in_delta() {
        let column = min_int64_map_state_column();
        // Delta carries (1, -1) and (3, 1); old has (1, 1) and (2, 2).
        // After per-key sum: (1, 0), (2, 2), (3, 1).
        // merge_value_count_map_state prunes zero entries (spec §3.5 —
        // eager pruning at every merge step), so the (1, 0) row drops.
        let r = merge_state_value(
            Some(make_map_state(&[(1, 1), (2, 2)])),
            Some(make_map_state(&[(1, -1), (3, 1)])),
            &column,
        )
        .expect("merge");
        let value = r.expect("merged map");
        assert_eq!(map_state_pairs(&value), vec![(2, 2), (3, 1)]);
    }

    #[test]
    fn prune_zero_entries_from_map_entries_removes_zero_keeps_others() {
        // The helper is purely about the value-zero case. Negative counts
        // (-1 here) are preserved — whether they survive into a "final"
        // state is a flow-level concern.
        let AggScalarValue::Map(entries) = make_map_state(&[(1, 1), (2, 0), (3, -1)]) else {
            panic!("expected Map state");
        };
        let pruned = prune_zero_entries_from_map_entries(entries);
        assert_eq!(
            map_state_pairs(&AggScalarValue::Map(pruned)),
            vec![(1, 1), (3, -1)]
        );
    }

    #[test]
    fn derive_visible_from_detail_map_min_returns_smallest_active_value() {
        let map = make_map_state(&[(5, 1), (10, 2), (20, 1)]);
        let r = derive_visible_from_detail_map(&map, MinMax::Min).expect("derive");
        assert!(
            matches!(r, Some(AggScalarValue::Int64(5))),
            "expected Some(Int64(5)), got {r:?}"
        );
    }

    #[test]
    fn derive_visible_from_detail_map_max_returns_largest_active_value() {
        let map = make_map_state(&[(5, 1), (10, 2), (20, 1)]);
        let r = derive_visible_from_detail_map(&map, MinMax::Max).expect("derive");
        assert!(
            matches!(r, Some(AggScalarValue::Int64(20))),
            "expected Some(Int64(20)), got {r:?}"
        );
    }

    #[test]
    fn derive_visible_from_detail_map_all_zero_returns_none() {
        let map = make_map_state(&[(1, 0), (2, 0)]);
        let min = derive_visible_from_detail_map(&map, MinMax::Min).expect("derive min");
        let max = derive_visible_from_detail_map(&map, MinMax::Max).expect("derive max");
        assert!(min.is_none(), "expected None, got {min:?}");
        assert!(max.is_none(), "expected None, got {max:?}");
    }

    #[test]
    fn derive_visible_from_detail_map_with_negative_counts_skipped() {
        // The -1 count at key 1 is excluded from the derivation, so the
        // visible MIN must be 2 (the smallest key with count > 0).
        let map = make_map_state(&[(1, -1), (2, 2), (3, 1)]);
        let r = derive_visible_from_detail_map(&map, MinMax::Min).expect("derive");
        assert!(
            matches!(r, Some(AggScalarValue::Int64(2))),
            "expected Some(Int64(2)), got {r:?}"
        );
    }

    #[test]
    fn merge_then_prune_then_derive_visible_end_to_end() {
        // INSERT 5 rows over 3 distinct values {10, 20, 30}.
        // Initial detail state: {10:1, 20:1, 30:1, ... 10:1, 20:1, 30:1}
        // — but we collapse it into the merged state directly:
        // {10:1, 20:1, 30:1} (each value once) then later we insert dup rows.
        // For simplicity: simulate the merged state after the 5-row INSERT
        // as 10:2, 20:2, 30:1 (5 rows total).
        let column = min_int64_map_state_column();
        let after_insert = merge_state_value(
            None,
            Some(make_map_state(&[(10, 2), (20, 2), (30, 1)])),
            &column,
        )
        .expect("merge insert")
        .expect("some");
        assert_eq!(
            map_state_pairs(&after_insert),
            vec![(10, 2), (20, 2), (30, 1)]
        );

        // DELETE 10 (boundary — 10 was the current MIN). With 2 prior 10s
        // and one deletion, the merged state has 10 -> 2 + (-1) = 1.
        // Final MIN is still 10. Spec example deletes BOTH 10s; replicate
        // that here by passing a -2 delta for the 10 key.
        let after_delete = merge_state_value(
            Some(after_insert),
            Some(make_map_state(&[(10, -2)])),
            &column,
        )
        .expect("merge delete")
        .expect("some");
        // The (10, 0) entry gets pruned eagerly in the same call.
        assert_eq!(map_state_pairs(&after_delete), vec![(20, 2), (30, 1)]);

        // Derive visible MIN — should now be 20 (the second-smallest value).
        let visible = derive_visible_from_detail_map(&after_delete, MinMax::Min).expect("derive");
        assert!(
            matches!(visible, Some(AggScalarValue::Int64(20))),
            "expected Some(Int64(20)), got {visible:?}"
        );

        // Sanity-check MAX too.
        let max = derive_visible_from_detail_map(&after_delete, MinMax::Max).expect("max");
        assert!(
            matches!(max, Some(AggScalarValue::Int64(30))),
            "expected Some(Int64(30)), got {max:?}"
        );
        let _ = column;
    }

    /// Sibling test for `negate_aggregate_state_chunks_flips_count_and_sum`:
    /// exercise a layout WITH a MIN-Single map state and assert the
    /// MapArray is negated entry-wise.
    #[test]
    fn negate_aggregate_state_chunks_with_min_map_state_flips_counts() {
        use arrow::array::MapArray;

        let state_col = min_int64_map_state_column();
        let layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "mn".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: true,
                source_index: 0,
            }],
            state_columns: vec![state_col.clone()],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };

        // Build a single-row physical batch:
        //   row_id = "g"
        //   visible mn = 10
        //   state map = {10:1, 20:2}
        let entries_struct = StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
            ],
            None,
        );
        let map_field = match &state_col.data_type {
            DataType::Map(field, _) => field.clone(),
            other => panic!("unexpected state column type {other:?}"),
        };
        let map_array = MapArray::try_new(
            map_field,
            OffsetBuffer::new(vec![0_i32, 2].into()),
            entries_struct,
            None,
            false,
        )
        .expect("map array");

        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["g"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10_i64)])) as ArrayRef,
                Arc::new(map_array) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");
        let negated = negate_aggregate_state_chunks(vec![chunk], &layout).expect("negate");

        assert_eq!(negated.len(), 1);
        let negated_state = negated[0]
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("state map");
        let neg_values = negated_state
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values");
        let neg_keys = negated_state
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("keys");
        assert_eq!((neg_keys.value(0), neg_keys.value(1)), (10, 20));
        assert_eq!((neg_values.value(0), neg_values.value(1)), (-1, -2));

        // Visible column must be unchanged by negate.
        let visible = negated[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("visible col");
        assert_eq!(visible.value(0), 10);
    }

    /// Sibling of `negate_aggregate_state_chunks_with_min_map_state_flips_counts`
    /// for the MAX-Single map state. The negate path is function-agnostic (it
    /// operates on the Arrow MapArray), but cover MAX explicitly so a future
    /// MIN-specific tweak can't silently regress MAX.
    #[test]
    fn negate_aggregate_state_chunks_with_max_map_state_flips_counts() {
        use arrow::array::MapArray;

        let state_col = max_int64_map_state_column();
        let layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "mx".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: true,
                source_index: 0,
            }],
            state_columns: vec![state_col.clone()],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };

        // Build a single-row physical batch:
        //   row_id = "g"
        //   visible mx = 20
        //   state map = {10:1, 20:2}
        let entries_struct = StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("key", DataType::Int64, false)),
                Arc::new(Field::new("value", DataType::Int64, true)),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
            ],
            None,
        );
        let map_field = match &state_col.data_type {
            DataType::Map(field, _) => field.clone(),
            other => panic!("unexpected state column type {other:?}"),
        };
        let map_array = MapArray::try_new(
            map_field,
            OffsetBuffer::new(vec![0_i32, 2].into()),
            entries_struct,
            None,
            false,
        )
        .expect("map array");

        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["g"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(20_i64)])) as ArrayRef,
                Arc::new(map_array) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");
        let negated = negate_aggregate_state_chunks(vec![chunk], &layout).expect("negate");

        assert_eq!(negated.len(), 1);
        let negated_state = negated[0]
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("state map");
        let neg_values = negated_state
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values");
        let neg_keys = negated_state
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("keys");
        assert_eq!((neg_keys.value(0), neg_keys.value(1)), (10, 20));
        assert_eq!((neg_values.value(0), neg_values.value(1)), (-1, -2));

        // Visible column must be unchanged by negate.
        let visible = negated[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("visible col");
        assert_eq!(visible.value(0), 20);
    }

    // ---- End-to-end AVG merge test ----

    #[test]
    fn merge_aggregate_state_batches_avg_int_to_double() {
        let layout = make_avg_layout_int_to_double();
        // The layout has no group keys, so the computed row_id is always "" (empty join).
        let row_id = "";
        let mut old: HashMap<String, AggregatePhysicalRow> = HashMap::new();
        old.insert(
            row_id.to_string(),
            AggregatePhysicalRow {
                row_id: row_id.to_string(),
                visible_values: vec![Some(AggScalarValue::Float64(5.0))],
                state_values: vec![
                    Some(AggScalarValue::Int64(10)),
                    Some(AggScalarValue::Int64(2)),
                ],
            },
        );
        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![row_id])) as ArrayRef,
                Arc::new(arrow::array::Float64Array::from(vec![Some(10.0)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![20_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
            ],
        )
        .expect("batch");
        let delta = vec![record_batch_to_chunk(batch).expect("chunk")];
        let merged = merge_aggregate_state_batches(&old, &delta, &layout).expect("merge");
        assert_eq!(merged.len(), 1);
        let visible = merged[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // (10 + 20) / (2 + 2) = 30 / 4 = 7.5
        assert_eq!(visible.value(0), 7.5);
    }

    // ---- Bug C1: AVG Decimal128 scale correctness tests ----

    /// AVG(Decimal128(20, 4)) with state-shaped input batch (from SUM executor).
    ///
    /// The SUM executor produces Decimal128(38, 4) (SUM keeps input scale).
    /// The AvgSum state column is declared at visible scale (10) by avg_sum_state_type.
    /// materialize_aggregate_result_batch must rescale the raw i128 when writing:
    ///   batch scale = 4, state scale = 10 -> multiply by 10^(10-4) = 10^6
    ///
    /// sum raw value at scale 4 = 3005000 (represents 300.5000)
    /// After rescale to scale 10: 3005000 * 10^6 = 3005000000000
    /// count = 2
    /// Expected visible AVG = 3005000000000 / 2 = 1502500000000 (150.2500000000 at scale 10)
    ///
    /// Bug C1: without the fix, raw 3005000 is stored as-is in the scale-10 state column,
    /// then derive_avg_visible produces 3005000 / 2 = 1502500 which represents 0.0001502500
    /// at scale 10 - off by 10^6.
    #[test]
    fn avg_decimal128_materialize_correct_scale() {
        use arrow::array::Decimal128Array;

        let shape = crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
            &parse_query("select k1, avg(d) as a from ice.ns.orders group by k1"),
        )
        .expect("classify");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };

        // Output columns: [k1 Int64, a AVG(Decimal(20,4)) -> Decimal128(38, 10)]
        let output_columns = vec![
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a".to_string(),
                data_type: DataType::Decimal128(38, 10),
                nullable: true,
            },
        ];
        let layout = build_aggregate_mv_layout(&shape, &output_columns).expect("layout");

        // The AvgSum state column must be declared at visible scale (10).
        let sum_col = layout
            .state_columns
            .iter()
            .find(|c| c.state_role == AggregateStateRole::AvgSum)
            .expect("AvgSum state column");
        assert_eq!(
            sum_col.data_type,
            DataType::Decimal128(38, 10),
            "AvgSum state column must use visible scale (10)"
        );

        // State-shaped input batch from executor: [k1, sum_col, count_col, row_count].
        // visible_outputs = [GroupKey(0), Aggregate(0)] plus hidden retraction count.
        // sum = 3005000 at scale 4 represents 300.5000 (SUM keeps input scale)
        // count = 2
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("__agg_state_a__sum", DataType::Decimal128(38, 4), true),
                Field::new("__agg_state_a__count", DataType::Int64, false),
                Field::new(AGG_RETRACTION_COUNT_STATE_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(vec![3005000_i128])
                        .with_precision_and_scale(38, 4)
                        .expect("precision/scale"),
                ) as ArrayRef,
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
            ],
        )
        .expect("state-shaped batch");

        let chunk =
            materialize_aggregate_result_batch(&batch, &layout, &shape).expect("materialize");

        // Physical schema: [row_id, k1_visible, a_visible, __agg_state_a__sum, __agg_state_a__count]
        // visible 'a' is at column index 2, state sum is at index 3.
        // Expected: 150.2500000000 at scale 10 => raw i128 = 1502500000000
        let visible_a = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("visible a Decimal128");
        assert_eq!(
            visible_a.value(0),
            1502500000000_i128,
            "visible AVG should be 150.2500000000 (scale 10), \
             i.e. raw i128 = 1502500000000; \
             without fix, got {}",
            visible_a.value(0)
        );

        // Also verify the state column was rescaled: stored value should be 3005000 * 10^6.
        let state_sum = chunk
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("state sum Decimal128");
        assert_eq!(
            state_sum.value(0),
            3005000_000000_i128,
            "state sum must be rescaled from scale-4 to scale-10: 3005000 * 10^6 = 3005000000000"
        );
    }

    /// derive_avg_visible for Decimal128 at uniform scale: sum and visible share the same scale.
    /// This is the normal path after materialize has already rescaled the sum to state scale.
    /// No further rescaling needed inside derive_avg_visible.
    #[test]
    fn derive_avg_visible_decimal128_same_scale() {
        // sum_i128 = 3005000000000 at scale 10 (represents 300.5000000000, after rescaling)
        // count = 2
        // visible_dt = Decimal128(38, 10)
        // Expected: 3005000000000 / 2 = 1502500000000 (represents 150.2500000000 at scale 10)
        let result = derive_avg_visible(
            Some(AggScalarValue::Decimal128(3005000_000000_i128)),
            Some(AggScalarValue::Int64(2)),
            &DataType::Decimal128(38, 10),
        )
        .expect("derive");
        assert!(
            matches!(result, Some(AggScalarValue::Decimal128(1502500000000_i128))),
            "expected Some(Decimal128(1502500000000)), got {result:?}"
        );
    }

    // ---- Bug I1: agg_scalar_values_equal Float64/Decimal256 tests ----

    /// agg_scalar_values_equal must return true for Float64 values that are bit-equal.
    /// Without the fix, this returns false causing validate_loaded_physical_row to fail
    /// with "does not match state column" on the second refresh.
    #[test]
    fn agg_scalar_values_equal_float64_equal_values() {
        assert!(
            agg_scalar_values_equal(
                &Some(AggScalarValue::Float64(1.5)),
                &Some(AggScalarValue::Float64(1.5)),
            ),
            "Float64 equal values must compare equal"
        );
    }

    #[test]
    fn agg_scalar_values_equal_float64_nan_both_nan() {
        // NaN == NaN at bit level (used for NaN preservation in merge)
        assert!(
            agg_scalar_values_equal(
                &Some(AggScalarValue::Float64(f64::NAN)),
                &Some(AggScalarValue::Float64(f64::NAN)),
            ),
            "Float64 NaN == NaN must be bit-equal"
        );
    }

    #[test]
    fn agg_scalar_values_equal_float64_different_values() {
        assert!(
            !agg_scalar_values_equal(
                &Some(AggScalarValue::Float64(1.5)),
                &Some(AggScalarValue::Float64(2.5)),
            ),
            "Float64 different values must not compare equal"
        );
    }

    /// validate_loaded_physical_row succeeds for MIN(Float64) where visible == state.
    /// Bug I1: without Float64 in agg_scalar_values_equal, the equality check returns
    /// false and this fails with "does not match state column" corruption error.
    #[test]
    fn load_aggregate_physical_rows_min_float64_succeeds() {
        use arrow::array::Float64Array;
        // Build a MIN(Float64) layout.
        let layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: vec![AggregateVisibleColumn {
                name: "mn".to_string(),
                data_type: DataType::Float64,
                sql_type: SqlType::Double,
                nullable: true,
                source_index: 0,
            }],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_mn".to_string(),
                data_type: DataType::Float64,
                sql_type: SqlType::Double,
                nullable: true,
                visible_source_index: 0,
                aggregate_index: 0,
                function: AggregateFunctionKind::Min,
                state_role: AggregateStateRole::Single,
                count_star: false,
            }],
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };
        // Physical schema: [row_id, mn_visible, __agg_state_mn_state]
        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.25)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.25)])) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");

        // This must succeed: visible and state are equal Float64 values.
        let rows = load_aggregate_physical_rows(&[chunk], &layout)
            .expect("MIN(Float64) load must succeed");
        assert_eq!(rows.len(), 1);
    }
}
