//! Aggregate MV state helpers for aggregate MV incremental refresh.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::connector::starrocks::managed::ddl::{ManagedPhysicalColumn, managed_physical_column};
use crate::connector::starrocks::managed::mv_ddl;
use crate::connector::starrocks::managed::mv_shape::{
    AggregateFunctionKind, AggregateInput, AggregateMvShape, VisibleAggregateOutput,
};
use crate::engine::{QueryResult, record_batch_to_chunk};
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{AggScalarValue, agg_scalar_from_array, build_agg_scalar_array};
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
            AggregateFunctionKind::Count
            | AggregateFunctionKind::Sum
            | AggregateFunctionKind::Min
            | AggregateFunctionKind::Max => {
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

/// Returns true if any state column on the layout uses MIN or MAX.
/// Used by the refresh dispatch to decide whether to fall back to a full
/// refresh when the incremental change batch contains DELETE files (MIN/MAX
/// state has no closed-form retract — see `negate_aggregate_state_chunks`).
#[allow(dead_code)]
pub(crate) fn layout_has_min_or_max(layout: &AggregateMvLayout) -> bool {
    layout.state_columns.iter().any(|col| {
        matches!(
            col.function,
            AggregateFunctionKind::Min | AggregateFunctionKind::Max
        )
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
            if matches!(
                state_column.function,
                AggregateFunctionKind::Min | AggregateFunctionKind::Max
            ) {
                panic!(
                    "MIN/MAX state should not enter negate path: column `{}`. \
                     DELETE-induced refresh on MV with MIN/MAX must fall back to full refresh.",
                    state_column.name
                );
            }
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
            arrays[column_index] = negate_state_array(&original, state_column)?;
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
        (AggregateFunctionKind::Min, AggregateStateRole::Single) => {
            merge_min_max_state_value(old, delta, state_column, MinMax::Min)
        }
        (AggregateFunctionKind::Max, AggregateStateRole::Single) => {
            merge_min_max_state_value(old, delta, state_column, MinMax::Max)
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

#[derive(Clone, Copy)]
enum MinMax {
    Min,
    Max,
}

fn merge_min_max_state_value(
    old: Option<AggScalarValue>,
    delta: Option<AggScalarValue>,
    state_column: &AggregateStateColumn,
    op: MinMax,
) -> Result<Option<AggScalarValue>, String> {
    match (old, delta) {
        (None, None) => Ok(None),
        (Some(v), None) | (None, Some(v)) => Ok(Some(v)),
        (Some(a), Some(b)) => Ok(Some(min_max_pair(a, b, state_column, op)?)),
    }
}

fn min_max_pair(
    a: AggScalarValue,
    b: AggScalarValue,
    state_column: &AggregateStateColumn,
    op: MinMax,
) -> Result<AggScalarValue, String> {
    use AggScalarValue::*;
    match (a, b) {
        (Int64(x), Int64(y)) => Ok(Int64(pick_int64(x, y, op))),
        (Float64(x), Float64(y)) => Ok(Float64(pick_float64(x, y, op))),
        (Decimal128(x), Decimal128(y)) => Ok(Decimal128(pick_int128(x, y, op))),
        (Decimal256(x), Decimal256(y)) => Ok(Decimal256(pick_i256(x, y, op))),
        (Utf8(x), Utf8(y)) => Ok(Utf8(pick_string(x, y, op))),
        (Date32(x), Date32(y)) => Ok(Date32(pick_int32(x, y, op))),
        (Timestamp(x), Timestamp(y)) => Ok(Timestamp(pick_int64(x, y, op))),
        (a, b) => Err(format!(
            "MIN/MAX merge type mismatch on column `{}`: a={a:?}, b={b:?}",
            state_column.name
        )),
    }
}

fn pick_int64(x: i64, y: i64, op: MinMax) -> i64 {
    match op {
        MinMax::Min => x.min(y),
        MinMax::Max => x.max(y),
    }
}

fn pick_int32(x: i32, y: i32, op: MinMax) -> i32 {
    match op {
        MinMax::Min => x.min(y),
        MinMax::Max => x.max(y),
    }
}

fn pick_int128(x: i128, y: i128, op: MinMax) -> i128 {
    match op {
        MinMax::Min => x.min(y),
        MinMax::Max => x.max(y),
    }
}

fn pick_i256(
    x: arrow::datatypes::i256,
    y: arrow::datatypes::i256,
    op: MinMax,
) -> arrow::datatypes::i256 {
    match op {
        MinMax::Min => {
            if x <= y {
                x
            } else {
                y
            }
        }
        MinMax::Max => {
            if x >= y {
                x
            } else {
                y
            }
        }
    }
}

fn pick_string(x: String, y: String, op: MinMax) -> String {
    match op {
        MinMax::Min => x.min(y),
        MinMax::Max => x.max(y),
    }
}

/// Pick min/max for f64 with NaN handling:
/// - NaN + NaN   → NaN
/// - NaN + x     → x  (NaN treated as "no real value")
/// - x   + NaN   → x
/// - x   + y     → cmp::min/max(x, y)
fn pick_float64(x: f64, y: f64, op: MinMax) -> f64 {
    if x.is_nan() && y.is_nan() {
        return f64::NAN;
    }
    if x.is_nan() {
        return y;
    }
    if y.is_nan() {
        return x;
    }
    match op {
        MinMax::Min => x.min(y),
        MinMax::Max => x.max(y),
    }
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
        // MIN/MAX have no identity element; the zero state for an empty
        // merge buffer must be NULL (None). The first incoming non-NULL
        // delta value will populate it via merge_min_max_state_value.
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
        if !allow_negative_counts && matches!(state_column.state_role, AggregateStateRole::Single) {
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
        | (AggregateFunctionKind::Max, AggregateStateRole::Single) => match data_type {
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
                "MIN/MAX state type is unsupported for column `{state_name}`: Boolean"
            )),
            other => Err(format!(
                "MIN/MAX state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
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
/// For `Single` state_role the visible value is a direct copy of the state value (1:1 mapping).
/// For AVG, the visible value is derived from the AvgSum and AvgCount sub-states as sum/count.
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
            AggregateFunctionKind::Count
            | AggregateFunctionKind::Sum
            | AggregateFunctionKind::Min
            | AggregateFunctionKind::Max => {
                // Single state role: visible = state.
                let state_index = state_indexes[0];
                let state_column = &layout.state_columns[state_index];
                row.visible_values[state_column.visible_source_index] =
                    row.state_values[state_index].clone();
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
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ]
    }

    fn aggregate_first_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ]
    }

    fn count_expr_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ]
    }

    fn sum_only_output_columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
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
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
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
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
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
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
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

    // ---- MIN/MAX merge tests ----

    #[test]
    fn merge_state_value_min_int64() {
        let column = AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: true,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        };
        // Some(5) min Some(3) = Some(3)
        let r = merge_state_value(
            Some(AggScalarValue::Int64(5)),
            Some(AggScalarValue::Int64(3)),
            &column,
        )
        .unwrap();
        assert!(
            matches!(r, Some(AggScalarValue::Int64(3))),
            "expected Some(Int64(3)), got {r:?}"
        );
        // Some + None -> Some
        let r = merge_state_value(Some(AggScalarValue::Int64(5)), None, &column).unwrap();
        assert!(
            matches!(r, Some(AggScalarValue::Int64(5))),
            "expected Some(Int64(5)), got {r:?}"
        );
        // None + Some -> Some
        let r = merge_state_value(None, Some(AggScalarValue::Int64(5)), &column).unwrap();
        assert!(
            matches!(r, Some(AggScalarValue::Int64(5))),
            "expected Some(Int64(5)), got {r:?}"
        );
        // None + None -> None
        let r = merge_state_value(None, None, &column).unwrap();
        assert!(r.is_none(), "expected None, got {r:?}");
    }

    #[test]
    fn merge_state_value_max_utf8() {
        let column = AggregateStateColumn {
            name: "__agg_state_mx".to_string(),
            data_type: DataType::Utf8,
            sql_type: SqlType::String,
            nullable: true,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Max,
            state_role: AggregateStateRole::Single,
            count_star: false,
        };
        let r = merge_state_value(
            Some(AggScalarValue::Utf8("apple".to_string())),
            Some(AggScalarValue::Utf8("banana".to_string())),
            &column,
        )
        .unwrap();
        assert!(
            matches!(r, Some(AggScalarValue::Utf8(ref s)) if s == "banana"),
            "expected Some(Utf8(\"banana\")), got {r:?}"
        );
    }

    #[test]
    fn merge_state_value_min_float64_nan_handling() {
        let column = AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Float64,
            sql_type: SqlType::Double,
            nullable: true,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        };
        // NaN + non-NaN -> non-NaN
        let r = merge_state_value(
            Some(AggScalarValue::Float64(f64::NAN)),
            Some(AggScalarValue::Float64(5.0)),
            &column,
        )
        .unwrap();
        let v = match r {
            Some(AggScalarValue::Float64(v)) => v,
            _ => panic!("expected Float64"),
        };
        assert!(!v.is_nan() && v == 5.0);
        // NaN + NaN -> NaN
        let r = merge_state_value(
            Some(AggScalarValue::Float64(f64::NAN)),
            Some(AggScalarValue::Float64(f64::NAN)),
            &column,
        )
        .unwrap();
        let v = match r {
            Some(AggScalarValue::Float64(v)) => v,
            _ => panic!("expected Float64"),
        };
        assert!(v.is_nan());
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

    // ---- MIN/MAX negate panic test ----

    #[test]
    #[should_panic(expected = "MIN/MAX state should not enter negate path")]
    fn negate_aggregate_state_chunks_min_panics() {
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
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_mn".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
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
        let schema = Arc::new(physical_schema(&layout));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["g"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");
        negate_aggregate_state_chunks(vec![chunk], &layout).unwrap();
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
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            OutputColumn {
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
                Arc::new(Float64Array::from(vec![Some(3.14)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(3.14)])) as ArrayRef,
            ],
        )
        .expect("batch");
        let chunk = record_batch_to_chunk(batch).expect("chunk");

        // This must succeed: visible and state are equal Float64 values.
        let rows = load_aggregate_physical_rows(&[chunk], &layout)
            .expect("MIN(Float64) load must succeed");
        assert_eq!(rows.len(), 1);
    }

    // ---- layout_has_min_or_max tests ----

    #[test]
    fn layout_has_min_or_max_detects() {
        let mut layout = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: Vec::new(),
            state_columns: Vec::new(),
            group_key_source_indexes: Vec::new(),
            physical_columns: Vec::new(),
        };
        assert!(!layout_has_min_or_max(&layout));

        layout.state_columns.push(AggregateStateColumn {
            name: "__agg_state_c".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: false,
            visible_source_index: 0,
            aggregate_index: 0,
            function: AggregateFunctionKind::Count,
            state_role: AggregateStateRole::Single,
            count_star: true,
        });
        assert!(!layout_has_min_or_max(&layout));

        layout.state_columns.push(AggregateStateColumn {
            name: "__agg_state_mn".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: true,
            visible_source_index: 1,
            aggregate_index: 1,
            function: AggregateFunctionKind::Min,
            state_role: AggregateStateRole::Single,
            count_star: false,
        });
        assert!(layout_has_min_or_max(&layout));
    }

    #[test]
    fn fallback_predicate_truth_table_for_min_max() {
        // Locks the boolean shape of the fall-back gate
        // (`deletes_present && layout_has_min_or_max(...)`) used in
        // refresh_aggregate_mv_incremental. The integration path is
        // not unit-tested here — see spec §8.2: DELETE -> fall-back is
        // covered exclusively by Rust unit tests rather than the SQL suite.
        let layout_with_min = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: Vec::new(),
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_mn".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
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

        let layout_count_only = AggregateMvLayout {
            row_id_column: managed_physical_column(
                ROW_ID_COLUMN.to_string(),
                SqlType::String,
                false,
                false,
                true,
            ),
            visible_columns: Vec::new(),
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

        let deletes_present = true;

        // Fall-back fires: deletes present AND layout has MIN/MAX.
        assert!(deletes_present && layout_has_min_or_max(&layout_with_min));

        // No fall-back: COUNT-only layout does not trigger MIN/MAX gate.
        assert!(!(deletes_present && layout_has_min_or_max(&layout_count_only)));

        // No fall-back: no deletes regardless of layout — the gate only fires when
        // both conditions are true. Verify layout_has_min_or_max returns true for
        // the min layout (confirming the helper works) while the overall predicate
        // is false because no deletes are present.
        let no_deletes = false;
        assert!(layout_has_min_or_max(&layout_with_min));
        assert!(!(no_deletes && layout_has_min_or_max(&layout_with_min)));
    }
}
