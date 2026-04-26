//! Aggregate MV state helpers intentionally staged before the following
//! aggregate MV wiring tasks use them.
#![allow(dead_code)]

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
use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{AggScalarValue, agg_scalar_from_array, build_agg_scalar_array};
use crate::sql::analysis::OutputColumn;
use crate::sql::parser::ast::SqlType;
use crate::standalone::engine::{QueryResult, record_batch_to_chunk};

pub(crate) const ROW_ID_COLUMN: &str = "__row_id__";
pub(crate) const AGG_STATE_PREFIX: &str = "__agg_state_";

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub(crate) function: AggregateFunctionKind,
    pub(crate) count_star: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregatePhysicalRow {
    pub(crate) row_id: String,
    pub(crate) visible_values: Vec<Option<AggScalarValue>>,
    pub(crate) state_values: Vec<Option<AggScalarValue>>,
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

    let mut state_columns = Vec::with_capacity(shape.aggregates.len());
    for (aggregate_index, aggregate) in shape.aggregates.iter().enumerate() {
        let visible_source_index = aggregate_visible_source_index(shape, aggregate_index)?;
        let visible = output_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "aggregate MV visible source index out of range: aggregate_index={aggregate_index} source_index={visible_source_index}"
            )
        })?;
        let sql_type = mv_ddl::arrow_data_type_to_sql_type(&visible.data_type)?;
        let state_name = format!(
            "{}{}",
            AGG_STATE_PREFIX,
            sanitize_state_column_name(&aggregate.output_name)
        );
        validate_state_column_type(aggregate.function, &visible.data_type, &state_name)?;
        physical_columns.push(managed_physical_column(
            state_name.clone(),
            sql_type.clone(),
            visible.nullable,
            false,
            false,
        ));
        state_columns.push(AggregateStateColumn {
            name: state_name,
            data_type: visible.data_type.clone(),
            sql_type,
            nullable: visible.nullable,
            visible_source_index,
            function: aggregate.function,
            count_star: matches!(aggregate.input, AggregateInput::Star),
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

pub(crate) fn materialize_aggregate_result_chunks(
    result: QueryResult,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| materialize_aggregate_result_batch(&chunk.batch, layout))
        .collect()
}

pub(crate) fn load_aggregate_physical_rows(
    chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<HashMap<String, AggregatePhysicalRow>, String> {
    let mut rows = HashMap::new();
    for chunk in chunks {
        load_aggregate_physical_rows_from_batch(&chunk.batch, layout, &mut rows)?;
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

pub(crate) fn merge_aggregate_state_batches(
    old_rows: &HashMap<String, AggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    let mut merged = old_rows.clone();
    let delta_rows = load_aggregate_physical_rows(delta_chunks, layout)?;
    for delta in delta_rows.into_values() {
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
        for (state_index, state_column) in layout.state_columns.iter().enumerate() {
            let next_value = merge_state_value(
                row.state_values.get(state_index).cloned().unwrap_or(None),
                delta.state_values.get(state_index).cloned().unwrap_or(None),
                state_column,
            )?;
            row.state_values[state_index] = next_value.clone();
            row.visible_values[state_column.visible_source_index] = next_value;
        }
    }
    physical_rows_to_chunks(merged.into_values().collect(), layout)
}

fn materialize_aggregate_result_batch(
    batch: &RecordBatch,
    layout: &AggregateMvLayout,
) -> Result<Chunk, String> {
    if batch.num_columns() != layout.visible_columns.len() {
        return Err(format!(
            "aggregate MV materialize column count mismatch: batch_columns={} visible_columns={}",
            batch.num_columns(),
            layout.visible_columns.len()
        ));
    }
    let mut arrays =
        Vec::with_capacity(1 + layout.visible_columns.len() + layout.state_columns.len());
    arrays.push(build_row_id_array(batch, &layout.group_key_source_indexes)?);
    arrays.extend(batch.columns().iter().cloned());
    for state_column in &layout.state_columns {
        let array = batch.column(state_column.visible_source_index).clone();
        arrays.push(array);
    }
    let physical_batch = RecordBatch::try_new(Arc::new(physical_schema(layout)), arrays)
        .map_err(|e| format!("build aggregate MV physical batch failed: {e}"))?;
    record_batch_to_chunk(physical_batch)
}

fn build_row_id_array(
    batch: &RecordBatch,
    group_key_indexes: &[usize],
) -> Result<ArrayRef, String> {
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(group_key_indexes.len());
        for &column_index in group_key_indexes {
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
) -> Result<(), String> {
    for row in load_aggregate_physical_rows_from_batch_owned(batch, layout)? {
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
        validate_loaded_physical_row(batch, row, &row_id, &visible_values, &state_values, layout)?;
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
    match state_column.function {
        AggregateFunctionKind::Count => merge_count_state_value(old, delta, state_column),
        AggregateFunctionKind::Sum => merge_sum_state_value(old, delta, state_column),
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
    match state_column.function {
        AggregateFunctionKind::Count => Some(AggScalarValue::Int64(0)),
        AggregateFunctionKind::Sum => None,
    }
}

fn validate_loaded_physical_row(
    batch: &RecordBatch,
    row: usize,
    row_id: &str,
    visible_values: &[Option<AggScalarValue>],
    state_values: &[Option<AggScalarValue>],
    layout: &AggregateMvLayout,
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
        if state_column.function == AggregateFunctionKind::Count {
            validate_loaded_count_state(
                state_value,
                &state_column.name,
                row_id,
                state_column.count_star,
            )?;
        }
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
                layout.visible_columns[state_column.visible_source_index].name, state_column.name
            ));
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
) -> Result<(), String> {
    match state_value {
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
        (Some(AggScalarValue::Utf8(left)), Some(AggScalarValue::Utf8(right))) => left == right,
        (Some(AggScalarValue::Date32(left)), Some(AggScalarValue::Date32(right))) => left == right,
        (Some(AggScalarValue::Timestamp(left)), Some(AggScalarValue::Timestamp(right))) => {
            left == right
        }
        (Some(AggScalarValue::Decimal128(left)), Some(AggScalarValue::Decimal128(right))) => {
            left == right
        }
        _ => false,
    }
}

fn validate_state_column_type(
    function: AggregateFunctionKind,
    data_type: &DataType,
    state_name: &str,
) -> Result<(), String> {
    match function {
        AggregateFunctionKind::Count => match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(()),
            other => Err(format!(
                "aggregate MV COUNT state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
        AggregateFunctionKind::Sum => match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _) => Ok(()),
            other => Err(format!(
                "aggregate MV SUM state type is unsupported for column `{state_name}`: {other:?}"
            )),
        },
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

fn sanitize_state_column_name(name: &str) -> String {
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
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(c)),
            ],
        )
        .expect("batch")
    }

    fn physical_chunks_with_count_state(
        layout: &AggregateMvLayout,
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

    fn physical_chunks_with_bad_row_id(layout: &AggregateMvLayout) -> Vec<Chunk> {
        let mut chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("chunk"),
                ],
            },
            layout,
        )
        .expect("physical");
        let batch = &chunks[0].batch;
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(StringArray::from(vec!["bad-row-id"]));
        let corrupted = RecordBatch::try_new(batch.schema(), columns).expect("corrupted batch");
        chunks[0] = record_batch_to_chunk(corrupted).expect("corrupted chunk");
        chunks
    }

    fn physical_chunks_with_mismatched_sum_state(layout: &AggregateMvLayout) -> Vec<Chunk> {
        let mut chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("chunk"),
                ],
            },
            layout,
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
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let batch = visible_result_batch(vec![1], vec![2], vec![30]);
        let result = QueryResult {
            columns: Vec::new(),
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        };

        let chunks = materialize_aggregate_result_chunks(result, &layout).expect("materialize");
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
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let old = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(visible_result_batch(vec![1], vec![2], vec![30]))
                        .expect("old chunk"),
                ],
            },
            &layout,
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
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
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
        let layout =
            build_aggregate_mv_layout(&aggregate_first_shape(), &aggregate_first_output_columns())
                .expect("layout");
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
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
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
        )
        .expect("old physical");

        let err = load_aggregate_physical_rows(&old, &layout).expect_err("duplicate rejected");
        assert!(err.contains("duplicate row id"), "err={err}");
    }

    #[test]
    fn load_rejects_null_count_state_as_corruption() {
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let chunks = physical_chunks_with_count_state(&layout, None);

        let err = load_aggregate_physical_rows(&chunks, &layout).expect_err("null count rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("COUNT"), "err={err}");
        assert!(err.contains("NULL"), "err={err}");
    }

    #[test]
    fn load_rejects_zero_count_state_as_corruption() {
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let chunks = physical_chunks_with_count_state(&layout, Some(0));

        let err = load_aggregate_physical_rows(&chunks, &layout).expect_err("zero count rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("COUNT"), "err={err}");
        assert!(err.contains("state"), "err={err}");
    }

    #[test]
    fn load_allows_zero_count_expr_state() {
        let layout = build_aggregate_mv_layout(&count_expr_shape(), &count_expr_output_columns())
            .expect("layout");
        let chunks = materialize_aggregate_result_chunks(
            QueryResult {
                columns: Vec::new(),
                chunks: vec![
                    record_batch_to_chunk(count_expr_result_batch(vec![1], vec![0]))
                        .expect("chunk"),
                ],
            },
            &layout,
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
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let chunks = physical_chunks_with_bad_row_id(&layout);

        let err =
            load_aggregate_physical_rows(&chunks, &layout).expect_err("row id mismatch rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("row id"), "err={err}");
        assert!(err.contains("visible group key"), "err={err}");
    }

    #[test]
    fn load_rejects_visible_aggregate_state_mismatch_as_corruption() {
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
        let chunks = physical_chunks_with_mismatched_sum_state(&layout);

        let err = load_aggregate_physical_rows(&chunks, &layout)
            .expect_err("visible/state mismatch rejected");
        assert!(err.contains("corruption"), "err={err}");
        assert!(err.contains("visible aggregate"), "err={err}");
        assert!(err.contains("state"), "err={err}");
    }

    #[test]
    fn merge_rejects_duplicate_delta_row_id() {
        let layout = build_aggregate_mv_layout(&test_shape(), &output_columns()).expect("layout");
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
        )
        .expect("delta physical");

        let err = merge_aggregate_state_batches(&HashMap::new(), &delta, &layout)
            .expect_err("duplicate delta rejected");
        assert!(err.contains("duplicate row id"), "err={err}");
    }
}
