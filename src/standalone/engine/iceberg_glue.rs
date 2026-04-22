//! Glue between the iceberg catalog (`crate::standalone::iceberg`) and the
//! standalone query path: load an iceberg table as a single RecordBatch,
//! apply aggregate-table semantics if needed, and normalize the inbound
//! batch schema so it matches the table's declared column ordering.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;

use crate::standalone::engine::block_on_standalone_async;
use crate::standalone::engine::local::aggregate::merge_aggregate_table_rows_if_needed;
use crate::standalone::engine::local::{ColumnDef, normalize_identifier};
use crate::standalone::iceberg::{IcebergLoadedTable, build_insert_batch};

pub(crate) fn load_full_iceberg_batch(loaded: &IcebergLoadedTable) -> Result<RecordBatch, String> {
    let batches = block_on_standalone_async(async {
        loaded
            .table
            .scan()
            .build()
            .map_err(|e| format!("build iceberg scan failed: {e}"))?
            .to_arrow()
            .await
            .map_err(|e| format!("open iceberg arrow stream failed: {e}"))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("read iceberg scan batches failed: {e}"))
    })??;
    let normalized_batches = batches
        .into_iter()
        .map(|batch| normalize_iceberg_source_batch(batch, &loaded.columns))
        .collect::<Result<Vec<_>, _>>()?;
    let combined = concat_or_empty_batches(&loaded.columns, normalized_batches)?;
    apply_iceberg_table_semantics_if_needed(loaded, combined)
}

pub(crate) fn apply_iceberg_table_semantics_if_needed(
    loaded: &IcebergLoadedTable,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    let Some(merged_rows) = merge_aggregate_table_rows_if_needed(
        &loaded.columns,
        loaded.key_desc.as_ref(),
        &loaded.column_aggregations,
        &batch,
    )?
    else {
        return Ok(batch);
    };
    build_insert_batch(loaded, &merged_rows)
}

pub(crate) fn normalize_iceberg_source_batch(
    batch: RecordBatch,
    columns: &[ColumnDef],
) -> Result<RecordBatch, String> {
    let field_indices = iceberg_field_indices(&batch)?;
    let arrays = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, _)| batch.column(idx).clone())
        .collect::<Vec<_>>();
    let arrays = columns
        .iter()
        .map(|column| {
            let normalized = normalize_identifier(&column.name)
                .map_err(|e| format!("normalize source column `{}` failed: {e}", column.name))?;
            let batch_idx = field_indices
                .get(&normalized)
                .copied()
                .ok_or_else(|| format!("iceberg source batch missing column `{}`", column.name))?;
            normalize_iceberg_array_type(&arrays[batch_idx], &column.name, &column.data_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("rebuild normalized iceberg source batch failed: {e}"))
}

fn iceberg_field_indices(batch: &RecordBatch) -> Result<HashMap<String, usize>, String> {
    let mut indices = HashMap::with_capacity(batch.num_columns());
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized = normalize_identifier(field.name()).map_err(|e| {
            format!(
                "normalize iceberg batch column name `{}` failed: {e}",
                field.name()
            )
        })?;
        if indices.insert(normalized.clone(), idx).is_some() {
            return Err(format!(
                "duplicate iceberg batch column `{}` after normalization",
                field.name()
            ));
        }
    }
    Ok(indices)
}

fn normalize_iceberg_array_type(
    array: &ArrayRef,
    column_name: &str,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    arrow::compute::cast(array, target_type).map_err(|e| {
        format!(
            "cast iceberg column `{column_name}` from {:?} to {:?} failed: {e}",
            array.data_type(),
            target_type
        )
    })
}

pub(crate) fn concat_or_empty_batches(
    columns: &[ColumnDef],
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, String> {
    if let Some(first) = batches.first() {
        arrow::compute::concat_batches(&first.schema(), batches.iter())
            .map_err(|e| format!("concat standalone batches failed: {e}"))
    } else {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns
            .iter()
            .map(|column| arrow::array::new_empty_array(&column.data_type))
            .collect::<Vec<_>>();
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("build empty standalone batch failed: {e}"))
    }
}
