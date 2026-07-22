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

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::exec::change_op::{ChangeOp, change_op_array, change_op_field};
use crate::exec::chunk::Chunk;
use crate::mv::aggregate_state::mv_agg_state::{
    AggregateMvLayout, build_old_state_map, merge_aggregate_state_batches_with_retractions,
};
use crate::mv::refresh::execution_context::MvRefreshPruningLimits;
use crate::runtime::query_result::record_batch_to_chunk;

pub(crate) struct IcebergAggregateMergeResult {
    pub(crate) delete_row_ids: Vec<String>,
    pub(crate) insert_chunks: Vec<Chunk>,
    pub(crate) new_total_rows: i64,
}

struct AggregateStateMergeCoreResult {
    old_row_ids: BTreeSet<String>,
    touched_row_ids: BTreeSet<String>,
    upsert_chunks: Vec<Chunk>,
    new_total_rows: i64,
}

pub(crate) fn merge_aggregate_target_state(
    layout: &AggregateMvLayout,
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
) -> Result<IcebergAggregateMergeResult, String> {
    let core = merge_aggregate_state_chunks_core(
        old_chunks,
        delta_chunks,
        layout,
        MvRefreshPruningLimits::default(),
    )?;
    let insert_chunks =
        filter_physical_chunks_by_row_ids(layout, &core.upsert_chunks, &core.touched_row_ids)?;
    let delete_row_ids = core
        .touched_row_ids
        .iter()
        .filter(|row_id| core.old_row_ids.contains(*row_id))
        .cloned()
        .collect();

    Ok(IcebergAggregateMergeResult {
        delete_row_ids,
        insert_chunks,
        new_total_rows: core.new_total_rows,
    })
}

pub(crate) fn merge_aggregate_state_chunks_for_change_stream(
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    merge_aggregate_state_chunks_for_change_stream_with_pruning_limits(
        old_chunks,
        delta_chunks,
        layout,
        MvRefreshPruningLimits::default(),
    )
}

pub(crate) fn merge_aggregate_state_chunks_for_change_stream_with_pruning_limits(
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
    pruning_limits: MvRefreshPruningLimits,
) -> Result<Vec<Chunk>, String> {
    let core = merge_aggregate_state_chunks_core(old_chunks, delta_chunks, layout, pruning_limits)?;
    build_aggregate_change_stream_chunks(
        layout,
        old_chunks,
        &core.upsert_chunks,
        &core.touched_row_ids,
    )
}

fn merge_aggregate_state_chunks_core(
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
    pruning_limits: MvRefreshPruningLimits,
) -> Result<AggregateStateMergeCoreResult, String> {
    let touched_row_ids = delta_row_ids(layout, delta_chunks)?;
    let old_row_ids = physical_row_ids(layout, old_chunks)?;
    let touched_old_chunks = if pruning_limits
        .touched_group_count_exceeds_limit(touched_row_ids.len())
    {
        tracing::warn!(
            touched_group_count = touched_row_ids.len(),
            max_touched_groups = pruning_limits.max_touched_groups,
            fallback_reason = "touched_group_threshold",
            "falling back to full aggregate old-state merge because touched group count exceeds configured threshold"
        );
        old_chunks.to_vec()
    } else {
        filter_physical_chunks_by_row_ids(layout, old_chunks, &touched_row_ids)?
    };
    let old_rows = build_old_state_map(&touched_old_chunks, layout)?;
    let merge_result =
        merge_aggregate_state_batches_with_retractions(&old_rows, delta_chunks, layout)?;
    let upsert_chunks =
        filter_physical_chunks_by_row_ids(layout, &merge_result.upsert_chunks, &touched_row_ids)?;
    let replaced_old_count = touched_row_ids
        .iter()
        .filter(|row_id| old_row_ids.contains(*row_id))
        .count();
    let insert_row_count = upsert_chunks
        .iter()
        .map(|chunk| chunk.batch.num_rows())
        .sum::<usize>();
    let new_total_rows = old_row_ids
        .len()
        .checked_sub(replaced_old_count)
        .and_then(|count| count.checked_add(insert_row_count))
        .ok_or_else(|| "iceberg aggregate MV target row count overflow".to_string())?;
    let new_total_rows = i64::try_from(new_total_rows)
        .map_err(|_| "iceberg aggregate MV target row count overflow".to_string())?;
    Ok(AggregateStateMergeCoreResult {
        old_row_ids,
        touched_row_ids,
        upsert_chunks,
        new_total_rows,
    })
}

pub(crate) fn build_aggregate_change_chunks(
    layout: &AggregateMvLayout,
    merge: IcebergAggregateMergeResult,
) -> Result<Vec<Chunk>, String> {
    let mut chunks = Vec::new();
    if !merge.delete_row_ids.is_empty() {
        let row_count = merge.delete_row_ids.len();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(&layout.row_id_column.column.name, DataType::Utf8, false),
                change_op_field(),
            ])),
            vec![
                Arc::new(StringArray::from(merge.delete_row_ids)) as ArrayRef,
                change_op_array(ChangeOp::Delete, row_count),
            ],
        )
        .map_err(|e| format!("build iceberg aggregate DELETE change chunk failed: {e}"))?;
        chunks.push(record_batch_to_chunk(batch)?);
    }

    for insert_chunk in merge.insert_chunks {
        let batch = insert_chunk.batch;
        validate_physical_aggregate_schema(layout, &batch, "iceberg aggregate insert chunk")?;
        let row_count = batch.num_rows();
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(change_op_field());
        let mut columns = batch.columns().to_vec();
        columns.push(change_op_array(ChangeOp::Insert, row_count));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| format!("build iceberg aggregate INSERT change chunk failed: {e}"))?;
        chunks.push(record_batch_to_chunk(batch)?);
    }

    Ok(chunks)
}

fn build_aggregate_change_stream_chunks(
    layout: &AggregateMvLayout,
    old_chunks: &[Chunk],
    upsert_chunks: &[Chunk],
    touched_row_ids: &BTreeSet<String>,
) -> Result<Vec<Chunk>, String> {
    let mut chunks = Vec::new();
    let delete_chunks = filter_physical_chunks_by_row_ids(layout, old_chunks, touched_row_ids)?;
    for delete_chunk in delete_chunks {
        chunks.push(append_change_op_to_physical_chunk(
            layout,
            delete_chunk,
            ChangeOp::Delete,
            "aggregate state merge DELETE change chunk",
        )?);
    }

    let insert_chunks = filter_physical_chunks_by_row_ids(layout, upsert_chunks, touched_row_ids)?;
    for insert_chunk in insert_chunks {
        chunks.push(append_change_op_to_physical_chunk(
            layout,
            insert_chunk,
            ChangeOp::Insert,
            "aggregate state merge INSERT change chunk",
        )?);
    }
    Ok(chunks)
}

fn append_change_op_to_physical_chunk(
    layout: &AggregateMvLayout,
    chunk: Chunk,
    op: ChangeOp,
    context: &str,
) -> Result<Chunk, String> {
    let batch = chunk.batch;
    validate_physical_aggregate_schema(layout, &batch, context)?;
    let row_count = batch.num_rows();
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(change_op_field());
    let mut columns = batch.columns().to_vec();
    columns.push(change_op_array(op, row_count));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build {context} failed: {e}"))?;
    record_batch_to_chunk(batch)
}

fn validate_physical_aggregate_schema(
    layout: &AggregateMvLayout,
    batch: &RecordBatch,
    context: &str,
) -> Result<(), String> {
    if batch.num_columns() < layout.physical_columns.len() {
        return Err(format!(
            "{context}: physical aggregate schema column count mismatch: got {} expected at least {}",
            batch.num_columns(),
            layout.physical_columns.len()
        ));
    }

    let schema = batch.schema();
    for (idx, expected_column) in layout.physical_columns.iter().enumerate() {
        let actual = schema.field(idx);
        let expected_name = &expected_column.column.name;
        if actual.name() != expected_name {
            return Err(format!(
                "{context}: physical aggregate schema column {idx} name mismatch: got `{}` expected `{expected_name}`",
                actual.name()
            ));
        }
        let expected_type =
            crate::sql::literal::sql_type_to_arrow_type(&expected_column.column.data_type)
                .map_err(|e| {
                    format!(
                        "{context}: convert expected physical aggregate column `{expected_name}` type failed: {e}"
                    )
                })?;
        // Use a metadata-ignoring shape comparison instead of strict `!=`.
        // Map<K, V> columns scanned from Iceberg parquet carry
        // `PARQUET:field_id` metadata on inner Struct fields that the
        // layout-derived `expected_type` does not have, and the Iceberg map
        // convention uses non-null inner key fields while the
        // `sql_type_to_arrow_type`-derived expected uses nullable inner key
        // fields. Both are semantically the same shape. Top-level column
        // nullability is still enforced by the `is_nullable` check below.
        let type_matches = crate::sql::literal::arrow_type_equals_ignoring_metadata(
            actual.data_type(),
            &expected_type,
        ) || matches!(
            (actual.data_type(), &expected_type),
            (DataType::LargeBinary, DataType::Binary) | (DataType::Binary, DataType::LargeBinary)
        );
        if !type_matches {
            return Err(format!(
                "{context}: physical aggregate schema column {idx} `{expected_name}` type mismatch: got {:?} expected {:?}",
                actual.data_type(),
                expected_type
            ));
        }
        if actual.is_nullable() != expected_column.column.nullable {
            return Err(format!(
                "{context}: physical aggregate schema column {idx} `{expected_name}` nullability mismatch: got {} expected {}",
                actual.is_nullable(),
                expected_column.column.nullable
            ));
        }
    }
    Ok(())
}

fn delta_row_ids(
    layout: &AggregateMvLayout,
    delta_chunks: &[Chunk],
) -> Result<BTreeSet<String>, String> {
    let mut row_ids = BTreeSet::new();
    let row_id_column = &layout.row_id_column.column.name;
    for chunk in delta_chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!("iceberg aggregate delta missing row id column `{row_id_column}`: {e}")
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!("iceberg aggregate delta row id column `{row_id_column}` must be Utf8")
            })?;
        for row in 0..row_id_array.len() {
            if row_id_array.is_null(row) {
                return Err(format!(
                    "iceberg aggregate delta row id column `{row_id_column}` cannot be NULL"
                ));
            }
            row_ids.insert(row_id_array.value(row).to_string());
        }
    }
    Ok(row_ids)
}

fn physical_row_ids(
    layout: &AggregateMvLayout,
    chunks: &[Chunk],
) -> Result<BTreeSet<String>, String> {
    let mut row_ids = BTreeSet::new();
    let row_id_column = &layout.row_id_column.column.name;
    for chunk in chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!("iceberg aggregate physical chunk missing row id column `{row_id_column}`: {e}")
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!("iceberg aggregate physical row id column `{row_id_column}` must be Utf8")
            })?;
        for row in 0..row_id_array.len() {
            if row_id_array.is_null(row) {
                return Err(format!(
                    "iceberg aggregate physical row id column `{row_id_column}` cannot be NULL"
                ));
            }
            row_ids.insert(row_id_array.value(row).to_string());
        }
    }
    Ok(row_ids)
}

fn filter_physical_chunks_by_row_ids(
    layout: &AggregateMvLayout,
    chunks: &[Chunk],
    row_ids: &BTreeSet<String>,
) -> Result<Vec<Chunk>, String> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let row_id_column = &layout.row_id_column.column.name;
    let mut out = Vec::new();
    for chunk in chunks {
        let schema = chunk.batch.schema();
        let row_id_index = schema.index_of(row_id_column).map_err(|e| {
            format!("iceberg aggregate physical chunk missing row id column `{row_id_column}`: {e}")
        })?;
        let row_id_array = chunk
            .batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!("iceberg aggregate physical row id column `{row_id_column}` must be Utf8")
            })?;
        let keep = (0..row_id_array.len())
            .map(|row| {
                if row_id_array.is_null(row) {
                    false
                } else {
                    row_ids.contains(row_id_array.value(row))
                }
            })
            .collect::<Vec<_>>();
        if !keep.iter().any(|keep| *keep) {
            continue;
        }
        let filter = BooleanArray::from(keep);
        let columns = chunk
            .batch
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::filter(column.as_ref(), &filter)
                    .map_err(|e| format!("filter iceberg aggregate physical chunk failed: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filtered = RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("rebuild iceberg aggregate physical chunk failed: {e}"))?;
        out.push(record_batch_to_chunk(filtered)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, LargeBinaryBuilder, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::mv::aggregate_state::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateVisibleColumn,
    };
    use crate::mv::aggregate_state::physical_column::starrocks_physical_column;
    use crate::mv::aggregate_state::state_codec::encode_count_state;
    use crate::mv::model::{AggregateFunctionKind, AggregateStateRole};
    use novarocks_catalog::schema::SqlType;

    fn chunk(batch: RecordBatch) -> crate::exec::chunk::Chunk {
        record_batch_to_chunk(batch).expect("chunk")
    }

    fn encoded_utf8_group_row_id(value: &str) -> String {
        format!("utf8:V:{value}")
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn test_count_layout() -> AggregateMvLayout {
        let row_id_column = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let region_column =
            starrocks_physical_column("region".to_string(), SqlType::String, true, true, false);
        let count_column =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let count_state_column = starrocks_physical_column(
            "__agg_state_c".to_string(),
            SqlType::Binary,
            false,
            false,
            false,
        );

        AggregateMvLayout {
            row_id_column: row_id_column.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: "region".to_string(),
                    data_type: DataType::Utf8,
                    sql_type: SqlType::String,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    source_index: 1,
                },
            ],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::LargeBinary,
                sql_type: SqlType::Binary,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            aggregate_input_types: vec![None],
            group_key_source_indexes: vec![0],
            physical_columns: vec![
                row_id_column,
                region_column,
                count_column,
                count_state_column,
            ],
        }
    }

    fn count_physical_batch(rows: &[(&str, &str, i64, i64)]) -> RecordBatch {
        let mut state_builder = LargeBinaryBuilder::new();
        for row in rows {
            state_builder.append_value(encode_count_state(row.3));
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(state_builder.finish()) as ArrayRef,
            ],
        )
        .expect("physical batch")
    }

    fn count_physical_batch_with_positions(
        rows: &[(&str, &str, i64, i64, &str, i64)],
    ) -> RecordBatch {
        let mut state_builder = LargeBinaryBuilder::new();
        for row in rows {
            state_builder.append_value(encode_count_state(row.3));
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
                Field::new(
                    crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                    DataType::Utf8,
                    false,
                ),
                Field::new(
                    crate::exec::row_position::ICEBERG_ROW_POS_COL,
                    DataType::Int64,
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(state_builder.finish()) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.4).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.5).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("physical batch with positions")
    }

    fn count_physical_batch_with_raw_state(rows: &[(&str, &str, i64, &[u8])]) -> RecordBatch {
        let mut state_builder = LargeBinaryBuilder::new();
        for row in rows {
            state_builder.append_value(row.3);
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(state_builder.finish()) as ArrayRef,
            ],
        )
        .expect("physical batch")
    }

    fn assert_field(field: &Field, name: &str, data_type: &DataType, nullable: bool) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type);
        assert_eq!(field.is_nullable(), nullable);
    }

    fn row_ids_from_chunks(chunks: &[crate::exec::chunk::Chunk]) -> Vec<String> {
        let mut row_ids = Vec::new();
        for chunk in chunks {
            let row_id_array = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("row id");
            row_ids.extend((0..row_id_array.len()).map(|row| row_id_array.value(row).to_string()));
        }
        row_ids.sort();
        row_ids
    }

    #[test]
    fn merge_result_marks_replaced_and_removed_groups() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let r2 = encoded_utf8_group_row_id("r2");
        let r3 = encoded_utf8_group_row_id("r3");
        let old = vec![chunk(count_physical_batch(&[
            (r1.as_str(), "r1", 2, 2),
            (r2.as_str(), "r2", 1, 1),
        ]))];
        let delta = vec![chunk(count_physical_batch(&[
            (r1.as_str(), "r1", 1, 1),
            (r2.as_str(), "r2", -1, -1),
            (r3.as_str(), "r3", 5, 5),
        ]))];

        let result = merge_aggregate_target_state(&layout, &old, &delta).expect("merge");

        assert_eq!(result.delete_row_ids, vec![r1.to_string(), r2.to_string()]);
        assert_eq!(
            result
                .insert_chunks
                .iter()
                .map(|c| c.batch.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(result.new_total_rows, 2);
    }

    #[test]
    fn build_change_chunks_emits_delete_and_insert_contracts() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let r3 = encoded_utf8_group_row_id("r3");
        let merge = IcebergAggregateMergeResult {
            delete_row_ids: vec![r1.clone()],
            insert_chunks: vec![chunk(count_physical_batch(&[(r3.as_str(), "r3", 5, 5)]))],
            new_total_rows: 1,
        };

        let chunks = build_aggregate_change_chunks(&layout, merge).expect("change chunks");

        assert_eq!(chunks.len(), 2);
        let delete_batch = &chunks[0].batch;
        assert_eq!(delete_batch.num_columns(), 2);
        assert_field(
            delete_batch.schema().field(0),
            "__row_id__",
            &DataType::Utf8,
            false,
        );
        assert_field(
            delete_batch.schema().field(1),
            "__change_op",
            &DataType::Int8,
            false,
        );
        let delete_ops = delete_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("delete op");
        assert_eq!(
            delete_ops.value(0),
            crate::exec::change_op::CHANGE_OP_DELETE
        );

        let insert_batch = &chunks[1].batch;
        let insert_schema = insert_batch.schema();
        let fields = insert_schema.fields();
        assert_eq!(fields.len(), 5);
        assert_field(fields[0].as_ref(), "__row_id__", &DataType::Utf8, false);
        assert_field(fields[1].as_ref(), "region", &DataType::Utf8, true);
        assert_field(fields[2].as_ref(), "c", &DataType::Int64, false);
        assert_field(
            fields[3].as_ref(),
            "__agg_state_c",
            &DataType::LargeBinary,
            false,
        );
        assert_field(fields[4].as_ref(), "__change_op", &DataType::Int8, false);
        let insert_ops = insert_batch
            .column(4)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("insert op");
        assert_eq!(
            insert_ops.value(0),
            crate::exec::change_op::CHANGE_OP_INSERT
        );
    }

    #[test]
    fn change_stream_delete_rows_preserve_old_file_pos_metadata() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let old = vec![chunk(count_physical_batch_with_positions(&[(
            r1.as_str(),
            "r1",
            2,
            2,
            "s3://bucket/table/data-1.parquet",
            42,
        )]))];
        let delta = vec![chunk(count_physical_batch(&[(r1.as_str(), "r1", 1, 1)]))];

        let chunks = merge_aggregate_state_chunks_for_change_stream(&old, &delta, &layout)
            .expect("change stream");

        assert_eq!(chunks.len(), 2);
        let delete_batch = &chunks[0].batch;
        let delete_fields = delete_batch.schema().fields().clone();
        assert_eq!(delete_fields.len(), 7);
        assert_field(
            delete_fields[0].as_ref(),
            "__row_id__",
            &DataType::Utf8,
            false,
        );
        assert_field(delete_fields[1].as_ref(), "region", &DataType::Utf8, true);
        assert_field(delete_fields[2].as_ref(), "c", &DataType::Int64, false);
        assert_field(
            delete_fields[3].as_ref(),
            "__agg_state_c",
            &DataType::LargeBinary,
            false,
        );
        assert_field(
            delete_fields[4].as_ref(),
            crate::exec::row_position::ICEBERG_FILE_PATH_COL,
            &DataType::Utf8,
            false,
        );
        assert_field(
            delete_fields[5].as_ref(),
            crate::exec::row_position::ICEBERG_ROW_POS_COL,
            &DataType::Int64,
            false,
        );
        assert_field(
            delete_fields[6].as_ref(),
            "__change_op",
            &DataType::Int8,
            false,
        );
        let file_values = delete_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file path column");
        let pos_values = delete_batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("position column");
        assert_eq!(file_values.value(0), "s3://bucket/table/data-1.parquet");
        assert_eq!(pos_values.value(0), 42);
        let delete_ops = delete_batch
            .column(6)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("delete op");
        assert_eq!(
            delete_ops.value(0),
            crate::exec::change_op::CHANGE_OP_DELETE
        );

        let insert_batch = &chunks[1].batch;
        assert_eq!(
            insert_batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["__row_id__", "region", "c", "__agg_state_c", "__change_op"]
        );
    }

    #[test]
    fn delta_row_ids_rejects_missing_non_utf8_and_null_row_id() {
        let layout = test_count_layout();

        let missing = chunk(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "region",
                    DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(StringArray::from(vec!["r1"])) as ArrayRef],
            )
            .expect("missing batch"),
        );
        let err = delta_row_ids(&layout, &[missing]).expect_err("missing row id");
        assert!(err.contains("missing row id column"), "err={err}");

        let non_utf8 = chunk(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "__row_id__",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
            )
            .expect("non utf8 batch"),
        );
        let err = delta_row_ids(&layout, &[non_utf8]).expect_err("non utf8 row id");
        assert!(err.contains("must be Utf8"), "err={err}");

        let null_row_id = chunk(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "__row_id__",
                    DataType::Utf8,
                    true,
                )])),
                vec![Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef],
            )
            .expect("null row id batch"),
        );
        let err = delta_row_ids(&layout, &[null_row_id]).expect_err("null row id");
        assert!(err.contains("cannot be NULL"), "err={err}");
    }

    #[test]
    fn merge_result_filters_untouched_groups_but_counts_full_state() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let r2 = encoded_utf8_group_row_id("r2");
        let r3 = encoded_utf8_group_row_id("r3");
        let old = vec![chunk(count_physical_batch(&[
            (r1.as_str(), "r1", 2, 2),
            (r2.as_str(), "r2", 7, 7),
        ]))];
        let delta = vec![chunk(count_physical_batch(&[
            (r1.as_str(), "r1", 1, 1),
            (r3.as_str(), "r3", 5, 5),
        ]))];

        let result = merge_aggregate_target_state(&layout, &old, &delta).expect("merge");

        assert_eq!(result.new_total_rows, 3);
        assert_eq!(result.delete_row_ids, vec![r1.clone()]);
        assert_eq!(row_ids_from_chunks(&result.insert_chunks), vec![r1, r3]);
    }

    #[test]
    fn merge_filters_old_state_by_delta_row_ids_before_decoding_state_bytes() {
        let layout = test_count_layout();
        let touched = encoded_utf8_group_row_id("touched");
        let untouched = encoded_utf8_group_row_id("untouched");
        let valid_state = encode_count_state(2);
        let invalid_state = b"not-a-valid-count-state";
        let old = vec![chunk(count_physical_batch_with_raw_state(&[
            (touched.as_str(), "touched", 2, valid_state.as_slice()),
            (
                untouched.as_str(),
                "untouched",
                99,
                invalid_state.as_slice(),
            ),
        ]))];
        let delta = vec![chunk(count_physical_batch(&[(
            touched.as_str(),
            "touched",
            1,
            1,
        )]))];

        let result = merge_aggregate_target_state(&layout, &old, &delta)
            .expect("untouched invalid old state must not be decoded");

        assert_eq!(result.delete_row_ids, vec![touched.clone()]);
        assert_eq!(row_ids_from_chunks(&result.insert_chunks), vec![touched]);
        assert_eq!(result.new_total_rows, 2);
    }

    #[test]
    fn merge_over_touched_group_threshold_uses_full_old_state() {
        let layout = test_count_layout();
        let touched = encoded_utf8_group_row_id("touched");
        let another_touched = encoded_utf8_group_row_id("another_touched");
        let untouched = encoded_utf8_group_row_id("untouched");
        let valid_state = encode_count_state(2);
        let invalid_state = b"not-a-valid-count-state";
        let old = vec![chunk(count_physical_batch_with_raw_state(&[
            (touched.as_str(), "touched", 2, valid_state.as_slice()),
            (
                untouched.as_str(),
                "untouched",
                99,
                invalid_state.as_slice(),
            ),
        ]))];
        let delta = vec![chunk(count_physical_batch(&[
            (touched.as_str(), "touched", 1, 1),
            (another_touched.as_str(), "another_touched", 5, 5),
        ]))];

        let err = merge_aggregate_state_chunks_for_change_stream_with_pruning_limits(
            &old,
            &delta,
            &layout,
            crate::mv::refresh::execution_context::MvRefreshPruningLimits {
                max_touched_groups: 1,
                max_affected_partitions: 4_096,
            },
        )
        .expect_err("over-threshold merge should decode full old state");

        assert!(
            err.contains("aggregate MV state corruption"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_change_chunks_rejects_invalid_insert_physical_schema() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let valid = count_physical_batch(&[(r1.as_str(), "r1", 1, 1)]);
        let wrong_order = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("c", DataType::Int64, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            vec![
                valid.column(0).clone(),
                valid.column(2).clone(),
                valid.column(1).clone(),
                valid.column(3).clone(),
            ],
        )
        .expect("wrong order batch");
        let merge = IcebergAggregateMergeResult {
            delete_row_ids: Vec::new(),
            insert_chunks: vec![chunk(wrong_order)],
            new_total_rows: 1,
        };

        let err = build_aggregate_change_chunks(&layout, merge)
            .expect_err("invalid insert schema rejected");

        assert!(err.contains("insert chunk"), "err={err}");
        assert!(err.contains("expected `region`"), "err={err}");
    }

    #[test]
    fn validate_physical_schema_rejects_wrong_order_type_and_nullability() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let valid = count_physical_batch(&[(r1.as_str(), "r1", 1, 1)]);
        validate_physical_aggregate_schema(&layout, &valid, "test valid").expect("valid schema");

        let wrong_order = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("c", DataType::Int64, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            vec![
                valid.column(0).clone(),
                valid.column(2).clone(),
                valid.column(1).clone(),
                valid.column(3).clone(),
            ],
        )
        .expect("wrong order batch");
        let err = validate_physical_aggregate_schema(&layout, &wrong_order, "wrong order")
            .expect_err("wrong order rejected");
        assert!(err.contains("column 1"), "err={err}");
        assert!(err.contains("expected `region`"), "err={err}");

        let wrong_type = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int32, false),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            vec![
                valid.column(0).clone(),
                valid.column(1).clone(),
                Arc::new(arrow::array::Int32Array::from(vec![1])) as ArrayRef,
                valid.column(3).clone(),
            ],
        )
        .expect("wrong type batch");
        let err = validate_physical_aggregate_schema(&layout, &wrong_type, "wrong type")
            .expect_err("wrong type rejected");
        assert!(err.contains("type mismatch"), "err={err}");

        let wrong_nullability = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, false),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
            ])),
            valid.columns().to_vec(),
        )
        .expect("wrong nullability batch");
        let err =
            validate_physical_aggregate_schema(&layout, &wrong_nullability, "wrong nullability")
                .expect_err("wrong nullability rejected");
        assert!(err.contains("nullability mismatch"), "err={err}");
    }

    /// Regression test for IVM-P5 four-bug-chain bug #4: Map<K, V> columns
    /// scanned from Iceberg parquet carry `PARQUET:field_id` metadata on
    /// the inner Struct fields, the Iceberg map convention names the
    /// entries field `key_value`, and the inner key is non-nullable.
    /// `validate_physical_aggregate_schema` must accept this shape against a
    /// layout-derived expected type that uses `sql_type_to_arrow_type`'s
    /// "entries" + nullable-inner-key convention with no metadata.
    ///
    /// The test rebuilds a synthetic batch whose Map column carries
    /// PARQUET:field_id metadata on inner Struct fields, then casts the
    /// Map array values into the field-id-annotated type so the batch is
    /// constructable. Validation must succeed.
    #[test]
    fn validate_physical_schema_accepts_min_max_state_map_with_field_id_metadata() {
        use std::collections::HashMap;

        let row_id_column = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let region_column =
            starrocks_physical_column("region".to_string(), SqlType::String, true, true, false);
        let mn_column =
            starrocks_physical_column("mn".to_string(), SqlType::BigInt, true, true, false);
        let state_column = starrocks_physical_column(
            "__agg_state_mn".to_string(),
            SqlType::Map(Box::new(SqlType::BigInt), Box::new(SqlType::BigInt)),
            false,
            false,
            false,
        );

        let layout = AggregateMvLayout {
            row_id_column: row_id_column.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: "region".to_string(),
                    data_type: DataType::Utf8,
                    sql_type: SqlType::String,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "mn".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: true,
                    source_index: 1,
                },
            ],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_mn".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Min,
                state_role: AggregateStateRole::Single,
                count_star: false,
            }],
            aggregate_input_types: vec![Some(DataType::Int64)],
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id_column, region_column, mn_column, state_column],
        };

        // Step 1: build an empty Map via the standard MapBuilder using
        // Iceberg-rust 0.9 convention (entries field name "key_value",
        // non-null inner key).
        let mut builder = arrow::array::MapBuilder::new(
            Some(arrow::array::MapFieldNames {
                entry: "key_value".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            arrow::array::Int64Builder::new(),
            arrow::array::Int64Builder::new(),
        );
        builder.append(true).expect("append empty map row");
        let raw_map = builder.finish();

        // Step 2: synthesize a MapArray whose inner Struct Fields carry
        // PARQUET:field_id metadata, by reconstructing the MapArray from
        // its children using a Field with the new metadata.
        let mut key_meta = HashMap::new();
        key_meta.insert("PARQUET:field_id".to_string(), "9".to_string());
        let mut value_meta = HashMap::new();
        value_meta.insert("PARQUET:field_id".to_string(), "10".to_string());
        let key_field = Arc::new(Field::new("key", DataType::Int64, false).with_metadata(key_meta));
        let value_field =
            Arc::new(Field::new("value", DataType::Int64, true).with_metadata(value_meta));
        let entries_struct_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            key_field.as_ref().clone(),
            value_field.as_ref().clone(),
        ]));
        let entries_field = Arc::new(Field::new("key_value", entries_struct_type.clone(), false));

        let raw_struct = raw_map.entries();
        let new_struct = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ]),
            raw_struct.columns().to_vec(),
            raw_struct.nulls().cloned(),
        );
        let map_with_field_ids = arrow::array::MapArray::new(
            entries_field.clone(),
            raw_map.offsets().clone(),
            new_struct,
            raw_map.nulls().cloned(),
            false,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("__row_id__", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("mn", DataType::Int64, true),
            Field::new("__agg_state_mn", DataType::Map(entries_field, false), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r1"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["r1"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(map_with_field_ids) as ArrayRef,
            ],
        )
        .expect("field-id annotated map batch");

        // The validator must accept this shape, even though it would fail
        // under strict `!=` (PARQUET:field_id metadata on inner Struct
        // fields plus differing inner-key nullability vs the expected type
        // derived from sql_type_to_arrow_type).
        validate_physical_aggregate_schema(&layout, &batch, "field-id annotated map")
            .expect("Map<K, V> column with field-id metadata must validate");
    }
}
