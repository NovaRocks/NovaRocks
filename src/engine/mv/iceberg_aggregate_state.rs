use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::starrocks::managed::mv_agg_state::{
    AggregateMvLayout, build_old_state_map, load_aggregate_physical_rows,
    merge_aggregate_state_batches_with_retractions,
};
use crate::engine::record_batch_to_chunk;
use crate::exec::change_op::{ChangeOp, change_op_array, change_op_field};
use crate::exec::chunk::Chunk;

pub(crate) struct IcebergAggregateMergeResult {
    pub(crate) delete_row_ids: Vec<String>,
    pub(crate) insert_chunks: Vec<Chunk>,
    pub(crate) new_total_rows: i64,
}

pub(crate) fn merge_aggregate_target_state(
    layout: &AggregateMvLayout,
    old_chunks: &[Chunk],
    delta_chunks: &[Chunk],
) -> Result<IcebergAggregateMergeResult, String> {
    let old_rows = build_old_state_map(old_chunks, layout)?;
    let old_row_ids = old_rows.keys().cloned().collect::<BTreeSet<_>>();
    let touched_row_ids = delta_row_ids(layout, delta_chunks)?;
    let merge_result =
        merge_aggregate_state_batches_with_retractions(&old_rows, delta_chunks, layout)?;
    let merged_rows = load_aggregate_physical_rows(&merge_result.upsert_chunks, layout)?;
    let insert_chunks =
        filter_physical_chunks_by_row_ids(layout, &merge_result.upsert_chunks, &touched_row_ids)?;
    let delete_row_ids = touched_row_ids
        .iter()
        .filter(|row_id| old_row_ids.contains(*row_id))
        .cloned()
        .collect();
    let new_total_rows = i64::try_from(merged_rows.len())
        .map_err(|_| "iceberg aggregate MV target row count overflow".to_string())?;

    Ok(IcebergAggregateMergeResult {
        delete_row_ids,
        insert_chunks,
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

fn validate_physical_aggregate_schema(
    layout: &AggregateMvLayout,
    batch: &RecordBatch,
    context: &str,
) -> Result<(), String> {
    if batch.num_columns() != layout.physical_columns.len() {
        return Err(format!(
            "{context}: physical aggregate schema column count mismatch: got {} expected {}",
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
            crate::engine::sql_expr::sql_type_to_arrow_type(&expected_column.column.data_type)
                .map_err(|e| {
                    format!(
                        "{context}: convert expected physical aggregate column `{expected_name}` type failed: {e}"
                    )
                })?;
        if actual.data_type() != &expected_type {
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

pub(crate) fn load_current_aggregate_target_state(
    target_table: &iceberg::table::Table,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    crate::runtime::global_async_runtime::data_block_on(load_current_aggregate_target_state_async(
        target_table,
        layout,
    ))?
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

async fn load_current_aggregate_target_state_async(
    target_table: &iceberg::table::Table,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    let select_cols = layout
        .physical_columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let scan = target_table
        .scan()
        .select(select_cols)
        .build()
        .map_err(|e| format!("build iceberg aggregate target state scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg aggregate target state files failed: {e}"))?;
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.predicate = None;
            task
        })
    });
    let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("read iceberg aggregate target state scan failed: {e}"))?;

    let mut chunks = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg aggregate target state scan error: {e}"))?;
        validate_physical_aggregate_schema(layout, &batch, "iceberg aggregate target state scan")?;
        if batch.num_rows() == 0 {
            continue;
        }
        chunks.push(record_batch_to_chunk(batch)?);
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::connector::starrocks::managed::ddl::managed_physical_column;
    use crate::connector::starrocks::managed::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
    };
    use crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind;
    use crate::sql::parser::ast::SqlType;

    fn chunk(batch: RecordBatch) -> crate::exec::chunk::Chunk {
        crate::engine::record_batch_to_chunk(batch).expect("chunk")
    }

    fn encoded_utf8_group_row_id(value: &str) -> String {
        format!("utf8:V:{value}")
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn test_count_layout() -> AggregateMvLayout {
        let row_id_column = managed_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let region_column =
            managed_physical_column("region".to_string(), SqlType::String, true, true, false);
        let count_column =
            managed_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let count_state_column = managed_physical_column(
            "__agg_state_c".to_string(),
            SqlType::BigInt,
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
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
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
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::Int64, false),
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
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )) as ArrayRef,
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
        let old = vec![chunk(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("__row_id__", DataType::Utf8, false),
                    Field::new("region", DataType::Utf8, true),
                    Field::new("c", DataType::Int64, false),
                    Field::new("__agg_state_c", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![r1.as_str(), r2.as_str()])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["r1", "r2"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
                ],
            )
            .expect("old batch"),
        )];
        let delta = vec![chunk(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("__row_id__", DataType::Utf8, false),
                    Field::new("region", DataType::Utf8, true),
                    Field::new("c", DataType::Int64, false),
                    Field::new("__agg_state_c", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![
                        r1.as_str(),
                        r2.as_str(),
                        r3.as_str(),
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["r1", "r2", "r3"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1, -1, 5])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1, -1, 5])) as ArrayRef,
                ],
            )
            .expect("delta batch"),
        )];

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
        assert_field(fields[3].as_ref(), "__agg_state_c", &DataType::Int64, false);
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
    fn build_change_chunks_rejects_invalid_insert_physical_schema() {
        let layout = test_count_layout();
        let r1 = encoded_utf8_group_row_id("r1");
        let valid = count_physical_batch(&[(r1.as_str(), "r1", 1, 1)]);
        let wrong_order = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("c", DataType::Int64, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_state_c", DataType::Int64, false),
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
                Field::new("__agg_state_c", DataType::Int64, false),
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
                Field::new("__agg_state_c", DataType::Int64, false),
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
                Field::new("__agg_state_c", DataType::Int64, false),
            ])),
            valid.columns().to_vec(),
        )
        .expect("wrong nullability batch");
        let err =
            validate_physical_aggregate_schema(&layout, &wrong_nullability, "wrong nullability")
                .expect_err("wrong nullability rejected");
        assert!(err.contains("nullability mismatch"), "err={err}");
    }
}
