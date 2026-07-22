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

//! INSERT dispatch through connector table sinks.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, StringArray, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::backend::ResolvedTable;
use crate::engine::backend_resolver::{TargetBackend, resolve_existing_table_target};
use crate::engine::insert::reorder_insert_rows;
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::expr::cast_with_special_rules;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_result::QueryResult;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{InsertSource, ObjectName, OverwriteMode};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::ColumnDef;

pub(crate) fn run_insert(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    overwrite_mode: OverwriteMode,
    current_catalog: Option<&str>,
    current_database: &str,
    query_opts: Option<&QueryOptions>,
) -> Result<StatementResult, String> {
    let is_overwrite = matches!(
        overwrite_mode,
        OverwriteMode::FullTable | OverwriteMode::DynamicPartitions,
    );
    // Detect branch/tag suffix in the table name (e.g. `t.branch_dev`).
    let (stripped_parts, ref_suffix) = split_ref_suffix(&name.parts);
    let effective_name;
    let name = match ref_suffix {
        Some(IcebergRefSuffix::Tag(ref tag_name)) => {
            return Err(format!(
                "iceberg ref: tag '{tag_name}' is read-only; use a branch as DML target"
            ));
        }
        Some(IcebergRefSuffix::Branch(_)) => {
            effective_name = ObjectName {
                parts: stripped_parts,
            };
            &effective_name
        }
        None => name,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    let target = resolve_existing_table_target(state, name, current_catalog, current_database)?;
    let (catalog, sink) = {
        let reg = state.connectors.read().expect("connector registry read");
        (
            reg.catalog_backend(target.backend_name)?,
            reg.table_sink(target.backend_name)?,
        )
    };
    let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Insert,
    )?;

    // Branch-qualified INSERT requires an iceberg backend and v3 table format.
    if ref_suffix.is_some() {
        if target.backend_name != "iceberg" {
            return Err(format!(
                "iceberg ref: branch-qualified INSERT is only supported for iceberg backends, \
                 got `{}`",
                target.backend_name
            ));
        }
        // UnionAll is not supported for branch writes.
        if matches!(source, InsertSource::UnionAll(_)) {
            return Err(
                "iceberg ref: branch-qualified INSERT does not support UNION ALL sources"
                    .to_string(),
            );
        }
    }

    // INSERT OVERWRITE PARTITIONS is only meaningful on a partitioned iceberg
    // table (the partition-table + v3-row-lineage requirements are checked
    // engine-side once metadata is loaded; see OverwritePartitionsCommit). The
    // backend gate is fail-fast here so non-iceberg backends get a precise
    // error rather than the generic OVERWRITE one.
    if matches!(overwrite_mode, OverwriteMode::DynamicPartitions)
        && target.backend_name != "iceberg"
    {
        return Err(format!(
            "INSERT OVERWRITE PARTITIONS is only supported for iceberg backends, \
             target uses backend `{}`",
            target.backend_name
        ));
    }

    // INSERT OVERWRITE is only supported on iceberg backends in phase 1.
    // For non-iceberg targets, fail fast with a clear message instead of
    // silently doing INSERT INTO.
    if is_overwrite && target.backend_name != "iceberg" {
        return Err(format!(
            "INSERT OVERWRITE is only supported for iceberg backends in phase 1, \
             target uses backend `{}`",
            target.backend_name
        ));
    }

    // Iceberg user writes route through the write-transaction runner. Keep
    // UNION ALL split here so each part is validated and recorded as its own
    // operation, preserving the existing recursive behavior.
    let needs_iceberg_pipeline =
        target.backend_name == "iceberg" && !matches!(source, InsertSource::UnionAll(_));
    if needs_iceberg_pipeline {
        return crate::engine::iceberg_writer::execute_iceberg_insert_or_overwrite(
            state,
            &target,
            &resolved,
            columns,
            source,
            overwrite_mode,
            &target_ref,
        );
    }

    match source {
        InsertSource::Values(rows) => {
            let reordered = reorder_insert_rows(rows, columns, &resolved.columns)?;
            sink.append_rows(&resolved, &reordered)?;
        }
        InsertSource::SelectLiteralRow(row) => {
            let reordered =
                reorder_insert_rows(std::slice::from_ref(row), columns, &resolved.columns)?;
            sink.append_rows(&resolved, &reordered)?;
        }
        InsertSource::UnionAll(parts) => {
            for part in parts {
                run_insert(
                    state,
                    name,
                    columns,
                    part,
                    overwrite_mode,
                    current_catalog,
                    current_database,
                    query_opts,
                )?;
            }
        }
        InsertSource::FromQuery(query) => {
            if !sink.supports_pipeline_insert() {
                return Err(format!(
                    "backend {} does not support INSERT SELECT",
                    target.backend_name
                ));
            }
            let batch = execute_insert_from_query_on_pipeline(
                state,
                current_catalog,
                &target,
                &resolved,
                columns,
                query,
                query_opts,
            )?;
            if batch.num_rows() > 0 {
                sink.append_batch(&resolved, batch)?;
            }
        }
    }
    if target.backend_name == "iceberg" {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    }
    crate::engine::statistics::observe_insert(
        state,
        &target.namespace,
        &target.table,
        columns,
        source,
        overwrite_mode,
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_insert_from_query_on_pipeline(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    query: &sqlparser::ast::Query,
    query_opts: Option<&QueryOptions>,
) -> Result<RecordBatch, String> {
    let query_result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        &target.namespace,
        query,
        query_opts.cloned(),
    )?;

    align_query_result_to_target(&query_result, insert_columns, &resolved.columns)
}

fn align_query_result_to_target(
    result: &QueryResult,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<RecordBatch, String> {
    let mapping =
        build_target_column_mapping(insert_columns, target_columns, result.columns.len())?;

    let target_schema = Arc::new(Schema::new(
        target_columns
            .iter()
            .map(|c| {
                Field::new(
                    &c.name,
                    crate::formats::parquet::local_io::normalize_map_entries_nullability(
                        &c.data_type,
                    ),
                    c.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));

    let column_count = target_columns.len();
    let mut per_target_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); column_count];
    for chunk in &result.chunks {
        let batch = &chunk.batch;
        if batch.num_columns() < result.columns.len() {
            return Err(format!(
                "INSERT SELECT chunk has {} columns but query returns {}",
                batch.num_columns(),
                result.columns.len()
            ));
        }
        let chunk_rows = batch.num_rows();
        for (target_idx, source_idx) in mapping.iter().enumerate() {
            let target_column = &target_columns[target_idx];
            let target_type = crate::formats::parquet::local_io::normalize_map_entries_nullability(
                &target_column.data_type,
            );
            let array: ArrayRef = match source_idx {
                Some(idx) => {
                    let src = batch.column(*idx);
                    if src.data_type() == &target_type {
                        src.clone()
                    } else {
                        cast_insert_select_source_array(src, &target_type, &target_column.name)?
                    }
                }
                None => new_null_array(&target_type, chunk_rows),
            };
            per_target_columns[target_idx].push(array);
        }
    }

    let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for (target_idx, arrays) in per_target_columns.into_iter().enumerate() {
        let target_column = &target_columns[target_idx];
        let target_type = crate::formats::parquet::local_io::normalize_map_entries_nullability(
            &target_column.data_type,
        );
        let merged: ArrayRef = if arrays.is_empty() {
            new_null_array(&target_type, 0)
        } else if arrays.len() == 1 {
            arrays.into_iter().next().unwrap()
        } else {
            let refs: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs).map_err(|e| {
                format!(
                    "INSERT SELECT failed to concat chunks for column `{}`: {e}",
                    target_column.name
                )
            })?
        };
        final_columns.push(merged);
    }

    RecordBatch::try_new(target_schema, final_columns)
        .map_err(|e| format!("build INSERT SELECT batch failed: {e}"))
}

fn cast_insert_select_source_array(
    src: &ArrayRef,
    target_type: &DataType,
    target_column_name: &str,
) -> Result<ArrayRef, String> {
    let cast_input = remote_binary_text_cast_input(src, target_type, target_column_name)?;
    if cast_input.data_type() == target_type {
        return Ok(cast_input);
    }
    cast_with_special_rules(&cast_input, target_type).map_err(|e| {
        format!(
            "INSERT SELECT cannot cast column `{}` from {:?} to {:?}: {}",
            target_column_name,
            src.data_type(),
            target_type,
            e
        )
    })
}

fn remote_binary_text_cast_input(
    src: &ArrayRef,
    target_type: &DataType,
    target_column_name: &str,
) -> Result<ArrayRef, String> {
    if matches!(target_type, DataType::Binary | DataType::LargeBinary) {
        return Ok(Arc::clone(src));
    }
    let Some(binary) = src.as_any().downcast_ref::<BinaryArray>() else {
        return Ok(Arc::clone(src));
    };

    let mut values = Vec::with_capacity(binary.len());
    for row in 0..binary.len() {
        if binary.is_null(row) {
            values.push(None);
            continue;
        }
        let text = std::str::from_utf8(binary.value(row)).map_err(|e| {
            format!(
                "INSERT SELECT column `{target_column_name}` contains non-UTF8 remote text: {e}"
            )
        })?;
        values.push(Some(text.to_string()));
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn build_target_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
    source_column_count: usize,
) -> Result<Vec<Option<usize>>, String> {
    if insert_columns.is_empty() {
        if source_column_count != target_columns.len() {
            return Err(format!(
                "INSERT SELECT column count mismatch: target has {} columns, SELECT produces {}",
                target_columns.len(),
                source_column_count
            ));
        }
        return Ok((0..target_columns.len()).map(Some).collect());
    }

    if insert_columns.len() != source_column_count {
        return Err(format!(
            "INSERT SELECT column count mismatch: INSERT lists {} columns, SELECT produces {}",
            insert_columns.len(),
            source_column_count
        ));
    }

    let mut insert_index_by_name: HashMap<String, usize> =
        HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        let key = normalize_identifier(&column.name)?;
        mapping.push(insert_index_by_name.remove(&key));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!(
            "unknown INSERT column `{name}` not found in target table"
        ));
    }
    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::runtime::query_result::QueryResultColumn;

    #[test]
    fn insert_select_align_casts_remote_binary_text_to_target_int64() {
        let source_field = Field::new("col_0", DataType::Binary, true);
        let source_array =
            Arc::new(BinaryArray::from(vec![Some(b"42".as_slice()), None])) as ArrayRef;
        let source_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![source_field.clone()])),
            vec![source_array],
        )
        .expect("source batch");
        let source_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId(0),
                source_field,
                None,
                None,
            )])
            .expect("source chunk schema"),
        );
        let source_chunk =
            Chunk::try_new_with_chunk_schema(source_batch, source_schema).expect("source chunk");
        let result = QueryResult {
            columns: vec![QueryResultColumn {
                name: "idx".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                logical_type: None,
            }],
            chunks: vec![source_chunk],
        };
        let target_columns = vec![ColumnDef {
            name: "idx".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];

        let batch = align_query_result_to_target(&result, &[], &target_columns).unwrap();

        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert_eq!(values.value(0), 42);
        assert!(values.is_null(1));
    }
}
