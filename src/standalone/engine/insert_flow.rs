//! INSERT dispatch through connector table sinks.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::connector::backend::ResolvedTable;
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{InsertSource, ObjectName};
use crate::standalone::engine::backend_resolver::{TargetBackend, resolve_existing_table_target};
use crate::standalone::engine::catalog::{ColumnDef, normalize_identifier};
use crate::standalone::engine::insert::reorder_insert_rows;
use crate::standalone::engine::{StandaloneState, StatementResult};

pub(crate) fn run_insert(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = resolve_existing_table_target(state, name, current_catalog, current_database)?;
    let (catalog, sink) = {
        let reg = state.connectors.read().expect("connector registry read");
        (
            reg.catalog_backend(target.backend_name)?,
            reg.table_sink(target.backend_name)?,
        )
    };
    let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;

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
        InsertSource::GenerateSeriesSelect(source) => {
            crate::standalone::engine::sqlparse::generate_series::insert_generate_series_rows_by_backend(
                state,
                &target,
                &resolved,
                source,
                columns,
            )?;
        }
        InsertSource::UnionAll(parts) => {
            for part in parts {
                run_insert(
                    state,
                    name,
                    columns,
                    part,
                    current_catalog,
                    current_database,
                )?;
            }
        }
        InsertSource::FromQuery(query) => {
            if !sink.supports_pipeline_insert() {
                if target.backend_name == "iceberg" {
                    return Err("unsupported INSERT SELECT source for iceberg table".to_string());
                }
                return Err(format!(
                    "backend {} does not support INSERT SELECT",
                    target.backend_name
                ));
            }
            let batch =
                execute_insert_from_query_on_pipeline(state, &target, &resolved, columns, query)?;
            if batch.num_rows() > 0 {
                sink.append_batch(&resolved, batch)?;
            }
        }
    }
    Ok(StatementResult::Ok)
}

fn execute_insert_from_query_on_pipeline(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    query: &sqlparser::ast::Query,
) -> Result<RecordBatch, String> {
    let query_result = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        crate::standalone::engine::execute_query(
            query,
            &catalog,
            &target.namespace,
            state.exchange_port,
            None,
        )?
    };

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
                    crate::standalone::engine::parquet::normalize_map_entries_nullability(
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
            let target_type = crate::standalone::engine::parquet::normalize_map_entries_nullability(
                &target_column.data_type,
            );
            let array: ArrayRef = match source_idx {
                Some(idx) => {
                    let src = batch.column(*idx);
                    if src.data_type() == &target_type {
                        src.clone()
                    } else {
                        arrow::compute::cast(src.as_ref(), &target_type).map_err(|e| {
                            format!(
                                "INSERT SELECT cannot cast column `{}` from {:?} to {:?}: {}",
                                target_column.name,
                                src.data_type(),
                                target_type,
                                e
                            )
                        })?
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
        let target_type = crate::standalone::engine::parquet::normalize_map_entries_nullability(
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
