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

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, Statement};

use crate::connector::iceberg::changes::{
    DeletedDataFileRef, IcebergChangeBatch, build_factory_for_table,
    expected_object_store_bucket_for_table, normalize_delete_projection_path,
    previous_snapshot_data_file_lineage_index, scan_deleted_data_file_rows,
    scan_equality_delete_rows_for_table_at,
};
use crate::engine::query_prep::{IcebergFileForQuery, build_iceberg_delta_table_def_with_files};
use crate::engine::{StandaloneState, execute_query};
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::node::iceberg_delta_scan::BaseDataFileLineage;
use crate::runtime::query_result::QueryResult;
use crate::sql::catalog::local::PlannerMemoryCatalog;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) struct IvmDeltaSourceFiles {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub files: Vec<IcebergFileForQuery>,
}

pub(crate) struct IvmDeltaSourceInput<'a> {
    pub state: &'a Arc<StandaloneState>,
    pub current_database: &'a str,
    pub base_ref: &'a TableIdentity,
    pub loaded: &'a crate::connector::iceberg::catalog::IcebergLoadedTable,
}

pub(crate) fn build_delta_source_files(
    input: IvmDeltaSourceInput<'_>,
    batch: IcebergChangeBatch,
) -> Result<IvmDeltaSourceFiles, String> {
    let previous_snapshot_id = batch.previous_snapshot_id;
    let current_snapshot_id = batch.current_snapshot_id;

    let mut files: Vec<IcebergFileForQuery> = batch
        .inserts
        .iter()
        .map(|f| IcebergFileForQuery {
            path: f.path.clone(),
            size: f.size,
            record_count: f.record_count,
            partition_spec_id: f.partition_spec_id,
            partition_key: f.partition_key.clone(),
            first_row_id: f.first_row_id,
            data_sequence_number: f.data_sequence_number,
            change_op: Some(CHANGE_OP_INSERT),
            row_id_allow_list: None,
        })
        .collect();

    let needs_delete_scan = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    if needs_delete_scan {
        let object_store_config = input.loaded.object_store_config.as_ref();
        let factory = build_factory_for_table(&input.loaded.table, object_store_config)?;
        let expected_object_store_bucket =
            expected_object_store_bucket_for_table(&input.loaded.table)?;
        let size_lookup = |path: &str| -> Option<u64> {
            let _ = path;
            None
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| {
                normalize_delete_projection_path(
                    &file.path,
                    object_store_config,
                    expected_object_store_bucket.as_deref(),
                )
            })
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|e| e.to_string())?;
        let current_lineage = if !batch.deletes.is_empty() {
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                &input.loaded.table,
                batch.current_snapshot_id,
            )?
        } else {
            HashMap::new()
        };
        let previous_lineage = if !batch.deletes.is_empty() && !batch.deleted_data_files.is_empty()
        {
            previous_snapshot_data_file_lineage_index(
                &input.loaded.table,
                batch.previous_snapshot_id,
            )?
        } else {
            HashMap::new()
        };
        let lineage_lookup = |path: &str| {
            current_lineage
                .get(path)
                .or_else(|| previous_lineage.get(path))
                .map(|lineage| lineage.first_row_id)
        };
        // Iceberg v3 Puffin DV is cumulative per data file: the DV file added
        // at the current snapshot replaces the prior one with the *union* of
        // its positions. If we let `scan_deletes_*` reverse-project that union
        // straight into the delta stream, every refresh would re-emit deletes
        // already applied at previous refreshes, double-counting against the
        // MV aggregate state. Subtract the prior snapshot's position-delete
        // set so the IVM delta source sees only the *newly* deleted rows.
        let touched_referenced_data_files: HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|d| d.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions = if !touched_referenced_data_files.is_empty() {
            crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
                &input.loaded.table,
                batch.previous_snapshot_id,
                &factory,
                &|path: &str| {
                    normalize_delete_projection_path(
                        path,
                        object_store_config,
                        expected_object_store_bucket.as_deref(),
                    )
                },
                |data_file_path: &str| touched_referenced_data_files.contains(data_file_path),
            )
            .map_err(|e| e.to_string())?
        } else {
            HashMap::new()
        };
        let mut deleted_rows =
            crate::connector::iceberg::scan_deletes::scan_deletes_with_base_row_id_lookup_and_path_normalizer(
                &batch.deletes,
                &factory,
                size_lookup,
                lineage_lookup,
                &deleted_data_file_paths,
                &previously_deleted_positions,
                |path| {
                    normalize_delete_projection_path(
                        path,
                        object_store_config,
                        expected_object_store_bucket.as_deref(),
                    )
                },
            )
            .map_err(|e| e.to_string())?;
        deleted_rows.extend(scan_equality_delete_rows_for_table_at(
            &input.loaded.table,
            &batch.equality_deletes,
            batch.current_snapshot_id,
            &factory,
            object_store_config,
        )?);
        let deleted_data_files = deleted_data_files_with_previous_lineage(
            &input.loaded.table,
            &batch.deleted_data_files,
            batch.previous_snapshot_id,
        )?;
        deleted_rows.extend(scan_deleted_data_file_rows(
            &input.loaded.table,
            deleted_data_files.as_ref(),
            object_store_config,
        )?);
        let deleted_rows = dedupe_deleted_rows_by_row_id(deleted_rows)?;
        if !deleted_rows.is_empty() {
            let (path, size, record_count) = crate::engine::mv_flow::write_mv_delete_temp_parquet(
                &input.base_ref.namespace,
                &input.base_ref.table,
                &deleted_rows,
            )?;
            files.push(
                crate::engine::query_prep::delete_temp_iceberg_file_for_query(
                    path,
                    size,
                    record_count,
                    Some(CHANGE_OP_DELETE),
                ),
            );
        }
    }

    Ok(IvmDeltaSourceFiles {
        previous_snapshot_id,
        current_snapshot_id,
        files,
    })
}

pub(crate) fn execute_delta_source_query(
    input: IvmDeltaSourceInput<'_>,
    select_sql: &str,
    source_files: IvmDeltaSourceFiles,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let Statement::Query(query) = statement else {
        return Err("IVM delta source SQL must be a SELECT query".to_string());
    };

    let (catalog_name, namespace, table_name) =
        crate::engine::mv_flow::validate_incremental_mv_base_ref(&query, input.base_ref)?;
    let entry = {
        let registry = input
            .state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&catalog_name)?
    };
    let table_def = build_iceberg_delta_table_def_with_files(
        &entry,
        &catalog_name,
        &namespace,
        &table_name,
        input.loaded.clone(),
        source_files.files,
    )?;

    let mut delta_catalog = PlannerMemoryCatalog::default();
    delta_catalog.create_database(&namespace)?;
    delta_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register iceberg delta source table: {e}"))?;

    let mut executable = query.as_ref().clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut executable);
    let connectors_snapshot = input
        .state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    execute_query(
        &executable,
        &delta_catalog,
        &connectors_snapshot,
        input.current_database,
        input.state.exchange_port,
        None,
    )
}

pub(crate) fn projection_select_with_change_op(select_sql: &str) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("projection_select_with_change_op normalize error: {e}"))?;
    let mut statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("projection_select_with_change_op parse error: {e}"))?;
    if sql_mentions_identifier(&statement.to_string(), CHANGE_OP_COLUMN) {
        return Err(format!(
            "projection_select_with_change_op: {CHANGE_OP_COLUMN} is a reserved delta source column"
        ));
    }

    let Statement::Query(query) = &mut statement else {
        return Err("projection_select_with_change_op: expected SELECT query".to_string());
    };
    if crate::mv::aggregate_state::mv_shape::query_has_aggregate_surface(query.as_ref()) {
        return Err(
            "projection_select_with_change_op: projection/filter SELECT must not be aggregate"
                .to_string(),
        );
    }
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("projection_select_with_change_op: expected SELECT body".to_string());
    };

    select
        .projection
        .push(SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
            CHANGE_OP_COLUMN,
        ))));
    Ok(statement.to_string())
}

fn dedupe_deleted_rows_by_row_id(
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use arrow::array::{Array, BooleanArray, Int64Array};
    use arrow::compute::filter_record_batch;

    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let row_id_index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case("_row_id"))
            .ok_or_else(|| {
                "IVM delta delete source requires Iceberg v3 `_row_id` column".to_string()
            })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "IVM delta delete source `_row_id` column must be BIGINT".to_string())?;
        let keep = (0..batch.num_rows())
            .map(|row| {
                if row_ids.is_null(row) {
                    return Err("IVM delta delete source `_row_id` cannot be NULL".to_string());
                }
                Ok(seen.insert(row_ids.value(row)))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if keep.iter().all(|keep| *keep) {
            out.push(batch);
        } else if keep.iter().any(|keep| *keep) {
            let filtered = filter_record_batch(&batch, &BooleanArray::from(keep))
                .map_err(|e| format!("deduplicate IVM delta delete rows by _row_id failed: {e}"))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        }
    }
    Ok(out)
}

fn sql_mentions_identifier(sql: &str, identifier: &str) -> bool {
    sql.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token.eq_ignore_ascii_case(identifier))
}

fn deleted_data_files_with_previous_lineage<'a>(
    table: &iceberg::table::Table,
    deleted_data_files: &'a [DeletedDataFileRef],
    previous_snapshot_id: i64,
) -> Result<Cow<'a, [DeletedDataFileRef]>, String> {
    if !deleted_data_files_need_previous_lineage(deleted_data_files) {
        return Ok(Cow::Borrowed(deleted_data_files));
    }

    let previous_lineage = previous_snapshot_data_file_lineage_index(table, previous_snapshot_id)?;
    let mut enriched = deleted_data_files.to_vec();
    enrich_deleted_data_files_with_previous_lineage(&mut enriched, &previous_lineage)?;
    Ok(Cow::Owned(enriched))
}

fn deleted_data_files_need_previous_lineage(deleted_data_files: &[DeletedDataFileRef]) -> bool {
    deleted_data_files
        .iter()
        .any(|file| file.first_row_id.is_none() || file.data_sequence_number.is_none())
}

fn enrich_deleted_data_files_with_previous_lineage(
    deleted_data_files: &mut [DeletedDataFileRef],
    previous_lineage: &HashMap<String, BaseDataFileLineage>,
) -> Result<(), String> {
    for file in deleted_data_files {
        if file.first_row_id.is_some() && file.data_sequence_number.is_some() {
            continue;
        }

        let lineage = previous_lineage.get(&file.path).ok_or_else(|| {
            format!(
                "iceberg MV deleted-data-file reverse projection requires previous-snapshot \
                 data-file lineage for {}; the previous-snapshot data-file lineage index does \
                 not contain the file",
                file.path
            )
        })?;
        if file.first_row_id.is_none() {
            file.first_row_id = Some(lineage.first_row_id);
        }
        if file.data_sequence_number.is_none() {
            file.data_sequence_number = Some(lineage.data_sequence_number);
        }
    }
    Ok(())
}
