//! Materialized-view statement dispatch through `MvBackend`.

use std::sync::Arc;

use crate::engine::catalog::{InMemoryCatalog, normalize_identifier};
use crate::engine::query_prep::IcebergFileForQuery;
use crate::engine::{StandaloneState, StatementResult, execute_query};
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::sql::parser::query_refs::{
    extract_three_part_table_ref_occurrences, extract_three_part_table_refs,
    strip_catalog_from_three_part_names,
};

fn mv_backend(
    state: &Arc<StandaloneState>,
) -> Result<Arc<dyn crate::connector::backend::MvBackend>, String> {
    state
        .connectors
        .read()
        .expect("connector registry read")
        .mv_backend("managed")
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.create_mv(stmt, current_catalog, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.drop_mv(stmt, current_catalog, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.refresh_mv(stmt, current_catalog, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let result: QueryResult = mv_backend(state)?.list_mvs(stmt, current_catalog)?;
    Ok(StatementResult::Query(result))
}

/// Analyze the output column types of a MV SELECT SQL without executing it.
///
/// Runs the semantic analyzer on the ORIGINAL (un-rewritten) SQL and returns
/// the visible output columns. This is used by the aggregate MV refresh path
/// to obtain visible-shaped types for `build_aggregate_mv_layout`, which expects
/// types matching `shape.visible_outputs` — not the state-shaped columns that
/// the rewritten SELECT (AVG → SUM + COUNT) produces.
pub(crate) fn analyze_visible_output_types(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
) -> Result<Vec<crate::sql::analysis::OutputColumn>, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err(
            "aggregate MV visible type analysis: stored SQL must be a SELECT query".to_string(),
        );
    };

    // Register iceberg tables referenced by the query so the analyzer can
    // resolve their column types. Uses the non-forced variant so tables already
    // present in the local catalog are skipped without touching the iceberg backend.
    // If registration fails (e.g., iceberg connector unavailable), we only propagate
    // the error when the table is genuinely missing from the local catalog; if it is
    // already present the registration failure is harmless and we proceed.
    //
    // Safety contract for this swallow path: it is safe ONLY because the production
    // refresh path (execute_query_for_mv_refresh / execute_query_for_mv_incremental_refresh)
    // separately calls refresh_external_tables_for_query (force=true) before execution,
    // ensuring catalog freshness. This analyzer-only path tolerates registration failure
    // when tables are already cached locally to keep test fixtures simple (tests pre-populate
    // the catalog without a live iceberg backend). If a non-refresh caller ever invokes this
    // function, registration failures should be propagated rather than swallowed.
    let three_parts = extract_three_part_table_refs(&query);
    if !three_parts.is_empty() {
        let reg_result = crate::engine::query_prep::register_external_tables_for_query(
            state,
            None,
            current_database,
            &query,
        );
        if let Err(ref reg_err) = reg_result {
            // Evaluate whether all referenced tables are already resolvable in the
            // local catalog (after stripping the catalog prefix). If yes, swallow the
            // registration error because the analyzer will resolve correctly. If not,
            // propagate it so callers see a meaningful "table not found" error.
            let catalog = state.catalog.read().expect("standalone catalog read lock");
            let all_present = three_parts.iter().all(|(_cat, ns, tbl)| {
                let ns_normalized = crate::engine::catalog::normalize_identifier(ns)
                    .unwrap_or_else(|_| ns.to_lowercase());
                let tbl_normalized = crate::engine::catalog::normalize_identifier(tbl)
                    .unwrap_or_else(|_| tbl.to_lowercase());
                catalog.get(&ns_normalized, &tbl_normalized).is_ok()
            });
            if !all_present {
                return Err(format!(
                    "aggregate MV visible type analysis: failed to register iceberg tables: {reg_err}"
                ));
            }
        }
    }

    let mut analyzable = query.as_ref().clone();
    if !three_parts.is_empty() {
        strip_catalog_from_three_part_names(&mut analyzable);
    }
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let (resolved, _cte_registry) =
        crate::sql::analyzer::analyze(&analyzable, &*catalog, current_database)
            .map_err(|e| format!("aggregate MV visible type analysis failed: {e}"))?;
    Ok(resolved.output_columns)
}

pub(crate) fn execute_query_for_mv_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let three_parts = extract_three_part_table_refs(&query);
    if !three_parts.is_empty() {
        crate::engine::query_prep::refresh_external_tables_for_query(
            state,
            None,
            current_database,
            &query,
        )?;
    }

    let mut executable = query.as_ref().clone();
    if !three_parts.is_empty() {
        strip_catalog_from_three_part_names(&mut executable);
    }
    // Clone-then-release: pipeline execution must not hold
    // `state.catalog.read()`. See iceberg_writer::run_select_to_chunks.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    execute_query(
        &executable,
        &catalog_snapshot,
        current_database,
        state.exchange_port,
        None,
    )
}

fn normalize_incremental_mv_base_ref(
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

pub(crate) fn validate_incremental_mv_base_ref(
    query: &sqlparser::ast::Query,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    let refs = extract_three_part_table_ref_occurrences(query);
    if refs.len() != 1 {
        return Err(format!(
            "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got {}",
            refs.len()
        ));
    }

    let actual = {
        let (catalog, namespace, table) = &refs[0];
        (
            normalize_identifier(catalog).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid catalog reference: {e}")
            })?,
            normalize_identifier(namespace).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid namespace reference: {e}")
            })?,
            normalize_identifier(table).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid table reference: {e}")
            })?,
        )
    };
    let expected = normalize_incremental_mv_base_ref(base_ref)?;
    if actual != expected {
        return Err(format!(
            "incremental MV refresh stored SQL base table mismatch: expected {}.{}.{}, got {}.{}.{}",
            expected.0, expected.1, expected.2, actual.0, actual.1, actual.2
        ));
    }
    Ok(expected)
}

pub(crate) fn execute_query_for_mv_incremental_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    delta_files: Vec<IcebergFileForQuery>,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let (catalog_name, namespace, table_name) = validate_incremental_mv_base_ref(&query, base_ref)?;
    let is_s3 = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&catalog_name)?.is_s3()
    };
    if !is_s3 && delta_files.len() > 1 {
        return Err(
            "incremental MV refresh over local iceberg supports at most one delta file".to_string(),
        );
    }

    let table_def = crate::engine::query_prep::build_iceberg_table_def_with_files(
        state,
        &catalog_name,
        &namespace,
        &table_name,
        delta_files,
    )?;
    let mut incremental_catalog = InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register incremental iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &incremental_catalog,
        current_database,
        state.exchange_port,
        None,
    )
}

pub(crate) fn write_mv_delete_temp_parquet(
    namespace: &str,
    table_name: &str,
    deleted_rows: &[arrow::record_batch::RecordBatch],
) -> Result<(String, i64, Option<i64>), String> {
    let first_batch = deleted_rows
        .first()
        .ok_or_else(|| "delete-side mv refresh has no rows to write".to_string())?;
    let dir = std::env::temp_dir().join(format!(
        "novarocks_mv_deletes_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("create temp dir for delete-side mv refresh: {e}"))?;
    let path = dir.join(format!("{namespace}_{table_name}.parquet"));
    let schema = first_batch.schema();
    let file = std::fs::File::create(&path)
        .map_err(|e| format!("create temp parquet for delete-side mv refresh: {e}"))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("create temp parquet writer for delete-side mv refresh: {e}"))?;
    for batch in deleted_rows {
        writer
            .write(batch)
            .map_err(|e| format!("write temp parquet batch for delete-side mv refresh: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("close temp parquet writer for delete-side mv refresh: {e}"))?;

    // The downstream HDFS_SCAN treats this size as `range.file_len` and seeks
    // to `(file_len - 8)` to read the parquet footer magic. We must report the
    // actual on-disk parquet size, not the in-memory Arrow column footprint —
    // the latter is materially smaller (one row of a couple of i64/string
    // columns is ~200-400 bytes in memory but ~700+ bytes as a parquet file
    // including magic + schema + footer), which makes the reader truncate and
    // surface "Invalid Parquet file. Corrupt footer".
    let total_size = std::fs::metadata(&path)
        .map(|m| m.len() as i64)
        .map_err(|e| format!("stat temp parquet for delete-side mv refresh: {e}"))?;
    let total_rows = Some(
        deleted_rows
            .iter()
            .map(|batch| batch.num_rows() as i64)
            .sum(),
    );

    Ok((format!("file://{}", path.display()), total_size, total_rows))
}

/// Run the MV's SELECT statement against a one-shot in-memory catalog
/// where the base table's storage is a single temp parquet file
/// containing the supplied deleted rows. Mirrors the insert-side
/// `execute_query_for_mv_incremental_refresh` but without iceberg-file
/// list construction — the caller has already projected the rows.
///
/// Returns the empty `QueryResult` when `deleted_rows` is empty.
pub(crate) fn execute_query_for_mv_incremental_deletes(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    deleted_rows: Vec<arrow::record_batch::RecordBatch>,
) -> Result<QueryResult, String> {
    if deleted_rows.is_empty() {
        return Ok(QueryResult::empty());
    }

    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };
    let (catalog_name, namespace, table_name) = validate_incremental_mv_base_ref(&query, base_ref)?;

    let (path, total_size, total_rows) =
        write_mv_delete_temp_parquet(&namespace, &table_name, &deleted_rows)?;

    // Build a TableDef whose storage is the temp parquet file. Reuse
    // build_iceberg_table_def_with_files's column-shape logic by giving
    // it a one-element file list.
    let delete_files = vec![
        crate::engine::query_prep::delete_temp_iceberg_file_for_query(
            path, total_size, total_rows, None,
        ),
    ];

    let table_def = crate::engine::query_prep::build_iceberg_table_def_with_files(
        state,
        &catalog_name,
        &namespace,
        &table_name,
        delete_files,
    )?;
    let mut incremental_catalog = InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register delete-side iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &incremental_catalog,
        current_database,
        state.exchange_port,
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize sql");
        let statement =
            crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        *query
    }

    fn base_ref() -> crate::connector::starrocks::managed::store::IcebergTableRef {
        crate::connector::starrocks::managed::store::IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
        }
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_projection_subquery_extra_ref() {
        let query =
            parse_query("select k, (select count(*) from ice.db.t) as c from ice.db.t where v > 0");
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_where_subquery_extra_ref() {
        let query =
            parse_query("select k from ice.db.t where exists (select 1 from ice.db.t where v > 0)");
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn validate_incremental_mv_base_ref_rejects_having_subquery_extra_ref() {
        let query = parse_query(
            "select k, count(*) from ice.db.t group by k \
             having count(*) > (select count(*) from ice.db.t)",
        );
        let err = super::validate_incremental_mv_base_ref(&query, &base_ref())
            .expect_err("extra 3-part ref must fail");

        assert!(err.contains("exactly one 3-part Iceberg table, got 2"));
    }

    #[test]
    fn delete_temp_delta_file_omits_row_lineage_metadata() {
        let file = crate::engine::query_prep::delete_temp_iceberg_file_for_query(
            "file:///tmp/delete.parquet".to_string(),
            128,
            Some(1),
            None,
        );

        assert_eq!(file.first_row_id, None);
        assert_eq!(file.data_sequence_number, None);
    }

    #[test]
    fn mv_delete_temp_parquet_preserves_iceberg_field_ids() {
        let metadata = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
        let field = Field::new("renamed_id", DataType::Int32, false).with_metadata(metadata);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(
            batch
                .schema()
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("7")
        );

        let (path, _, _) = super::write_mv_delete_temp_parquet("ns", "orders", &[batch])
            .expect("write temp parquet");
        let local_path = path.strip_prefix("file://").expect("file path");
        let file = std::fs::File::open(local_path).expect("open temp parquet");
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("builder");
        assert_eq!(
            builder
                .schema()
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("7")
        );
    }

    /// Regression: the returned `total_size` must equal the on-disk parquet
    /// file length, not the in-memory Arrow column footprint. The downstream
    /// HDFS_SCAN treats this value as `range.file_len` and seeks to
    /// `(file_len - 8)` to read the parquet footer magic; a smaller value
    /// (Arrow buffer size) makes the reader read into data bytes and report
    /// "Invalid Parquet file. Corrupt footer".
    #[test]
    fn mv_delete_temp_parquet_size_matches_on_disk_length() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            ],
        )
        .expect("batch");

        let (path, total_size, _) =
            super::write_mv_delete_temp_parquet("ns", "orders", &[batch]).expect("write");
        let local_path = path.strip_prefix("file://").expect("file path");
        let on_disk = std::fs::metadata(local_path)
            .expect("stat temp parquet")
            .len() as i64;

        assert_eq!(
            total_size, on_disk,
            "write_mv_delete_temp_parquet must return on-disk file length \
             (got total_size={total_size}, on_disk={on_disk}); a smaller value \
             causes downstream HDFS_SCAN to treat the file as truncated"
        );
    }
}
