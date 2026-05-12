use std::sync::Arc;

use sqlparser::ast::{Expr, Ident, SelectItem, SetExpr, Statement};

use crate::connector::iceberg::changes::{
    IcebergChangeBatch, build_factory_for_table, normalize_delete_projection_path,
    scan_deleted_data_file_rows, scan_equality_delete_rows_for_table,
};
use crate::connector::starrocks::managed::store::IcebergTableRef;
use crate::engine::catalog::InMemoryCatalog;
use crate::engine::query_prep::{IcebergFileForQuery, build_iceberg_delta_table_def_with_files};
use crate::engine::{QueryResult, StandaloneState, execute_query};
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};

pub(crate) struct IvmDeltaSourceFiles {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub files: Vec<IcebergFileForQuery>,
}

pub(crate) struct IvmDeltaSourceInput<'a> {
    pub state: &'a Arc<StandaloneState>,
    pub current_database: &'a str,
    pub base_ref: &'a IcebergTableRef,
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
        })
        .collect();

    let needs_delete_scan = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    if needs_delete_scan {
        let object_store_config = input.loaded.object_store_config.as_ref();
        let factory = build_factory_for_table(&input.loaded.table, object_store_config)?;
        let size_lookup = |path: &str| -> Option<u64> {
            let _ = path;
            None
        };
        let mut deleted_rows =
            crate::connector::iceberg::scan_deletes::scan_deletes_with_path_normalizer(
                &batch.deletes,
                &factory,
                input.loaded.table.file_io(),
                size_lookup,
                |path| normalize_delete_projection_path(path, object_store_config),
            )
            .map_err(|e| e.to_string())?;
        deleted_rows.extend(scan_equality_delete_rows_for_table(
            &input.loaded.table,
            &batch.equality_deletes,
            &factory,
            object_store_config,
        )?);
        deleted_rows.extend(scan_deleted_data_file_rows(
            &input.loaded.table,
            &batch.deleted_data_files,
            object_store_config,
        )?);
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
        &namespace,
        &table_name,
        input.loaded.clone(),
        source_files.files,
    )?;

    let mut delta_catalog = InMemoryCatalog::default();
    delta_catalog.create_database(&namespace)?;
    delta_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register iceberg delta source table: {e}"))?;

    let mut executable = query.as_ref().clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &delta_catalog,
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
    if super::mv_shape::query_has_aggregate_surface(query.as_ref()) {
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

fn sql_mentions_identifier(sql: &str, identifier: &str) -> bool {
    sql.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token.eq_ignore_ascii_case(identifier))
}

#[cfg(test)]
mod tests {
    use super::projection_select_with_change_op;

    #[test]
    fn projection_select_with_change_op_preserves_where_for_projection_filter() {
        let rewritten =
            projection_select_with_change_op("select k, v + 1 as v1 from ice.db.t where v > 0")
                .expect("rewrite");
        let upper = rewritten.to_uppercase();

        assert!(upper.starts_with("SELECT K, V + 1 AS V1, __CHANGE_OP FROM"));
        assert!(upper.contains(" WHERE V > 0"));
    }

    #[test]
    fn projection_select_with_change_op_rejects_group_by_aggregate() {
        let err = projection_select_with_change_op(
            "select k, sum(v) as total from ice.db.t where v > 0 group by k",
        )
        .expect_err("aggregate SELECT must fail");

        assert!(err.contains("projection/filter"));
        assert!(err.contains("aggregate"));
    }

    #[test]
    fn projection_select_with_change_op_rejects_existing_change_op() {
        let err = projection_select_with_change_op("select k as __change_op from ice.db.t")
            .expect_err("reserved output must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("reserved"));
    }

    #[test]
    fn projection_select_with_change_op_rejects_non_query() {
        let err = projection_select_with_change_op("insert into t values (1)")
            .expect_err("non-query must fail");

        assert!(err.contains("expected SELECT query"));
    }
}
