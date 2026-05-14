//! Managed-lake CREATE TABLE AS SELECT (CTAS).
//!
//! Mirrors `iceberg_ctas` but targets the managed-lake backend. The flow:
//! 1. Resolve the target table name to (namespace, table).
//! 2. Run the SELECT once to recover the result-set schema.
//! 3. Convert that schema into `TableColumnDef`s and create the managed
//!    table via the managed DDL path.
//! 4. Re-run the SELECT as an INSERT INTO ... SELECT so the rows land in
//!    the newly-created table through the standard managed sink.
//!
//! If step 4 fails we drop the empty table we just created so the SQL
//! transaction-style "CTAS succeeds or rolls back" contract is preserved.

use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::runtime::query_result::QueryResultColumn;
use crate::sql::parser::ast::{
    CreateTableKind, CreateTableStmt, InsertSource, OverwriteMode, TableColumnDef,
};

pub(crate) fn execute_managed_ctas(
    state: &Arc<StandaloneState>,
    stmt: CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    debug_assert!(
        stmt.as_select.is_some(),
        "managed CTAS dispatch requires AS SELECT"
    );

    let target = crate::engine::backend_resolver::resolve_table_target(
        state,
        &stmt.name,
        current_catalog,
        current_database,
    )?;

    // IF NOT EXISTS / already-exists check against the managed catalog.
    let already_exists = {
        let lake = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        lake.contains_table(&target.namespace, &target.table)?
    };
    if already_exists {
        if stmt.if_not_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "managed CTAS failed: table {}.{} already exists",
            target.namespace, target.table,
        ));
    }

    // Step A: plan SELECT, capture its output schema (the chunks are
    // discarded here — we re-run the SELECT as INSERT below so the
    // managed sink owns the write path).
    let query = stmt
        .as_select
        .as_ref()
        .expect("managed CTAS requires AS SELECT")
        .clone();
    let (_, schema_cols) =
        crate::engine::iceberg_writer::run_select_to_chunks_and_schema(state, &target, &query)?;
    if schema_cols.is_empty() {
        return Err(
            "managed CTAS: SELECT produced no output columns; schema cannot be inferred"
                .to_string(),
        );
    }

    let columns = query_result_columns_to_table_column_defs(&schema_cols)?;
    let CreateTableKind::Iceberg {
        key_desc,
        bucket_count,
        ..
    } = &stmt.kind;

    // Step B: create the managed table with the inferred schema.
    let table_object = crate::sql::parser::ast::ObjectName {
        parts: vec![target.table.clone()],
    };
    crate::connector::starrocks::managed::ddl::create_managed_table(
        state.as_ref(),
        &table_object,
        &target.namespace,
        &columns,
        key_desc.as_ref(),
        *bucket_count,
    )
    .map_err(|e| format!("managed CTAS failed: cannot create table: {e}"))?;

    // Step C: re-run the SELECT as INSERT INTO target SELECT ... to
    // populate the new table. Any failure here drops the table so we
    // do not leave an empty managed table behind.
    let insert_source = InsertSource::FromQuery(query);
    let table_name = crate::sql::parser::ast::ObjectName {
        parts: vec![target.namespace.clone(), target.table.clone()],
    };
    let insert_result = crate::engine::insert_flow::run_insert(
        state,
        &table_name,
        &[],
        &insert_source,
        OverwriteMode::None,
        current_catalog,
        current_database,
    );
    if let Err(err) = insert_result {
        // Rollback: drop the table we just created.
        let _ = crate::connector::starrocks::managed::ddl::drop_managed_table(
            state,
            &target.namespace,
            &target.table,
        );
        return Err(format!("managed CTAS failed during data write: {err}"));
    }
    Ok(StatementResult::Ok)
}

/// Convert the SELECT output schema into `TableColumnDef`s suitable for
/// the managed `create_table` API. Reuses the iceberg helper for the
/// Arrow-to-SQL type mapping so both CTAS paths agree on what types are
/// accepted.
fn query_result_columns_to_table_column_defs(
    cols: &[QueryResultColumn],
) -> Result<Vec<TableColumnDef>, String> {
    cols.iter()
        .map(|c| {
            let data_type = crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(&c.data_type)?;
            Ok(TableColumnDef {
                name: c.name.clone(),
                data_type,
                nullable: c.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect()
}
