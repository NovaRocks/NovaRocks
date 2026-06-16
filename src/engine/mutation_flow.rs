use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    IcebergCommitCollector, IcebergUpdateMode, ensure_no_variant_columns_for_row_level_mutation,
    select_iceberg_update_mode,
};
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy, local_writer_commit_input, new_local_writer_write_id,
    write_commit_has_files,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::query_result::QueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::codegen::iceberg_write_sink::{IcebergWriteSinkMode, IcebergWriteSinkSpec};
use crate::sql::parser::ast::{
    InsertSource, MergeMatchedAction, MergeNotMatchedAction, MergeStmt, ObjectName, OverwriteMode,
    UpdateStmt,
};

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // Detect branch/tag suffix in the target table name.
    let (stripped_parts, ref_suffix) = split_ref_suffix(&stmt.table.parts);
    let effective_name;
    let table_name: &ObjectName = match ref_suffix {
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
        None => &stmt.table,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;

    // Reject variant tables before any planning. Without this guard the
    // failure surfaces deep inside `materialize_update_matches` as a
    // planner error about the row-lineage `__nr_t` column.
    ensure_no_variant_columns_for_row_level_mutation(&table).map_err(|e| format!("UPDATE: {e}"))?;

    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                table_ident, fmt as u8,
            ));
        }
    }

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    validate_update_assignments(&stmt.assignments, &target_columns, &partition_columns)?;

    let matched = materialize_update_matches(state, &target, stmt, current_catalog)?;
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    validate_unique_target_row_ids(&matched.row_ids)?;

    let mode = select_iceberg_update_mode(&table)?;
    match mode {
        IcebergUpdateMode::CopyOnWrite => execute_cow_update(
            state,
            &target,
            catalog,
            table_ident,
            table,
            matched,
            &target_columns,
            entry,
            &target_ref,
        ),
        IcebergUpdateMode::MergeOnRead => execute_mor_update(
            state,
            &target,
            catalog,
            table_ident,
            table,
            matched,
            entry,
            &target_ref,
            build_update_mor_distributed_write(
                state,
                &target,
                stmt,
                current_catalog,
                &target_columns,
                &target_ref,
            )?,
        ),
    }
}

fn materialize_update_matches(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
) -> Result<MatchedUpdateBatch, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    // The match SELECT runs against the standalone analyzer with
    // `current_database = target.namespace` (so 1-part target name resolves
    // to the iceberg target). Source relations may live in a different
    // namespace; `mutation_source_to_sql` qualifies them with their
    // namespace so the analyzer can find them.
    let target_sql = format!("{} AS {}", target.table, target_alias);
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql.as_deref(),
    );
    execute_update_match_query(state, Some(&target.catalog), &match_sql, &target.namespace)
}

fn mutation_source_to_sql(
    state: &Arc<StandaloneState>,
    source: &Option<crate::sql::parser::ast::MutationSource>,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Option<String>, String> {
    match source {
        None => Ok(None),
        Some(source) => {
            mutation_source_relation_to_sql(state, source, current_catalog, target).map(Some)
        }
    }
}

fn mutation_source_relation_to_sql(
    state: &Arc<StandaloneState>,
    source: &crate::sql::parser::ast::MutationSource,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<String, String> {
    use crate::sql::parser::ast::MutationSource;
    match source {
        MutationSource::Table { name, alias } => {
            // The match SELECT runs with `current_database = target.namespace`
            // and `current_catalog = Some(target.catalog)`. Resolve the source
            // against the user's surface name to get its concrete (catalog,
            // namespace, table). Emit a 1-part name when the source shares the
            // target's namespace+catalog (lets refresh follow the
            // current-catalog path), and a 2-part `<namespace>.<table>` name
            // otherwise so the standalone analyzer can find it directly.
            let resolved = crate::engine::backend_resolver::resolve_existing_table_target(
                state,
                name,
                current_catalog,
                &target.namespace,
            )?;
            let mut sql =
                if resolved.catalog == target.catalog && resolved.namespace == target.namespace {
                    resolved.table.clone()
                } else {
                    format!("{}.{}", resolved.namespace, resolved.table)
                };
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
            Ok(sql)
        }
        MutationSource::Query { query, alias } => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_string())?;
            Ok(format!("({query}) AS {alias}"))
        }
    }
}

fn build_update_mor_distributed_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    target_ref: &str,
) -> Result<MorUpdateDistributedWrite, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let data_sink_spec = crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    // The old-row deletions are written by the BE as deletion vectors. Build a
    // DeletionVectors-mode sink pinned to the base snapshot, mirroring the
    // Phase-1 DV DELETE path.
    //
    // This snapshot is derived from this builder's own `load_table` above,
    // whereas `execute_mor_update` derives the collector/transaction base
    // snapshot from the caller's already-loaded `table`. Under the single-writer
    // assumption both loads observe the same current snapshot, so the DV sink's
    // planned snapshot and the commit base snapshot must agree; if a concurrent
    // writer ever breaks that, the commit's base-snapshot conflict check fails
    // fast rather than committing against a stale base.
    let base_snapshot_id = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
    } else {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    };
    let mut dv_sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    dv_sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
    dv_sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let data_query = build_update_mor_data_sink_query(
        target,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql.as_deref(),
        target_columns,
        target_ref,
        new_sequence_number,
    )?;
    // The DV SELECT shares the data query's `[FOR VERSION AS OF] [CROSS JOIN
    // source] WHERE <pred>` tail, so the matched old rows are identical.
    let dv_query = build_update_dv_sink_query(
        target,
        target_alias,
        source_sql.as_deref(),
        where_sql.as_deref(),
        target_ref,
        &dv_sink_spec.target_columns,
    )?;
    Ok(MorUpdateDistributedWrite {
        data_query,
        data_sink_spec,
        dv_query,
        dv_sink_spec,
    })
}

fn build_merge_mor_distributed_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    matched_rows: &MatchedUpdateBatch,
    target_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<MorUpdateDistributedWrite, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let data_sink_spec = crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    // MERGE matched-UPDATE is `main`-only; pin the DV sink to the current
    // snapshot so the BE writes deletion vectors for the matched old rows.
    // This snapshot is derived from this builder's own `load_table` above;
    // `execute_mor_update` derives the commit base snapshot from the caller's
    // loaded `table`. Under the single-writer assumption both observe the same
    // current snapshot and must agree (the commit's base-snapshot check fails
    // fast otherwise).
    let base_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let mut dv_sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    dv_sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
    dv_sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let data_query = build_merge_mor_data_sink_query_from_matched(
        matched_rows,
        target_columns,
        new_sequence_number,
    )?;
    let dv_query =
        build_merge_mor_dv_sink_query_from_matched(matched_rows, &dv_sink_spec.target_columns)?;
    Ok(MorUpdateDistributedWrite {
        data_query,
        data_sink_spec,
        dv_query,
        dv_sink_spec,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_update_mor_data_sink_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    target_ref: &str,
    new_sequence_number: i64,
) -> Result<sqlparser::ast::Query, String> {
    let assignment_by_column = assignments_sql
        .iter()
        .map(|(column, expr)| (column.to_ascii_lowercase(), *expr))
        .collect::<HashMap<_, _>>();
    let mut select_items = Vec::with_capacity(target_columns.len() + 2);
    for column in target_columns {
        let expr = assignment_by_column
            .get(&column.name.to_ascii_lowercase())
            .map(|expr| format!("({expr})"))
            .unwrap_or_else(|| qualify_column(target_alias, &column.name));
        select_items.push(format!("{expr} AS {}", sql_identifier(&column.name)));
    }
    select_items.push(format!(
        "{} AS {}",
        qualify_column(target_alias, crate::exec::row_position::ICEBERG_ROW_ID_COL),
        sql_identifier(crate::exec::row_position::ICEBERG_ROW_ID_COL)
    ));
    select_items.push(format!(
        "{} AS {}",
        new_sequence_number,
        sql_identifier(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
    ));
    let sql = build_update_distributed_select_sql(
        target,
        target_alias,
        source_sql,
        where_sql,
        target_ref,
        select_items,
        Some(qualify_column(
            target_alias,
            crate::exec::row_position::ICEBERG_ROW_ID_COL,
        )),
    );
    parse_generated_query(&sql, "MOR UPDATE data sink")
}

/// Build the DELETE side of a MOR UPDATE as a SELECT of the position-delete
/// sink's input columns (`_file`, `_pos`, and partition source columns, with
/// `_file` first) for the matched old rows. Reuses the same target / version /
/// CROSS JOIN / WHERE tail as the data sink query so both sinks observe an
/// identical matched set.
fn build_update_dv_sink_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    target_alias: &str,
    source_sql: Option<&str>,
    where_sql: Option<&str>,
    target_ref: &str,
    dv_sink_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let select_items = dv_sink_columns
        .iter()
        .map(|column| {
            format!(
                "{} AS {}",
                qualify_column(target_alias, &column.name),
                sql_identifier(&column.name)
            )
        })
        .collect::<Vec<_>>();
    let sql = build_update_distributed_select_sql(
        target,
        target_alias,
        source_sql,
        where_sql,
        target_ref,
        select_items,
        Some(qualify_column(
            target_alias,
            crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN,
        )),
    );
    parse_generated_query(&sql, "MOR UPDATE DV sink")
}

/// Build the DELETE side of a MERGE matched-UPDATE as a VALUES projection of
/// the position-delete sink's input columns (`_file`, `_pos`, and partition
/// source columns). The matched old-row identities and partition values are
/// taken from the already-materialized `matched` batch; the BE turns them into
/// deletion-vector files keyed by `_file`.
fn build_merge_mor_dv_sink_query_from_matched(
    matched: &MatchedUpdateBatch,
    dv_sink_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    if matched.row_ids.is_empty() {
        return Err("MERGE MOR UPDATE DV sink requires at least one matched row".to_string());
    }
    if matched.file_paths.len() != matched.row_ids.len()
        || matched.row_positions.len() != matched.row_ids.len()
    {
        return Err(format!(
            "MERGE MOR UPDATE matched identity count mismatch: file_paths={}, row_positions={}, row_ids={}",
            matched.file_paths.len(),
            matched.row_positions.len(),
            matched.row_ids.len()
        ));
    }
    // Partition source values are read positionally from `old_rows`, so its row
    // count must match the matched identities (mirrors the data sink's
    // `new_rows.num_rows()` guard).
    if matched.old_rows.num_rows() != matched.row_ids.len() {
        return Err(format!(
            "MERGE MOR UPDATE matched old-row count mismatch: old_rows={}, row_ids={}",
            matched.old_rows.num_rows(),
            matched.row_ids.len()
        ));
    }

    let alias = "__nr_dv";
    // The first two sink columns are always the row-identity `_file` / `_pos`;
    // any remaining columns are partition source columns read from old_rows.
    let file_col = crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN;
    let pos_col = crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN;
    let partition_columns = &dv_sink_columns[dv_sink_columns
        .iter()
        .position(|column| {
            !column.name.eq_ignore_ascii_case(file_col)
                && !column.name.eq_ignore_ascii_case(pos_col)
        })
        .unwrap_or(dv_sink_columns.len())..];

    let mut rows = Vec::with_capacity(matched.row_ids.len());
    for row in 0..matched.row_ids.len() {
        let mut values = Vec::with_capacity(dv_sink_columns.len());
        values.push(sql_string_literal(&matched.file_paths[row]));
        values.push(matched.row_positions[row].to_string());
        for partition_column in partition_columns {
            let idx = matched
                .old_rows
                .schema()
                .index_of(&partition_column.name)
                .map_err(|_| {
                    format!(
                        "MERGE MOR UPDATE old-row batch missing partition source column `{}`",
                        partition_column.name
                    )
                })?;
            let literal =
                crate::engine::sql_expr::literal_from_batch(matched.old_rows.column(idx), row)?;
            values.push(
                crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
                    &literal,
                    &partition_column.data_type,
                )?,
            );
        }
        rows.push(format!("({})", values.join(", ")));
    }

    let value_columns = dv_sink_columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        rows.join(", "),
        sql_identifier(alias),
        value_columns
    );

    let mut select_items = Vec::with_capacity(dv_sink_columns.len());
    select_items.push(format!(
        "CAST({} AS STRING) AS {}",
        qualify_column(alias, file_col),
        sql_identifier(file_col)
    ));
    select_items.push(format!(
        "CAST({} AS BIGINT) AS {}",
        qualify_column(alias, pos_col),
        sql_identifier(pos_col)
    ));
    for partition_column in partition_columns {
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(
                &qualify_column(alias, &partition_column.name),
                partition_column,
            )?,
            sql_identifier(&partition_column.name)
        ));
    }

    let sql = format!(
        "SELECT {} FROM {} ORDER BY {}",
        select_items.join(", "),
        values_sql,
        qualify_column(alias, file_col)
    );
    parse_generated_query(&sql, "MERGE MOR UPDATE DV sink")
}

fn build_merge_mor_data_sink_query_from_matched(
    matched: &MatchedUpdateBatch,
    target_columns: &[crate::engine::catalog::ColumnDef],
    new_sequence_number: i64,
) -> Result<sqlparser::ast::Query, String> {
    if matched.row_ids.is_empty() {
        return Err("MERGE MOR UPDATE data sink requires at least one matched row".to_string());
    }
    if matched.new_rows.num_rows() != matched.row_ids.len() {
        return Err(format!(
            "MERGE MOR UPDATE matched row count mismatch: new_rows={}, row_ids={}",
            matched.new_rows.num_rows(),
            matched.row_ids.len()
        ));
    }

    let alias = "__nr_m";
    let mut value_column_names = target_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    value_column_names.push(crate::exec::row_position::ICEBERG_ROW_ID_COL);
    value_column_names.push(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL);

    let mut rows = Vec::with_capacity(matched.new_rows.num_rows());
    for row in 0..matched.new_rows.num_rows() {
        let mut values = Vec::with_capacity(target_columns.len() + 2);
        for target_column in target_columns {
            let idx = matched
                .new_rows
                .schema()
                .index_of(&target_column.name)
                .map_err(|_| {
                    format!(
                        "MERGE MOR UPDATE new-row batch missing target column `{}`",
                        target_column.name
                    )
                })?;
            let literal =
                crate::engine::sql_expr::literal_from_batch(matched.new_rows.column(idx), row)?;
            values.push(
                crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
                    &literal,
                    &target_column.data_type,
                )?,
            );
        }
        values.push(matched.row_ids[row].to_string());
        values.push(new_sequence_number.to_string());
        rows.push(format!("({})", values.join(", ")));
    }

    let value_columns = value_column_names
        .iter()
        .map(|name| sql_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        rows.join(", "),
        sql_identifier(alias),
        value_columns
    );

    let mut select_items = Vec::with_capacity(target_columns.len() + 2);
    for column in target_columns {
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(
                &qualify_column(alias, &column.name),
                column,
            )?,
            sql_identifier(&column.name)
        ));
    }
    select_items.push(format!(
        "CAST({} AS BIGINT) AS {}",
        qualify_column(alias, crate::exec::row_position::ICEBERG_ROW_ID_COL),
        sql_identifier(crate::exec::row_position::ICEBERG_ROW_ID_COL)
    ));
    select_items.push(format!(
        "CAST({} AS BIGINT) AS {}",
        qualify_column(
            alias,
            crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL
        ),
        sql_identifier(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
    ));

    let sql = format!(
        "SELECT {} FROM {} ORDER BY {}",
        select_items.join(", "),
        values_sql,
        qualify_column(alias, crate::exec::row_position::ICEBERG_ROW_ID_COL)
    );
    parse_generated_query(&sql, "MERGE MOR UPDATE data sink")
}

fn build_update_distributed_select_sql(
    target: &crate::engine::backend_resolver::TargetBackend,
    target_alias: &str,
    source_sql: Option<&str>,
    where_sql: Option<&str>,
    target_ref: &str,
    select_items: Vec<String>,
    order_by: Option<String>,
) -> String {
    let version_clause = if target_ref == "main" {
        String::new()
    } else {
        format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref))
    };
    let mut sql = format!(
        "SELECT {} FROM {}{} AS {}",
        select_items.join(", "),
        qualify_iceberg_table(target),
        version_clause,
        sql_identifier(target_alias)
    );
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    if let Some(order_by) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by);
    }
    sql
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context} generated non-query statement: {other}")),
    }
}

fn qualify_iceberg_table(target: &crate::engine::backend_resolver::TargetBackend) -> String {
    format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table)
    )
}

fn qualify_column(alias: &str, column: &str) -> String {
    format!("{}.{}", sql_identifier(alias), sql_identifier(column))
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn execute_mor_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: MatchedUpdateBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
    write: MorUpdateDistributedWrite,
) -> Result<StatementResult, String> {
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    // For branch DML, read partition metadata at the branch head snapshot.
    let read_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
    } else {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    };
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    // The old-row deletions are written as deletion vectors on the BE and
    // committed together with the BE-written replacement data files in one
    // RowDeltaDvFromFiles snapshot. The coordinator no longer materializes
    // positions or pre-loads referenced-file partitions.
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDvFromFiles,
            table_ident,
            read_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    run_mor_update_distributed_transaction(
        state,
        target,
        catalog,
        table,
        collector,
        entry,
        read_snapshot_id,
        target_ref,
        write,
    )?;
    Ok(StatementResult::Ok)
}

struct MorUpdateDistributedWrite {
    data_query: sqlparser::ast::Query,
    data_sink_spec: IcebergWriteSinkSpec,
    /// SELECT that projects `[_file, _pos, <partition src>]` for the matched
    /// old rows. The BE writes a deletion-vector (Puffin) file per data file
    /// through `dv_sink_spec`; the coordinator never materializes positions.
    dv_query: sqlparser::ast::Query,
    dv_sink_spec: IcebergWriteSinkSpec,
}

struct DistributedMorUpdateExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: MorUpdateDistributedWrite,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedMorUpdateExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        // Write the replacement rows (content=Data) on the BE.
        let data = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.write.data_query,
            self.write.data_sink_spec.clone(),
            None,
            None,
        )?;
        if data.write_abort.is_none()
            && data
                .write_commit
                .as_ref()
                .is_none_or(|commit| !write_commit_has_files(commit))
        {
            return Err(
                "MOR UPDATE distributed data sink produced no replacement data files".to_string(),
            );
        }
        // Write the old-row deletion vectors (content=PositionDeletes/Puffin)
        // on the BE, shuffled per `_file` so each data file gets one DV writer.
        let dv = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.write.dv_query,
            self.write.dv_sink_spec.clone(),
            None,
            Some(crate::engine::iceberg_write_shuffle_by_output_index(0)),
        )?;
        // The matched set is already known non-empty (the data guard above
        // enforced it), so a file-less but non-aborted DV result is a real bug:
        // without it `merge_write_commits` would return the replacement data
        // files with `write_abort=None` and the transaction would commit the
        // new rows WITHOUT the old-row deletion vectors (old+new rows both
        // live). Fail fast instead of silently half-committing.
        if dv.write_abort.is_none()
            && dv
                .write_commit
                .as_ref()
                .is_none_or(|commit| !write_commit_has_files(commit))
        {
            return Err(
                "MOR UPDATE distributed DV sink produced no deletion-vector files".to_string(),
            );
        }
        // Both sets of sink_commit_infos flow into one collector → one commit:
        // data files committed as content=Data, DV files as Puffin DVs.
        Ok(merge_write_commits(data, dv))
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

/// Merge the two BE writes of a MOR UPDATE (replacement data files + old-row
/// deletion vectors) into a single coordinated result. The two writers' commit
/// inputs are concatenated into one `WriteCommitInput` so a single collector
/// drives one `RowDeltaDvFromFiles` commit. If either side reported a
/// `write_abort`, that is propagated so the transaction runner can clean up.
fn merge_write_commits(
    data: CoordinatedQueryResult,
    dv: CoordinatedQueryResult,
) -> CoordinatedQueryResult {
    let write_abort = data.write_abort.or(dv.write_abort);
    // Invariant on the success path: both halves are present. The data-side
    // guard ("produced no replacement data files") and the DV-side guard
    // ("produced no deletion-vector files") in `run_coordinated_write` reject
    // a file-less, non-aborted result before we get here, so `(Some, None)`
    // (data files but no DV) is unreachable without a `write_abort`. The
    // residual single-sided arms can therefore only occur on an aborted write
    // (where `write_abort` is set and the partial commit is discarded by the
    // transaction runner); keep them explicit so the invariant is documented
    // rather than silently papered over.
    let write_commit = match (data.write_commit, dv.write_commit) {
        (Some(mut data_commit), Some(dv_commit)) => {
            data_commit.writers.extend(dv_commit.writers);
            Some(data_commit)
        }
        (Some(commit), None) | (None, Some(commit)) => {
            debug_assert!(
                write_abort.is_some(),
                "merge_write_commits saw a single-sided MOR UPDATE commit without an abort; \
                 the data/DV produced-no-files guards should make this unreachable on success",
            );
            Some(commit)
        }
        (None, None) => None,
    };
    CoordinatedQueryResult {
        query_result: data.query_result,
        write_commit,
        write_abort,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_mor_update_distributed_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    write: MorUpdateDistributedWrite,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:mor-update-distributed:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDeltaDvFromFiles,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedMorUpdateExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write,
        commit_executor,
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

fn build_position_delete_groups_from_matched(
    matched: &MatchedUpdateBatch,
    referenced_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    let mut by_file: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for (path, pos) in matched.file_paths.iter().zip(matched.row_positions.iter()) {
        by_file.entry(path.clone()).or_default().push(*pos);
    }
    let mut out = Vec::with_capacity(by_file.len());
    for (file, positions) in by_file {
        let partition = referenced_partitions.get(&file).ok_or_else(|| {
            format!("matched iceberg data file `{file}` is missing partition metadata")
        })?;
        out.push(crate::connector::iceberg::commit::PositionDeleteGroup {
            referenced_data_file: file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions,
        });
    }
    Ok(out)
}

// Intentionally a single-variant carrier. The COW and MOR UPDATE write plans are now
// distributed (see `DistributedCowUpdateExecutor` / `DistributedMorUpdateExecutor`), so
// the only path still routed through `MutationWriteExecutor` is the MERGE matched-DELETE
// side, which still injects coordinator-built delete groups. This enum is deliberately
// retained for Phase 3 (atomic MERGE), which folds every MERGE branch into one collector;
// do not collapse it as a "dead abstraction" before then.
enum MutationWritePlan {
    MergeMatchedDelete { matched: MatchedUpdateBatch },
}

impl MutationWritePlan {
    fn attempt_name(&self) -> &'static str {
        match self {
            Self::MergeMatchedDelete { .. } => "merge-delete",
        }
    }
}

struct MutationWriteExecutor {
    commit_executor: IcebergWriteCommitExecutor,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    plan: Mutex<Option<MutationWritePlan>>,
}

impl IcebergWriteTransactionExecutor for MutationWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let plan = self
            .plan
            .lock()
            .expect("mutation write plan lock poisoned")
            .take()
            .ok_or_else(|| "mutation write plan was already consumed".to_string())?;
        match plan {
            MutationWritePlan::MergeMatchedDelete { matched } => {
                if matched.row_ids.is_empty() {
                    return Ok(no_mutation_write_result());
                }
                let referenced_partitions =
                    crate::engine::delete_flow::load_referenced_data_file_partitions(&self.table)?;
                let delete_groups =
                    build_position_delete_groups_from_matched(&matched, &referenced_partitions)?;
                for group in delete_groups {
                    self.collector.inject_delete_group(group);
                }
            }
        }
        Ok(mutation_write_result(Vec::new()))
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }

    fn has_preloaded_commit_output(&self) -> bool {
        self.collector.has_injected_written_files()
    }
}

fn no_mutation_write_result() -> CoordinatedQueryResult {
    CoordinatedQueryResult {
        query_result: QueryResult::empty(),
        write_commit: None,
        write_abort: None,
    }
}

fn mutation_write_result(
    sink_commit_infos: Vec<crate::types::TSinkCommitInfo>,
) -> CoordinatedQueryResult {
    CoordinatedQueryResult {
        query_result: QueryResult::empty(),
        write_commit: Some(local_writer_commit_input(
            new_local_writer_write_id(),
            sink_commit_infos,
        )),
        write_abort: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_mutation_write_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    commit_op_kind: CommitOpKind,
    operation_kind: IcebergOperationKind,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    plan: MutationWritePlan,
) -> Result<(), String> {
    let attempt_name = plan.attempt_name();
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let collector_for_executor = Arc::clone(&collector);
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table: table.clone(),
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind,
        attempt_id: format!(
            "{}.{}.{}:{}:{}",
            target.catalog,
            target.namespace,
            target.table,
            attempt_name,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = MutationWriteExecutor {
        commit_executor,
        table,
        collector: collector_for_executor,
        plan: Mutex::new(Some(plan)),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_cow_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: MatchedUpdateBatch,
    target_columns: &[crate::engine::catalog::ColumnDef],
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let metadata = table.metadata();
    // For branch DML, commit against the branch head snapshot.
    let base_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(metadata, target_ref)?
    } else {
        metadata.current_snapshot().map(|s| s.snapshot_id())
    };
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::CowUpdate,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    // Plan the distributed whole-file rewrite: one coordinated BE write per
    // touched data file, scoped to that file via an `ExplicitFiles`-bound scan.
    // The FE never reads/rewrites data; it only commits the BE-written files
    // through the unchanged `CowUpdateCommit`.
    let write = build_cow_update_distributed_write(
        state,
        target,
        &table,
        &matched,
        target_columns,
        &entry,
        base_snapshot_id,
    )?;
    run_cow_update_distributed_transaction(
        state,
        target,
        catalog,
        table,
        collector,
        entry,
        base_snapshot_id,
        target_ref,
        write,
    )?;
    Ok(StatementResult::Ok)
}

/// Per-touched-file plan for the distributed COW UPDATE rewrite. Each entry
/// describes one BE write scoped to exactly one old data file: the synthetic
/// `ExplicitFiles`-bound table to register before the write (and drop after),
/// the rewrite SELECT that re-emits every row of that file (replacing matched
/// rows with their new values and preserving `_row_id`), and the matched row
/// ids that live in this file (recorded on the resulting `CowUpdateTouchedFile`).
struct CowFileRewritePlan {
    old_file: String,
    namespace: String,
    synthetic_table_name: String,
    synthetic_table_def: crate::sql::catalog::TableDef,
    rewrite_query: sqlparser::ast::Query,
    matched_row_ids: Vec<i64>,
}

/// Fully-planned distributed COW UPDATE write: the per-file rewrite plans, the
/// shared row-lineage data sink spec, and the commit-side rewrite-set identity.
struct CowUpdateDistributedWrite {
    file_plans: Vec<CowFileRewritePlan>,
    data_sink_spec: IcebergWriteSinkSpec,
    base_snapshot_id: i64,
    target_table_uuid: String,
    updated_row_ids: Vec<i64>,
}

#[allow(clippy::too_many_arguments)]
fn build_cow_update_distributed_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    target_columns: &[crate::engine::catalog::ColumnDef],
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
) -> Result<CowUpdateDistributedWrite, String> {
    let base_snapshot_id =
        base_snapshot_id.ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let data_sink_spec = crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
        target, &resolved, table, entry,
    )?;

    // Index the snapshot's data files by path so each touched file inherits its
    // `first_row_id` / `data_sequence_number` / pre-existing delete files. The
    // BE scan computes `_row_id = first_row_id + _pos` and honors these deletes,
    // so the rewrite re-emits exactly the rows that were live in the file.
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            table,
            base_snapshot_id,
        )?;
    let mut data_file_by_path = std::collections::HashMap::with_capacity(data_files.len());
    for file in data_files {
        data_file_by_path.insert(file.path.clone(), file);
    }

    // Group matched rows by their owning data file, preserving the new-row batch
    // index so the rewrite query can project the replacement values.
    let mut matched_rows_by_file: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, file_path) in matched.file_paths.iter().enumerate() {
        matched_rows_by_file
            .entry(file_path.clone())
            .or_default()
            .push(idx);
    }

    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let mut file_plans = Vec::with_capacity(matched_rows_by_file.len());
    for (old_file, matched_indices) in matched_rows_by_file {
        let data_file = data_file_by_path.get(&old_file).cloned().ok_or_else(|| {
            format!("COW UPDATE matched data file `{old_file}` is missing from snapshot metadata")
        })?;
        let synthetic_table_name = format!(
            "__nr_cow_{}_{}",
            target.table,
            uuid::Uuid::new_v4().simple()
        );
        let synthetic_table_def =
            build_cow_rewrite_synthetic_table_def(entry, target, &synthetic_table_name, data_file)?;
        let matched_row_ids = matched_indices
            .iter()
            .map(|idx| matched.row_ids[*idx])
            .collect::<Vec<_>>();
        let rewrite_query = build_cow_rewrite_query(
            target,
            &synthetic_table_name,
            matched,
            &matched_indices,
            target_columns,
            new_sequence_number,
        )?;
        file_plans.push(CowFileRewritePlan {
            old_file,
            namespace: target.namespace.clone(),
            synthetic_table_name,
            synthetic_table_def,
            rewrite_query,
            matched_row_ids,
        });
    }

    Ok(CowUpdateDistributedWrite {
        file_plans,
        data_sink_spec,
        base_snapshot_id,
        target_table_uuid: table.metadata().uuid().to_string(),
        updated_row_ids: matched.row_ids.clone(),
    })
}

/// Build a synthetic `ExplicitFiles`-bound `TableDef` over exactly one data
/// file. The single-file scan inherits the file's `first_row_id`,
/// `data_sequence_number`, and pre-existing delete files so the BE reads the
/// live rows and exposes the v3 row-lineage `_row_id` /
/// `_last_updated_sequence_number` virtual columns the rewrite query projects.
fn build_cow_rewrite_synthetic_table_def(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    data_file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> Result<crate::sql::catalog::TableDef, String> {
    if data_file.first_row_id.is_none() {
        return Err(format!(
            "COW UPDATE requires first_row_id for iceberg data file `{}`",
            data_file.path
        ));
    }
    let loaded =
        crate::connector::iceberg::catalog::load_table(entry, &target.namespace, &target.table)?;
    let table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        entry,
        &target.catalog,
        &target.namespace,
        &target.table,
        loaded,
        vec![data_file],
    )?;
    // The single-file scan must expose `_row_id` / `_last_updated_sequence_number`
    // for the rewrite projection; the table is v3 row-lineage (COW mode was
    // selected) and the file carries `first_row_id`, so the builder advertises
    // them. Guard against a silent drop.
    if !table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|c| crate::exec::row_position::is_iceberg_row_id(&c.name))
    {
        return Err(format!(
            "COW UPDATE synthetic scan for table {}.{} does not expose _row_id; \
             the data file lacks v3 row-lineage metadata",
            target.namespace, target.table
        ));
    }
    Ok(crate::sql::catalog::TableDef {
        name: synthetic_table_name.to_string(),
        ..table_def
    })
}

/// Build the whole-file rewrite SELECT for one touched data file (approach
/// "drive-from-matched"): scan every live row of the file via the synthetic
/// `ExplicitFiles` table, LEFT JOIN the matched new rows that belong to this
/// file on `_row_id`, and project user columns (replacement value where
/// matched, original otherwise) plus `_row_id` and a conditional
/// `_last_updated_sequence_number`. Ordered by `_row_id` for deterministic
/// output. The matched new values come from the already-materialized
/// `matched.new_rows`, so this path is uniform for both UPDATE and
/// MERGE matched-UPDATE (no source re-join).
fn build_cow_rewrite_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    matched: &MatchedUpdateBatch,
    matched_indices: &[usize],
    target_columns: &[crate::engine::catalog::ColumnDef],
    new_sequence_number: i64,
) -> Result<sqlparser::ast::Query, String> {
    if matched_indices.is_empty() {
        return Err("COW UPDATE rewrite query requires at least one matched row".to_string());
    }
    let scan_alias = "__nr_cow_t";
    let match_alias = "__nr_cow_m";
    let row_id_col = crate::exec::row_position::ICEBERG_ROW_ID_COL;
    let last_seq_col = crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL;

    // VALUES relation of the matched new rows in this file: (_row_id, <user
    // columns...>). Values are typed literals read positionally from the
    // already-materialized `new_rows` batch (mirrors the MERGE MOR data sink).
    let mut value_rows = Vec::with_capacity(matched_indices.len());
    for &idx in matched_indices {
        let mut values = Vec::with_capacity(target_columns.len() + 1);
        values.push(matched.row_ids[idx].to_string());
        for target_column in target_columns {
            let col_idx = matched
                .new_rows
                .schema()
                .index_of(&target_column.name)
                .map_err(|_| {
                    format!(
                        "COW UPDATE new-row batch missing target column `{}`",
                        target_column.name
                    )
                })?;
            let literal =
                crate::engine::sql_expr::literal_from_batch(matched.new_rows.column(col_idx), idx)?;
            values.push(
                crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
                    &literal,
                    &target_column.data_type,
                )?,
            );
        }
        value_rows.push(format!("({})", values.join(", ")));
    }
    let mut match_value_columns = Vec::with_capacity(target_columns.len() + 1);
    match_value_columns.push(sql_identifier(row_id_col));
    for target_column in target_columns {
        match_value_columns.push(sql_identifier(&target_column.name));
    }
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        value_rows.join(", "),
        sql_identifier(match_alias),
        match_value_columns.join(", ")
    );

    let matched_predicate = format!("{} IS NOT NULL", qualify_column(match_alias, row_id_col));

    let mut select_items = Vec::with_capacity(target_columns.len() + 2);
    for column in target_columns {
        // Replacement value where the row matched, original scan value
        // otherwise. The CASE result is cast to the target column type so the
        // sink sees the declared schema (mirrors the MOR/MERGE data sinks).
        let case_expr = format!(
            "CASE WHEN {matched_predicate} THEN {} ELSE {} END",
            qualify_column(match_alias, &column.name),
            qualify_column(scan_alias, &column.name),
        );
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(&case_expr, column)?,
            sql_identifier(&column.name)
        ));
    }
    select_items.push(format!(
        "{} AS {}",
        qualify_column(scan_alias, row_id_col),
        sql_identifier(row_id_col)
    ));
    // Matched rows advance to the new sequence number; untouched rows keep the
    // per-row `_last_updated_sequence_number` the scan synthesized from the
    // file's data sequence number.
    select_items.push(format!(
        "CAST(CASE WHEN {matched_predicate} THEN {} ELSE {} END AS BIGINT) AS {}",
        new_sequence_number,
        qualify_column(scan_alias, last_seq_col),
        sql_identifier(last_seq_col)
    ));

    // Reference the synthetic table explicitly under `default_catalog` so a
    // session-level Iceberg current catalog cannot route it back through the
    // CatalogMgr entry (mirrors the time-travel rewrite).
    let scan_sql = format!(
        "{}.{}.{} AS {}",
        sql_identifier("default_catalog"),
        sql_identifier(&target.namespace),
        sql_identifier(synthetic_table_name),
        sql_identifier(scan_alias),
    );
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} = {} ORDER BY {}",
        select_items.join(", "),
        scan_sql,
        values_sql,
        qualify_column(scan_alias, row_id_col),
        qualify_column(match_alias, row_id_col),
        qualify_column(scan_alias, row_id_col),
    );
    parse_generated_query(&sql, "COW UPDATE rewrite")
}

struct DistributedCowUpdateExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<CowUpdateDistributedWrite>>,
    commit_executor: IcebergWriteCommitExecutor,
    cow_update_rewrite: Mutex<Option<CowUpdateRewriteSet>>,
}

impl IcebergWriteTransactionExecutor for DistributedCowUpdateExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let write = self
            .write
            .lock()
            .expect("COW UPDATE write plan lock poisoned")
            .take()
            .ok_or_else(|| "COW UPDATE write plan was already consumed".to_string())?;
        if write.file_plans.is_empty() {
            return Ok(no_mutation_write_result());
        }

        let mut merged_commit: Option<WriteCommitInput> = None;
        let mut touched_data_files = Vec::with_capacity(write.file_plans.len());
        for plan in write.file_plans {
            let new_files = self.run_one_file_rewrite(&plan, &write.data_sink_spec)?;
            // Merge this file's writer commits into the single transaction-wide
            // `WriteCommitInput`; the collector turns all of them into committed
            // data files in one `CowUpdateCommit`.
            if let Some(commit) = new_files.write_commit {
                match merged_commit.as_mut() {
                    Some(existing) => existing.writers.extend(commit.writers),
                    None => merged_commit = Some(commit),
                }
            }
            touched_data_files.push(CowUpdateTouchedFile {
                old_file: plan.old_file,
                new_files: new_files.paths,
                row_ids: plan.matched_row_ids,
            });
        }

        let write_commit = merged_commit.ok_or_else(|| {
            "COW UPDATE distributed rewrite produced no replacement data files".to_string()
        })?;
        if !write_commit_has_files(&write_commit) {
            return Err(
                "COW UPDATE distributed rewrite produced no replacement data files".to_string(),
            );
        }

        *self
            .cow_update_rewrite
            .lock()
            .expect("COW UPDATE rewrite lock poisoned") = Some(CowUpdateRewriteSet {
            base_snapshot_id: write.base_snapshot_id,
            target_table_uuid: write.target_table_uuid,
            updated_row_ids: write.updated_row_ids,
            touched_data_files,
            // Pure UPDATE appends no net-new data files; only a folded MERGE
            // not-matched INSERT (M3) populates this.
            appended_files: Vec::new(),
        });

        Ok(CoordinatedQueryResult {
            query_result: QueryResult::empty(),
            write_commit: Some(write_commit),
            write_abort: None,
        })
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let commit_executor = IcebergWriteCommitExecutor {
            state: Arc::clone(&self.commit_executor.state),
            target: self.commit_executor.target.clone(),
            catalog: Arc::clone(&self.commit_executor.catalog),
            table: self.commit_executor.table.clone(),
            collector: Arc::clone(&self.commit_executor.collector),
            fs: self.commit_executor.fs.clone(),
            cleanup_path_mapper: self.commit_executor.cleanup_path_mapper.clone(),
            cow_update_rewrite: self
                .cow_update_rewrite
                .lock()
                .expect("COW UPDATE rewrite lock poisoned")
                .clone(),
            target_ref: self.commit_executor.target_ref.clone(),
            snapshot_properties: self.commit_executor.snapshot_properties.clone(),
        };
        commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

/// One file's BE rewrite output: the replacement data-file paths (for the
/// `CowUpdateTouchedFile.new_files` mapping) and the coordinated write commit
/// that carries them.
struct CowFileRewriteOutput {
    paths: Vec<String>,
    write_commit: Option<WriteCommitInput>,
}

impl DistributedCowUpdateExecutor {
    /// Register the synthetic single-file table, run the scoped BE rewrite, and
    /// always drop the synthetic table afterwards (even on error). The write's
    /// reported data-file paths become this old file's `new_files`.
    fn run_one_file_rewrite(
        &self,
        plan: &CowFileRewritePlan,
        data_sink_spec: &IcebergWriteSinkSpec,
    ) -> Result<CowFileRewriteOutput, String> {
        crate::engine::query_prep::register_external_table_for_query(
            &self.state,
            &plan.namespace,
            plan.synthetic_table_def.clone(),
        )?;
        let result = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &plan.rewrite_query,
            data_sink_spec.clone(),
            None,
            None,
        );
        let drop_result = crate::engine::query_prep::drop_registered_external_table(
            &self.state,
            &plan.namespace,
            &plan.synthetic_table_name,
        );
        let result = result?;
        drop_result?;

        if let Some(abort) = &result.write_abort {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` aborted: {}",
                plan.old_file, abort.reason
            ));
        }
        let write_commit = result.write_commit.filter(write_commit_has_files);
        let Some(commit) = write_commit else {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` produced no replacement data files",
                plan.old_file
            ));
        };
        // Extract the replacement file paths from the writer-reported sink
        // commit infos. These go through the same `convert_sink_commit_info`
        // the commit collector uses, so the recorded `new_files` paths match
        // the collector's `written` paths exactly (CowUpdateCommit requires
        // bidirectional set equality).
        let mut paths = Vec::new();
        for writer in &commit.writers {
            for info in &writer.sink_commit_infos {
                let file = self
                    .commit_executor
                    .collector
                    .convert_sink_commit_info(info.clone())?;
                paths.push(file.path);
            }
        }
        if paths.is_empty() {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` produced no replacement data files",
                plan.old_file
            ));
        }
        Ok(CowFileRewriteOutput {
            paths,
            write_commit: Some(commit),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn run_cow_update_distributed_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    write: CowUpdateDistributedWrite,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:cow-update-distributed:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::CowUpdate,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedCowUpdateExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        commit_executor,
        cow_update_rewrite: Mutex::new(None),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    old_rows: RecordBatch,
    new_rows: RecordBatch,
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_mgr(
        state,
        current_catalog,
        current_database,
        &query,
        None,
    )?;
    matched_update_batch_from_query_result(result)
}

fn matched_update_batch_from_query_result(
    result: crate::engine::QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let Some(first_chunk) = result.chunks.first() else {
        return empty_matched_update_batch();
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|chunk| chunk.batch.clone())
        .collect::<Vec<_>>();
    let batch = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate UPDATE match batches failed: {e}"))?;
    matched_update_batch_from_record_batch(&batch)
}

fn matched_update_batch_from_record_batch(
    batch: &RecordBatch,
) -> Result<MatchedUpdateBatch, String> {
    if batch.num_rows() == 0 {
        return empty_matched_update_batch();
    }

    let file_col = cast(required_column(batch, "__nr_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast __nr_file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "__nr_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_pos to Int64 failed: {e}"))?;
    let row_id_col = cast(required_column(batch, "__nr_row_id")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_row_id to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__nr_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_pos was not Int64 after cast".to_string())?;
    let row_id_arr = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_row_id was not Int64 after cast".to_string())?;

    let mut file_paths = Vec::with_capacity(batch.num_rows());
    let mut row_positions = Vec::with_capacity(batch.num_rows());
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) || row_id_arr.is_null(row) {
            return Err("UPDATE match query produced null row identity columns".to_string());
        }
        file_paths.push(file_arr.value(row).to_string());
        row_positions.push(pos_arr.value(row));
        row_ids.push(row_id_arr.value(row));
    }

    let old_indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().starts_with("__nr_"))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let old_fields = old_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect::<Vec<_>>();
    let old_schema = Arc::new(Schema::new(old_fields));
    let old_columns = old_indices
        .iter()
        .map(|idx| batch.column(*idx).clone())
        .collect::<Vec<_>>();
    let old_rows = RecordBatch::try_new(old_schema.clone(), old_columns)
        .map_err(|e| format!("build UPDATE old-row batch failed: {e}"))?;

    let mut new_columns = Vec::with_capacity(old_schema.fields().len());
    for (old_idx, field) in old_indices.iter().zip(old_schema.fields().iter()) {
        let new_name = format!("__nr_new_{}", field.name());
        let column = match batch.schema().index_of(&new_name) {
            Ok(idx) => cast(batch.column(idx), field.data_type()).map_err(|e| {
                format!(
                    "cast UPDATE assignment column `{new_name}` to {:?} failed: {e}",
                    field.data_type()
                )
            })?,
            Err(_) => batch.column(*old_idx).clone(),
        };
        new_columns.push(column);
    }
    let new_rows = RecordBatch::try_new(old_schema, new_columns)
        .map_err(|e| format!("build UPDATE new-row batch failed: {e}"))?;

    Ok(MatchedUpdateBatch {
        row_ids,
        file_paths,
        row_positions,
        old_rows,
        new_rows,
    })
}

fn empty_matched_update_batch() -> Result<MatchedUpdateBatch, String> {
    let schema = Arc::new(Schema::empty());
    let empty = RecordBatch::new_empty(schema);
    Ok(MatchedUpdateBatch {
        row_ids: Vec::new(),
        file_paths: Vec::new(),
        row_positions: Vec::new(),
        old_rows: empty.clone(),
        new_rows: empty,
    })
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

fn iceberg_table_columns(
    table: &iceberg::table::Table,
) -> Result<Vec<crate::engine::catalog::ColumnDef>, String> {
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    Ok(arrow_schema
        .fields()
        .iter()
        .map(|field| crate::engine::catalog::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect())
}

fn iceberg_partition_source_columns(table: &iceberg::table::Table) -> Result<Vec<String>, String> {
    let schema = table.metadata().current_schema();
    let mut out = Vec::new();
    for field in table.metadata().default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} is missing from iceberg schema",
                field.source_id
            )
        })?;
        out.push(source.name.clone());
    }
    Ok(out)
}

fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[crate::engine::catalog::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}

fn validate_unique_target_row_ids(row_ids: &[i64]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for row_id in row_ids {
        if !seen.insert(*row_id) {
            return Err(format!(
                "UPDATE source matched target row _row_id={} more than once; deduplicate the source before retrying",
                row_id
            ));
        }
    }
    Ok(())
}

fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };
    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}

// ---------------------------------------------------------------------------
// MERGE INTO
// ---------------------------------------------------------------------------

const MERGE_TARGET_DEFAULT_ALIAS: &str = "__nr_t";
const MERGE_SOURCE_DEFAULT_ALIAS: &str = "__nr_s";

pub(crate) fn execute_merge_statement(
    state: &Arc<StandaloneState>,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;

    // Reject variant tables before any planning (mirrors UPDATE entry).
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("MERGE INTO: {e}"))?;

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;

    // The match SELECT is built against the v3 row-lineage target so the
    // matched-side path can reuse the UPDATE executor. Validate the v3
    // requirement up front instead of letting the executor surface it.
    let _ = select_iceberg_update_mode(&table)?;

    if let Some(clause) = stmt.matched.as_ref()
        && let MergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target_columns, &partition_columns)?;
    }
    let insert_columns_resolved = if let Some(clause) = stmt.not_matched.as_ref() {
        Some(resolve_merge_insert_columns(
            &clause.action,
            &target_columns,
        )?)
    } else {
        None
    };

    let match_rows = materialize_merge_match(
        state,
        &target,
        stmt,
        current_catalog,
        &target_columns,
        insert_columns_resolved.as_deref(),
    )?;

    let mut applied_change = false;
    if let Some(clause) = stmt.not_matched.as_ref() {
        let insert_columns = insert_columns_resolved
            .as_ref()
            .expect("not_matched populated => insert columns resolved");
        let insert_batch = match_rows.unmatched_insert_batch(&target_columns, insert_columns)?;
        if insert_batch.num_rows() > 0 {
            execute_merge_unmatched_insert(
                state,
                &target,
                stmt,
                current_catalog,
                &target_columns,
                insert_columns,
            )?;
            applied_change = true;
        }
        let _ = clause;
    }

    if let Some(clause) = stmt.matched.as_ref() {
        let matched = matched_update_batch_from_record_batch(&match_rows.matched_batch()?)?;
        if !matched.row_ids.is_empty() {
            validate_unique_target_row_ids(&matched.row_ids)?;
            match &clause.action {
                MergeMatchedAction::Update { .. } => {
                    let mode = select_iceberg_update_mode(&table)?;
                    let catalog_for_op = build_iceberg_catalog(&entry)?;
                    let table_for_op =
                        block_on_iceberg(async { catalog_for_op.load_table(&table_ident).await })?
                            .map_err(|e| format!("reload iceberg table {}: {e}", &table_ident))?;
                    match mode {
                        IcebergUpdateMode::CopyOnWrite => execute_cow_update(
                            state,
                            &target,
                            catalog_for_op.clone(),
                            table_ident.clone(),
                            table_for_op,
                            matched,
                            &target_columns,
                            entry.clone(),
                            "main",
                        )?,
                        IcebergUpdateMode::MergeOnRead => {
                            let write = build_merge_mor_distributed_write(
                                state,
                                &target,
                                &matched,
                                &target_columns,
                            )?;
                            execute_mor_update(
                                state,
                                &target,
                                catalog_for_op.clone(),
                                table_ident.clone(),
                                table_for_op,
                                matched,
                                entry.clone(),
                                "main",
                                write,
                            )?
                        }
                    };
                    applied_change = true;
                }
                MergeMatchedAction::Delete => {
                    let catalog_for_op = build_iceberg_catalog(&entry)?;
                    let table_for_op =
                        block_on_iceberg(async { catalog_for_op.load_table(&table_ident).await })?
                            .map_err(|e| format!("reload iceberg table {}: {e}", &table_ident))?;
                    execute_merge_matched_delete(
                        state,
                        &target,
                        catalog_for_op.clone(),
                        table_ident.clone(),
                        table_for_op,
                        matched,
                        entry.clone(),
                    )?;
                    applied_change = true;
                }
            }
        }
    }

    let _ = applied_change;
    Ok(StatementResult::Ok)
}

/// Resolved target column ordering for `WHEN NOT MATCHED INSERT`. Each entry
/// maps a target column name to either an explicit value expression (sourced
/// from `INSERT (cols) VALUES (exprs)`) or a `NULL` default when the user did
/// not list the column. Validates that every named column exists, that the
/// list has no duplicates, and that no reserved row-lineage column is named.
struct MergeInsertColumns {
    columns: Vec<MergeInsertColumn>,
}

struct MergeInsertColumn {
    name: String,
    /// `Some(idx)` when the user supplied a value for this target column at
    /// position `idx` in the `VALUES` tuple. `None` means "no value
    /// supplied"; we project a NULL of the column's type instead.
    value_index: Option<usize>,
}

impl std::ops::Deref for MergeInsertColumns {
    type Target = [MergeInsertColumn];
    fn deref(&self) -> &[MergeInsertColumn] {
        &self.columns
    }
}

fn resolve_merge_insert_columns(
    action: &MergeNotMatchedAction,
    target_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<MergeInsertColumns, String> {
    let target_names_lower: Vec<String> = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();

    // Empty `INSERT VALUES (...)` (no column list) means "values match target
    // schema in declaration order". Iceberg row-lineage columns (`_row_id`
    // etc.) are reserved/owned and never appear in the user-visible target
    // schema returned from `iceberg_table_columns`, so we don't have to
    // filter them here.
    if action.columns.is_empty() {
        if action.values.len() != target_columns.len() {
            return Err(format!(
                "MERGE WHEN NOT MATCHED INSERT VALUES count {} does not match target column count {}",
                action.values.len(),
                target_columns.len()
            ));
        }
        let columns = target_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| MergeInsertColumn {
                name: col.name.clone(),
                value_index: Some(idx),
            })
            .collect();
        return Ok(MergeInsertColumns { columns });
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut by_target: HashMap<String, usize> = HashMap::new();
    for (idx, raw_name) in action.columns.iter().enumerate() {
        let lower = raw_name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "MERGE INSERT cannot assign reserved Iceberg metadata column `{raw_name}`"
            ));
        }
        if !target_names_lower.contains(&lower) {
            return Err(format!(
                "MERGE INSERT references unknown target column `{raw_name}`"
            ));
        }
        if !seen.insert(lower.clone()) {
            return Err(format!(
                "MERGE INSERT lists target column `{raw_name}` more than once"
            ));
        }
        by_target.insert(lower, idx);
    }

    let columns = target_columns
        .iter()
        .map(|col| MergeInsertColumn {
            name: col.name.clone(),
            value_index: by_target.get(&col.name.to_ascii_lowercase()).copied(),
        })
        .collect();
    Ok(MergeInsertColumns { columns })
}

struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

impl MergeMatchRows {
    fn empty() -> Self {
        Self {
            full: RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn matched_batch(&self) -> Result<RecordBatch, String> {
        if self.full.num_rows() == 0 {
            return Ok(self.full.clone());
        }
        let filter = self.row_filter("matched", "__nr_matched_apply")?;
        filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE matched rows failed: {e}"))
    }

    fn unmatched_insert_batch(
        &self,
        target_columns: &[crate::engine::catalog::ColumnDef],
        insert_columns: &MergeInsertColumns,
    ) -> Result<RecordBatch, String> {
        let target_arrow_schema = arrow::datatypes::Schema::new(
            target_columns
                .iter()
                .map(|c| {
                    arrow::datatypes::Field::new(c.name.clone(), c.data_type.clone(), c.nullable)
                })
                .collect::<Vec<_>>(),
        );
        let target_arrow_schema = Arc::new(target_arrow_schema);
        if self.full.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }
        let filter = self.row_filter("unmatched", "__nr_unmatched_apply")?;
        let filtered = filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE unmatched rows failed: {e}"))?;
        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_columns.len());
        for (target_col, insert_entry) in target_columns.iter().zip(insert_columns.iter()) {
            debug_assert_eq!(target_col.name, insert_entry.name);
            let column = match insert_entry.value_index {
                Some(_) => {
                    let projected_name = format!("__nr_ins_{}", target_col.name);
                    let idx = filtered.schema().index_of(&projected_name).map_err(|_| {
                        format!("MERGE INSERT projection missing column `{projected_name}`")
                    })?;
                    cast(filtered.column(idx), &target_col.data_type).map_err(|e| {
                        format!(
                            "cast MERGE INSERT column `{}` to {:?} failed: {e}",
                            target_col.name, target_col.data_type
                        )
                    })?
                }
                None => arrow::array::new_null_array(&target_col.data_type, filtered.num_rows()),
            };
            columns.push(column);
        }
        RecordBatch::try_new(target_arrow_schema, columns)
            .map_err(|e| format!("build MERGE INSERT batch failed: {e}"))
    }

    fn row_filter(&self, kind: &str, apply_col: &str) -> Result<BooleanArray, String> {
        let kind_col = cast(
            required_column(&self.full, "__nr_match_kind")?,
            &DataType::Utf8,
        )
        .map_err(|e| format!("cast __nr_match_kind to Utf8 failed: {e}"))?;
        let kind_arr = kind_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "__nr_match_kind was not Utf8 after cast".to_string())?;
        let apply_col = cast(required_column(&self.full, apply_col)?, &DataType::Boolean)
            .map_err(|e| format!("cast {apply_col} to Boolean failed: {e}"))?;
        let apply_arr = apply_col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "MERGE apply column was not Boolean after cast".to_string())?;

        let mut bits = Vec::with_capacity(self.full.num_rows());
        for row in 0..self.full.num_rows() {
            if kind_arr.is_null(row) {
                bits.push(false);
                continue;
            }
            let matches_kind = kind_arr.value(row) == kind;
            let applies = !apply_arr.is_null(row) && apply_arr.value(row);
            bits.push(matches_kind && applies);
        }
        Ok(BooleanArray::from(bits))
    }
}

fn materialize_merge_match(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
) -> Result<MergeMatchRows, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = format!("{} AS {}", target.table, target_alias);

    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    // `mutation_source_to_sql` preserves the user-provided alias when present.
    // When the source carries no alias, inject `__nr_s` so the projection /
    // ON predicate can reference source columns deterministically.
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let on_sql = stmt.on.to_string();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| (a.column.clone(), a.value.to_string()))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow: Vec<(&str, &str)> = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index
                        .map(|idx| (col.name.clone(), action.values[idx].to_string()))
                })
                .collect(),
            _ => Vec::new(),
        };
    let insert_values_sql_borrow: Vec<(&str, &str)> = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &on_sql,
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
    );

    let result = execute_merge_match_query(state, Some(&target.catalog), &sql, &target.namespace)?;
    Ok(result)
}

fn execute_merge_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MergeMatchRows, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal MERGE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_mgr(
        state,
        current_catalog,
        current_database,
        &query,
        None,
    )?;
    let Some(first_chunk) = result.chunks.first() else {
        return Ok(MergeMatchRows::empty());
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|c| c.batch.clone())
        .collect::<Vec<_>>();
    let full = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate MERGE match batches failed: {e}"))?;
    Ok(MergeMatchRows { full })
}

fn build_merge_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: &str,
    on_sql: &str,
    matched_predicate_sql: Option<&str>,
    not_matched_predicate_sql: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    matched_assignments_sql: &[(&str, &str)],
    insert_values_sql: &[(&str, &str)],
) -> String {
    let quote_ident = |ident: &str| format!("`{}`", ident.replace('`', "``"));
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            quote_ident(column)
        } else {
            format!("{target_alias}.{}", quote_ident(column))
        }
    };
    let nullable_target_column = |column: &str| {
        let row_id = qualify("_row_id");
        let value = qualify(column);
        format!("CASE WHEN {row_id} IS NOT NULL THEN {value} ELSE NULL END")
    };
    let target_select_items = target_columns
        .iter()
        .map(|column| {
            format!(
                "{} AS {}",
                nullable_target_column(&column.name),
                quote_ident(&column.name)
            )
        })
        .collect::<Vec<_>>();

    let mut select_items = vec![
        format!("{} AS __nr_file", nullable_target_column("_file")),
        format!("{} AS __nr_pos", nullable_target_column("_pos")),
        format!("{} AS __nr_row_id", nullable_target_column("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            nullable_target_column("_last_updated_sequence_number")
        ),
        format!(
            "(CASE WHEN {} IS NOT NULL THEN 'matched' ELSE 'unmatched' END) AS __nr_match_kind",
            qualify("_row_id")
        ),
    ];
    select_items.extend(target_select_items);
    select_items.push(format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END) AS __nr_matched_apply",
        matched_predicate_sql.unwrap_or("TRUE")
    ));
    select_items.push(format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END) AS __nr_unmatched_apply",
        not_matched_predicate_sql.unwrap_or("TRUE")
    ));
    for (column, expr) in matched_assignments_sql {
        select_items.push(format!("({expr}) AS __nr_new_{column}"));
    }
    for (column, expr) in insert_values_sql {
        select_items.push(format!("({expr}) AS __nr_ins_{column}"));
    }

    format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        on_sql
    )
}

fn build_merge_unmatched_insert_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    insert_columns: &MergeInsertColumns,
) -> Result<sqlparser::ast::Query, String> {
    let target_alias = stmt
        .target_alias
        .as_deref()
        .unwrap_or(MERGE_TARGET_DEFAULT_ALIAS);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };
    let not_matched = stmt
        .not_matched
        .as_ref()
        .ok_or_else(|| "MERGE unmatched INSERT write requires a not-matched clause".to_string())?;
    let select_items = target_columns
        .iter()
        .zip(insert_columns.iter())
        .map(|(target_column, insert_column)| {
            if target_column.name != insert_column.name {
                return Err(format!(
                    "MERGE INSERT column order mismatch: target `{}`, insert `{}`",
                    target_column.name, insert_column.name
                ));
            }
            let raw_expr = match insert_column.value_index {
                Some(idx) => format!("({})", not_matched.action.values[idx]),
                None => "NULL".to_string(),
            };
            let expr =
                crate::engine::iceberg_writer::target_cast_expr_sql(&raw_expr, target_column)?;
            Ok(format!("{expr} AS {}", sql_identifier(&target_column.name)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target_sql = format!(
        "{} AS {}",
        qualify_iceberg_table(target),
        sql_identifier(target_alias)
    );
    let mut predicates = vec![format!(
        "{} IS NULL",
        qualify_column(target_alias, crate::exec::row_position::ICEBERG_ROW_ID_COL)
    )];
    if let Some(predicate) = not_matched.predicate.as_ref() {
        predicates.push(format!("({predicate})"));
    }
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} WHERE {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        stmt.on,
        predicates.join(" AND ")
    );
    parse_generated_query(&sql, "MERGE unmatched INSERT sink")
}

fn execute_merge_matched_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: MatchedUpdateBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<StatementResult, String> {
    let metadata = table.metadata();
    let current_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDv,
            table_ident,
            current_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    run_mutation_write_transaction(
        state,
        target,
        catalog,
        table,
        collector,
        entry,
        CommitOpKind::RowDeltaDv,
        IcebergOperationKind::RowDelta,
        current_snapshot_id,
        "main",
        MutationWritePlan::MergeMatchedDelete { matched },
    )?;
    Ok(StatementResult::Ok)
}

fn execute_merge_unmatched_insert(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[crate::engine::catalog::ColumnDef],
    insert_columns: &MergeInsertColumns,
) -> Result<StatementResult, String> {
    let query = build_merge_unmatched_insert_query(
        state,
        target,
        stmt,
        current_catalog,
        target_columns,
        insert_columns,
    )?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    crate::engine::iceberg_writer::execute_iceberg_insert_or_overwrite(
        state,
        target,
        &resolved,
        &[],
        &InsertSource::FromQuery(Box::new(query)),
        OverwriteMode::None,
        "main",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_target() -> crate::engine::backend_resolver::TargetBackend {
        crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
        }
    }

    #[test]
    fn mor_update_uses_be_dv_sink_not_coordinator_inject() {
        let source = include_str!("mutation_flow.rs");

        // The MOR-update entry must no longer materialize the old-row
        // deletions on the coordinator. Both coordinator-local delete helpers
        // must be gone from `execute_mor_update`.
        let execute = source
            .split("fn execute_mor_update")
            .nth(1)
            .expect("execute_mor_update fn")
            .split("\nstruct MorUpdateDistributedWrite")
            .next()
            .expect("execute_mor_update body");
        assert!(
            !execute.contains("build_position_delete_groups_from_matched"),
            "execute_mor_update must not build coordinator-local position-delete groups"
        );
        assert!(
            !execute.contains("inject_delete_group"),
            "execute_mor_update must not inject coordinator-local delete groups"
        );

        // The MOR-update transaction must commit BE-written DV files via
        // RowDeltaDvFromFiles and must not fall back to the coordinator-built
        // RowDeltaDv path. The trailing space avoids matching RowDeltaDvFromFiles.
        let transaction = source
            .split("fn run_mor_update_distributed_transaction")
            .nth(1)
            .expect("run_mor_update_distributed_transaction fn")
            .split("\nfn build_position_delete_groups_from_matched")
            .next()
            .expect("run_mor_update_distributed_transaction body");
        assert!(
            transaction.contains("RowDeltaDvFromFiles"),
            "MOR-update transaction must commit BE-written Puffin DV files via RowDeltaDvFromFiles"
        );
        assert!(
            !transaction.contains("CommitOpKind::RowDeltaDv "),
            "MOR-update transaction must not commit via the coordinator-built RowDeltaDv path"
        );
        assert!(
            !transaction.contains("inject_delete_group"),
            "MOR-update transaction must not inject coordinator-local delete groups"
        );
    }

    #[test]
    fn cow_update_uses_distributed_rewrite_not_local_scan() {
        let source = include_str!("mutation_flow.rs");

        // The COW-update write must be a distributed BE rewrite: each touched
        // file is rewritten by an `execute_query_as_iceberg_write` call (BE
        // writes the replacement data files; FE only commits). It must no
        // longer call the in-process FE rewrite helpers `write_cow_update_files`
        // / `build_cow_rewrite_batches`, which scanned and rewrote files locally
        // on the coordinator. Slice the whole COW-distributed executor section
        // (planner + executor + per-file rewrite) and assert on it.
        let body = source
            .split("struct DistributedCowUpdateExecutor")
            .nth(1)
            .expect("DistributedCowUpdateExecutor")
            .split("fn run_cow_update_distributed_transaction")
            .next()
            .expect("COW UPDATE distributed executor body");
        assert!(
            body.contains("execute_query_as_iceberg_write"),
            "COW UPDATE write path must issue a distributed BE rewrite via execute_query_as_iceberg_write"
        );
        assert!(
            !body.contains("write_cow_update_files"),
            "COW UPDATE write path must not call the in-process FE rewrite write_cow_update_files"
        );
        assert!(
            !body.contains("build_cow_rewrite_batches"),
            "COW UPDATE write path must not call the in-process FE local scan build_cow_rewrite_batches"
        );
    }

    #[test]
    fn cow_rewrite_query_rewrites_whole_file_and_preserves_row_id() {
        // Two matched rows (row_ids 7,9) for one touched file; the rewrite query
        // must scan the whole file via the synthetic ExplicitFiles table, LEFT
        // JOIN the matched new values on `_row_id`, project user columns
        // (replacement where matched, original scan value otherwise), preserve
        // `_row_id`, and bump `_last_updated_sequence_number` only for matched
        // rows.
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Utf8, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![2, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["bb", "dd"])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7, 9],
            file_paths: vec!["f.parquet".to_string(), "f.parquet".to_string()],
            row_positions: vec![1, 3],
            old_rows,
            new_rows,
        };

        let query = build_cow_rewrite_query(
            &iceberg_target(),
            "__nr_cow_t_abc",
            &matched,
            &[0, 1],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Utf8),
            ],
            42,
        )
        .expect("query");
        let sql = query.to_string();

        // Scans the synthetic ExplicitFiles table under default_catalog (so a
        // session iceberg catalog cannot reroute it), LEFT JOINs the matched
        // VALUES on `_row_id`, and orders by `_row_id`.
        assert!(sql.contains("`default_catalog`"), "{sql}");
        assert!(sql.contains("`__nr_cow_t_abc`"), "{sql}");
        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("VALUES"), "{sql}");
        // Whole-file rewrite: no outer WHERE filter (all rows re-emitted).
        assert!(!sql.contains(" WHERE "), "{sql}");
        // Conditional replacement on the match key, `_row_id` preserved from the
        // scan, and the new sequence number applied only to matched rows.
        assert!(sql.contains("CASE WHEN"), "{sql}");
        assert!(sql.contains("IS NOT NULL"), "{sql}");
        assert!(sql.contains("AS `_row_id`"), "{sql}");
        assert!(sql.contains("_last_updated_sequence_number"), "{sql}");
        assert!(sql.contains("42"), "{sql}");
        // Replacement values flow from the matched new_rows VALUES.
        assert!(sql.contains("'bb'"), "{sql}");
        assert!(sql.contains("'dd'"), "{sql}");
        assert!(sql.contains("ORDER BY"), "{sql}");
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }

    #[test]
    fn duplicate_row_ids_are_rejected() {
        let err = validate_unique_target_row_ids(&[7, 8, 7]).expect_err("duplicate");
        assert!(err.contains("_row_id=7"), "{err}");
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        let sql = build_update_match_query_sql(
            "ice.db1.t AS t",
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
        );
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
        assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
    }

    #[test]
    fn update_mor_data_sink_query_projects_row_lineage_columns() {
        let query = build_update_mor_data_sink_query(
            &iceberg_target(),
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
            &[col("id"), col("v")],
            "main",
            42,
        )
        .expect("query");
        let sql = query.to_string();
        assert!(sql.contains("_row_id"), "{sql}");
        assert!(sql.contains("_last_updated_sequence_number"), "{sql}");
        assert!(sql.contains("42"), "{sql}");
        assert!(sql.contains("ORDER BY"), "{sql}");
    }

    #[test]
    fn merge_mor_data_sink_query_uses_materialized_matched_rows() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Int64, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![22])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7],
            file_paths: vec!["file.parquet".to_string()],
            row_positions: vec![3],
            old_rows,
            new_rows,
        };

        let query =
            build_merge_mor_data_sink_query_from_matched(&matched, &[col("id"), col("v")], 42)
                .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("VALUES"), "{sql}");
        assert!(sql.contains("AS `_row_id`"), "{sql}");
        assert!(sql.contains("_last_updated_sequence_number"), "{sql}");
        assert!(!sql.contains("JOIN"), "{sql}");
        assert!(!sql.contains("ice.db1.t"), "{sql}");
    }

    fn typed_col(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn dv_sink_columns() -> Vec<ColumnDef> {
        vec![
            typed_col(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN,
                DataType::Utf8,
            ),
            typed_col(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN,
                DataType::Int64,
            ),
            typed_col("id", DataType::Int64),
        ]
    }

    #[test]
    fn update_dv_sink_query_projects_file_pos_and_partition_sources() {
        let query = build_update_dv_sink_query(
            &iceberg_target(),
            "t",
            Some("staging.s AS s"),
            Some("t.id = s.id"),
            "main",
            &dv_sink_columns(),
        )
        .expect("query");
        let sql = query.to_string();

        // Projects the position-delete identity + partition source columns,
        // all qualified by the target alias, with the same CROSS JOIN / WHERE
        // tail as the data sink query and ordered by `_file` for the per-file
        // DV shuffle.
        assert!(sql.contains("`t`.`_file` AS `_file`"), "{sql}");
        assert!(sql.contains("`t`.`_pos` AS `_pos`"), "{sql}");
        assert!(sql.contains("`t`.`id` AS `id`"), "{sql}");
        assert!(sql.contains("CROSS JOIN staging.s AS s"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
        assert!(sql.contains("ORDER BY `t`.`_file`"), "{sql}");
        assert!(!sql.contains("FOR VERSION AS OF"), "{sql}");
    }

    #[test]
    fn update_dv_sink_query_pins_branch_read_snapshot() {
        let query = build_update_dv_sink_query(
            &iceberg_target(),
            "t",
            None,
            Some("t.id = 1"),
            "dev",
            &dv_sink_columns(),
        )
        .expect("query");
        let sql = query.to_string();
        assert!(
            sql.contains("FOR SYSTEM_TIME AS OF '__nr_ref:dev'"),
            "{sql}"
        );
    }

    #[test]
    fn merge_mor_dv_sink_query_uses_materialized_matched_identities() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Int64, true),
        ]));
        // Distinctive partition value (42) so we can assert it flows from
        // old_rows into the generated VALUES rather than from any other field
        // (row_ids=7, row_positions=3, file path). `v` is not a partition
        // source column so its value (55) must not leak into the projection.
        let old_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![42])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![55])) as ArrayRef,
            ],
        )
        .expect("old rows");
        let new_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7],
            file_paths: vec!["file.parquet".to_string()],
            row_positions: vec![3],
            old_rows,
            new_rows,
        };

        let query = build_merge_mor_dv_sink_query_from_matched(&matched, &dv_sink_columns())
            .expect("query");
        let sql = query.to_string();

        // VALUES-based DV side: identities come from the matched batch (no
        // target scan / join), partition source `id` read from old_rows.
        assert!(sql.contains("VALUES"), "{sql}");
        assert!(sql.contains("'file.parquet'"), "{sql}");
        assert!(sql.contains("AS `_file`"), "{sql}");
        assert!(sql.contains("AS `_pos`"), "{sql}");
        assert!(sql.contains("AS `id`"), "{sql}");
        // The partition value must come from old_rows: the distinctive literal
        // 42 appears in the VALUES, while the non-partition `v` value 55 does
        // not leak into the generated SQL.
        assert!(
            sql.contains("42"),
            "partition value must come from old_rows: {sql}"
        );
        assert!(
            !sql.contains("55"),
            "non-partition column value must not leak: {sql}"
        );
        assert!(sql.contains("ORDER BY"), "{sql}");
        assert!(!sql.contains("JOIN"), "{sql}");
        assert!(!sql.contains("ice.db1.t"), "{sql}");
    }

    #[test]
    fn merge_match_query_projects_nullable_target_columns() {
        let sql = build_merge_match_query_sql(
            "ice.db1.t AS t",
            "t",
            "staging.s AS s",
            "t.id = s.id",
            None,
            None,
            &[col("id"), col("v")],
            &[("v", "s.v")],
            &[("id", "s.id"), ("v", "s.v")],
        );

        assert!(!sql.contains("t.*"), "{sql}");
        assert!(
            sql.contains("CASE WHEN t.`_row_id` IS NOT NULL THEN t.`id` ELSE NULL END AS `id`"),
            "{sql}"
        );
        assert!(sql.contains("(s.v) AS __nr_new_v"), "{sql}");
        assert!(sql.contains("(s.id) AS __nr_ins_id"), "{sql}");
    }

    #[test]
    fn merge_unmatched_insert_query_uses_distributed_append_shape() {
        let raw = crate::sql::parser::parse_sql_raw(
            "MERGE INTO t AS t \
             USING (SELECT 3 AS id, 4 AS v) AS s \
             ON t.id = s.id \
             WHEN NOT MATCHED AND s.id > 0 THEN INSERT (id) VALUES (s.id)",
        )
        .expect("parse MERGE");
        let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(&raw)
            .expect("convert MERGE");
        let target_columns = vec![col("id"), col("v")];
        let insert_columns = resolve_merge_insert_columns(
            &stmt.not_matched.as_ref().expect("not matched").action,
            &target_columns,
        )
        .expect("insert columns");
        let state = Arc::new(StandaloneState::default());

        let query = build_merge_unmatched_insert_query(
            &state,
            &iceberg_target(),
            &stmt,
            None,
            &target_columns,
            &insert_columns,
        )
        .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("_row_id"), "{sql}");
        assert!(sql.contains("IS NULL"), "{sql}");
        assert!(sql.contains("CAST((s.id) AS BIGINT) AS `id`"), "{sql}");
        assert!(sql.contains("CAST(NULL AS BIGINT) AS `v`"), "{sql}");
        assert!(sql.contains("(s.id > 0)"), "{sql}");
    }
}
